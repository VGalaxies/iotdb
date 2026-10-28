/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.streamnode.it;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.commons.queryengine.plan.relational.metadata.ColumnSchema;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.EventScanNode;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.commons.stream.IoTDBSubscriptionSource;
import org.apache.iotdb.commons.stream.IoTDBTarget;
import org.apache.iotdb.commons.stream.PartitionKey;
import org.apache.iotdb.commons.stream.StreamNodeTableTypeProvider;
import org.apache.iotdb.commons.stream.StreamTask;
import org.apache.iotdb.commons.stream.TumbleWindow;
import org.apache.iotdb.commons.utils.AuthUtils;
import org.apache.iotdb.isession.ITableSession;
import org.apache.iotdb.isession.SessionConfig;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.env.cluster.node.ConfigNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.streamnode.client.ConfigNodeInfo;
import org.apache.iotdb.streamnode.engine.task.AbstractStreamTaskInstance;
import org.apache.iotdb.streamnode.engine.task.EventStreamTaskInstance;
import org.apache.iotdb.streamnode.engine.task.StreamSubTask;

import org.apache.tsfile.common.constant.TsFileConstant;
import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.type.FloatType;
import org.apache.tsfile.read.common.type.IntType;
import org.apache.tsfile.read.common.type.StringType;
import org.apache.tsfile.read.common.type.TimestampType;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class})
public class StreamTaskInstanceIT extends AbstractStreamNodeIT {

  private static final Logger LOGGER = LoggerFactory.getLogger(StreamTaskInstanceIT.class);

  private static final String DATABASE_NAME = "stream_task_instance_db";
  private static final String TABLE_NAME = "stream_task_instance_table";
  private static final String SINK_TABLE_NAME = "stream_task_instance_sink_table";
  private static final String TASK_NAME = "stream_task_instance_it";
  private static final int EXPECTED_ROW_COUNT = 100;

  private static final String ENCRYPTED_DEFAULT_PASSWORD =
      AuthUtils.encryptPassword(SessionConfig.DEFAULT_PASSWORD);

  private EventStreamTaskInstance taskInstance;

  @After
  public void stopTaskInstance() {
    if (taskInstance != null) {
      try {
        taskInstance.drop();
      } catch (Exception e) {
        LOGGER.warn("Error dropping stream task instance", e);
      }
    }
  }

  @Test
  public void testStreamTaskInstanceDispatchesSubscriptionDataToSubTasks() throws Exception {
    final String host = EnvFactory.getEnv().getIP();
    final int port = Integer.parseInt(EnvFactory.getEnv().getPort());
    final ConfigNodeWrapper configNodeWrapper =
        EnvFactory.getEnv().getConfigNodeWrapperList().get(0);
    ConfigNodeInfo.getInstance()
        .updateConfigNodeList(
            Collections.singletonList(
                new TEndPoint(configNodeWrapper.getIp(), configNodeWrapper.getPort())));

    createTestTables();

    final StreamTask streamTask =
        buildStreamTask(TASK_NAME, Collections.singletonList("status"), host, port);

    createSubscriptionTopic((IoTDBSubscriptionSource) streamTask.getSource(), TASK_NAME);

    taskInstance = (EventStreamTaskInstance) AbstractStreamTaskInstance.create(streamTask);
    taskInstance.start();
    Assert.assertTrue("StreamTaskInstance should be running", taskInstance.isRunning());

    // The subscription only delivers writes that arrive after the topic exists, so insert the
    // rows now that the source is running, like IoTDBSubscriptionSourceInstanceIT does.
    insertTestRows();

    AWAIT.untilAsserted(
        () -> {
          final Map<PartitionKey, StreamSubTask> subTasks = taskInstance.getSubTasksSnapshot();
          Assert.assertTrue(
              "Should create at least two status-partitioned subtasks, got " + subTasks.size(),
              subTasks.size() >= 2);
          Assert.assertTrue(
              "Status partition keys should contain one segment",
              subTasks.keySet().stream().allMatch(key -> key.segmentNum() == 1));
          Assert.assertTrue(
              "Status partitioning should create distinct partition keys",
              subTasks.keySet().stream()
                      .map(key -> String.valueOf(key.segmentValue(0)))
                      .distinct()
                      .count()
                  >= 2);
          Assert.assertTrue(
              "Subtasks should observe subscription commit progress",
              subTasks.values().stream().anyMatch(subTask -> subTask.getCommitId() >= 1));
        });

    assertCommitProgressCanBePropagated();
  }

  private StreamTask buildStreamTask(
      final String taskName,
      final List<String> partitionColumns,
      final String host,
      final int port) {
    final Symbol time = new Symbol("time");
    final Symbol deviceId = new Symbol("device_id");
    final Symbol temperature = new Symbol("temperature");
    final Symbol status = new Symbol("status");
    final EventScanNode eventScanNode =
        new EventScanNode(
            new PlanNodeId("event-scan"),
            List.of(time, deviceId, temperature, status),
            Map.of(
                time,
                    new ColumnSchema(
                        "time", TimestampType.TIMESTAMP, false, TsTableColumnCategory.TIME),
                deviceId,
                    new ColumnSchema(
                        "device_id", StringType.STRING, false, TsTableColumnCategory.TAG),
                temperature,
                    new ColumnSchema(
                        "temperature", FloatType.FLOAT, false, TsTableColumnCategory.FIELD),
                status,
                    new ColumnSchema("status", IntType.INT32, false, TsTableColumnCategory.FIELD)));

    final StreamTask streamTask = new StreamTask();
    streamTask.setTaskName(taskName);
    streamTask.setDatabase(DATABASE_NAME);
    streamTask.setSource(
        new IoTDBSubscriptionSource(
            DATABASE_NAME,
            TABLE_NAME,
            null,
            partitionColumns,
            host,
            port,
            SessionConfig.DEFAULT_USER,
            ENCRYPTED_DEFAULT_PASSWORD));
    final TumbleWindow window = new TumbleWindow(1000L, 0L);
    window.setEventInputColumns(
        List.of(TsFileConstant.TIME_COLUMN_ID, "device_id", "temperature", "status"));
    streamTask.setWindow(window);
    streamTask.setTypeProvider(
        new StreamNodeTableTypeProvider(
            Map.of(
                time, TimestampType.TIMESTAMP,
                deviceId, StringType.STRING,
                temperature, FloatType.FLOAT,
                status, IntType.INT32)));
    streamTask.setCalcPlan(eventScanNode.serializeToByteBuffer());
    streamTask.setTarget(
        new IoTDBTarget(
            host + ":" + port,
            DATABASE_NAME,
            SINK_TABLE_NAME,
            List.of("time", "device_id", "temperature", "status"),
            List.of(TSDataType.TIMESTAMP, TSDataType.STRING, TSDataType.FLOAT, TSDataType.INT32),
            List.of(
                ColumnCategory.TIME,
                ColumnCategory.TAG,
                ColumnCategory.FIELD,
                ColumnCategory.FIELD),
            null));
    return streamTask;
  }

  private void assertCommitProgressCanBePropagated() throws Exception {
    AWAIT.untilAsserted(
        () -> {
          final long minimumSubTaskCommitId = taskInstance.getMinimumSubTaskCommitId();
          Assert.assertTrue(
              "All created subtasks should advance past the initial commit index",
              minimumSubTaskCommitId >= 1);

          taskInstance.commitProcessedProgress();
          Assert.assertTrue(
              "StreamTaskInstance should commit processed source progress",
              taskInstance.getLastCommittedSourceIndex() >= minimumSubTaskCommitId);
        });
  }

  private void createTestTables() throws Exception {
    try (final ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      session.executeNonQueryStatement("CREATE DATABASE " + DATABASE_NAME);
      session.executeNonQueryStatement("USE " + DATABASE_NAME);
      session.executeNonQueryStatement(
          String.format(
              "CREATE TABLE %s (device_id STRING TAG, temperature FLOAT FIELD, status INT32 FIELD)",
              TABLE_NAME));
      session.executeNonQueryStatement(
          String.format(
              "CREATE TABLE %s (device_id STRING TAG, temperature FLOAT FIELD, status INT32 FIELD)",
              SINK_TABLE_NAME));
    }
  }

  private void insertTestRows() throws Exception {
    try (final ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      session.executeNonQueryStatement("USE " + DATABASE_NAME);
      for (int i = 0; i < EXPECTED_ROW_COUNT; i++) {
        session.executeNonQueryStatement(
            String.format(
                "INSERT INTO %s (time, device_id, temperature, status) VALUES (%d, '%s', %f, %d)",
                TABLE_NAME,
                System.currentTimeMillis() + i,
                "device_" + (i % 10),
                20.0f + i * 0.1f,
                i % 2));
      }

      session.executeNonQueryStatement("FLUSH");
    }
  }
}
