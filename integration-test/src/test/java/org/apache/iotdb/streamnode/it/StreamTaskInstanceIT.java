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

import org.apache.iotdb.commons.stream.IoTDBSubscriptionSource;
import org.apache.iotdb.commons.stream.PartitionKey;
import org.apache.iotdb.commons.stream.PeriodWindow;
import org.apache.iotdb.commons.stream.StreamTask;
import org.apache.iotdb.isession.ITableSession;
import org.apache.iotdb.isession.SessionConfig;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.streamnode.engine.task.StreamSubTask;
import org.apache.iotdb.streamnode.engine.task.StreamTaskInstance;

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
  private static final String TASK_NAME = "stream_task_instance_it";
  private static final int EXPECTED_ROW_COUNT = 100;

  private StreamTaskInstance taskInstance;

  @Override
  @After
  public void tearDown() throws Exception {
    try {
      if (taskInstance != null) {
        try {
          taskInstance.stop();
        } catch (Exception e) {
          LOGGER.warn("Error stopping stream task instance", e);
        }
      }
    } finally {
      super.tearDown();
    }
  }

  @Test
  public void testStreamTaskInstanceDispatchesSubscriptionDataToSubTasks() throws Exception {
    final String host = EnvFactory.getEnv().getIP();
    final int port = Integer.parseInt(EnvFactory.getEnv().getPort());

    insertTestData();

    final StreamTask streamTask =
        buildStreamTask(TASK_NAME, Collections.singletonList("status"), host, port);

    taskInstance = new StreamTaskInstance(streamTask);
    taskInstance.start();
    Assert.assertTrue("StreamTaskInstance should be running", taskInstance.isRunning());

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
    final StreamTask streamTask = new StreamTask();
    streamTask.setTaskName(taskName);
    streamTask.setSource(
        new IoTDBSubscriptionSource(
            DATABASE_NAME,
            TABLE_NAME,
            null,
            partitionColumns,
            host,
            port,
            SessionConfig.DEFAULT_USER,
            SessionConfig.DEFAULT_PASSWORD));
    streamTask.setWindow(new PeriodWindow(1, 0));
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

  private void insertTestData() throws Exception {
    try (final ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      session.executeNonQueryStatement("CREATE DATABASE " + DATABASE_NAME);
      session.executeNonQueryStatement("USE " + DATABASE_NAME);
      session.executeNonQueryStatement(
          String.format(
              "CREATE TABLE %s (device_id STRING TAG, temperature FLOAT FIELD, status INT32 FIELD)",
              TABLE_NAME));

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
