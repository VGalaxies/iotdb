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
import org.apache.iotdb.isession.ITableSession;
import org.apache.iotdb.isession.SessionConfig;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.streamnode.conf.StreamNodeConfig;
import org.apache.iotdb.streamnode.engine.dispatcher.ColumnPartitionedTabletDispatcher;
import org.apache.iotdb.streamnode.engine.source.IoTDBSubscriptionSourceInstance;
import org.apache.iotdb.streamnode.engine.task.StreamSubTask;
import org.apache.iotdb.streamnode.engine.task.StreamSubTask.DataSlice;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class})
public class StreamNodeManualRuntimeAssemblyIT extends AbstractStreamNodeIT {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(StreamNodeManualRuntimeAssemblyIT.class);

  private static final String DATABASE_NAME = "stream_manual_runtime_db";
  private static final String TABLE_NAME = "stream_manual_runtime_source";
  private static final String TASK_NAME = "stream_manual_runtime_it";
  private static final int EXPECTED_ROW_COUNT = 48;

  private IoTDBSubscriptionSourceInstance sourceInstance;

  @Override
  @After
  public void tearDown() throws Exception {
    try {
      if (sourceInstance != null) {
        sourceInstance.stop();
      }
    } catch (final Exception e) {
      LOGGER.warn("Error stopping source instance", e);
    } finally {
      super.tearDown();
    }
  }

  @Test
  public void testManualComponentsCompleteSourceDispatcherSubTaskFlow() throws Exception {
    final String host = EnvFactory.getEnv().getIP();
    final int port = Integer.parseInt(EnvFactory.getEnv().getPort());

    insertPartitionedData();

    final IoTDBSubscriptionSource source =
        new IoTDBSubscriptionSource(
            DATABASE_NAME,
            TABLE_NAME,
            null,
            Arrays.asList("status", "bucket"),
            host,
            port,
            SessionConfig.DEFAULT_USER,
            SessionConfig.DEFAULT_PASSWORD);
    final StreamNodeConfig nodeConfig = new StreamNodeConfig();
    nodeConfig.setSnInternalAddress(host);
    nodeConfig.setSnInternalPort(10821);

    final Map<PartitionKey, RecordingStreamSubTask> subTasks = new ConcurrentHashMap<>();
    final ColumnPartitionedTabletDispatcher dispatcher =
        new ColumnPartitionedTabletDispatcher(
            source.getPartitionColumns(),
            partitionKey -> subTasks.computeIfAbsent(partitionKey, RecordingStreamSubTask::new));

    final CountDownLatch rowsDispatchedLatch = new CountDownLatch(EXPECTED_ROW_COUNT);
    final AtomicInteger dispatchedRows = new AtomicInteger(0);
    final AtomicLong maxSourceIndex = new AtomicLong(-1L);
    final AtomicLong lastCommittedSourceIndex = new AtomicLong(-1L);

    final RecordingIoTDBSubscriptionSourceInstance recordingSourceInstance =
        new RecordingIoTDBSubscriptionSourceInstance(
            source,
            TASK_NAME,
            (tablet, sourceIndex) -> {
              dispatcher.dispatch(tablet, sourceIndex);
              dispatchedRows.addAndGet(tablet.getRowSize());
              maxSourceIndex.updateAndGet(current -> Math.max(current, sourceIndex));
              for (int row = 0; row < tablet.getRowSize(); row++) {
                rowsDispatchedLatch.countDown();
              }
            },
            nodeConfig);
    sourceInstance = recordingSourceInstance;

    sourceInstance.start();

    Assert.assertTrue(
        "Manually assembled source should dispatch all rows through the dispatcher",
        rowsDispatchedLatch.await(60, TimeUnit.SECONDS));

    AWAIT.untilAsserted(
        () -> {
          Assert.assertTrue(
              "Should dispatch at least " + EXPECTED_ROW_COUNT + " rows",
              dispatchedRows.get() >= EXPECTED_ROW_COUNT);
          Assert.assertTrue(
              "Multi-column partitioning should create multiple subtasks", subTasks.size() >= 2);
          Assert.assertTrue(
              "Partition keys should contain status and bucket segments",
              subTasks.keySet().stream().allMatch(key -> key.segmentNum() == 2));
          Assert.assertTrue(
              "Every subtask should receive at least one data slice",
              subTasks.values().stream().allMatch(task -> !task.getReceivedSlices().isEmpty()));
          Assert.assertTrue(
              "Every subtask should advance commit progress",
              subTasks.values().stream().allMatch(task -> task.getCommitId() >= 1));
        });

    final long minimumSubTaskCommitId = getMinimumSubTaskCommitId(subTasks);
    Assert.assertTrue(
        "Source commit should be based on downstream subtask progress",
        minimumSubTaskCommitId >= 1);
    Assert.assertTrue(maxSourceIndex.get() >= minimumSubTaskCommitId);

    commitSourceProgressFromSubTasks(sourceInstance, subTasks, lastCommittedSourceIndex);
    Assert.assertEquals(minimumSubTaskCommitId, lastCommittedSourceIndex.get());
    Assert.assertEquals(1, recordingSourceInstance.getCommitCallCount());
    Assert.assertEquals(minimumSubTaskCommitId, recordingSourceInstance.getLastCommitIndex());
  }

  private long commitSourceProgressFromSubTasks(
      final IoTDBSubscriptionSourceInstance source,
      final Map<PartitionKey, RecordingStreamSubTask> subTasks,
      final AtomicLong lastCommittedSourceIndex) {
    final long minimumSubTaskCommitId = getMinimumSubTaskCommitId(subTasks);
    if (minimumSubTaskCommitId > lastCommittedSourceIndex.get()) {
      source.commit(minimumSubTaskCommitId);
      lastCommittedSourceIndex.set(minimumSubTaskCommitId);
    }
    return lastCommittedSourceIndex.get();
  }

  private long getMinimumSubTaskCommitId(final Map<PartitionKey, RecordingStreamSubTask> subTasks) {
    return subTasks.values().stream().mapToLong(StreamSubTask::getCommitId).min().orElse(-1L);
  }

  private void insertPartitionedData() throws Exception {
    try (final ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      session.executeNonQueryStatement("CREATE DATABASE " + DATABASE_NAME);
      session.executeNonQueryStatement("USE " + DATABASE_NAME);
      session.executeNonQueryStatement(
          String.format(
              "CREATE TABLE %s (device_id STRING TAG, status INT32 FIELD, bucket INT32 FIELD, value INT64 FIELD)",
              TABLE_NAME));

      for (int row = 0; row < EXPECTED_ROW_COUNT; row++) {
        session.executeNonQueryStatement(
            String.format(
                "INSERT INTO %s (time, device_id, status, bucket, value) VALUES (%d, '%s', %d, %d, %d)",
                TABLE_NAME,
                System.currentTimeMillis() + row,
                "device_" + (row % 4),
                row % 2,
                (row / 2) % 3,
                row));
      }
      session.executeNonQueryStatement("FLUSH");
    }
  }

  private static class RecordingStreamSubTask extends StreamSubTask {

    private final List<DataSlice> receivedSlices = new CopyOnWriteArrayList<>();

    private RecordingStreamSubTask(final PartitionKey partitionKey) {
      super(partitionKey, new PeriodWindow(1, 0));
    }

    @Override
    public java.util.concurrent.Future<Void> offer(final List<DataSlice> dataSlices) {
      receivedSlices.addAll(dataSlices);
      return super.offer(dataSlices);
    }

    private List<DataSlice> getReceivedSlices() {
      return receivedSlices;
    }
  }

  private static class RecordingIoTDBSubscriptionSourceInstance
      extends IoTDBSubscriptionSourceInstance {

    private final AtomicInteger commitCallCount = new AtomicInteger(0);
    private final AtomicLong lastCommitIndex = new AtomicLong(-1L);

    private RecordingIoTDBSubscriptionSourceInstance(
        final IoTDBSubscriptionSource sourceConfig,
        final String taskName,
        final java.util.function.BiConsumer<org.apache.tsfile.write.record.Tablet, Long>
            dataConsumer,
        final StreamNodeConfig streamNodeConfig) {
      super(sourceConfig, taskName, dataConsumer, streamNodeConfig);
    }

    @Override
    public synchronized void commit(final long idx) {
      super.commit(idx);
      commitCallCount.incrementAndGet();
      lastCommitIndex.set(idx);
    }

    private int getCommitCallCount() {
      return commitCallCount.get();
    }

    private long getLastCommitIndex() {
      return lastCommitIndex.get();
    }
  }
}
