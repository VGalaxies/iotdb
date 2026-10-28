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
import org.apache.iotdb.commons.stream.StreamProperties;
import org.apache.iotdb.commons.stream.TumbleWindow;
import org.apache.iotdb.commons.utils.AuthUtils;
import org.apache.iotdb.isession.ITableSession;
import org.apache.iotdb.isession.SessionConfig;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.streamnode.conf.StreamNodeConfig;
import org.apache.iotdb.streamnode.engine.dispatcher.ColumnPartitionedTabletDispatcher;
import org.apache.iotdb.streamnode.engine.dispatcher.TabletDispatcher;
import org.apache.iotdb.streamnode.engine.sink.CommitTracker;
import org.apache.iotdb.streamnode.engine.source.IoTDBSubscriptionSourceInstance;
import org.apache.iotdb.streamnode.engine.task.StreamSubTask;
import org.apache.iotdb.streamnode.engine.task.StreamSubTaskContext;
import org.apache.iotdb.streamnode.engine.task.StreamSubTaskStateMachine;
import org.apache.iotdb.streamnode.engine.window.IEventInfo;
import org.apache.iotdb.streamnode.engine.window.WindowEngine;

import org.apache.tsfile.common.constant.TsFileConstant;
import org.apache.tsfile.write.record.Tablet;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Integration test for IoTDBSubscriptionSourceInstance. This test verifies that the subscription
 * source can connect to an IoTDB cluster, subscribe to a table, and receive data through the
 * dataConsumer callback.
 */
@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class})
public class IoTDBSubscriptionSourceInstanceIT extends AbstractStreamNodeIT {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(IoTDBSubscriptionSourceInstanceIT.class);

  private static final String DATABASE_NAME = "test_db";
  private static final String TABLE_NAME = "test_table";
  private static final String OTHER_TABLE_NAME = "ignored_table";
  private static final String ENCRYPTED_DEFAULT_PASSWORD =
      AuthUtils.encryptPassword(SessionConfig.DEFAULT_PASSWORD);
  private static final int EXPECTED_ROW_COUNT = 100;

  /** Deterministic window layout used by {@code testSourceCommitIsGatedBySinkFlush}. */
  private static final long WINDOW_SIZE_MS = 1000L;

  private static final long ROW_INTERVAL_MS = 50L;
  private static final int ROWS_PER_WINDOW = (int) (WINDOW_SIZE_MS / ROW_INTERVAL_MS);

  /** Window-aligned start timestamp so the tumbles fall on exact boundaries. */
  private static final long FIRST_ROW_TIME = 1_700_000_000_000L;

  private IoTDBSubscriptionSourceInstance sourceInstance;
  private final List<Tablet> receivedTablets = new CopyOnWriteArrayList<>();
  private final AtomicInteger receivedRowCount = new AtomicInteger(0);

  @After
  public void dropSourceInstance() {
    if (sourceInstance != null) {
      try {
        sourceInstance.drop();
      } catch (Exception e) {
        LOGGER.warn("Error dropping source instance", e);
      }
    }
  }

  @Test
  public void testSubscriptionSourceReceivesData() throws Exception {
    final String host = EnvFactory.getEnv().getIP();
    final int port = Integer.parseInt(EnvFactory.getEnv().getPort());

    // Step 1: Create database and table
    createTestTable();

    // Step 2: Create and configure IoTDBSubscriptionSource
    final IoTDBSubscriptionSource source =
        new IoTDBSubscriptionSource(
            DATABASE_NAME,
            TABLE_NAME,
            null, // no preFilter
            null, // no partitionColumns
            host,
            port,
            SessionConfig.DEFAULT_USER,
            ENCRYPTED_DEFAULT_PASSWORD);
    createSubscriptionTopic(source, "test_task");

    // Step 3: Create StreamNodeConfig
    final StreamNodeConfig nodeConfig = new StreamNodeConfig();
    nodeConfig.setSnInternalAddress(host);
    nodeConfig.setSnInternalPort(10820);

    // Step 4: Create IoTDBSubscriptionSourceInstance with data consumer
    final CountDownLatch dataReceivedLatch = new CountDownLatch(EXPECTED_ROW_COUNT);
    sourceInstance = new IoTDBSubscriptionSourceInstance(source, "test_task", nodeConfig);

    // Step 5: Start the source instance
    sourceInstance.start(
        (tablet, commitIndex) -> {
          LOGGER.info(
              "Received tablet with {} rows, commitIndex: {}", tablet.getRowSize(), commitIndex);
          receivedTablets.add(tablet);
          int rows = tablet.getRowSize();
          receivedRowCount.addAndGet(rows);
          for (int i = 0; i < rows; i++) {
            dataReceivedLatch.countDown();
          }
        });
    LOGGER.info("IoTDBSubscriptionSourceInstance started");

    // Step 6: Insert test data after starting the source instance
    insertTestRows();

    // Step 7: Wait for data to be received
    boolean receivedAll = dataReceivedLatch.await(60, TimeUnit.SECONDS);
    Assert.assertTrue(
        "Should receive all " + EXPECTED_ROW_COUNT + " rows within timeout", receivedAll);

    // Step 8: Verify received data
    AWAIT.untilAsserted(
        () -> {
          int totalRows = receivedRowCount.get();
          LOGGER.info("Total rows received: {}", totalRows);
          Assert.assertTrue(
              "Should receive at least " + EXPECTED_ROW_COUNT + " rows, got " + totalRows,
              totalRows >= EXPECTED_ROW_COUNT);
        });

    // Step 9: Verify tablet structure
    Assert.assertFalse("Should have received at least one tablet", receivedTablets.isEmpty());
    Tablet firstTablet = receivedTablets.get(0);
    Assert.assertNotNull("Tablet should not be null", firstTablet);
    Assert.assertTrue("Tablet should have rows", firstTablet.getRowSize() > 0);

    LOGGER.info(
        "Test completed successfully. Received {} tablets with total {} rows",
        receivedTablets.size(),
        receivedRowCount.get());
  }

  @Test
  public void testSubscriptionSourceCommit() throws Exception {
    final String host = EnvFactory.getEnv().getIP();
    final int port = Integer.parseInt(EnvFactory.getEnv().getPort());

    // Create database and table before subscribing
    createTestTable();

    // Create source
    final IoTDBSubscriptionSource source =
        new IoTDBSubscriptionSource(
            DATABASE_NAME,
            TABLE_NAME,
            null,
            null,
            host,
            port,
            SessionConfig.DEFAULT_USER,
            ENCRYPTED_DEFAULT_PASSWORD);
    createSubscriptionTopic(source, "test_commit_task");

    // Create config
    final StreamNodeConfig nodeConfig = new StreamNodeConfig();
    nodeConfig.setSnInternalAddress(host);
    nodeConfig.setSnInternalPort(10820);

    // Track commit indices
    final List<Long> commitIndices = new CopyOnWriteArrayList<>();
    final CountDownLatch dataReceivedLatch = new CountDownLatch(EXPECTED_ROW_COUNT);

    sourceInstance = new IoTDBSubscriptionSourceInstance(source, "test_commit_task", nodeConfig);

    // Start and wait for data
    sourceInstance.start(
        (tablet, commitIndex) -> {
          commitIndices.add(commitIndex);
          int rows = tablet.getRowSize();
          for (int i = 0; i < rows; i++) {
            dataReceivedLatch.countDown();
          }
        });

    // Insert test data after starting the source instance
    insertTestRows();

    boolean receivedAll = dataReceivedLatch.await(60, TimeUnit.SECONDS);
    Assert.assertTrue("Should receive all data", receivedAll);

    // Verify commit indices are sequential and increasing
    AWAIT.untilAsserted(
        () -> {
          Assert.assertFalse("Should have commit indices", commitIndices.isEmpty());
          for (int i = 1; i < commitIndices.size(); i++) {
            Assert.assertTrue(
                "Commit indices should be increasing",
                commitIndices.get(i) > commitIndices.get(i - 1));
          }
        });

    // Test commit operation
    if (!commitIndices.isEmpty()) {
      long maxIndex = commitIndices.stream().max(Long::compare).orElse(0L);
      sourceInstance.commit(maxIndex);
      LOGGER.info("Successfully committed up to index {}", maxIndex);
    }
  }

  @Test
  public void testSubscriptionSourceDispatchesDataToSubTasks() throws Exception {
    final String host = EnvFactory.getEnv().getIP();
    final int port = Integer.parseInt(EnvFactory.getEnv().getPort());

    createTestTable();

    final IoTDBSubscriptionSource source =
        new IoTDBSubscriptionSource(
            DATABASE_NAME,
            TABLE_NAME,
            null,
            Collections.singletonList("status"),
            host,
            port,
            SessionConfig.DEFAULT_USER,
            ENCRYPTED_DEFAULT_PASSWORD);
    createSubscriptionTopic(source, "test_dispatch_task");

    final StreamNodeConfig nodeConfig = new StreamNodeConfig();
    nodeConfig.setSnInternalAddress(host);
    nodeConfig.setSnInternalPort(10820);

    final Map<PartitionKey, StreamSubTask> subTasks = new ConcurrentHashMap<>();
    final TabletDispatcher dispatcher =
        new ColumnPartitionedTabletDispatcher(
            source.getPartitionColumns(),
            (partitionKey, slices) ->
                subTasks
                    .computeIfAbsent(
                        partitionKey, key -> new StreamSubTask(key, new TumbleWindow(1000, 0)))
                    .offer(slices),
            null);

    final AtomicLong firstCommitIndex = new AtomicLong(-1);
    final CountDownLatch firstTabletDispatchedLatch = new CountDownLatch(1);

    sourceInstance = new IoTDBSubscriptionSourceInstance(source, "test_dispatch_task", nodeConfig);

    sourceInstance.start(
        (tablet, commitIndex) -> {
          dispatcher.dispatch(tablet, commitIndex);
          firstCommitIndex.compareAndSet(-1, commitIndex);
          firstTabletDispatchedLatch.countDown();
        });

    insertTestRows();

    Assert.assertTrue(
        "Should dispatch at least one tablet",
        firstTabletDispatchedLatch.await(60, TimeUnit.SECONDS));

    AWAIT.untilAsserted(
        () -> {
          Assert.assertFalse("Should create at least one subtask", subTasks.isEmpty());
          Assert.assertTrue(
              "At least one subtask should advance to the first source commit index",
              subTasks.values().stream()
                  .anyMatch(subTask -> subTask.getCommitId() >= firstCommitIndex.get()));
        });

    sourceInstance.commit(firstCommitIndex.get());
  }

  @Test
  public void testSubscriptionSourceDispatchesByMultiplePartitionColumns() throws Exception {
    final String host = EnvFactory.getEnv().getIP();
    final int port = Integer.parseInt(EnvFactory.getEnv().getPort());

    createBucketedTestTable();

    final IoTDBSubscriptionSource source =
        new IoTDBSubscriptionSource(
            DATABASE_NAME,
            TABLE_NAME,
            null,
            Arrays.asList("status", "bucket"),
            host,
            port,
            SessionConfig.DEFAULT_USER,
            ENCRYPTED_DEFAULT_PASSWORD);
    createSubscriptionTopic(source, "test_multi_partition_dispatch_task");

    final StreamNodeConfig nodeConfig = new StreamNodeConfig();
    nodeConfig.setSnInternalAddress(host);
    nodeConfig.setSnInternalPort(10820);

    final Map<PartitionKey, StreamSubTask> subTasks = new ConcurrentHashMap<>();
    final TabletDispatcher dispatcher =
        new ColumnPartitionedTabletDispatcher(
            source.getPartitionColumns(),
            (partitionKey, slices) ->
                subTasks
                    .computeIfAbsent(
                        partitionKey, key -> new StreamSubTask(key, new TumbleWindow(1000, 0)))
                    .offer(slices),
            null);

    final CountDownLatch dataReceivedLatch = new CountDownLatch(EXPECTED_ROW_COUNT);
    final AtomicInteger dispatchedRows = new AtomicInteger(0);
    final AtomicLong maxCommitIndex = new AtomicLong(-1);

    sourceInstance =
        new IoTDBSubscriptionSourceInstance(
            source, "test_multi_partition_dispatch_task", nodeConfig);

    sourceInstance.start(
        (tablet, commitIndex) -> {
          dispatcher.dispatch(tablet, commitIndex);
          dispatchedRows.addAndGet(tablet.getRowSize());
          maxCommitIndex.updateAndGet(current -> Math.max(current, commitIndex));
          for (int i = 0; i < tablet.getRowSize(); i++) {
            dataReceivedLatch.countDown();
          }
        });

    insertBucketedTestRows();

    Assert.assertTrue(
        "Should receive bucketed table rows", dataReceivedLatch.await(60, TimeUnit.SECONDS));

    AWAIT.untilAsserted(
        () -> {
          Assert.assertTrue(
              "Should dispatch at least " + EXPECTED_ROW_COUNT + " rows",
              dispatchedRows.get() >= EXPECTED_ROW_COUNT);
          Assert.assertFalse("Should create partitioned subtasks", subTasks.isEmpty());
          Assert.assertTrue(
              "Multiple configured partition columns should create two-segment partition keys",
              subTasks.keySet().stream().allMatch(key -> key.segmentNum() == 2));
          Assert.assertTrue(
              "All partitioned subtasks should observe commit progress",
              subTasks.values().stream().allMatch(subTask -> subTask.getCommitId() >= 1));
        });

    sourceInstance.commit(maxCommitIndex.get());
  }

  @Test
  public void testSubscriptionSourceDispatchesMissingPartitionColumnToNullKey() throws Exception {
    final String host = EnvFactory.getEnv().getIP();
    final int port = Integer.parseInt(EnvFactory.getEnv().getPort());

    createTestTable();

    final IoTDBSubscriptionSource source =
        new IoTDBSubscriptionSource(
            DATABASE_NAME,
            TABLE_NAME,
            null,
            Collections.singletonList("missing_partition_column"),
            host,
            port,
            SessionConfig.DEFAULT_USER,
            ENCRYPTED_DEFAULT_PASSWORD);
    createSubscriptionTopic(source, "test_missing_partition_dispatch_task");

    final StreamNodeConfig nodeConfig = new StreamNodeConfig();
    nodeConfig.setSnInternalAddress(host);
    nodeConfig.setSnInternalPort(10820);

    final Map<PartitionKey, StreamSubTask> subTasks = new ConcurrentHashMap<>();
    final TabletDispatcher dispatcher =
        new ColumnPartitionedTabletDispatcher(
            source.getPartitionColumns(),
            (partitionKey, slices) ->
                subTasks
                    .computeIfAbsent(
                        partitionKey, key -> new StreamSubTask(key, new TumbleWindow(1000, 0)))
                    .offer(slices),
            null);

    final CountDownLatch dataReceivedLatch = new CountDownLatch(EXPECTED_ROW_COUNT);
    final AtomicLong maxCommitIndex = new AtomicLong(-1);

    sourceInstance =
        new IoTDBSubscriptionSourceInstance(
            source, "test_missing_partition_dispatch_task", nodeConfig);

    sourceInstance.start(
        (tablet, commitIndex) -> {
          dispatcher.dispatch(tablet, commitIndex);
          maxCommitIndex.updateAndGet(current -> Math.max(current, commitIndex));
          for (int i = 0; i < tablet.getRowSize(); i++) {
            dataReceivedLatch.countDown();
          }
        });

    insertTestRows();

    Assert.assertTrue(
        "Should receive rows with missing partition configuration",
        dataReceivedLatch.await(60, TimeUnit.SECONDS));

    AWAIT.untilAsserted(
        () -> {
          Assert.assertEquals(
              "Missing partition column should route rows to one null-key subtask",
              1,
              subTasks.size());
          final PartitionKey partitionKey = subTasks.keySet().iterator().next();
          Assert.assertEquals(1, partitionKey.segmentNum());
          Assert.assertNull(partitionKey.segmentValue(0));
          Assert.assertTrue(
              "Null-key subtask should observe commit progress",
              subTasks.values().iterator().next().getCommitId() >= 1);
        });

    sourceInstance.commit(maxCommitIndex.get());
  }

  @Test
  public void testSubscriptionSourceOnlyReceivesConfiguredTable() throws Exception {
    final String host = EnvFactory.getEnv().getIP();
    final int port = Integer.parseInt(EnvFactory.getEnv().getPort());

    createTwoTables();

    final IoTDBSubscriptionSource source =
        new IoTDBSubscriptionSource(
            DATABASE_NAME,
            TABLE_NAME,
            null,
            null,
            host,
            port,
            SessionConfig.DEFAULT_USER,
            ENCRYPTED_DEFAULT_PASSWORD);
    createSubscriptionTopic(source, "test_table_filter_task");

    final StreamNodeConfig nodeConfig = new StreamNodeConfig();
    nodeConfig.setSnInternalAddress(host);
    nodeConfig.setSnInternalPort(10820);

    final List<String> receivedTableNames = new CopyOnWriteArrayList<>();
    final AtomicInteger receivedRows = new AtomicInteger(0);
    final CountDownLatch dataReceivedLatch = new CountDownLatch(EXPECTED_ROW_COUNT);

    sourceInstance =
        new IoTDBSubscriptionSourceInstance(source, "test_table_filter_task", nodeConfig);

    sourceInstance.start(
        (tablet, commitIndex) -> {
          receivedTableNames.add(tablet.getTableName());
          receivedRows.addAndGet(tablet.getRowSize());
          for (int i = 0; i < tablet.getRowSize(); i++) {
            dataReceivedLatch.countDown();
          }
        });

    insertTwoTablesRows();

    Assert.assertTrue(
        "Should receive subscribed table rows", dataReceivedLatch.await(60, TimeUnit.SECONDS));
    AWAIT.untilAsserted(
        () -> Assert.assertTrue("Should receive tablets", !receivedTableNames.isEmpty()));

    Assert.assertTrue(
        "Configured table should deliver expected rows", receivedRows.get() >= EXPECTED_ROW_COUNT);
    Assert.assertTrue(
        "Subscription source should not receive rows from " + OTHER_TABLE_NAME,
        receivedTableNames.stream().allMatch(TABLE_NAME::equals));
  }

  /** Helper method to create test database and table in IoTDB using table model. */
  private void createTestTable() throws Exception {
    try (final ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      // Create database
      session.executeNonQueryStatement("CREATE DATABASE " + DATABASE_NAME);
      LOGGER.info("Created database: {}", DATABASE_NAME);

      // Use database
      session.executeNonQueryStatement("USE " + DATABASE_NAME);

      // Create table with schema
      session.executeNonQueryStatement(
          String.format(
              "CREATE TABLE %s (device_id STRING TAG, temperature FLOAT FIELD, status INT32 FIELD)",
              TABLE_NAME));
      LOGGER.info("Created table: {}", TABLE_NAME);
    }
  }

  private void insertTestRows() throws Exception {
    try (final ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      session.executeNonQueryStatement("USE " + DATABASE_NAME);
      // Insert test data using SQL statements
      for (int i = 0; i < EXPECTED_ROW_COUNT; i++) {
        long timestamp = System.currentTimeMillis() + i;
        String deviceId = "device_" + (i % 10);
        float temperature = 20.0f + i * 0.1f;
        int status = i % 2;
        session.executeNonQueryStatement(
            String.format(
                "INSERT INTO %s (time, device_id, temperature, status) VALUES (%d, '%s', %f, %d)",
                TABLE_NAME, timestamp, deviceId, temperature, status));
      }

      session.executeNonQueryStatement("FLUSH");
      LOGGER.info("Inserted {} rows into {}.{}", EXPECTED_ROW_COUNT, DATABASE_NAME, TABLE_NAME);
    }
  }

  private void createBucketedTestTable() throws Exception {
    try (final ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      session.executeNonQueryStatement("CREATE DATABASE " + DATABASE_NAME);
      session.executeNonQueryStatement("USE " + DATABASE_NAME);
      session.executeNonQueryStatement(
          String.format(
              "CREATE TABLE %s (device_id STRING TAG, temperature FLOAT FIELD, status INT32 FIELD, bucket INT32 FIELD)",
              TABLE_NAME));
    }
  }

  private void insertBucketedTestRows() throws Exception {
    try (final ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      session.executeNonQueryStatement("USE " + DATABASE_NAME);
      int offset = 0;
      for (int status = 0; status < 2; status++) {
        for (int bucket = 0; bucket < 2; bucket++) {
          for (int i = 0; i < EXPECTED_ROW_COUNT / 4; i++) {
            session.executeNonQueryStatement(
                String.format(
                    "INSERT INTO %s (time, device_id, temperature, status, bucket) VALUES (%d, '%s', %f, %d, %d)",
                    TABLE_NAME,
                    System.currentTimeMillis() + offset,
                    "device_" + (offset % 10),
                    20.0f + offset * 0.1f,
                    status,
                    bucket));
            offset++;
          }
        }
      }

      session.executeNonQueryStatement("FLUSH");
    }
  }

  private void createTwoTables() throws Exception {
    try (final ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      session.executeNonQueryStatement("CREATE DATABASE " + DATABASE_NAME);
      session.executeNonQueryStatement("USE " + DATABASE_NAME);
      createTable(session, TABLE_NAME);
      createTable(session, OTHER_TABLE_NAME);
    }
  }

  private void insertTwoTablesRows() throws Exception {
    try (final ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      session.executeNonQueryStatement("USE " + DATABASE_NAME);
      for (int i = 0; i < EXPECTED_ROW_COUNT; i++) {
        insertRow(session, TABLE_NAME, i, i % 2);
        insertRow(session, OTHER_TABLE_NAME, i, 100 + i);
      }

      session.executeNonQueryStatement("FLUSH");
    }
  }

  private void createTable(final ITableSession session, final String tableName) throws Exception {
    session.executeNonQueryStatement(
        String.format(
            "CREATE TABLE %s (device_id STRING TAG, temperature FLOAT FIELD, status INT32 FIELD)",
            tableName));
  }

  private void insertRow(
      final ITableSession session, final String tableName, final int offset, final int status)
      throws Exception {
    session.executeNonQueryStatement(
        String.format(
            "INSERT INTO %s (time, device_id, temperature, status) VALUES (%d, '%s', %f, %d)",
            tableName,
            System.currentTimeMillis() + offset,
            "device_" + (offset % 10),
            20.0f + offset * 0.1f,
            status));
  }

  /**
   * Verifies that the source commit index is gated by the downstream sink flush rather than by data
   * arrival: with a real {@link CommitTracker} wired to the source and the real window engines
   * producing events, dispatching all source data must NOT advance the commit while nothing has
   * flushed; once the window destroy events are handed to the tracker (simulating a successful
   * write-back), the source commit must advance.
   */
  @Test
  public void testSourceCommitIsGatedBySinkFlush() throws Exception {
    final String host = EnvFactory.getEnv().getIP();
    final int port = Integer.parseInt(EnvFactory.getEnv().getPort());

    createTestTable();

    // No partition columns => a single sub-task, so one window engine processes every row and the
    // window boundaries (and therefore the committed index) are fully determined by the timestamps.
    final IoTDBSubscriptionSource source =
        new IoTDBSubscriptionSource(
            DATABASE_NAME,
            TABLE_NAME,
            null,
            null,
            host,
            port,
            SessionConfig.DEFAULT_USER,
            ENCRYPTED_DEFAULT_PASSWORD);
    createSubscriptionTopic(source, "test_commit_gating_task");

    final StreamNodeConfig nodeConfig = new StreamNodeConfig();
    nodeConfig.setSnInternalAddress(host);
    nodeConfig.setSnInternalPort(10820);

    // A single poll consumer keeps the source-index assignment deterministic: with several poll
    // threads, commit indices are handed out in poll order and the index <-> row mapping may change
    // between runs.
    final StreamProperties streamProperties = new StreamProperties();
    streamProperties.setSubscriptionPollThreadCount(1);

    final List<Long> committedIndices = new CopyOnWriteArrayList<>();
    final AtomicReference<IoTDBSubscriptionSourceInstance> sourceRef = new AtomicReference<>();
    final CommitTracker commitTracker =
        new CommitTracker(
            index -> {
              committedIndices.add(index);
              final IoTDBSubscriptionSourceInstance current = sourceRef.get();
              if (current != null) {
                current.commit(index);
              }
            });

    final Map<PartitionKey, StreamSubTask> subTasks = new ConcurrentHashMap<>();
    // Window ids are task-scoped and shared by all sub-tasks; a per-sub-task allocator would hand
    // the same id to different partitions and trip "window already open".
    final AtomicLong windowIdAllocator = new AtomicLong(0);
    final TabletDispatcher dispatcher =
        new ColumnPartitionedTabletDispatcher(
            source.getPartitionColumns(),
            (partitionKey, slices) ->
                subTasks
                    .computeIfAbsent(
                        partitionKey,
                        key -> createWindowSubTask(key, commitTracker, windowIdAllocator))
                    .offer(slices),
            commitTracker);

    final CountDownLatch rowsDispatchedLatch = new CountDownLatch(EXPECTED_ROW_COUNT);
    final AtomicLong maxSourceIndex = new AtomicLong(-1L);

    sourceInstance =
        new IoTDBSubscriptionSourceInstance(
            source, streamProperties, "test_commit_gating_task", nodeConfig);
    sourceRef.set(sourceInstance);

    sourceInstance.start(
        (tablet, sourceIndex) -> {
          maxSourceIndex.updateAndGet(current -> Math.max(current, sourceIndex));
          dispatcher.dispatch(tablet, sourceIndex);
          for (int row = 0; row < tablet.getRowSize(); row++) {
            rowsDispatchedLatch.countDown();
          }
        });

    // Fixed, window-aligned timestamps: 100 rows 50 ms apart over a 1000 ms tumble means each
    // window holds exactly 20 rows, so the committed indices are known in advance.
    insertFixedWindowAlignedRows();

    Assert.assertTrue(
        "All source rows should be dispatched", rowsDispatchedLatch.await(60, TimeUnit.SECONDS));
    Assert.assertEquals(
        "Every row should be dispatched as its own source index",
        (long) EXPECTED_ROW_COUNT,
        maxSourceIndex.get());

    // Nothing has flushed yet: the source commit must still be frozen even though all rows have
    // been dispatched and all windows have been accounted for.
    Assert.assertTrue(
        "Source commit must not advance before the sink flushes", committedIndices.isEmpty());

    // Simulate the write-back flush by handing every queued window event (close then destroy)
    // to the tracker. Only the destroy event (eof) may advance the commit.
    for (final StreamSubTask subTask : subTasks.values()) {
      flushWindowEvents(subTask.getWindowEngine(), commitTracker);
    }

    // Windows 0..3 close (their last source indices are 20/40/60/80) and window 4 stays open:
    // commit must advance exactly once per closed window and stop at the last closed boundary.
    final List<Long> expectedCommitted = new ArrayList<>();
    for (long index = ROWS_PER_WINDOW;
        index + ROWS_PER_WINDOW <= EXPECTED_ROW_COUNT;
        index += ROWS_PER_WINDOW) {
      expectedCommitted.add(index);
    }
    Assert.assertEquals(
        "Commit must advance exactly at every closed window boundary",
        expectedCommitted,
        committedIndices);

    sourceInstance.commit(committedIndices.get(committedIndices.size() - 1));
  }

  /** Builds a sub-task whose window engine reports window/tablet progress to the commit tracker. */
  private StreamSubTask createWindowSubTask(
      final PartitionKey partitionKey,
      final CommitTracker commitTracker,
      final AtomicLong windowIdAllocator) {
    final StreamSubTaskStateMachine stateMachine =
        new StreamSubTaskStateMachine("test-window-subtask-" + partitionKey, Runnable::run);
    final StreamSubTaskContext subTaskContext =
        new StreamSubTaskContext("test_commit_gating_task", partitionKey, stateMachine);
    subTaskContext.setCommitTracker(commitTracker);
    subTaskContext.setWindowIdAllocator(windowIdAllocator);

    final TumbleWindow window = new TumbleWindow(WINDOW_SIZE_MS, 0L);
    window.setEventInputColumns(
        Arrays.asList(TsFileConstant.TIME_COLUMN_ID, "device_id", "temperature", "status"));
    // No stream properties (watermark / lateness / max-delay all disabled: windows close purely on
    // event time) and no shared tick scheduler: the test drives the engine directly via push().
    final WindowEngine windowEngine =
        WindowEngine.create(window, subTaskContext.getDriverContext(), null, null);

    final StreamSubTask subTask =
        new StreamSubTask(
            partitionKey,
            windowEngine,
            null,
            null,
            null,
            "test_commit_gating_task",
            subTaskContext);
    // Drive the window engine directly: there is no compute/sink pipeline in this test.
    subTask.setConsumer(windowEngine::push);
    return subTask;
  }

  /**
   * Replays queued window events into the tracker the way the sink pipeline would after a flush:
   * close events live on the compute queue and destroy markers on the recompute queue, and only the
   * destroy marker (eof) may advance the source commit, so both queues must be drained.
   */
  private void flushWindowEvents(
      final WindowEngine windowEngine, final CommitTracker commitTracker) {
    // The compute queue holds the close events; the sink consumes their results, which is a no-op
    // here, so just drain them.
    while (windowEngine.hasPendingComputeEvent()) {
      windowEngine.pollComputeEvent();
    }
    IEventInfo event;
    while (windowEngine.hasPendingRecomputeEvent()) {
      event = windowEngine.pollRecomputeEvent();
      if (event.getWindowId() >= 0 && event.isEOF()) {
        commitTracker.onWindowDestroy(event.getWindowId());
      }
    }
  }

  /**
   * Inserts a fixed, window-aligned dataset: all rows share one device (so they land in a single
   * data region / topic partition) and are {@link #ROW_INTERVAL_MS} apart starting at a window
   * boundary (so every tumble holds exactly {@link #ROWS_PER_WINDOW} rows). Combined with a single
   * poll consumer this makes both the source-index ordering and the window boundaries
   * deterministic.
   */
  private void insertFixedWindowAlignedRows() throws Exception {
    try (final ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      session.executeNonQueryStatement("USE " + DATABASE_NAME);
      for (int i = 0; i < EXPECTED_ROW_COUNT; i++) {
        session.executeNonQueryStatement(
            String.format(
                "INSERT INTO %s (time, device_id, temperature, status) VALUES (%d, '%s', %f, %d)",
                TABLE_NAME,
                FIRST_ROW_TIME + i * ROW_INTERVAL_MS,
                "device_0",
                20.0f + i * 0.1f,
                i % 2));
      }
      session.executeNonQueryStatement("FLUSH");
    }
  }
}
