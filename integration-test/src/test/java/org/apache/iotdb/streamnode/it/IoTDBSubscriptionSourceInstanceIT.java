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
import org.apache.iotdb.streamnode.engine.dispatcher.TabletDispatcher;
import org.apache.iotdb.streamnode.engine.source.IoTDBSubscriptionSourceInstance;
import org.apache.iotdb.streamnode.engine.task.StreamSubTask;

import org.apache.tsfile.write.record.Tablet;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  private static final int EXPECTED_ROW_COUNT = 100;

  private IoTDBSubscriptionSourceInstance sourceInstance;
  private final List<Tablet> receivedTablets = new CopyOnWriteArrayList<>();
  private final AtomicInteger receivedRowCount = new AtomicInteger(0);

  @Override
  @After
  public void tearDown() throws Exception {
    if (sourceInstance != null) {
      try {
        sourceInstance.stop();
      } catch (Exception e) {
        LOGGER.warn("Error stopping source instance", e);
      }
    }
    super.tearDown();
  }

  @Test
  public void testSubscriptionSourceReceivesData() throws Exception {
    final String host = EnvFactory.getEnv().getIP();
    final int port = Integer.parseInt(EnvFactory.getEnv().getPort());

    // Step 1: Create database and table, insert test data
    insertTestData(host, port);

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
            SessionConfig.DEFAULT_PASSWORD);

    // Step 3: Create StreamNodeConfig
    final StreamNodeConfig nodeConfig = new StreamNodeConfig();
    nodeConfig.setSnInternalAddress(host);
    nodeConfig.setSnInternalPort(10820);

    // Step 4: Create IoTDBSubscriptionSourceInstance with data consumer
    final CountDownLatch dataReceivedLatch = new CountDownLatch(EXPECTED_ROW_COUNT);
    sourceInstance =
        new IoTDBSubscriptionSourceInstance(
            source,
            "test_task",
            (tablet, commitIndex) -> {
              LOGGER.info(
                  "Received tablet with {} rows, commitIndex: {}",
                  tablet.getRowSize(),
                  commitIndex);
              receivedTablets.add(tablet);
              int rows = tablet.getRowSize();
              receivedRowCount.addAndGet(rows);
              for (int i = 0; i < rows; i++) {
                dataReceivedLatch.countDown();
              }
            },
            nodeConfig);

    // Step 5: Start the source instance
    sourceInstance.start();
    LOGGER.info("IoTDBSubscriptionSourceInstance started");

    // Step 6: Wait for data to be received
    boolean receivedAll = dataReceivedLatch.await(60, TimeUnit.SECONDS);
    Assert.assertTrue(
        "Should receive all " + EXPECTED_ROW_COUNT + " rows within timeout", receivedAll);

    // Step 7: Verify received data
    AWAIT.untilAsserted(
        () -> {
          int totalRows = receivedRowCount.get();
          LOGGER.info("Total rows received: {}", totalRows);
          Assert.assertTrue(
              "Should receive at least " + EXPECTED_ROW_COUNT + " rows, got " + totalRows,
              totalRows >= EXPECTED_ROW_COUNT);
        });

    // Step 8: Verify tablet structure
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

    // Insert test data
    insertTestData(host, port);

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
            SessionConfig.DEFAULT_PASSWORD);

    // Create config
    final StreamNodeConfig nodeConfig = new StreamNodeConfig();
    nodeConfig.setSnInternalAddress(host);
    nodeConfig.setSnInternalPort(10820);

    // Track commit indices
    final List<Long> commitIndices = new CopyOnWriteArrayList<>();
    final CountDownLatch dataReceivedLatch = new CountDownLatch(EXPECTED_ROW_COUNT);

    sourceInstance =
        new IoTDBSubscriptionSourceInstance(
            source,
            "test_commit_task",
            (tablet, commitIndex) -> {
              commitIndices.add(commitIndex);
              int rows = tablet.getRowSize();
              for (int i = 0; i < rows; i++) {
                dataReceivedLatch.countDown();
              }
            },
            nodeConfig);

    // Start and wait for data
    sourceInstance.start();
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

    insertTestData(host, port);

    final IoTDBSubscriptionSource source =
        new IoTDBSubscriptionSource(
            DATABASE_NAME,
            TABLE_NAME,
            null,
            Collections.singletonList("status"),
            host,
            port,
            SessionConfig.DEFAULT_USER,
            SessionConfig.DEFAULT_PASSWORD);

    final StreamNodeConfig nodeConfig = new StreamNodeConfig();
    nodeConfig.setSnInternalAddress(host);
    nodeConfig.setSnInternalPort(10820);

    final Map<PartitionKey, StreamSubTask> subTasks = new ConcurrentHashMap<>();
    final TabletDispatcher dispatcher =
        new ColumnPartitionedTabletDispatcher(
            source.getPartitionColumns(),
            partitionKey ->
                subTasks.computeIfAbsent(
                    partitionKey, key -> new StreamSubTask(key, new PeriodWindow(1, 0))));

    final AtomicLong firstCommitIndex = new AtomicLong(-1);
    final CountDownLatch firstTabletDispatchedLatch = new CountDownLatch(1);

    sourceInstance =
        new IoTDBSubscriptionSourceInstance(
            source,
            "test_dispatch_task",
            (tablet, commitIndex) -> {
              dispatcher.dispatch(tablet, commitIndex);
              firstCommitIndex.compareAndSet(-1, commitIndex);
              firstTabletDispatchedLatch.countDown();
            },
            nodeConfig);

    sourceInstance.start();
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

    insertBucketedTestData();

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
    nodeConfig.setSnInternalPort(10820);

    final Map<PartitionKey, StreamSubTask> subTasks = new ConcurrentHashMap<>();
    final TabletDispatcher dispatcher =
        new ColumnPartitionedTabletDispatcher(
            source.getPartitionColumns(),
            partitionKey ->
                subTasks.computeIfAbsent(
                    partitionKey, key -> new StreamSubTask(key, new PeriodWindow(1, 0))));

    final CountDownLatch dataReceivedLatch = new CountDownLatch(EXPECTED_ROW_COUNT);
    final AtomicInteger dispatchedRows = new AtomicInteger(0);
    final AtomicLong maxCommitIndex = new AtomicLong(-1);

    sourceInstance =
        new IoTDBSubscriptionSourceInstance(
            source,
            "test_multi_partition_dispatch_task",
            (tablet, commitIndex) -> {
              dispatcher.dispatch(tablet, commitIndex);
              dispatchedRows.addAndGet(tablet.getRowSize());
              maxCommitIndex.updateAndGet(current -> Math.max(current, commitIndex));
              for (int i = 0; i < tablet.getRowSize(); i++) {
                dataReceivedLatch.countDown();
              }
            },
            nodeConfig);

    sourceInstance.start();
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

    insertTestData(host, port);

    final IoTDBSubscriptionSource source =
        new IoTDBSubscriptionSource(
            DATABASE_NAME,
            TABLE_NAME,
            null,
            Collections.singletonList("missing_partition_column"),
            host,
            port,
            SessionConfig.DEFAULT_USER,
            SessionConfig.DEFAULT_PASSWORD);

    final StreamNodeConfig nodeConfig = new StreamNodeConfig();
    nodeConfig.setSnInternalAddress(host);
    nodeConfig.setSnInternalPort(10820);

    final Map<PartitionKey, StreamSubTask> subTasks = new ConcurrentHashMap<>();
    final TabletDispatcher dispatcher =
        new ColumnPartitionedTabletDispatcher(
            source.getPartitionColumns(),
            partitionKey ->
                subTasks.computeIfAbsent(
                    partitionKey, key -> new StreamSubTask(key, new PeriodWindow(1, 0))));

    final CountDownLatch dataReceivedLatch = new CountDownLatch(EXPECTED_ROW_COUNT);
    final AtomicLong maxCommitIndex = new AtomicLong(-1);

    sourceInstance =
        new IoTDBSubscriptionSourceInstance(
            source,
            "test_missing_partition_dispatch_task",
            (tablet, commitIndex) -> {
              dispatcher.dispatch(tablet, commitIndex);
              maxCommitIndex.updateAndGet(current -> Math.max(current, commitIndex));
              for (int i = 0; i < tablet.getRowSize(); i++) {
                dataReceivedLatch.countDown();
              }
            },
            nodeConfig);

    sourceInstance.start();
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

    insertTwoTablesData();

    final IoTDBSubscriptionSource source =
        new IoTDBSubscriptionSource(
            DATABASE_NAME,
            TABLE_NAME,
            null,
            null,
            host,
            port,
            SessionConfig.DEFAULT_USER,
            SessionConfig.DEFAULT_PASSWORD);

    final StreamNodeConfig nodeConfig = new StreamNodeConfig();
    nodeConfig.setSnInternalAddress(host);
    nodeConfig.setSnInternalPort(10820);

    final List<String> receivedTableNames = new CopyOnWriteArrayList<>();
    final AtomicInteger receivedRows = new AtomicInteger(0);
    final CountDownLatch dataReceivedLatch = new CountDownLatch(EXPECTED_ROW_COUNT);

    sourceInstance =
        new IoTDBSubscriptionSourceInstance(
            source,
            "test_table_filter_task",
            (tablet, commitIndex) -> {
              receivedTableNames.add(tablet.getTableName());
              receivedRows.addAndGet(tablet.getRowSize());
              for (int i = 0; i < tablet.getRowSize(); i++) {
                dataReceivedLatch.countDown();
              }
            },
            nodeConfig);

    sourceInstance.start();

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

  /**
   * Helper method to insert test data into IoTDB using table model.
   *
   * @param host IoTDB host
   * @param port IoTDB port
   */
  private void insertTestData(String host, int port) throws Exception {
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

  private void insertBucketedTestData() throws Exception {
    try (final ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      session.executeNonQueryStatement("CREATE DATABASE " + DATABASE_NAME);
      session.executeNonQueryStatement("USE " + DATABASE_NAME);
      session.executeNonQueryStatement(
          String.format(
              "CREATE TABLE %s (device_id STRING TAG, temperature FLOAT FIELD, status INT32 FIELD, bucket INT32 FIELD)",
              TABLE_NAME));

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

  private void insertTwoTablesData() throws Exception {
    try (final ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      session.executeNonQueryStatement("CREATE DATABASE " + DATABASE_NAME);
      session.executeNonQueryStatement("USE " + DATABASE_NAME);
      createTable(session, TABLE_NAME);
      createTable(session, OTHER_TABLE_NAME);

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
}
