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

package org.apache.iotdb.subscription.it.local;

import org.apache.iotdb.commons.stream.IoTDBSubscriptionSource;
import org.apache.iotdb.isession.ITableSession;
import org.apache.iotdb.isession.SessionConfig;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.streamnode.conf.StreamNodeConfig;
import org.apache.iotdb.streamnode.engine.source.IoTDBSubscriptionSourceInstance;

import org.apache.tsfile.write.record.Tablet;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.iotdb.subscription.it.IoTDBSubscriptionITConstant.AWAIT;

/**
 * Integration test for IoTDBSubscriptionSourceInstance. This test verifies that the subscription
 * source can connect to an IoTDB cluster, subscribe to a table, and receive data through the
 * dataConsumer callback.
 */
@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class})
public class IoTDBSubscriptionSourceInstanceIT extends AbstractSubscriptionLocalIT {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(IoTDBSubscriptionSourceInstanceIT.class);

  private static final String DATABASE_NAME = "test_db";
  private static final String TABLE_NAME = "test_table";
  private static final int EXPECTED_ROW_COUNT = 100;

  private IoTDBSubscriptionSourceInstance sourceInstance;
  private final List<Tablet> receivedTablets = new CopyOnWriteArrayList<>();
  private final AtomicInteger receivedRowCount = new AtomicInteger(0);

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
  }

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
}
