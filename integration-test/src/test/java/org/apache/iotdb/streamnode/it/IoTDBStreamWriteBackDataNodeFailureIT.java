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

import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.commons.schema.column.ColumnHeaderConstant;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.isession.SessionConfig;
import org.apache.iotdb.it.env.StreamEnvFactory;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.env.cluster.node.StreamNodeWrapper;
import org.apache.iotdb.itbase.category.StreamClusterIT;
import org.apache.iotdb.itbase.env.BaseEnv;

import org.awaitility.Awaitility;
import org.awaitility.core.ConditionFactory;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Verifies stream write-back against a single-replica DataNode outage.
 *
 * <p>Source and sink tables live in different databases (one DataRegion each). If they land on the
 * same DataNode, the sink region is migrated so the source DataNode stays available for JDBC and
 * subscription while the sink DataNode is killed.
 */
@Category({StreamClusterIT.class})
public class IoTDBStreamWriteBackDataNodeFailureIT {

  private static final ConditionFactory AWAIT =
      Awaitility.await()
          .pollInSameThread()
          .pollDelay(1, TimeUnit.SECONDS)
          .pollInterval(1, TimeUnit.SECONDS)
          .atMost(60, TimeUnit.SECONDS);

  private static final String SRC_DB = "src_db";
  private static final String SINK_DB = "sink_db";
  private static final String SRC_TABLE = "src";
  private static final String SINK_TABLE = "sink";
  private static final String DEVICE_ID = "d1";
  private static final String STREAM_NAME = "stream_writeback_dn_failure";
  private static final long WINDOW_SIZE_MS = 200L;
  private static final long WRITE_INTERVAL_MS = WINDOW_SIZE_MS / 2;
  private static final long WRITE_DURATION_MS = 30_000L;
  private static final long EVENT_TIME_ORIGIN_MS =
      Instant.parse("2026-01-01T00:00:00Z").toEpochMilli();
  private static final long SHORT_OUTAGE_MS = 500L;
  private static final long LONG_OUTAGE_MS = 3_000L;

  @BeforeClass
  public static void setUp() throws Exception {
    StreamEnvFactory.getEnv(1)
        .getConfig()
        .getCommonConfig()
        .setSubscriptionEnabled(true)
        .setPipeMemoryManagementEnabled(false)
        .setIsPipeEnableMemoryCheck(false)
        .setSchemaReplicationFactor(1)
        .setDataReplicationFactor(1)
        .setDefaultDataRegionGroupNumPerDatabase(1)
        .setDataRegionConsensusProtocolClass(ConsensusFactory.IOT_CONSENSUS);
    StreamEnvFactory.getEnv(1).initClusterEnvironment(1, 2, 30);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    StreamEnvFactory.getEnv().cleanClusterEnvironment();
  }

  @After
  public void cleanUp() throws Exception {
    final BaseEnv env = StreamEnvFactory.getEnv();
    try (Connection connection = env.getAvailableTableConnection();
        Statement statement = connection.createStatement()) {
      try {
        statement.execute("DROP STREAM IF EXISTS " + STREAM_NAME);
      } catch (Exception ignored) {
        // best-effort
      }
      try {
        statement.execute("DROP DATABASE IF EXISTS " + SRC_DB);
      } catch (Exception ignored) {
        // best-effort
      }
      try {
        statement.execute("DROP DATABASE IF EXISTS " + SINK_DB);
      } catch (Exception ignored) {
        // best-effort
      }
    }
  }

  @Test
  public void testShortOutageWriteBackEventuallySucceeds() throws Exception {
    runOutageScenario(true);
  }

  @Test
  public void testLongOutageDropsFailedWindowThenContinues() throws Exception {
    runOutageScenario(false);
  }

  private void runOutageScenario(final boolean shortOutage) throws Exception {
    final BaseEnv env = StreamEnvFactory.getEnv();
    final DataNodeWrapper sourceWrapper;
    final DataNodeWrapper sinkWrapper;
    final int sinkIndex;
    try (Connection bootstrap = env.getAvailableTableConnection();
        Statement statement = bootstrap.createStatement()) {
      createDatabasesAndTables(statement);
      insertSeedRow(statement, SRC_DB, SRC_TABLE, 1L);
      insertSeedRow(statement, SINK_DB, SINK_TABLE, 1L);

      final RegionLocation sourceRegion = waitForSingleDataRegion(statement, SRC_DB);
      final RegionLocation sinkRegion = waitForSingleDataRegion(statement, SINK_DB);
      ensureSourceAndSinkOnDifferentDataNodes(statement, sourceRegion, sinkRegion);

      final RegionLocation finalSource = waitForSingleDataRegion(statement, SRC_DB);
      final RegionLocation finalSink = waitForSingleDataRegion(statement, SINK_DB);
      Assert.assertNotEquals(
          "Source and sink DataRegions must be on different DataNodes",
          finalSource.dataNodeId,
          finalSink.dataNodeId);

      final int sourceIndex = dataNodeIndex(env, finalSource.dataNodeId);
      sinkIndex = dataNodeIndex(env, finalSink.dataNodeId);
      sourceWrapper = env.getDataNodeWrapper(sourceIndex);
      sinkWrapper = env.getDataNodeWrapper(sinkIndex);
    }

    createAndStartStream(env, sourceWrapper);

    final AtomicInteger insertedRows = new AtomicInteger(0);
    final AtomicReference<Throwable> writerError = new AtomicReference<>();
    final Thread writerThread =
        startContinuousWriter(env, sourceWrapper, insertedRows, writerError);

    AWAIT.untilAsserted(
        () ->
            Assert.assertTrue(
                "Write-back should succeed before the sink DataNode is killed",
                countSinkRows(env, sourceWrapper) >= 1));

    env.shutdownDataNode(sinkIndex);
    env.ensureNodeStatus(
        Collections.singletonList(sinkWrapper), Collections.singletonList(NodeStatus.Stopped));

    Thread.sleep(shortOutage ? SHORT_OUTAGE_MS : LONG_OUTAGE_MS);

    env.startDataNode(sinkIndex);
    env.ensureNodeStatus(
        Collections.singletonList(sinkWrapper), Collections.singletonList(NodeStatus.Running));

    writerThread.join();
    if (writerError.get() != null) {
      throw new AssertionError("Background source writer failed", writerError.get());
    }
    Assert.assertTrue("Background writer should insert source rows", insertedRows.get() >= 2);
    insertSourceRow(
        env,
        sourceWrapper,
        EVENT_TIME_ORIGIN_MS + insertedRows.get() * WRITE_INTERVAL_MS + WINDOW_SIZE_MS,
        0.0f);

    final int expectedWindows = expectedWindowCount(insertedRows.get());
    if (shortOutage) {
      AWAIT.untilAsserted(
          () ->
              Assert.assertEquals(
                  "Short outage should write back every closed window",
                  expectedWindows,
                  countSinkRows(env, sourceWrapper)));
    } else {
      Thread.sleep(2 * WINDOW_SIZE_MS);
      final int actual = countSinkRows(env, sourceWrapper);
      Assert.assertTrue(
          "Long outage should drop at least one closed window, actual="
              + actual
              + " expected="
              + expectedWindows,
          actual < expectedWindows);
    }
    if (shortOutage) {
      assertStreamNodeLogContains("Write failed (attempt");
    } else {
      assertStreamNodeLogContains("Write failed after");
    }
  }

  private static void createDatabasesAndTables(final Statement statement) throws SQLException {
    statement.execute("CREATE DATABASE " + SRC_DB);
    statement.execute("CREATE DATABASE " + SINK_DB);
    statement.execute("USE " + SRC_DB);
    statement.execute(
        "CREATE TABLE "
            + SRC_TABLE
            + " (time TIMESTAMP TIME, device_id STRING TAG, value FLOAT FIELD)");
    statement.execute("USE " + SINK_DB);
    statement.execute(
        "CREATE TABLE "
            + SINK_TABLE
            + " (time TIMESTAMP TIME, device_id STRING TAG, max_value FLOAT FIELD)");
  }

  private static void insertSeedRow(
      final Statement statement, final String database, final String table, final long time)
      throws SQLException {
    statement.execute("USE " + database);
    statement.execute(
        String.format(
            "INSERT INTO %s (time, device_id, %s) VALUES (%d, '%s', 1.0)",
            table, table.equals(SRC_TABLE) ? "value" : "max_value", time, DEVICE_ID));
  }

  private static void createAndStartStream(final BaseEnv env, final DataNodeWrapper sourceWrapper)
      throws Exception {
    try (Connection connection =
            env.getConnection(
                sourceWrapper,
                SessionConfig.DEFAULT_USER,
                SessionConfig.DEFAULT_PASSWORD,
                BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("USE " + SRC_DB);
      statement.execute(
          String.format(
              "CREATE STREAM %s FROM %s.%s PARTITION BY device_id TUMBLE(%dms) INTO %s.%s "
                  + "SELECT ${START_TIME}, ${1}, max(value) FROM ${ROWS}",
              STREAM_NAME, SRC_DB, SRC_TABLE, WINDOW_SIZE_MS, SINK_DB, SINK_TABLE));
      statement.execute("START STREAM " + STREAM_NAME);
      AWAIT.untilAsserted(
          () -> {
            try (ResultSet rs = statement.executeQuery("SHOW STREAMS")) {
              boolean running = false;
              while (rs.next()) {
                if (STREAM_NAME.equals(rs.getString("stream_name"))) {
                  Assert.assertEquals("RUNNING", rs.getString("status"));
                  running = true;
                  break;
                }
              }
              Assert.assertTrue("Stream " + STREAM_NAME + " should be RUNNING", running);
            }
          });
    }
  }

  private static Thread startContinuousWriter(
      final BaseEnv env,
      final DataNodeWrapper sourceWrapper,
      final AtomicInteger insertedRows,
      final AtomicReference<Throwable> writerError) {
    final Thread writerThread =
        new Thread(
            () -> {
              final long deadlineMs = System.currentTimeMillis() + WRITE_DURATION_MS;
              int seq = 0;
              try {
                while (System.currentTimeMillis() < deadlineMs) {
                  insertSourceRow(
                      env,
                      sourceWrapper,
                      EVENT_TIME_ORIGIN_MS + seq * WRITE_INTERVAL_MS,
                      10.0f + seq);
                  insertedRows.incrementAndGet();
                  seq++;
                  Thread.sleep(WRITE_INTERVAL_MS);
                }
              } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
              } catch (final Exception e) {
                writerError.set(e);
              }
            },
            "stream-writeback-source-writer");
    writerThread.setDaemon(true);
    writerThread.start();
    return writerThread;
  }

  private static void insertSourceRow(
      final BaseEnv env, final DataNodeWrapper sourceWrapper, final long time, final float value)
      throws Exception {
    try (Connection connection =
            env.getConnection(
                sourceWrapper,
                SessionConfig.DEFAULT_USER,
                SessionConfig.DEFAULT_PASSWORD,
                BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("USE " + SRC_DB);
      statement.execute(
          String.format(
              "INSERT INTO %s (time, device_id, value) VALUES (%d, '%s', %s)",
              SRC_TABLE, time, DEVICE_ID, value));
    }
  }

  private static int countSinkRows(final BaseEnv env, final DataNodeWrapper sourceWrapper)
      throws Exception {
    try (Connection connection =
            env.getConnection(
                sourceWrapper,
                SessionConfig.DEFAULT_USER,
                SessionConfig.DEFAULT_PASSWORD,
                BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("USE " + SINK_DB);
      try (ResultSet rs =
          statement.executeQuery(
              String.format(
                  "SELECT count(*) AS cnt FROM %s WHERE device_id = '%s' AND time > 1",
                  SINK_TABLE, DEVICE_ID))) {
        Assert.assertTrue(rs.next());
        return rs.getInt("cnt");
      }
    }
  }

  private static int expectedWindowCount(final int insertedRows) {
    if (insertedRows <= 0) {
      return 0;
    }
    return (int) (((insertedRows - 1L) * WRITE_INTERVAL_MS) / WINDOW_SIZE_MS) + 1;
  }

  private static RegionLocation waitForSingleDataRegion(
      final Statement statement, final String database) {
    final RegionLocation[] location = new RegionLocation[1];
    AWAIT.untilAsserted(
        () -> {
          location[0] = findSingleDataRegion(statement, database);
          Assert.assertNotNull("DataRegion for " + database + " should exist", location[0]);
        });
    return location[0];
  }

  private static RegionLocation findSingleDataRegion(
      final Statement statement, final String database) throws SQLException {
    RegionLocation found = null;
    int count = 0;
    try (ResultSet rs = statement.executeQuery("SHOW REGIONS")) {
      while (rs.next()) {
        if (!"DataRegion".equalsIgnoreCase(rs.getString(ColumnHeaderConstant.TYPE))) {
          continue;
        }
        final String regionDatabase = rs.getString(ColumnHeaderConstant.DATABASE);
        if (regionDatabase == null || !regionDatabase.contains(database)) {
          continue;
        }
        count++;
        found =
            new RegionLocation(
                rs.getInt(ColumnHeaderConstant.REGION_ID),
                rs.getInt(ColumnHeaderConstant.DATA_NODE_ID));
      }
    }
    Assert.assertEquals("Database " + database + " should have exactly 1 DataRegion", 1, count);
    return found;
  }

  private static void ensureSourceAndSinkOnDifferentDataNodes(
      final Statement statement, final RegionLocation source, final RegionLocation sink)
      throws SQLException {
    if (source.dataNodeId != sink.dataNodeId) {
      return;
    }
    final int targetDataNodeId = otherDataNodeId(statement, source.dataNodeId);
    statement.execute(
        String.format(
            "MIGRATE REGION %d FROM %d TO %d", sink.regionId, sink.dataNodeId, targetDataNodeId));
    AWAIT.untilAsserted(
        () -> {
          final RegionLocation relocated = findSingleDataRegion(statement, SINK_DB);
          Assert.assertEquals(targetDataNodeId, relocated.dataNodeId);
        });
  }

  private static int otherDataNodeId(final Statement statement, final int excludeId)
      throws SQLException {
    try (ResultSet rs = statement.executeQuery("SHOW DATANODES")) {
      while (rs.next()) {
        final int nodeId = rs.getInt(ColumnHeaderConstant.NODE_ID);
        if (nodeId != excludeId) {
          return nodeId;
        }
      }
    }
    throw new AssertionError("No other DataNode besides " + excludeId);
  }

  private static int dataNodeIndex(final BaseEnv env, final int dataNodeId) throws SQLException {
    try (Connection connection = env.getAvailableTableConnection();
        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("SHOW DATANODES")) {
      while (rs.next()) {
        if (rs.getInt(ColumnHeaderConstant.NODE_ID) != dataNodeId) {
          continue;
        }
        final int rpcPort = rs.getInt(ColumnHeaderConstant.RPC_PORT);
        for (int i = 0; i < env.getDataNodeWrapperList().size(); i++) {
          if (env.getDataNodeWrapper(i).getPort() == rpcPort) {
            return i;
          }
        }
      }
    }
    throw new AssertionError("Cannot map DataNodeId " + dataNodeId + " to wrapper index");
  }

  private static void assertStreamNodeLogContains(final String snippet) throws IOException {
    final StreamNodeWrapper wrapper =
        StreamEnvFactory.getEnv().getNodeWrapperList().stream()
            .filter(node -> node instanceof StreamNodeWrapper)
            .map(node -> (StreamNodeWrapper) node)
            .findFirst()
            .orElseThrow(() -> new AssertionError("StreamNode wrapper not found"));
    final Path logPath = Paths.get(wrapper.getLogPath());
    Assert.assertTrue("StreamNode log should exist: " + logPath, Files.exists(logPath));
    final String log = Files.readString(logPath, StandardCharsets.UTF_8);
    Assert.assertTrue(
        "StreamNode log should contain: " + snippet + "\nactual log:\n" + log,
        log.contains(snippet));
  }

  private static class RegionLocation {
    private final int regionId;
    private final int dataNodeId;

    private RegionLocation(final int regionId, final int dataNodeId) {
      this.regionId = regionId;
      this.dataNodeId = dataNodeId;
    }
  }
}
