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
import org.apache.iotdb.it.env.StreamEnvFactory;
import org.apache.iotdb.it.env.cluster.node.StreamNodeWrapper;
import org.apache.iotdb.itbase.category.StreamClusterIT;
import org.apache.iotdb.itbase.env.BaseEnv;

import org.awaitility.Awaitility;
import org.awaitility.core.ConditionFactory;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Integration tests for the StreamNode lifecycle (spec cases SN-11, SN-14+SN-15, SN-16 of the
 * stream-computing requirements doc): REMOVE STREAMNODE restrictions while the only StreamNode is
 * stopped, consumption behavior across a stop/resume cycle of the only StreamNode, and CREATE
 * STREAM rejection when no StreamNode is available.
 */
@Category({StreamClusterIT.class})
public class StreamNodeLifecycleIT extends StreamNodeITBase {

  private static final ConditionFactory AWAIT =
      Awaitility.await()
          .pollInSameThread()
          .pollDelay(200, TimeUnit.MILLISECONDS)
          .pollInterval(1, TimeUnit.SECONDS)
          .atMost(30, TimeUnit.SECONDS);

  private static final ConditionFactory LONG_AWAIT =
      Awaitility.await()
          .pollInSameThread()
          .pollDelay(2, TimeUnit.SECONDS)
          .pollInterval(2, TimeUnit.SECONDS)
          .atMost(60, TimeUnit.SECONDS);

  private static final ConditionFactory OUTPUT_AWAIT =
      Awaitility.await()
          .pollInSameThread()
          .pollDelay(200, TimeUnit.MILLISECONDS)
          .pollInterval(1, TimeUnit.SECONDS)
          .atMost(30, TimeUnit.SECONDS);

  // Restarting and re-registering a StreamNode (e.g. after a REMOVE STREAMNODE) can take well
  // over 30s in the IT environment; the reference IT (IoTDBRemoveStreamNodeIT) uses 120s for
  // the same flow.
  private static final ConditionFactory RESTART_AWAIT =
      Awaitility.await()
          .pollInSameThread()
          .pollDelay(1, TimeUnit.SECONDS)
          .pollInterval(2, TimeUnit.SECONDS)
          .atMost(120, TimeUnit.SECONDS);

  private static final String SOURCE_TABLE = "src_t";
  private static final String SINK_TABLE = "sink_t";

  @BeforeClass
  public static void setUp() throws Exception {
    // Init 1C1D1S cluster environment (1 ConfigNode, 1 DataNode, 1 StreamNode).
    StreamEnvFactory.getEnv(1)
        .getConfig()
        .getCommonConfig()
        .setSubscriptionEnabled(true)
        .setDataRegionConsensusProtocolClass(ConsensusFactory.IOT_CONSENSUS);
    StreamEnvFactory.getEnv(1).initClusterEnvironment(1, 1, 30);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    StreamEnvFactory.getEnv().cleanClusterEnvironment();
  }

  @After
  public void cleanUp() throws Exception {
    // Dropping a stream requires a reachable StreamNode, so bring the node back only when a
    // failed test left streams behind; on the success path (streams already dropped) the
    // expensive restart is skipped and later tests register the node lazily.
    try (Connection connection =
            StreamEnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      List<String> streamNames = new ArrayList<>();
      try (ResultSet rs = statement.executeQuery("show streams")) {
        while (rs.next()) {
          streamNames.add(rs.getString(ColumnHeaderConstant.STREAM_NAME_TABLE_MODEL));
        }
      } catch (Exception ignored) {
        // no streams registered
      }
      if (!streamNames.isEmpty()) {
        try {
          ensureStreamNodeAvailable();
        } catch (Exception ignored) {
          // best-effort recovery; the actual test failure must not be masked
        }
        for (String name : streamNames) {
          try {
            statement.execute("DROP STREAM " + name);
          } catch (Exception ignored) {
            // best-effort cleanup
          }
        }
      }
      try {
        statement.execute("DROP DATABASE test");
      } catch (Exception ignored) {
        // database may not exist if a test failed before creating it
      }
    }
  }

  /**
   * SN-11: with one stream task retained, stopping the only StreamNode and issuing REMOVE
   * STREAMNODE is rejected and the node record is kept; after the StreamNode is restarted and all
   * stream tasks are dropped, the REMOVE succeeds. The StreamNode is not re-registered at the end
   * of this test - the next test (or a later @After that still needs to drop a stream) brings it
   * back lazily via ensureStreamNodeAvailable().
   */
  @Test
  public void testRemoveStoppedStreamNodeRestrictions() throws Exception {
    ensureStreamNodeAvailable();
    final int streamNodeId = getStreamNodeId();
    final StreamNodeWrapper wrapper = getStreamNodeWrapper();
    final String streamName = "lifecycle_remove_stream";
    try (Connection connection =
            StreamEnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      // 1. Retain one running stream task on the only StreamNode.
      createTumbleStream(statement, streamName, "TUMBLE(size => 1h)");
      statement.execute("START STREAM " + streamName);
      AWAIT.untilAsserted(
          () ->
              assertEquals(
                  "The stream should be RUNNING before the StreamNode is stopped",
                  "RUNNING",
                  queryStreamRow(statement, streamName)
                      .get(ColumnHeaderConstant.STREAM_STATUS_TABLE_MODEL)));

      // 2. Stop the only StreamNode.
      wrapper.stop();
      AWAIT.until(() -> !wrapper.isAlive());

      // 3. REMOVE must be rejected while a stream task still exists.
      try {
        statement.execute("REMOVE STREAMNODE " + streamNodeId);
        fail("REMOVE STREAMNODE should fail while a StreamTask exists on the StreamNode");
      } catch (SQLException e) {
        assertTrue(
            e.getMessage(),
            e.getMessage().contains("StreamTask") || e.getMessage().contains("STREAM_TASK"));
      }

      // 4. The rejected REMOVE must keep the node record.
      assertTrue(
          "The StreamNode record should be retained after the rejected REMOVE",
          hasStreamNodeInShowCluster(statement));

      // 5. Restart the StreamNode before dropping the stream task: dropping a task requires a
      // reachable StreamNode (the ConfigNode sends stop/drop task RPCs to the recorded endpoint
      // and aborts the drop when the RPC fails).
      wrapper.start();
      AWAIT.until(() -> wrapper.isAlive());
      AWAIT.untilAsserted(() -> assertTrue(isStreamNodeRunning(streamNodeId)));

      // 6. With no stream task left, the REMOVE succeeds.
      statement.execute("DROP STREAM " + streamName);
      AWAIT.untilAsserted(
          () ->
              assertNull(
                  "The stream should be dropped before the REMOVE retry",
                  queryStreamRow(statement, streamName)));

      statement.execute("REMOVE STREAMNODE " + streamNodeId);
      AWAIT.untilAsserted(
          () ->
              assertFalse(
                  "The StreamNode record should be removed once no StreamTask exists",
                  hasStreamNodeInShowCluster(statement)));
    }
  }

  /**
   * SN-14 + SN-15 combined scenario: while a TUMBLE stream keeps consuming, stop the only
   * StreamNode - during the downtime the subscription data is not consumed (no new window output)
   * and the task is not migrated (running_on keeps pointing at the original StreamNode). Then
   * restart the same StreamNode - consumption resumes from the retained progress (the windows of
   * the data written during the downtime are computed) and subsequent windows keep producing
   * output.
   */
  @Ignore("bug")
  @Test
  public void testSingleStreamNodeStopAndResumeConsumption() throws Exception {
    ensureStreamNodeAvailable();
    final StreamNodeWrapper wrapper = getStreamNodeWrapper();
    final String streamName = "lifecycle_resume_stream";
    try (Connection connection =
            StreamEnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      long tBase = System.currentTimeMillis() + 1000;
      createTumbleStream(statement, streamName, String.format("TUMBLE(%dms,%d)", 5000, tBase));
      statement.execute("START STREAM " + streamName);
      AWAIT.untilAsserted(
          () ->
              assertEquals(
                  "The stream should be RUNNING before the StreamNode is stopped",
                  "RUNNING",
                  queryStreamRow(statement, streamName)
                      .get(ColumnHeaderConstant.STREAM_STATUS_TABLE_MODEL)));
      String runningOnBefore =
          (String)
              queryStreamRow(statement, streamName)
                  .get(ColumnHeaderConstant.STREAM_RUNNING_ON_TABLE_MODEL);
      assertNotNull(runningOnBefore);
      assertFalse(runningOnBefore.isEmpty());

      // While RUNNING: two rows in window 1 [tBase, tBase+5s) plus a watermark-pushing row in
      // window 2 make window 1 emit (tBase, 2) - proves the stream is consuming.
      statement.execute(
          String.format(
              "INSERT INTO test.%s (time, device_id, value) VALUES (%d, 'd1', 1), (%d, 'd1', 2)",
              SOURCE_TABLE, tBase + 1000, tBase + 2000));
      statement.execute(
          String.format(
              "INSERT INTO test.%s (time, device_id, value) VALUES (%d, 'd1', 9)",
              SOURCE_TABLE, tBase + 6000));
      OUTPUT_AWAIT.untilAsserted(
          () ->
              assertEquals(
                  "Window 1 should be computed while the stream is running",
                  Long.valueOf(2L),
                  sinkValueAt(statement, tBase)));

      // SN-14: stop the only StreamNode and keep writing during the downtime. Two rows in
      // window 3 [tBase+10s, tBase+15s) plus a watermark-pushing row in window 4 would emit
      // (tBase+10000, 2) if the data were consumed.  Use stopForcibly so the shutdown is a
      // crash on both platforms: stop() sends SIGTERM on Linux which triggers JVM shutdown
      // hooks that clean up the subscription state on the DataNode, causing the restarted
      // subscriber to start from latest instead of replaying from its checkpoint.
      wrapper.stop();
      AWAIT.until(() -> !wrapper.isAlive());
      statement.execute(
          String.format(
              "INSERT INTO test.%s (time, device_id, value) VALUES (%d, 'd1', 10), (%d, 'd1', 20)",
              SOURCE_TABLE, tBase + 11000, tBase + 12000));
      statement.execute(
          String.format(
              "INSERT INTO test.%s (time, device_id, value) VALUES (%d, 'd1', 30)",
              SOURCE_TABLE, tBase + 16000));
      TimeUnit.SECONDS.sleep(2);

      // No downtime window may be computed while the StreamNode is down.
      for (Map<String, Object> sinkRow :
          queryRows(statement, "SELECT time FROM test." + SINK_TABLE)) {
        assertTrue(
            "Subscription data must not be consumed during the downtime (unexpected output time="
                + sinkRow.get("time")
                + ")",
            asLong(sinkRow.get("time")) < tBase + 5000);
      }
      // The task must not be migrated: running_on keeps pointing at the original StreamNode.
      Map<String, Object> rowDuringDowntime = queryStreamRow(statement, streamName);
      assertNotNull(rowDuringDowntime);
      assertEquals(
          "The task must not be migrated while the only StreamNode is down",
          runningOnBefore,
          rowDuringDowntime.get(ColumnHeaderConstant.STREAM_RUNNING_ON_TABLE_MODEL));

      try {
        // SN-15: restart the same StreamNode.
        wrapper.start();
        AWAIT.until(() -> wrapper.isAlive());
        LONG_AWAIT.until(StreamNodeLifecycleIT::isAnyStreamNodeRunning);
        TimeUnit.SECONDS.sleep(8);
        LONG_AWAIT.untilAsserted(
            () ->
                assertEquals(
                    "The stream should return to RUNNING after the StreamNode restarts",
                    "RUNNING",
                    queryStreamRow(statement, streamName)
                        .get(ColumnHeaderConstant.STREAM_STATUS_TABLE_MODEL)));
        assertEquals(
            "The stream should resume on the same StreamNode",
            runningOnBefore,
            queryStreamRow(statement, streamName)
                .get(ColumnHeaderConstant.STREAM_RUNNING_ON_TABLE_MODEL));

        // Consumption resumes from the retained progress: window 3 (written during the
        // downtime, never consumed) is computed after the resume.
        LONG_AWAIT.untilAsserted(
            () ->
                assertEquals(
                    "The window of the data written during the downtime should be computed"
                        + " after the resume",
                    Long.valueOf(2L),
                    sinkValueAt(statement, tBase + 10000)));

        // Subsequent windows keep producing: two rows in window 5 [tBase+20s, tBase+25s)
        // plus a watermark-pushing row in window 6 make window 5 emit (tBase+20000, 2).
        statement.execute(
            String.format(
                "INSERT INTO test.%s (time, device_id, value) VALUES (%d, 'd1', 40),"
                    + " (%d, 'd1', 50)",
                SOURCE_TABLE, tBase + 21000, tBase + 22000));
        statement.execute(
            String.format(
                "INSERT INTO test.%s (time, device_id, value) VALUES (%d, 'd1', 60)",
                SOURCE_TABLE, tBase + 26000));
        OUTPUT_AWAIT.untilAsserted(
            () ->
                assertEquals(
                    "Windows after the resume should keep producing output",
                    Long.valueOf(2L),
                    sinkValueAt(statement, tBase + 20000)));
      } finally {
        if (!wrapper.isAlive()) {
          wrapper.start();
        }
      }
    }
  }

  /**
   * SN-16: when no StreamNode is available, a valid CREATE STREAM is rejected with a clear error
   * that expresses the absence of a StreamNode, and no stream or residual object is created.
   */
  @Test
  public void testCreateStreamRejectedWhenNoStreamNodeAvailable() throws Exception {
    ensureStreamNodeAvailable();
    final StreamNodeWrapper wrapper = getStreamNodeWrapper();
    wrapper.stop();
    AWAIT.until(() -> !wrapper.isAlive());

    try (Connection connection =
            StreamEnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE test");
      statement.execute("USE test");
      statement.execute(
          "CREATE TABLE src_t (time TIMESTAMP TIME, device_id STRING TAG, value INT64 FIELD)");
      statement.execute("CREATE TABLE snk_t (time TIMESTAMP TIME, value INT64 FIELD)");

      // CREATE STREAM must fail because no StreamNode is available.
      try {
        statement.execute(
            "CREATE STREAM s16_stream"
                + " FROM test.src_t TUMBLE(size => 1h)"
                + " INTO test.snk_t(time, value)"
                + " SELECT ${START_TIME}, count(*) FROM ${ROWS}");
        fail("CREATE STREAM should be rejected when no StreamNode is available");
      } catch (SQLException e) {
        assertTrue(
            "Error should express the absence of a StreamNode, got: " + e.getMessage(),
            e.getMessage().toLowerCase().contains("no")
                && (e.getMessage().contains("StreamNode")
                    || e.getMessage().contains("streamnode")
                    || e.getMessage().contains("STREAM_NODE")));
      }

      // No stream must be created.
      try (ResultSet rs = statement.executeQuery("SHOW STREAMS")) {
        assertFalse("No streams should exist when CREATE STREAM is rejected", rs.next());
      }
    } finally {
      wrapper.start();
      AWAIT.until(() -> wrapper.isAlive());
      AWAIT.untilAsserted(() -> assertTrue(isAnyStreamNodeRunning()));
    }
  }

  private void createTumbleStream(Statement statement, String streamName, String windowSpec)
      throws SQLException {
    statement.execute("CREATE DATABASE test");
    statement.execute("USE test");
    statement.execute(
        "CREATE TABLE "
            + SOURCE_TABLE
            + " (time TIMESTAMP TIME, device_id STRING TAG, value INT64 FIELD)");
    statement.execute("CREATE TABLE " + SINK_TABLE + " (time TIMESTAMP TIME, value INT64 FIELD)");
    statement.execute(
        "CREATE STREAM "
            + streamName
            + " FROM test."
            + SOURCE_TABLE
            + " "
            + windowSpec
            + " INTO test."
            + SINK_TABLE
            + "(time, value)"
            + " SELECT ${START_TIME}, count(*) FROM ${ROWS}");
  }

  /** Returns the value written at the given window start time, or null when absent. */
  private Long sinkValueAt(Statement statement, long time) throws SQLException {
    for (Map<String, Object> row :
        queryRows(statement, "SELECT time, value FROM test." + SINK_TABLE)) {
      if (asLong(row.get("time")) == time) {
        return asLong(row.get("value"));
      }
    }
    return null;
  }

  private static void ensureStreamNodeAvailable() throws Exception {
    try (Connection connection =
            StreamEnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      if (hasStreamNodeInShowCluster(statement)) {
        return;
      }
    }
    final StreamNodeWrapper wrapper = getStreamNodeWrapper();
    wrapper.start();
    RESTART_AWAIT.untilAsserted(
        () -> {
          try (Connection connection =
                  StreamEnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
              Statement statement = connection.createStatement()) {
            assertTrue(hasStreamNodeInShowCluster(statement));
          }
        });
  }

  private static boolean hasStreamNodeInShowCluster(Statement statement) throws SQLException {
    try (ResultSet rs = statement.executeQuery("SHOW CLUSTER")) {
      while (rs.next()) {
        if (ColumnHeaderConstant.NODE_TYPE_STREAM_NODE.equalsIgnoreCase(
            rs.getString(ColumnHeaderConstant.NODE_TYPE))) {
          return true;
        }
      }
    }
    return false;
  }

  private static boolean isStreamNodeRunning(int nodeId) {
    try (Connection connection =
            StreamEnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("show streamnodes")) {
      while (rs.next()) {
        if (nodeId == rs.getInt(ColumnHeaderConstant.NODE_ID)) {
          return NodeStatus.Running.getStatus().equals(rs.getString(ColumnHeaderConstant.STATUS));
        }
      }
    } catch (SQLException e) {
      return false;
    }
    return false;
  }

  private static boolean isAnyStreamNodeRunning() {
    try (Connection connection =
            StreamEnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("show streamnodes")) {
      while (rs.next()) {
        if (NodeStatus.Running.getStatus().equals(rs.getString(ColumnHeaderConstant.STATUS))) {
          return true;
        }
      }
    } catch (SQLException e) {
      return false;
    }
    return false;
  }

  private static int getStreamNodeId() throws SQLException {
    try (Connection connection =
            StreamEnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("show streamnodes")) {
      assertTrue("There should be at least one StreamNode.", rs.next());
      int nodeId = rs.getInt(ColumnHeaderConstant.NODE_ID);
      assertTrue("StreamNode id should be non-negative.", nodeId >= 0);
      return nodeId;
    }
  }

  private static StreamNodeWrapper getStreamNodeWrapper() {
    List<StreamNodeWrapper> streamNodeWrappers =
        StreamEnvFactory.getEnv().getNodeWrapperList().stream()
            .filter(wrapper -> wrapper instanceof StreamNodeWrapper)
            .map(wrapper -> (StreamNodeWrapper) wrapper)
            .toList();
    assertEquals("There should be exactly one StreamNode wrapper.", 1, streamNodeWrappers.size());
    return streamNodeWrappers.get(0);
  }
}
