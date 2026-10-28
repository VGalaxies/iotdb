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

import org.apache.iotdb.commons.schema.column.ColumnHeaderConstant;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.it.env.StreamEnvFactory;
import org.apache.iotdb.it.env.cluster.env.AbstractEnv;
import org.apache.iotdb.it.env.cluster.node.StreamNodeWrapper;
import org.apache.iotdb.itbase.category.StreamClusterIT;
import org.apache.iotdb.itbase.env.BaseEnv;

import org.awaitility.Awaitility;
import org.awaitility.core.ConditionFactory;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
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
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Integration tests for the runtime status columns of {@code information_schema.streams} (spec
 * cases META-04/05/08 of the stream-computing requirements doc). Assertions follow the spec, so
 * failures pinpoint the gaps that the implementation must close.
 *
 * <p>Timing background: the ConfigNode monitors streams every 5s and declares a heartbeat lost
 * after {@code streamHeartbeatLostThresholdMS} (default 15s), so transitions take up to ~20s; the
 * awaits below use generous timeouts.
 */
@Category({StreamClusterIT.class})
public class StreamNodeRuntimeStatusIT extends StreamNodeITBase {

  private static final ConditionFactory AWAIT =
      Awaitility.await()
          .pollInSameThread()
          .pollDelay(1, TimeUnit.SECONDS)
          .pollInterval(1, TimeUnit.SECONDS)
          .atMost(20, TimeUnit.SECONDS);

  private static final ConditionFactory LONG_AWAIT =
      Awaitility.await()
          .pollInSameThread()
          .pollDelay(2, TimeUnit.SECONDS)
          .pollInterval(2, TimeUnit.SECONDS)
          .atMost(60, TimeUnit.SECONDS);

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
  public void cleanUpTestDatabase() throws Exception {
    // A failed META-05/META-08 run may leave the StreamNode down; bring it back so cleanup works.
    try {
      StreamNodeWrapper wrapper = getStreamNodeWrapper();
      if (!wrapper.isAlive()) {
        wrapper.start();
        AWAIT.until(() -> wrapper.isAlive());
      }
    } catch (Exception ignored) {
      // best-effort recovery; the actual test failure must not be masked
    }
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
      for (String name : streamNames) {
        try {
          statement.execute("DROP STREAM " + name);
        } catch (Exception ignored) {
          // best-effort cleanup
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
   * META-04: while the stream is running, status is RUNNING, running_on points to the only
   * StreamNode, and last_heartbeat_time keeps advancing.
   */
  @Test
  public void testRunningStreamStatusRunningOnAndHeartbeat() throws Exception {
    final String streamName = "runtime_status_stream";
    try (Connection connection =
            StreamEnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      createTumbleStream(statement, streamName, "TUMBLE(size => 1h)");
      statement.execute("START STREAM " + streamName);

      AWAIT.untilAsserted(
          () -> {
            Map<String, Object> row = queryStreamRow(statement, streamName);
            assertNotNull("Stream row should be queryable after START STREAM", row);
            assertEquals(
                "status should be RUNNING after START STREAM",
                "RUNNING",
                row.get(ColumnHeaderConstant.STREAM_STATUS_TABLE_MODEL));
            String runningOn = (String) row.get(ColumnHeaderConstant.STREAM_RUNNING_ON_TABLE_MODEL);
            assertNotNull("running_on should be set while the stream is running", runningOn);
            assertFalse(
                "running_on should be non-empty while the stream is running", runningOn.isEmpty());
          });

      Map<String, Object> row = queryStreamRow(statement, streamName);
      String runningOnFirst = (String) row.get(ColumnHeaderConstant.STREAM_RUNNING_ON_TABLE_MODEL);
      assertTrue(
          "running_on should identify a single StreamNode as host:port but was: " + runningOnFirst,
          runningOnFirst.matches("^[^:/]+:\\d+$"));

      // With a single StreamNode deployed, running_on must keep pointing at it.
      Map<String, Object> rowAgain = queryStreamRow(statement, streamName);
      assertEquals(
          "running_on should keep pointing at the only StreamNode",
          runningOnFirst,
          rowAgain.get(ColumnHeaderConstant.STREAM_RUNNING_ON_TABLE_MODEL));

      // The heartbeat must advance (the ConfigNode collects heartbeats every 5s).
      long heartbeatFirst =
          asLong(
              queryStreamRow(statement, streamName)
                  .get(ColumnHeaderConstant.STREAM_LAST_HEARTBEAT_TIME_TABLE_MODEL));
      AWAIT.until(
          () ->
              asLong(
                      queryStreamRow(statement, streamName)
                          .get(ColumnHeaderConstant.STREAM_LAST_HEARTBEAT_TIME_TABLE_MODEL))
                  > heartbeatFirst);
      long heartbeatSecond =
          asLong(
              queryStreamRow(statement, streamName)
                  .get(ColumnHeaderConstant.STREAM_LAST_HEARTBEAT_TIME_TABLE_MODEL));
      assertTrue(
          "last_heartbeat_time should be recent while the stream is running",
          System.currentTimeMillis() - heartbeatSecond < 30_000L);
    }
  }

  /**
   * META-05: stopping the only StreamNode moves the stream to STOPPED (with last_down_time and a
   * spec-formatted down_reason, heartbeat frozen); restarting it brings the stream back to RUNNING
   * with last_up_time, running_on and heartbeat restored.
   */
  @Test
  public void testStreamNodeStopAndRecoverFieldTransitions() throws Exception {
    final String streamName = "runtime_recover_stream";
    StreamNodeWrapper wrapper = getStreamNodeWrapper();
    try (Connection connection =
            StreamEnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      createTumbleStream(statement, streamName, "TUMBLE(size => 1h)");
      statement.execute("START STREAM " + streamName);

      AWAIT.untilAsserted(
          () ->
              assertEquals(
                  "RUNNING",
                  queryStreamRow(statement, streamName)
                      .get(ColumnHeaderConstant.STREAM_STATUS_TABLE_MODEL)));
      String runningOnBeforeStop =
          (String)
              queryStreamRow(statement, streamName)
                  .get(ColumnHeaderConstant.STREAM_RUNNING_ON_TABLE_MODEL);

      wrapper.stop();
      try {
        LONG_AWAIT.untilAsserted(
            () -> {
              Map<String, Object> row = queryStreamRow(statement, streamName);
              assertNotNull(row);
              Object status = row.get(ColumnHeaderConstant.STREAM_STATUS_TABLE_MODEL);
              assertTrue(
                  "status must be STOPPED or UNKNOWN while the only StreamNode is down",
                  "STOPPED".equals(status) || "UNKNOWN".equals(status));
              long lastDownTime =
                  asLong(row.get(ColumnHeaderConstant.STREAM_LAST_DOWN_TIME_TABLE_MODEL));
              assertTrue(
                  "last_down_time should be recorded when the stream goes down", lastDownTime > 0);
              String downReason =
                  (String) row.get(ColumnHeaderConstant.STREAM_LAST_DOWN_REASON_TABLE_MODEL);
              assertNotNull("down_reason should be set while the stream is down", downReason);
              assertTrue(
                  "down_reason must follow the spec formats ManualBy(<user>) or"
                      + " Exception:<message> but was: "
                      + downReason,
                  downReason.matches("^(ManualBy\\(.*\\)|Exception:.+)$")
                      || downReason.contains("Heartbeat Lost"));
            });

        // While the StreamNode is down the heartbeat must not advance.
        long heartbeatDownFirst =
            asLong(
                queryStreamRow(statement, streamName)
                    .get(ColumnHeaderConstant.STREAM_LAST_HEARTBEAT_TIME_TABLE_MODEL));
        TimeUnit.SECONDS.sleep(10);
        long heartbeatDownSecond =
            asLong(
                queryStreamRow(statement, streamName)
                    .get(ColumnHeaderConstant.STREAM_LAST_HEARTBEAT_TIME_TABLE_MODEL));
        assertEquals(
            "last_heartbeat_time must not advance while the StreamNode is down",
            heartbeatDownFirst,
            heartbeatDownSecond);
      } finally {
        wrapper.start();
        AWAIT.until(() -> wrapper.isAlive());
      }

      // Wait until the StreamNode has rejoined the cluster before expecting recovery.
      LONG_AWAIT.until(StreamNodeRuntimeStatusIT::isStreamNodeRunning);

      LONG_AWAIT.untilAsserted(
          () -> {
            Map<String, Object> row = queryStreamRow(statement, streamName);
            assertNotNull(row);
            assertEquals(
                "status should return to RUNNING after the StreamNode restarts",
                "RUNNING",
                row.get(ColumnHeaderConstant.STREAM_STATUS_TABLE_MODEL));
            assertEquals(
                "running_on should point at the same (only) StreamNode after recovery",
                runningOnBeforeStop,
                row.get(ColumnHeaderConstant.STREAM_RUNNING_ON_TABLE_MODEL));
            long lastDownTime =
                asLong(row.get(ColumnHeaderConstant.STREAM_LAST_DOWN_TIME_TABLE_MODEL));
            long lastUpTime = asLong(row.get(ColumnHeaderConstant.STREAM_LAST_UP_TIME_TABLE_MODEL));
            assertTrue(
                "last_up_time should be newer than last_down_time after recovery",
                lastUpTime > lastDownTime);
            assertTrue(
                "last_heartbeat_time should resume at/after the new last_up_time",
                asLong(row.get(ColumnHeaderConstant.STREAM_LAST_HEARTBEAT_TIME_TABLE_MODEL))
                    >= lastUpTime);
          });
    }
  }

  private static StreamNodeWrapper getStreamNodeWrapper() {
    List<StreamNodeWrapper> streamNodeWrappers =
        ((AbstractEnv) StreamEnvFactory.getEnv())
            .getNodeWrapperList().stream()
                .filter(wrapper -> wrapper instanceof StreamNodeWrapper)
                .map(wrapper -> (StreamNodeWrapper) wrapper)
                .collect(Collectors.toList());
    assertEquals("There should be exactly one StreamNode wrapper.", 1, streamNodeWrappers.size());
    return streamNodeWrappers.get(0);
  }

  private static boolean isStreamNodeRunning() {
    try (Connection connection =
            StreamEnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("show streamnodes")) {
      while (rs.next()) {
        if ("Running".equals(rs.getString(ColumnHeaderConstant.STATUS))) {
          return true;
        }
      }
    } catch (SQLException e) {
      return false;
    }
    return false;
  }

  private void createTumbleStream(Statement statement, String streamName, String windowSpec)
      throws SQLException {
    statement.execute("CREATE DATABASE test");
    statement.execute("USE test");
    statement.execute(
        "CREATE TABLE "
            + SOURCE_TABLE
            + " (time TIMESTAMP TIME, device_id STRING TAG, value INT64 FIELD)");
    statement.execute(
        "CREATE TABLE "
            + SINK_TABLE
            + " (time TIMESTAMP TIME, device_id STRING TAG, value INT64 FIELD)");
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
            + " SELECT time, value FROM test."
            + SOURCE_TABLE);
  }
}
