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
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@Category({StreamClusterIT.class})
public class IoTDBRemoveStreamNodeIT {
  private static final String streamName = "stream_test";

  private static final ConditionFactory AWAIT =
      Awaitility.await()
          .pollInSameThread()
          .pollDelay(1, TimeUnit.SECONDS)
          .pollInterval(1, TimeUnit.SECONDS)
          .atMost(120, TimeUnit.SECONDS);

  @BeforeClass
  public static void setUp() throws Exception {
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

  @Test
  public void testShowClusterContainsStreamNode() throws Exception {
    ensureStreamNodeAvailable();
    try (Connection connection =
            StreamEnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      AWAIT.untilAsserted(() -> assertTrue(hasStreamNodeInShowCluster(statement)));
    }
  }

  @Test
  public void testRemoveStreamNodeWithoutTaskSuccess() throws Exception {
    ensureStreamNodeAvailable();
    final int streamNodeId = getStreamNodeId();
    final StreamNodeWrapper wrapper = getStreamNodeWrapper();

    try (Connection connection =
            StreamEnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute(String.format("REMOVE STREAMNODE %d", streamNodeId));
      AWAIT.untilAsserted(() -> assertFalse(hasStreamNodeInShowCluster(statement)));
    }
    AWAIT.untilAsserted(() -> assertFalse(wrapper.isAlive()));

    ensureStreamNodeAvailable();
    try (Connection connection =
            StreamEnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      assertTrue(hasStreamNodeInShowCluster(statement));
      AWAIT.untilAsserted(() -> assertTrue(hasStreamNodeInShowCluster(statement)));
    }
  }

  @Test
  public void testStopNeverStartedStreamSucceedsWithoutRegisteredStreamNode() throws Exception {
    ensureStreamNodeAvailable();
    final int streamNodeId = getStreamNodeId();
    final StreamNodeWrapper wrapper = getStreamNodeWrapper();
    final String databaseName = "test_stop_without_streamnode";
    final String stoppedStreamName = "stop_without_streamnode";

    try (Connection connection =
            StreamEnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE " + databaseName);
      statement.execute("USE " + databaseName);
      statement.execute(
          "CREATE TABLE source_table("
              + "time TIMESTAMP TIME, device_id STRING TAG, value INT64 FIELD)");
      statement.execute("CREATE TABLE sink_table(time TIMESTAMP TIME, value INT64 FIELD)");
      statement.execute(
          "CREATE STREAM "
              + stoppedStreamName
              + " FROM "
              + databaseName
              + ".source_table TUMBLE(size => 1h) INTO "
              + databaseName
              + ".sink_table(time, value)"
              + " SELECT ${START_TIME}, count(*) FROM ${ROWS}");

      statement.execute("REMOVE STREAMNODE " + streamNodeId);
      AWAIT.untilAsserted(() -> assertFalse(hasStreamNodeInShowCluster(statement)));
      AWAIT.untilAsserted(() -> assertFalse(wrapper.isAlive()));

      statement.execute("STOP STREAM " + stoppedStreamName);
      assertEquals("STOPPED", getStreamStatus(statement, stoppedStreamName));
    } finally {
      ensureStreamNodeAvailable();
      try (Connection connection =
              StreamEnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
          Statement statement = connection.createStatement()) {
        if (isStreamPresent(statement, stoppedStreamName)) {
          statement.execute("DROP STREAM " + stoppedStreamName);
        }
        statement.execute("DROP DATABASE IF EXISTS " + databaseName);
      }
    }
  }

  @Test
  public void testRemoveStreamNodeWithRunningTaskFail() throws Exception {
    ensureStreamNodeAvailable();
    final int streamNodeId = getStreamNodeId();
    try (Connection connection =
            StreamEnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE test");
      statement.execute("USE test");
      statement.execute(
          "CREATE TABLE electricity_meter("
              + "  time TIMESTAMP TIME,"
              + "  meter_id STRING TAG,"
              + "  total_consumption FLOAT FIELD)");
      statement.execute(
          "CREATE TABLE electricity_meter_stats("
              + "  time TIMESTAMP TIME,"
              + "  meter_id STRING TAG,"
              + "  hourly_consumption FLOAT FIELD)");
      statement.execute(
          String.format(
              "CREATE STREAM %s"
                  + "  PERIOD(10s)"
                  + "  INTO electricity_meter_stats(time, meter_id, hourly_consumption)"
                  + "  SELECT ${START_TIME}, meter_id,"
                  + "    last(total_consumption) - first(total_consumption) FROM electricity_meter"
                  + "  WHERE time >= ${START_TIME} and time <= ${END_TIME}"
                  + "  GROUP BY meter_id",
              streamName));
      AWAIT.until(() -> isStreamNodeRunning(streamNodeId));
      statement.execute(String.format("START STREAM %s", streamName));

      try {
        statement.execute("REMOVE STREAMNODE " + streamNodeId);
        fail("REMOVE STREAMNODE should fail when StreamTask exists on the only StreamNode");
      } catch (SQLException e) {
        assertTrue(
            e.getMessage(),
            e.getMessage().contains("StreamTask") || e.getMessage().contains("STREAM_TASK"));
      }

      assertTrue(hasStreamNodeInShowCluster(statement));
      statement.execute("DROP STREAM " + streamName);
      statement.execute("DROP DATABASE test");
    }
  }

  @Test
  public void testDropStreamFailsWhenStreamNodeStopped() throws Exception {
    ensureStreamNodeAvailable();
    final int streamNodeId = getStreamNodeId();
    final StreamNodeWrapper wrapper = getStreamNodeWrapper();
    try (Connection connection =
            StreamEnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE test_drop_stopped");
      statement.execute("USE test_drop_stopped");
      statement.execute(
          "CREATE TABLE source_table("
              + "  time TIMESTAMP TIME,"
              + "  device_id STRING TAG,"
              + "  value INT64 FIELD)");
      statement.execute(
          "CREATE TABLE sink_table("
              + "  time TIMESTAMP TIME,"
              + "  device_id STRING TAG,"
              + "  value INT64 FIELD)");
      AWAIT.until(() -> isStreamNodeRunning(streamNodeId));
      statement.execute(
          String.format(
              "CREATE STREAM %s"
                  + "  PERIOD(10s)"
                  + "  INTO sink_table(time, device_id, value)"
                  + "  SELECT ${START_TIME}, device_id, max(value) FROM source_table"
                  + "  WHERE time >= ${START_TIME} and time <= ${END_TIME}"
                  + "  GROUP BY device_id",
              streamName));
      statement.execute(String.format("START STREAM %s", streamName));

      wrapper.stop();
      AWAIT.untilAsserted(() -> assertFalse(isStreamNodeRunning(streamNodeId)));

      try {
        statement.execute("DROP STREAM " + streamName);
        fail("DROP STREAM should fail when the StreamTask's StreamNode is stopped");
      } catch (SQLException e) {
        assertTrue(
            e.getMessage(),
            e.getMessage().contains("Cannot drop stream")
                || e.getMessage().contains("STREAM_NODE_STOP_ERROR"));
      }
      assertTrue(isStreamPresent(statement, streamName));

      wrapper.start();
      AWAIT.until(() -> wrapper.isAlive());
      AWAIT.until(() -> isStreamNodeRunning(streamNodeId));
      statement.execute("DROP STREAM " + streamName);
      statement.execute("DROP DATABASE test_drop_stopped");
    } finally {
      if (!wrapper.isAlive()) {
        wrapper.start();
      }
      AWAIT.until(() -> isStreamNodeRunning(streamNodeId));
    }
  }

  @Test
  public void testRemoveNonExistStreamNode() throws Exception {
    ensureStreamNodeAvailable();
    try (Connection connection =
            StreamEnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      try {
        statement.execute("REMOVE STREAMNODE 99999");
        fail("REMOVE STREAMNODE should fail for non-exist node");
      } catch (SQLException e) {
        assertTrue(
            e.getMessage(),
            e.getMessage().contains("not in the cluster")
                || e.getMessage().contains("STREAM_NODE")
                || e.getMessage().contains("incorrect"));
      }
    }
  }

  private static void ensureStreamNodeAvailable() throws Exception {
    final StreamNodeWrapper wrapper = getStreamNodeWrapper();
    if (!wrapper.isAlive()) {
      wrapper.start();
    }
    AWAIT.until(IoTDBRemoveStreamNodeIT::isAnyStreamNodeRunning);
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

  private static boolean isStreamPresent(Statement statement, String streamName)
      throws SQLException {
    try (ResultSet rs = statement.executeQuery("show streams")) {
      while (rs.next()) {
        if (streamName.equals(rs.getString("stream_name"))) {
          return true;
        }
      }
    }
    return false;
  }

  private static String getStreamStatus(final Statement statement, final String streamName)
      throws SQLException {
    try (ResultSet resultSet =
        statement.executeQuery(
            "SELECT status FROM information_schema.streams WHERE stream_name = '"
                + streamName
                + "'")) {
      assertTrue("The stream should exist", resultSet.next());
      return resultSet.getString(ColumnHeaderConstant.STREAM_STATUS_TABLE_MODEL);
    }
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
