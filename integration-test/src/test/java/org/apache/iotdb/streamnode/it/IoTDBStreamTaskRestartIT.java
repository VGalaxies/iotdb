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
import org.apache.iotdb.it.env.cluster.node.ConfigNodeWrapper;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.env.cluster.node.StreamNodeWrapper;
import org.apache.iotdb.itbase.category.StreamClusterIT;
import org.apache.iotdb.itbase.env.BaseEnv;

import org.awaitility.Awaitility;
import org.awaitility.core.ConditionFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@Category({StreamClusterIT.class})
@RunWith(Parameterized.class)
public class IoTDBStreamTaskRestartIT {

  private static final BaseEnv ENV = StreamEnvFactory.getEnv(1);
  private static final ConditionFactory AWAIT =
      Awaitility.await()
          .pollInSameThread()
          .pollDelay(1, TimeUnit.SECONDS)
          .pollInterval(1, TimeUnit.SECONDS)
          .atMost(30, TimeUnit.SECONDS);

  private static final String DATABASE = "stream_task_restart";
  private static final String STREAM_NAME = "restart_stream";
  private final String configNodeConsensusProtocol;

  public IoTDBStreamTaskRestartIT(final String configNodeConsensusProtocol) {
    this.configNodeConsensusProtocol = configNodeConsensusProtocol;
  }

  @Parameterized.Parameters(name = "{0}")
  public static Collection<Object[]> consensusProtocols() {
    return Arrays.asList(
        new Object[][] {{ConsensusFactory.SIMPLE_CONSENSUS}, {ConsensusFactory.RATIS_CONSENSUS}});
  }

  @Before
  public void setUp() throws Exception {
    ENV.getConfig()
        .getCommonConfig()
        .setConfigNodeConsensusProtocolClass(configNodeConsensusProtocol)
        .setSubscriptionEnabled(true)
        .setPipeMemoryManagementEnabled(false)
        .setIsPipeEnableMemoryCheck(false)
        .setDataRegionConsensusProtocolClass(ConsensusFactory.IOT_CONSENSUS);
    ENV.initClusterEnvironment(1, 1, 30);
  }

  @After
  public void tearDown() throws Exception {
    ENV.cleanClusterEnvironment();
  }

  @Test
  public void testStreamTaskRecoversAfterClusterRestart() throws Exception {
    final StreamNodeWrapper streamNode = getStreamNodeWrapper();
    final ConfigNodeWrapper configNode = ENV.getConfigNodeWrapper(0);
    final DataNodeWrapper dataNode = ENV.getDataNodeWrapper(0);

    try (Connection connection = ENV.getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE " + DATABASE);
      statement.execute("USE " + DATABASE);
      statement.execute("CREATE TABLE source_table (value INT64 FIELD)");
      statement.execute("CREATE TABLE sink_table (value INT64 FIELD)");
      statement.execute(
          "CREATE STREAM "
              + STREAM_NAME
              + " FROM source_table TUMBLE(1ms, 0)"
              + " INTO sink_table(time, value)"
              + " SELECT ${START_TIME}, max(value) FROM ${ROWS}");
      statement.execute("START STREAM " + STREAM_NAME);

      AWAIT.untilAsserted(() -> assertStreamStatus(statement, "RUNNING"));

      statement.execute("INSERT INTO source_table(time, value) VALUES (1000, 7), (1001, 11)");
      AWAIT.untilAsserted(
          () -> {
            try (ResultSet resultSet =
                statement.executeQuery("SELECT value FROM sink_table WHERE time = 1000")) {
              assertTrue("The closed window should be written to the sink", resultSet.next());
              assertEquals(7L, resultSet.getLong("value"));
              assertFalse(resultSet.next());
            }
          });

      streamNode.stop();
      AWAIT.untilAsserted(() -> assertStreamNodeStatus(statement, NodeStatus.Stopped));
    }

    dataNode.stop();
    configNode.stop();
    Thread.sleep(100);

    configNode.start();
    dataNode.start();
    ENV.ensureNodeStatus(
        Arrays.asList(configNode, dataNode), Arrays.asList(NodeStatus.Running, NodeStatus.Running));
    streamNode.start();
    assertTrue(streamNode.isAlive());

    AWAIT.untilAsserted(
        () -> {
          try (Connection connection = ENV.getConnection(BaseEnv.TABLE_SQL_DIALECT);
              Statement statement = connection.createStatement()) {
            assertStreamStatus(statement, "RUNNING");
          }
        });

    try (Connection connection = ENV.getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("USE " + DATABASE);
      statement.execute("INSERT INTO source_table(time, value) VALUES (2000, 13), (2001, 17)");
      AWAIT.untilAsserted(
          () -> {
            assertStreamStatus(statement, "RUNNING");
            try (ResultSet resultSet =
                statement.executeQuery("SELECT value FROM sink_table WHERE time = 2000")) {
              assertTrue("The recovered stream should process a new window", resultSet.next());
              assertEquals(13L, resultSet.getLong("value"));
              assertFalse(resultSet.next());
            }
          });
    }
  }

  @Test
  public void testStoppedStreamRemainsStoppedAfterClusterRestart() throws Exception {
    final StreamNodeWrapper streamNode = getStreamNodeWrapper();
    final ConfigNodeWrapper configNode = ENV.getConfigNodeWrapper(0);
    final DataNodeWrapper dataNode = ENV.getDataNodeWrapper(0);

    try (Connection connection = ENV.getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE " + DATABASE);
      statement.execute("USE " + DATABASE);
      statement.execute("CREATE TABLE source_table (value INT64 FIELD)");
      statement.execute("CREATE TABLE sink_table (value INT64 FIELD)");
      statement.execute(
          "CREATE STREAM "
              + STREAM_NAME
              + " FROM source_table TUMBLE(1ms, 0)"
              + " INTO sink_table(time, value)"
              + " SELECT ${START_TIME}, max(value) FROM ${ROWS}");
      statement.execute("START STREAM " + STREAM_NAME);
      AWAIT.untilAsserted(() -> assertStreamStatus(statement, "RUNNING"));
      statement.execute("STOP STREAM " + STREAM_NAME);
      AWAIT.untilAsserted(() -> assertStreamStatus(statement, "STOPPED"));

      streamNode.stop();
      AWAIT.untilAsserted(() -> assertStreamNodeStatus(statement, NodeStatus.Stopped));
    }

    dataNode.stop();
    configNode.stop();
    Thread.sleep(100);

    configNode.start();
    dataNode.start();
    ENV.ensureNodeStatus(
        Arrays.asList(configNode, dataNode), Arrays.asList(NodeStatus.Running, NodeStatus.Running));
    streamNode.start();
    assertTrue(streamNode.isAlive());

    AWAIT.untilAsserted(
        () -> {
          try (Connection connection = ENV.getConnection(BaseEnv.TABLE_SQL_DIALECT);
              Statement statement = connection.createStatement()) {
            assertStreamStatus(statement, "STOPPED");
          }
        });
  }

  private static void assertStreamStatus(final Statement statement, final String expectedStatus)
      throws SQLException {
    try (ResultSet resultSet =
        statement.executeQuery(
            "SELECT status FROM information_schema.streams WHERE stream_name = '"
                + STREAM_NAME
                + "'")) {
      assertTrue("The stream should exist", resultSet.next());
      assertEquals(expectedStatus, resultSet.getString("status"));
      assertFalse(resultSet.next());
    }
  }

  private static void assertStreamNodeStatus(
      final Statement statement, final NodeStatus expectedStatus) throws SQLException {
    try (ResultSet resultSet = statement.executeQuery("SHOW CLUSTER")) {
      while (resultSet.next()) {
        if (ColumnHeaderConstant.NODE_TYPE_STREAM_NODE.equalsIgnoreCase(
            resultSet.getString(ColumnHeaderConstant.NODE_TYPE))) {
          assertEquals(
              expectedStatus.getStatus(), resultSet.getString(ColumnHeaderConstant.STATUS));
          return;
        }
      }
    }
    throw new AssertionError("StreamNode is missing from SHOW CLUSTER");
  }

  private static StreamNodeWrapper getStreamNodeWrapper() {
    return ENV.getNodeWrapperList().stream()
        .filter(StreamNodeWrapper.class::isInstance)
        .map(StreamNodeWrapper.class::cast)
        .findFirst()
        .orElseThrow(() -> new AssertionError("StreamNode wrapper not found"));
  }
}
