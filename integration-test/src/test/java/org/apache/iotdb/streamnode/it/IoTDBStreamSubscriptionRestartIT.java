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

import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.commons.stream.IoTDBSubscriptionSource;
import org.apache.iotdb.commons.subscription.meta.topic.TopicMeta;
import org.apache.iotdb.confignode.rpc.thrift.TGetAllTopicInfoResp;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.it.env.StreamEnvFactory;
import org.apache.iotdb.it.env.cluster.config.MppBaseConfig;
import org.apache.iotdb.it.env.cluster.config.MppCommonConfig;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.itbase.category.StreamClusterIT;
import org.apache.iotdb.itbase.env.BaseEnv;
import org.apache.iotdb.rpc.TSStatusCode;

import org.awaitility.Awaitility;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

@Category({StreamClusterIT.class})
public class IoTDBStreamSubscriptionRestartIT {

  private static final BaseEnv ENV = StreamEnvFactory.getEnv(1);
  private static final String DATABASE = "stream_subscription_restart";
  private static final String STREAM_NAME = "subscription_restart_stream";
  private static final String TOPIC_NAME = IoTDBSubscriptionSource.getTopicName(STREAM_NAME);

  @BeforeClass
  public static void setUp() throws Exception {
    ENV.getConfig()
        .getCommonConfig()
        .setSubscriptionEnabled(true)
        .setPipeMemoryManagementEnabled(false)
        .setIsPipeEnableMemoryCheck(false)
        .setDataRegionConsensusProtocolClass(ConsensusFactory.IOT_CONSENSUS);
    // Keep CN subscription metadata available while the DN rejects subscription handshakes.
    ENV.getConfig().getDataNodeCommonConfig().setSubscriptionEnabled(false);
    ENV.initClusterEnvironment(1, 1, 30);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    ENV.cleanClusterEnvironment();
  }

  /**
   * START with DN subscription disabled must fail after assigning a Topic owner. Enabling
   * subscription and restarting the same DN must preserve that Topic, allow START with a higher
   * owner epoch, and deliver a closed source window to the sink table.
   */
  @Test
  public void testStartStreamAfterEnablingSubscriptionAndRestartingDataNode() throws Exception {
    final TopicMeta failedStartTopic;
    try (Connection connection = ENV.getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.setQueryTimeout(60);
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

      final SQLException failure =
          assertThrows(SQLException.class, () -> statement.execute("START STREAM " + STREAM_NAME));
      assertEquals(
          failure.getMessage(),
          TSStatusCode.STREAM_START_ERROR.getStatusCode(),
          failure.getErrorCode());
      assertStreamStatus(statement, "CREATED");
      failedStartTopic = getTopicMeta();
      assertTrue(
          "Failed START must already have assigned an owner epoch",
          failedStartTopic.getOwnerEpoch() >= 0);
    }

    final DataNodeWrapper dataNode = ENV.getDataNodeWrapper(0);
    ENV.shutdownDataNode(0);
    assertFalse(
        "The DN must exit before its subscription configuration is changed", dataNode.isAlive());
    ENV.getConfig().getDataNodeCommonConfig().setSubscriptionEnabled(true);
    dataNode.changeConfig(
        (MppBaseConfig) ENV.getConfig().getDataNodeConfig(),
        (MppCommonConfig) ENV.getConfig().getDataNodeCommonConfig(),
        null);
    ENV.startDataNode(0);
    ENV.ensureNodeStatus(
        Collections.singletonList(dataNode), Collections.singletonList(NodeStatus.Running));

    final TopicMeta restoredTopic = getTopicMeta();
    assertEquals(failedStartTopic.getCreationTime(), restoredTopic.getCreationTime());
    assertEquals(failedStartTopic.getOwnerEpoch(), restoredTopic.getOwnerEpoch());

    // Reconnect after the restart while retaining the Stream, Topic, CN, and StreamNode.
    try (Connection connection = ENV.getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.setQueryTimeout(60);
      statement.execute("USE " + DATABASE);
      statement.execute("START STREAM " + STREAM_NAME);
      assertStreamStatus(statement, "RUNNING");
      final TopicMeta startedTopic = getTopicMeta();
      assertEquals(failedStartTopic.getMaxOwnerEpoch() + 1, startedTopic.getOwnerEpoch());
      assertEquals(failedStartTopic.getOwnerId(), startedTopic.getOwnerId());

      // The second event closes the first 1 ms window without a wall-clock trigger.
      statement.execute("INSERT INTO source_table(time, value) VALUES (1000, 7), (1001, 11)");
      Awaitility.await()
          .pollInSameThread()
          .pollInterval(1, TimeUnit.SECONDS)
          .atMost(120, TimeUnit.SECONDS)
          .untilAsserted(
              () -> {
                assertStreamStatus(statement, "RUNNING");
                try (ResultSet resultSet =
                    statement.executeQuery("SELECT value FROM sink_table WHERE time = 1000")) {
                  assertTrue(
                      "Restarted Stream must write a closed window to the sink", resultSet.next());
                  assertEquals(7L, resultSet.getLong("value"));
                  assertFalse(resultSet.next());
                }
              });
    }
  }

  private static void assertStreamStatus(final Statement statement, final String status)
      throws SQLException {
    try (ResultSet resultSet = statement.executeQuery("SHOW STREAMS")) {
      assertTrue(resultSet.next());
      assertEquals(STREAM_NAME, resultSet.getString("stream_name"));
      assertEquals(status, resultSet.getString("status"));
      assertFalse(resultSet.next());
    }
  }

  private static TopicMeta getTopicMeta() throws Exception {
    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) ENV.getLeaderConfigNodeConnection()) {
      final TGetAllTopicInfoResp response = client.getAllTopicInfo();
      assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), response.getStatus().getCode());
      for (final ByteBuffer buffer : response.getAllTopicInfo()) {
        final TopicMeta topic = TopicMeta.deserialize(buffer);
        if (TOPIC_NAME.equals(topic.getTopicName()) && topic.visibleUnderTableModel()) {
          return topic;
        }
      }
      throw new AssertionError("Stream subscription topic is missing: " + TOPIC_NAME);
    }
  }
}
