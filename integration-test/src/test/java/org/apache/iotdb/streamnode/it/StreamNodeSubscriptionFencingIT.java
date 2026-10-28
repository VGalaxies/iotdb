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
import org.apache.iotdb.commons.stream.IoTDBSubscriptionSource;
import org.apache.iotdb.commons.subscription.meta.topic.TopicMeta;
import org.apache.iotdb.confignode.rpc.thrift.TGetAllTopicInfoResp;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.isession.SessionConfig;
import org.apache.iotdb.it.env.StreamEnvFactory;
import org.apache.iotdb.itbase.category.StreamClusterIT;
import org.apache.iotdb.itbase.env.BaseEnv;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.rpc.subscription.config.ConsumerConfig;
import org.apache.iotdb.rpc.subscription.config.ConsumerConstant;
import org.apache.iotdb.rpc.subscription.payload.poll.PollPayload;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionPollRequest;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionPollRequestType;
import org.apache.iotdb.rpc.subscription.payload.request.PipeSubscribeHandshakeReq;
import org.apache.iotdb.rpc.subscription.payload.request.PipeSubscribePollReq;
import org.apache.iotdb.rpc.subscription.payload.request.PipeSubscribeSubscribeReq;
import org.apache.iotdb.rpc.subscription.payload.response.PipeSubscribePollResp;
import org.apache.iotdb.service.rpc.thrift.TPipeSubscribeResp;
import org.apache.iotdb.session.subscription.SubscriptionTableSession;
import org.apache.iotdb.session.subscription.SubscriptionTableSessionBuilder;

import org.awaitility.Awaitility;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@Category({StreamClusterIT.class})
public class StreamNodeSubscriptionFencingIT {

  private static final BaseEnv ENV = StreamEnvFactory.getEnv(1);
  private static final String STREAM_NAME = "stream_owner_switch";
  private static final String TOPIC_NAME = IoTDBSubscriptionSource.getTopicName(STREAM_NAME);

  @BeforeClass
  public static void setUp() throws Exception {
    ENV.getConfig()
        .getCommonConfig()
        .setSubscriptionEnabled(true)
        .setPipeMemoryManagementEnabled(false)
        .setIsPipeEnableMemoryCheck(false)
        .setSubscriptionOwnerLeaseDurationMsMin(1000)
        .setDataRegionConsensusProtocolClass(ConsensusFactory.IOT_CONSENSUS);
    ENV.initClusterEnvironment(1, 1, 30);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    ENV.cleanClusterEnvironment();
  }

  /**
   * Stops a running Stream and simulates reassignment by changing its Topic's owner ID and epoch.
   * The existing subscription and Topic survive the switch: the old StreamNode's connection must
   * lose poll and subscribe access, including after reconnecting, while the new owner can
   * subscribe.
   */
  @Test
  public void testOldStreamNodeCannotSubscribeOrPollAfterSwitch() throws Exception {
    try (Connection connection = ENV.getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE stream_fencing");
      statement.execute("USE stream_fencing");
      statement.execute("CREATE TABLE source_table (value INT64 FIELD)");
      statement.execute("CREATE TABLE sink_table (value INT64 FIELD)");
      statement.execute(
          "CREATE STREAM "
              + STREAM_NAME
              + " FROM source_table TUMBLE(1ms, 0)"
              + " INTO sink_table(time, value)"
              + " SELECT ${START_TIME}, max(value) FROM ${ROWS}");
      statement.execute("START STREAM " + STREAM_NAME);

      final String oldRunningOn = getRunningOn(statement);
      final TopicMeta oldTopic = getTopicMeta();
      assertNotNull(oldTopic.getOwnerId());
      assertEquals(oldRunningOn, oldTopic.getOwnerId());
      assertTrue(oldTopic.getOwnerEpoch() >= 0);

      // Keep a real DataNode connection authenticated with the old StreamNode's credentials.
      // Its own consumer group avoids taking messages away from the actual Stream task.
      try (SubscriptionTableSession oldNodeSession = newSubscriptionSession()) {
        oldNodeSession.open();
        handshake(oldNodeSession, oldTopic, "old_stream_node");
        assertStatus(TSStatusCode.SUCCESS_STATUS, subscribe(oldNodeSession));
        insertRows(statement, 1000, 7);

        // This edition permits only one registered StreamNode. Stop its runtime and use the
        // scheduler's owner-transfer operation to simulate reassignment. DROP STREAM would delete
        // the Topic and require removing the probe subscription, invalidating the stale-poll check.
        statement.execute("STOP STREAM " + STREAM_NAME);
        Awaitility.await()
            .pollInSameThread()
            .atMost(120, TimeUnit.SECONDS)
            .untilAsserted(() -> assertStreamStopped(statement));
        final String newOwnerId = "replacement_stream_node";
        final long newOwnerEpoch = oldTopic.getOwnerEpoch() + 1;
        try (SubscriptionTableSession controlSession = newSubscriptionSession()) {
          controlSession.open();
          controlSession.alterTopicOwner(TOPIC_NAME, newOwnerId, newOwnerEpoch);
        }
        final TopicMeta newTopic = getTopicMeta();
        assertNotEquals(oldTopic.getOwnerId(), newTopic.getOwnerId());
        assertEquals(newOwnerId, newTopic.getOwnerId());
        assertEquals(newOwnerEpoch, newTopic.getOwnerEpoch());

        try (SubscriptionTableSession newNodeSession = newSubscriptionSession()) {
          newNodeSession.open();
          handshake(newNodeSession, newTopic, "new_stream_node");
          assertStatus(TSStatusCode.SUCCESS_STATUS, subscribe(newNodeSession));
        }

        insertRows(statement, 3000, 11);

        final TPipeSubscribeResp pollResponse = poll(oldNodeSession);
        assertStatus(TSStatusCode.SUBSCRIPTION_OWNER_FENCED, pollResponse);
        assertTrue(
            PipeSubscribePollResp.fromTPipeSubscribeResp(pollResponse).getResponses().isEmpty());
        assertStatus(TSStatusCode.SUBSCRIPTION_OWNER_FENCED, subscribe(oldNodeSession));

        // Reconnecting must not let the old StreamNode recover its subscription privileges.
        try (SubscriptionTableSession reconnectedOldNode = newSubscriptionSession()) {
          reconnectedOldNode.open();
          handshake(reconnectedOldNode, oldTopic, "reconnected_old_stream_node");
          assertStatus(TSStatusCode.SUBSCRIPTION_OWNER_FENCED, subscribe(reconnectedOldNode));
        }
      }
    }
  }

  private static void assertStreamStopped(final Statement statement) throws Exception {
    try (ResultSet resultSet = statement.executeQuery("SHOW STREAMS")) {
      assertTrue(resultSet.next());
      assertEquals(STREAM_NAME, resultSet.getString("stream_name"));
      assertEquals("STOPPED", resultSet.getString("status"));
    }
  }

  private static SubscriptionTableSession newSubscriptionSession() {
    return new SubscriptionTableSession(
        new SubscriptionTableSessionBuilder()
            .host(ENV.getIP())
            .port(Integer.parseInt(ENV.getPort())));
  }

  private static void handshake(
      final SubscriptionTableSession session, final TopicMeta topic, final String consumerId)
      throws Exception {
    final Map<String, String> attributes = new HashMap<>();
    attributes.put(ConsumerConstant.CONSUMER_ID_KEY, consumerId);
    attributes.put(ConsumerConstant.CONSUMER_GROUP_ID_KEY, "stream_fencing_probe");
    attributes.put(ConsumerConstant.USERNAME_KEY, SessionConfig.DEFAULT_USER);
    attributes.put(ConsumerConstant.PASSWORD_KEY, SessionConfig.DEFAULT_PASSWORD);
    attributes.put(ConsumerConstant.SQL_DIALECT_KEY, "table");
    attributes.put(ConsumerConstant.OWNER_ID_KEY, topic.getOwnerId());
    attributes.put(ConsumerConstant.OWNER_EPOCH_KEY, String.valueOf(topic.getOwnerEpoch()));
    // This manually driven connection must survive the owner-lease drain without heartbeats.
    attributes.put(ConsumerConstant.HEARTBEAT_INTERVAL_MS_KEY, "300000");
    assertStatus(
        TSStatusCode.SUCCESS_STATUS,
        session
            .getSessionConnection()
            .pipeSubscribe(
                PipeSubscribeHandshakeReq.toTPipeSubscribeReq(new ConsumerConfig(attributes))));
  }

  private static TPipeSubscribeResp subscribe(final SubscriptionTableSession session)
      throws Exception {
    return session
        .getSessionConnection()
        .pipeSubscribe(
            PipeSubscribeSubscribeReq.toTPipeSubscribeReq(Collections.singleton(TOPIC_NAME)));
  }

  private static TPipeSubscribeResp poll(final SubscriptionTableSession session) throws Exception {
    return session
        .getSessionConnection()
        .pipeSubscribe(
            PipeSubscribePollReq.toTPipeSubscribeReq(
                new SubscriptionPollRequest(
                    SubscriptionPollRequestType.POLL.getType(),
                    new PollPayload(Collections.singleton(TOPIC_NAME)),
                    1000L,
                    1024 * 1024L)));
  }

  private static TopicMeta getTopicMeta() throws Exception {
    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) ENV.getLeaderConfigNodeConnection()) {
      final TGetAllTopicInfoResp response = client.getAllTopicInfo();
      assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), response.getStatus().getCode());
      for (ByteBuffer buffer : response.getAllTopicInfo()) {
        final TopicMeta topic = TopicMeta.deserialize(buffer);
        if (TOPIC_NAME.equals(topic.getTopicName())) {
          return topic;
        }
      }
      throw new AssertionError("Stream subscription topic is missing: " + TOPIC_NAME);
    }
  }

  private static String getRunningOn(final Statement statement) throws Exception {
    try (ResultSet resultSet = statement.executeQuery("SHOW STREAMS")) {
      assertTrue(resultSet.next());
      assertEquals(STREAM_NAME, resultSet.getString("stream_name"));
      assertEquals("RUNNING", resultSet.getString("status"));
      final String runningOn = resultSet.getString("running_on");
      assertNotNull(runningOn);
      assertFalse(runningOn.isEmpty());
      assertFalse(resultSet.next());
      return runningOn;
    }
  }

  private static void insertRows(final Statement statement, final long time, final long value)
      throws Exception {
    statement.execute(
        String.format("INSERT INTO source_table(time, value) VALUES (%d, %d)", time, value));
    // A later event closes the preceding window even when no wall-clock trigger is used.
    statement.execute(
        String.format("INSERT INTO source_table(time, value) VALUES (%d, %d)", time + 1, value));
  }

  private static void assertStatus(final TSStatusCode expected, final TPipeSubscribeResp response) {
    assertEquals(
        response.getStatus().toString(), expected.getStatusCode(), response.getStatus().getCode());
  }
}
