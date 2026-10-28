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
import org.apache.iotdb.commons.stream.IoTDBSubscriptionSource;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.it.env.StreamEnvFactory;
import org.apache.iotdb.it.env.cluster.config.MppCommonConfig;
import org.apache.iotdb.it.env.cluster.node.StreamNodeWrapper;
import org.apache.iotdb.itbase.category.StreamClusterIT;
import org.apache.iotdb.itbase.env.BaseEnv;
import org.apache.iotdb.rpc.subscription.config.TopicConstant;
import org.apache.iotdb.session.subscription.ISubscriptionTableSession;
import org.apache.iotdb.session.subscription.SubscriptionTableSessionBuilder;

import org.awaitility.Awaitility;
import org.awaitility.core.ConditionFactory;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@Category({StreamClusterIT.class})
public class StreamNodeSubscriptionRecoveryIT {

  private static final BaseEnv ENV = StreamEnvFactory.getEnv(1);
  private static final String DATABASE = "stream_recovery";
  private static final String STREAM = "stream_subscription_recovery";
  private static final String TOPIC = IoTDBSubscriptionSource.getTopicName(STREAM);
  private static final String IDLE_DATABASE = "stream_idle_subscription";
  private static final String IDLE_STREAM = "stream_idle_subscription";
  private static final String IDLE_TOPIC = IoTDBSubscriptionSource.getTopicName(IDLE_STREAM);
  private static final int BATCH_SIZE = 10;
  private static final long RECYCLE_INTERVAL_MS = 1000L;
  private static final ConditionFactory AWAIT =
      Awaitility.await()
          .pollInSameThread()
          .pollInterval(1, TimeUnit.SECONDS)
          // The crashed client's default heartbeat interval is 30s; cleanup takes at least 90s.
          .atMost(180, TimeUnit.SECONDS);

  @BeforeClass
  public static void setUp() throws Exception {
    ENV.getConfig()
        .getCommonConfig()
        .setSubscriptionEnabled(true)
        .setPipeMemoryManagementEnabled(false)
        .setIsPipeEnableMemoryCheck(false)
        .setDataRegionGroupExtensionPolicy("CUSTOM")
        .setDefaultDataRegionGroupNumPerDatabase(1)
        .setDataRegionConsensusProtocolClass(ConsensusFactory.IOT_CONSENSUS);
    // A short server default makes accidental fallback to timed redelivery observable.
    final Properties properties = new Properties();
    properties.setProperty(
        "subscription_recycle_uncommitted_event_interval_ms", Long.toString(RECYCLE_INTERVAL_MS));
    ((MppCommonConfig) ENV.getConfig().getDataNodeCommonConfig()).updateProperties(properties);
    ENV.initClusterEnvironment(1, 1, 30);
  }

  @After
  public void cleanTestResources() throws Exception {
    // Keep the shared cluster usable even if the crash test fails before restarting its SN.
    final StreamNodeWrapper streamNode = getStreamNode();
    if (!streamNode.isAlive()) {
      streamNode.start();
      ENV.ensureNodeStatus(List.of(streamNode), List.of(NodeStatus.Running));
    }
    try (Connection connection = ENV.getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.setQueryTimeout(30);
      for (final String stream : List.of(STREAM, IDLE_STREAM)) {
        statement.execute("DROP STREAM IF EXISTS " + stream);
      }
      for (final String database : List.of(DATABASE, IDLE_DATABASE)) {
        statement.execute("DROP DATABASE IF EXISTS " + database);
      }
    }
  }

  @AfterClass
  public static void tearDown() throws Exception {
    ENV.cleanClusterEnvironment();
  }

  /**
   * Crashes the only SN after a batch is committed and waits for all its subscription clients to
   * expire. The checkpoint must survive cleanup, and the restarted SN must consume rows written
   * during and after downtime without replaying the already committed batch or seeking explicitly.
   */
  @Test
  public void testRestartResumesAfterSubscriptionClientCleanup() throws Exception {
    final StreamNodeWrapper streamNode = getStreamNode();
    final long streamNodePid = streamNode.getPid();
    assertTrue("The SN process must be identifiable before simulating a crash", streamNodePid > 0);
    final ProcessHandle streamNodeProcess = ProcessHandle.of(streamNodePid).orElseThrow();

    try (Connection connection = ENV.getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE " + DATABASE);
      statement.execute("USE " + DATABASE);
      statement.execute("CREATE TABLE source_table (value INT64 FIELD)");
      statement.execute("CREATE TABLE sink_table (value INT64 FIELD)");
      statement.execute(
          "CREATE STREAM "
              + STREAM
              + " FROM source_table TUMBLE(1ms, 0)"
              + " INTO sink_table(time, value)"
              + " SELECT ${START_TIME}, max(value) FROM ${ROWS}"
              // Close and destroy the final window without needing an extra uncommitted row.
              + " WITH (subscription_poll_thread_count = 1,"
              + " max_delay_window_close = 5s, max_idle_window_retain = 5s)");
      statement.execute("START STREAM " + STREAM);
      AWAIT.untilAsserted(() -> assertSubscribed(statement));

      insertBatch(statement, 1000);
      AWAIT.untilAsserted(
          () -> {
            assertSinkRows(statement, 1000, BATCH_SIZE);
            // Sink visibility alone precedes the source ACK. Wait for every queue to drain.
            assertSubscriptionDrained(statement);
          });
      // Force termination so server-side inactivity cleanup, rather than a graceful unsubscribe,
      // removes the last consumer. StreamNodeWrapper.stopForcibly() delegates to graceful stop(),
      // so kill the identified test process directly and wait for its actual exit.
      assertTrue(streamNodeProcess.destroyForcibly());
      AWAIT.untilAsserted(() -> assertFalse(streamNode.isAlive()));
      AWAIT.untilAsserted(
          () -> {
            try (ResultSet resultSet = statement.executeQuery("SHOW SUBSCRIPTIONS ON " + TOPIC)) {
              assertFalse(
                  "The crashed SN's subscription clients must be cleaned up", resultSet.next());
            }
          });
      // Removing the old output makes a replay observable instead of hiding it behind upserts.
      statement.execute("DELETE FROM sink_table WHERE time < 2000");
      assertSinkRows(statement, 2000, 0);
      insertBatch(statement, 2000);
      streamNode.start();
      AWAIT.untilAsserted(
          () -> {
            assertSubscribed(statement);
            assertSinkRows(statement, 2000, BATCH_SIZE);
          });

      insertBatch(statement, 3000);
      AWAIT.untilAsserted(
          () -> {
            assertSinkRows(statement, 2000, 2 * BATCH_SIZE);
            assertSubscriptionDrained(statement);
          });
    }
  }

  /**
   * Leaves the SN idle with three polled but uncommitted rows in an open window for five server
   * recycle intervals. Delivery time must stay unchanged and closing the window must count each row
   * once. Re-enabling a finite Topic interval must then redeliver the next open window's message,
   * proving the same running subscription exercises the recycling path.
   */
  @Test
  public void testIdleStreamNodeDoesNotRedeliverUncommittedMessages() throws Exception {
    final ConditionFactory idleAwait = AWAIT.pollInterval(200, TimeUnit.MILLISECONDS);
    try (Connection connection = ENV.getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.setQueryTimeout(30);
      statement.execute("CREATE DATABASE " + IDLE_DATABASE);
      statement.execute("USE " + IDLE_DATABASE);
      statement.execute("CREATE TABLE source_table (value INT64 FIELD)");
      statement.execute(
          "CREATE TABLE sink_table (row_count INT64 FIELD, total_value DOUBLE FIELD)");
      statement.execute(
          "CREATE STREAM "
              + IDLE_STREAM
              + " FROM source_table TUMBLE(1s, 0)"
              + " INTO sink_table(time, row_count, total_value)"
              + " SELECT ${START_TIME}, count(value), sum(value) FROM ${ROWS}"
              // With no system-time close/destroy trigger, ACK waits for event-time progress.
              + " WITH (subscription_poll_thread_count = 1)");
      statement.execute("START STREAM " + IDLE_STREAM);
      statement.execute(
          "INSERT INTO source_table(time, value) VALUES (1000, 7), (1001, 11), (1002, 13)");

      final AtomicLong lastDeliveryTime = new AtomicLong();
      idleAwait.untilAsserted(
          () -> lastDeliveryTime.set(assertUncommittedSubscription(statement, IDLE_TOPIC)));
      Awaitility.await()
          .pollInSameThread()
          .pollInterval(200, TimeUnit.MILLISECONDS)
          .during(5 * RECYCLE_INTERVAL_MS, TimeUnit.MILLISECONDS)
          .atMost(15, TimeUnit.SECONDS)
          .untilAsserted(
              () -> {
                // LastPollTime advances only when a message is delivered, not on empty polls.
                assertEquals(
                    "An idle SN's uncommitted message must not be redelivered",
                    lastDeliveryTime.get(),
                    assertUncommittedSubscription(statement, IDLE_TOPIC));
                try (ResultSet resultSet = statement.executeQuery("SELECT * FROM sink_table")) {
                  assertFalse("The first window must remain open", resultSet.next());
                }
              });

      statement.execute("INSERT INTO source_table(time, value) VALUES (2000, 17)");
      idleAwait.untilAsserted(
          () -> {
            try (ResultSet resultSet =
                statement.executeQuery(
                    "SELECT time, row_count, total_value FROM sink_table ORDER BY time")) {
              assertTrue("Advancing event time must close the first window", resultSet.next());
              assertEquals(1000L, resultSet.getLong("time"));
              assertEquals(
                  "Each input row must be counted once", 3L, resultSet.getLong("row_count"));
              assertEquals(31.0, resultSet.getDouble("total_value"), 0.0);
              assertFalse("The second window must remain open", resultSet.next());
            }
          });

      idleAwait.untilAsserted(
          () -> lastDeliveryTime.set(assertUncommittedSubscription(statement, IDLE_TOPIC)));
      // Positive control: the finite interval must redeliver on this same idle SN and queue.
      try (ISubscriptionTableSession session =
          new SubscriptionTableSessionBuilder()
              .host(ENV.getIP())
              .port(Integer.parseInt(ENV.getPort()))
              .build()) {
        final Properties properties = new Properties();
        properties.setProperty(
            TopicConstant.RECYCLE_UNCOMMITTED_EVENT_INTERVAL_MS_KEY,
            Long.toString(RECYCLE_INTERVAL_MS));
        session.alterTopic(IDLE_TOPIC, properties);
        idleAwait.untilAsserted(
            () ->
                assertTrue(
                    "A finite recycle interval must cause redelivery without new input",
                    assertUncommittedSubscription(statement, IDLE_TOPIC) > lastDeliveryTime.get()));
      }
    }
  }

  private static StreamNodeWrapper getStreamNode() {
    final List<StreamNodeWrapper> streamNodes =
        ENV.getNodeWrapperList().stream()
            .filter(StreamNodeWrapper.class::isInstance)
            .map(StreamNodeWrapper.class::cast)
            .toList();
    assertEquals(1, streamNodes.size());
    return streamNodes.get(0);
  }

  private static void insertBatch(final Statement statement, final long startTime)
      throws Exception {
    final StringBuilder sql = new StringBuilder("INSERT INTO source_table(time, value) VALUES ");
    for (int i = 0; i < BATCH_SIZE; i++) {
      if (i > 0) {
        sql.append(',');
      }
      final long time = startTime + i;
      sql.append('(').append(time).append(',').append(time).append(')');
    }
    statement.execute(sql.toString());
  }

  private static void assertSinkRows(
      final Statement statement, final long firstTime, final int expectedRows) throws Exception {
    try (ResultSet resultSet =
        statement.executeQuery("SELECT time, value FROM sink_table ORDER BY time")) {
      for (int i = 0; i < expectedRows; i++) {
        assertTrue("Missing recovered sink row " + i, resultSet.next());
        final long expectedTime = firstTime + (i / BATCH_SIZE) * 1000 + i % BATCH_SIZE;
        assertEquals(expectedTime, resultSet.getLong("time"));
        assertEquals(expectedTime, resultSet.getLong("value"));
      }
      assertFalse("Committed rows must not be replayed", resultSet.next());
    }
  }

  private static void assertSubscribed(final Statement statement) throws Exception {
    try (ResultSet resultSet = statement.executeQuery("SHOW SUBSCRIPTIONS ON " + TOPIC)) {
      assertTrue("The Stream must have an active subscription", resultSet.next());
      assertEquals("default_group", resultSet.getString(ColumnHeaderConstant.CONSUMER_GROUP_NAME));
      assertFalse(resultSet.next());
    }
  }

  private static void assertSubscriptionDrained(final Statement statement) throws Exception {
    try (ResultSet resultSet = statement.executeQuery("SHOW SUBSCRIPTIONS DETAILS ON " + TOPIC)) {
      boolean found = false;
      while (resultSet.next()) {
        found = true;
        assertTrue(resultSet.getBoolean(ColumnHeaderConstant.SUBSCRIPTION_INITIALIZED));
        assertEquals(0, resultSet.getLong(ColumnHeaderConstant.RAW_WAL_GAP));
        assertEquals(0, resultSet.getLong(ColumnHeaderConstant.IN_FLIGHT_EVENT_COUNT));
        assertEquals(0, resultSet.getLong(ColumnHeaderConstant.PREFETCHED_EVENT_COUNT));
        assertEquals(0, resultSet.getLong(ColumnHeaderConstant.PENDING_EVENT_COUNT));
      }
      assertTrue("Subscription runtime details must be present", found);
    }
  }

  private static long assertUncommittedSubscription(final Statement statement, final String topic)
      throws Exception {
    try (ResultSet resultSet = statement.executeQuery("SHOW SUBSCRIPTIONS DETAILS ON " + topic)) {
      assertTrue("The Stream must have a subscription queue", resultSet.next());
      assertTrue(resultSet.getBoolean(ColumnHeaderConstant.SUBSCRIPTION_ACTIVE));
      assertTrue(resultSet.getBoolean(ColumnHeaderConstant.SUBSCRIPTION_INITIALIZED));
      assertTrue(
          "The SN must hold a polled message awaiting window completion",
          resultSet.getLong(ColumnHeaderConstant.IN_FLIGHT_EVENT_COUNT) > 0);
      assertEquals(0L, resultSet.getLong(ColumnHeaderConstant.RAW_WAL_GAP));
      assertEquals(0L, resultSet.getLong(ColumnHeaderConstant.PREFETCHED_EVENT_COUNT));
      assertEquals(0L, resultSet.getLong(ColumnHeaderConstant.PENDING_EVENT_COUNT));
      final long lastPollTime = resultSet.getLong(ColumnHeaderConstant.LAST_POLL_TIME);
      assertTrue("A message must have been delivered to the SN", lastPollTime > 0);
      assertFalse("Only one source region is expected", resultSet.next());
      return lastPollTime;
    }
  }
}
