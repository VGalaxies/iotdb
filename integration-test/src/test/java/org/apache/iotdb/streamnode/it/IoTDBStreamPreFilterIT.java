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
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.it.env.StreamEnvFactory;
import org.apache.iotdb.itbase.category.StreamClusterIT;
import org.apache.iotdb.itbase.env.BaseEnv;
import org.apache.iotdb.session.subscription.ISubscriptionTableSession;
import org.apache.iotdb.session.subscription.SubscriptionTableSessionBuilder;
import org.apache.iotdb.session.subscription.model.Topic;

import org.awaitility.Awaitility;
import org.awaitility.core.ConditionFactory;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * Verifies that a {@code CREATE STREAM ... WHERE ...} pre-filter is pushed down to the managed
 * subscription topic and removes non-matching rows before windows and aggregation run, and that
 * unsupported pre-filters are rejected at stream creation time.
 */
@Category({StreamClusterIT.class})
public class IoTDBStreamPreFilterIT {

  private static final BaseEnv ENV = StreamEnvFactory.getEnv(1);

  private static final String EQ_DB = "stream_pre_filter_eq";
  private static final String EQ_STREAM = "stream_pre_filter_eq_task";
  private static final String AGG_DB = "stream_pre_filter_agg";
  private static final String AGG_STREAM = "stream_pre_filter_agg_task";
  private static final String REJECT_DB = "stream_pre_filter_reject";
  private static final String REJECT_STREAM = "stream_pre_filter_reject_task";

  private static final ConditionFactory AWAIT =
      Awaitility.await()
          .pollInSameThread()
          .pollDelay(1, TimeUnit.SECONDS)
          .pollInterval(1, TimeUnit.SECONDS)
          .atMost(120, TimeUnit.SECONDS);

  @BeforeClass
  public static void setUp() throws Exception {
    ENV.getConfig()
        .getCommonConfig()
        .setSubscriptionEnabled(true)
        .setPipeMemoryManagementEnabled(false)
        .setDataRegionConsensusProtocolClass(ConsensusFactory.IOT_CONSENSUS);
    ENV.initClusterEnvironment(1, 1, 30);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    ENV.cleanClusterEnvironment();
  }

  @After
  public void cleanUp() throws Exception {
    try (Connection connection = ENV.getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      for (final String stream : new String[] {EQ_STREAM, AGG_STREAM, REJECT_STREAM}) {
        statement.execute("DROP STREAM IF EXISTS " + stream);
      }
      for (final String database : new String[] {EQ_DB, AGG_DB, REJECT_DB}) {
        statement.execute("DROP DATABASE IF EXISTS " + database);
      }
    }
  }

  /** A TAG pre-filter is applied before the window: a rejected row cannot win max(value). */
  @Test
  public void testPreFilterFiltersBeforeAggregation() throws Exception {
    try (Connection connection = ENV.getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      prepareDatabase(statement, EQ_DB);
      createStream(statement, EQ_STREAM, "WHERE region = 'north' AND device_id LIKE 'd_%'");
      statement.execute("START STREAM " + EQ_STREAM);
      awaitStreamRunning(statement, EQ_STREAM);
      awaitTopicTagFilter(EQ_STREAM);

      final long first = System.currentTimeMillis() + 2_000L;
      final long second = first + 1_000L;
      statement.execute(
          "INSERT INTO source_table(time, region, device_id, value) VALUES ("
              + first
              + ", 'north', 'd1', 1), ("
              + first
              + ", 'south', 'd1', 999), ("
              + first
              + ", 'north', 'x1', 500)");
      statement.execute(
          "INSERT INTO source_table(time, region, device_id, value) VALUES ("
              + second
              + ", 'north', 'd2', 2)");

      awaitSinkValue(statement, first, 1);
      awaitSinkValue(statement, second, 2);
    }
  }

  /** IN and LIKE remain part of the same pushdown expression. */
  @Test
  public void testInAndLikePreFilter() throws Exception {
    try (Connection connection = ENV.getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      prepareDatabase(statement, AGG_DB);
      createStream(
          statement, AGG_STREAM, "WHERE region IN ('north', 'west') AND device_id LIKE 'd_%'");
      statement.execute("START STREAM " + AGG_STREAM);
      awaitStreamRunning(statement, AGG_STREAM);

      final long base = System.currentTimeMillis() + 2_000L;
      statement.execute(
          "INSERT INTO source_table(time, region, device_id, value) VALUES ("
              + base
              + ", 'north', 'd1', 10), ("
              + (base + 1000)
              + ", 'west', 'd2', 20), ("
              + (base + 2000)
              + ", 'east', 'd3', 30), ("
              + (base + 3000)
              + ", 'north', 'x1', 40)");
      // A matching pusher at the end closes every preceding 1ms window, so the final sink count
      // proves that the rejected rows never entered a window.
      statement.execute(
          "INSERT INTO source_table(time, region, device_id, value) VALUES ("
              + (base + 4000)
              + ", 'north', 'd1', 50)");

      awaitSinkValue(statement, base, 10);
      awaitSinkValue(statement, base + 1000, 20);
      awaitSinkValue(statement, base + 4000, 50);
      Assert.assertEquals(
          "only rows satisfying the IN + LIKE pre-filter may reach the sink",
          3,
          countSinkRows(statement));
    }
  }

  /** A pre-filter on a FIELD column is rejected before a stream task is created. */
  @Test
  public void testNonTagColumnPreFilterRejected() throws Exception {
    try (Connection connection = ENV.getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      prepareDatabase(statement, REJECT_DB);
      final Exception exception =
          assertThrows(
              Exception.class, () -> createStream(statement, REJECT_STREAM, "WHERE value > 0"));
      assertNotNull(exception.getMessage());
      assertTrue(
          exception.getMessage(),
          exception.getMessage().contains("PreFilter column must be TAG column"));
      assertFalse(streamExists(statement, REJECT_STREAM));
    }
  }

  /** An expression unsupported by the subscription tag-filter is rejected at creation time. */
  @Test
  public void testUnsupportedPreFilterRejected() throws Exception {
    try (Connection connection = ENV.getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      prepareDatabase(statement, REJECT_DB);
      final Exception exception =
          assertThrows(
              Exception.class,
              () -> createStream(statement, REJECT_STREAM, "WHERE region = device_id"));
      assertNotNull(exception.getMessage());
      assertTrue(
          exception.getMessage(),
          exception.getMessage().contains("PreFilter clause is not supported by the tag filter"));
      assertFalse(streamExists(statement, REJECT_STREAM));
    }
  }

  private static void prepareDatabase(final Statement statement, final String database)
      throws SQLException {
    statement.execute("CREATE DATABASE " + database);
    statement.execute("USE " + database);
    statement.execute(
        "CREATE TABLE source_table (time TIMESTAMP TIME, region STRING TAG, "
            + "device_id STRING TAG, value INT64 FIELD)");
    statement.execute("CREATE TABLE sink_table (time TIMESTAMP TIME, value INT64 FIELD)");
  }

  private static void createStream(
      final Statement statement, final String streamName, final String preFilter)
      throws SQLException {
    statement.execute(
        "CREATE STREAM "
            + streamName
            + " FROM source_table "
            + preFilter
            + " TUMBLE(1ms, 0) INTO sink_table(time, value)"
            + " SELECT ${START_TIME}, max(value) FROM ${ROWS}"
            + " WITH (max_delay_window_close = 5s)");
  }

  private static void awaitStreamRunning(final Statement statement, final String streamName) {
    AWAIT.untilAsserted(
        () ->
            Assert.assertEquals(
                "stream " + streamName + " should be RUNNING before source rows are written",
                "RUNNING",
                queryStreamColumn(statement, streamName, "status")));
  }

  private static void awaitTopicTagFilter(final String streamName) throws Exception {
    final String topicName = IoTDBSubscriptionSource.getTopicName(streamName);
    try (ISubscriptionTableSession session =
        new SubscriptionTableSessionBuilder()
            .host(ENV.getIP())
            .port(Integer.parseInt(ENV.getPort()))
            .build()) {
      session.open();
      AWAIT.untilAsserted(
          () -> {
            final Optional<Topic> topic = session.getTopic(topicName);
            assertTrue("managed topic should exist: " + topicName, topic.isPresent());
            assertTrue(
                "topic should carry tag-filter: " + topic.get().getTopicAttributes(),
                topic.get().getTopicAttributes().contains("tag-filter"));
          });
    }
  }

  private static void awaitSinkValue(
      final Statement statement, final long time, final long expectedValue) {
    AWAIT.untilAsserted(
        () -> {
          try (ResultSet resultSet =
              statement.executeQuery("SELECT value FROM sink_table WHERE time = " + time)) {
            assertTrue("missing sink row at time " + time, resultSet.next());
            assertEquals(
                "unexpected value at time " + time, expectedValue, resultSet.getLong("value"));
            assertFalse("duplicate sink row at time " + time, resultSet.next());
          }
        });
  }

  private static int countSinkRows(final Statement statement) throws SQLException {
    try (ResultSet resultSet = statement.executeQuery("SELECT count(*) FROM sink_table")) {
      assertTrue(resultSet.next());
      return resultSet.getInt(1);
    }
  }

  private static String queryStreamColumn(
      final Statement statement, final String streamName, final String column) throws SQLException {
    try (ResultSet resultSet =
        statement.executeQuery(
            "SELECT "
                + column
                + " FROM information_schema.streams WHERE stream_name = '"
                + streamName
                + "'")) {
      return resultSet.next() ? resultSet.getString(column) : null;
    }
  }

  private static boolean streamExists(final Statement statement, final String streamName)
      throws SQLException {
    try (ResultSet resultSet =
        statement.executeQuery(
            "SELECT 1 FROM information_schema.streams WHERE stream_name = '" + streamName + "'")) {
      return resultSet.next();
    }
  }
}
