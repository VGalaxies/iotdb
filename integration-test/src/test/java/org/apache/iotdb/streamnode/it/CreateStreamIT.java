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
import org.apache.iotdb.rpc.subscription.config.ConsumerConstant;
import org.apache.iotdb.session.subscription.ISubscriptionTableSession;
import org.apache.iotdb.session.subscription.SubscriptionTableSessionBuilder;
import org.apache.iotdb.session.subscription.consumer.ISubscriptionTablePullConsumer;
import org.apache.iotdb.session.subscription.consumer.table.SubscriptionTablePullConsumerBuilder;
import org.apache.iotdb.session.subscription.payload.SubscriptionMessage;

import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.awaitility.Awaitility;
import org.awaitility.core.ConditionFactory;
import org.junit.*;
import org.junit.experimental.categories.Category;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Integration test covering the CREATE STREAM functional cases SRC-01 … SRC-33 from the stream
 * computation spec. Runs against a 1C1D1S cluster (1 ConfigNode, 1 DataNode, 1 StreamNode).
 */
@Category({StreamClusterIT.class})
public class CreateStreamIT {

  private static final String DB = "test";

  private static final ConditionFactory AWAIT =
      Awaitility.await()
          .pollInSameThread()
          .pollDelay(1, TimeUnit.SECONDS)
          .pollInterval(1, TimeUnit.SECONDS)
          .atMost(10, TimeUnit.SECONDS);

  private static BaseEnv env() {
    return StreamEnvFactory.getEnv();
  }

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

  @After
  public void cleanUpTestDatabase() throws Exception {
    try (Connection connection = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      final List<String> streamNames = new ArrayList<>();
      try (ResultSet rs = statement.executeQuery("show streams")) {
        while (rs.next()) {
          streamNames.add(rs.getString("stream_name"));
        }
      } catch (Exception ignored) {
        // no streams registered
      }
      for (final String name : streamNames) {
        try {
          statement.execute("DROP STREAM " + name);
        } catch (Exception ignored) {
          // best-effort cleanup
        }
      }
      for (final String db : new String[] {DB, "stream_selftest"}) {
        try {
          statement.execute("DROP DATABASE " + db);
        } catch (Exception ignored) {
          // database may not exist if a test failed before creating it
        }
      }
    }
    // tree-model source databases created by createTreeToTableSourceView (SRC-08/09/10)
    try (Connection treeConn = env().getConnection(BaseEnv.TREE_SQL_DIALECT);
        Statement treeStmt = treeConn.createStatement()) {
      for (final String db : new String[] {"root.srctree08", "root.srctree09", "root.srctree10"}) {
        try {
          treeStmt.execute("DROP DATABASE " + db);
        } catch (Exception ignored) {
          // database may not exist if the view-source case did not run
        }
      }
    }
  }

  private void createDatabase(final Statement statement) throws SQLException {
    statement.execute("CREATE DATABASE " + DB);
    statement.execute("USE " + DB);
  }

  private void assertStreamExists(final Statement statement, final String streamName)
      throws SQLException {
    try (ResultSet rs =
        statement.executeQuery(
            "SELECT stream_name FROM information_schema.streams WHERE stream_name = '"
                + streamName
                + "'")) {
      assertTrue("Stream should exist: " + streamName, rs.next());
    }
  }

  private void assertStreamAbsent(final Statement statement, final String streamName)
      throws SQLException {
    try (ResultSet rs =
        statement.executeQuery(
            "SELECT stream_name FROM information_schema.streams WHERE stream_name = '"
                + streamName
                + "'")) {
      assertFalse("Stream should not be created: " + streamName, rs.next());
    }
  }

  private String getStreamColumn(
      final Statement statement, final String streamName, final String column) throws SQLException {
    try (ResultSet rs =
        statement.executeQuery(
            "SELECT "
                + column
                + " FROM information_schema.streams WHERE stream_name = '"
                + streamName
                + "'")) {
      return rs.next() ? rs.getString(column) : null;
    }
  }

  /**
   * Waits until {@code information_schema.streams} reports the stream as RUNNING, i.e. until the
   * subscription of the source is bound. START STREAM returns once the StreamNode has accepted the
   * task, and the subscription only delivers rows written after it is bound: a source row written
   * before that point never enters the subscription and therefore never enters any window.
   */
  private void awaitStreamRunning(final Statement statement, final String streamName)
      throws SQLException {
    AWAIT.untilAsserted(
        () -> {
          final String status = getStreamColumn(statement, streamName, "status");
          Assert.assertEquals(
              "stream "
                  + streamName
                  + " should be RUNNING before source rows are written; actual status="
                  + status,
              "RUNNING",
              status);
        });
  }

  private String getStreamProperties(final Statement statement, final String streamName)
      throws SQLException {
    try (ResultSet rs = statement.executeQuery("SHOW STREAMS")) {
      while (rs.next()) {
        if (streamName.equals(rs.getString("stream_name"))) {
          return rs.getString("properties");
        }
      }
    }
    return null;
  }

  /**
   * Dumps every column of every row of {@code tableName} as printable text. {@code SELECT *}
   * already returns all rows and all columns; the column labels come from the result metadata so no
   * column has to be named explicitly. Used to surface the actual table content in assertion
   * messages, instead of only reporting that an expected value was not found.
   */
  private String dumpTable(final Statement statement, final String tableName) throws SQLException {
    final StringBuilder builder = new StringBuilder("=== ").append(tableName).append(" ===\n");
    try (ResultSet rs = statement.executeQuery("SELECT * FROM " + tableName)) {
      final ResultSetMetaData metaData = rs.getMetaData();
      final int columnCount = metaData.getColumnCount();
      for (int i = 1; i <= columnCount; i++) {
        builder.append(metaData.getColumnLabel(i)).append(i < columnCount ? " | " : "\n");
      }
      int rowCount = 0;
      while (rs.next()) {
        for (int i = 1; i <= columnCount; i++) {
          final Object value = rs.getObject(i);
          builder
              .append(value == null ? "NULL" : value.toString())
              .append(i < columnCount ? " | " : "\n");
        }
        rowCount++;
      }
      builder.append("rows=").append(rowCount);
    }
    return builder.toString();
  }

  /**
   * Opens an independent pull consumer (its own consumer group, manual commit) on the stream's own
   * subscription topic, i.e. it reads exactly the same subscription flow that the stream source
   * consumes.
   *
   * <p>The topic is managed by the stream and owned by the StreamNode running the task, so owner
   * fencing is enabled for it: the DataNode only accepts a subscribe or poll that presents the
   * topic's current owner-id and owner-epoch, and otherwise answers with
   * SUBSCRIPTION_OWNER_REQUIRED or SUBSCRIPTION_OWNER_FENCED. The owner identity is therefore read
   * from the topic and carried by this consumer; its own consumer group keeps it from taking
   * messages away from the stream task.
   */
  private ISubscriptionTablePullConsumer openStreamTopicConsumer(
      final ISubscriptionTableSession subscriptionSession, final String topicName)
      throws Exception {
    final String topicAttributes = awaitTopicOwnerAttributes(subscriptionSession, topicName);
    final ISubscriptionTablePullConsumer consumer =
        new SubscriptionTablePullConsumerBuilder()
            .host(env().getIP())
            .port(Integer.parseInt(env().getPort()))
            .consumerGroupId("src26_group")
            .consumerId("src26_consumer")
            .ownerId(topicAttribute(topicAttributes, ConsumerConstant.OWNER_ID_KEY))
            .ownerEpoch(
                Long.parseLong(topicAttribute(topicAttributes, ConsumerConstant.OWNER_EPOCH_KEY)))
            .autoCommit(false)
            .build();
    consumer.open();
    // The topic is created asynchronously when the stream starts; its owner may not be ready yet,
    // so subscribing is retried until it succeeds.
    AWAIT.untilAsserted(() -> consumer.subscribe(topicName));
    return consumer;
  }

  /**
   * Waits until the stream-managed topic exposes the owner identity assigned to it when the stream
   * was scheduled, and returns the topic attributes carrying it.
   */
  private String awaitTopicOwnerAttributes(
      final ISubscriptionTableSession subscriptionSession, final String topicName)
      throws Exception {
    final AtomicReference<String> attributes = new AtomicReference<>();
    AWAIT.untilAsserted(
        () -> {
          final String current =
              subscriptionSession
                  .getTopic(topicName)
                  .orElseThrow(() -> new AssertionError("Topic should exist: " + topicName))
                  .getTopicAttributes();
          assertNotNull(
              "Topic should carry the owner id, actual=" + current,
              topicAttribute(current, ConsumerConstant.OWNER_ID_KEY));
          assertNotNull(
              "Topic should carry the owner epoch, actual=" + current,
              topicAttribute(current, ConsumerConstant.OWNER_EPOCH_KEY));
          attributes.set(current);
        });
    return attributes.get();
  }

  /**
   * Extracts the value of {@code key} from the topic attributes rendered as {@code {k=v, k=v}}. The
   * negative lookbehind keeps a longer key ending with {@code key} (e.g. {@code max-owner-epoch})
   * from matching.
   */
  private static String topicAttribute(final String attributes, final String key) {
    final Matcher matcher =
        Pattern.compile("(?<![\\w-])" + Pattern.quote(key) + "=([^,}]+)").matcher(attributes);
    return matcher.find() ? matcher.group(1).trim() : null;
  }

  /**
   * Polls the subscription flow and returns the column names the subscribed {@link Tablet}s
   * actually carry. Returns as soon as the referenced FIELD {@code s1} has been observed. The time
   * column is not part of {@link Tablet#getSchemas()} (it travels in the timestamps), while TAG
   * columns are always retained by the subscription column filter.
   */
  private Set<String> pollSubscribedColumns(final ISubscriptionTablePullConsumer consumer)
      throws Exception {
    final Set<String> subscribedColumns = new LinkedHashSet<>();
    final long deadline = System.currentTimeMillis() + 60_000L;
    while (!subscribedColumns.contains("s1") && System.currentTimeMillis() < deadline) {
      final List<SubscriptionMessage> messages = consumer.poll(Duration.ofSeconds(1));
      for (final SubscriptionMessage message : messages) {
        final Iterator<Tablet> tablets = message.getRecordTabletIterator();
        while (tablets.hasNext()) {
          for (final IMeasurementSchema schema : tablets.next().getSchemas()) {
            subscribedColumns.add(schema.getMeasurementName());
          }
        }
      }
      if (!messages.isEmpty()) {
        consumer.commitSync(messages);
      }
    }
    return subscribedColumns;
  }

  /**
   * Asserts that executing {@code sql} fails. When {@code expectedSubstring} is non-null, the
   * failure message must contain it; otherwise any failure is accepted (e.g. a parse error).
   */
  private void assertCreateFails(
      final Statement statement, final String sql, final String expectedSubstring) {
    final Exception e = Assert.assertThrows(Exception.class, () -> statement.execute(sql));
    if (expectedSubstring != null) {
      assertTrue(
          "Unexpected failure message: " + e.getMessage(),
          e.getMessage() != null && e.getMessage().contains(expectedSubstring));
    }
  }

  /** Creates a tree database, a single timeseries and a tree-to-table view over it. */
  private void createTreeToTableSourceView(final String viewName, final String treeDb)
      throws SQLException {
    try (Connection treeConn = env().getConnection(BaseEnv.TREE_SQL_DIALECT);
        Statement treeStmt = treeConn.createStatement()) {
      treeStmt.execute("CREATE DATABASE " + treeDb);
      treeStmt.execute("CREATE TIMESERIES " + treeDb + ".d1.s1 WITH DATATYPE=INT32, ENCODING=RLE");
    }
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      stmt.execute("USE " + DB);
      stmt.execute(
          "CREATE OR REPLACE VIEW "
              + viewName
              + " (device_id STRING TAG, s1 INT32 FIELD) AS "
              + treeDb
              + ".**");
    }
  }

  // --------------------------------------------------------------------------------------------
  // Positive create / metadata
  // --------------------------------------------------------------------------------------------

  /**
   * SRC-01 Minimal TUMBLE stream: creation succeeds and SHOW STREAMS records source table t1 with
   * status CREATED.
   */
  @Test
  public void testSRC01MinimalValidTumble() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      createDatabase(stmt);
      stmt.execute("CREATE TABLE t1(time TIMESTAMP TIME, device_id STRING TAG, value FLOAT FIELD)");
      stmt.execute("CREATE TABLE rows_calc(time TIMESTAMP TIME, rows INT64 FIELD)");

      stmt.execute(
          "CREATE STREAM stream1 "
              + "FROM t1 TUMBLE(1h) "
              + "INTO rows_calc(time, rows) "
              + "SELECT ${END_TIME}, count(value) FROM ${ROWS}");

      assertStreamExists(stmt, "stream1");
      final String source = getStreamColumn(stmt, "stream1", "source");
      assertNotNull("source should be recorded", source);
      assertTrue("stream should bind base table t1", source.contains("tableName='t1'"));
      Assert.assertEquals("CREATED", getStreamColumn(stmt, "stream1", "status"));
    }
  }

  /**
   * SRC-02 CREATE STREAM IF NOT EXISTS for a new stream: creation succeeds and the stream exists.
   */
  @Test
  public void testSRC02IfNotExistsFirstCreate() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      Thread.sleep(5000);
      createDatabase(stmt);
      stmt.execute("CREATE TABLE t1(time TIMESTAMP TIME, device_id STRING TAG, value FLOAT FIELD)");
      stmt.execute("CREATE TABLE rows_calc(time TIMESTAMP TIME, rows INT64 FIELD)");

      stmt.execute(
          "CREATE STREAM IF NOT EXISTS stream2 "
              + "FROM t1 TUMBLE(1h) "
              + "INTO rows_calc(time, rows) "
              + "SELECT ${END_TIME}, count(value) FROM ${ROWS}");

      assertStreamExists(stmt, "stream2");
    }
  }

  /**
   * SRC-04 CREATE STREAM IF NOT EXISTS with a different definition: succeeds and keeps the original
   * event window.
   */
  @Test
  public void testSRC04DuplicateWithIfNotExists() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      createDatabase(stmt);
      stmt.execute("CREATE TABLE t1(time TIMESTAMP TIME, device_id STRING TAG, value FLOAT FIELD)");
      stmt.execute("CREATE TABLE rows_calc(time TIMESTAMP TIME, rows INT64 FIELD)");

      stmt.execute(
          "CREATE STREAM stream4 "
              + "FROM t1 TUMBLE(1h) "
              + "INTO rows_calc(time, rows) "
              + "SELECT ${END_TIME}, count(value) FROM ${ROWS}");
      final String originalWindow = getStreamColumn(stmt, "stream4", "event_window");

      // A second CREATE STREAM IF NOT EXISTS with a different definition must not override the
      // original stream.
      stmt.execute(
          "CREATE STREAM IF NOT EXISTS stream4 "
              + "FROM t1 TUMBLE(2h) "
              + "INTO rows_calc(time, rows) "
              + "SELECT ${END_TIME}, count(value) FROM ${ROWS}");

      assertStreamExists(stmt, "stream4");
      Assert.assertEquals(
          "original definition should be preserved",
          originalWindow,
          getStreamColumn(stmt, "stream4", "event_window"));
    }
  }

  /** SRC-06 unqualified vs database-qualified source table: both bind to the same base table. */
  @Test
  public void testSRC06CurrentVsExplicitDbSource() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      stmt.execute("CREATE DATABASE stream_selftest");
      stmt.execute("USE stream_selftest");
      stmt.execute(
          "CREATE TABLE source_t(time TIMESTAMP TIME, device_id STRING TAG, value FLOAT FIELD)");
      stmt.execute("CREATE TABLE sink_t(time TIMESTAMP TIME, rows INT64 FIELD)");

      stmt.execute(
          "CREATE STREAM s_cur "
              + "FROM source_t TUMBLE(1h) "
              + "INTO sink_t(time, rows) "
              + "SELECT ${END_TIME}, count(value) FROM ${ROWS}");
      stmt.execute(
          "CREATE STREAM s_exp "
              + "FROM stream_selftest.source_t TUMBLE(1h) "
              + "INTO sink_t(time, rows) "
              + "SELECT ${END_TIME}, count(value) FROM ${ROWS}");

      assertStreamExists(stmt, "s_cur");
      assertStreamExists(stmt, "s_exp");
      Assert.assertTrue(
          "unqualified source should bind source_t",
          getStreamColumn(stmt, "s_cur", "source").contains("tableName='source_t'"));
      Assert.assertTrue(
          "qualified source should bind source_t",
          getStreamColumn(stmt, "s_exp", "source").contains("tableName='source_t'"));
    }
  }

  /** SRC-12 PERIOD window without FROM: creation succeeds. */
  @Test
  public void testSRC12PeriodWithoutFrom() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      createDatabase(stmt);
      stmt.execute("CREATE TABLE t1(time TIMESTAMP TIME, device_id STRING TAG, value FLOAT FIELD)");
      stmt.execute("CREATE TABLE rows_calc(time TIMESTAMP TIME, rows INT64 FIELD)");

      stmt.execute(
          "CREATE STREAM stream12 "
              + "PERIOD(1h) "
              + "INTO rows_calc(time, rows) "
              + "SELECT ${START_TIME}, count(*) FROM t1");

      assertStreamExists(stmt, "stream12");
    }
  }

  /** SRC-31 WITH multiple properties: creation succeeds and left_closed/watermark are persisted. */
  @Test
  public void testSRC31WithMultiProperties() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      createDatabase(stmt);
      stmt.execute("CREATE TABLE t1(time TIMESTAMP TIME, device_id STRING TAG, value FLOAT FIELD)");
      stmt.execute("CREATE TABLE rows_calc(time TIMESTAMP TIME, rows INT64 FIELD)");

      stmt.execute(
          "CREATE STREAM stream31 "
              + "FROM t1 TUMBLE(1h) "
              + "INTO rows_calc(time, rows) "
              + "SELECT ${END_TIME}, count(value) FROM ${ROWS} "
              + "WITH (left_closed = false, watermark = 2m, allowed_lateness = 5m, "
              + "max_delay_window_close = 10m)");

      assertStreamExists(stmt, "stream31");
      final String properties = getStreamProperties(stmt, "stream31");
      assertNotNull("properties should be recorded", properties);
      assertTrue("left_closed should be persisted", properties.contains("leftClosed=false"));
      assertTrue("watermark should be persisted", properties.contains("watermark=120000"));
      assertTrue(
          "allowed_lateness should be persisted", properties.contains("allowedLateness=300000"));
      assertTrue(
          "max_delay_window_close should be persisted",
          properties.contains("maxDelayWindowClose=600000"));
    }
  }

  // --------------------------------------------------------------------------------------------
  // Negative create — validation (assert exact message substring)
  // --------------------------------------------------------------------------------------------

  /** SRC-03 duplicate stream without IF NOT EXISTS: rejected with an 'already exists' error. */
  @Test
  public void testSRC03DuplicateNoIfNotExists() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      createDatabase(stmt);
      stmt.execute("CREATE TABLE t1(time TIMESTAMP TIME, device_id STRING TAG, value FLOAT FIELD)");
      stmt.execute("CREATE TABLE rows_calc(time TIMESTAMP TIME, rows INT64 FIELD)");

      stmt.execute(
          "CREATE STREAM stream3 "
              + "FROM t1 TUMBLE(1h) "
              + "INTO rows_calc(time, rows) "
              + "SELECT ${END_TIME}, count(value) FROM ${ROWS}");

      assertCreateFails(
          stmt,
          "CREATE STREAM stream3 "
              + "FROM t1 TUMBLE(1h) "
              + "INTO rows_calc(time, rows) "
              + "SELECT ${END_TIME}, count(value) FROM ${ROWS}",
          "already exists");
    }
  }

  /** SRC-11 TUMBLE without FROM: rejected with a required-FROM error. */
  @Test
  public void testSRC11TumbleWithoutFrom() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      createDatabase(stmt);
      stmt.execute("CREATE TABLE t1(time TIMESTAMP TIME, device_id STRING TAG, value FLOAT FIELD)");
      stmt.execute("CREATE TABLE rows_calc(time TIMESTAMP TIME, rows INT64 FIELD)");

      assertCreateFails(
          stmt,
          "CREATE STREAM s11 TUMBLE(1h) INTO rows_calc(time, rows) "
              + "SELECT ${START_TIME}, count(*) FROM t1",
          "Tumble event window must be used with FROM clause");
      assertStreamAbsent(stmt, "s11");
    }
  }

  /** SRC-13 PERIOD with FROM: rejected with a forbidden-FROM error. */
  @Test
  public void testSRC13PeriodWithFrom() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      createDatabase(stmt);
      stmt.execute("CREATE TABLE t1(time TIMESTAMP TIME, device_id STRING TAG, value FLOAT FIELD)");
      stmt.execute("CREATE TABLE rows_calc(time TIMESTAMP TIME, rows INT64 FIELD)");

      assertCreateFails(
          stmt,
          "CREATE STREAM s13 FROM t1 PERIOD(1h) INTO rows_calc(time, rows) "
              + "SELECT ${START_TIME}, count(*) FROM t1",
          "Period event window cannot be used with FROM clause");
      assertStreamAbsent(stmt, "s13");
    }
  }

  /** SRC-14 PERIOD with PARTITION BY: rejected with a forbidden-PARTITION-BY error. */
  @Test
  public void testSRC14PeriodWithPartitionBy() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      createDatabase(stmt);
      stmt.execute("CREATE TABLE t1(time TIMESTAMP TIME, device_id STRING TAG, value FLOAT FIELD)");
      stmt.execute("CREATE TABLE rows_calc(time TIMESTAMP TIME, rows INT64 FIELD)");

      assertCreateFails(
          stmt,
          "CREATE STREAM s14 PARTITION BY device_id PERIOD(1h) INTO rows_calc(time, rows) "
              + "SELECT ${START_TIME}, count(*) FROM t1",
          "Period event window cannot be used with PARTITION BY");
      assertStreamAbsent(stmt, "s14");
    }
  }

  /** SRC-18 PARTITION BY the time column: rejected since time is not a TAG column. */
  @Test
  public void testSRC18PartitionByTime() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      createDatabase(stmt);
      stmt.execute(
          "CREATE TABLE t2(time TIMESTAMP TIME, city STRING TAG, device_id STRING TAG, "
              + "value FLOAT FIELD)");
      stmt.execute("CREATE TABLE sink18(time TIMESTAMP TIME, rows INT64 FIELD)");

      assertCreateFails(
          stmt,
          "CREATE STREAM s18 FROM t2 PARTITION BY time TUMBLE(1h) "
              + "INTO sink18(time, rows) SELECT ${START_TIME}, count(*) FROM ${ROWS}",
          "PARTITION BY column must be TAG column");
      assertStreamAbsent(stmt, "s18");
    }
  }

  /** SRC-19 PARTITION BY a FIELD column: rejected since it is not a TAG column. */
  @Test
  public void testSRC19PartitionByField() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      createDatabase(stmt);
      stmt.execute(
          "CREATE TABLE t2(time TIMESTAMP TIME, city STRING TAG, device_id STRING TAG, "
              + "value FLOAT FIELD)");
      stmt.execute("CREATE TABLE sink19(time TIMESTAMP TIME, rows INT64 FIELD)");

      assertCreateFails(
          stmt,
          "CREATE STREAM s19 FROM t2 PARTITION BY value TUMBLE(1h) "
              + "INTO sink19(time, rows) SELECT ${START_TIME}, count(*) FROM ${ROWS}",
          "PARTITION BY column must be TAG column");
      assertStreamAbsent(stmt, "s19");
    }
  }

  /** SRC-20 PARTITION BY an ATTRIBUTE column: rejected since it is not a TAG column. */
  @Test
  public void testSRC20PartitionByAttribute() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      createDatabase(stmt);
      stmt.execute(
          "CREATE TABLE t2(time TIMESTAMP TIME, city STRING TAG, device_id STRING TAG, "
              + "model STRING ATTRIBUTE, value FLOAT FIELD)");
      stmt.execute("CREATE TABLE sink20(time TIMESTAMP TIME, rows INT64 FIELD)");

      assertCreateFails(
          stmt,
          "CREATE STREAM s20 FROM t2 PARTITION BY model TUMBLE(1h) "
              + "INTO sink20(time, rows) SELECT ${START_TIME}, count(*) FROM ${ROWS}",
          "PARTITION BY column must be TAG column");
      assertStreamAbsent(stmt, "s20");
    }
  }

  /**
   * SRC-21 PARTITION BY a TAG plus an ATTRIBUTE column: rejected since the ATTRIBUTE is not a TAG
   * column.
   */
  @Test
  public void testSRC21PartitionByTagAndAttribute() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      createDatabase(stmt);
      stmt.execute(
          "CREATE TABLE t2(time TIMESTAMP TIME, city STRING TAG, device_id STRING TAG, "
              + "model STRING ATTRIBUTE, value FLOAT FIELD)");
      stmt.execute("CREATE TABLE sink21(time TIMESTAMP TIME, rows INT64 FIELD)");

      assertCreateFails(
          stmt,
          "CREATE STREAM s21 FROM t2 PARTITION BY city, model TUMBLE(1h) "
              + "INTO sink21(time, rows) SELECT ${START_TIME}, count(*) FROM ${ROWS}",
          "PARTITION BY column must be TAG column");
      assertStreamAbsent(stmt, "s21");
    }
  }

  /** SRC-22 PARTITION BY an expression: rejected since it is not a column reference. */
  @Test
  public void testSRC22PartitionByExpression() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      createDatabase(stmt);
      stmt.execute(
          "CREATE TABLE t2(time TIMESTAMP TIME, city STRING TAG, device_id STRING TAG, "
              + "value FLOAT FIELD)");
      stmt.execute("CREATE TABLE sink22(time TIMESTAMP TIME, rows INT64 FIELD)");

      assertCreateFails(
          stmt,
          "CREATE STREAM s22 FROM t2 PARTITION BY value + 1 TUMBLE(1h) "
              + "INTO sink22(time, rows) SELECT ${START_TIME}, count(*) FROM ${ROWS}",
          "column reference");
      assertStreamAbsent(stmt, "s22");
    }
  }

  /** SRC-28 source table does not exist: rejected with a 'does not exist' error. */
  @Test
  public void testSRC28SourceTableNotExist() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      createDatabase(stmt);
      stmt.execute("CREATE TABLE rows_calc(time TIMESTAMP TIME, rows INT64 FIELD)");

      assertCreateFails(
          stmt,
          "CREATE STREAM s28 FROM nonexistent_tbl TUMBLE(1h) "
              + "INTO rows_calc(time, rows) SELECT ${START_TIME}, count(*) FROM nonexistent_tbl",
          "does not exist");
      assertStreamAbsent(stmt, "s28");
    }
  }

  // --------------------------------------------------------------------------------------------
  // Negative create — parse/syntax (assert failure + no stream created)
  // --------------------------------------------------------------------------------------------

  /** SRC-05 database-qualified stream name: rejected by the grammar and not created. */
  @Test
  public void testSRC05DatabaseQualifiedStreamName() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      createDatabase(stmt);
      stmt.execute("CREATE TABLE t1(time TIMESTAMP TIME, device_id STRING TAG, value FLOAT FIELD)");
      stmt.execute("CREATE TABLE rows_calc(time TIMESTAMP TIME, rows INT64 FIELD)");

      assertCreateFails(
          stmt,
          "CREATE STREAM db1.s05 FROM t1 TUMBLE(1h) INTO rows_calc(time, rows) "
              + "SELECT ${START_TIME}, count(*) FROM t1",
          null);
      assertStreamAbsent(stmt, "db1.s05");
    }
  }

  /** SRC-07 multiple source tables: rejected by the grammar and not created. */
  @Test
  public void testSRC07MultipleSourceTables() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      createDatabase(stmt);
      stmt.execute("CREATE TABLE t1(time TIMESTAMP TIME, device_id STRING TAG, value FLOAT FIELD)");
      stmt.execute("CREATE TABLE t2(time TIMESTAMP TIME, device_id STRING TAG, value FLOAT FIELD)");
      stmt.execute("CREATE TABLE rows_calc(time TIMESTAMP TIME, rows INT64 FIELD)");

      assertCreateFails(
          stmt,
          "CREATE STREAM s07 FROM t1, t2 TUMBLE(1h) INTO rows_calc(time, rows) "
              + "SELECT ${START_TIME}, count(*) FROM t1",
          null);
      assertStreamAbsent(stmt, "s07");

      // a top-level JOIN in stream_source is likewise rejected by the grammar
      assertCreateFails(
          stmt,
          "CREATE STREAM s07b FROM t1 JOIN t2 ON t1.time = t2.time TUMBLE(1h) "
              + "INTO rows_calc(time, rows) SELECT ${START_TIME}, count(*) FROM t1",
          null);
      assertStreamAbsent(stmt, "s07b");
    }
  }

  /** SRC-29 missing event window type: rejected by the grammar and not created. */
  @Test
  public void testSRC29MissingEventType() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      createDatabase(stmt);
      stmt.execute("CREATE TABLE t1(time TIMESTAMP TIME, device_id STRING TAG, value FLOAT FIELD)");
      stmt.execute("CREATE TABLE rows_calc(time TIMESTAMP TIME, rows INT64 FIELD)");

      assertCreateFails(
          stmt,
          "CREATE STREAM s29 FROM t1 INTO rows_calc(time, rows) SELECT ${START_TIME}, count(*) FROM t1",
          null);
      assertStreamAbsent(stmt, "s29");
    }
  }

  /** SRC-30 missing INTO clause: rejected by the grammar and not created. */
  @Test
  public void testSRC30MissingInto() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      createDatabase(stmt);
      stmt.execute("CREATE TABLE t1(time TIMESTAMP TIME, device_id STRING TAG, value FLOAT FIELD)");

      assertCreateFails(
          stmt,
          "CREATE STREAM s30 FROM t1 TUMBLE(1h) SELECT ${START_TIME}, count(*) FROM t1",
          null);
      assertStreamAbsent(stmt, "s30");
    }
  }

  /** SRC-32 malformed WITH property: rejected by the grammar and not created. */
  @Test
  public void testSRC32WithPropertyFormatError() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      createDatabase(stmt);
      stmt.execute("CREATE TABLE t1(time TIMESTAMP TIME, device_id STRING TAG, value FLOAT FIELD)");
      stmt.execute("CREATE TABLE rows_calc(time TIMESTAMP TIME, rows INT64 FIELD)");

      // missing '='
      assertCreateFails(
          stmt,
          "CREATE STREAM s32 FROM t1 TUMBLE(1h) INTO rows_calc(time, rows) "
              + "SELECT ${START_TIME}, count(*) FROM t1 WITH (watermark 2m)",
          null);
      assertStreamAbsent(stmt, "s32");

      // missing value
      assertCreateFails(
          stmt,
          "CREATE STREAM s32b FROM t1 TUMBLE(1h) INTO rows_calc(time, rows) "
              + "SELECT ${START_TIME}, count(*) FROM t1 WITH (watermark =)",
          null);
      assertStreamAbsent(stmt, "s32b");

      // wrong delimiter (missing ',' between two properties)
      assertCreateFails(
          stmt,
          "CREATE STREAM s32c FROM t1 TUMBLE(1h) INTO rows_calc(time, rows) "
              + "SELECT ${START_TIME}, count(*) FROM t1 WITH (watermark = 2m left_closed = false)",
          null);
      assertStreamAbsent(stmt, "s32c");
    }
  }

  /**
   * SRC-33 missing subquery: rejected since the subquery is mandatory despite the [subquery]
   * grammar notation.
   */
  @Test
  public void testSRC33SubqueryRequired() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      createDatabase(stmt);
      stmt.execute("CREATE TABLE t1(time TIMESTAMP TIME, device_id STRING TAG, value FLOAT FIELD)");
      stmt.execute("CREATE TABLE rows_calc(time TIMESTAMP TIME, rows INT64 FIELD)");

      // The [subquery] brackets in the grammar are not actually optional.
      assertCreateFails(
          stmt, "CREATE STREAM s33 FROM t1 TUMBLE(1h) INTO rows_calc(time, rows)", null);
      assertStreamAbsent(stmt, "s33");
    }
  }

  /**
   * SRC-10 tree-to-table view as source: the tree-to-table view must be rejected, since only a base
   * table may be a stream source.
   */
  @Test
  public void testSRC10TreeToTableViewAsSource() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      createDatabase(stmt);
      stmt.execute("CREATE TABLE sink10(time TIMESTAMP TIME, v INT32 FIELD)");
      createTreeToTableSourceView("tree_to_table_view", "root.srctree10");

      assertCreateFails(
          stmt,
          "CREATE STREAM s10 FROM tree_to_table_view TUMBLE(1h) INTO sink10(time, v) "
              + "SELECT time, s1 FROM tree_to_table_view",
          "cannot be a system table or a view");
      assertStreamAbsent(stmt, "s10");
    }
  }

  // --------------------------------------------------------------------------------------------
  // Runtime (start stream, insert, await write-back)
  // --------------------------------------------------------------------------------------------

  /**
   * SRC-15 TUMBLE without partition: emits a single global aggregate row per window. The window
   * aggregate is the sum of {@code value}, so W1 (the rows at base+1000, base+2000 and base+3000)
   * must yield 1.0 + 2.0 + 3.0 = 6.0.
   */
  @Test
  public void testSRC15TumbleNoPartitionGlobalWindow() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      createDatabase(stmt);
      stmt.execute(
          "CREATE TABLE t15(time TIMESTAMP TIME, device_id STRING TAG, value FLOAT FIELD)");
      stmt.execute("CREATE TABLE sink15(time TIMESTAMP TIME, total DOUBLE FIELD)");

      final long base = System.currentTimeMillis() + 2000;
      stmt.execute(
          "CREATE STREAM s15 FROM t15 TUMBLE(5s, "
              + base
              + ") INTO sink15(time, total) SELECT ${START_TIME}, sum(value) FROM ${ROWS}");
      stmt.execute("START STREAM s15");
      // Write the source rows only once the stream is RUNNING, so that no row can be written
      // before the subscription is bound and therefore enter no window at all.
      awaitStreamRunning(stmt, "s15");

      // three rows in W1 = [base, base+5s), one row in W2 to advance the watermark and close W1
      stmt.execute(
          "INSERT INTO t15(time, device_id, value) VALUES (" + (base + 1000) + ", 'd1', 1.0)");
      stmt.execute(
          "INSERT INTO t15(time, device_id, value) VALUES (" + (base + 2000) + ", 'd2', 2.0)");
      stmt.execute(
          "INSERT INTO t15(time, device_id, value) VALUES (" + (base + 3000) + ", 'd3', 3.0)");
      Thread.sleep(5000);
      stmt.execute(
          "INSERT INTO t15(time, device_id, value) VALUES (" + (base + 6000) + ", 'd1', 4.0)");

      AWAIT.untilAsserted(
          () -> {
            boolean found = false;
            try (ResultSet rs = stmt.executeQuery("SELECT total FROM sink15")) {
              while (rs.next()) {
                if (Math.abs(rs.getDouble("total") - 6.0) < 1e-9) {
                  found = true;
                }
              }
            }
            assertTrue(
                "sink should contain the global window sum of 6.0, actual sink15 content:\n"
                    + dumpTable(stmt, "sink15"),
                found);
          });
    }
  }

  /** SRC-16 PARTITION BY all TAG columns: each group emits independently. */
  @Test
  public void testSRC16PartitionByAllTags() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      createDatabase(stmt);
      stmt.execute(
          "CREATE TABLE t16(time TIMESTAMP TIME, city STRING TAG, device_id STRING TAG, "
              + "value FLOAT FIELD)");
      stmt.execute(
          "CREATE TABLE sink16(time TIMESTAMP TIME, city STRING TAG, device_id STRING TAG, "
              + "rows INT64 FIELD)");

      final long base = System.currentTimeMillis() + 2000;
      stmt.execute(
          "CREATE STREAM s16 FROM t16 PARTITION BY city, device_id TUMBLE(5s, "
              + base
              + ") INTO sink16(time, city, device_id, rows) "
              + "SELECT ${START_TIME}, ${1}, ${2}, count(*) FROM ${ROWS}");
      stmt.execute("START STREAM s16");
      awaitStreamRunning(stmt, "s16");

      stmt.execute(
          "INSERT INTO t16(time, city, device_id, value) VALUES ("
              + (base + 1000)
              + ", 'bj', 'd1', 1.0)");
      stmt.execute(
          "INSERT INTO t16(time, city, device_id, value) VALUES ("
              + (base + 2000)
              + ", 'bj', 'd1', 2.0)");
      stmt.execute(
          "INSERT INTO t16(time, city, device_id, value) VALUES ("
              + (base + 1000)
              + ", 'sh', 'd1', 3.0)");
      // W2 rows advance each group's watermark, closing W1 independently
      stmt.execute(
          "INSERT INTO t16(time, city, device_id, value) VALUES ("
              + (base + 6000)
              + ", 'bj', 'd1', 4.0)");
      stmt.execute(
          "INSERT INTO t16(time, city, device_id, value) VALUES ("
              + (base + 6000)
              + ", 'sh', 'd1', 5.0)");

      AWAIT.untilAsserted(
          () -> {
            final Map<String, Integer> rows = new HashMap<>();
            try (ResultSet rs = stmt.executeQuery("SELECT city, device_id, rows FROM sink16")) {
              while (rs.next()) {
                rows.merge(
                    rs.getString("city") + "-" + rs.getString("device_id"),
                    rs.getInt("rows"),
                    Integer::sum);
              }
            }
            assertTrue("group (bj,d1) should emit count 2", rows.getOrDefault("bj-d1", 0) >= 2);
            assertTrue("group (sh,d1) should emit count 1", rows.getOrDefault("sh-d1", 0) >= 1);
          });
    }
  }

  /**
   * SRC-17 PARTITION BY a partial TAG set: rows aggregate per city without cross-group
   * interference.
   */
  @Test
  public void testSRC17PartitionByPartialTag() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      createDatabase(stmt);
      stmt.execute(
          "CREATE TABLE t17(time TIMESTAMP TIME, city STRING TAG, device_id STRING TAG, "
              + "value FLOAT FIELD)");
      stmt.execute("CREATE TABLE sink17(time TIMESTAMP TIME, city STRING TAG, rows INT64 FIELD)");

      final long base = System.currentTimeMillis() + 2000;
      stmt.execute(
          "CREATE STREAM s17 FROM t17 PARTITION BY city TUMBLE(5s, "
              + base
              + ") INTO sink17(time, city, rows) SELECT ${START_TIME}, ${1}, count(*) FROM ${ROWS}");
      stmt.execute("START STREAM s17");
      awaitStreamRunning(stmt, "s17");

      stmt.execute(
          "INSERT INTO t17(time, city, device_id, value) VALUES ("
              + (base + 1000)
              + ", 'bj', 'd1', 1.0)");
      stmt.execute(
          "INSERT INTO t17(time, city, device_id, value) VALUES ("
              + (base + 2000)
              + ", 'bj', 'd2', 2.0)");
      stmt.execute(
          "INSERT INTO t17(time, city, device_id, value) VALUES ("
              + (base + 1000)
              + ", 'sh', 'd1', 3.0)");
      // Don't let the window-closing trigger data contaminate the results.
      Thread.sleep(3000);
      stmt.execute(
          "INSERT INTO t17(time, city, device_id, value) VALUES ("
              + (base + 6000)
              + ", 'bj', 'd1', 4.0)");
      stmt.execute(
          "INSERT INTO t17(time, city, device_id, value) VALUES ("
              + (base + 6000)
              + ", 'sh', 'd1', 5.0)");

      AWAIT.untilAsserted(
          () -> {
            final Map<String, Integer> rows = new HashMap<>();
            try (ResultSet rs = stmt.executeQuery("SELECT city, rows FROM sink17")) {
              while (rs.next()) {
                rows.merge(rs.getString("city"), rs.getInt("rows"), Integer::sum);
              }
            }
            // One Statement cannot hold two open ResultSets, so the dump runs after the cursor
            // above is closed. Showing the raw sink next to the aggregated map tells apart "no row
            // yet" from "rows leaked into the wrong city group".
            final String actual =
                "aggregated=" + rows + "\nactual sink17 content:\n" + dumpTable(stmt, "sink17");
            assertTrue(
                "city bj should aggregate to count 2, " + actual, rows.getOrDefault("bj", 0) >= 2);
            assertTrue(
                "city sh should aggregate to count 1, " + actual, rows.getOrDefault("sh", 0) >= 1);
          });
    }
  }

  /**
   * SRC-23/24 drop then recreate the source table: the running stream survives and continues from
   * new writes.
   */
  @Test
  public void testSRC23_24DropAndRecreateSourceTable() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      createDatabase(stmt);
      stmt.execute(
          "CREATE TABLE t24(time TIMESTAMP TIME, device_id STRING TAG, value FLOAT FIELD)");
      stmt.execute("CREATE TABLE sink24(time TIMESTAMP TIME, rows INT64 FIELD)");

      final long base = System.currentTimeMillis() + 2000;
      stmt.execute(
          "CREATE STREAM s24 FROM t24 TUMBLE(5s, "
              + base
              + ") INTO sink24(time, rows) SELECT ${START_TIME}, count(*) FROM ${ROWS}");
      stmt.execute("START STREAM s24");
      awaitStreamRunning(stmt, "s24");

      // SRC-23: dropping the source table is not blocked by the running stream
      stmt.execute("DROP TABLE t24");
      assertStreamExists(stmt, "s24");

      // SRC-24: recreating the same-name table lets the stream continue from new writes
      stmt.execute(
          "CREATE TABLE t24(time TIMESTAMP TIME, device_id STRING TAG, value FLOAT FIELD)");
      final long now = System.currentTimeMillis();
      stmt.execute("INSERT INTO t24(time, device_id, value) VALUES (" + now + ", 'd1', 1.0)");
      stmt.execute(
          "INSERT INTO t24(time, device_id, value) VALUES (" + (now + 7000) + ", 'd2', 2.0)");

      AWAIT.untilAsserted(
          () -> {
            boolean found = false;
            try (ResultSet rs = stmt.executeQuery("SELECT rows FROM sink24")) {
              while (rs.next()) {
                if (rs.getInt("rows") >= 1) {
                  found = true;
                }
              }
            }
            assertTrue("stream should continue computing after source table recreation", found);
          });
    }
  }

  /**
   * SRC-25 TIME and all TAG columns propagate into the sink output. The partition TAG columns are
   * referenced as ${1}/${2} because a partition key is group-invariant; writing the raw column
   * names next to an aggregate would instead require an explicit GROUP BY.
   */
  @Test
  public void testSRC25TimeAndAllTagPropagation() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      createDatabase(stmt);
      stmt.execute(
          "CREATE TABLE t25(time TIMESTAMP TIME, city STRING TAG, device_id STRING TAG, "
              + "value FLOAT FIELD)");
      stmt.execute(
          "CREATE TABLE sink25(time TIMESTAMP TIME, city STRING TAG, device_id STRING TAG, "
              + "rows INT64 FIELD)");

      final long base = System.currentTimeMillis() + 2000;
      stmt.execute(
          "CREATE STREAM s25 FROM t25 PARTITION BY city, device_id TUMBLE(5s, "
              + base
              + ") INTO sink25(time, city, device_id, rows) "
              + "SELECT ${START_TIME}, ${1}, ${2}, count(*) FROM ${ROWS}");
      stmt.execute("START STREAM s25");
      awaitStreamRunning(stmt, "s25");

      stmt.execute(
          "INSERT INTO t25(time, city, device_id, value) VALUES ("
              + (base + 1000)
              + ", 'bj', 'd1', 1.0)");
      stmt.execute(
          "INSERT INTO t25(time, city, device_id, value) VALUES ("
              + (base + 2000)
              + ", 'bj', 'd1', 2.0)");
      stmt.execute(
          "INSERT INTO t25(time, city, device_id, value) VALUES ("
              + (base + 6000)
              + ", 'bj', 'd1', 3.0)");

      AWAIT.untilAsserted(
          () -> {
            try (ResultSet rs = stmt.executeQuery("SELECT city, device_id, rows FROM sink25")) {
              assertTrue("sink should have a row", rs.next());
              Assert.assertEquals("bj", rs.getString("city"));
              Assert.assertEquals("d1", rs.getString("device_id"));
              assertTrue("count should be >= 2", rs.getInt("rows") >= 2);
            }
          });
    }
  }

  /**
   * SRC-26 FIELD auto-derivation: the query references only s1/state, so the stream's subscription
   * flow carries the TIME column, the TAG column plus those two FIELDs, and the unreferenced FIELD
   * s2 never enters the subscribed columns. The subscribed columns are read back from the
   * subscription flow itself (the columns of the subscribed tablets), while the sink proves the
   * referenced FIELDs are actually usable.
   */
  @Test
  public void testSRC26FieldAutoDerivation() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      createDatabase(stmt);
      stmt.execute(
          "CREATE TABLE t26(time TIMESTAMP TIME, device_id STRING TAG, "
              + "s1 FLOAT FIELD, s2 FLOAT FIELD, state BOOLEAN FIELD)");
      stmt.execute("CREATE TABLE sink26(time TIMESTAMP TIME, s1 FLOAT FIELD, state BOOLEAN FIELD)");

      final long base = System.currentTimeMillis() + 2000;
      stmt.execute(
          "CREATE STREAM s26 FROM t26 PARTITION BY device_id TUMBLE(5s, "
              + base
              + ") INTO sink26(time, s1, state) "
              + "SELECT ${START_TIME}, s1, state FROM ${ROWS}");
      stmt.execute("START STREAM s26");
      awaitStreamRunning(stmt, "s26");

      // The stream owns the subscription topic __stream__s26; wait until the source created it.
      final String topicName = IoTDBSubscriptionSource.getTopicName("s26");
      try (ISubscriptionTableSession subscriptionSession =
          new SubscriptionTableSessionBuilder()
              .host(env().getIP())
              .port(Integer.parseInt(env().getPort()))
              .build()) {
        subscriptionSession.open();
        AWAIT.untilAsserted(() -> assertTrue(subscriptionSession.getTopic(topicName).isPresent()));

        // Subscribe before writing: the consumer group must not miss the rows written below.
        final ISubscriptionTablePullConsumer consumer =
            openStreamTopicConsumer(subscriptionSession, topicName);
        try {
          stmt.execute(
              "INSERT INTO t26(time, device_id, s1, s2, state) VALUES ("
                  + (base + 1000)
                  + ", 'd1', 10.0, 20.0, true)");
          stmt.execute(
              "INSERT INTO t26(time, device_id, s1, s2, state) VALUES ("
                  + (base + 6000)
                  + ", 'd1', 12.0, 22.0, true)");

          final Set<String> subscribedColumns = pollSubscribedColumns(consumer);
          assertTrue(
              "subscription should carry the referenced FIELD columns, actual=" + subscribedColumns,
              subscribedColumns.containsAll(List.of("device_id", "s1", "state")));
          assertFalse(
              "Unreferenced FIELD s2 must not enter the subscription columns, actual="
                  + subscribedColumns,
              subscribedColumns.contains("s2"));
        } finally {
          consumer.unsubscribe(topicName);
          consumer.close();
        }
      }

      AWAIT.untilAsserted(
          () -> {
            try (ResultSet rs = stmt.executeQuery("SELECT s1, state FROM sink26")) {
              boolean found = false;
              while (rs.next()) {
                if (rs.getDouble("s1") == 10.0 && rs.getBoolean("state")) {
                  found = true;
                }
              }
              assertTrue("sink should propagate the referenced FIELD columns s1 and state", found);
            }
          });
    }
  }

  /**
   * SRC-27 (a) ATTRIBUTE boundary: an ATTRIBUTE column is not propagated into the ${ROWS}
   * subscription dataset, so the calc plan cannot obtain it from there. Reading it from ${ROWS} is
   * rejected when the stream is created, and no stream task is produced.
   */
  @Test
  public void testSRC27aRowsDatasetRejectsAttribute() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      createDatabase(stmt);
      stmt.execute(
          "CREATE TABLE t27(time TIMESTAMP TIME, device_id STRING TAG, model STRING ATTRIBUTE, "
              + "s1 FLOAT FIELD)");
      stmt.execute("CREATE TABLE sink27a(time TIMESTAMP TIME, model STRING FIELD)");

      final long base = System.currentTimeMillis() + 2000;
      assertCreateFails(
          stmt,
          "CREATE STREAM s27a FROM t27 TUMBLE(5s, "
              + base
              + ") INTO sink27a(time, model) SELECT ${START_TIME}, model FROM ${ROWS}",
          null);
      assertStreamAbsent(stmt, "s27a");
    }
  }

  /**
   * SRC-27 (b) ATTRIBUTE boundary: an explicit {@code FROM t27} re-queries the ordinary table
   * directly, so the ATTRIBUTE column is readable there. The "not propagated by the subscription"
   * boundary of (a) must not be widened into "not readable from the table".
   */
  @Test
  public void testSRC27bExplicitSourceReadsAttribute() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      createDatabase(stmt);
      stmt.execute(
          "CREATE TABLE t27(time TIMESTAMP TIME, device_id STRING TAG, model STRING ATTRIBUTE, "
              + "s1 FLOAT FIELD)");
      stmt.execute("CREATE TABLE sink27b(time TIMESTAMP TIME, model STRING FIELD)");

      final long base = System.currentTimeMillis() + 2000;
      stmt.execute(
          "CREATE STREAM s27b FROM t27 TUMBLE(5s, "
              + base
              + ") INTO sink27b(time, model) SELECT time, model FROM t27");
      stmt.execute("START STREAM s27b");
      awaitStreamRunning(stmt, "s27b");

      stmt.execute(
          "INSERT INTO t27(time, device_id, model, s1) VALUES ("
              + (base + 1000)
              + ", 'd1', 'M1', 1.0)");
      // the second window advances the watermark and closes the first one
      stmt.execute(
          "INSERT INTO t27(time, device_id, model, s1) VALUES ("
              + (base + 6000)
              + ", 'd1', 'M2', 2.0)");

      AWAIT.untilAsserted(
          () -> {
            boolean found = false;
            try (ResultSet rs = stmt.executeQuery("SELECT * FROM sink27b")) {
              while (rs.next()) {
                if ("M2".equals(rs.getString("model"))) {
                  found = true;
                }
              }
            }
            // dumpTable runs after the cursor above is closed: one Statement cannot hold two open
            // ResultSets at the same time.
            assertTrue(
                "explicit-FROM stream should read ATTRIBUTE model, actual sink27b content:\n"
                    + dumpTable(stmt, "sink27b"),
                found);
          });
    }
  }
}
