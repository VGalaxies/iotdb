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
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Integration tests for the metadata of {@code information_schema.streams} (spec cases
 * META-01/02/03/06/07 of the stream-computing requirements doc). All assertions follow the spec
 * (the "Stream Computing Module - Requirements Analysis" spec) rather than the current
 * implementation, so failures pinpoint the gaps that the implementation must close.
 */
@Category({StreamClusterIT.class})
public class StreamNodeStreamsTableIT extends StreamNodeITBase {

  private static final ConditionFactory AWAIT =
      Awaitility.await()
          .pollInSameThread()
          .pollDelay(1, TimeUnit.SECONDS)
          .pollInterval(1, TimeUnit.SECONDS)
          .atMost(10, TimeUnit.SECONDS);

  /** Expected columns of information_schema.streams, in order (failed_count has no constant). */
  private static final String[] EXPECTED_COLUMNS = {
    ColumnHeaderConstant.STREAM_NAME_TABLE_MODEL,
    ColumnHeaderConstant.CREATION_TIME_TABLE_MODEL,
    ColumnHeaderConstant.STREAM_CREATOR_TABLE_MODEL,
    ColumnHeaderConstant.STREAM_SOURCE_TABLE_MODEL,
    ColumnHeaderConstant.STREAM_EVENT_WINDOW_TABLE_MODEL,
    ColumnHeaderConstant.STREAM_SUB_QUERY_TABLE_MODEL,
    ColumnHeaderConstant.STREAM_TARGET_TABLE_MODEL,
    ColumnHeaderConstant.STREAM_STATUS_TABLE_MODEL,
    ColumnHeaderConstant.STREAM_RUNNING_ON_TABLE_MODEL,
    ColumnHeaderConstant.STREAM_LAST_UP_TIME_TABLE_MODEL,
    ColumnHeaderConstant.STREAM_LAST_DOWN_TIME_TABLE_MODEL,
    ColumnHeaderConstant.STREAM_LAST_HEARTBEAT_TIME_TABLE_MODEL,
    ColumnHeaderConstant.STREAM_LAST_DOWN_REASON_TABLE_MODEL,
    ColumnHeaderConstant.STREAM_PROPERTIES_TABLE_MODEL
  };

  /** Columns the spec types as int64 timestamps (TIMESTAMP is the 64-bit implementation). */
  private static final Set<String> TIME_COLUMNS =
      new HashSet<>(
          Arrays.asList(
              ColumnHeaderConstant.CREATION_TIME_TABLE_MODEL,
              ColumnHeaderConstant.STREAM_LAST_UP_TIME_TABLE_MODEL,
              ColumnHeaderConstant.STREAM_LAST_DOWN_TIME_TABLE_MODEL,
              ColumnHeaderConstant.STREAM_LAST_HEARTBEAT_TIME_TABLE_MODEL));

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
      for (String database : new String[] {"test", "test_a", "test_b"}) {
        try {
          statement.execute("DROP DATABASE " + database);
        } catch (Exception ignored) {
          // database may not exist if a test failed before creating it
        }
      }
    }
  }

  /** META-01: information_schema.streams returns the 15 spec-defined columns with their types. */
  @Test
  public void testStreamsTableSchemaMatchesSpec() throws Exception {
    try (Connection connection =
            StreamEnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      createTumbleStream(statement, "meta_schema_stream");

      try (ResultSet rs = statement.executeQuery("SELECT * FROM information_schema.streams")) {
        ResultSetMetaData metadata = rs.getMetaData();
        List<String> actualColumns = new ArrayList<>();
        for (int i = 1; i <= metadata.getColumnCount(); i++) {
          actualColumns.add(metadata.getColumnLabel(i));
        }
        assertEquals(
            "information_schema.streams columns (names, count and order) must match the spec",
            Arrays.asList(EXPECTED_COLUMNS),
            actualColumns);

        for (int i = 1; i <= metadata.getColumnCount(); i++) {
          String column = metadata.getColumnLabel(i);
          int type = metadata.getColumnType(i);
          if (TIME_COLUMNS.contains(column)) {
            assertTrue(
                "Column " + column + " must be a 64-bit time column (TIMESTAMP/BIGINT)",
                type == Types.TIMESTAMP || type == Types.BIGINT);
          } else {
            assertTrue(
                "Column " + column + " must be a string column",
                type == Types.VARCHAR
                    || type == Types.NVARCHAR
                    || type == Types.CHAR
                    || type == Types.NCHAR
                    || type == Types.LONGVARCHAR
                    || type == Types.LONGNVARCHAR);
          }
        }
      }
    }
  }

  /**
   * META-02: after CREATE STREAM (TUMBLE) the row reflects name, creation time, creator,
   * source/target, window, sub-query and properties of the definition.
   */
  @Test
  public void testCreateTumbleStreamMetadataMatchesDefinition() throws Exception {
    final String streamName = "meta_tumble_stream";
    long beforeCreate = System.currentTimeMillis();
    try (Connection connection =
            StreamEnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      createTumbleStream(statement, streamName);

      AWAIT.untilAsserted(
          () ->
              assertNotNull(
                  "Stream row should be queryable after CREATE STREAM",
                  queryStreamRow(statement, streamName)));

      Map<String, Object> row = queryStreamRow(statement, streamName);
      assertEquals(streamName, row.get(ColumnHeaderConstant.STREAM_NAME_TABLE_MODEL));
      assertEquals("root", row.get(ColumnHeaderConstant.STREAM_CREATOR_TABLE_MODEL));
      long creationTime = asLong(row.get(ColumnHeaderConstant.CREATION_TIME_TABLE_MODEL));
      assertTrue(
          "creation_time should be close to the creation moment",
          creationTime >= beforeCreate - 5000L
              && creationTime <= System.currentTimeMillis() + 5000L);

      String sourceTableName = (String) row.get(ColumnHeaderConstant.STREAM_SOURCE_TABLE_MODEL);
      assertNotNull(sourceTableName);
      assertTrue(
          "source_table_name should contain the source table name but was: " + sourceTableName,
          sourceTableName.contains(SOURCE_TABLE));
      String eventWindow = (String) row.get(ColumnHeaderConstant.STREAM_EVENT_WINDOW_TABLE_MODEL);
      assertNotNull(eventWindow);
      assertTrue(
          "stream_event_window should reflect the TUMBLE window but was: " + eventWindow,
          eventWindow.contains("Tumble"));
      String subQuery = (String) row.get(ColumnHeaderConstant.STREAM_SUB_QUERY_TABLE_MODEL);
      assertNotNull(subQuery);
      assertTrue(
          "sub_query should reflect the sub-query definition but was: " + subQuery,
          subQuery.contains("value") && subQuery.contains(SOURCE_TABLE));
      String targetTableName = (String) row.get(ColumnHeaderConstant.STREAM_TARGET_TABLE_MODEL);
      assertNotNull(targetTableName);
      assertTrue(
          "target_table_name should contain the target table name but was: " + targetTableName,
          targetTableName.contains(SINK_TABLE));
      Object properties = row.get(ColumnHeaderConstant.STREAM_PROPERTIES_TABLE_MODEL);
      assertNotNull("properties should not be null", properties);
      assertFalse("properties should not be empty", properties.toString().isEmpty());
    }
  }

  /** META-03: a PERIOD stream without a source table has a NULL source_table_name. */
  @Test
  public void testPeriodStreamWithoutSourceHasNullSourceTableName() throws Exception {
    final String streamName = "meta_period_stream";
    try (Connection connection =
            StreamEnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE test");
      statement.execute("USE test");
      statement.execute(
          "CREATE TABLE t1 (time TIMESTAMP TIME, device_id STRING TAG, value INT64 FIELD)");
      statement.execute("CREATE TABLE rows_calc (time TIMESTAMP TIME, rows INT64 FIELD)");
      statement.execute(
          "CREATE STREAM "
              + streamName
              + " PERIOD(1h) INTO test.rows_calc(time, rows)"
              + " SELECT ${START_TIME}, count(*) FROM test.t1");

      AWAIT.untilAsserted(
          () ->
              assertNotNull(
                  "Stream row should be queryable after CREATE STREAM",
                  queryStreamRow(statement, streamName)));

      Map<String, Object> row = queryStreamRow(statement, streamName);
      assertEquals(
          "source_table_name must be empty for a PERIOD stream without a source table",
          "",
          row.get(ColumnHeaderConstant.STREAM_SOURCE_TABLE_MODEL));
    }
  }

  /**
   * META-06: properties is serialized as comma-separated key=value pairs reflecting the four time
   * control options configured in WITH.
   */
  @Test
  public void testPropertiesSerializedAsCommaSeparatedKeyValuePairs() throws Exception {
    final String streamName = "meta_props_stream";
    try (Connection connection =
            StreamEnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
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
              + " TUMBLE(size => 1h)"
              + " INTO test."
              + SINK_TABLE
              + "(time, value)"
              + " SELECT time, value FROM test."
              + SOURCE_TABLE
              + " WITH (watermark = 10s, allowed_lateness = 5m, max_delay_window_close = 1h,"
              + " max_idle_window_retain = 2h)");

      AWAIT.untilAsserted(
          () ->
              assertNotNull(
                  "Stream row should be queryable after CREATE STREAM",
                  queryStreamRow(statement, streamName)));

      Map<String, Object> row = queryStreamRow(statement, streamName);
      Object propertiesObj = row.get(ColumnHeaderConstant.STREAM_PROPERTIES_TABLE_MODEL);
      assertNotNull("properties column should exist and be non-null", propertiesObj);
      String properties = propertiesObj.toString();

      // The properties column returns the toString() of StreamProperties,
      // e.g. "StreamProperties{watermark=10000, allowedLateness=300000, ...}".
      String innerProperties =
          properties.substring(properties.indexOf('{') + 1, properties.lastIndexOf('}'));
      Map<String, String> keyValuePairs = new HashMap<>();
      for (String segment : innerProperties.split(",")) {
        String trimmed = segment.trim();
        int idx = trimmed.indexOf('=');
        assertTrue(
            "properties segment must be key=value: " + trimmed + " (full: " + properties + ")",
            idx > 0);
        keyValuePairs.put(normalizeKey(trimmed.substring(0, idx)), trimmed.substring(idx + 1));
      }
      assertPropertyValue(keyValuePairs, "watermark", "10000", "10s", "10000ms");
      assertPropertyValue(keyValuePairs, "allowed_lateness", "300000", "5m", "300000ms");
      assertPropertyValue(keyValuePairs, "max_delay_window_close", "3600000", "1h", "3600000ms");
      assertPropertyValue(keyValuePairs, "max_idle_window_retain", "7200000", "2h", "7200000ms");
    }
  }

  /**
   * META-07: a stream created with an unqualified name in database A is shown with the same
   * unqualified name from both database A and database B.
   */
  @Test
  public void testGlobalStreamNameDisplayedUnqualifiedAcrossDatabases() throws Exception {
    final String streamName = "global_stream";
    try (Connection connection =
            StreamEnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE test_a");
      statement.execute("CREATE DATABASE test_b");
      statement.execute("USE test_a");
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
              + " FROM test_a."
              + SOURCE_TABLE
              + " TUMBLE(size => 1h)"
              + " INTO test_a."
              + SINK_TABLE
              + "(time, value)"
              + " SELECT time, value FROM test_a."
              + SOURCE_TABLE);

      // Query from the database that owns the stream.
      statement.execute("USE test_a");
      assertEquals(
          "stream_name must be displayed unqualified from the owning database",
          streamName,
          queryStreamName(statement, streamName));

      // Query from another database: same global unqualified name, no database prefix.
      statement.execute("USE test_b");
      assertEquals(
          "stream_name must be displayed unqualified from any database",
          streamName,
          queryStreamName(statement, streamName));
    }
  }

  private static final String SOURCE_TABLE = "src_t";
  private static final String SINK_TABLE = "sink_t";

  private void createTumbleStream(Statement statement, String streamName) throws SQLException {
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
            + " TUMBLE(size => 1h, origin => 2000-01-01T00:00:00)"
            + " INTO test."
            + SINK_TABLE
            + "(time, value)"
            + " SELECT time, value FROM test."
            + SOURCE_TABLE);
  }

  private String queryStreamName(Statement statement, String streamName) throws SQLException {
    try (ResultSet rs =
        statement.executeQuery(
            "SELECT stream_name FROM information_schema.streams WHERE stream_name = '"
                + streamName
                + "'")) {
      assertTrue(
          "Stream " + streamName + " should be visible regardless of the current database",
          rs.next());
      return rs.getString(ColumnHeaderConstant.STREAM_NAME_TABLE_MODEL);
    }
  }

  /** Normalizes a properties key so snake_case and camelCase spellings compare equal. */
  private static String normalizeKey(String key) {
    return key.replace("_", "").toLowerCase();
  }

  private static void assertPropertyValue(
      Map<String, String> keyValuePairs, String key, String... acceptedValues) {
    String value = keyValuePairs.get(normalizeKey(key));
    assertNotNull(
        "properties should contain key '" + key + "' but has: " + keyValuePairs.keySet(), value);
    assertTrue(
        "properties['"
            + key
            + "'] = '"
            + value
            + "' does not reflect the configured value (accepted: "
            + Arrays.toString(acceptedValues)
            + ")",
        Arrays.asList(acceptedValues).contains(value));
  }
}
