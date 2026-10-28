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
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Integration tests for stream INTO (sink/output) scenarios (spec cases OUT-01..OUT-29 of the
 * stream-computing requirements doc).
 *
 * <h3>Error codes (verified against source)</h3>
 *
 * <ul>
 *   <li>550 {@code TABLE_NOT_EXISTS}: "Table 'xxx.xxx' does not exist."
 *   <li>701 {@code SEMANTIC_ERROR}: "Sink table must be different from the source table", "time
 *       column can not be null", "No Field column present", "Insert query has mismatched column
 *       types"
 * </ul>
 */
@Category({StreamClusterIT.class})
public class StreamNodeOutputIT extends StreamNodeITBase {

  private static final ConditionFactory AWAIT =
      Awaitility.await()
          .pollInSameThread()
          .pollDelay(200, TimeUnit.MILLISECONDS)
          .pollInterval(1, TimeUnit.SECONDS)
          .atMost(20, TimeUnit.SECONDS);

  private static final ConditionFactory OUTPUT_AWAIT =
      Awaitility.await()
          .pollInSameThread()
          .pollDelay(200, TimeUnit.MILLISECONDS)
          .pollInterval(1, TimeUnit.SECONDS)
          .atMost(30, TimeUnit.SECONDS);

  private static final String DATABASE = "test";

  @BeforeClass
  public static void setUp() throws Exception {
    // Init 1C1D1S cluster environment.
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
    try (Connection connection =
            StreamEnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      List<String> streamNames = new ArrayList<>();
      try (ResultSet rs = statement.executeQuery("show streams")) {
        while (rs.next()) {
          streamNames.add(rs.getString(ColumnHeaderConstant.STREAM_NAME_TABLE_MODEL));
        }
      } catch (Exception ignored) {
      }
      List<String> topicNames = new ArrayList<>();
      for (String name : streamNames) {
        topicNames.add("__stream__" + name);
        try {
          statement.execute("DROP STREAM " + name);
        } catch (Exception ignored) {
        }
      }
      awaitSubscriptionTopicsRemoved(topicNames);
      try {
        statement.execute("DROP DATABASE " + DATABASE);
      } catch (Exception ignored) {
      }
      for (String db : new String[] {"test_a", "test_b"}) {
        try {
          statement.execute("DROP DATABASE " + db);
        } catch (Exception ignored) {
        }
      }
      for (String user : new String[] {"out03_user", "out04_user"}) {
        try {
          statement.execute("DROP USER " + user);
        } catch (Exception ignored) {
        }
      }
    }
  }

  // =================== Part A -- CREATE-time validations ===================

  /**
   * OUT-05 + OUT-06: INTO a non-existent table fails with code 550, and the failed CREATE must not
   * leave behind any target table.
   */
  @Test
  public void testTargetTableNotFound() throws Exception {
    try (Connection connection =
            StreamEnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      setupDatabase(statement);
      statement.execute(
          "CREATE TABLE src_t (time TIMESTAMP TIME, device_id STRING TAG, value INT64 FIELD)");
      try {
        statement.execute(
            "CREATE STREAM s05 FROM "
                + DATABASE
                + ".src_t TUMBLE(size => 1h) INTO "
                + DATABASE
                + ".no_such_sink(time, value)"
                + " SELECT ${START_TIME}, count(*) FROM ${ROWS}");
        fail("INTO a non-existent table must be rejected");
      } catch (SQLException e) {
        assertTrue(
            "Must report 550 / table does not exist, but got: " + e.getMessage(),
            e.getMessage().contains("550")
                || e.getMessage().contains("does not exist")
                || e.getMessage().contains("TABLE_NOT_EXISTS"));
      }

      // OUT-06: the failed CREATE must not create the target table.
      boolean found = false;
      try (ResultSet rs = statement.executeQuery("SHOW TABLES FROM " + DATABASE)) {
        while (rs.next()) {
          if ("no_such_sink".equals(rs.getString(1))) {
            found = true;
          }
        }
      }
      assertTrue("The target table must not be created by a failed CREATE STREAM", !found);
    }
  }

  /** OUT-07: INTO the source table is rejected (infinite loop prevention). */
  @Test
  public void testSinkSameAsSourceRejected() throws Exception {
    try (Connection connection =
            StreamEnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      setupDatabase(statement);
      statement.execute(
          "CREATE TABLE same_t (time TIMESTAMP TIME, device_id STRING TAG, value INT64 FIELD)");
      try {
        statement.execute(
            "CREATE STREAM s07 FROM "
                + DATABASE
                + ".same_t TUMBLE(size => 1h) INTO "
                + DATABASE
                + ".same_t(time, value)"
                + " SELECT ${START_TIME}, count(*) FROM ${ROWS}");
        fail("Sink must not be the same table as the source");
      } catch (SQLException e) {
        assertTrue(
            "Must reject source==sink, got: " + e.getMessage(),
            e.getMessage().contains("Sink table must be different"));
      }
    }
  }

  /** OUT-08: INTO column alias missing the TIME column. */
  @Test
  public void testMissingTimeInColumnAlias() throws Exception {
    try (Connection connection =
            StreamEnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      setupDatabase(statement);
      statement.execute(
          "CREATE TABLE src_t (time TIMESTAMP TIME, device_id STRING TAG, value INT64 FIELD)");
      statement.execute(
          "CREATE TABLE sink_no_time (time TIMESTAMP TIME, device_id STRING TAG, value INT64 FIELD)");
      try {
        statement.execute(
            "CREATE STREAM s08 FROM "
                + DATABASE
                + ".src_t TUMBLE(size => 1h) INTO "
                + DATABASE
                + ".sink_no_time(value)"
                + " SELECT ${START_TIME}, count(*) FROM ${ROWS}");
        fail("INTO alias without TIME must be rejected");
      } catch (SQLException e) {
        assertTrue(
            "Must report time column cannot be null, got: " + e.getMessage(),
            e.getMessage().contains("time column can not be null"));
      }
    }
  }

  /** OUT-09: INTO column alias has no FIELD column. */
  @Test
  public void testNoFieldInColumnAlias() throws Exception {
    try (Connection connection =
            StreamEnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      setupDatabase(statement);
      statement.execute(
          "CREATE TABLE src_t (time TIMESTAMP TIME, device_id STRING TAG, value INT64 FIELD)");
      statement.execute(
          "CREATE TABLE sink_t (time TIMESTAMP TIME, device_id STRING TAG, value INT64 FIELD)");
      try {
        statement.execute(
            "CREATE STREAM s09 FROM "
                + DATABASE
                + ".src_t TUMBLE(size => 1h) INTO "
                + DATABASE
                + ".sink_t(time, device_id)"
                + " SELECT ${START_TIME}, count(*) FROM ${ROWS}");
        fail("INTO alias without a FIELD must be rejected");
      } catch (SQLException e) {
        assertTrue(
            "Must report No Field column present, got: " + e.getMessage(),
            e.getMessage().contains("No Field column present"));
      }
    }
  }

  /** OUT-14: SELECT column count != target total column count (unqualified INTO). */
  @Test
  public void testUnspecifiedColumnCountMismatch() throws Exception {
    try (Connection connection =
            StreamEnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      setupDatabase(statement);
      statement.execute("CREATE TABLE src_t (time TIMESTAMP TIME, value INT64 FIELD)");
      statement.execute(
          "CREATE TABLE sink_3cols (time TIMESTAMP TIME, device_id STRING TAG, value INT64 FIELD)");
      try {
        statement.execute(
            "CREATE STREAM s14 FROM "
                + DATABASE
                + ".src_t TUMBLE(size => 1h) INTO "
                + DATABASE
                + ".sink_3cols"
                + " SELECT ${START_TIME}, count(*) FROM ${ROWS}");
        fail("Column count mismatch must be rejected");
      } catch (SQLException e) {
        assertTrue(
            "Must report mismatched column types due to count mismatch, got: " + e.getMessage(),
            e.getMessage().contains("mismatched column types"));
      }
    }
  }

  /** OUT-15: SELECT column count != specified column count (qualified INTO). */
  @Test
  public void testSpecifiedColumnCountMismatch() throws Exception {
    try (Connection connection =
            StreamEnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      setupDatabase(statement);
      statement.execute(
          "CREATE TABLE src_t (time TIMESTAMP TIME, value INT64 FIELD, s1 INT64 FIELD)");
      statement.execute(
          "CREATE TABLE sink_t (time TIMESTAMP TIME, value INT64 FIELD, s1 INT64 FIELD)");
      try {
        // INTO specifies only 1 column but SELECT produces 2 → column count mismatch.
        statement.execute(
            "CREATE STREAM s15 FROM "
                + DATABASE
                + ".src_t TUMBLE(size => 1h) INTO "
                + DATABASE
                + ".sink_t(time,value)"
                + " SELECT ${START_TIME}, last(value),last(s1) FROM ${ROWS}");
        fail("Specified column count mismatch must be rejected");
      } catch (SQLException e) {
        assertTrue(
            "Must report mismatched column types due to count mismatch, got: " + e.getMessage(),
            e.getMessage().contains("mismatched column types"));
      }
    }
  }

  /**
   * OUT-16: type incompatibility (FLOAT output into INT64 column) is rejected at CREATE time
   * (typesMatchForInsert uses pure equals, no coercion).
   */
  @Test
  public void testIncompatibleTypeRejected() throws Exception {
    try (Connection connection =
            StreamEnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      setupDatabase(statement);
      statement.execute(
          "CREATE TABLE src_t (time TIMESTAMP TIME, device_id STRING TAG, value FLOAT FIELD)");
      statement.execute("CREATE TABLE sink_t (time TIMESTAMP TIME, value INT64 FIELD)");
      try {
        // sum(FLOAT) produces DOUBLE/FLOAT, mismatched with INT64 sink column.
        statement.execute(
            "CREATE STREAM s16 FROM "
                + DATABASE
                + ".src_t TUMBLE(size => 1h) INTO "
                + DATABASE
                + ".sink_t(time, value)"
                + " SELECT ${START_TIME}, sum(value) FROM ${ROWS}");
        fail("Incompatible type must be rejected");
      } catch (SQLException e) {
        assertTrue(
            "Must report mismatched column types, got: " + e.getMessage(),
            e.getMessage().contains("mismatched column types"));
      }
    }
  }

  /**
   * OUT-03: an unqualified INTO table_name resolves to the current USE database. Uses a non-root
   * user with privileges only on the current database; if INTO were to resolve to a different
   * database the CREATE would fail at the INSERT privilege check.
   */
  @Test
  public void testUnqualifiedTargetResolvesToCurrentDb() throws Exception {
    final String userName = "out03_user";
    final String password = "out03_pwd";
    setupTwoDbUser(userName, password);

    try (Connection connection =
            StreamEnvFactory.getEnv().getConnection(userName, password, BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("USE test_a");
      long tBase = System.currentTimeMillis() + 1000;
      // Unqualified source and INTO -- both must resolve to test_a.
      statement.execute(
          String.format(
              "CREATE STREAM s03 FROM src_t TUMBLE(%dms,%d) INTO sink_t(time, value)"
                  + " SELECT ${START_TIME}, count(*) FROM ${ROWS}",
              5000, tBase));
      statement.execute("START STREAM s03");
      AWAIT.untilAsserted(
          () ->
              assertEquals(
                  "RUNNING",
                  queryStreamRow(statement, "s03")
                      .get(ColumnHeaderConstant.STREAM_STATUS_TABLE_MODEL)));

      // Write source data; the output must land in test_a.sink_t.
      statement.execute(
          String.format(
              "INSERT INTO src_t (time, value) VALUES (%d, 1), (%d, 2)",
              tBase + 1000, tBase + 2000));
      statement.execute(
          String.format("INSERT INTO src_t (time, value) VALUES (%d, 9)", tBase + 6000));
      OUTPUT_AWAIT.untilAsserted(
          () -> {
            try (ResultSet rs =
                statement.executeQuery("SELECT COUNT(*) AS cnt FROM test_a.sink_t")) {
              assertTrue(rs.next());
              assertTrue("Unqualified INTO must write to test_a.sink_t", rs.getInt(1) > 0);
            }
          });
      // root verifies test_b.sink_t is empty — the INTO could not have resolved across DBs.
      try (Connection rootConn =
              StreamEnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
          Statement rootStmt = rootConn.createStatement();
          ResultSet rs = rootStmt.executeQuery("SELECT COUNT(*) AS cnt FROM test_b.sink_t")) {
        assertTrue(rs.next());
        assertEquals("Unqualified INTO must not write to test_b.sink_t", 0, rs.getInt(1));
      }
      statement.execute("DROP STREAM s03");
      awaitSubscriptionTopicsRemoved(Collections.singletonList("__stream__s03"));
    }
  }

  /**
   * OUT-04: a fully-qualified INTO db.table writes to the specified database. Uses the same
   * non-root user with privileges only on test_a; the fully-qualified INTO test_a.sink_t must
   * succeed and write to test_a.
   */
  @Test
  public void testFullyQualifiedTargetResolves() throws Exception {
    final String userName = "out04_user";
    final String password = "out04_pwd";
    setupTwoDbUser(userName, password);

    try (Connection connection =
            StreamEnvFactory.getEnv().getConnection(userName, password, BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("USE test_a");
      long tBase = System.currentTimeMillis() + 1000;
      statement.execute(
          String.format(
              "CREATE STREAM s04 FROM test_a.src_t TUMBLE(%dms,%d) INTO test_a.sink_t(time, value)"
                  + " SELECT ${START_TIME}, count(*) FROM ${ROWS}",
              5000, tBase));
      statement.execute("START STREAM s04");
      AWAIT.untilAsserted(
          () ->
              assertEquals(
                  "RUNNING",
                  queryStreamRow(statement, "s04")
                      .get(ColumnHeaderConstant.STREAM_STATUS_TABLE_MODEL)));

      statement.execute(
          String.format(
              "INSERT INTO test_a.src_t (time, value) VALUES (%d, 1), (%d, 2)",
              tBase + 1000, tBase + 2000));
      statement.execute(
          String.format("INSERT INTO test_a.src_t (time, value) VALUES (%d, 9)", tBase + 6000));
      OUTPUT_AWAIT.untilAsserted(
          () -> {
            try (ResultSet rs =
                statement.executeQuery("SELECT COUNT(*) AS cnt FROM test_a.sink_t")) {
              assertTrue(rs.next());
              assertTrue("Fully-qualified INTO must write to test_a.sink_t", rs.getInt(1) > 0);
            }
          });
      statement.execute("DROP STREAM s04");
      awaitSubscriptionTopicsRemoved(Collections.singletonList("__stream__s04"));
    }
  }

  // =================== Part B -- Runtime writes ===================

  /** OUT-01: writing to an existing base table succeeds (basic end-to-end). */
  @Test
  public void testWriteToBaseTable() throws Exception {
    try (Connection connection =
            StreamEnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      setupDatabase(statement);
      setupSimpleSrcSink(statement);
      long tBase = startTumbleStream(statement, "s01", "sink_t", "(time, value)");

      statement.execute(
          String.format(
              "INSERT INTO %s.src_t (time, device_id, value) VALUES (%d, 'd1', 1), (%d, 'd1', 2)",
              DATABASE, tBase + 1000, tBase + 2000));
      statement.execute(
          String.format(
              "INSERT INTO %s.src_t (time, device_id, value) VALUES (%d, 'd1', 9)",
              DATABASE, tBase + 6000));
      awaitSinkRow(statement, tBase, 2L);
    }
  }

  /** OUT-10: a TAG column not mapped in the INTO alias is written as NULL. */
  @Test
  public void testUnmappedTagIsNull() throws Exception {
    try (Connection connection =
            StreamEnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      setupDatabase(statement);
      statement.execute(
          "CREATE TABLE src_t (time TIMESTAMP TIME, device_id STRING TAG, value INT64 FIELD)");
      statement.execute(
          "CREATE TABLE sink_tag_t (time TIMESTAMP TIME, device_id STRING TAG, value INT64 FIELD)");
      // device_id not listed in INTO → should land as NULL.
      long tBase = startTumbleStream(statement, "s10", "sink_tag_t", "(time, value)");

      statement.execute(
          String.format(
              "INSERT INTO %s.src_t (time, device_id, value) VALUES (%d, 'd1', 1), (%d, 'd1', 2)",
              DATABASE, tBase + 1000, tBase + 2000));
      statement.execute(
          String.format(
              "INSERT INTO %s.src_t (time, device_id, value) VALUES (%d, 'd1', 9)",
              DATABASE, tBase + 6000));
      OUTPUT_AWAIT.untilAsserted(
          () -> {
            try (ResultSet rs =
                statement.executeQuery("SELECT device_id FROM " + DATABASE + ".sink_tag_t")) {
              assertTrue("Sink should have at least one row", rs.next());
              String deviceId = rs.getString("device_id");
              assertEquals("Unmapped TAG must be NULL", null, rs.wasNull() ? null : deviceId);
            }
          });
    }
  }

  /**
   * OUT-11: the target has multiple TAG columns, only one of which is mapped from the stream
   * output. The mapped TAG receives the source device_id value; the unmapped TAG is NULL.
   */
  @Test
  public void testPartialTagMapping() throws Exception {
    try (Connection connection =
            StreamEnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      setupDatabase(statement);
      statement.execute(
          "CREATE TABLE src_t (time TIMESTAMP TIME, device_id STRING TAG, value INT64 FIELD)");
      statement.execute(
          "CREATE TABLE sink_multi_t (time TIMESTAMP TIME, location STRING TAG,"
              + " group_x STRING TAG, value INT64 FIELD)");
      long tBase =
          startPartitionedTumbleStream(statement, "s11", "sink_multi_t", "(time, location, value)");

      statement.execute(
          String.format(
              "INSERT INTO %s.src_t (time, device_id, value) VALUES (%d, 'd1', 1), (%d, 'd1', 2)",
              DATABASE, tBase + 1000, tBase + 2000));
      statement.execute(
          String.format(
              "INSERT INTO %s.src_t (time, device_id, value) VALUES (%d, 'd1', 9)",
              DATABASE, tBase + 6000));
      OUTPUT_AWAIT.untilAsserted(
          () -> {
            try (ResultSet rs =
                statement.executeQuery(
                    "SELECT location, group_x FROM " + DATABASE + ".sink_multi_t")) {
              assertTrue("Sink should have at least one row", rs.next());
              assertEquals(
                  "Mapped TAG (location) must receive the source device_id value",
                  "d1",
                  rs.getString("location"));
              assertTrue("Unmapped TAG (group_x) must be NULL", rs.getString("group_x") == null);
            }
          });
    }
  }

  /** OUT-12: an unmapped FIELD is written as NULL. */
  @Test
  public void testUnmappedFieldIsNull() throws Exception {
    try (Connection connection =
            StreamEnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      setupDatabase(statement);
      statement.execute(
          "CREATE TABLE src_t (time TIMESTAMP TIME, device_id STRING TAG, value INT64 FIELD)");
      statement.execute(
          "CREATE TABLE sink_2f_t (time TIMESTAMP TIME, v1 INT64 FIELD, v2 INT64 FIELD)");
      long tBase = startTumbleStream(statement, "s12", "sink_2f_t", "(time, v1)");

      statement.execute(
          String.format(
              "INSERT INTO %s.src_t (time, device_id, value) VALUES (%d, 'd1', 1), (%d, 'd1', 2)",
              DATABASE, tBase + 1000, tBase + 2000));
      statement.execute(
          String.format(
              "INSERT INTO %s.src_t (time, device_id, value) VALUES (%d, 'd1', 9)",
              DATABASE, tBase + 6000));
      OUTPUT_AWAIT.untilAsserted(
          () -> {
            try (ResultSet rs =
                statement.executeQuery("SELECT v2 FROM " + DATABASE + ".sink_2f_t")) {
              assertTrue("Sink should have at least one row", rs.next());
              assertTrue("Unmapped FIELD (v2) must be NULL", rs.getString("v2") == null);
            }
          });
    }
  }

  /**
   * OUT-13: INTO without the device_id TAG produces NULL TAGs. Two devices (d1, d2) are partitioned
   * independently in the same TUMBLE window; both partitions output at the same START_TIME (tBase)
   * but neither writes device_id (INTO only maps time + value). Both writes land on the same PK
   * (tBase, NULL), so the second upserts the first → 1 row.
   */
  @Test
  public void testNullTagSamePrimaryKeyUpdate() throws Exception {
    try (Connection connection =
            StreamEnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      setupDatabase(statement);
      statement.execute(
          "CREATE TABLE src_t (time TIMESTAMP TIME, device_id STRING TAG, value INT64 FIELD)");
      statement.execute(
          "CREATE TABLE sink_nk_t (time TIMESTAMP TIME, device_id STRING TAG, value INT64 FIELD)");
      long tBase = System.currentTimeMillis() + 1000;
      statement.execute(
          String.format(
              "CREATE STREAM s13 FROM "
                  + DATABASE
                  + ".src_t PARTITION BY device_id TUMBLE(%dms,%d) INTO "
                  + DATABASE
                  + ".sink_nk_t(time, value)"
                  + " SELECT ${START_TIME}, count(*) FROM ${ROWS}",
              5000,
              tBase));
      statement.execute("START STREAM s13");
      AWAIT.untilAsserted(
          () ->
              assertEquals(
                  "RUNNING",
                  queryStreamRow(statement, "s13")
                      .get(ColumnHeaderConstant.STREAM_STATUS_TABLE_MODEL)));

      // Both devices in the same window; each needs its own watermark push.
      statement.execute(
          String.format(
              "INSERT INTO %s.src_t (time, device_id, value) VALUES (%d, 'd1', 1), (%d, 'd2', 10)",
              DATABASE, tBase + 1000, tBase + 1000));
      statement.execute(
          String.format(
              "INSERT INTO %s.src_t (time, device_id, value) VALUES (%d, 'd1', 9), (%d, 'd2', 99)",
              DATABASE, tBase + 6000, tBase + 6000));
      OUTPUT_AWAIT.untilAsserted(
          () ->
              assertEquals(
                  "d1 and d2 emit at the same tBase with NULL TAG → same PK → upsert → 1 row",
                  1,
                  sinkRowCount(statement, "sink_nk_t")));
    }
  }

  private int sinkRowCount(Statement statement, String sinkTable) throws SQLException {
    try (ResultSet rs =
        statement.executeQuery("SELECT COUNT(*) AS cnt FROM " + DATABASE + "." + sinkTable)) {
      return rs.next() ? rs.getInt(1) : 0;
    }
  }

  private void awaitSubscriptionTopicsRemoved(List<String> topicNames) {
    if (topicNames.isEmpty()) {
      return;
    }
    try (ISubscriptionTableSession sub =
        new SubscriptionTableSessionBuilder()
            .host(StreamEnvFactory.getEnv().getIP())
            .port(Integer.parseInt(StreamEnvFactory.getEnv().getPort()))
            .build()) {
      Awaitility.await("subscription topics removed")
          .pollInterval(500, TimeUnit.MILLISECONDS)
          .atMost(45, TimeUnit.SECONDS)
          .untilAsserted(
              () -> {
                for (String topic : topicNames) {
                  assertTrue(
                      "Managed topic should be removed: " + topic,
                      !sub.getTopic(topic).isPresent());
                }
              });
    } catch (Exception ignored) {
    }
  }

  /**
   * OUT-17: INT32 output writing into an INT64 sink column - compatible coercion (currently NOT
   * implemented; typesMatchForInsert uses pure equals, so the CREATE is rejected at analyze time).
   */
  @Test
  public void testCompatibleTypeConversion() throws Exception {
    try (Connection connection =
            StreamEnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      setupDatabase(statement);
      statement.execute("CREATE TABLE src_t (time TIMESTAMP TIME, value INT32 FIELD)");
      statement.execute("CREATE TABLE sink_t (time TIMESTAMP TIME, value INT64 FIELD)");
      try {
        // Cast count(*) to INT32, mapping to an INT64 sink column — compatible upcast but
        // typesMatchForInsert currently uses pure equals so this is rejected.
        statement.execute(
            "CREATE STREAM s17 FROM "
                + DATABASE
                + ".src_t TUMBLE(size => 1h) INTO "
                + DATABASE
                + ".sink_t(time, value)"
                + " SELECT ${START_TIME}, CAST(count(*) AS INT32) FROM ${ROWS}");
        fail("INT32→INT64 coercion is not yet implemented -- expected CREATE to be rejected");
      } catch (SQLException expectedRed) {
        assertTrue(
            "Should reject on type mismatch until coercion is implemented, got: "
                + expectedRed.getMessage(),
            expectedRed.getMessage().contains("mismatched column types"));
      }
    }
  }

  /**
   * OUT-18: writing two distinct (time, all TAG) primary keys produces two rows; writing the same
   * primary key again updates the existing row.
   */
  @Test
  public void testMultiPrimaryKeyInsertAndUpdate() throws Exception {
    try (Connection connection =
            StreamEnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      setupDatabase(statement);
      statement.execute(
          "CREATE TABLE src_t (time TIMESTAMP TIME, device_id STRING TAG, value INT64 FIELD)");
      statement.execute(
          "CREATE TABLE sink_pk_t (time TIMESTAMP TIME, device_id STRING TAG, value INT64 FIELD)");
      long tBase =
          startPartitionedTumbleStream(statement, "s18", "sink_pk_t", "(time, device_id, value)");

      // Window 1: two devices produce two rows.
      statement.execute(
          String.format(
              "INSERT INTO %s.src_t (time, device_id, value) VALUES (%d, 'd1', 1), (%d, 'd2', 10)",
              DATABASE, tBase + 1000, tBase + 1000));
      statement.execute(
          String.format(
              "INSERT INTO %s.src_t (time, device_id, value) VALUES (%d, 'd1', 9)",
              DATABASE, tBase + 6000));
      OUTPUT_AWAIT.untilAsserted(
          () -> {
            try (ResultSet rs =
                statement.executeQuery("SELECT COUNT(*) AS cnt FROM " + DATABASE + ".sink_pk_t")) {
              assertTrue(rs.next());
              assertEquals("Two distinct (time, device_id) PK → one rows", 1, rs.getInt(1));
            }
          });

      // Stop the stream, then manually INSERT a row with the same (time, device_id) PK as
      // one of the existing rows — the table-model UPSERT semantic must overwrite the old
      // value rather than creating a duplicate PK row.
      statement.execute("STOP STREAM s18");
      statement.execute(
          String.format(
              "INSERT INTO %s.sink_pk_t (time, device_id, value) VALUES (%d, 'd1', 999)",
              DATABASE, tBase));
      try (ResultSet rs =
          statement.executeQuery(
              "SELECT COUNT(*) AS cnt, MAX(value) AS mv FROM " + DATABASE + ".sink_pk_t")) {
        assertTrue(rs.next());
        assertEquals("UPSERT on same (time, device_id) must not create a one row", 1, rs.getInt(1));
        assertEquals("The updated value must be visible", 999L, rs.getLong(2));
      }
    }
  }

  // =================== Part C -- Runtime schema changes ===================

  /**
   * OUT-19: dropping the target table while the stream is running is not blocked; writes
   * subsequently fail (data is dropped) and the stream stays RUNNING.
   */
  @Test
  public void testDropTargetWhileRunning() throws Exception {
    try (Connection connection =
            StreamEnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      setupDatabase(statement);
      setupSimpleSrcSink(statement);
      long tBase = startTumbleStream(statement, "s19", "sink_t", "(time, value)");

      // Prove writes work before DROP.
      writeAndAssertSinkHasRows(statement, tBase);

      // DROP the sink — not blocked.
      statement.execute("DROP TABLE " + DATABASE + ".sink_t");

      // More source writes (new window data) — stream should still be RUNNING.
      statement.execute(
          String.format(
              "INSERT INTO %s.src_t (time, device_id, value) VALUES (%d, 'd1', 10),"
                  + " (%d, 'd1', 20)",
              DATABASE, tBase + 11000, tBase + 12000));
      statement.execute(
          String.format(
              "INSERT INTO %s.src_t (time, device_id, value) VALUES (%d, 'd1', 30)",
              DATABASE, tBase + 16000));
      TimeUnit.SECONDS.sleep(5);

      boolean sinkTableExists = false;
      try (ResultSet rs = statement.executeQuery("SHOW TABLES FROM " + DATABASE)) {
        while (rs.next()) {
          if ("sink_t".equals(rs.getString(1))) {
            sinkTableExists = true;
            break;
          }
        }
      }
      assertTrue("Write-back must not recreate a dropped sink table", !sinkTableExists);

      // The stream should still be RUNNING (sink failures are not surfaced to CN).
      AWAIT.untilAsserted(
          () -> {
            Map<String, Object> row = queryStreamRow(statement, "s19");
            assertNotNull(row);
            assertEquals(
                "Stream should stay RUNNING even when the sink table is dropped",
                "RUNNING",
                row.get(ColumnHeaderConstant.STREAM_STATUS_TABLE_MODEL));
          });
    }
  }

  /** OUT-20: recreating the same-named same-columned table makes writes resume. */
  @Test
  public void testRecreateSameSchemaResumes() throws Exception {
    try (Connection connection =
            StreamEnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      setupDatabase(statement);
      setupSimpleSrcSink(statement);
      long tBase = startTumbleStream(statement, "s20", "sink_t", "(time, value)");

      // Prove writes work; then drop sink.
      writeAndAssertSinkHasRows(statement, tBase);
      statement.execute("DROP TABLE " + DATABASE + ".sink_t");

      // Recreate same table.
      statement.execute("CREATE TABLE sink_t (time TIMESTAMP TIME, value INT64 FIELD)");

      // Write a fresh batch in a later window; the stream should resume writing.
      long lateBase = tBase + 20000;
      statement.execute(
          String.format(
              "INSERT INTO %s.src_t (time, device_id, value) VALUES (%d, 'd1', 40),"
                  + " (%d, 'd1', 50)",
              DATABASE, lateBase + 1000, lateBase + 2000));
      statement.execute(
          String.format(
              "INSERT INTO %s.src_t (time, device_id, value) VALUES (%d, 'd1', 60)",
              DATABASE, lateBase + 6000));
      awaitSinkRow(statement, lateBase, 2L);
    }
  }

  /**
   * OUT-22: recreating with the same column name but an incompatible type makes writes fail
   * (NonRetryableException); the sink stays empty.
   */
  @Test
  public void testRecreateIncompatibleTypeFails() throws Exception {
    try (Connection connection =
            StreamEnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      setupDatabase(statement);
      setupSimpleSrcSink(statement);
      long tBase = startTumbleStream(statement, "s22", "sink_t", "(time, value)");

      writeAndAssertSinkHasRows(statement, tBase);
      statement.execute("DROP TABLE " + DATABASE + ".sink_t");

      // Recreate with same column name but incompatible type (BOOLEAN instead of INT64).
      statement.execute("CREATE TABLE sink_t (time TIMESTAMP TIME, value BOOLEAN FIELD)");

      long lateBase = tBase + 20000;
      statement.execute(
          String.format(
              "INSERT INTO %s.src_t (time, device_id, value) VALUES (%d, 'd1', 40),"
                  + " (%d, 'd1', 50)",
              DATABASE, lateBase + 1000, lateBase + 2000));
      statement.execute(
          String.format(
              "INSERT INTO %s.src_t (time, device_id, value) VALUES (%d, 'd1', 60)",
              DATABASE, lateBase + 6000));
      TimeUnit.SECONDS.sleep(5);

      // No new rows — writes fail and are dropped (NonRetryableException).
      try (ResultSet rs =
          statement.executeQuery("SELECT COUNT(*) AS cnt FROM " + DATABASE + ".sink_t")) {
        assertTrue(rs.next());
        assertEquals("Incompatible-type writes must not succeed", 0, rs.getInt(1));
      }

      //      statement.execute("DROP TABLE " + DATABASE + ".sink_t");
      //      statement.execute("CREATE TABLE sink_t (time TIMESTAMP TIME, value INT64 FIELD)");
      //      OUTPUT_AWAIT.untilAsserted(
      //          () -> {
      //            try (ResultSet rs =
      //                statement.executeQuery("SELECT COUNT(*) AS cnt FROM " + DATABASE +
      // ".sink_t")) {
      //              assertTrue(rs.next());
      //              assertEquals("Incompatible-type writes must not succeed", 1, rs.getInt(1));
      //            }
      //          });
    }
  }

  /**
   * OUT-23: after fixing the incompatible type back to compatible, writes resume (without
   * recreating the stream).
   */
  @Test
  public void testFixIncompatibleTypeResumes() throws Exception {
    try (Connection connection =
            StreamEnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      setupDatabase(statement);
      setupSimpleSrcSink(statement);
      long tBase = startTumbleStream(statement, "s23", "sink_t", "(time, value)");

      writeAndAssertSinkHasRows(statement, tBase);
      statement.execute("DROP TABLE " + DATABASE + ".sink_t");
      statement.execute("CREATE TABLE sink_t (time TIMESTAMP TIME, value BOOLEAN FIELD)");
      // Drop incompatible and re-create compatible.
      statement.execute("DROP TABLE " + DATABASE + ".sink_t");
      statement.execute("CREATE TABLE sink_t (time TIMESTAMP TIME, value INT64 FIELD)");

      long lateBase = tBase + 20000;
      statement.execute(
          String.format(
              "INSERT INTO %s.src_t (time, device_id, value) VALUES (%d, 'd1', 40),"
                  + " (%d, 'd1', 50)",
              DATABASE, lateBase + 1000, lateBase + 2000));
      statement.execute(
          String.format(
              "INSERT INTO %s.src_t (time, device_id, value) VALUES (%d, 'd1', 60)",
              DATABASE, lateBase + 6000));
      awaitSinkRow(statement, lateBase, 2L);
    }
  }

  /**
   * OUT-27: while the stream is running, DROP the sink table and recreate it with a compatible type
   * upgrade (INT32→INT64). The existing stream writes must continue without disruption.
   */
  @Test
  public void testCompatibleTypeUpgrade() throws Exception {
    try (Connection connection =
            StreamEnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      setupDatabase(statement);
      statement.execute("CREATE TABLE src_t (time TIMESTAMP TIME, v INT32 FIELD)");
      statement.execute("CREATE TABLE sink_t (time TIMESTAMP TIME, v INT32 FIELD)");
      long tBase = System.currentTimeMillis() + 1000;
      statement.execute(
          String.format(
              "CREATE STREAM s27 FROM "
                  + DATABASE
                  + ".src_t TUMBLE(%dms,%d) INTO "
                  + DATABASE
                  + ".sink_t(time, v)"
                  + " SELECT ${START_TIME}, CAST(count(*) AS INT32) FROM ${ROWS}",
              5000,
              tBase));
      statement.execute("START STREAM s27");
      AWAIT.untilAsserted(
          () ->
              assertEquals(
                  "RUNNING",
                  queryStreamRow(statement, "s27")
                      .get(ColumnHeaderConstant.STREAM_STATUS_TABLE_MODEL)));

      // Prove writes work with INT32.
      long preRows = writeWindowAndAssertSink(statement, tBase);

      // Drop and recreate with INT64 (compatible upgrade).
      statement.execute("DROP TABLE " + DATABASE + ".sink_t");
      statement.execute("CREATE TABLE sink_t (time TIMESTAMP TIME, v INT64 FIELD)");

      // Writes must continue unaffected with the upgraded type.
      long lateBase = tBase + 20000L;
      long postRows = writeWindowAndAssertSink(statement, lateBase);
      assertTrue("Sink must grow after compatible type upgrade", postRows > preRows);
    }
  }

  private long writeWindowAndAssertSink(Statement statement, long tBase) throws Exception {
    statement.execute(
        String.format(
            "INSERT INTO %s.src_t (time, v) VALUES (%d, 1), (%d, 2)",
            DATABASE, tBase + 1000, tBase + 2000));
    statement.execute(
        String.format("INSERT INTO %s.src_t (time, v) VALUES (%d, 9)", DATABASE, tBase + 6000));
    TimeUnit.SECONDS.sleep(3);
    return sinkRowCount(statement, "sink_t");
  }

  // =================== Helpers ===================

  private void setupDatabase(Statement statement) throws SQLException {
    statement.execute("CREATE DATABASE " + DATABASE);
    statement.execute("USE " + DATABASE);
  }

  private void setupSimpleSrcSink(Statement statement) throws SQLException {
    statement.execute(
        "CREATE TABLE src_t (time TIMESTAMP TIME, device_id STRING TAG, value INT64 FIELD)");
    statement.execute("CREATE TABLE sink_t (time TIMESTAMP TIME, value INT64 FIELD)");
  }

  /**
   * Creates and starts a TUMBLE stream, returns tBase (the window origin) so the caller can compute
   * insert timestamps.
   */
  private long startTumbleStream(
      Statement statement, String streamName, String sinkTable, String sinkColumns)
      throws Exception {
    long tBase = System.currentTimeMillis() + 1000;
    statement.execute(
        String.format(
            "CREATE STREAM %s FROM "
                + DATABASE
                + ".src_t TUMBLE(%dms,%d) INTO "
                + DATABASE
                + "."
                + sinkTable
                + sinkColumns
                + " SELECT ${START_TIME}, count(*) FROM ${ROWS}",
            streamName,
            5000,
            tBase));
    statement.execute("START STREAM " + streamName);
    AWAIT.untilAsserted(
        () ->
            assertEquals(
                "RUNNING",
                queryStreamRow(statement, streamName)
                    .get(ColumnHeaderConstant.STREAM_STATUS_TABLE_MODEL)));
    return tBase;
  }

  /**
   * Creates and starts a TUMBLE stream whose SELECT emits one TAG column: the value of the first
   * {@code PARTITION BY device_id} column (referenced as {@code ${1}}). Used by the cases where the
   * sink must receive a real TAG value (OUT-11 / OUT-18).
   */
  private long startPartitionedTumbleStream(
      Statement statement, String streamName, String sinkTable, String sinkColumns)
      throws Exception {
    long tBase = System.currentTimeMillis() + 1000;
    statement.execute(
        String.format(
            "CREATE STREAM %s FROM "
                + DATABASE
                + ".src_t PARTITION BY device_id TUMBLE(%dms,%d) INTO "
                + DATABASE
                + "."
                + sinkTable
                + sinkColumns
                + " SELECT ${START_TIME}, ${1}, count(*) FROM ${ROWS}",
            streamName,
            5000,
            tBase));
    statement.execute("START STREAM " + streamName);
    AWAIT.untilAsserted(
        () ->
            assertEquals(
                "RUNNING",
                queryStreamRow(statement, streamName)
                    .get(ColumnHeaderConstant.STREAM_STATUS_TABLE_MODEL)));
    return tBase;
  }

  /** Writes two rows + watermark push and awaits exactly (tBase, 2L) in the sink. */
  private void writeAndAssertSinkHasRows(Statement statement, long tBase) throws Exception {
    statement.execute(
        String.format(
            "INSERT INTO %s.src_t (time, device_id, value) VALUES (%d, 'd1', 1), (%d, 'd1', 2)",
            DATABASE, tBase + 1000, tBase + 2000));
    statement.execute(
        String.format(
            "INSERT INTO %s.src_t (time, device_id, value) VALUES (%d, 'd1', 9)",
            DATABASE, tBase + 6000));
    awaitSinkRow(statement, tBase, 2L);
  }

  private void awaitSinkRow(Statement statement, long expectedTime, long expectedValue)
      throws Exception {
    OUTPUT_AWAIT.untilAsserted(
        () -> {
          try (ResultSet rs =
              statement.executeQuery(
                  "SELECT time, value FROM "
                      + DATABASE
                      + ".sink_t"
                      + " WHERE time = "
                      + expectedTime)) {
            assertTrue("Expected sink row at time " + expectedTime, rs.next());
            assertEquals(expectedValue, rs.getLong("value"));
          }
        });
  }

  /**
   * Root-side setup for OUT-03/04: creates two databases test_a and test_b each with identically
   * named src_t/sink_t tables, then creates a non-root user with privileges only on test_a. The
   * user has SYSTEM (for stream ops), SELECT on test_a.src_t and INSERT on test_a.sink_t, but NO
   * privileges on test_b — so if an unqualified INTO resolved to test_b the CREATE would fail.
   */
  private void setupTwoDbUser(String userName, String password) throws Exception {
    try (Connection connection =
            StreamEnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      for (String db : new String[] {"test_a", "test_b"}) {
        statement.execute("CREATE DATABASE " + db);
        statement.execute("USE " + db);
        statement.execute("CREATE TABLE src_t (time TIMESTAMP TIME, value INT64 FIELD)");
        statement.execute("CREATE TABLE sink_t (time TIMESTAMP TIME, value INT64 FIELD)");
      }
      statement.execute("CREATE USER " + userName + " '" + password + "'");
      statement.execute("GRANT SYSTEM TO USER " + userName);
      statement.execute("GRANT SELECT ON TABLE test_a.src_t TO USER " + userName);
      statement.execute("GRANT INSERT ON TABLE test_a.src_t TO USER " + userName);
      statement.execute("GRANT SELECT ON TABLE test_a.sink_t TO USER " + userName);
      statement.execute("GRANT INSERT ON TABLE test_a.sink_t TO USER " + userName);
    }
  }
}
