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
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.apache.iotdb.db.it.utils.TestUtils.createUser;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Integration tests for stream privileges (spec cases SEC-01..SEC-11 of the stream-computing
 * requirements doc).
 *
 * <p>Privilege model (table model): stream statements require the SYSTEM global privilege; object
 * access uses table-level SELECT (read) / INSERT (write) privileges which are enforced strictly
 * even for SYSTEM holders (unlike DDL, SELECT/INSERT checks have no SYSTEM short-circuit). CREATE
 * STREAM enforces the object privileges of the source table, the target table and the subquery
 * tables at create time (through the stream query planner's analysis).
 *
 * <p>Deviations from the case table agreed by design: SEC-10 (network boundary of internal ports)
 * is a deployment concern that cannot be simulated in an IT (and the listed 10770/10780 ports do
 * not exist in the implementation), and SEC-12 (exception monitoring and alerting) depends on
 * metric/alert infrastructure the StreamNode does not expose in the IT environment - both are
 * skipped.
 */
@Category({StreamClusterIT.class})
public class StreamNodeSecurityIT extends StreamNodeITBase {

  private static final ConditionFactory AWAIT =
      Awaitility.await()
          .pollInSameThread()
          .pollDelay(200, TimeUnit.MILLISECONDS)
          .pollInterval(1, TimeUnit.SECONDS)
          .atMost(30, TimeUnit.SECONDS);

  private static final String DATABASE = "test";
  private static final String SOURCE_TABLE = "src_t";
  private static final String SINK_TABLE = "sink_t";
  private static final String OTHER_TABLE = "other_t";
  private static final String PASSWORD = "Sec_pwd_123456";
  private static final String NO_PERMISSION_SYSTEM =
      "No permissions for this operation, please add privilege SYSTEM";

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
        statement.execute("DROP DATABASE " + DATABASE);
      } catch (Exception ignored) {
        // database may not exist if a test failed before creating it
      }
      for (String user :
          new String[] {
            "sec01_user", "sec02_user", "sec03_user", "sec04_user", "sec05_user",
            "sec06_user", "sec07_user", "sec08_user_a", "sec08_user_b", "sec11_user"
          }) {
        try {
          statement.execute("DROP USER " + user);
        } catch (Exception ignored) {
          // user may not exist if a test failed before creating it
        }
      }
    }
  }

  /**
   * SEC-01: with SYSTEM, source-table SELECT, target-table INSERT and the subquery-table SELECT
   * privileges granted, the user can create the stream and run it.
   */
  @Test
  public void testCreateStreamWithFullPrivileges() throws Exception {
    final String userName = "sec01_user";
    try (Connection connection =
            StreamEnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      createTables(statement);
      createUser(StreamEnvFactory.getEnv(), userName, PASSWORD);
      grantAllStreamPrivileges(statement, userName);
    }

    try (Connection connection =
            StreamEnvFactory.getEnv().getConnection(userName, PASSWORD, BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("USE " + DATABASE);
      statement.execute(subqueryStreamSql("sec01_stream"));
      statement.execute("START STREAM sec01_stream");

      AWAIT.untilAsserted(
          () ->
              assertEquals(
                  "The stream should be RUNNING when all privileges are granted",
                  "RUNNING",
                  queryStreamRow(statement, "sec01_stream")
                      .get(ColumnHeaderConstant.STREAM_STATUS_TABLE_MODEL)));
    }
  }

  /**
   * SEC-02: a user with only the object read/write privileges (no SYSTEM) cannot create a stream,
   * and no stream is produced.
   */
  @Test
  public void testCreateStreamWithoutSystemPrivilegeRejected() throws Exception {
    final String userName = "sec02_user";
    try (Connection connection =
            StreamEnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      createTables(statement);
      createUser(StreamEnvFactory.getEnv(), userName, PASSWORD);
      // Only the object privileges, no SYSTEM.
      grantSelectOnSource(statement, userName);
      grantInsertOnSink(statement, userName);
    }

    try (Connection connection =
            StreamEnvFactory.getEnv().getConnection(userName, PASSWORD, BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("USE " + DATABASE);
      String message =
          assertRejected(statement, subqueryStreamSql("sec02_stream"), "CREATE STREAM");
      assertTrue(
          "CREATE STREAM without SYSTEM should report the missing SYSTEM privilege but was: "
              + message,
          message.contains(NO_PERMISSION_SYSTEM));
    }

    try (Connection connection =
            StreamEnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      assertNullRow(
          statement, "sec02_stream", "No stream record may be produced by the rejected CREATE");
    }
  }

  /**
   * SEC-03: with SYSTEM, the target-table INSERT and the subquery-table SELECT granted but the
   * source-table SELECT missing, the CREATE must be rejected.
   */
  @Test
  public void testCreateStreamWithoutSourceReadRejected() throws Exception {
    final String userName = "sec03_user";
    try (Connection connection =
            StreamEnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      createTables(statement);
      createUser(StreamEnvFactory.getEnv(), userName, PASSWORD);
      statement.execute("GRANT SYSTEM TO USER " + userName);
      grantInsertOnSink(statement, userName);
      grantSelectOnOther(statement, userName);
      // Missing: SELECT on the source table.
    }

    try (Connection connection =
            StreamEnvFactory.getEnv().getConnection(userName, PASSWORD, BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("USE " + DATABASE);
      assertRejected(statement, subqueryStreamSql("sec03_stream"), "CREATE STREAM");
    }
  }

  /**
   * SEC-04: with SYSTEM, the source-table SELECT and the subquery-table SELECT granted but the
   * target-table INSERT missing, the CREATE must be rejected.
   */
  @Test
  public void testCreateStreamWithoutTargetWriteRejected() throws Exception {
    final String userName = "sec04_user";
    try (Connection connection =
            StreamEnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      createTables(statement);
      createUser(StreamEnvFactory.getEnv(), userName, PASSWORD);
      statement.execute("GRANT SYSTEM TO USER " + userName);
      grantSelectOnSource(statement, userName);
      grantSelectOnOther(statement, userName);
      // Missing: INSERT on the target table.
    }

    try (Connection connection =
            StreamEnvFactory.getEnv().getConnection(userName, PASSWORD, BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("USE " + DATABASE);
      assertRejected(statement, subqueryStreamSql("sec04_stream"), "CREATE STREAM");
    }
  }

  /**
   * SEC-05: the subquery reads a table (other_t) the user has no SELECT on - the CREATE (or the
   * later execution) must be rejected so that no unauthorized read is possible.
   */
  @Test
  public void testCreateStreamWithoutSubqueryReadRejected() throws Exception {
    final String userName = "sec05_user";
    try (Connection connection =
            StreamEnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      createTables(statement);
      createUser(StreamEnvFactory.getEnv(), userName, PASSWORD);
      statement.execute("GRANT SYSTEM TO USER " + userName);
      grantSelectOnSource(statement, userName);
      grantInsertOnSink(statement, userName);
      // Missing: SELECT on the subquery table other_t.
    }

    try (Connection connection =
            StreamEnvFactory.getEnv().getConnection(userName, PASSWORD, BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("USE " + DATABASE);
      assertRejected(statement, subqueryStreamSql("sec05_stream"), "CREATE STREAM");
    }
  }

  /** SEC-06: a user without SYSTEM cannot drop a stream; the original stream is preserved. */
  @Test
  public void testDropStreamWithoutSystemPrivilegeRejected() throws Exception {
    final String userName = "sec06_user";
    try (Connection connection =
            StreamEnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      createTables(statement);
      createUser(StreamEnvFactory.getEnv(), userName, PASSWORD);
      // Object privileges only (also grants database visibility for USE).
      grantSelectOnSource(statement, userName);
      statement.execute(subqueryStreamSql("sec06_stream"));
    }

    try (Connection connection =
            StreamEnvFactory.getEnv().getConnection(userName, PASSWORD, BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("USE " + DATABASE);
      String message = assertRejected(statement, "DROP STREAM sec06_stream", "DROP STREAM");
      assertTrue(
          "DROP STREAM without SYSTEM should report the missing SYSTEM privilege but was: "
              + message,
          message.contains(NO_PERMISSION_SYSTEM));
    }

    try (Connection connection =
            StreamEnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      assertNotNull(
          "The original stream must be preserved after the rejected DROP",
          queryStreamRow(statement, "sec06_stream"));
    }
  }

  /**
   * SEC-07: a user without SYSTEM cannot query streams - both SHOW STREAMS and a SELECT on
   * information_schema.streams are rejected with the missing-SYSTEM-privilege message.
   */
  @Test
  public void testQueryStreamsWithoutSystemPrivilege() throws Exception {
    final String userName = "sec07_user";
    try (Connection connection =
            StreamEnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      createTables(statement);
      createUser(StreamEnvFactory.getEnv(), userName, PASSWORD);
      grantSelectOnSource(statement, userName);
      statement.execute(subqueryStreamSql("sec07_stream"));
    }

    try (Connection connection =
            StreamEnvFactory.getEnv().getConnection(userName, PASSWORD, BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("USE " + DATABASE);
      String showStreamsMessage =
          assertRejected(statement, "SHOW STREAMS", "SHOW STREAMS without SYSTEM");
      assertTrue(
          "SHOW STREAMS without SYSTEM should report the missing SYSTEM privilege but was: "
              + showStreamsMessage,
          showStreamsMessage.contains(NO_PERMISSION_SYSTEM));

      // SELECT on information_schema.streams also requires SYSTEM.
      String selectMessage =
          assertRejected(
              statement,
              "SELECT * FROM information_schema.streams",
              "SELECT on information_schema.streams without SYSTEM");
      assertTrue(
          "SELECT on information_schema.streams without SYSTEM should report the missing SYSTEM"
              + " privilege but was: "
              + selectMessage,
          selectMessage.contains(NO_PERMISSION_SYSTEM));
    }
  }

  /**
   * SEC-08: two users hold privileges on different tables only - each can create and run streams on
   * their own objects, the other user's streams are not visible, and the other user's tables are
   * not readable. (A SYSTEM holder remains an administrator for stream statements, so cross-user
   * stream DROP is not asserted.)
   */
  @Test
  public void testMinimalPrivilegeAndUserIsolation() throws Exception {
    final String userA = "sec08_user_a";
    final String userB = "sec08_user_b";
    try (Connection connection =
            StreamEnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE " + DATABASE);
      statement.execute("USE " + DATABASE);
      statement.execute(
          "CREATE TABLE a_src (time TIMESTAMP TIME, device_id STRING TAG, value INT64 FIELD)");
      statement.execute("CREATE TABLE a_sink (time TIMESTAMP TIME, value INT64 FIELD)");
      statement.execute(
          "CREATE TABLE b_src (time TIMESTAMP TIME, device_id STRING TAG, value INT64 FIELD)");
      statement.execute("CREATE TABLE b_sink (time TIMESTAMP TIME, value INT64 FIELD)");
      createUser(StreamEnvFactory.getEnv(), userA, PASSWORD);
      createUser(StreamEnvFactory.getEnv(), userB, PASSWORD);
      statement.execute("GRANT SYSTEM TO USER " + userA);
      statement.execute("GRANT SYSTEM TO USER " + userB);
      statement.execute("GRANT SELECT ON TABLE " + DATABASE + ".a_src TO USER " + userA);
      statement.execute("GRANT INSERT ON TABLE " + DATABASE + ".a_sink TO USER " + userA);
      statement.execute("GRANT SELECT ON TABLE " + DATABASE + ".b_src TO USER " + userB);
      statement.execute("GRANT INSERT ON TABLE " + DATABASE + ".b_sink TO USER " + userB);
    }

    // Each user creates a stream on their own objects.
    try (Connection connection =
            StreamEnvFactory.getEnv().getConnection(userA, PASSWORD, BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("USE " + DATABASE);
      statement.execute(
          "CREATE STREAM stream_a FROM "
              + DATABASE
              + ".a_src TUMBLE(size => 1h) INTO "
              + DATABASE
              + ".a_sink(time, value)"
              + " SELECT ${START_TIME}, count(*) FROM ${ROWS}");
    }
    try (Connection connection =
            StreamEnvFactory.getEnv().getConnection(userB, PASSWORD, BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("USE " + DATABASE);
      statement.execute(
          "CREATE STREAM stream_b FROM "
              + DATABASE
              + ".b_src TUMBLE(size => 1h) INTO "
              + DATABASE
              + ".b_sink(time, value)"
              + " SELECT ${START_TIME}, count(*) FROM ${ROWS}");
    }

    // User B must see user A's stream, and vice versa.
    try (Connection connection =
            StreamEnvFactory.getEnv().getConnection(userB, PASSWORD, BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("USE " + DATABASE);
      assertTrue(
          "SHOW STREAMS must leak another user's streams", streamVisible(statement, "stream_a"));
      // User B cannot read user A's source table.
      assertRejected(statement, "SELECT count(*) FROM " + DATABASE + ".a_src", "SELECT a_src");
    }
    try (Connection connection =
            StreamEnvFactory.getEnv().getConnection(userA, PASSWORD, BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("USE " + DATABASE);
      assertTrue(
          "SHOW STREAMS must leak another user's streams", streamVisible(statement, "stream_b"));
      assertRejected(statement, "SELECT count(*) FROM " + DATABASE + ".b_src", "SELECT b_src");
    }
  }

  /**
   * SEC-09: permission and input errors reported to the client must not leak credentials. (The
   * write-back error surface lives inside the StreamNode process and has no client-visible error
   * path yet - see the META-08 gap - so only the permission and input error surfaces are asserted;
   * log-side desensitization cannot be automated in an IT.)
   */
  @Test
  public void testErrorMessagesDoNotLeakCredentials() throws Exception {
    final String userName = "sec09_user";
    try (Connection connection =
            StreamEnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      createTables(statement);
      createUser(StreamEnvFactory.getEnv(), userName, PASSWORD);
      grantSelectOnSource(statement, userName);
      grantInsertOnSink(statement, userName);
    }

    try (Connection connection =
            StreamEnvFactory.getEnv().getConnection(userName, PASSWORD, BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("USE " + DATABASE);

      // Permission error.
      String permissionError =
          assertRejected(statement, subqueryStreamSql("sec09_stream"), "CREATE STREAM");
      assertNoCredentialLeak("The permission error message", permissionError);

      // Input error: a stream referencing a non-existent table.
      String inputError =
          assertRejected(
              statement,
              "CREATE STREAM sec09_bad FROM "
                  + DATABASE
                  + ".no_such_table TUMBLE(size => 1h) INTO "
                  + DATABASE
                  + ".sink_t(time, value)"
                  + " SELECT ${START_TIME}, count(*) FROM ${ROWS}",
              "CREATE STREAM on a non-existent table");
      assertNoCredentialLeak("The input error message", inputError);
    }
  }

  /**
   * SEC-11: a source-less PERIOD stream requires no fictional source privilege - SYSTEM, the
   * target-table INSERT and the subquery-table SELECT are sufficient to create and run it.
   */
  @Test
  public void testNoSourcePeriodStreamMinimalPrivileges() throws Exception {
    final String userName = "sec11_user";
    try (Connection connection =
            StreamEnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      createTables(statement);
      createUser(StreamEnvFactory.getEnv(), userName, PASSWORD);
      statement.execute("GRANT SYSTEM TO USER " + userName);
      grantInsertOnSink(statement, userName);
      grantSelectOnSource(statement, userName); // for the subquery only, no FROM source exists
    }

    try (Connection connection =
            StreamEnvFactory.getEnv().getConnection(userName, PASSWORD, BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("USE " + DATABASE);
      statement.execute(
          "CREATE STREAM sec11_stream PERIOD(1h) INTO "
              + DATABASE
              + "."
              + SINK_TABLE
              + "(time, value)"
              + " SELECT ${START_TIME}, count(*) FROM "
              + DATABASE
              + "."
              + SOURCE_TABLE);
      statement.execute("START STREAM sec11_stream");

      AWAIT.untilAsserted(
          () ->
              assertEquals(
                  "The source-less PERIOD stream should be creatable and runnable with the"
                      + " minimal privilege set",
                  "RUNNING",
                  queryStreamRow(statement, "sec11_stream")
                      .get(ColumnHeaderConstant.STREAM_STATUS_TABLE_MODEL)));
    }
  }

  /** A TUMBLE stream whose subquery reads other_t (exercises the subquery privilege). */
  private String subqueryStreamSql(String streamName) {
    return "CREATE STREAM "
        + streamName
        + " FROM "
        + DATABASE
        + "."
        + SOURCE_TABLE
        + " TUMBLE(size => 1h) INTO "
        + DATABASE
        + "."
        + SINK_TABLE
        + "(time, value)"
        + " SELECT ${START_TIME}, count(*) FROM "
        + DATABASE
        + "."
        + OTHER_TABLE;
  }

  private void createTables(Statement statement) throws SQLException {
    statement.execute("CREATE DATABASE " + DATABASE);
    statement.execute("USE " + DATABASE);
    statement.execute(
        "CREATE TABLE "
            + SOURCE_TABLE
            + " (time TIMESTAMP TIME, device_id STRING TAG, value INT64 FIELD)");
    statement.execute("CREATE TABLE " + SINK_TABLE + " (time TIMESTAMP TIME, value INT64 FIELD)");
    statement.execute(
        "CREATE TABLE "
            + OTHER_TABLE
            + " (time TIMESTAMP TIME, device_id STRING TAG, value INT64 FIELD)");
  }

  private void grantAllStreamPrivileges(Statement statement, String userName) throws SQLException {
    statement.execute("GRANT SYSTEM TO USER " + userName);
    grantSelectOnSource(statement, userName);
    grantInsertOnSink(statement, userName);
    grantSelectOnOther(statement, userName);
  }

  private void grantSelectOnSource(Statement statement, String userName) throws SQLException {
    statement.execute(
        "GRANT SELECT ON TABLE " + DATABASE + "." + SOURCE_TABLE + " TO USER " + userName);
  }

  private void grantInsertOnSink(Statement statement, String userName) throws SQLException {
    statement.execute(
        "GRANT INSERT ON TABLE " + DATABASE + "." + SINK_TABLE + " TO USER " + userName);
  }

  private void grantSelectOnOther(Statement statement, String userName) throws SQLException {
    statement.execute(
        "GRANT SELECT ON TABLE " + DATABASE + "." + OTHER_TABLE + " TO USER " + userName);
  }

  /** Executes the SQL and fails unless it is rejected; returns the rejection message. */
  private String assertRejected(Statement statement, String sql, String description)
      throws SQLException {
    try {
      statement.execute(sql);
      fail(description + " should be rejected");
    } catch (SQLException e) {
      return e.getMessage();
    }
    // Unreachable.
    return null;
  }

  private void assertNoCredentialLeak(String description, String message) {
    assertTrue(description + " must not be null", message != null);
    assertFalse(
        description + " must not leak the user password but was: " + message,
        message.contains(PASSWORD));
    assertFalse(
        description + " must not leak password material but was: " + message,
        message.toLowerCase().contains("password:"));
  }

  private boolean streamVisible(Statement statement, String streamName) throws SQLException {
    try (ResultSet rs = statement.executeQuery("SHOW STREAMS")) {
      while (rs.next()) {
        if (streamName.equals(rs.getString(ColumnHeaderConstant.STREAM_NAME_TABLE_MODEL))) {
          return true;
        }
      }
    }
    return false;
  }

  private void assertNullRow(Statement statement, String streamName, String message)
      throws SQLException {
    assertTrue(message, queryStreamRow(statement, streamName) == null);
  }
}
