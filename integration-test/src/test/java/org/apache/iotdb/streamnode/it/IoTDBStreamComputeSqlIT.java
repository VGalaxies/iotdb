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
 * specific language permissions and limitations under the
 * License.
 */

package org.apache.iotdb.streamnode.it;

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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * Integration tests for the stream-computation compute-task and placeholder SQL cases (SQL-01
 * through SQL-30). Runs against a 1C1D1S cluster (1 ConfigNode, 1 DataNode, 1 StreamNode).
 *
 * <p>SQL-03 and SQL-24 use the trigger table t1 as both the stream source and the subquery's query
 * table (same-table form); SQL-28 and SQL-29 carry the original cross-table semantics (other_t +
 * WHERE, a JOIN b) as separate cases. SQL-17 and SQL-30 are negative: any JOIN participation of
 * {@code ${rows}} (with a plain table, or with another {@code ${rows}}) is rejected at creation.
 */
@Category({StreamClusterIT.class})
public class IoTDBStreamComputeSqlIT {

  private static final String DB = "sql01";
  private static final String DB02 = "sql02";
  private static final String DB03 = "sql03";
  private static final String DB04 = "sql04";
  private static final String DB05 = "sql05";
  private static final String DB06 = "sql06";
  private static final String DB07 = "sql07";
  private static final String DB08 = "sql08";
  private static final String DB09 = "sql09";
  private static final String DB10 = "sql10";
  private static final String DB11 = "sql11";
  private static final String DB12 = "sql12";
  private static final String DB13 = "sql13";
  private static final String DB14 = "sql14";
  private static final String DB15 = "sql15";
  private static final String DB16 = "sql16";
  private static final String DB17 = "sql17";
  private static final String DB18 = "sql18";
  private static final String DB19 = "sql19";
  private static final String DB20 = "sql20";
  private static final String DB21 = "sql21";
  private static final String DB22 = "sql22";
  private static final String DB23 = "sql23";
  private static final String DB24 = "sql24";
  private static final String DB25 = "sql25";
  private static final String DB26 = "sql26";
  private static final String DB27 = "sql27";
  private static final String DB28 = "sql28";
  private static final String DB29 = "sql29";
  private static final String DB30 = "sql30";

  // Reduced from 120s: legitimate results always appear within ~4s (TUMBLE
  // windows are event-time driven at 5s, PERIOD fires every 2s; verified across
  // 120s/15s/7s/5s rounds — every pass set identical, no latency-type failure),
  // while known product defects leave the result table empty or emit wrong
  // counts. The short bound keeps the same verdicts but fails much faster.
  // Override with -DstreamComputeSqlIT.awaitSeconds=N (e.g. 15) when diagnosing
  // a suspicious red that could be latency-related.
  private static final int AWAIT_TIMEOUT_SECONDS =
      Integer.getInteger("streamComputeSqlIT.awaitSeconds", 5);

  private static final ConditionFactory AWAIT =
      Awaitility.await()
          .pollInSameThread()
          .pollDelay(200, TimeUnit.MILLISECONDS)
          .pollInterval(500, TimeUnit.MILLISECONDS)
          .atMost(AWAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS);

  // Longer bound for cases whose legitimate result cannot land within AWAIT_TIMEOUT_SECONDS:
  // SQL-08 must observe TWO PERIOD triggers (2s apart) and the first row only lands after stream
  // task startup (~4.4s observed), so its second row arrives ~6.5s in; SQL-09's first row may land
  // after cold start. 5s truncates those legitimate results (verified 2026-10-15: both cases pass
  // at 15s, red at 5s purely on arrival latency). Passing polls still return as soon as the
  // assertion holds, so the longer cap only lengthens the failure path.
  private static final int SLOW_AWAIT_TIMEOUT_SECONDS = Math.max(15, AWAIT_TIMEOUT_SECONDS);

  private static final ConditionFactory SLOW_AWAIT =
      Awaitility.await()
          .pollInSameThread()
          .pollDelay(200, TimeUnit.MILLISECONDS)
          .pollInterval(500, TimeUnit.MILLISECONDS)
          .atMost(SLOW_AWAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS);

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
      for (final String databaseName :
          new String[] {
            DB, DB02, DB03, DB04, DB05, DB06, DB07, DB08, DB09, DB10, DB11, DB12, DB13, DB14, DB15,
            DB16, DB17, DB18, DB19, DB20, DB21, DB22, DB23, DB24, DB25, DB26, DB27, DB28, DB29, DB30
          }) {
        try {
          statement.execute("DROP DATABASE " + databaseName);
        } catch (Exception ignored) {
          // database may not exist if a test failed before creating it
        }
      }
    }
  }

  private void createDatabase(final Statement statement) throws Exception {
    statement.execute("CREATE DATABASE " + DB);
    statement.execute("USE " + DB);
  }

  /**
   * SQL-01 query the trigger table: the compute task queries the trigger table (${ROWS}) and
   * aggregates the current window's data after the window is triggered. The write-back must equal
   * an independent count of the same window.
   */
  @Test
  public void testSQL01QueryTriggerTableAggregateWindow() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      createDatabase(stmt);
      stmt.execute(
          "CREATE TABLE t1(time TIMESTAMP TIME, device_id STRING TAG, amount FLOAT FIELD)");
      stmt.execute("CREATE TABLE rows_calc(time TIMESTAMP TIME, rows INT64 FIELD)");

      // Align window boundaries to a near-future origin so inserted rows land deterministically
      // in W1 = [base, base+5s).
      final long base = System.currentTimeMillis() + 2000;

      // Computation task queries the trigger table (${ROWS}) and aggregates the current window.
      stmt.execute(
          "CREATE STREAM sql01 "
              + "FROM t1 TUMBLE(5s, "
              + base
              + ") INTO rows_calc(time, rows) "
              + "SELECT ${START_TIME}, count(*) FROM ${ROWS}");
      stmt.execute("START STREAM sql01");

      // All rows use one device: distinct TAG values map to different tablets whose delivery
      // order is not guaranteed (B-8); a single device keeps batches ordered by commit time.
      // Three rows in W1.
      stmt.execute(
          "INSERT INTO t1(time, device_id, amount) VALUES (" + (base + 500) + ", 'd1', 1.0)");
      stmt.execute(
          "INSERT INTO t1(time, device_id, amount) VALUES (" + (base + 1500) + ", 'd1', 2.0)");
      stmt.execute(
          "INSERT INTO t1(time, device_id, amount) VALUES (" + (base + 2500) + ", 'd1', 3.0)");
      // One row in W2 advances the watermark and closes W1, triggering its computation.
      stmt.execute(
          "INSERT INTO t1(time, device_id, amount) VALUES (" + (base + 6000) + ", 'd1', 4.0)");

      // Independent "Oracle": we inserted exactly 3 rows whose event time falls in W1.
      final long expectedW1Count = 3L;

      AWAIT.untilAsserted(
          () -> {
            boolean windowAggregateMatches = false;
            try (ResultSet rs = stmt.executeQuery("SELECT time, rows FROM rows_calc")) {
              while (rs.next()) {
                // The trigger-table query must aggregate the current window and write back a row
                // whose count equals the independent count of that window.
                if (rs.getInt("rows") == expectedW1Count) {
                  windowAggregateMatches = true;
                }
              }
            }
            assertTrue(
                "rows_calc should contain the W1 window aggregate == "
                    + expectedW1Count
                    + " (querying the trigger table ${ROWS})",
                windowAggregateMatches);
          });
    }
  }

  /**
   * SQL-02 query another table (other_t): the compute task is a PERIOD/TUMBLE subquery that reads a
   * table different from the trigger (source) table. With full permissions the stream must execute,
   * and the written-back value must come from {@code other_t} and equal an independent query
   * against {@code other_t}.
   *
   * <p>Independent Oracle source: we insert exactly two rows into {@code other_t} with {@code base}
   * values 100 and 200, so the expected aggregate {@code sum(base) = 300} is hand-computed from the
   * inserts — it is NOT read back from the product. The trigger table {@code t1} only drives the
   * windowing; its content is irrelevant to the result.
   */
  @Test
  public void testSQL02QueryOtherTable() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      stmt.execute("CREATE DATABASE " + DB02);
      stmt.execute("USE " + DB02);
      // Trigger (source) table t1.
      stmt.execute(
          "CREATE TABLE t1(time TIMESTAMP TIME, device_id STRING TAG, amount FLOAT FIELD)");
      // The "other" table that the compute task queries.
      stmt.execute(
          "CREATE TABLE other_t(time TIMESTAMP TIME, device_id STRING TAG, base INT64 FIELD)");
      // Sink table.
      stmt.execute("CREATE TABLE calc(time TIMESTAMP TIME, other_sum DOUBLE FIELD)");

      // Independent Oracle data: other_t contributes exactly base = 100 + 200 = 300.
      stmt.execute(
          "INSERT INTO other_t(time, device_id, base) VALUES ("
              + (System.currentTimeMillis() + 1000)
              + ", 'd1', 100)");
      stmt.execute(
          "INSERT INTO other_t(time, device_id, base) VALUES ("
              + (System.currentTimeMillis() + 1000)
              + ", 'd2', 200)");

      // Align window boundaries to a near-future origin so inserted t1 rows land deterministically
      // in W1 = [base, base+5s).
      final long base = System.currentTimeMillis() + 2000;

      // Compute task queries the OTHER table (other_t), not the trigger table ${ROWS}.
      stmt.execute(
          "CREATE STREAM sql02 "
              + "FROM t1 TUMBLE(5s, "
              + base
              + ") INTO calc(time, other_sum) "
              + "SELECT ${START_TIME}, sum(base) FROM other_t");
      stmt.execute("START STREAM sql02");

      // Rows in t1 drive the windowing; their content is irrelevant to the calc result.
      stmt.execute(
          "INSERT INTO t1(time, device_id, amount) VALUES (" + (base + 500) + ", 'd1', 1.0)");
      stmt.execute(
          "INSERT INTO t1(time, device_id, amount) VALUES (" + (base + 1500) + ", 'd2', 2.0)");
      stmt.execute(
          "INSERT INTO t1(time, device_id, amount) VALUES (" + (base + 2500) + ", 'd3', 3.0)");
      // One row in W2 advances the watermark and closes W1, triggering the computation.
      stmt.execute(
          "INSERT INTO t1(time, device_id, amount) VALUES (" + (base + 6000) + ", 'd1', 4.0)");

      // Independent "Oracle": sum(base) over other_t = 100 + 200 = 300.
      final double expectedOtherSum = 300.0;

      AWAIT.untilAsserted(
          () -> {
            boolean resultFromOtherT = false;
            try (ResultSet rs = stmt.executeQuery("SELECT time, other_sum FROM calc")) {
              while (rs.next()) {
                // The written-back value must equal the independent aggregate of other_t.
                if (rs.getDouble("other_sum") == expectedOtherSum) {
                  resultFromOtherT = true;
                }
              }
            }
            assertTrue(
                "calc should contain a row whose other_sum == "
                    + expectedOtherSum
                    + " (computed from other_t, independent of the trigger table)",
                resultFromOtherT);
          });
    }
  }

  /**
   * SQL-03 subquery WHERE (same-table form): the compute-task subquery filters on a time
   * placeholder ({@code WHERE time < ${START_TIME}}) and reads the trigger table {@code t1} by name
   * (not {@code ${ROWS}}). The predicate must really be applied inside the subquery, and it must
   * not be hoisted into the source clause's pre-filter.
   *
   * <p>Same-table rationale: {@code ${ROWS}} supports no WHERE pre-filter (see the placeholder
   * constraints in the requirements doc), so the only way to keep the WHERE time-placeholder
   * semantics on the trigger table itself is to read {@code t1} by name. This also exercises the
   * cross-table calc-plan code path (reading a table other than {@code ${ROWS}}), which may surface
   * B-1.
   *
   * <p>Expected value derivation: {@code t1} (the trigger table) is loaded with exactly one row
   * strictly before the W1 start ({@code base - 2000}) and three rows inside W1 ({@code base +
   * 500/1500/2500}) plus a W2 row ({@code base + 6000}) to close W1. Under {@code time <
   * ${START_TIME}} (= {@code base}), only the {@code base - 2000} row qualifies, so {@code
   * count(*)} is hand-computed as 1. The value is derived from the INSERT statements only — it is
   * never read back from the product. It discriminates both failure modes: 5 would mean the WHERE
   * was ignored (the whole table has 5 rows), and no row at all would mean the WHERE was hoisted
   * onto the source, because every W1/W2 trigger row has {@code time >= ${START_TIME}} and would
   * then be filtered out before the window could ever close.
   */
  @Test
  public void testSQL03SubqueryWhereTimePlaceholder() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      stmt.execute("CREATE DATABASE " + DB03);
      stmt.execute("USE " + DB03);
      // Trigger table = query table (same-table form): the subquery reads t1 by name.
      stmt.execute(
          "CREATE TABLE t1(time TIMESTAMP TIME, device_id STRING TAG, amount FLOAT FIELD)");
      // Sink table.
      stmt.execute("CREATE TABLE calc(time TIMESTAMP TIME, cnt INT64 FIELD)");

      // Align window boundaries to a near-future origin so W1 = [base, base+5s).
      final long base = System.currentTimeMillis() + 2000;

      // Independent Oracle data: exactly one t1 row lies strictly before W1 starts. It is written
      // BEFORE the stream starts so it is not seen as a trigger event (no pre-W1 window output),
      // but it IS stored in t1 and thus visible to the subquery `FROM t1 WHERE time < base`.
      stmt.execute(
          "INSERT INTO t1(time, device_id, amount) VALUES (" + (base - 2000) + ", 'd0', 0.0)");

      // The WHERE lives inside the subquery (the calc plan), not in the source clause. The
      // subquery reads the trigger table t1 BY NAME (not ${ROWS}).
      stmt.execute(
          "CREATE STREAM sql03 "
              + "FROM t1 TUMBLE(5s, "
              + base
              + ") INTO calc(time, cnt) "
              + "SELECT ${START_TIME}, count(*) FROM t1 WHERE time < ${START_TIME}");
      stmt.execute("START STREAM sql03");

      // Three trigger rows in W1, then one in W2 to advance the watermark and close W1. Every
      // one of them satisfies time >= ${START_TIME}, so a hoisted pre-filter would drop them all.
      stmt.execute(
          "INSERT INTO t1(time, device_id, amount) VALUES (" + (base + 500) + ", 'd1', 1.0)");
      stmt.execute(
          "INSERT INTO t1(time, device_id, amount) VALUES (" + (base + 1500) + ", 'd2', 2.0)");
      stmt.execute(
          "INSERT INTO t1(time, device_id, amount) VALUES (" + (base + 2500) + ", 'd3', 3.0)");
      stmt.execute(
          "INSERT INTO t1(time, device_id, amount) VALUES (" + (base + 6000) + ", 'd1', 4.0)");

      // Independent "Oracle": count(*) over t1 with time < base == 1 (only the base-2000 row).
      final long expectedCnt = 1L;

      AWAIT.untilAsserted(
          () -> {
            boolean subqueryWhereApplied = false;
            try (ResultSet rs = stmt.executeQuery("SELECT time, cnt FROM calc")) {
              while (rs.next()) {
                // Exactly the W1 row must carry the hand-computed filtered count.
                if (rs.getLong("time") == base && rs.getLong("cnt") == expectedCnt) {
                  subqueryWhereApplied = true;
                }
              }
            }
            assertTrue(
                "calc should contain the W1 row with cnt == "
                    + expectedCnt
                    + " (subquery WHERE time < ${START_TIME} applied inside the subquery on the"
                    + " same trigger table t1, not hoisted to the source pre-filter)",
                subqueryWhereApplied);
          });
    }
  }

  /**
   * SQL-04 SELECT / INTO correspondence: the SELECT item order is permuted and the INTO column list
   * is permuted with it. Each SELECT item must land in the INTO column written at the same
   * position.
   *
   * <p>Independent Oracle source: W1 receives exactly three rows with {@code amount} 10, 20 and 30,
   * so {@code sum(amount) = 60} and {@code count(*) = 3} are hand-computed from the INSERT
   * statements — never read back from the product. {@code sum} over an INT64 column yields DOUBLE
   * in this engine, so {@code total} is declared DOUBLE to keep the statement on the legal side of
   * the type rule: an INT64 {@code total} is rejected at creation with 701 and would silently turn
   * this positive case into the SQL-06 negative one. The sink table declares its columns as {@code
   * (time, cnt, total)} while the INTO list is {@code (time, total, cnt)}, i.e. the two measure
   * columns are declared in the opposite order. Writing by correspondence therefore yields {@code
   * total = 60, cnt = 3}; an implementation that ignored the INTO list and wrote by the table's
   * declaration order would put 60 into {@code cnt} and 3 into {@code total} and fail the
   * assertion.
   */
  @Test
  public void testSQL04SelectIntoColumnCorrespondence() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      stmt.execute("CREATE DATABASE " + DB04);
      stmt.execute("USE " + DB04);
      stmt.execute(
          "CREATE TABLE t1(time TIMESTAMP TIME, device_id STRING TAG, amount INT64 FIELD)");
      // Note the declaration order: cnt comes before total. total is DOUBLE because sum(INT64)
      // is DOUBLE.
      stmt.execute("CREATE TABLE calc(time TIMESTAMP TIME, cnt INT64 FIELD, total DOUBLE FIELD)");

      // Align window boundaries to a near-future origin so W1 = [base, base+5s).
      final long base = System.currentTimeMillis() + 2000;

      // SELECT order is (start_time, sum, count); the INTO list mirrors it as (time, total, cnt).
      stmt.execute(
          "CREATE STREAM sql04 "
              + "FROM t1 TUMBLE(5s, "
              + base
              + ") INTO calc(time, total, cnt) "
              + "SELECT ${START_TIME}, sum(amount), count(*) FROM ${ROWS}");
      stmt.execute("START STREAM sql04");

      // Independent Oracle data: three rows in W1 with value 10 + 20 + 30.
      stmt.execute(
          "INSERT INTO t1(time, device_id, amount) VALUES (" + (base + 500) + ", 'd1', 10)");
      stmt.execute(
          "INSERT INTO t1(time, device_id, amount) VALUES (" + (base + 1500) + ", 'd1', 20)");
      stmt.execute(
          "INSERT INTO t1(time, device_id, amount) VALUES (" + (base + 2500) + ", 'd1', 30)");
      // One row in W2 advances the watermark and closes W1, triggering the computation.
      stmt.execute(
          "INSERT INTO t1(time, device_id, amount) VALUES (" + (base + 6000) + ", 'd1', 40)");

      // Independent "Oracle": sum(amount) over W1 == 60 and count(*) over W1 == 3.
      final double expectedTotal = 60.0d;
      final long expectedCnt = 3L;

      AWAIT.untilAsserted(
          () -> {
            boolean writtenByCorrespondence = false;
            try (ResultSet rs = stmt.executeQuery("SELECT time, cnt, total FROM calc")) {
              while (rs.next()) {
                // total must carry sum(amount) and cnt must carry count(*), even though the sink
                // table declares cnt first.
                if (rs.getLong("time") == base
                    && rs.getDouble("total") == expectedTotal
                    && rs.getLong("cnt") == expectedCnt) {
                  writtenByCorrespondence = true;
                }
              }
            }
            assertTrue(
                "calc should contain the W1 row with total == "
                    + expectedTotal
                    + " and cnt == "
                    + expectedCnt
                    + " (SELECT items written into the INTO columns at the same position)",
                writtenByCorrespondence);
          });
    }
  }

  private static boolean streamExists(final Statement statement, final String streamName)
      throws Exception {
    try (ResultSet rs =
        statement.executeQuery(
            "SELECT stream_name FROM information_schema.streams WHERE stream_name = '"
                + streamName
                + "'")) {
      return rs.next();
    }
  }

  /**
   * SQL-05 SELECT / INTO column-count mismatch: creation must be rejected when the number of SELECT
   * items differs from the number of INTO columns. The analysis is done by {@code
   * StatementAnalyzer.typesMatchForInsert}, which makes {@code analyzeStreamSink} throw a {@code
   * SemanticException} carrying {@code
   * INSERT_QUERY_HAS_MISMATCHED_COLUMN_TYPES_BETWEEN_TABLE_AND_QUERY} (701) as soon as the column
   * counts differ.
   *
   * <p>Both directions are covered — fewer SELECT items than INTO columns, and more. Everything
   * else in these statements is the same shape that SQL-01/SQL-04 accept, so the only thing that
   * can reject them is the column count.
   */
  @Test
  public void testSQL05SelectIntoColumnCountMismatch() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      stmt.execute("CREATE DATABASE " + DB05);
      stmt.execute("USE " + DB05);
      stmt.execute(
          "CREATE TABLE t1(time TIMESTAMP TIME, device_id STRING TAG, amount INT64 FIELD)");
      stmt.execute("CREATE TABLE calc(time TIMESTAMP TIME, cnt INT64 FIELD)");

      // (a) Fewer SELECT items than INTO columns: 1 SELECT item vs 2 INTO columns.
      assertThrows(
          Exception.class,
          () ->
              stmt.execute(
                  "CREATE STREAM sql05a "
                      + "FROM t1 TUMBLE(5s) INTO calc(time, cnt) "
                      + "SELECT ${START_TIME} FROM ${ROWS}"));

      // (b) More SELECT items than INTO columns: 3 SELECT items vs 2 INTO columns.
      assertThrows(
          Exception.class,
          () ->
              stmt.execute(
                  "CREATE STREAM sql05b "
                      + "FROM t1 TUMBLE(5s) INTO calc(time, cnt) "
                      + "SELECT ${START_TIME}, count(*), count(amount) FROM ${ROWS}"));

      // A rejected creation must not leave the stream registered.
      assertFalse(
          "sql05a must not be registered after the rejected CREATE STREAM",
          streamExists(stmt, "sql05a"));
      assertFalse(
          "sql05b must not be registered after the rejected CREATE STREAM",
          streamExists(stmt, "sql05b"));
    }
  }

  /**
   * SQL-06 SELECT / INTO type incompatibility: with the column count matching, creation must be
   * rejected when a SELECT item's type is not compatible with the target column.
   *
   * <p>The size check in {@code StatementAnalyzer.typesMatchForInsert} passes here, so the
   * per-position {@code !tableTypes.get(i).equals(queryTypes.get(i))} branch rejects the statement
   * with the same 701 {@code SemanticException}. The DOUBLE variant was already observed
   * empirically while authoring SQL-04: {@code 701: Insert query has mismatched column types:
   * Table: [TIMESTAMP, INT64, INT64], Query: [TIMESTAMP, DOUBLE, INT64]} — {@code sum} over an
   * INT64 column yields DOUBLE.
   *
   * <p>Only clearly incompatible directions are used (an aggregate DOUBLE into an INT64 column, and
   * a TIMESTAMP placeholder into an INT64 column), so no interpretation of "widening is compatible"
   * can make the expectation ambiguous.
   */
  @Test
  public void testSQL06SelectIntoTypeIncompatible() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      stmt.execute("CREATE DATABASE " + DB06);
      stmt.execute("USE " + DB06);
      stmt.execute(
          "CREATE TABLE t1(time TIMESTAMP TIME, device_id STRING TAG, amount INT64 FIELD)");
      stmt.execute("CREATE TABLE calc(time TIMESTAMP TIME, cnt INT64 FIELD)");

      // (a) sum(amount) over an INT64 column yields DOUBLE, which cannot go into an INT64 column.
      assertThrows(
          Exception.class,
          () ->
              stmt.execute(
                  "CREATE STREAM sql06a "
                      + "FROM t1 TUMBLE(5s) INTO calc(time, cnt) "
                      + "SELECT ${START_TIME}, sum(amount) FROM ${ROWS}"));

      // (b) A TIMESTAMP placeholder cannot go into an INT64 column.
      assertThrows(
          Exception.class,
          () ->
              stmt.execute(
                  "CREATE STREAM sql06b "
                      + "FROM t1 TUMBLE(5s) INTO calc(time, cnt) "
                      + "SELECT ${START_TIME}, ${END_TIME} FROM ${ROWS}"));

      // A rejected creation must not leave the stream registered.
      assertFalse(
          "sql06a must not be registered after the rejected CREATE STREAM",
          streamExists(stmt, "sql06a"));
      assertFalse(
          "sql06b must not be registered after the rejected CREATE STREAM",
          streamExists(stmt, "sql06b"));
    }
  }

  /**
   * SQL-07 placeholder case-insensitivity: the compute task uses the same {@code start_time}
   * placeholder written in upper-case {@code ${START_TIME}}, lower-case {@code ${start_time}} and
   * mixed-case {@code ${Start_Time}} side by side. Placeholder names are case-insensitive, so every
   * variant must be replaced with the same constant on each trigger. The mixed-case sample
   * deliberately keeps the underscore — {@code ${Start_Time}} differs from {@code ${start_time}}
   * only in letter case, so it is a pure case variant; an earlier draft used {@code ${StartTime}},
   * which additionally drops the underscore and is therefore a DIFFERENT identifier, not a case
   * variant (it would legitimately stay rejected even after case normalization, like the undefined
   * named placeholder of SQL-26).
   *
   * <p>Expected values: the trigger table {@code t1} receives three rows whose event time falls in
   * W1 = [base, base+5s), so the window start {@code ${START_TIME}} is exactly {@code base}
   * (hand-computed from the TUMBLE origin), and {@code count(*)} over W1 is 3. Therefore the three
   * case variants must all resolve to {@code base}, and {@code count(*)} must be 3.
   */
  @Test
  public void testSQL07PlaceholderCaseInsensitive() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      stmt.execute("CREATE DATABASE " + DB07);
      stmt.execute("USE " + DB07);
      stmt.execute(
          "CREATE TABLE t1(time TIMESTAMP TIME, device_id STRING TAG, amount FLOAT FIELD)");
      // upper / lower / mixed columns each carry one case variant of the start_time placeholder.
      // upper / lower carry the window-start TIMESTAMP: the analyzer types ${start_time} /
      // ${Start_Time} as TIMESTAMP, and a TIMESTAMP value cannot be written into an INT64 column
      // (the rule SQL-06b verifies; confirmed empirically by a 701 on this very case
      // when these columns were INT64). A table allows only one TIME column, so they must be
      // TIMESTAMP attribute columns — same pattern as prev/next/endt in SQL-09/10/11.
      stmt.execute(
          "CREATE TABLE calc(time TIMESTAMP TIME, upper TIMESTAMP FIELD, lower TIMESTAMP FIELD, mixed INT64 FIELD)");

      // Align window boundaries to a near-future origin so inserted rows land deterministically in
      // W1 = [base, base+5s).
      final long base = System.currentTimeMillis() + 2000;

      // Three case variants of the same placeholder, plus count(*) to anchor the window aggregate.
      stmt.execute(
          "CREATE STREAM sql07 "
              + "FROM t1 TUMBLE(5s, "
              + base
              + ") INTO calc(time, upper, lower, mixed) "
              + "SELECT ${START_TIME}, ${start_time}, ${Start_Time}, count(*) FROM ${ROWS}");
      stmt.execute("START STREAM sql07");

      // All rows use one device: distinct TAG values map to different tablets whose delivery
      // order is not guaranteed (B-8); a single device keeps batches ordered by commit time.
      // Three rows in W1.
      stmt.execute(
          "INSERT INTO t1(time, device_id, amount) VALUES (" + (base + 500) + ", 'd1', 1.0)");
      stmt.execute(
          "INSERT INTO t1(time, device_id, amount) VALUES (" + (base + 1500) + ", 'd1', 2.0)");
      stmt.execute(
          "INSERT INTO t1(time, device_id, amount) VALUES (" + (base + 2500) + ", 'd1', 3.0)");
      // One row in W2 advances the watermark and closes W1, triggering its computation.
      stmt.execute(
          "INSERT INTO t1(time, device_id, amount) VALUES (" + (base + 6000) + ", 'd1', 4.0)");

      // Independent "Oracle": W1 start time == base, and W1 contains exactly 3 rows.
      final long expectedStart = base;
      final long expectedCount = 3L;

      AWAIT.untilAsserted(
          () -> {
            boolean allCaseVariantsResolveSame = false;
            try (ResultSet rs = stmt.executeQuery("SELECT time, upper, lower, mixed FROM calc")) {
              while (rs.next()) {
                // The three placeholder case variants must all resolve to the same window start,
                // and count(*) must match the independent window count.
                if (rs.getLong("time") == expectedStart
                    && rs.getLong("upper") == expectedStart
                    && rs.getLong("lower") == expectedStart
                    && rs.getLong("mixed") == expectedCount) {
                  allCaseVariantsResolveSame = true;
                }
              }
            }
            assertTrue(
                "calc must contain a row where upper==lower==time=="
                    + expectedStart
                    + " and mixed=="
                    + expectedCount
                    + " (placeholder case variants resolve to the same constant)",
                allCaseVariantsResolveSame);
          });
    }
  }

  /**
   * SQL-08 {@code ${prev_time}} on the first PERIOD trigger: a system-triggered (PERIOD) stream
   * writes the per-trigger {@code ${prev_time}} into a sink table. For a timed (PERIOD) trigger,
   * {@code ${prev_time}} is the previous system trigger time and is NULL for the first window: (a)
   * the earliest written row must have a NULL prev_time, and (b) the next row's prev_time must
   * equal that earliest row's window start time (the first system trigger time).
   *
   * <p>The SELECT also emits {@code count(*)}: the first trigger's only other output would be the
   * NULL prev_time, and a row whose every measurement is NULL is discarded by the sink (SQL-22
   * verified semantics), which would make the first trigger's row unobservable. The non-NULL count
   * keeps every row writable so the NULL prev_time itself is what gets asserted.
   */
  @Test
  public void testSQL08PrevTimeFirstPeriod() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      stmt.execute("CREATE DATABASE " + DB08);
      stmt.execute("USE " + DB08);
      // A regular table (NOT the trigger/${ROWS} table — PERIOD is system-triggered) that the
      // compute task scans once per period, so each trigger emits exactly one sink row.
      stmt.execute(
          "CREATE TABLE t1(time TIMESTAMP TIME, device_id STRING TAG, amount FLOAT FIELD)");
      stmt.execute(
          "INSERT INTO t1(time, device_id, amount) VALUES ("
              + (System.currentTimeMillis() + 1000)
              + ", 'd1', 1.0)");
      // prev_time resolves to a TIMESTAMP-typed value (per the analyzer), so the sink column must
      // be a TIMESTAMP attribute (a table may have only one TIME column, hence not TIMESTAMP TIME).
      // cnt keeps every row non-NULL so the sink does not discard it as an all-NULL row.
      stmt.execute("CREATE TABLE calc(time TIMESTAMP TIME, prev TIMESTAMP FIELD, cnt INT64 FIELD)");

      // First period fires ~1.5s after now; second ~3.5s. Short period to observe two triggers
      // fast.
      final long origin = System.currentTimeMillis() + 1500;
      stmt.execute(
          "CREATE STREAM sql08 "
              + "PERIOD(2s, "
              + origin
              + ") INTO calc(time, prev, cnt) "
              + "SELECT ${START_TIME}, ${prev_time}, count(*) FROM t1");
      stmt.execute("START STREAM sql08");

      SLOW_AWAIT.untilAsserted(
          () -> {
            final List<Long> nullPrevTimes = new ArrayList<>();
            final List<Long> prevValues = new ArrayList<>();
            try (ResultSet rs =
                stmt.executeQuery("SELECT time, prev, cnt FROM calc ORDER BY time")) {
              while (rs.next()) {
                final long time = rs.getLong("time");
                final long prev = rs.getLong("prev");
                // wasNull() must be consulted immediately after reading prev: it reflects the
                // last column read, so reading cnt first would report cnt's null-ness instead and
                // misread a NULL prev_time as 0.
                final boolean prevIsNull = rs.wasNull();
                if (prevIsNull) {
                  nullPrevTimes.add(time);
                } else {
                  prevValues.add(prev);
                }
              }
            }
            // (a) the first window must have a NULL prev_time.
            assertTrue(
                "the first PERIOD row must have a NULL prev_time (got "
                    + nullPrevTimes.size()
                    + " null-prev rows, "
                    + prevValues.size()
                    + " non-null)",
                !nullPrevTimes.isEmpty());
            // (b) the second window's prev_time must equal the first window's start time.
            final long firstStart = nullPrevTimes.get(0);
            boolean secondPrevEqualsFirstStart = false;
            for (final long p : prevValues) {
              if (p == firstStart) {
                secondPrevEqualsFirstStart = true;
              }
            }
            assertTrue(
                "the second PERIOD row's prev_time must equal the first window start time "
                    + firstStart
                    + " (non-null prev values: "
                    + prevValues
                    + ")",
                secondPrevEqualsFirstStart);
          });
    }
  }

  /**
   * SQL-09 {@code ${prev_time}} on the first TUMBLE (event) window: an event-triggered TUMBLE
   * stream writes the per-window {@code ${prev_time}} into a sink table. The first window's
   * prev_time must be NULL and the second window's prev_time must equal the previous window's start
   * time.
   *
   * <p>Expected values: for {@code TUMBLE(5s, base)} the windows are W1 = [base, base+5s) and W2 =
   * [base+5s, base+10s), so the second window's prev_time must equal the first window's start:
   * W2.prev_time == base. The first window's prev_time must be NULL. Both expectations are
   * hand-computed from the window origin and size, not read back from the product.
   */
  @Test
  public void testSQL09PrevTimeFirstTumble() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      stmt.execute("CREATE DATABASE " + DB09);
      stmt.execute("USE " + DB09);
      stmt.execute(
          "CREATE TABLE t1(time TIMESTAMP TIME, device_id STRING TAG, amount FLOAT FIELD)");
      // prev_time resolves to a TIMESTAMP-typed value; store it in a TIMESTAMP attribute column.
      stmt.execute("CREATE TABLE calc(time TIMESTAMP TIME, prev TIMESTAMP FIELD, cnt INT64 FIELD)");

      // Align window boundaries to a near-future origin so inserted rows land deterministically in
      // W1 = [base, base+5s) and W2 = [base+5s, base+10s).
      final long base = System.currentTimeMillis() + 2000;

      // Compute task reads the trigger table (${ROWS}) and emits the window start, prev_time and
      // the per-window row count.
      stmt.execute(
          "CREATE STREAM sql09 "
              + "FROM t1 TUMBLE(5s, "
              + base
              + ") INTO calc(time, prev, cnt) "
              + "SELECT ${START_TIME}, ${prev_time}, count(*) FROM ${ROWS}");
      stmt.execute("START STREAM sql09");

      // All rows use one device: distinct TAG values map to different tablets whose delivery
      // order is not guaranteed (B-8); a single device keeps batches ordered by commit time.
      // Three rows in W1.
      stmt.execute(
          "INSERT INTO t1(time, device_id, amount) VALUES (" + (base + 500) + ", 'd1', 1.0)");
      stmt.execute(
          "INSERT INTO t1(time, device_id, amount) VALUES (" + (base + 1500) + ", 'd1', 2.0)");
      stmt.execute(
          "INSERT INTO t1(time, device_id, amount) VALUES (" + (base + 2500) + ", 'd1', 3.0)");
      // One row in W2 advances the watermark and closes W1, triggering its computation.
      stmt.execute(
          "INSERT INTO t1(time, device_id, amount) VALUES (" + (base + 6000) + ", 'd1', 4.0)");

      // Two more rows in W2.
      stmt.execute(
          "INSERT INTO t1(time, device_id, amount) VALUES (" + (base + 5500) + ", 'd1', 5.0)");
      stmt.execute(
          "INSERT INTO t1(time, device_id, amount) VALUES (" + (base + 6500) + ", 'd1', 6.0)");
      // One row in W3 closes W2, triggering its computation.
      stmt.execute(
          "INSERT INTO t1(time, device_id, amount) VALUES (" + (base + 11000) + ", 'd1', 7.0)");

      // Independent Oracle: W1 start == base, W1 count == 3, W2 start == base+5s, W2 count == 3.
      final long expectedW1Start = base;
      final long expectedW2Start = base + 5000L;
      final long expectedW1Count = 3L;
      final long expectedW2Count = 3L;

      SLOW_AWAIT.untilAsserted(
          () -> {
            boolean firstWindowNull = false;
            boolean secondWindowEqualsBase = false;
            final StringBuilder seen = new StringBuilder();
            try (ResultSet rs =
                stmt.executeQuery("SELECT time, prev, cnt FROM calc ORDER BY time")) {
              while (rs.next()) {
                final long time = rs.getLong("time");
                final long prev = rs.getLong("prev");
                // wasNull() must be consulted immediately after reading prev: it reflects the
                // last column read, so reading cnt first would report cnt's null-ness instead and
                // misread a NULL prev_time as 0.
                final boolean prevIsNull = rs.wasNull();
                final long cnt = rs.getLong("cnt");
                seen.append("(")
                    .append(time)
                    .append(",")
                    .append(prevIsNull ? "NULL" : prev)
                    .append(",")
                    .append(cnt)
                    .append(") ");
                if (prevIsNull && time == expectedW1Start && cnt == expectedW1Count) {
                  firstWindowNull = true;
                }
                if (!prevIsNull
                    && time == expectedW2Start
                    && prev == expectedW1Start
                    && cnt == expectedW2Count) {
                  secondWindowEqualsBase = true;
                }
              }
            }
            // (a) the first window must have a NULL prev_time.
            assertTrue(
                "the first TUMBLE window (start="
                    + expectedW1Start
                    + ") must have a NULL prev_time; rows seen: "
                    + seen,
                firstWindowNull);
            // (b) the second window's prev_time must equal the first window's start (base).
            assertTrue(
                "the second TUMBLE window (start="
                    + expectedW2Start
                    + ") prev_time must equal "
                    + expectedW1Start
                    + "; rows seen: "
                    + seen,
                secondWindowEqualsBase);
          });
    }
  }

  /**
   * SQL-10 {@code ${next_time}} for both PERIOD (system-triggered) and TUMBLE (event-triggered)
   * windows: each trigger/window must also output the next trigger time (the next system trigger
   * time for PERIOD, the next event trigger time for TUMBLE). For a PERIOD(p) trigger at time T the
   * next_time must be T + p (the next scheduled system trigger); for a TUMBLE(5s, base) window the
   * next_time must be the window end (base + 5s for the first window).
   *
   * <p>Expected values: for the TUMBLE part, {@code TumbleWindowEngine.createEventInfo} sets {@code
   * nextTime = end}, so W1.next_time == W1.end == base + 5000, hand-computed from the window
   * origin. For the PERIOD part, the next system trigger is exactly one period after the current
   * trigger, so next_time == start_time + periodMs by construction.
   */
  @Test
  public void testSQL10NextTimePeriodAndTumble() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      stmt.execute("CREATE DATABASE " + DB10);
      stmt.execute("USE " + DB10);
      stmt.execute(
          "CREATE TABLE t1(time TIMESTAMP TIME, device_id STRING TAG, amount FLOAT FIELD)");
      // next_time resolves to a TIMESTAMP-typed value (per the analyzer), so the sink columns must
      // be TIMESTAMP attributes (a table may have only one TIME column).
      stmt.execute(
          "CREATE TABLE calc_t(time TIMESTAMP TIME, next TIMESTAMP FIELD, cnt INT64 FIELD)");
      stmt.execute("CREATE TABLE calc_p(time TIMESTAMP TIME, next TIMESTAMP FIELD)");

      // --- TUMBLE (event window) part: next_time must equal the window end ---
      // Align window boundaries to a near-future origin so inserted rows land deterministically in
      // W1 = [base, base+5s).
      final long base = System.currentTimeMillis() + 2000;
      final long expectedW1Next = base + 5000L;

      // Compute task reads the trigger table (${ROWS}) and emits the window start, next_time and
      // the
      // per-window row count. FROM ${ROWS} keeps this on the event-window path.
      stmt.execute(
          "CREATE STREAM sql10_t "
              + "FROM t1 TUMBLE(5s, "
              + base
              + ") INTO calc_t(time, next, cnt) "
              + "SELECT ${START_TIME}, ${next_time}, count(*) FROM ${ROWS}");
      stmt.execute("START STREAM sql10_t");

      // All rows use one device: distinct TAG values map to different tablets whose delivery
      // order is not guaranteed (B-8); a single device keeps batches ordered by commit time.
      // Three rows in W1.
      stmt.execute(
          "INSERT INTO t1(time, device_id, amount) VALUES (" + (base + 500) + ", 'd1', 1.0)");
      stmt.execute(
          "INSERT INTO t1(time, device_id, amount) VALUES (" + (base + 1500) + ", 'd1', 2.0)");
      stmt.execute(
          "INSERT INTO t1(time, device_id, amount) VALUES (" + (base + 2500) + ", 'd1', 3.0)");
      // One row in W2 advances the watermark and closes W1, triggering its computation.
      stmt.execute(
          "INSERT INTO t1(time, device_id, amount) VALUES (" + (base + 6000) + ", 'd1', 4.0)");

      // Independent Oracle: W1 start == base, W1 next_time == base + 5000, W1 count == 3.
      final long expectedW1Start = base;
      final long expectedW1Count = 3L;

      AWAIT.untilAsserted(
          () -> {
            boolean tumbleNextMatches = false;
            final StringBuilder seen = new StringBuilder();
            try (ResultSet rs =
                stmt.executeQuery("SELECT time, next, cnt FROM calc_t ORDER BY time")) {
              while (rs.next()) {
                final long time = rs.getLong("time");
                final long next = rs.getLong("next");
                final long cnt = rs.getLong("cnt");
                seen.append("(")
                    .append(time)
                    .append(",")
                    .append(next)
                    .append(",")
                    .append(cnt)
                    .append(") ");
                // The first window's next_time must equal its end (base + 5000).
                if (time == expectedW1Start && next == expectedW1Next && cnt == expectedW1Count) {
                  tumbleNextMatches = true;
                }
              }
            }
            assertTrue(
                "TUMBLE W1 row must have next_time == window end ("
                    + expectedW1Next
                    + "); rows seen: "
                    + seen,
                tumbleNextMatches);
          });

      // --- PERIOD (system-triggered) part: next_time must equal the next system trigger (start +
      // p)
      final long periodMs = 2000L;
      final long origin = System.currentTimeMillis() + 1500;
      // PERIOD is system-triggered: the calc plan scans a plain table (FROM t1), not ${ROWS}.
      stmt.execute(
          "CREATE STREAM sql10_p "
              + "PERIOD(2s, "
              + origin
              + ") INTO calc_p(time, next) "
              + "SELECT ${START_TIME}, ${next_time} FROM t1");
      stmt.execute("START STREAM sql10_p");

      // A row is written on each system trigger; next_time must equal start_time + period.
      AWAIT.untilAsserted(
          () -> {
            boolean periodNextMatches = false;
            final StringBuilder seen = new StringBuilder();
            try (ResultSet rs = stmt.executeQuery("SELECT time, next FROM calc_p ORDER BY time")) {
              while (rs.next()) {
                final long time = rs.getLong("time");
                final long next = rs.getLong("next");
                seen.append("(").append(time).append(",").append(next).append(") ");
                if (next == time + periodMs) {
                  periodNextMatches = true;
                }
              }
            }
            assertTrue(
                "PERIOD row must have next_time == start_time + period ("
                    + periodMs
                    + "); rows seen: "
                    + seen,
                periodNextMatches);
          });
    }
  }

  /**
   * SQL-11 {@code ${start_time}} / {@code ${end_time}} for both PERIOD (system-triggered) and
   * TUMBLE (event-triggered) windows: each trigger/window must output its own start and end
   * timestamps, consistent with the current trigger's boundaries. For a TUMBLE(5s, base) window the
   * start must be the window origin (base) and the end must be the window end (base + 5s); for a
   * PERIOD(p) trigger at time T the window is [T, T + p) so the end must be T + p.
   *
   * <p>Expected values: the TUMBLE W1 start == base and end == base + 5000 are hand-computed from
   * the window origin and size. For PERIOD the end is exactly one period after the trigger (start),
   * so end == start + periodMs by construction.
   */
  @Test
  public void testSQL11StartEndTimePeriodAndTumble() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      stmt.execute("CREATE DATABASE " + DB11);
      stmt.execute("USE " + DB11);
      stmt.execute(
          "CREATE TABLE t1(time TIMESTAMP TIME, device_id STRING TAG, amount FLOAT FIELD)");
      // start_time / end_time resolve to TIMESTAMP-typed values, so the extra sink columns must be
      // TIMESTAMP attributes (a table may have only one TIME column).
      stmt.execute(
          "CREATE TABLE calc_st(time TIMESTAMP TIME, endt TIMESTAMP FIELD, cnt INT64 FIELD)");
      stmt.execute("CREATE TABLE calc_sp(time TIMESTAMP TIME, endt TIMESTAMP FIELD)");

      // --- TUMBLE (event window) part: start_time == window origin, end_time == window end ---
      // Align window boundaries to a near-future origin so inserted rows land deterministically in
      // W1 = [base, base+5s).
      final long base = System.currentTimeMillis() + 2000;
      final long expectedW1End = base + 5000L;

      // FROM ${ROWS} keeps this on the event-window path. ${START_TIME} lands in the
      // sink TIME column (time); ${END_TIME} lands in the endt attribute column.
      stmt.execute(
          "CREATE STREAM sql11_t "
              + "FROM t1 TUMBLE(5s, "
              + base
              + ") INTO calc_st(time, endt, cnt) "
              + "SELECT ${START_TIME}, ${END_TIME}, count(*) FROM ${ROWS}");
      stmt.execute("START STREAM sql11_t");

      // All rows use one device: distinct TAG values map to different tablets whose delivery
      // order is not guaranteed (B-8); a single device keeps batches ordered by commit time.
      // Three rows in W1.
      stmt.execute(
          "INSERT INTO t1(time, device_id, amount) VALUES (" + (base + 500) + ", 'd1', 1.0)");
      stmt.execute(
          "INSERT INTO t1(time, device_id, amount) VALUES (" + (base + 1500) + ", 'd1', 2.0)");
      stmt.execute(
          "INSERT INTO t1(time, device_id, amount) VALUES (" + (base + 2500) + ", 'd1', 3.0)");
      // One row in W2 advances the watermark and closes W1, triggering its computation.
      stmt.execute(
          "INSERT INTO t1(time, device_id, amount) VALUES (" + (base + 6000) + ", 'd1', 4.0)");

      // Independent Oracle: W1 start == base, W1 end == base + 5000, W1 count == 3.
      final long expectedW1Start = base;
      final long expectedW1Count = 3L;

      AWAIT.untilAsserted(
          () -> {
            boolean tumbleBoundsMatch = false;
            final StringBuilder seen = new StringBuilder();
            try (ResultSet rs =
                stmt.executeQuery("SELECT time, endt, cnt FROM calc_st ORDER BY time")) {
              while (rs.next()) {
                final long time = rs.getLong("time");
                final long endt = rs.getLong("endt");
                final long cnt = rs.getLong("cnt");
                seen.append("(")
                    .append(time)
                    .append(",")
                    .append(endt)
                    .append(",")
                    .append(cnt)
                    .append(") ");
                // start_time (time) == window origin; end_time (endt) == window end (base + 5000).
                if (time == expectedW1Start && endt == expectedW1End && cnt == expectedW1Count) {
                  tumbleBoundsMatch = true;
                }
              }
            }
            assertTrue(
                "TUMBLE W1 row must have start_time (time) == "
                    + expectedW1Start
                    + " and end_time == window end ("
                    + expectedW1End
                    + "); rows seen: "
                    + seen,
                tumbleBoundsMatch);
          });

      // --- PERIOD (system-triggered) part: end_time == start_time + period ---
      final long periodMs = 2000L;
      final long origin = System.currentTimeMillis() + 1500;
      // PERIOD is system-triggered: the calc plan scans a plain table (FROM t1), not ${ROWS}.
      stmt.execute(
          "CREATE STREAM sql11_p "
              + "PERIOD(2s, "
              + origin
              + ") INTO calc_sp(time, endt) "
              + "SELECT ${START_TIME}, ${END_TIME} FROM t1");
      stmt.execute("START STREAM sql11_p");

      // A row is written on each system trigger; end_time must equal start_time + period.
      AWAIT.untilAsserted(
          () -> {
            boolean periodBoundsMatch = false;
            final StringBuilder seen = new StringBuilder();
            try (ResultSet rs = stmt.executeQuery("SELECT time, endt FROM calc_sp ORDER BY time")) {
              while (rs.next()) {
                final long time = rs.getLong("time");
                final long endt = rs.getLong("endt");
                seen.append("(").append(time).append(",").append(endt).append(") ");
                if (endt == time + periodMs) {
                  periodBoundsMatch = true;
                }
              }
            }
            assertTrue(
                "PERIOD row must have end_time == start_time + period ("
                    + periodMs
                    + "); rows seen: "
                    + seen,
                periodBoundsMatch);
          });
    }
  }

  /**
   * SQL-12 {@code ${row_num}} for TUMBLE (event window): the placeholder must return the record
   * count of the current triggered window's dataset, across windows that emit a single row and
   * windows that emit multiple rows. The scenario inserts three rows into W1 and one row into W2,
   * and asserts that each window's output carries {@code row_num} equal to the number of rows it
   * actually received.
   *
   * <p>Expected values: {@code TumbleWindowEngine.createEventInfo} sets {@code rowCount =
   * getPendingRowCount()}, and {@code StreamPlaceholderColumnTransformer} maps {@code ROW_NUM ->
   * currentEventInfo.getRowCount()} as an INT64 constant column. So for a window that received N
   * source rows, every output row's {@code row_num} column must equal N (3 for W1, 1 for W2),
   * hand-computed from the inserted data and the window boundaries. {@code ${row_num}} is only
   * valid for TUMBLE windows, so this test uses TUMBLE.
   */
  @Test
  public void testSQL12RowNumTumble() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      stmt.execute("CREATE DATABASE " + DB12);
      stmt.execute("USE " + DB12);
      stmt.execute(
          "CREATE TABLE t1(time TIMESTAMP TIME, device_id STRING TAG, amount FLOAT FIELD)");
      // ${row_num} resolves to an INT64 value, so the sink column must be INT64 FIELD.
      stmt.execute("CREATE TABLE calc(time TIMESTAMP TIME, rn INT64 FIELD, cnt INT64 FIELD)");

      // Align window boundaries to a near-future origin so inserted rows land deterministically:
      //   W1 = [base, base+5000): 3 rows.
      //   W2 = [base+5000, base+10000): 1 row (also closes W1, triggering its calc).
      //   W3 = [base+10000, ...): 1 row (closes W2, triggering its calc).
      final long base = System.currentTimeMillis() + 2000;
      final long w1Start = base;
      final long w2Start = base + 5000L;

      // With count(*) (proven pattern from SQL-09/SQL-10) each TUMBLE window emits exactly one row
      // carrying window-level constants. So W1 emits 1 row (rn==3) and W2 emits 1 row (rn==1) —
      // covering both a multi-row window and a single-row window with a deterministic output
      // shape.
      stmt.execute(
          "CREATE STREAM sql12 "
              + "FROM t1 TUMBLE(5s, "
              + base
              + ") INTO calc(time, rn, cnt) "
              + "SELECT ${START_TIME}, ${row_num}, count(*) FROM ${ROWS}");
      stmt.execute("START STREAM sql12");

      // All rows use one device: distinct TAG values map to different tablets whose delivery
      // order is not guaranteed (B-8); a single device keeps batches ordered by commit time.
      // 3 rows in W1 (proven reliable in SQL-10's TUMBLE half, which asserted count(*)=3).
      stmt.execute(
          "INSERT INTO t1(time, device_id, amount) VALUES (" + (base + 500) + ", 'd1', 1.0)");
      stmt.execute(
          "INSERT INTO t1(time, device_id, amount) VALUES (" + (base + 1500) + ", 'd1', 2.0)");
      stmt.execute(
          "INSERT INTO t1(time, device_id, amount) VALUES (" + (base + 2500) + ", 'd1', 3.0)");
      // 1 row in W2 — closes W1 and triggers its calc (row_num must equal 3).
      stmt.execute(
          "INSERT INTO t1(time, device_id, amount) VALUES (" + (base + 5500) + ", 'd1', 4.0)");
      // 1 row in W3 closes W2 and triggers its calc (row_num must equal 1).
      stmt.execute(
          "INSERT INTO t1(time, device_id, amount) VALUES (" + (base + 11000) + ", 'd1', 5.0)");

      AWAIT.untilAsserted(
          () -> {
            int w1Rows = 0;
            int w2Rows = 0;
            long w1Rn = -1;
            long w2Rn = -1;
            long w1Cnt = -1;
            long w2Cnt = -1;
            final StringBuilder seen = new StringBuilder();
            try (ResultSet rs = stmt.executeQuery("SELECT time, rn, cnt FROM calc ORDER BY time")) {
              while (rs.next()) {
                final long time = rs.getLong("time");
                final long rn = rs.getLong("rn");
                final long cnt = rs.getLong("cnt");
                seen.append("(")
                    .append(time)
                    .append(",")
                    .append(rn)
                    .append(",")
                    .append(cnt)
                    .append(") ");
                if (time == w1Start) {
                  w1Rows++;
                  w1Rn = rn;
                  w1Cnt = cnt;
                } else if (time == w2Start) {
                  w2Rows++;
                  w2Rn = rn;
                  w2Cnt = cnt;
                }
              }
            }
            // W1 (3 source rows) must emit exactly one row with row_num == 3 == count(*).
            assertTrue(
                "W1 (start=" + w1Start + ") must emit 1 row with row_num==3; rows seen: " + seen,
                w1Rows == 1 && w1Rn == 3L && w1Cnt == 3L);
            // W2 (1 source row) must emit exactly one row with row_num == 1 == count(*).
            assertTrue(
                "W2 (start=" + w2Start + ") must emit 1 row with row_num==1; rows seen: " + seen,
                w2Rows == 1 && w2Rn == 1L && w2Cnt == 1L);
          });
    }
  }

  /**
   * SQL-13 {@code ${n}} group-index placeholder: with {@code PARTITION BY city, device_id}, the
   * SELECT uses {@code ${1}} and {@code ${2}} to reference the 1st and 2nd partition TAG
   * respectively, with a 1-based index. The scenario inserts one group whose city and device_id are
   * deliberately different strings, so the assertion proves (a) {@code ${1}} resolves to the 1st
   * partition column (city), not a 0-based off-by-one, and (b) {@code ${2}} resolves to the 2nd
   * partition column (device_id), confirming order.
   *
   * <p>Expected values: {@code WindowEventInfo.getN(int n)} returns {@code
   * partitionKey.segmentValue(n - 1)}, i.e. a 1-based index into the {@code PARTITION BY} columns;
   * and {@code StreamPlaceholderColumnTransformer} maps {@code N -> currentEventInfo.getN(n)}. So
   * for {@code PARTITION BY city, device_id}, {@code ${1}} must equal the city value and {@code
   * ${2}} the device_id value. The expected strings ('bj', 'd1') are taken from the inserted data.
   * {@code ${n}} is only valid for the event window (TUMBLE), so this test uses TUMBLE.
   */
  @Test
  public void testSQL13GroupIndexPlaceholder() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      createDatabase(stmt);
      stmt.execute(
          "CREATE TABLE t1(time TIMESTAMP TIME, city STRING TAG, device_id STRING TAG, amount DOUBLE FIELD)");
      stmt.execute(
          "CREATE TABLE calc(time TIMESTAMP TIME, c1 STRING FIELD, c2 STRING FIELD, cnt INT64 FIELD)");

      // Align window boundaries to a near-future origin so inserted rows land deterministically:
      //   W1 = [base, base+5000): 3 rows, all in the same group (city='bj', device_id='d1').
      //   W2 = [base+5000, base+10000): 1 row, a different group (closes W1 when its watermark
      //        advances).
      final long base = System.currentTimeMillis() + 2000;
      final long w1Start = base;

      // With count(*) each partition group emits exactly one row; ${1}/${2} carry the group's
      // 1st/2nd partition TAG values.
      stmt.execute(
          "CREATE STREAM sql13 "
              + "FROM t1 PARTITION BY city, device_id TUMBLE(5s, "
              + base
              + ") INTO calc(time, c1, c2, cnt) "
              + "SELECT ${START_TIME}, ${1}, ${2}, count(*) FROM ${ROWS}");
      stmt.execute("START STREAM sql13");

      // Single partition group (city='bj', device_id='d1'): 3 rows in W1, plus one closing
      // row in W2 that advances THIS group's watermark and triggers the W1 window.
      // (PARTITION BY windows fire per-group: a group needs a later row of its own to close.)
      // NOTE: the calc sink row's time is ${START_TIME}=w1Start for every group's W1, so
      // multiple groups would collide on the TIME primary key (same timestamp overwrites).
      // We therefore use one group, which still fully validates ${1}/${2} (1-based, ordered).
      stmt.execute(
          "INSERT INTO t1(time, city, device_id, amount) VALUES ("
              + (base + 500)
              + ", 'bj', 'd1', 1.0)");
      stmt.execute(
          "INSERT INTO t1(time, city, device_id, amount) VALUES ("
              + (base + 1500)
              + ", 'bj', 'd1', 2.0)");
      stmt.execute(
          "INSERT INTO t1(time, city, device_id, amount) VALUES ("
              + (base + 2500)
              + ", 'bj', 'd1', 3.0)");
      stmt.execute(
          "INSERT INTO t1(time, city, device_id, amount) VALUES ("
              + (base + 6000)
              + ", 'bj', 'd1', 4.0)");

      // Independent Oracle: the single W1 group is (city='bj', device_id='d1') with 3 rows.
      // ${1} must resolve to the 1st partition column (city='bj') and ${2} to the 2nd
      // (device_id='d1'), confirming a 1-based index (under 0-based, ${1} would be 'd1').
      AWAIT.untilAsserted(
          () -> {
            boolean w1Matches = false;
            int rows = 0;
            final StringBuilder seen = new StringBuilder();
            try (ResultSet rs =
                stmt.executeQuery("SELECT time, c1, c2, cnt FROM calc ORDER BY time")) {
              while (rs.next()) {
                final long time = rs.getLong("time");
                final String c1 = rs.getString("c1");
                final String c2 = rs.getString("c2");
                final long cnt = rs.getLong("cnt");
                rows++;
                seen.append("(")
                    .append(time)
                    .append(",")
                    .append(c1)
                    .append(",")
                    .append(c2)
                    .append(",")
                    .append(cnt)
                    .append(") ");
                if (time == w1Start && "bj".equals(c1) && "d1".equals(c2) && cnt == 3L) {
                  w1Matches = true;
                }
              }
            }
            // Exactly one W1 row carrying the correct 1-based partition TAGs.
            assertTrue(
                "W1 group row must carry ${1}=city('bj'), ${2}=device_id('d1'), cnt==3; rows seen: "
                    + seen,
                w1Matches && rows == 1);
          });
    }
  }

  /**
   * SQL-14 {@code ${rows}} legal position: {@code ${rows}} is the reference to the trigger table's
   * per-group, per-trigger dataset and may ONLY be used as a query table name ({@code FROM
   * ${rows}}). The window is scoped so that the aggregation only sees the rows belonging to the
   * current window (and current group) of this trigger — not all rows ever written to the table.
   *
   * <p>{@code ${rows}} references the trigger table's per-group dataset for the current trigger,
   * and is only usable as {@code FROM ${rows}}. We insert 3 rows into W1 and 2 rows into W2 of a
   * single (implicit) group, then a 1-row W3 that advances the watermark and closes W2. The
   * expectation is therefore: W1 emits exactly one row with {@code count(*)=3} (the 3 W1 rows only,
   * NOT the 5 total rows), and W2 emits exactly one row with {@code count(*)=2} (the 2 W2 rows
   * only). The counts come from the inserted-row counts.
   */
  @Test
  public void testSQL14RowsLegalPosition() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      stmt.execute("CREATE DATABASE " + DB14);
      stmt.execute("USE " + DB14);
      stmt.execute("CREATE TABLE t1(time TIMESTAMP TIME, amount DOUBLE FIELD)");
      stmt.execute("CREATE TABLE calc(time TIMESTAMP TIME, cnt INT64 FIELD)");

      // W1 = [base, base+5000): 3 rows ; W2 = [base+5000, base+10000): 2 rows ;
      // W3 = [base+10000, base+15000): 1 closing row that advances the watermark and fires W2.
      final long base = System.currentTimeMillis() + 2000;
      final long w1Start = base;
      final long w2Start = base + 5000;

      // ${rows} is used strictly as the FROM table in the SELECT clause; the stream source is the
      // real table t1. count(*) proves the dataset is window-scoped (current window only).
      stmt.execute(
          "CREATE STREAM sql14 "
              + "FROM t1 TUMBLE(5s, "
              + base
              + ") INTO calc(time, cnt) "
              + "SELECT ${START_TIME}, count(*) FROM ${ROWS}");
      stmt.execute("START STREAM sql14");

      // W1: 3 rows.
      stmt.execute("INSERT INTO t1(time, amount) VALUES (" + (base + 500) + ", 1.0)");
      stmt.execute("INSERT INTO t1(time, amount) VALUES (" + (base + 1500) + ", 2.0)");
      stmt.execute("INSERT INTO t1(time, amount) VALUES (" + (base + 2500) + ", 3.0)");
      // W2: 2 rows.
      stmt.execute("INSERT INTO t1(time, amount) VALUES (" + (base + 6000) + ", 4.0)");
      stmt.execute("INSERT INTO t1(time, amount) VALUES (" + (base + 6500) + ", 5.0)");
      // W3: 1 closing row -> fires W2 (watermark passes base+10000).
      stmt.execute("INSERT INTO t1(time, amount) VALUES (" + (base + 11000) + ", 6.0)");

      // Independent Oracle: W1 -> 3 rows, W2 -> 2 rows (each window counts only its own rows).
      final long expectedW1Count = 3L;
      final long expectedW2Count = 2L;

      AWAIT.untilAsserted(
          () -> {
            boolean w1Matches = false;
            boolean w2Matches = false;
            int rows = 0;
            final StringBuilder seen = new StringBuilder();
            try (ResultSet rs = stmt.executeQuery("SELECT time, cnt FROM calc ORDER BY time")) {
              while (rs.next()) {
                final long time = rs.getLong("time");
                final long cnt = rs.getLong("cnt");
                rows++;
                seen.append("(").append(time).append(",").append(cnt).append(") ");
                if (time == w1Start && cnt == expectedW1Count) {
                  w1Matches = true;
                }
                if (time == w2Start && cnt == expectedW2Count) {
                  w2Matches = true;
                }
              }
            }
            // ${rows} legal in FROM; dataset scoped to current window (W1=3 not 5, W2=2).
            assertTrue(
                "calc must carry W1 count==3 and W2 count==2 (window-scoped ${rows}); rows seen: "
                    + seen,
                w1Matches && w2Matches && rows == 2);
          });
    }
  }

  /**
   * SQL-15 {@code ${rows}} in a non-FROM position: the statement must be rejected when {@code
   * ${rows}} is used anywhere other than the {@code FROM} clause of the calc query. Its only legal
   * position is {@code FROM ${rows}} (see SQL-14).
   *
   * <p>Grammar signal: in {@code RelationalSql.g4}, {@code '$' '{' ROWS '}'} only parses as a
   * {@code streamRowsRelation} (a table reference) in the FROM/relation context. A {@code ${rows}}
   * appearing in the SELECT item list would have to parse as a {@code streamPlaceholder}, but that
   * rule's keyword set (PREV_VALUE, CURRENT_VALUE, PREV_TIME, NEXT_TIME, START_TIME, END_TIME,
   * ROW_NUM, INTEGER_VALUE) does NOT include ROWS, so the parser rejects it (700). The oracle is
   * the rejection, asserted as "throws" plus "not registered".
   *
   * <p>The statement is deliberately shaped exactly like the legal SQL-14 (source {@code FROM t1},
   * legal {@code FROM ${rows}} in the calc query) plus one extra illegal {@code ${rows}} in the
   * SELECT list, so the only thing that can reject it is the illegal position of {@code ${rows}}.
   */
  @Test
  public void testSQL15RowsInSelectList() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      stmt.execute("CREATE DATABASE " + DB15);
      stmt.execute("USE " + DB15);
      stmt.execute("CREATE TABLE t1(time TIMESTAMP TIME, amount DOUBLE FIELD)");
      stmt.execute("CREATE TABLE calc(time TIMESTAMP TIME, cnt INT64 FIELD)");

      // ${rows} appears in the SELECT item list (a non-FROM position) while the rest of the
      // statement is exactly the legal SQL-14 shape. This must be rejected at creation.
      assertThrows(
          Exception.class,
          () ->
              stmt.execute(
                  "CREATE STREAM sql15 "
                      + "FROM t1 TUMBLE(5s, "
                      + (System.currentTimeMillis() + 2000)
                      + ") INTO calc(time, cnt) "
                      + "SELECT ${ROWS}, ${START_TIME}, count(*) FROM ${ROWS}"));
      assertFalse(
          "sql15 must not be registered after the rejected CREATE STREAM (${rows} in SELECT list)",
          streamExists(stmt, "sql15"));
    }
  }

  /**
   * SQL-16 (negative): {@code ${rows}} with a WHERE clause that filters a column NOT in the
   * subscription must be rejected at creation or execution.
   *
   * <p>The calc plan reads from {@code ${ROWS}}; we add an ATTRIBUTE column {@code model} to the
   * trigger table because an ATTRIBUTE never enters the trigger subscription dataset, so it is
   * unambiguously a non-subscribed column. The WHERE filters {@code model = 'x'}.
   *
   * <p>The rejection may happen at either stage. The test first attempts the CREATE — if it throws,
   * the stream must not be registered and the case is satisfied via the creation path. If the
   * CREATE is accepted (the currently observed behavior: the CREATE completes without throwing even
   * though {@code model} never enters the {@code ${ROWS}} dataset, i.e. no column-presence
   * validation exists at creation), the rejection must then happen at execution: a control stream
   * with the same trigger data but a legal compute task proves the subscription/trigger/window
   * machinery fires, and the violating task must never produce a sink row. Any row appearing in
   * {@code calc} means the WHERE on the non-subscribed column was neither rejected at creation nor
   * at execution.
   *
   * <p>Note: in the current implementation the execution-time refusal takes the form of a retried
   * {@code NullPointerException} in {@code WindowDataBlockBuilder.buildTsBlockColumnMapping} (the
   * column-mapping lookup for {@code model} misses because it is not in the subscription dataset),
   * producing no output. This test guards the observable invariant "the violating WHERE never
   * produces output"; if the engine ever stops failing on missing columns and silently executes the
   * WHERE, a calc row appears and this test turns red.
   */
  @Test
  public void testSQL16RowsWithWhere() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      stmt.execute("CREATE DATABASE " + DB16);
      stmt.execute("USE " + DB16);
      // The calc plan reads from ${ROWS}. The WHERE must filter a column that is NOT subscribed:
      // an ATTRIBUTE column never enters the trigger subscription dataset, so 'model' (STRING
      // ATTRIBUTE) is unambiguously a non-subscribed column.
      stmt.execute(
          "CREATE TABLE t1(time TIMESTAMP TIME, id STRING TAG, amount DOUBLE FIELD, model STRING ATTRIBUTE)");
      stmt.execute("CREATE TABLE calc(time TIMESTAMP TIME, cnt INT64 FIELD)");
      // Control sink: a legal compute task over the same trigger data, proving the window
      // machinery fires for this exact data when the violating task produces nothing.
      stmt.execute("CREATE TABLE calc_control(time TIMESTAMP TIME, cnt INT64 FIELD)");
      final long base = System.currentTimeMillis() + 2000;

      // Stage 1 — creation: if the CREATE is rejected, the requirement is satisfied via the
      // creation path; the stream must not be registered and the case is done.
      boolean creationRejected = false;
      try {
        stmt.execute(
            "CREATE STREAM sql16 FROM t1 TUMBLE(5s, "
                + base
                + ") INTO calc(time, cnt) SELECT ${START_TIME}, count(*) FROM ${ROWS} WHERE model = 'x'");
      } catch (SQLException e) {
        creationRejected = true;
      }
      if (creationRejected) {
        assertFalse(
            "sql16 must not be registered after the rejected CREATE STREAM (${rows} with WHERE)",
            streamExists(stmt, "sql16"));
        return;
      }

      // Stage 2 — execution: the CREATE was accepted, so the rejection must happen at execution.
      // A START-time refusal already counts.
      boolean startRejected = false;
      try {
        stmt.execute("START STREAM sql16");
      } catch (SQLException e) {
        startRejected = true;
      }

      // Control stream: same trigger table and window, legal compute task (no WHERE).
      stmt.execute(
          "CREATE STREAM sql16_control FROM t1 TUMBLE(5s, "
              + base
              + ") INTO calc_control(time, cnt) SELECT ${START_TIME}, count(*) FROM ${ROWS}");
      stmt.execute("START STREAM sql16_control");

      // Three rows in W1 = [base, base+5s) carrying model='x' (a matching value, so a bogusly
      // executed WHERE would still emit a row), then one row in W2 to advance the watermark and
      // close W1 — identical trigger data for both streams.
      stmt.execute(
          "INSERT INTO t1(time, id, amount, model) VALUES (" + (base + 500) + ", 'd1', 1.0, 'x')");
      stmt.execute(
          "INSERT INTO t1(time, id, amount, model) VALUES (" + (base + 1500) + ", 'd2', 2.0, 'x')");
      stmt.execute(
          "INSERT INTO t1(time, id, amount, model) VALUES (" + (base + 2500) + ", 'd3', 3.0, 'x')");
      stmt.execute(
          "INSERT INTO t1(time, id, amount, model) VALUES (" + (base + 6000) + ", 'd1', 4.0, 'x')");

      // The control sink must receive the W1 aggregate — proving the subscription, trigger and
      // window pipeline all work for this exact data, so an empty calc cannot be blamed on the
      // machinery.
      AWAIT.untilAsserted(
          () -> {
            boolean controlFired = false;
            try (ResultSet rs = stmt.executeQuery("SELECT time, cnt FROM calc_control")) {
              while (rs.next()) {
                if (rs.getLong("time") == base && rs.getLong("cnt") == 3L) {
                  controlFired = true;
                }
              }
            }
            assertTrue(
                "control stream must produce the W1 aggregate (time==base, cnt==3) for the same"
                    + " trigger data",
                controlFired);
          });

      // Grace period (2s: the violating task gets the same trigger data at the
      // same time as the control stream, so any illegal row would show up within
      // the same processing latency), then the violating task must never have
      // produced a sink row: its WHERE on the non-subscribed model column must be
      // rejected at execution.
      Thread.sleep(2000);
      try (ResultSet rs = stmt.executeQuery("SELECT time, cnt FROM calc")) {
        assertFalse(
            "sql16 must not produce any calc row: the WHERE filtering the non-subscribed"
                + " ATTRIBUTE column model must be rejected at creation or execution;"
                + " creation was accepted and START was "
                + (startRejected ? "rejected" : "accepted")
                + ", so execution must refuse the task",
            rs.next());
      }
    }
  }

  /**
   * SQL-17 (negative): joining {@code ${rows}} with another table is not supported. The {@code
   * ${rows}} placeholder may only appear in FROM as the window dataset itself; any JOIN
   * participation (either {@code ${rows} JOIN ${rows}} or {@code ${rows} JOIN plain-table}) must be
   * rejected at creation.
   *
   * <p>We attempt {@code CREATE STREAM sql17 ... SELECT ${START_TIME}, sum(t2.w) FROM ${ROWS} r
   * JOIN t2 ON r.id = t2.id}; the DDL must throw and the stream must not be registered. The {@code
   * ${rows} JOIN ${rows}} form is covered separately by SQL-30.
   */
  @Test
  public void testSQL17RowsJoin() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      stmt.execute("CREATE DATABASE " + DB17);
      stmt.execute("USE " + DB17);
      stmt.execute("CREATE TABLE t1(time TIMESTAMP TIME, id STRING TAG, amount DOUBLE FIELD)");
      stmt.execute("CREATE TABLE t2(time TIMESTAMP TIME, id STRING TAG, w INT64 FIELD)");
      stmt.execute("CREATE TABLE calc(time TIMESTAMP TIME, s DOUBLE FIELD)");
      final long base = System.currentTimeMillis() + 2000;

      // ${rows} JOIN plain-table is not supported: CREATE must be rejected and the stream
      // must not be registered. (The ${rows} JOIN ${rows} form is also disallowed; see SQL-30.)
      assertThrows(
          SQLException.class,
          () ->
              stmt.execute(
                  "CREATE STREAM sql17 FROM t1 TUMBLE(5s, "
                      + base
                      + ") INTO calc(time, s) "
                      + "SELECT ${START_TIME}, sum(t2.w) FROM ${ROWS} r JOIN t2 ON r.id = t2.id"));
      assertFalse(
          "sql17 must not be registered: ${rows} JOIN plain-table must be rejected at creation",
          streamExists(stmt, "sql17"));
    }
  }

  /**
   * SQL-18 (negative): a value placeholder (here {@code ${START_TIME}}, representing the time / n /
   * row_num family) placed in a clause other than SELECT / WHERE must be rejected at creation or
   * execution.
   *
   * <p>We place {@code ${START_TIME}} in the INTO target-column list (an identifier position, not a
   * value-expression position); the DDL must be rejected.
   */
  @Test
  public void testSQL18PlaceholderInIllegalClause() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      stmt.execute("CREATE DATABASE " + DB18);
      stmt.execute("USE " + DB18);
      stmt.execute("CREATE TABLE t1(time TIMESTAMP TIME, id STRING TAG, amount DOUBLE FIELD)");
      stmt.execute("CREATE TABLE calc(time TIMESTAMP TIME, cnt INT64 FIELD)");
      final long base = System.currentTimeMillis() + 2000;

      // ${START_TIME} as a target column name in INTO (illegal: identifier position).
      assertThrows(
          SQLException.class,
          () ->
              stmt.execute(
                  "CREATE STREAM sql18 FROM t1 TUMBLE(5s, "
                      + base
                      + ") INTO calc(${START_TIME}, cnt) SELECT ${START_TIME}, count(*) FROM ${ROWS}"));
      assertFalse(
          "sql18 must not be registered after the rejected CREATE STREAM (${START_TIME} in INTO)",
          streamExists(stmt, "sql18"));
    }
  }

  /**
   * SQL-19 (negative): using TUMBLE-only placeholders ({@code ${row_num}}, {@code ${n}}, {@code
   * ${rows}}) inside a PERIOD (timer) stream must be rejected, producing no valid task.
   *
   * <p>{@code ${row_num}} / {@code ${n}} / {@code ${rows}} are only valid for TUMBLE event windows.
   */
  @Test
  public void testSQL19PeriodWithTumbleOnlyPlaceholder() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      stmt.execute("CREATE DATABASE " + DB19);
      stmt.execute("USE " + DB19);
      stmt.execute("CREATE TABLE t1(time TIMESTAMP TIME, city STRING TAG, amount DOUBLE FIELD)");
      stmt.execute(
          "CREATE TABLE calc(time TIMESTAMP TIME, rn INT64 FIELD, grp STRING FIELD, cnt INT64 FIELD)");
      final long base = System.currentTimeMillis() + 2000;

      // PERIOD stream referencing the three TUMBLE-only placeholders.
      assertThrows(
          SQLException.class,
          () ->
              stmt.execute(
                  "CREATE STREAM sql19 FROM t1 PERIOD(2s, "
                      + base
                      + ") PARTITION BY city INTO calc(time, rn, grp, cnt) SELECT ${START_TIME}, ${row_num}, ${1}, count(*) FROM ${ROWS}"));
      assertFalse(
          "sql19 must not be registered after the rejected CREATE STREAM (PERIOD + TUMBLE-only placeholders)",
          streamExists(stmt, "sql19"));
    }
  }

  /**
   * SQL-25 (negative): a group-index placeholder with an out-of-range index must be rejected at
   * creation or execution.
   *
   * <p>PARTITION BY city, device_id has 2 group columns, so valid indices are ${1} and ${2}. We use
   * ${0} (below 1) and ${3} (above the column count).
   */
  @Test
  public void testSQL25GroupIndexOutOfBounds() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      stmt.execute("CREATE DATABASE " + DB25);
      stmt.execute("USE " + DB25);
      stmt.execute(
          "CREATE TABLE t1(time TIMESTAMP TIME, city STRING TAG, device_id STRING TAG, amount DOUBLE FIELD)");
      stmt.execute(
          "CREATE TABLE calc(time TIMESTAMP TIME, c0 STRING FIELD, c3 STRING FIELD, cnt INT64 FIELD)");
      final long base = System.currentTimeMillis() + 2000;

      // ${0} (below lower bound) and ${3} (above the 2-column partition) are both illegal.
      assertThrows(
          SQLException.class,
          () ->
              stmt.execute(
                  "CREATE STREAM sql25 FROM t1 PARTITION BY city, device_id TUMBLE(5s, "
                      + base
                      + ") INTO calc(time, c0, c3, cnt) SELECT ${START_TIME}, ${0}, ${3}, count(*) FROM ${ROWS}"));
      assertFalse(
          "sql25 must not be registered after the rejected CREATE STREAM (out-of-range ${0}/${3})",
          streamExists(stmt, "sql25"));
    }
  }

  /**
   * SQL-26 (negative): a named placeholder that is not a numeric group index (e.g. {@code ${city}},
   * {@code ${device_id}}) must be rejected at creation or execution, without guessing column names:
   * group references may only use numeric indices such as {@code ${1}} and {@code ${2}}.
   */
  @Test
  public void testSQL26UndefinedNamedPlaceholder() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      stmt.execute("CREATE DATABASE " + DB26);
      stmt.execute("USE " + DB26);
      stmt.execute(
          "CREATE TABLE t1(time TIMESTAMP TIME, city STRING TAG, device_id STRING TAG, amount DOUBLE FIELD)");
      stmt.execute("CREATE TABLE calc(time TIMESTAMP TIME, c STRING FIELD, cnt INT64 FIELD)");
      final long base = System.currentTimeMillis() + 2000;

      // ${city} is a named placeholder; only ${1}/${2} numeric indices are allowed for grouping.
      assertThrows(
          SQLException.class,
          () ->
              stmt.execute(
                  "CREATE STREAM sql26 FROM t1 PARTITION BY city, device_id TUMBLE(5s, "
                      + base
                      + ") INTO calc(time, c, cnt) SELECT ${START_TIME}, ${city}, count(*) FROM ${ROWS}"));
      assertFalse(
          "sql26 must not be registered after the rejected CREATE STREAM (named placeholder ${city})",
          streamExists(stmt, "sql26"));
    }
  }

  /**
   * SQL-20 re-substitution per trigger: when several windows fire in a row, every legal placeholder
   * must be re-substituted with the window values of the trigger that fires it; the constants
   * computed for the first trigger must not be reused (start_time / end_time / row_num must change
   * with the window).
   *
   * <p>Expected values (hand-computed from the inserted data and the window semantics):
   *
   * <ul>
   *   <li>W1 starts at base with a 5s window, so end_time == base + 5000; W1 holds 3 rows, so
   *       row_num == cnt == 3.
   *   <li>W2 starts at base + 5000, so end_time == base + 10000; W2 holds 1 row, so row_num == cnt
   *       == 1.
   * </ul>
   *
   * If placeholders reused stale constants, the W2 row would wrongly carry base as start_time and 3
   * as row_num; the difference between the two rows directly disproves that.
   */
  @Test
  public void testSQL20ReSubstitutionPerTrigger() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      stmt.execute("CREATE DATABASE " + DB20);
      stmt.execute("USE " + DB20);
      // Single group, no PARTITION BY, to avoid TIME-primary-key collisions across groups.
      stmt.execute("CREATE TABLE t1(time TIMESTAMP TIME, amount DOUBLE FIELD)");
      stmt.execute(
          "CREATE TABLE calc(time TIMESTAMP TIME, st TIMESTAMP FIELD, et TIMESTAMP FIELD, rn INT64 FIELD, cnt INT64 FIELD)");
      final long base = System.currentTimeMillis() + 2000;

      // Start the stream BEFORE inserting (mirrors SQL-14): the subscription runs in tail mode and
      // only consumes rows written after the subscription is established. Inserting first would let
      // the 5 rows slip past the subscription and the calc table would stay empty.
      stmt.execute(
          "CREATE STREAM sql20 "
              + "FROM t1 TUMBLE(5s, "
              + base
              + ") INTO calc(time, st, et, rn, cnt) "
              + "SELECT ${START_TIME}, ${START_TIME}, ${END_TIME}, ${row_num}, count(*) FROM ${ROWS}");
      stmt.execute("START STREAM sql20");

      // W1 (3 rows) first; each window emits exactly 1 row via count(*).
      stmt.execute("INSERT INTO t1(time, amount) VALUES (" + (base + 500) + ", 1.0)");
      stmt.execute("INSERT INTO t1(time, amount) VALUES (" + (base + 1500) + ", 2.0)");
      stmt.execute("INSERT INTO t1(time, amount) VALUES (" + (base + 2500) + ", 3.0)");
      // W2 (1 row) advances watermark and closes W1.
      stmt.execute("INSERT INTO t1(time, amount) VALUES (" + (base + 6000) + ", 4.0)");
      // W3 (1 row) closes W2 so its computation actually fires.
      stmt.execute("INSERT INTO t1(time, amount) VALUES (" + (base + 11000) + ", 5.0)");

      final long w1Start = base;
      final long w1End = base + 5000L;
      final long w2Start = base + 5000L;
      final long w2End = base + 10000L;

      AWAIT.untilAsserted(
          () -> {
            boolean w1Matches = false;
            boolean w2Matches = false;
            final StringBuilder seen = new StringBuilder();
            try (ResultSet rs =
                stmt.executeQuery("SELECT time, st, et, rn, cnt FROM calc ORDER BY time")) {
              while (rs.next()) {
                final long time = rs.getLong("time");
                final long st = rs.getLong("st");
                final long et = rs.getLong("et");
                final long rn = rs.getLong("rn");
                final long cnt = rs.getLong("cnt");
                seen.append("(")
                    .append(time)
                    .append(",")
                    .append(st)
                    .append(",")
                    .append(et)
                    .append(",")
                    .append(rn)
                    .append(",")
                    .append(cnt)
                    .append(") ");
                if (time == w1Start) {
                  w1Matches = st == w1Start && et == w1End && rn == 3L && cnt == 3L;
                }
                if (time == w2Start) {
                  w2Matches = st == w2Start && et == w2End && rn == 1L && cnt == 1L;
                }
              }
            }
            // Each trigger must re-substitute placeholders with its OWN window values, not reuse
            // W1's constants for W2 (st/et/rn/cnt must all differ between W1 and W2).
            assertTrue(
                "each trigger must re-substitute placeholders per window (W1 and W2 differ); rows seen: "
                    + seen,
                w1Matches && w2Matches);
          });
    }
  }

  /**
   * SQL-27 AVG result type: AVG over a FLOAT FIELD column must be written into a DOUBLE FIELD
   * column — creation and computation succeed, and the result is validated as DOUBLE.
   *
   * <p>Expected value: t1's amount column is FLOAT; W1 of the single (d1) group receives values
   * 1.0/2.0/3.0, so AVG == (1.0+2.0+3.0)/3.0 == 2.0 (DOUBLE, hand-computed, not product output). A
   * single W2 row advances the watermark and closes W1.
   */
  @Test
  public void testSQL27AvgFloatToDouble() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      stmt.execute("CREATE DATABASE " + DB27);
      stmt.execute("USE " + DB27);
      stmt.execute(
          "CREATE TABLE t1(time TIMESTAMP TIME, device_id STRING TAG, amount FLOAT FIELD)");
      stmt.execute("CREATE TABLE calc(time TIMESTAMP TIME, avg_val DOUBLE FIELD)");
      final long base = System.currentTimeMillis() + 2000;

      // Start BEFORE insert: the subscription runs in tail mode, only consuming rows written
      // after it is established.
      stmt.execute(
          "CREATE STREAM sql27 "
              + "FROM t1 PARTITION BY device_id TUMBLE(5s, "
              + base
              + ") INTO calc(time, avg_val) "
              + "SELECT ${START_TIME}, AVG(amount) FROM ${ROWS}");
      stmt.execute("START STREAM sql27");

      // W1 (3 rows) for device d1 — the multi-row window comes first. Values 1.0/2.0/3.0.
      stmt.execute(
          "INSERT INTO t1(time, device_id, amount) VALUES (" + (base + 500) + ", 'd1', 1.0)");
      stmt.execute(
          "INSERT INTO t1(time, device_id, amount) VALUES (" + (base + 1500) + ", 'd1', 2.0)");
      stmt.execute(
          "INSERT INTO t1(time, device_id, amount) VALUES (" + (base + 2500) + ", 'd1', 3.0)");
      // W2 (1 row) advances watermark and closes W1.
      stmt.execute(
          "INSERT INTO t1(time, device_id, amount) VALUES (" + (base + 6000) + ", 'd1', 4.0)");

      // Oracle (not product output): AVG of {1.0, 2.0, 3.0} == 2.0, written as DOUBLE into avg_val.
      final double expectedAvg = 2.0;
      AWAIT.untilAsserted(
          () -> {
            boolean w1Matches = false;
            final StringBuilder seen = new StringBuilder();
            try (ResultSet rs = stmt.executeQuery("SELECT time, avg_val FROM calc ORDER BY time")) {
              while (rs.next()) {
                final long time = rs.getLong("time");
                final double avgVal = rs.getDouble("avg_val");
                seen.append("(").append(time).append(",").append(avgVal).append(") ");
                if (time == base) {
                  w1Matches = Math.abs(avgVal - expectedAvg) < 1e-9;
                }
              }
            }
            assertTrue(
                "SQL-27: AVG(FLOAT) must write DOUBLE into avg_val == 2.0 (Oracle); rows seen: "
                    + seen,
                w1Matches);
          });
    }
  }

  /**
   * SQL-21 partial-NULL row retained: when some columns of a SELECT result are NULL and the rest
   * are non-NULL, the whole row must be retained, with NULL written into the corresponding columns.
   *
   * <p>The source table t1 has two FIELD columns v1 and v2; the three W1 rows of the single (d1)
   * group carry non-NULL v1 and all-NULL v2 (inserted as NULL literals). The calc projection is
   * {@code ${START_TIME}, AVG(v1), AVG(v2)}. Expected values: avg_v1 == 2.0 (non-NULL) and avg_v2
   * == NULL (an all-NULL input column aggregates to NULL). The whole row must be retained and
   * avg_v2 must be written as NULL. A single W2 row advances the watermark and closes W1.
   */
  @Test
  public void testSQL21PartialNullRowRetained() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      stmt.execute("CREATE DATABASE " + DB21);
      stmt.execute("USE " + DB21);
      stmt.execute(
          "CREATE TABLE t1(time TIMESTAMP TIME, device_id STRING TAG, v1 DOUBLE FIELD, v2 DOUBLE FIELD)");
      stmt.execute(
          "CREATE TABLE calc(time TIMESTAMP TIME, avg_v1 DOUBLE FIELD, avg_v2 DOUBLE FIELD)");
      final long base = System.currentTimeMillis() + 2000;

      // Create and start BEFORE insert: the subscription runs in tail mode.
      stmt.execute(
          "CREATE STREAM sql21 "
              + "FROM t1 PARTITION BY device_id TUMBLE(5s, "
              + base
              + ") INTO calc(time, avg_v1, avg_v2) "
              + "SELECT ${START_TIME}, AVG(v1), AVG(v2) FROM ${ROWS}");
      stmt.execute("START STREAM sql21");

      // W1 (3 rows) for device d1 — the multi-row window comes first. v1 non-null, v2 all NULL.
      stmt.execute(
          "INSERT INTO t1(time, device_id, v1, v2) VALUES (" + (base + 500) + ", 'd1', 1.0, NULL)");
      stmt.execute(
          "INSERT INTO t1(time, device_id, v1, v2) VALUES ("
              + (base + 1500)
              + ", 'd1', 2.0, NULL)");
      stmt.execute(
          "INSERT INTO t1(time, device_id, v1, v2) VALUES ("
              + (base + 2500)
              + ", 'd1', 3.0, NULL)");
      // W2 (1 row) advances watermark and closes W1.
      stmt.execute(
          "INSERT INTO t1(time, device_id, v1, v2) VALUES ("
              + (base + 6000)
              + ", 'd1', 4.0, NULL)");

      // Oracle (not product output): AVG(v1) of {1.0,2.0,3.0} == 2.0; AVG(v2) over all-NULL ==
      // NULL.
      final double expectedAvgV1 = 2.0;
      AWAIT.untilAsserted(
          () -> {
            int rowCount = 0;
            boolean rowMatches = false;
            final StringBuilder seen = new StringBuilder();
            try (ResultSet rs =
                stmt.executeQuery("SELECT time, avg_v1, avg_v2 FROM calc ORDER BY time")) {
              while (rs.next()) {
                rowCount++;
                final long time = rs.getLong("time");
                final double avgV1 = rs.getDouble("avg_v1");
                final Object avgV2 = rs.getObject("avg_v2");
                seen.append("(")
                    .append(time)
                    .append(",")
                    .append(avgV1)
                    .append(",")
                    .append(avgV2)
                    .append(") ");
                // Partial-null semantics: row retained, avg_v1 correct, avg_v2 NULL.
                if (time == base && Math.abs(avgV1 - expectedAvgV1) < 1e-9 && avgV2 == null) {
                  rowMatches = true;
                }
              }
            }
            assertTrue(
                "SQL-21: exactly one computed row expected (partial-null row retained); rows seen: "
                    + seen,
                rowCount == 1);
            assertTrue(
                "SQL-21: row must be retained with avg_v1==2.0 (non-null) and avg_v2==NULL (partial null); rows seen: "
                    + seen,
                rowMatches);
          });
    }
  }

  /**
   * SQL-22 all-NULL computed row discarded: when every column of a computed row is NULL, the row
   * must be discarded and not written back.
   *
   * <p>The NULL values are produced by aggregating all-NULL columns ({@code AVG(v)} over an
   * all-NULL column yields NULL, the mechanism verified by SQL-21). t1's v1/v2 columns are inserted
   * as all NULL, so {@code SELECT ${START_TIME}, AVG(v1), AVG(v2)} yields the candidate row
   * (windowStart, NULL, NULL) for every window — all value columns NULL, so it must be discarded
   * and {@code calc} must stay empty.
   *
   * <p>Since every computed row is discarded, no calc row can prove the pipeline is running — a
   * control stream (a legal {@code count(*)} task over the same data and windows writing {@code
   * calc_control}) is used instead. Once the control stream has produced its W1/W2 rows, {@code
   * calc} must still hold 0 rows.
   */
  @Test
  public void testSQL22AllNullRowDiscarded() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      stmt.execute("CREATE DATABASE " + DB22);
      stmt.execute("USE " + DB22);
      // v1/v2 inserted as all-NULL so AVG(v1)/AVG(v2) both compute to NULL (SQL-21-verified
      // mechanism).
      stmt.execute("CREATE TABLE t1(time TIMESTAMP TIME, v1 DOUBLE FIELD, v2 DOUBLE FIELD)");
      // AVG yields DOUBLE, so a/b must be DOUBLE columns.
      stmt.execute("CREATE TABLE calc(time TIMESTAMP TIME, a DOUBLE FIELD, b DOUBLE FIELD)");
      // Control sink: a legal compute task over the same data proves the pipeline fires.
      stmt.execute("CREATE TABLE calc_control(time TIMESTAMP TIME, cnt INT64 FIELD)");
      final long base = System.currentTimeMillis() + 2000;

      stmt.execute(
          "CREATE STREAM sql22 "
              + "FROM t1 TUMBLE(5s, "
              + base
              + ") INTO calc(time, a, b) "
              + "SELECT ${START_TIME}, AVG(v1), AVG(v2) FROM ${ROWS}");
      stmt.execute(
          "CREATE STREAM sql22_control FROM t1 TUMBLE(5s, "
              + base
              + ") INTO calc_control(time, cnt) SELECT ${START_TIME}, count(*) FROM ${ROWS}");
      stmt.execute("START STREAM sql22");
      stmt.execute("START STREAM sql22_control");

      // W1 (3 rows, all-NULL v1/v2), then W2/W3 rows to close W1/W2 — every window's candidate
      // row is (windowStart, NULL, NULL) and must be discarded.
      stmt.execute("INSERT INTO t1(time, v1, v2) VALUES (" + (base + 500) + ", NULL, NULL)");
      stmt.execute("INSERT INTO t1(time, v1, v2) VALUES (" + (base + 1500) + ", NULL, NULL)");
      stmt.execute("INSERT INTO t1(time, v1, v2) VALUES (" + (base + 2500) + ", NULL, NULL)");
      stmt.execute("INSERT INTO t1(time, v1, v2) VALUES (" + (base + 6000) + ", NULL, NULL)");
      stmt.execute("INSERT INTO t1(time, v1, v2) VALUES (" + (base + 11000) + ", NULL, NULL)");

      // The control stream must fire both W1 and W2 — proving the trigger/window/compute
      // pipeline works for this exact data, so an empty calc cannot be blamed on the machinery.
      final long w1Start = base;
      final long w2Start = base + 5000L;
      AWAIT.untilAsserted(
          () -> {
            boolean controlW1 = false;
            boolean controlW2 = false;
            try (ResultSet rs =
                stmt.executeQuery("SELECT time, cnt FROM calc_control ORDER BY time")) {
              while (rs.next()) {
                if (rs.getLong("time") == w1Start && rs.getLong("cnt") == 3L) {
                  controlW1 = true;
                }
                if (rs.getLong("time") == w2Start && rs.getLong("cnt") == 1L) {
                  controlW2 = true;
                }
              }
            }
            assertTrue(
                "SQL-22: control stream must fire W1 (cnt==3) and W2 (cnt==1) for the same"
                    + " trigger data",
                controlW1 && controlW2);
          });

      // Grace period (2s, same reasoning as SQL-16), then the all-NULL computed rows must ALL
      // have been discarded: calc stays empty. Any row in calc means the discard rule is not
      // implemented.
      Thread.sleep(2000);
      try (ResultSet rs = stmt.executeQuery("SELECT time, a, b FROM calc ORDER BY time")) {
        final StringBuilder seen = new StringBuilder();
        while (rs.next()) {
          seen.append("(")
              .append(rs.getLong("time"))
              .append(",")
              .append(rs.getObject("a"))
              .append(",")
              .append(rs.getObject("b"))
              .append(") ");
        }
        assertFalse(
            "SQL-22: all-NULL computed rows must be discarded, calc must stay empty; rows seen: "
                + seen,
            seen.length() > 0);
      }
    }
  }

  /**
   * SQL-23 first-window prev_time NULL but row retained: the first window must output a non-NULL
   * start_time together with a NULL prev_time; since the row is not all-NULL it must be retained.
   * The expected W1 row is {@code time==base, st==base (non-NULL), prev==NULL, cnt==3}.
   *
   * <p>The stream is {@code FROM t1 TUMBLE(5s, base) ... SELECT ${START_TIME}, ${start_time},
   * ${prev_time}, count(*) FROM ${ROWS}}. Expected values: W1 starts at base and holds 3 rows,
   * hand-computed from the inserted data.
   *
   * <p>Note: this test is currently red — the implementation computes the first window's prev_time
   * as the window start minus the window size (base - 5000, non-NULL) instead of NULL. The
   * assertion states the required behavior without weakening it. TUMBLE is used (not PERIOD) so
   * that PERIOD-specific limitations do not interfere.
   */
  @Test
  public void testSQL23FirstWindowPrevNullRetained() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      stmt.execute("CREATE DATABASE " + DB23);
      stmt.execute("USE " + DB23);
      stmt.execute("CREATE TABLE t1(time TIMESTAMP TIME, v DOUBLE FIELD)");
      stmt.execute(
          "CREATE TABLE calc(time TIMESTAMP TIME, st TIMESTAMP FIELD, prev TIMESTAMP FIELD, cnt INT64 FIELD)");
      final long base = System.currentTimeMillis() + 2000;
      stmt.execute(
          "CREATE STREAM sql23 "
              + "FROM t1 TUMBLE(5s, "
              + base
              + ") INTO calc(time, st, prev, cnt) "
              + "SELECT ${START_TIME}, ${start_time}, ${prev_time}, count(*) FROM ${ROWS}");
      stmt.execute("START STREAM sql23");
      stmt.execute("INSERT INTO t1(time, v) VALUES (" + (base + 500) + ", 1.0)");
      stmt.execute("INSERT INTO t1(time, v) VALUES (" + (base + 1500) + ", 2.0)");
      stmt.execute("INSERT INTO t1(time, v) VALUES (" + (base + 2500) + ", 3.0)");
      stmt.execute("INSERT INTO t1(time, v) VALUES (" + (base + 6000) + ", 4.0)");

      AWAIT.untilAsserted(
          () -> {
            boolean w1Matches = false;
            final StringBuilder seen = new StringBuilder();
            try (ResultSet rs =
                stmt.executeQuery("SELECT time, st, prev, cnt FROM calc ORDER BY time")) {
              while (rs.next()) {
                final long time = rs.getLong("time");
                final long st = rs.getLong("st");
                final Object prev = rs.getObject("prev");
                final long cnt = rs.getLong("cnt");
                seen.append("(")
                    .append(time)
                    .append(",")
                    .append(st)
                    .append(",")
                    .append(prev)
                    .append(",")
                    .append(cnt)
                    .append(") ");
                if (time == base) {
                  // Spec: first window keeps the row; st non-null, prev MUST be NULL, cnt==3.
                  w1Matches = (st == base) && (prev == null) && (cnt == 3L);
                }
              }
            }
            assertTrue(
                "SQL-23: first window must keep row with st==base(non-null), prev==NULL, cnt==3; rows seen: "
                    + seen,
                w1Matches);
          });
    }
  }

  /**
   * SQL-24 self-JOIN on the trigger table (same-table form): a legal JOIN whose both sides are the
   * trigger table {@code t1} (read by name, not {@code ${ROWS}}) must execute as an ordinary query,
   * and the result must match the hand-computed expectation.
   *
   * <p>Same-table rationale: {@code ${ROWS}} cannot participate in any JOIN (neither with another
   * {@code ${ROWS}} nor with a plain table — both forms are rejected at creation, see SQL-17/30),
   * so a self-JOIN of the trigger table is expressed by reading t1 by name {@code FROM t1 a JOIN t1
   * b}. This also exercises the cross-table calc-plan code path (reading a table other than {@code
   * ${ROWS}}), which may surface B-1.
   *
   * <p>Expected value derivation: {@code t1} carries ('x',1), ('y',2) in W1 (both JOIN data and
   * trigger) and a W2 row ('z',0) to close W1. The self-JOIN by k yields (x: 1+1) + (y: 2+2) + (z:
   * 0+0) = 6, so {@code calc.s == 6.0} (hand-computed, not product output). The W2 row uses a
   * distinct k='z' with v=0 so its self-join contribution is 0; the subquery reads the whole t1 by
   * name (including the W2 row) at W1 close time.
   *
   * <p>Note: this test states the required behavior without weakening it; if the calc plan's FROM
   * clause is a JOIN the current implementation may produce no output row (B-1).
   */
  @Test
  public void testSQL24PlainTableJoin() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      stmt.execute("CREATE DATABASE " + DB24);
      stmt.execute("USE " + DB24);
      // Trigger table = query table (same-table self-JOIN). k is the JOIN key; v the summed value.
      stmt.execute("CREATE TABLE t1(time TIMESTAMP TIME, k STRING TAG, v INT64 FIELD)");
      stmt.execute("CREATE TABLE calc(time TIMESTAMP TIME, s DOUBLE FIELD)");
      final long base = System.currentTimeMillis() + 2000;

      stmt.execute(
          "CREATE STREAM sql24 "
              + "FROM t1 TUMBLE(5s, "
              + base
              + ") INTO calc(time, s) "
              + "SELECT ${START_TIME}, sum(a.v + b.v) FROM t1 a JOIN t1 b ON a.k = b.k");
      stmt.execute("START STREAM sql24");
      // W1 rows: JOIN data and trigger. W2 row ('z',0) closes W1 and self-joins to 0.
      stmt.execute("INSERT INTO t1(time, k, v) VALUES (" + (base + 100) + ", 'x', 1)");
      stmt.execute("INSERT INTO t1(time, k, v) VALUES (" + (base + 200) + ", 'y', 2)");
      stmt.execute("INSERT INTO t1(time, k, v) VALUES (" + (base + 6000) + ", 'z', 0)");

      final double expectedS = 6.0; // Oracle: (1+1)+(2+2)+(0+0)=6, independent of product output
      AWAIT.untilAsserted(
          () -> {
            double s = -1.0;
            int rowCount = 0;
            final StringBuilder seen = new StringBuilder();
            try (ResultSet rs = stmt.executeQuery("SELECT time, s FROM calc ORDER BY time")) {
              while (rs.next()) {
                rowCount++;
                s = rs.getDouble("s");
                seen.append("(").append(rs.getLong("time")).append(",").append(s).append(") ");
              }
            }
            assertTrue(
                "SQL-24: self-JOIN t1 must sum to 6 (Oracle); rows seen: " + seen,
                rowCount == 1 && s == expectedS);
          });
    }
  }

  /**
   * SQL-28 cross-table subquery WHERE (the original SQL-03 semantics, kept as a separate case): the
   * compute-task subquery filters on a time placeholder ({@code WHERE time < ${START_TIME}}) and
   * reads a DIFFERENT table {@code other_t} (not the trigger table). The trigger table t1 only
   * drives the window; the result data comes from other_t.
   *
   * <p>Expected value derivation: {@code other_t} is loaded with exactly one row strictly before
   * the W1 start ({@code base - 2000}) and two rows inside W1 ({@code base + 1000}, {@code base +
   * 2000}), so {@code count(*)} under {@code time < ${START_TIME}} is hand-computed as 1. It
   * discriminates both failure modes: 3 would mean the WHERE was ignored, and no row at all would
   * mean the WHERE was hoisted onto the source.
   */
  @Test
  public void testSQL28OtherTableWhereTimePlaceholder() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      stmt.execute("CREATE DATABASE " + DB28);
      stmt.execute("USE " + DB28);
      // Trigger (source) table.
      stmt.execute(
          "CREATE TABLE t1(time TIMESTAMP TIME, device_id STRING TAG, amount FLOAT FIELD)");
      // The table the subquery reads and filters (different from the trigger table).
      stmt.execute(
          "CREATE TABLE other_t(time TIMESTAMP TIME, device_id STRING TAG, base INT64 FIELD)");
      // Sink table.
      stmt.execute("CREATE TABLE calc(time TIMESTAMP TIME, cnt INT64 FIELD)");

      final long base = System.currentTimeMillis() + 2000;

      // Independent Oracle data: exactly one other_t row lies strictly before W1 starts.
      stmt.execute(
          "INSERT INTO other_t(time, device_id, base) VALUES (" + (base - 2000) + ", 'd0', 10)");
      stmt.execute(
          "INSERT INTO other_t(time, device_id, base) VALUES (" + (base + 1000) + ", 'd1', 20)");
      stmt.execute(
          "INSERT INTO other_t(time, device_id, base) VALUES (" + (base + 2000) + ", 'd2', 30)");

      // The WHERE lives inside the subquery (the calc plan), not in the source clause.
      stmt.execute(
          "CREATE STREAM sql28 "
              + "FROM t1 TUMBLE(5s, "
              + base
              + ") INTO calc(time, cnt) "
              + "SELECT ${START_TIME}, count(*) FROM other_t WHERE time < ${START_TIME}");
      stmt.execute("START STREAM sql28");

      // t1 only drives the window; its content is unrelated to the result.
      stmt.execute(
          "INSERT INTO t1(time, device_id, amount) VALUES (" + (base + 500) + ", 'd1', 1.0)");
      stmt.execute(
          "INSERT INTO t1(time, device_id, amount) VALUES (" + (base + 1500) + ", 'd2', 2.0)");
      stmt.execute(
          "INSERT INTO t1(time, device_id, amount) VALUES (" + (base + 2500) + ", 'd3', 3.0)");
      stmt.execute(
          "INSERT INTO t1(time, device_id, amount) VALUES (" + (base + 6000) + ", 'd1', 4.0)");

      // Independent "Oracle": count(*) over other_t with time < base == 1.
      final long expectedCnt = 1L;

      AWAIT.untilAsserted(
          () -> {
            boolean subqueryWhereApplied = false;
            try (ResultSet rs = stmt.executeQuery("SELECT time, cnt FROM calc")) {
              while (rs.next()) {
                if (rs.getLong("time") == base && rs.getLong("cnt") == expectedCnt) {
                  subqueryWhereApplied = true;
                }
              }
            }
            assertTrue(
                "calc should contain the W1 row with cnt == "
                    + expectedCnt
                    + " (subquery WHERE time < ${START_TIME} applied inside the subquery on"
                    + " other_t, not hoisted to the source pre-filter)",
                subqueryWhereApplied);
          });
    }
  }

  /**
   * SQL-29 cross-table plain-table JOIN (the original SQL-24 semantics, kept as a separate case): a
   * legal JOIN of two DIFFERENT plain tables {@code a} and {@code b} (neither is the trigger table)
   * must execute as an ordinary query, and the result must match the hand-computed expectation.
   *
   * <p>The stream source is the trigger table t1 ({@code FROM t1 TUMBLE(5s, base)}); the calc query
   * is {@code SELECT ${START_TIME}, sum(a.v + b.w) FROM a JOIN b ON a.k = b.k}. With a =
   * ('x',1),('y',2) and b = ('x',10),('y',20), the join by k yields (x: 1+10) + (y: 2+20) = 33, so
   * calc.s == 33 (hand-computed, not product output).
   */
  @Test
  public void testSQL29PlainTableJoin() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      stmt.execute("CREATE DATABASE " + DB29);
      stmt.execute("USE " + DB29);
      stmt.execute("CREATE TABLE t1(time TIMESTAMP TIME, v DOUBLE FIELD)");
      stmt.execute("CREATE TABLE a(time TIMESTAMP TIME, k STRING TAG, v INT64 FIELD)");
      stmt.execute("CREATE TABLE b(time TIMESTAMP TIME, k STRING TAG, w INT64 FIELD)");
      stmt.execute("CREATE TABLE calc(time TIMESTAMP TIME, s DOUBLE FIELD)");
      final long base = System.currentTimeMillis() + 2000;
      // Pre-load the two plain tables to be joined.
      stmt.execute("INSERT INTO a(time, k, v) VALUES (" + (base + 100) + ", 'x', 1)");
      stmt.execute("INSERT INTO a(time, k, v) VALUES (" + (base + 200) + ", 'y', 2)");
      stmt.execute("INSERT INTO b(time, k, w) VALUES (" + (base + 100) + ", 'x', 10)");
      stmt.execute("INSERT INTO b(time, k, w) VALUES (" + (base + 200) + ", 'y', 20)");

      stmt.execute(
          "CREATE STREAM sql29 "
              + "FROM t1 TUMBLE(5s, "
              + base
              + ") INTO calc(time, s) "
              + "SELECT ${START_TIME}, sum(a.v + b.w) FROM a JOIN b ON a.k = b.k");
      stmt.execute("START STREAM sql29");
      stmt.execute("INSERT INTO t1(time, v) VALUES (" + (base + 500) + ", 1.0)");
      stmt.execute("INSERT INTO t1(time, v) VALUES (" + (base + 6000) + ", 2.0)");

      final double expectedS = 33.0; // Oracle: (1+10)+(2+20)=33, independent of product output
      AWAIT.untilAsserted(
          () -> {
            double s = -1.0;
            int rowCount = 0;
            final StringBuilder seen = new StringBuilder();
            try (ResultSet rs = stmt.executeQuery("SELECT time, s FROM calc ORDER BY time")) {
              while (rs.next()) {
                rowCount++;
                s = rs.getDouble("s");
                seen.append("(").append(rs.getLong("time")).append(",").append(s).append(") ");
              }
            }
            assertTrue(
                "SQL-29: JOIN a,b must sum to 33 (Oracle); rows seen: " + seen,
                rowCount == 1 && s == expectedS);
          });
    }
  }

  /**
   * SQL-30 (negative): joining {@code ${rows}} with another {@code ${rows}} is not supported. The
   * CREATE must be rejected and no stream registered. This complements SQL-17, which covers the
   * equally disallowed {@code ${rows} JOIN plain-table} form.
   */
  @Test
  public void testSQL30RowsJoinRows() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      stmt.execute("CREATE DATABASE " + DB30);
      stmt.execute("USE " + DB30);
      stmt.execute("CREATE TABLE t1(time TIMESTAMP TIME, id STRING TAG, amount DOUBLE FIELD)");
      stmt.execute("CREATE TABLE calc(time TIMESTAMP TIME, cnt INT64 FIELD)");
      final long base = System.currentTimeMillis() + 2000;

      // ${rows} JOIN ${rows}: two references to the same window dataset joined together —
      // disallowed.
      assertThrows(
          SQLException.class,
          () ->
              stmt.execute(
                  "CREATE STREAM sql30 FROM t1 TUMBLE(5s, "
                      + base
                      + ") INTO calc(time, cnt) SELECT ${START_TIME}, count(*) "
                      + "FROM ${ROWS} a JOIN ${ROWS} b ON a.id = b.id"));
      assertFalse(
          "sql30 must not be registered after the rejected CREATE STREAM (${rows} JOIN ${rows})",
          streamExists(stmt, "sql30"));
    }
  }
}
