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
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

/**
 * End-to-end StreamNode integration tests verifying:
 *
 * <ul>
 *   <li>Regime-aware {@code VARIATION} windowing with transient spike immunity, onset backtracking,
 *       and {@code ${PREV_VALUE}} / {@code ${CURRENT_VALUE}} placeholders;
 *   <li>Distributed partition-watermark alignment on {@code TUMBLE} streams;
 *   <li>Multi-granularity base-slice reuse with {@code KLL} quantile sketch across {@code 10s ->
 *       1m} streams plus automatic lossless raw-scan fallback after producer stop.
 * </ul>
 */
@Category({StreamClusterIT.class})
public class IoTDBVariationAndReuseStreamIT extends StreamNodeITBase {

  private static final String DB = "var_reuse_it_db";

  private static final ConditionFactory AWAIT =
      Awaitility.await()
          .pollInSameThread()
          .pollDelay(200, TimeUnit.MILLISECONDS)
          .pollInterval(500, TimeUnit.MILLISECONDS)
          .atMost(60, TimeUnit.SECONDS);

  private static BaseEnv env() {
    return StreamEnvFactory.getEnv();
  }

  @BeforeClass
  public static void setUp() throws Exception {
    StreamEnvFactory.getEnv(1)
        .getConfig()
        .getCommonConfig()
        .setSubscriptionEnabled(true)
        .setDataRegionGroupExtensionPolicy("CUSTOM")
        .setDefaultDataRegionGroupNumPerDatabase(1)
        .setDataRegionConsensusProtocolClass(ConsensusFactory.IOT_CONSENSUS);
    StreamEnvFactory.getEnv(1).initClusterEnvironment(1, 1, 30);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    StreamEnvFactory.getEnv().cleanClusterEnvironment();
  }

  @After
  public void cleanUpDatabaseAndStreams() throws Exception {
    try (Connection connection = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      final List<String> streamNames = new ArrayList<>();
      try (ResultSet rs = statement.executeQuery("SHOW STREAMS")) {
        while (rs.next()) {
          streamNames.add(rs.getString("stream_name"));
        }
      } catch (Exception ignored) {
        // best-effort cleanup
      }
      for (final String name : streamNames) {
        try {
          statement.execute("DROP STREAM " + name);
        } catch (Exception ignored) {
          // best-effort cleanup
        }
      }
      try {
        statement.execute("DROP DATABASE " + DB);
      } catch (Exception ignored) {
        // database may not exist
      }
    }
  }

  private void prepareDatabase(final Statement statement) throws SQLException {
    statement.execute("CREATE DATABASE " + DB);
    statement.execute("USE " + DB);
  }

  private void startAndAwaitRunning(final Statement statement, final String streamName)
      throws SQLException {
    statement.execute("START STREAM " + streamName);
    AWAIT.untilAsserted(
        () -> {
          final Map<String, Object> row = queryStreamRow(statement, streamName);
          final Object status = row == null ? null : row.get("status");
          assertEquals(
              "stream " + streamName + " should reach RUNNING before ingestion", "RUNNING", status);
        });
  }

  @Test
  public void testVariationStreamSpikeImmunityAndPlaceholders() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      prepareDatabase(stmt);
      stmt.execute(
          "CREATE TABLE var_src(time TIMESTAMP TIME, device_id STRING TAG, s1 DOUBLE FIELD)");
      stmt.execute(
          "CREATE TABLE var_sink("
              + "time TIMESTAMP TIME, "
              + "device_id STRING TAG, "
              + "prev_v DOUBLE FIELD, "
              + "curr_v DOUBLE FIELD, "
              + "cnt INT64 FIELD, "
              + "avg_v DOUBLE FIELD)");

      stmt.execute(
          "CREATE STREAM s_var "
              + "FROM var_src PARTITION BY device_id "
              + "VARIATION(col => s1, delta => 2.0) "
              + "INTO var_sink(time, device_id, prev_v, curr_v, cnt, avg_v) "
              + "SELECT ${START_TIME}, ${1}, ${PREV_VALUE}, ${CURRENT_VALUE}, ${ROW_NUM}, avg(s1) "
              + "FROM ${ROWS} "
              + "WITH ("
              + "variation_detector = 'evidence', "
              + "variation_theta = 2.0, "
              + "variation_epsilon = 2.0, "
              + "variation_min_samples = 3, "
              + "variation_adaptive_baseline = true"
              + ")");
      startAndAwaitRunning(stmt, "s_var");

      final long base = ((System.currentTimeMillis() + 120_000L) / 60_000L) * 60_000L;
      // Regime 1 around 10.0 with one isolated 85.0 transient spike (must NOT split).
      final double[] values = {
        10.0,
        10.1,
        9.9,
        10.0,
        85.0,
        10.1,
        9.9,
        10.0, // indices 0..7 -> Regime 1 (8 rows)
        30.0,
        30.2,
        29.9,
        30.1, // indices 8..11 -> Regime 2 (4 rows, onset = base + 8s)
        55.0,
        55.1,
        54.9,
        55.2 // indices 12..15 -> Regime 3 (triggers Regime 2 close)
      };
      for (int i = 0; i < values.length; i++) {
        stmt.execute(
            "INSERT INTO var_src(time, device_id, s1) VALUES ("
                + (base + i * 1_000L)
                + ", 'd1', "
                + values[i]
                + ")");
      }

      AWAIT.untilAsserted(
          () -> {
            final List<Map<String, Object>> rows =
                queryRows(
                    stmt,
                    "SELECT time, device_id, prev_v, curr_v, cnt, avg_v "
                        + "FROM var_sink ORDER BY time");
            assertTrue("Expected at least 2 closed regime windows, got " + rows, rows.size() >= 2);
          });

      final List<Map<String, Object>> sinkRows =
          queryRows(
              stmt,
              "SELECT time, device_id, prev_v, curr_v, cnt, avg_v FROM var_sink ORDER BY time");
      final Map<String, Object> r1 = sinkRows.get(0);
      assertEquals(base, asLong(r1.get("time")));
      assertEquals("d1", String.valueOf(r1.get("device_id")));
      assertEquals(8L, asLong(r1.get("cnt")));
      assertEquals(10.0, ((Number) r1.get("prev_v")).doubleValue(), 0.5);
      assertEquals(30.0, ((Number) r1.get("curr_v")).doubleValue(), 0.5);

      final Map<String, Object> r2 = sinkRows.get(1);
      assertEquals(base + 8_000L, asLong(r2.get("time")));
      assertEquals(4L, asLong(r2.get("cnt")));
      assertEquals(30.05, ((Number) r2.get("avg_v")).doubleValue(), 0.1);
      assertEquals(30.0, ((Number) r2.get("prev_v")).doubleValue(), 0.5);
      assertEquals(55.0, ((Number) r2.get("curr_v")).doubleValue(), 0.5);
    }
  }

  @Test
  public void testTumbleStreamWithAlignedPartitionWatermark() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      prepareDatabase(stmt);
      stmt.execute(
          "CREATE TABLE align_src(time TIMESTAMP TIME, device_id STRING TAG, val DOUBLE FIELD)");
      stmt.execute(
          "CREATE TABLE align_sink(time TIMESTAMP TIME, cnt INT64 FIELD, total DOUBLE FIELD)");

      stmt.execute(
          "CREATE STREAM s_align "
              + "FROM align_src "
              + "TUMBLE(10s, 0) "
              + "INTO align_sink(time, cnt, total) "
              + "SELECT ${START_TIME}, count(*), sum(val) FROM ${ROWS} "
              + "WITH ("
              + "align_partition_watermark = true, "
              + "partition_stale_timeout_ms = 10s"
              + ")");
      startAndAwaitRunning(stmt, "s_align");

      final long base = ((System.currentTimeMillis() + 120_000L) / 60_000L) * 60_000L;
      stmt.execute(
          "INSERT INTO align_src(time, device_id, val) VALUES "
              + "("
              + (base + 1_000L)
              + ", 'd1', 1.0), "
              + "("
              + (base + 3_000L)
              + ", 'd2', 2.0), "
              + "("
              + (base + 5_000L)
              + ", 'd1', 3.0), "
              + "("
              + (base + 8_000L)
              + ", 'd2', 4.0), "
              + "("
              + (base + 15_000L)
              + ", 'd1', 100.0)");

      AWAIT.untilAsserted(
          () -> {
            final List<Map<String, Object>> rows =
                queryRows(stmt, "SELECT time, cnt, total FROM align_sink ORDER BY time");
            assertEquals("Expected aligned tumble window row, got " + rows, 1, rows.size());
            assertEquals(base, asLong(rows.get(0).get("time")));
            assertEquals(4L, asLong(rows.get(0).get("cnt")));
            assertEquals(10.0, ((Number) rows.get(0).get("total")).doubleValue(), 1e-6);
          });
    }
  }

  @Test
  public void testMultiGranularitySliceReuseKllAndProducerStopFallback() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      prepareDatabase(stmt);
      stmt.execute(
          "CREATE TABLE reuse_src(time TIMESTAMP TIME, device_id STRING TAG, val DOUBLE FIELD)");
      stmt.execute("CREATE TABLE sink_10s(time TIMESTAMP TIME, cnt INT64 FIELD, p50 DOUBLE FIELD)");
      stmt.execute("CREATE TABLE sink_1m(time TIMESTAMP TIME, cnt INT64 FIELD, p99 DOUBLE FIELD)");

      stmt.execute(
          "CREATE STREAM s_10s "
              + "FROM reuse_src "
              + "TUMBLE(10s, 0) "
              + "INTO sink_10s(time, cnt, p50) "
              + "SELECT ${START_TIME}, count(*), approx_percentile(val, 0.50) FROM ${ROWS} "
              + "WITH ("
              + "enable_slice_reuse = true, "
              + "reuse_sketch_type = 'kll', "
              + "reuse_kll_k = 200, "
              + "slice_retention_ms = 5m"
              + ")");
      stmt.execute(
          "CREATE STREAM s_1m "
              + "FROM reuse_src "
              + "TUMBLE(1m, 0) "
              + "INTO sink_1m(time, cnt, p99) "
              + "SELECT ${START_TIME}, count(*), approx_percentile(val, 0.99) FROM ${ROWS} "
              + "WITH ("
              + "enable_slice_reuse = true, "
              + "reuse_sketch_type = 'kll', "
              + "reuse_kll_k = 200, "
              + "watermark = 1s"
              + ")");
      startAndAwaitRunning(stmt, "s_10s");
      startAndAwaitRunning(stmt, "s_1m");

      final long base = ((System.currentTimeMillis() + 120_000L) / 60_000L) * 60_000L;

      // Phase A: Write 60 points [1..60] across [base, base + 60s), then advance watermark past
      // base + 60s so s_10s publishes all 6 slices and s_1m consumes them via SharedSliceManager.
      final StringBuilder batch1 =
          new StringBuilder("INSERT INTO reuse_src(time, device_id, val) VALUES ");
      for (int i = 0; i < 60; i++) {
        if (i > 0) {
          batch1.append(", ");
        }
        batch1.append("(").append(base + i * 1_000L).append(", 'd1', ").append(i + 1).append(".0)");
      }
      stmt.execute(batch1.toString());
      stmt.execute(
          "INSERT INTO reuse_src(time, device_id, val) VALUES ("
              + (base + 60_500L)
              + ", 'd1', 100.5)");

      AWAIT.untilAsserted(
          () -> {
            final int count10s = countRows(stmt, "sink_10s");
            assertTrue("Expected all 6 base 10s slices emitted, got " + count10s, count10s >= 6);
          });

      stmt.execute(
          "INSERT INTO reuse_src(time, device_id, val) VALUES ("
              + (base + 65_000L)
              + ", 'd1', 100.8)");

      AWAIT.untilAsserted(
          () -> {
            final List<Map<String, Object>> rows =
                queryRows(stmt, "SELECT time, cnt, p99 FROM sink_1m ORDER BY time");
            assertTrue("Expected Phase A 1m reuse window, got " + rows, rows.size() >= 1);
            assertEquals(base, asLong(rows.get(0).get("time")));
            assertEquals(60L, asLong(rows.get(0).get("cnt")));
            final double p99 = ((Number) rows.get(0).get("p99")).doubleValue();
            assertTrue("Expected KLL P99 near 59..60, got " + p99, p99 >= 58.0 && p99 <= 60.0);
          });

      // Phase B: Stop producer s_10s and verify lossless raw-scan fallback on s_1m for [base+60s,
      // base+120s).
      stmt.execute("STOP STREAM s_10s");
      AWAIT.untilAsserted(
          () -> {
            final Map<String, Object> row = queryStreamRow(stmt, "s_10s");
            final Object status = row == null ? null : row.get("status");
            assertNotEquals("RUNNING", status);
          });

      final StringBuilder batch2 =
          new StringBuilder("INSERT INTO reuse_src(time, device_id, val) VALUES ");
      for (int i = 0; i < 60; i++) {
        if (i > 0) {
          batch2.append(", ");
        }
        // Offset by +100ms to avoid colliding with pusher timestamps 60_500 and 65_000.
        batch2
            .append("(")
            .append(base + 60_100L + i * 1_000L)
            .append(", 'd1', ")
            .append(101 + i)
            .append(".0)");
      }
      stmt.execute(batch2.toString());
      stmt.execute(
          "INSERT INTO reuse_src(time, device_id, val) VALUES ("
              + (base + 125_000L)
              + ", 'd1', 1000.0)");

      AWAIT.untilAsserted(
          () -> {
            final List<Map<String, Object>> rows =
                queryRows(stmt, "SELECT time, cnt, p99 FROM sink_1m ORDER BY time");
            assertTrue("Expected Phase B fallback 1m window, got " + rows, rows.size() >= 2);
            final Map<String, Object> w2 = rows.get(1);
            assertEquals(base + 60_000L, asLong(w2.get("time")));
            assertEquals(62L, asLong(w2.get("cnt")));
            final double p99 = ((Number) w2.get("p99")).doubleValue();
            assertTrue(
                "Expected fallback P99 in [158, 160], got " + p99, p99 >= 158.0 && p99 <= 160.0);
          });
    }
  }
}
