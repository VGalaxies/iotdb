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
import org.apache.iotdb.isession.ITableSession;
import org.apache.iotdb.it.env.StreamEnvFactory;
import org.apache.iotdb.it.env.cluster.config.MppCommonConfig;
import org.apache.iotdb.itbase.category.StreamClusterIT;
import org.apache.iotdb.itbase.env.BaseEnv;

import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.write.record.Tablet;
import org.awaitility.Awaitility;
import org.awaitility.core.ConditionFactory;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Integration tests for the stream-computation control-option cases (OPT-01 through OPT-79; the
 * case list does not number OPT-06/OPT-30). Each test verifies one window-lifecycle option of
 * CREATE STREAM (watermark / allowed_lateness / max_delay_window_close / max_idle_window_retain /
 * left_closed) against the behavior defined in the requirements document. Runs against a 1C1D1S
 * cluster (1 ConfigNode, 1 DataNode, 1 StreamNode).
 */
@Category({StreamClusterIT.class})
public class IoTDBStreamComputeControlOptionsIT {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(IoTDBStreamComputeControlOptionsIT.class);

  // Observation window used before negative assertions: the stream must stay silent for at
  // least this long before we conclude "no output". Event-time computation settles fast (window
  // state is carried by the data itself, no background timer involved), so 500ms is enough; bump
  // when diagnosing a suspicious negative that could be latency-related.
  private static final long NEGATIVE_OBSERVE_MS = 500;

  /**
   * Wall-clock silence window for the "system time must NOT close/revise the window" negative
   * checks. It deliberately outlasts the 5s spec minimum of max_delay_window_close /
   * max_idle_window_retain: an engine that wrongly applies the 5s minimum as a default fires at
   * ~5s, which this window catches, while a longer wait adds no discrimination.
   */
  private static final long SYSTEM_SILENCE_MS = 6_000L;

  // Event-time window results always appear within ~2s of the triggering insert (the window state
  // is carried by the data itself); the bound only matters while a known product defect keeps the
  // result table empty. Override with -DstreamComputeControlOptionsIT.awaitSeconds=N when
  // diagnosing a suspicious red that could be latency-related.
  private static final int AWAIT_TIMEOUT_SECONDS =
      Integer.getInteger("streamComputeControlOptionsIT.awaitSeconds", 6);

  private static final ConditionFactory AWAIT =
      Awaitility.await()
          .pollInSameThread()
          .pollDelay(100, TimeUnit.MILLISECONDS)
          .pollInterval(300, TimeUnit.MILLISECONDS)
          .atMost(AWAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS);

  /**
   * Await bound for results produced by the system-time (max_delay) trigger: unlike event-time
   * results the row only appears after the C=5s wall-clock trigger fires, and a heavily loaded
   * runner (e.g., CI executing the full suite) delays the DataNode -> StreamNode delivery by
   * multiple seconds on top. Green cases still exit early via polling; the larger bound only
   * extends the wait for known-red or slow cases.
   */
  private static final ConditionFactory SYSTEM_TRIGGER_AWAIT =
      Awaitility.await()
          .pollInSameThread()
          .pollDelay(100, TimeUnit.MILLISECONDS)
          .pollInterval(300, TimeUnit.MILLISECONDS)
          .atMost(15, TimeUnit.SECONDS);

  // ---- Shared constants for the LEFT_CLOSED=false combination matrix OPT-70 .. OPT-79 ----

  /**
   * Time scale of the case table: one minute runs in 1 second (the TUMBLE(10m) mark maps to 10s, 5m
   * to 5s, 2m to 2s).
   */
  private static final long OPT_MINUTE_MS = 1_000L;

  /** Length of the target window: the TUMBLE(10m) mark corresponds to 10s. */
  private static final long OPT_WINDOW_MS = 10 * OPT_MINUTE_MS;

  /**
   * "Omitted" as used by the case text: the option is not written at all (W/A/C/I default to 0).
   */
  private static final long NONE = -1L;

  /** Mark for ALLOWED_LATENESS=1m (used by OPT-75 only; see the OPT-75 javadoc for why). */
  private static final long A_1M = OPT_MINUTE_MS;

  /** Mark for ALLOWED_LATENESS=5m. */
  private static final long A_5M = 5 * OPT_MINUTE_MS;

  /** Mark for WATERMARK=2m. */
  private static final long W_2M = 2 * OPT_MINUTE_MS;

  /** Mark for MAX_DELAY_WINDOW_CLOSE=10m. */
  private static final long C_10M = 10 * OPT_MINUTE_MS;

  /** Mark for MAX_IDLE_WINDOW_RETAIN=5m. */
  private static final long I_5M = 5 * OPT_MINUTE_MS;

  /** E group: the event-time progress path. */
  private static final String E_GROUP = "e";

  /** S group: the system-time triggered path. */
  private static final String S_GROUP = "s";

  /** Output table name shared by OPT-70..OPT-79. */
  private static final String OUTPUT_TABLE = "sum_calc";

  /**
   * Upper bound for waiting for the first result or idle recomputation after a system-time trigger.
   * The trigger is asynchronous and includes sink write-back, so allow 30 seconds on a loaded CI
   * node independently of OPT_MINUTE_MS.
   */
  private static final long OPT_TOLERANCE_MS = 30_000L;

  /**
   * Upper bound for the deadlineIn(...) await polls: event-time path (window advances with the data
   * itself), C-omitted first result, watermark-destroy recompute, and E-path close/destroy. 30s
   * covers CI-slow back-write delays; on timeout assertEquals fails.
   */
  private static final long OPT_AWAIT_MS = 30_000L;

  private static BaseEnv env() {
    return StreamEnvFactory.getEnv();
  }

  @BeforeClass
  public static void setUp() throws Exception {
    StreamEnvFactory.getEnv(1)
        .getConfig()
        .getCommonConfig()
        .setSubscriptionEnabled(true)
        .setQueryMemoryProportion("1:100:200:50:200:200:200:50:1000")
        .setDataRegionConsensusProtocolClass(ConsensusFactory.IOT_CONSENSUS);
    // OPT-18 uses one tablet so a system-time trigger cannot split its incremental computation;
    // give that tablet enough WAL queue budget without raising the WAL buffer's entry limit.
    final Properties dataNodeProperties = new Properties();
    dataNodeProperties.setProperty("reject_proportion", "0.65");
    dataNodeProperties.setProperty("wal_buffer_queue_proportion", "0.25");
    ((MppCommonConfig) StreamEnvFactory.getEnv(1).getConfig().getDataNodeCommonConfig())
        .updateProperties(dataNodeProperties);
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
      for (int i = 1; i <= 39; i++) {
        try {
          statement.execute(String.format("DROP DATABASE opt%02d", i));
        } catch (Exception ignored) {
          // database may not exist if a test failed before creating it
        }
      }
      // OPT-40 .. OPT-79 use opt40..opt79 (OPT-70..79 and OPT-40..69 are the two batches)
      for (int i = 40; i <= 79; i++) {
        try {
          statement.execute(String.format("DROP DATABASE opt%02d", i));
        } catch (Exception ignored) {
          // database may not exist if a test failed before creating it
        }
      }
    }
  }

  private void createDatabase(final Statement statement, final String databaseName)
      throws Exception {
    statement.execute("CREATE DATABASE " + databaseName);
    statement.execute("USE " + databaseName);
  }

  /** Reads all sum values written back into the given output table. */
  private static List<Double> querySums(final Statement statement, final String table)
      throws SQLException {
    return querySums(statement, table, null);
  }

  /** Reads all sum values written back into the given output table for one device group. */
  private static List<Double> querySums(
      final Statement statement, final String table, final String deviceId) throws SQLException {
    final List<Double> sums = new ArrayList<>();
    final String where = deviceId == null ? "" : " WHERE device_id = '" + deviceId + "'";
    try (ResultSet rs = statement.executeQuery("SELECT sum_value FROM " + table + where)) {
      while (rs.next()) {
        sums.add(rs.getDouble(1));
      }
    }
    return sums;
  }

  /** Asserts the output table holds exactly one row with the expected sum. */
  private static void assertSingleSum(
      final Statement statement, final String table, final double expected) throws SQLException {
    assertSingleSum(statement, table, null, expected);
  }

  /** Asserts the output table holds exactly one row for the given device group. */
  private static void assertSingleSum(
      final Statement statement, final String table, final String deviceId, final double expected)
      throws SQLException {
    final List<Double> sums = querySums(statement, table, deviceId);
    assertEquals(1, sums.size());
    assertEquals(expected, sums.get(0), 1e-6);
  }

  /**
   * Reads the sum values written back for one device group's TARGET window only (time = window
   * start). The max_delay_window_close cases force-close a second window (W2/W2') with the same
   * option while the test still asserts on the first one, so a device-wide count would also see the
   * other window's row; the case list itself prescribes "query the target group and target window".
   */
  private static List<Double> queryWindowSums(
      final Statement statement, final String table, final String deviceId, final long windowStart)
      throws SQLException {
    final List<Double> sums = new ArrayList<>();
    try (ResultSet rs =
        statement.executeQuery(
            "SELECT sum_value FROM "
                + table
                + " WHERE device_id = '"
                + deviceId
                + "' AND time = "
                + windowStart)) {
      while (rs.next()) {
        sums.add(rs.getDouble(1));
      }
    }
    return sums;
  }

  /** Asserts the TARGET window row (time = window start) holds exactly the expected sum. */
  private static void assertSingleWindowSum(
      final Statement statement,
      final String table,
      final String deviceId,
      final long windowStart,
      final double expected)
      throws SQLException {
    final List<Double> sums = queryWindowSums(statement, table, deviceId, windowStart);
    assertEquals(1, sums.size());
    assertEquals(expected, sums.get(0), 1e-6);
  }

  /**
   * Reads the sum written back for the TARGET window only (time = window start), no device filter.
   * Used by non-partitioned streams (e.g., OPT-09) whose output table has no device_id column.
   */
  private static List<Double> queryWindowSumsNoPartition(
      final Statement statement, final String table, final long windowStart) throws SQLException {
    final List<Double> sums = new ArrayList<>();
    try (ResultSet rs =
        statement.executeQuery("SELECT sum_value FROM " + table + " WHERE time = " + windowStart)) {
      while (rs.next()) {
        sums.add(rs.getDouble(1));
      }
    }
    return sums;
  }

  /**
   * Asserts the TARGET window row (time = window start) holds exactly the expected sum, no device
   * filter.
   */
  private static void assertSingleWindowSumNoPartition(
      final Statement statement, final String table, final long windowStart, final double expected)
      throws SQLException {
    final List<Double> sums = queryWindowSumsNoPartition(statement, table, windowStart);
    assertEquals(1, sums.size());
    assertEquals(expected, sums.get(0), 1e-6);
  }

  /** Asserts the output table is empty after observing it for a bounded period. */
  private static void assertEmptyAfterObservation(final Statement statement, final String table)
      throws Exception {
    Thread.sleep(NEGATIVE_OBSERVE_MS);
    try (ResultSet rs = statement.executeQuery("SELECT * FROM " + table)) {
      assertFalse("expected no rows in " + table + " yet", rs.next());
    }
  }

  // -----------------------------------------------------------------------------------------------
  // Shared skeleton for OPT-70 .. OPT-79: target-group/target-window queries and assertions,
  // inserts, stream creation, and the runners for the E and S paths
  // -----------------------------------------------------------------------------------------------

  /** Current time plus the given milliseconds, used as a polling deadline. */
  private static long deadlineIn(final long millis) {
    return System.currentTimeMillis() + millis;
  }

  /**
   * Converts a minute mark of the case table into event time: atMark(base, 10) is the position of
   * mark 10:00.
   */
  private static long atMark(final long base, final int storyMinutes) {
    return base + storyMinutes * OPT_MINUTE_MS;
  }

  /**
   * Renders milliseconds in a time unit CREATE STREAM accepts (s): every value used by this group
   * of cases is a whole number of seconds.
   */
  private static String seconds(final long millis) {
    return (millis / 1000L) + "s";
  }

  /**
   * Builds the WITH clause. {@code leftClosed} is tri-state: {@code null} = omitted by the case
   * text (the option is not written, so the default true applies), while {@code TRUE}/{@code FALSE}
   * are written explicitly; passing {@code NONE} for the other options means omitted as well.
   */
  private static String optionsClause(
      final Boolean leftClosed,
      final long watermarkMs,
      final long latenessMs,
      final long maxDelayMs,
      final long idleMs) {
    final List<String> options = new ArrayList<>();
    if (leftClosed != null) {
      options.add("left_closed = " + leftClosed);
    }
    if (watermarkMs != NONE) {
      options.add("watermark = " + seconds(watermarkMs));
    }
    if (latenessMs != NONE) {
      options.add("allowed_lateness = " + seconds(latenessMs));
    }
    if (maxDelayMs != NONE) {
      options.add("max_delay_window_close = " + seconds(maxDelayMs));
    }
    if (idleMs != NONE) {
      options.add("max_idle_window_retain = " + seconds(idleMs));
    }
    return " WITH (" + String.join(", ", options) + ")";
  }

  /**
   * Creates the database and the tables, then a PARTITION BY device_id TUMBLE(10s) stream
   * (LEFT_CLOSED is tri-state).
   */
  private void createStream(
      final Statement statement,
      final String databaseName,
      final String streamName,
      final long base,
      final Boolean leftClosed,
      final long watermarkMs,
      final long latenessMs,
      final long maxDelayMs,
      final long idleMs)
      throws Exception {
    createDatabase(statement, databaseName);
    statement.execute(
        "CREATE TABLE t1(time TIMESTAMP TIME, device_id STRING TAG, value DOUBLE FIELD)");
    statement.execute(
        "CREATE TABLE "
            + OUTPUT_TABLE
            + "(time TIMESTAMP TIME, device_id STRING TAG, sum_value DOUBLE FIELD)");
    statement.execute(
        "CREATE STREAM "
            + streamName
            + " FROM t1 PARTITION BY device_id TUMBLE("
            + seconds(OPT_WINDOW_MS)
            + ", "
            + base
            + ") INTO "
            + OUTPUT_TABLE
            + "(time, device_id, sum_value) "
            + "SELECT ${START_TIME}, ${1}, sum(value) FROM ${ROWS}"
            + optionsClause(leftClosed, watermarkMs, latenessMs, maxDelayMs, idleMs));
    statement.execute("START STREAM " + streamName);
  }

  /** Inserts one row; the case requires every auxiliary progress record to use v=0. */
  private static void insertRow(
      final Statement statement, final String deviceId, final long eventTime, final double value)
      throws SQLException {
    statement.execute(
        "INSERT INTO t1(time, device_id, value) VALUES ("
            + eventTime
            + ", '"
            + deviceId
            + "', "
            + value
            + ")");
  }

  /**
   * Queries the output rows of the target group and the target window: filters by device_id first,
   * then picks the row whose time equals the window start.
   *
   * <p>The case text requires querying the target group and the target window of the output table
   * at every key point. Under left-open right-closed the row of mark 10:00 falls into the previous
   * window, and that window produces results too, so checking whether the whole table is empty is
   * not enough.
   */
  private static List<Double> queryTargetWindow(
      final Statement statement, final String deviceId, final long windowStart)
      throws SQLException {
    final String sql =
        "SELECT time, sum_value FROM " + OUTPUT_TABLE + " WHERE device_id = '" + deviceId + "'";
    final List<Double> sums = new ArrayList<>();
    try (ResultSet rs = statement.executeQuery(sql)) {
      while (rs.next()) {
        if (rs.getLong(1) == windowStart) {
          sums.add(rs.getDouble(2));
        }
      }
    }
    return sums;
  }

  /**
   * Asserts the target window of the target group holds exactly one row equal to the expectation.
   */
  private static void assertTargetWindowSum(
      final Statement statement,
      final String deviceId,
      final long windowStart,
      final double expected)
      throws SQLException {
    final List<Double> sums = queryTargetWindow(statement, deviceId, windowStart);
    assertEquals("row count of target window " + windowStart, 1, sums.size());
    assertEquals("aggregated value of target window " + windowStart, expected, sums.get(0), 1e-6);
  }

  /** Asserts the target window of the target group currently has no output row. */
  private static void assertNoTargetWindowRow(
      final Statement statement, final String deviceId, final long windowStart)
      throws SQLException {
    final List<Double> sums = queryTargetWindow(statement, deviceId, windowStart);
    assertEquals("target window " + windowStart + " should have no result", 0, sums.size());
  }

  /** Polls the target window until the expected aggregated value appears or the deadline passes. */
  private static void awaitTargetWindowSum(
      final Statement statement,
      final String deviceId,
      final long windowStart,
      final double expected,
      final long deadline)
      throws Exception {
    List<Double> sums = queryTargetWindow(statement, deviceId, windowStart);
    while ((sums.size() != 1 || Math.abs(sums.get(0) - expected) > 1e-6)
        && System.currentTimeMillis() < deadline) {
      Thread.sleep(200L);
      sums = queryTargetWindow(statement, deviceId, windowStart);
    }
    assertEquals("row count of target window " + windowStart, 1, sums.size());
    assertEquals("aggregated value of target window " + windowStart, expected, sums.get(0), 1e-6);
  }

  /**
   * Repeats querying the target window until the given deadline, asserting the aggregated value
   * never changes (used for "no revision during the observation period").
   */
  private static void awaitUnchangedTargetWindow(
      final Statement statement,
      final String deviceId,
      final long windowStart,
      final double expected,
      final long until)
      throws Exception {
    while (System.currentTimeMillis() < until) {
      assertTargetWindowSum(statement, deviceId, windowStart, expected);
      Thread.sleep(200L);
    }
    assertTargetWindowSum(statement, deviceId, windowStart, expected);
  }

  /**
   * Inserts one row, waits for the stream computation to settle ({@code NEGATIVE_OBSERVE_MS}), then
   * asserts the aggregated value of the target window is unchanged.
   *
   * <p>The case text says "query right after the write", but asserting immediately may pass only
   * because the computation has not settled yet, which proves nothing about "no revision"; staying
   * unchanged after a settle wait is the trustworthy negative conclusion.
   */
  private static void insertRowThenAssertUnchanged(
      final Statement statement,
      final String deviceId,
      final long eventTime,
      final double value,
      final long windowStart,
      final double expected)
      throws Exception {
    insertRow(statement, deviceId, eventTime, value);
    Thread.sleep(NEGATIVE_OBSERVE_MS);
    assertTargetWindowSum(statement, deviceId, windowStart, expected);
  }

  /**
   * Inserts an equal-point progress record, waits for the computation to settle, then asserts the
   * target window still has no result (the window must not close).
   */
  private static void insertRowThenAssertNoRow(
      final Statement statement,
      final String deviceId,
      final long eventTime,
      final double value,
      final long windowStart)
      throws Exception {
    insertRow(statement, deviceId, eventTime, value);
    Thread.sleep(NEGATIVE_OBSERVE_MS);
    assertNoTargetWindowRow(statement, deviceId, windowStart);
  }

  /**
   * The E group (event-time progress) path of OPT-40..OPT-79. The target window starts at mark
   * 10:00 (= base) and ends at mark 10:10 (= base+10s).
   *
   * <p>LEFT_CLOSED decides the window boundary ownership and the comparison operator: with true or
   * omitted the window is {@code [10:00,10:10)}, the row of mark 10:00 is inside it and the row of
   * 10:10 falls into the next window, and both closing and destroying act when the watermark
   * reaches the comparison line; with false the window is {@code (10:00,10:10]}, the row of mark
   * 10:00 falls into the previous window and the row of 10:10 is inside it, and the comparison line
   * must be strictly passed, so every comparison line first gets an equal-point (v=0) probe
   * confirming no action and then a v=0 probe 1ms later triggering the action. Every auxiliary
   * progress record uses v=0 and does not shift the expected sums.
   *
   * <p>The negative checks phrased as "query right after the write" in the case text are uniformly
   * implemented as "write -> settle wait (NEGATIVE_OBSERVE_MS) -> assert unchanged": asserting
   * immediately after the write may pass only because the computation has not settled yet, which
   * proves nothing about "no revision / no close".
   *
   * @param leftClosed LEFT_CLOSED; null means omitted by the case text (default true)
   * @param watermarkMs WATERMARK; NONE means omitted (default 0, so the watermark equals the max
   *     event time)
   * @param latenessMs ALLOWED_LATENESS; NONE means omitted (default 0, so the lateness line equals
   *     the window end)
   */
  private static void runEventTimePath(
      final Statement statement,
      final long base,
      final Boolean leftClosed,
      final long watermarkMs,
      final long latenessMs)
      throws Exception {
    final boolean leftOpen = Boolean.FALSE.equals(leftClosed);
    final long windowStart = base; // mark 10:00
    final long windowEnd = base + OPT_WINDOW_MS; // mark 10:10
    final long watermark = watermarkMs == NONE ? 0L : watermarkMs;
    // left-closed right-open acts on reaching the comparison line, left-open right-closed 1ms after
    final long triggerOffset = leftOpen ? 1L : 0L;
    // event time of the close probe: watermark = max event time - WATERMARK; mark 10:10 when W is
    // omitted, 10:12 when W=2m
    final long closeProbe = windowEnd + watermark;
    final double closeSum = leftOpen ? 6.0 : 3.0;
    final double destroySum = leftOpen ? 14.0 : 11.0;

    // 10:00(v=1), 10:05(v=2), 10:10(v=4): LEFT_CLOSED decides which rows belong to the target
    // window
    insertRow(statement, E_GROUP, windowStart, 1.0);
    insertRow(statement, E_GROUP, atMark(base, 5), 2.0);
    insertRow(statement, E_GROUP, windowEnd, 4.0);

    if (leftOpen) {
      // (10:00,10:10]: the 10:10 boundary row is already inside the window, so the close is 1ms
      // away
      Thread.sleep(NEGATIVE_OBSERVE_MS);
      assertNoTargetWindowRow(statement, E_GROUP, windowStart);
      if (watermark > 0) {
        // W=2m: first write the equal-point probe where the watermark exactly equals the window end
        // (mark 10:12); the window must still not close after settling
        insertRowThenAssertNoRow(statement, E_GROUP, closeProbe, 0.0, windowStart);
      }
      insertRow(statement, E_GROUP, closeProbe + triggerOffset, 0.0);
    } else if (watermark > 0) {
      // [10:00,10:10): the progress record of mark 10:12 brings the watermark to the window end, so
      // the window closes
      insertRow(statement, E_GROUP, closeProbe, 0.0);
    }
    // with W omitted and left-closed right-open, the 10:10 boundary row is itself the progress
    // record that reaches the comparison line
    awaitTargetWindowSum(statement, E_GROUP, windowStart, closeSum, deadlineIn(OPT_AWAIT_MS));

    double expected = closeSum;
    if (latenessMs != NONE) {
      // late data written after the close: must still not be revised after settling (asserting
      // immediately may mean nothing more than "not computed yet")
      insertRowThenAssertUnchanged(statement, E_GROUP, atMark(base, 6), 8.0, windowStart, closeSum);
      // end the retention period: lateness line = window end + ALLOWED_LATENESS, probe event time =
      // lateness line + WATERMARK
      final long destroyProbe = windowEnd + latenessMs + watermark;
      if (leftOpen) {
        // reaching the lateness line does not destroy yet; 1ms after passing it the window is
        // destroyed and fully recomputed
        insertRowThenAssertUnchanged(statement, E_GROUP, destroyProbe, 0.0, windowStart, closeSum);
        insertRow(statement, E_GROUP, destroyProbe + triggerOffset, 0.0);
      } else {
        // the watermark reaching the lateness line destroys the window and fully recomputes it
        insertRow(statement, E_GROUP, destroyProbe, 0.0);
      }
      awaitTargetWindowSum(statement, E_GROUP, windowStart, destroySum, deadlineIn(OPT_AWAIT_MS));
      expected = destroySum;
    }
    // the window is already destroyed (with A omitted, closing destroys it): late data with event
    // time <= the expiry line (= the window end) is dropped
    insertRow(statement, E_GROUP, atMark(base, 7), 16.0);
    Thread.sleep(NEGATIVE_OBSERVE_MS);
    assertTargetWindowSum(statement, E_GROUP, windowStart, expected);
  }

  /**
   * The S group (system-time triggered) path of OPT-40..OPT-79. The S window starts at mark 20:00
   * (= sBase) and ends at mark 20:10 (= sBase+10s): 20:05(v=10) is written at system time 00:30 and
   * opens the window, and 20:07(v=0) is written at 00:32 as an auxiliary progress record.
   *
   * <p>Closing: when C is not omitted it is triggered by "window open system time + C" (case mark
   * 00:40); when C is omitted the system time never closes the window, so only the watermark
   * reaching the window end can close it (under left-open right-closed mark 20:12 is the
   * equal-point probe and 20:12.001 closes it). After the first result 20:06(v=20) is written as
   * late data, and destruction has two routes. With A &gt; 0 the window survives its close and
   * keeps a late list, so either the idle trigger fires at "receive system time of that late row +
   * I" (case mark 00:46, when I is not omitted) or a watermark probe passing the lateness line
   * destroys the window and fully recomputes it: the result is revised to 30. With A omitted (=0)
   * no retention is granted, so close and destroy are one atomic step no matter whether the system
   * time (C) or the watermark closes the window, and the late row lands inside the expired line and
   * is dropped: the result stays 10.
   *
   * <p>As in the E path, the negative checks phrased as "query right after the write" in the case
   * text are uniformly implemented as "write -> settle wait (NEGATIVE_OBSERVE_MS) -> assert
   * unchanged", so that an unsettled computation cannot make them pass falsely.
   *
   * @param leftClosed LEFT_CLOSED; null means omitted by the case text (default true)
   * @param watermarkMs WATERMARK; NONE means omitted (default 0)
   * @param latenessMs ALLOWED_LATENESS; NONE means omitted (default 0)
   * @param maxDelayMs MAX_DELAY_WINDOW_CLOSE; NONE means omitted (default 0, system time not
   *     considered)
   * @param idleMs MAX_IDLE_WINDOW_RETAIN; NONE means omitted (default 0, system time not
   *     considered)
   */
  private static void runSystemTimePath(
      final Statement statement,
      final long sBase,
      final Boolean leftClosed,
      final long watermarkMs,
      final long latenessMs,
      final long maxDelayMs,
      final long idleMs)
      throws Exception {
    final boolean leftOpen = Boolean.FALSE.equals(leftClosed);
    final long windowStart = sBase; // mark 20:00
    final long windowEnd = sBase + OPT_WINDOW_MS; // mark 20:10
    final long watermark = watermarkMs == NONE ? 0L : watermarkMs;
    final boolean waitsForLateData = latenessMs != NONE;
    // left-closed right-open acts on reaching the comparison line, left-open right-closed 1ms after
    final long triggerOffset = leftOpen ? 1L : 0L;
    // event time of the watermark probes: the close line corresponds to mark 20:12 (20:10 when W is
    // omitted), the lateness line to 20:12/20:15/20:17
    final long closeProbe = windowEnd + watermark;
    final long destroyProbe = windowEnd + (waitsForLateData ? latenessMs : 0L) + watermark;

    // mark 00:30: write 20:05(v=10) and open the window; mark 00:32: write the auxiliary progress
    // record 20:07(v=0)
    final long t0 = System.currentTimeMillis();
    insertRow(statement, S_GROUP, atMark(sBase, 5), 10.0);
    // The engine arms the close deadline when the row reaches it, i.e. at or after t0: the first
    // insert into a fresh database pays the lazy storage-stack initialization (see OPT-16) before
    // the engine can see the row. t0 stays the anchor of the "no result before the close line"
    // checks - an anchor earlier than the real window open can only make them more conservative -
    // while the wait for the result is anchored on tInserted, the moment the data is known to be
    // in the server, so the initialization time is not charged to the close deadline.
    final long tInserted = System.currentTimeMillis();
    Thread.sleep(2 * OPT_MINUTE_MS);
    insertRow(statement, S_GROUP, atMark(sBase, 7), 0.0);

    if (maxDelayMs != NONE) {
      // close system time = window open system time + C (case mark 00:40): the target window must
      // have no result before that
      final long closeLine = t0 + maxDelayMs;
      while (System.currentTimeMillis() < closeLine - 200L) {
        assertNoTargetWindowRow(statement, S_GROUP, windowStart);
        Thread.sleep(200L);
      }
      assertNoTargetWindowRow(statement, S_GROUP, windowStart);
      awaitTargetWindowSum(
          statement, S_GROUP, windowStart, 10.0, tInserted + maxDelayMs + OPT_TOLERANCE_MS);
    } else {
      // C omitted: the system time does not close the window (still no result at case mark 00:40
      // and during the observation that follows), only the watermark can close it
      final long observeUntil = t0 + 10 * OPT_MINUTE_MS; // mark 00:40
      while (System.currentTimeMillis() < observeUntil) {
        assertNoTargetWindowRow(statement, S_GROUP, windowStart);
        Thread.sleep(200L);
      }
      assertNoTargetWindowRow(statement, S_GROUP, windowStart);
      if (leftOpen) {
        // (20:00,20:10]: reaching the close line does not close the window; closing happens 1ms
        // after passing it
        insertRowThenAssertNoRow(statement, S_GROUP, closeProbe, 0.0, windowStart);
        insertRow(statement, S_GROUP, closeProbe + triggerOffset, 0.0);
      } else {
        // [20:00,20:10): the watermark reaching the close line closes the window (with W omitted,
        // mark 20:10 is that progress record)
        insertRow(statement, S_GROUP, closeProbe, 0.0);
      }
      awaitTargetWindowSum(statement, S_GROUP, windowStart, 10.0, deadlineIn(OPT_AWAIT_MS));
    }

    // mark 00:41 (1 minute after the first result): write 20:06(v=20) as late data; it must still
    // not be revised after settling
    final long tFirstResult = System.currentTimeMillis();
    Thread.sleep(Math.max(0, tFirstResult + OPT_MINUTE_MS - System.currentTimeMillis()));
    final long tLateRow = System.currentTimeMillis();
    insertRowThenAssertUnchanged(statement, S_GROUP, atMark(sBase, 6), 20.0, windowStart, 10.0);
    // The settle wait of the call above has passed, so the engine's receive instant of the late
    // row is behind this point: like tInserted above, this deliberately late anchor keeps the wait
    // for the revised result from being charged for the delivery latency of the insert, while
    // tLateRow stays the (earlier, conservative) anchor of the "no revision yet" observation.
    final long tLateRowSettled = System.currentTimeMillis();

    if (idleMs != NONE && waitsForLateData) {
      // I>0 and A>0. Idle destroy arms at T_close+I on close, resets to tLateRow_receive+I on each
      // late row. If the first result settles so late that tLateRow is already past T_close+I, the
      // window was destroyed by onTick before the late row arrived, the late row is dropped at the
      // expired-line gate, and no revision happens — the window keeps its first result (10.0).
      final long closeTime = t0 + maxDelayMs;
      final long idleDestroyNoLate = closeTime + idleMs;
      if (tLateRow > idleDestroyNoLate) {
        // 首结果落库过慢：迟到行在窗口 idle 销毁后被过期线丢弃，不触发改写，expected 保持 10.0。
        // 显式 warning 提示该用例未按预期走"改写 30.0"路径，便于在 CI 日志中识别降级用例。
        LOGGER.warn(
            "[idle-drop] windowStart={}: first result settled {}ms after T_close, exceeds I={}ms;"
                + " late row dropped at expired line, expected stays 10.0, recompute-30.0 skipped",
            windowStart,
            tFirstResult - closeTime,
            idleMs);
        assertTargetWindowSum(statement, S_GROUP, windowStart, 10.0);
      } else {
        // idle destruction system time = receive system time of that late row + I (case mark 00:46)
        Thread.sleep(Math.max(0, tLateRow + idleMs - 200L - System.currentTimeMillis()));
        assertTargetWindowSum(statement, S_GROUP, windowStart, 10.0);
        awaitTargetWindowSum(
            statement, S_GROUP, windowStart, 30.0, tLateRowSettled + idleMs + OPT_TOLERANCE_MS);
      }
    } else if (waitsForLateData) {
      // still no revision at the case mark 00:46 observation point; afterwards a watermark probe
      // passing the lateness line destroys the window and fully recomputes
      awaitUnchangedTargetWindow(
          statement, S_GROUP, windowStart, 10.0, tLateRow + 5 * OPT_MINUTE_MS);
      if (leftOpen) {
        // reaching the lateness line does not destroy yet; 1ms after passing it the window is
        // destroyed and fully recomputed
        insertRowThenAssertUnchanged(statement, S_GROUP, destroyProbe, 0.0, windowStart, 10.0);
        insertRow(statement, S_GROUP, destroyProbe + triggerOffset, 0.0);
      } else {
        // the watermark reaching the lateness line destroys the window and fully recomputes it
        insertRow(statement, S_GROUP, destroyProbe, 0.0);
      }
      awaitTargetWindowSum(statement, S_GROUP, windowStart, 30.0, deadlineIn(OPT_AWAIT_MS));
    } else {
      // A omitted (=0): no retention is granted, so closing and destroying the window are one
      // atomic step, whether the system time (C) or the watermark closed it; the late row written
      // at 00:41 lands inside the expired line and is dropped.
      if (maxDelayMs != NONE) {
        // the case text still queries the 00:46 observation point and then writes its event-time
        // probe (mark 20:12, or 20:10 when W is omitted; left-open right-closed acts 1ms later);
        // the destroyed window must not be revived
        awaitUnchangedTargetWindow(
            statement, S_GROUP, windowStart, 10.0, tLateRow + 5 * OPT_MINUTE_MS);
        insertRowThenAssertUnchanged(statement, S_GROUP, destroyProbe, 0.0, windowStart, 10.0);
        if (leftOpen) {
          insertRow(statement, S_GROUP, destroyProbe + triggerOffset, 0.0);
          Thread.sleep(NEGATIVE_OBSERVE_MS);
          assertTargetWindowSum(statement, S_GROUP, windowStart, 10.0);
        }
      } else {
        Thread.sleep(NEGATIVE_OBSERVE_MS);
        assertTargetWindowSum(statement, S_GROUP, windowStart, 10.0);
      }
    }
  }

  /**
   * OPT-01 watermark default value. Create a TUMBLE(10m) stream WITHOUT any WITH clause, write
   * 10:00(v=1) and 10:05(v=2) into one group, then 10:10(v=0) as an event-time progress record.
   * Before the 10:10 record the window [10:00,10:10) must not be closed (watermark 10:05 < window
   * end); after it the window must close and output SUM(v)=3 (the v=0 record belongs to the next
   * window by left-closed semantics). This proves the default WATERMARK is 0: watermark = max event
   * time, so an event time exactly at the window end closes the window immediately.
   *
   * <p>Oracle: W1 contains exactly rows 1.0 and 2.0 (hand-counted); expected sum 3.0 comes only
   * from that hand count, never from reading back the product output.
   */
  @Test
  public void testOPT01WatermarkDefaultValue() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      createDatabase(stmt, "opt01");
      stmt.execute(
          "CREATE TABLE t1(time TIMESTAMP TIME, device_id STRING TAG, value DOUBLE FIELD)");
      stmt.execute("CREATE TABLE sum_calc(time TIMESTAMP TIME, sum_value DOUBLE FIELD)");

      final long base = System.currentTimeMillis() + 2000;

      stmt.execute(
          "CREATE STREAM opt01 "
              + "FROM t1 TUMBLE(10m, "
              + base
              + ") INTO sum_calc(time, sum_value) "
              + "SELECT ${START_TIME}, sum(value) FROM ${ROWS}");
      stmt.execute("START STREAM opt01");

      // W1 = [base, base+10m). Two in-window rows.
      stmt.execute("INSERT INTO t1(time, device_id, value) VALUES (" + base + ", 'd1', 1.0)");
      stmt.execute(
          "INSERT INTO t1(time, device_id, value) VALUES (" + (base + 300_000L) + ", 'd1', 2.0)");

      // Before the progress record the window must stay open: no output.
      assertEmptyAfterObservation(stmt, "sum_calc");

      // Progress record at exactly the window end: belongs to W2 (left-closed), only advances
      // the watermark. With default WATERMARK=0 the watermark reaches the window end and W1
      // closes.
      stmt.execute(
          "INSERT INTO t1(time, device_id, value) VALUES (" + (base + 600_000L) + ", 'd1', 0.0)");

      AWAIT.untilAsserted(
          () -> {
            final List<Double> sums = querySums(stmt, "sum_calc");
            assertEquals(1, sums.size());
            assertEquals(3.0, sums.get(0), 1e-6);
          });

      // The window must be output exactly once: still one row afterwards.
      Thread.sleep(NEGATIVE_OBSERVE_MS);
      assertSingleSum(stmt, "sum_calc", 3.0);
    }
  }

  /**
   * OPT-02 partition-scoped event-time progress. Create a PARTITION BY device_id TUMBLE(10m)
   * stream. Group a writes 10:00(v=1) and its own 10:10(v=0) progress record; group b writes only
   * 10:00(v=10). After a's advance only group a's window [10:00,10:10) may output SUM(v)=1; group b
   * must stay silent because the watermark granularity is the partition, so a's progress never
   * closes b's window. Only after b writes its own 10:10(v=0) progress record does b's window close
   * and output SUM(v)=10.
   *
   * <p>Oracle: group a's W1 contains exactly row 1.0; group b's W1 contains exactly row 10.0
   * (hand-counted, values deliberately far apart so a result is attributable to one group).
   */
  @Test
  public void testOPT02PartitionEventTimeProgressIsolation() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      createDatabase(stmt, "opt02");
      stmt.execute(
          "CREATE TABLE t1(time TIMESTAMP TIME, device_id STRING TAG, value DOUBLE FIELD)");
      stmt.execute(
          "CREATE TABLE sum_calc(time TIMESTAMP TIME, device_id STRING TAG, sum_value DOUBLE"
              + " FIELD)");

      final long base = System.currentTimeMillis() + 2000;

      stmt.execute(
          "CREATE STREAM opt02 "
              + "FROM t1 PARTITION BY device_id TUMBLE(10m, "
              + base
              + ") INTO sum_calc(time, device_id, sum_value) "
              + "SELECT ${START_TIME}, ${1}, sum(value) FROM ${ROWS}");
      stmt.execute("START STREAM opt02");

      // Group a: in-window row plus its own progress record (closes a's W1).
      stmt.execute("INSERT INTO t1(time, device_id, value) VALUES (" + base + ", 'a', 1.0)");
      stmt.execute(
          "INSERT INTO t1(time, device_id, value) VALUES (" + (base + 600_000L) + ", 'a', 0.0)");
      // Group b: in-window row only, no progress yet.
      stmt.execute("INSERT INTO t1(time, device_id, value) VALUES (" + base + ", 'b', 10.0)");

      // a's window must close (its own progress record) and output SUM=1.
      AWAIT.untilAsserted(
          () -> {
            final List<Double> sums = querySums(stmt, "sum_calc", "a");
            assertEquals(1, sums.size());
            assertEquals(1.0, sums.get(0), 1e-6);
          });

      // b must NOT be closed by a's advance: observe and stay empty.
      try (ResultSet rs = stmt.executeQuery("SELECT * FROM sum_calc WHERE device_id = 'b'")) {
        assertFalse("group b must not be closed by group a's progress", rs.next());
      }

      // b writes its own progress record; only then may b's window close.
      stmt.execute(
          "INSERT INTO t1(time, device_id, value) VALUES (" + (base + 600_000L) + ", 'b', 0.0)");
      AWAIT.untilAsserted(
          () -> {
            final List<Double> sums = querySums(stmt, "sum_calc", "b");
            assertEquals(1, sums.size());
            assertEquals(10.0, sums.get(0), 1e-6);
          });

      // Final state: each group output exactly once, values unchanged.
      Thread.sleep(NEGATIVE_OBSERVE_MS);
      assertSingleSum(stmt, "sum_calc", "a", 1.0);
      assertSingleSum(stmt, "sum_calc", "b", 10.0);
    }
  }

  /**
   * OPT-03 watermark close boundary with out-of-order input. Create a TUMBLE(10m) stream WITH
   * watermark=2m. Write 10:00(v=1), then 10:11:59.999(v=0): the max event time now exceeds the
   * window end, but the watermark is 10:09:59.999 < 10:10, so the window must NOT close (first
   * query empty). Write the out-of-order point 10:00:30(v=2): its event time is below the max event
   * time, so it must not advance the watermark either (second query still empty). Finally write
   * 10:12(v=0): the watermark reaches 10:12-2m = 10:10 >= window end, so the window closes and
   * outputs SUM(v)=3 (rows 1.0 and 2.0; both v=0 progress records belong to W2).
   *
   * <p>Oracle: W1 contains exactly rows 1.0 and 2.0 (hand-counted). The 10:11:59.999 record pins
   * the boundary: if the engine closes on max event time instead of the watermark, the first
   * assertion turns red.
   */
  @Test
  public void testOPT03WatermarkBoundaryAndOutOfOrder() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      createDatabase(stmt, "opt03");
      stmt.execute(
          "CREATE TABLE t1(time TIMESTAMP TIME, device_id STRING TAG, value DOUBLE FIELD)");
      stmt.execute("CREATE TABLE sum_calc(time TIMESTAMP TIME, sum_value DOUBLE FIELD)");

      final long base = System.currentTimeMillis() + 2000;

      stmt.execute(
          "CREATE STREAM opt03 "
              + "FROM t1 TUMBLE(10m, "
              + base
              + ") INTO sum_calc(time, sum_value) "
              + "SELECT ${START_TIME}, sum(value) FROM ${ROWS} "
              + "WITH (watermark = 2m)");
      stmt.execute("START STREAM opt03");

      // W1 = [base, base+10m). In-window row.
      stmt.execute("INSERT INTO t1(time, device_id, value) VALUES (" + base + ", 'd1', 1.0)");

      // Max event time reaches 10:11:59.999 but watermark = 10:09:59.999 < 10:10: no close.
      stmt.execute(
          "INSERT INTO t1(time, device_id, value) VALUES (" + (base + 659_999L) + ", 'd1', 0.0)");
      assertEmptyAfterObservation(stmt, "sum_calc");

      // Out-of-order point 10:00:30: event time < max event time, must not advance watermark.
      stmt.execute(
          "INSERT INTO t1(time, device_id, value) VALUES (" + (base + 30_000L) + ", 'd1', 2.0)");
      assertEmptyAfterObservation(stmt, "sum_calc");

      // 10:12: watermark = 10:12 - 2m = 10:10 >= window end 10:10, W1 closes with SUM=3.
      stmt.execute(
          "INSERT INTO t1(time, device_id, value) VALUES (" + (base + 720_000L) + ", 'd1', 0.0)");
      AWAIT.untilAsserted(
          () -> {
            final List<Double> sums = querySums(stmt, "sum_calc");
            assertEquals(1, sums.size());
            assertEquals(3.0, sums.get(0), 1e-6);
          });

      Thread.sleep(NEGATIVE_OBSERVE_MS);
      assertSingleSum(stmt, "sum_calc", 3.0);
    }
  }

  /**
   * OPT-04 watermark time units. Create five TUMBLE(10m) streams with watermark=1ms / 1s / 1m / 1h
   * / 1d. For each stream write the same window data (10:00 v=1, 10:05 v=2), then a v=0 progress
   * record whose event time is (window end + unit - 1ms) — the watermark stays 1ms below the window
   * end, so no output is allowed yet — and observe. Then write the at-boundary record (window end +
   * unit): the watermark reaches the window end and the window must output SUM=3.
   *
   * <p>Oracle: each stream's W1 contains exactly rows 1.0 and 2.0 (sum 3.0, hand-counted); the
   * close moment must equal window end + configured duration per unit conversion.
   */
  @Test
  public void testOPT04WatermarkTimeUnits() throws Exception {
    final String[] units = {"1ms", "1s", "1m", "1h", "1d"};
    final long[] unitMillis = {1L, 1_000L, 60_000L, 3_600_000L, 86_400_000L};
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      createDatabase(stmt, "opt04");
      final long base = System.currentTimeMillis() + 2000;

      // Create all five streams first: creation must succeed for every supported unit.
      for (int i = 0; i < units.length; i++) {
        final String suffix = units[i].substring(1);
        stmt.execute(
            "CREATE TABLE t_"
                + suffix
                + "(time TIMESTAMP TIME, device_id STRING TAG, value DOUBLE FIELD)");
        stmt.execute(
            "CREATE TABLE sum_" + suffix + "(time TIMESTAMP TIME, sum_value DOUBLE" + " FIELD)");
        stmt.execute(
            "CREATE STREAM opt04_"
                + suffix
                + " FROM t_"
                + suffix
                + " TUMBLE(10m, "
                + base
                + ") INTO sum_"
                + suffix
                + "(time, sum_value) "
                + "SELECT ${START_TIME}, sum(value) FROM ${ROWS} "
                + "WITH (watermark = "
                + units[i]
                + ")");
        stmt.execute("START STREAM opt04_" + suffix);
      }

      // Per unit: in-window rows, below-boundary record (must NOT close), at-boundary record.
      for (int i = 0; i < units.length; i++) {
        final String suffix = units[i].substring(1);
        final String table = "sum_" + suffix;
        final long unit = unitMillis[i];

        stmt.execute(
            "INSERT INTO t_"
                + suffix
                + "(time, device_id, value) VALUES ("
                + base
                + ", 'd1',"
                + " 1.0)");
        stmt.execute(
            "INSERT INTO t_"
                + suffix
                + "(time, device_id, value) VALUES ("
                + (base + 300_000L)
                + ", 'd1', 2.0)");

        // (window end + unit - 1ms): watermark = window end - 1ms < end, window must stay open.
        stmt.execute(
            "INSERT INTO t_"
                + suffix
                + "(time, device_id, value) VALUES ("
                + (base + 600_000L + unit - 1)
                + ", 'd1', 0.0)");
        assertEmptyAfterObservation(stmt, table);

        // (window end + unit): watermark reaches the window end, window closes with SUM=3.
        stmt.execute(
            "INSERT INTO t_"
                + suffix
                + "(time, device_id, value) VALUES ("
                + (base + 600_000L + unit)
                + ", 'd1', 0.0)");
        final String target = table;
        AWAIT.untilAsserted(
            () -> {
              final List<Double> sums = querySums(stmt, target);
              assertEquals(1, sums.size());
              assertEquals(3.0, sums.get(0), 1e-6);
            });
      }
    }
  }

  /**
   * OPT-05 watermark value type and unit constraints (negative). WATERMARK accepts a time duration
   * (ms/s/m/h/d) per the requirements table; a boolean, a string, or an unsupported unit (1mo) must
   * make CREATE STREAM fail with an explicit parameter error, and no stream task may be left behind
   * in the public stream metadata.
   *
   * <p>Oracle: the requirements option table types WATERMARK as "time length" with unit set
   * {ms,s,m,h,d}; anything else is rejected.
   */
  @Test
  public void testOPT05WatermarkValueTypeConstraints() throws Exception {
    final String[] illegalValues = {"true", "'abc'", "1mo"};
    final String[] streamNames = {"opt05_bool", "opt05_str", "opt05_mo"};
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      createDatabase(stmt, "opt05");
      stmt.execute(
          "CREATE TABLE t1(time TIMESTAMP TIME, device_id STRING TAG, value DOUBLE FIELD)");
      stmt.execute("CREATE TABLE sum_calc(time TIMESTAMP TIME, sum_value DOUBLE FIELD)");

      final long base = System.currentTimeMillis() + 2000;
      for (int i = 0; i < illegalValues.length; i++) {
        final String value = illegalValues[i];
        final String streamName = streamNames[i];
        try {
          stmt.execute(
              "CREATE STREAM "
                  + streamName
                  + " FROM t1 TUMBLE(10m, "
                  + base
                  + ") INTO sum_calc(time, sum_value) "
                  + "SELECT ${START_TIME}, sum(value) FROM ${ROWS} "
                  + "WITH (watermark = "
                  + value
                  + ")");
          throw new AssertionError("CREATE STREAM with watermark=" + value + " must be rejected");
        } catch (SQLException expected) {
          // rejected as required
        }
      }

      // Public stream metadata must not contain any of the rejected tasks.
      final List<String> registered = new ArrayList<>();
      try (ResultSet rs = stmt.executeQuery("show streams")) {
        while (rs.next()) {
          registered.add(rs.getString("stream_name"));
        }
      }
      for (final String streamName : streamNames) {
        assertFalse(
            "stream " + streamName + " must not exist in metadata",
            registered.contains(streamName));
      }
    }
  }

  /**
   * OPT-07 allowed_lateness default value. Create a TUMBLE(10m) stream WITHOUT any WITH clause.
   * Write normal data and a v=0 progress record that closes the window by event time (default
   * watermark=0); query the first result. Then write a late row belonging to the closed window and
   * advance the event time again. With default ALLOWED_LATENESS=0 the lateness line equals the
   * window end, so the window is destroyed at close time: the late row must be dropped (a row whose
   * event time is at or below the window computation line joins the full computation list, but the
   * window is already destroyed and the expiry line keeps it out) — output value and output count
   * must stay unchanged.
   *
   * <p>Oracle: first output SUM=3.0 (rows 1.0 and 2.0, hand-counted); the late row v=9 must not
   * change either the value (would be 12.0) or the row count (would be 2 revisions).
   */
  @Test
  public void testOPT07AllowedLatenessDefaultValue() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      createDatabase(stmt, "opt07");
      stmt.execute(
          "CREATE TABLE t1(time TIMESTAMP TIME, device_id STRING TAG, value DOUBLE FIELD)");
      stmt.execute("CREATE TABLE sum_calc(time TIMESTAMP TIME, sum_value DOUBLE FIELD)");

      final long base = System.currentTimeMillis() + 2000;

      stmt.execute(
          "CREATE STREAM opt07 "
              + "FROM t1 TUMBLE(10m, "
              + base
              + ") INTO sum_calc(time, sum_value) "
              + "SELECT ${START_TIME}, sum(value) FROM ${ROWS}");
      stmt.execute("START STREAM opt07");

      // Normal data plus progress record: W1 closes with SUM=3.
      stmt.execute("INSERT INTO t1(time, device_id, value) VALUES (" + base + ", 'd1', 1.0)");
      stmt.execute(
          "INSERT INTO t1(time, device_id, value) VALUES (" + (base + 300_000L) + ", 'd1', 2.0)");
      stmt.execute(
          "INSERT INTO t1(time, device_id, value) VALUES (" + (base + 600_000L) + ", 'd1', 0.0)");
      AWAIT.untilAsserted(
          () -> {
            final List<Double> sums = querySums(stmt, "sum_calc");
            assertEquals(1, sums.size());
            assertEquals(3.0, sums.get(0), 1e-6);
          });

      // Late row for the closed window, then more event-time progress.
      stmt.execute(
          "INSERT INTO t1(time, device_id, value) VALUES (" + (base + 350_000L) + ", 'd1', 9.0)");
      stmt.execute(
          "INSERT INTO t1(time, device_id, value) VALUES (" + (base + 720_000L) + ", 'd1', 0.0)");

      // Default ALLOWED_LATENESS=0: no revision — value and count unchanged.
      Thread.sleep(NEGATIVE_OBSERVE_MS);
      assertSingleSum(stmt, "sum_calc", 3.0);
    }
  }

  /**
   * OPT-08 allowed_lateness per-window isolation. Create a PARTITION BY device_id TUMBLE(10m)
   * stream WITH allowed_lateness=5m. Group a's window is [10:00,10:10) (ends first), group b's
   * window is [10:10,10:20) (ends later). Close both windows, write distinguishable late rows for
   * both, then advance group a's event time to a's lateness line (window end + 5m) FIRST: only
   * group a's result may be revised (3.0 -> 11.0) while group b stays at its first output (10.0).
   * Only after group b reaches its own lateness line (10:20 + 5m) does b revise (10.0 -> 30.0).
   * This proves the lateness line is evaluated per window, not per group or globally.
   *
   * <p>Oracle: a's W1 = 1.0+2.0 (+ late 8.0 after revision = 11.0); b's W1 = 10.0 (+ late 20.0
   * after revision = 30.0), all hand-counted.
   */
  @Test
  public void testOPT08AllowedLatenessWindowIsolation() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      createDatabase(stmt, "opt08");
      stmt.execute(
          "CREATE TABLE t1(time TIMESTAMP TIME, device_id STRING TAG, value DOUBLE FIELD)");
      stmt.execute(
          "CREATE TABLE sum_calc(time TIMESTAMP TIME, device_id STRING TAG, sum_value DOUBLE"
              + " FIELD)");

      final long base = System.currentTimeMillis() + 2000;

      stmt.execute(
          "CREATE STREAM opt08 "
              + "FROM t1 PARTITION BY device_id TUMBLE(10m, "
              + base
              + ") INTO sum_calc(time, device_id, sum_value) "
              + "SELECT ${START_TIME}, ${1}, sum(value) FROM ${ROWS} "
              + "WITH (allowed_lateness = 5m)");
      stmt.execute("START STREAM opt08");

      // Group a: window [base, base+10m). Close it with its own progress record.
      stmt.execute("INSERT INTO t1(time, device_id, value) VALUES (" + base + ", 'a', 1.0)");
      stmt.execute(
          "INSERT INTO t1(time, device_id, value) VALUES (" + (base + 300_000L) + ", 'a', 2.0)");
      stmt.execute(
          "INSERT INTO t1(time, device_id, value) VALUES (" + (base + 600_000L) + ", 'a', 0.0)");

      // Group b: window [base+10m, base+20m). Close it with its own progress record.
      stmt.execute(
          "INSERT INTO t1(time, device_id, value) VALUES (" + (base + 700_000L) + ", 'b', 10.0)");
      stmt.execute(
          "INSERT INTO t1(time, device_id, value) VALUES (" + (base + 1_200_000L) + ", 'b', 0.0)");

      // First outputs for both groups.
      AWAIT.untilAsserted(
          () -> {
            final List<Double> a = querySums(stmt, "sum_calc", "a");
            assertEquals(1, a.size());
            assertEquals(3.0, a.get(0), 1e-6);
            final List<Double> b = querySums(stmt, "sum_calc", "b");
            assertEquals(1, b.size());
            assertEquals(10.0, b.get(0), 1e-6);
          });

      // Distinguishable late rows for both closed windows: neither revises immediately.
      stmt.execute(
          "INSERT INTO t1(time, device_id, value) VALUES (" + (base + 350_000L) + ", 'a', 8.0)");
      stmt.execute(
          "INSERT INTO t1(time, device_id, value) VALUES (" + (base + 800_000L) + ", 'b', 20.0)");
      Thread.sleep(NEGATIVE_OBSERVE_MS);
      assertSingleSum(stmt, "sum_calc", "a", 3.0);
      assertSingleSum(stmt, "sum_calc", "b", 10.0);

      // Advance group a's event time to a's lateness line (base+10m+5m): only a may revise.
      stmt.execute(
          "INSERT INTO t1(time, device_id, value) VALUES (" + (base + 900_000L) + ", 'a', 0.0)");
      AWAIT.untilAsserted(
          () -> {
            final List<Double> a = querySums(stmt, "sum_calc", "a");
            assertEquals(1, a.size());
            assertEquals(11.0, a.get(0), 1e-6);
          });
      assertSingleSum(stmt, "sum_calc", "b", 10.0);

      // Advance group b's event time to b's own lateness line (base+20m+5m): now b revises.
      stmt.execute(
          "INSERT INTO t1(time, device_id, value) VALUES (" + (base + 1_500_000L) + ", 'b', 0.0)");
      AWAIT.untilAsserted(
          () -> {
            final List<Double> b = querySums(stmt, "sum_calc", "b");
            assertEquals(1, b.size());
            assertEquals(30.0, b.get(0), 1e-6);
          });
      assertSingleSum(stmt, "sum_calc", "a", 11.0);
    }
  }

  /**
   * OPT-09 allowed_lateness time units. Create five TUMBLE(10m) streams with allowed_lateness=1ms /
   * 1s / 1m / 1h / 1d. For each stream: close the window by event time (first output SUM=3), write
   * a late row (v=8, belongs to the closed W1), then an advance record at (window end + unit - 1ms)
   * — watermark still below the lateness line, no revision allowed — and observe. Then the at-line
   * record (window end + unit): the watermark reaches the lateness line, the window is destroyed
   * after a full recompute and the result must be revised to SUM=3+8=11.
   *
   * <p>Oracle: each stream's W1 = 1.0+2.0 (+ late 8.0 after revision = 11.0, hand-counted); the
   * revision moment must equal window end + configured duration per unit conversion.
   */
  @Test
  public void testOPT09AllowedLatenessTimeUnits() throws Exception {
    final String[] units = {"1ms", "1s", "1m", "1h", "1d"};
    final long[] unitMillis = {1L, 1_000L, 60_000L, 3_600_000L, 86_400_000L};
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      createDatabase(stmt, "opt09");
      final long base = System.currentTimeMillis() + 2000;

      // Create all five streams first: creation must succeed for every supported unit.
      for (int i = 0; i < units.length; i++) {
        final String suffix = units[i].substring(1);
        stmt.execute(
            "CREATE TABLE t_"
                + suffix
                + "(time TIMESTAMP TIME, device_id STRING TAG, value DOUBLE FIELD)");
        stmt.execute(
            "CREATE TABLE sum_" + suffix + "(time TIMESTAMP TIME, sum_value DOUBLE" + " FIELD)");
        stmt.execute(
            "CREATE STREAM opt09_"
                + suffix
                + " FROM t_"
                + suffix
                + " TUMBLE(10m, "
                + base
                + ") INTO sum_"
                + suffix
                + "(time, sum_value) "
                + "SELECT ${START_TIME}, sum(value) FROM ${ROWS} "
                + "WITH (allowed_lateness = "
                + units[i]
                + ")");
        stmt.execute("START STREAM opt09_" + suffix);
      }

      for (int i = 0; i < units.length; i++) {
        final String suffix = units[i].substring(1);
        final String table = "sum_" + suffix;
        final long unit = unitMillis[i];

        // Close the window with the standard data + progress record: first output SUM=3.
        stmt.execute(
            "INSERT INTO t_"
                + suffix
                + "(time, device_id, value) VALUES ("
                + base
                + ", 'd1',"
                + " 1.0)");
        stmt.execute(
            "INSERT INTO t_"
                + suffix
                + "(time, device_id, value) VALUES ("
                + (base + 300_000L)
                + ", 'd1', 2.0)");
        stmt.execute(
            "INSERT INTO t_"
                + suffix
                + "(time, device_id, value) VALUES ("
                + (base + 600_000L)
                + ", 'd1', 0.0)");
        AWAIT.untilAsserted(
            () -> {
              final List<Double> sums = queryWindowSumsNoPartition(stmt, table, base);
              assertEquals(1, sums.size());
              assertEquals(3.0, sums.get(0), 1e-6);
            });

        // Late row for the closed W1.
        stmt.execute(
            "INSERT INTO t_"
                + suffix
                + "(time, device_id, value) VALUES ("
                + (base + 350_000L)
                + ", 'd1', 8.0)");

        // (window end + unit - 1ms): watermark below the lateness line, no revision.
        // For unit=1ms the close record already pins max event time at the window end.
        if (unit - 1 > 0) {
          stmt.execute(
              "INSERT INTO t_"
                  + suffix
                  + "(time, device_id, value) VALUES ("
                  + (base + 600_000L + unit - 1)
                  + ", 'd1', 0.0)");
        }
        Thread.sleep(NEGATIVE_OBSERVE_MS);
        assertSingleWindowSumNoPartition(stmt, table, base, 3.0);

        // (window end + unit): watermark reaches the lateness line, revision to 11.
        stmt.execute(
            "INSERT INTO t_"
                + suffix
                + "(time, device_id, value) VALUES ("
                + (base + 600_000L + unit)
                + ", 'd1', 0.0)");
        final String target = table;
        AWAIT.untilAsserted(
            () -> {
              final List<Double> sums = queryWindowSumsNoPartition(stmt, target, base);
              assertEquals(1, sums.size());
              assertEquals(11.0, sums.get(0), 1e-6);
            });
      }
    }
  }

  /**
   * OPT-10 allowed_lateness value type and unit constraints (negative). ALLOWED_LATENESS accepts a
   * time duration (ms/s/m/h/d); a boolean, a string, or an unsupported unit (1mo) must make CREATE
   * STREAM fail with an explicit parameter error, and no stream task may be left behind in the
   * public stream metadata.
   *
   * <p>Oracle: the requirements option table types ALLOWED_LATENESS as "time length" with unit set
   * {ms,s,m,h,d}; anything else is rejected.
   */
  @Test
  public void testOPT10AllowedLatenessValueTypeConstraints() throws Exception {
    final String[] illegalValues = {"true", "'abc'", "1mo"};
    final String[] streamNames = {"opt10_bool", "opt10_str", "opt10_mo"};
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      createDatabase(stmt, "opt10");
      stmt.execute(
          "CREATE TABLE t1(time TIMESTAMP TIME, device_id STRING TAG, value DOUBLE FIELD)");
      stmt.execute("CREATE TABLE sum_calc(time TIMESTAMP TIME, sum_value DOUBLE FIELD)");

      final long base = System.currentTimeMillis() + 2000;
      for (int i = 0; i < illegalValues.length; i++) {
        final String value = illegalValues[i];
        final String streamName = streamNames[i];
        try {
          stmt.execute(
              "CREATE STREAM "
                  + streamName
                  + " FROM t1 TUMBLE(10m, "
                  + base
                  + ") INTO sum_calc(time, sum_value) "
                  + "SELECT ${START_TIME}, sum(value) FROM ${ROWS} "
                  + "WITH (allowed_lateness = "
                  + value
                  + ")");
          throw new AssertionError(
              "CREATE STREAM with allowed_lateness=" + value + " must be rejected");
        } catch (SQLException expected) {
          // rejected as required
        }
      }

      // Public stream metadata must not contain any of the rejected tasks.
      final List<String> registered = new ArrayList<>();
      try (ResultSet rs = stmt.executeQuery("show streams")) {
        while (rs.next()) {
          registered.add(rs.getString("stream_name"));
        }
      }
      for (final String streamName : streamNames) {
        assertFalse(
            "stream " + streamName + " must not exist in metadata",
            registered.contains(streamName));
      }
    }
  }

  /**
   * OPT-11 deferred late-data revision after window close (WATERMARK=2m + ALLOWED_LATENESS=5m).
   * Write normal data and close [10:00,10:10) by event time (max event 10:12 -> watermark 10:10):
   * record the first aggregation. Then write the late row 10:06(v=8) and query immediately — no
   * change allowed. Finally write 10:17(v=9): max event time 10:17 pushes the watermark to 10:15 =
   * lateness line (10:10 + 5m), the window is destroyed after a full recompute, and the target
   * window result must now include v=8 (SUM 3.0 -> 11.0). 10:17 itself belongs to W2 and only
   * advances the event time.
   *
   * <p>Oracle: W1 = 1.0+2.0, late 8.0 included only at revision (11.0, hand-counted); follows the
   * requirements W/A worked example.
   */
  @Test
  public void testOPT11DeferredLateDataRevision() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      createDatabase(stmt, "opt11");
      stmt.execute(
          "CREATE TABLE t1(time TIMESTAMP TIME, device_id STRING TAG, value DOUBLE FIELD)");
      stmt.execute("CREATE TABLE sum_calc(time TIMESTAMP TIME, sum_value DOUBLE FIELD)");

      final long base = System.currentTimeMillis() + 2000;

      stmt.execute(
          "CREATE STREAM opt11 "
              + "FROM t1 TUMBLE(10m, "
              + base
              + ") INTO sum_calc(time, sum_value) "
              + "SELECT ${START_TIME}, sum(value) FROM ${ROWS} "
              + "WITH (watermark = 2m, allowed_lateness = 5m)");
      stmt.execute("START STREAM opt11");

      // Normal data; max event 10:12 closes W1 (watermark 10:12-2m = 10:10).
      stmt.execute("INSERT INTO t1(time, device_id, value) VALUES (" + base + ", 'd1', 1.0)");
      stmt.execute(
          "INSERT INTO t1(time, device_id, value) VALUES (" + (base + 300_000L) + ", 'd1', 2.0)");
      stmt.execute(
          "INSERT INTO t1(time, device_id, value) VALUES (" + (base + 720_000L) + ", 'd1', 0.0)");
      AWAIT.untilAsserted(
          () -> {
            final List<Double> sums = querySums(stmt, "sum_calc");
            assertEquals(1, sums.size());
            assertEquals(3.0, sums.get(0), 1e-6);
          });

      // Late row for the closed window: no immediate revision.
      stmt.execute(
          "INSERT INTO t1(time, device_id, value) VALUES (" + (base + 360_000L) + ", 'd1', 8.0)");
      Thread.sleep(NEGATIVE_OBSERVE_MS);
      assertSingleSum(stmt, "sum_calc", 3.0);

      // 10:17 pushes the watermark to 10:15 = lateness line: destroy after full recompute.
      stmt.execute(
          "INSERT INTO t1(time, device_id, value) VALUES (" + (base + 1_020_000L) + ", 'd1', 9.0)");
      AWAIT.untilAsserted(
          () -> {
            final List<Double> sums = querySums(stmt, "sum_calc");
            assertEquals(1, sums.size());
            assertEquals(11.0, sums.get(0), 1e-6);
          });
    }
  }

  /**
   * OPT-12 final result at destroy time and late-data drop afterwards. Continues the OPT-11
   * sequence: after the lateness period ends (10:17 advance) the target window holds its FINAL
   * revised result (11.0). A further late point for the same window (10:07, v=16) must produce no
   * new output and no revision. Then a LATER window W2 [10:10,10:20) is completed (closed, revised
   * and destroyed by the 10:27 advance, final SUM=5.0), which pushes the group's expire line to
   * 10:20 — after that, data belonging to the earlier window W1 (10:08, v=7) must be dropped and
   * produce no result at all.
   *
   * <p>Oracle: W1 final = 1.0+2.0+8.0 = 11.0 (the post-destroy 16.0 and 7.0 excluded); W2 final =
   * 14.0 = 0.0(10:12 progress) + 9.0(10:17 advance) + 5.0(10:13 data); exactly 2 output rows at the
   * end, all hand-counted.
   */
  @Test
  public void testOPT12DestroyFinalResultAndLateDrop() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      createDatabase(stmt, "opt12");
      stmt.execute(
          "CREATE TABLE t1(time TIMESTAMP TIME, device_id STRING TAG, value DOUBLE FIELD)");
      stmt.execute("CREATE TABLE sum_calc(time TIMESTAMP TIME, sum_value DOUBLE FIELD)");

      final long base = System.currentTimeMillis() + 2000;

      stmt.execute(
          "CREATE STREAM opt12 "
              + "FROM t1 TUMBLE(10m, "
              + base
              + ") INTO sum_calc(time, sum_value) "
              + "SELECT ${START_TIME}, sum(value) FROM ${ROWS} "
              + "WITH (watermark = 2m, allowed_lateness = 5m)");
      stmt.execute("START STREAM opt12");

      // OPT-11 sequence: close W1, late 8.0, destroy at the lateness line, final 11.0.
      stmt.execute("INSERT INTO t1(time, device_id, value) VALUES (" + base + ", 'd1', 1.0)");
      stmt.execute(
          "INSERT INTO t1(time, device_id, value) VALUES (" + (base + 300_000L) + ", 'd1', 2.0)");
      stmt.execute(
          "INSERT INTO t1(time, device_id, value) VALUES (" + (base + 720_000L) + ", 'd1', 0.0)");
      AWAIT.untilAsserted(
          () -> {
            final List<Double> sums = querySums(stmt, "sum_calc");
            assertEquals(1, sums.size());
            assertEquals(3.0, sums.get(0), 1e-6);
          });
      stmt.execute(
          "INSERT INTO t1(time, device_id, value) VALUES (" + (base + 360_000L) + ", 'd1', 8.0)");
      stmt.execute(
          "INSERT INTO t1(time, device_id, value) VALUES (" + (base + 1_020_000L) + ", 'd1', 9.0)");
      AWAIT.untilAsserted(
          () -> {
            final List<Double> sums = querySums(stmt, "sum_calc");
            assertEquals(1, sums.size());
            assertEquals(11.0, sums.get(0), 1e-6);
          });

      // After destroy: another late point for W1 produces nothing.
      stmt.execute(
          "INSERT INTO t1(time, device_id, value) VALUES (" + (base + 420_000L) + ", 'd1', 16.0)");
      Thread.sleep(NEGATIVE_OBSERVE_MS);
      assertSingleSum(stmt, "sum_calc", 11.0);

      // Complete the later window W2 [base+10m, base+20m) and destroy it via the 10:27 advance.
      stmt.execute(
          "INSERT INTO t1(time, device_id, value) VALUES (" + (base + 780_000L) + ", 'd1', 5.0)");
      stmt.execute(
          "INSERT INTO t1(time, device_id, value) VALUES (" + (base + 1_620_000L) + ", 'd1', 0.0)");
      AWAIT.untilAsserted(
          () -> {
            final List<Double> sums = querySums(stmt, "sum_calc");
            assertEquals(2, sums.size());
            assertEquals(11.0, sums.get(0), 1e-6);
            assertEquals(14.0, sums.get(1), 1e-6);
          });

      // W2 destroyed -> expire line = 10:20: earlier-window data (10:08, v=7) must be dropped.
      stmt.execute(
          "INSERT INTO t1(time, device_id, value) VALUES (" + (base + 480_000L) + ", 'd1', 7.0)");
      Thread.sleep(NEGATIVE_OBSERVE_MS);
      final List<Double> sums = querySums(stmt, "sum_calc");
      assertEquals(2, sums.size());
      assertEquals(11.0, sums.get(0), 1e-6);
      assertEquals(14.0, sums.get(1), 1e-6);
    }
  }

  /**
   * OPT-13 max_delay_window_close default value. Create a TUMBLE(10m) stream WITHOUT any WITH
   * clause; write the first row to open the window and keep the event time below the close
   * condition. Observe for a bounded silence window (6s, outlasting the 5s spec minimum so an
   * erroneous 5s default close would fire inside it): with default MAX_DELAY_WINDOW_CLOSE=0 the
   * system time never closes the window (a default of 0 means the system time is not considered
   * after the window opens), so no output may appear. Then write a v=0 progress record reaching the
   * event-time close condition: only now the correct result appears.
   *
   * <p>Oracle: W1 = 1.0+2.0 (sum 3.0, hand-counted); the silent window past the 5s spec minimum is
   * the proof that no system-time trigger fires by default.
   */
  @Test
  public void testOPT13MaxDelayWindowCloseDefaultValue() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      createDatabase(stmt, "opt13");
      stmt.execute(
          "CREATE TABLE t1(time TIMESTAMP TIME, device_id STRING TAG, value DOUBLE FIELD)");
      stmt.execute("CREATE TABLE sum_calc(time TIMESTAMP TIME, sum_value DOUBLE FIELD)");

      final long base = System.currentTimeMillis() + 2000;

      stmt.execute(
          "CREATE STREAM opt13 "
              + "FROM t1 TUMBLE(10m, "
              + base
              + ") INTO sum_calc(time, sum_value) "
              + "SELECT ${START_TIME}, sum(value) FROM ${ROWS}");
      stmt.execute("START STREAM opt13");

      // Open the window; event time stays below the close condition.
      stmt.execute("INSERT INTO t1(time, device_id, value) VALUES (" + base + ", 'd1', 1.0)");
      stmt.execute(
          "INSERT INTO t1(time, device_id, value) VALUES (" + (base + 300_000L) + ", 'd1', 2.0)");

      // Default C=0: no system-time triggered output during the silence window (outlasts the 5s
      // spec minimum, so an erroneous 5s default close would be caught inside it).
      Thread.sleep(SYSTEM_SILENCE_MS);
      try (ResultSet rs = stmt.executeQuery("SELECT * FROM sum_calc")) {
        assertFalse("no system-time close expected with default max_delay_window_close", rs.next());
      }

      // Only the event-time close condition produces the correct result.
      stmt.execute(
          "INSERT INTO t1(time, device_id, value) VALUES (" + (base + 600_000L) + ", 'd1', 0.0)");
      AWAIT.untilAsserted(
          () -> {
            final List<Double> sums = querySums(stmt, "sum_calc");
            assertEquals(1, sums.size());
            assertEquals(3.0, sums.get(0), 1e-6);
          });
    }
  }

  /**
   * OPT-14 max_delay_window_close timer origin. Set max_delay_window_close=5s. Record t0 = the
   * moment the first window row is written; at t0+3s write another row (event time still below the
   * close condition) and keep polling. The window-close system time is initialized as window open
   * time + MAX_DELAY, so: no result before t0+5s; the correct result (SUM=7, rows 1.0+2.0+4.0) must
   * appear within scheduling tolerance after t0+5s and NOT as late as t0+8s (which would mean the
   * timer restarted on the second row).
   *
   * <p>Oracle: W1 = 1.0+2.0+4.0 = 7.0 (hand-counted); the appearance deadline t0+8s encodes the
   * requirement that the timer starts at window open, not at the latest row.
   */
  @Test
  public void testOPT14MaxDelayWindowCloseTimerOrigin() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      createDatabase(stmt, "opt14");
      stmt.execute(
          "CREATE TABLE t1(time TIMESTAMP TIME, device_id STRING TAG, value DOUBLE FIELD)");
      stmt.execute("CREATE TABLE sum_calc(time TIMESTAMP TIME, sum_value DOUBLE FIELD)");

      final long base = System.currentTimeMillis() + 2000;

      stmt.execute(
          "CREATE STREAM opt14 "
              + "FROM t1 TUMBLE(10m, "
              + base
              + ") INTO sum_calc(time, sum_value) "
              + "SELECT ${START_TIME}, sum(value) FROM ${ROWS} "
              + "WITH (max_delay_window_close = 5s)");
      stmt.execute("START STREAM opt14");

      final long t0 = System.currentTimeMillis();
      stmt.execute("INSERT INTO t1(time, device_id, value) VALUES (" + base + ", 'd1', 1.0)");
      stmt.execute(
          "INSERT INTO t1(time, device_id, value) VALUES (" + (base + 300_000L) + ", 'd1', 2.0)");

      // At t0+3s write the decoy row; its event time stays below the close condition.
      Thread.sleep(Math.max(0, t0 + 2_000L - System.currentTimeMillis()));
      stmt.execute(
          "INSERT INTO t1(time, device_id, value) VALUES (" + (base + 400_000L) + ", 'd1', 4.0)");

      // No result before t0+5s.
      Thread.sleep(Math.max(0, t0 + 4_900L - System.currentTimeMillis()));
      try (ResultSet rs = stmt.executeQuery("SELECT * FROM sum_calc")) {
        assertFalse("no result allowed before t0+5s", rs.next());
      }

      // The result must appear after t0+5s and no later than t0+8s (timer starts at window open).
      final long deadline = t0 + 8_000L;
      List<Double> sums = querySums(stmt, "sum_calc");
      while (sums.isEmpty() && System.currentTimeMillis() < deadline) {
        Thread.sleep(200L);
        sums = querySums(stmt, "sum_calc");
      }
      assertEquals(
          "window must close by system time t0+5s (not t0+8s from the second row)", 1, sums.size());
      assertEquals(7.0, sums.get(0), 1e-6);
    }
  }

  /**
   * OPT-15 max_delay_window_close executable time units. Create TUMBLE(10m) streams with
   * max_delay_window_close=5s and 1m (1h/1d need unreasonably long real waits and are excluded per
   * the classification). For the 5s stream: no premature output before t0+5s and the correct result
   * within 3s tolerance after it, measured from the window open point.
   *
   * <p>Runtime note: the 1m stream does NOT wait out the full 60s wall-clock close (it would
   * dominate the suite runtime). A bounded 6s silence check — past the 5s spec minimum, where a
   * unit misinterpretation would have fired — proves the unit is accepted and not converted to a
   * smaller magnitude, then the window is closed by event time and only the value is checked; the
   * exact close timing stays covered at the 5s scale.
   *
   * <p>Oracle: each stream's W1 = 1.0+2.0 (sum 3.0, hand-counted); the 5s stream must emit after
   * t0+5s (no upper wall-clock bound: the timer origin itself is covered by OPT-14), the 1m stream
   * never fires inside the silence window and emits via the event-time close.
   */
  @Test
  public void testOPT15MaxDelayWindowCloseTimeUnits() throws Exception {
    final String[] units = {"5s", "1m"};
    final long[] unitMillis = {5_000L, 60_000L};
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      createDatabase(stmt, "opt15");
      final long base = System.currentTimeMillis() + 2000;

      for (int i = 0; i < units.length; i++) {
        final String suffix = units[i];
        stmt.execute(
            "CREATE TABLE t_"
                + suffix
                + "(time TIMESTAMP TIME, device_id STRING TAG, value DOUBLE FIELD)");
        stmt.execute(
            "CREATE TABLE sum_" + suffix + "(time TIMESTAMP TIME, sum_value DOUBLE" + " FIELD)");
        stmt.execute(
            "CREATE STREAM opt15_"
                + suffix
                + " FROM t_"
                + suffix
                + " TUMBLE(10m, "
                + base
                + ") INTO sum_"
                + suffix
                + "(time, sum_value) "
                + "SELECT ${START_TIME}, sum(value) FROM ${ROWS} "
                + "WITH (max_delay_window_close = "
                + suffix
                + ")");
        stmt.execute("START STREAM opt15_" + suffix);
      }

      for (int i = 0; i < units.length; i++) {
        final String suffix = units[i];
        final String table = "sum_" + suffix;
        final long unit = unitMillis[i];

        final long t0 = System.currentTimeMillis();
        stmt.execute(
            "INSERT INTO t_"
                + suffix
                + "(time, device_id, value) VALUES ("
                + base
                + ", 'd1',"
                + " 1.0)");
        stmt.execute(
            "INSERT INTO t_"
                + suffix
                + "(time, device_id, value) VALUES ("
                + (base + 300_000L)
                + ", 'd1', 2.0)");

        if (unit > SYSTEM_SILENCE_MS) {
          // Executable-unit compression for 1m (and any unit above the silence window): waiting
          // out the full 60s wall-clock close would dominate the suite runtime. The bounded
          // silence check still proves the unit is accepted and NOT interpreted at or below the
          // 5s spec minimum (the check runs past t0+5s, where a misparse would have fired); the
          // exact close timing stays covered at the 5s scale by the other unit in this loop.
          // Close the window by event time instead and check the value.
          Thread.sleep(SYSTEM_SILENCE_MS);
          try (ResultSet rs = stmt.executeQuery("SELECT * FROM " + table)) {
            assertFalse("no premature output allowed for t_" + suffix, rs.next());
          }
          stmt.execute(
              "INSERT INTO t_"
                  + suffix
                  + "(time, device_id, value) VALUES ("
                  + (base + 600_000L)
                  + ", 'd1', 0.0)");
          final String target = table;
          AWAIT.untilAsserted(
              () -> {
                final List<Double> sums = querySums(stmt, target);
                assertEquals(1, sums.size());
                assertEquals(3.0, sums.get(0), 1e-6);
              });
        } else {
          // No premature output before t0 + unit.
          Thread.sleep(Math.max(0, t0 + unit - 500L - System.currentTimeMillis()));
          try (ResultSet rs = stmt.executeQuery("SELECT * FROM " + table)) {
            assertFalse("no output allowed before t0+" + suffix, rs.next());
          }

          // The correct result must appear after t0 + unit: the system-trigger bound covers
          // delivery/write-back latency on a heavily loaded runner, while a green case still
          // exits the poll as soon as the row appears.
          SYSTEM_TRIGGER_AWAIT.untilAsserted(
              () -> {
                final List<Double> sums = querySums(stmt, table);
                assertEquals("window must close by system time t0+" + suffix, 1, sums.size());
                assertEquals(3.0, sums.get(0), 1e-6);
              });
        }
      }
    }
  }

  /**
   * OPT-16 max_delay_window_close minimum value. Create one stream with max_delay_window_close=5s
   * and one with 4s. The 5s stream must be created and emit its result after the configured
   * duration; the 4s stream must be rejected (when set above 0 the minimum is 5 seconds) and leave
   * no task in the stream metadata.
   *
   * <p>Oracle: 4s < 5s minimum is illegal by the requirements option table; the 5s stream's W1 =
   * 1.0+2.0 (sum 3.0, hand-counted).
   */
  @Test
  public void testOPT16MaxDelayWindowCloseMinimum() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      createDatabase(stmt, "opt16");
      stmt.execute(
          "CREATE TABLE t1(time TIMESTAMP TIME, device_id STRING TAG, value DOUBLE FIELD)");
      stmt.execute("CREATE TABLE sum_calc(time TIMESTAMP TIME, sum_value DOUBLE FIELD)");

      final long base = System.currentTimeMillis() + 2000;

      // 5s stream: legal minimum, must be created.
      stmt.execute(
          "CREATE STREAM opt16_ok FROM t1 TUMBLE(10m, "
              + base
              + ") INTO sum_calc(time, sum_value) "
              + "SELECT ${START_TIME}, sum(value) FROM ${ROWS} "
              + "WITH (max_delay_window_close = 5s)");
      stmt.execute("START STREAM opt16_ok");

      // 4s stream: below the 5s minimum, must be rejected.
      try {
        stmt.execute(
            "CREATE STREAM opt16_bad FROM t1 TUMBLE(10m, "
                + base
                + ") INTO sum_calc(time, sum_value) "
                + "SELECT ${START_TIME}, sum(value) FROM ${ROWS} "
                + "WITH (max_delay_window_close = 4s)");
        throw new AssertionError("CREATE STREAM with max_delay_window_close=4s must be rejected");
      } catch (SQLException expected) {
        // rejected as required
      }
      final List<String> registered = new ArrayList<>();
      try (ResultSet rs = stmt.executeQuery("show streams")) {
        while (rs.next()) {
          registered.add(rs.getString("stream_name"));
        }
      }
      assertFalse("stream opt16_bad must not exist in metadata", registered.contains("opt16_bad"));

      // 5s stream emits after the configured duration from window open. The insert statements
      // are issued BEFORE the await (not timed): the first insert into a fresh database pays
      // ~3s of lazy storage-stack initialization (data region + WAL + subscription queue
      // binding) on the DataNode, while the MAX_DELAY_WINDOW_CLOSE clock only starts when the
      // data reaches the engine, so timing from the insert issue time would charge the await
      // budget for cold-start time the window logic never sees. The system-trigger bound (5s
      // trigger + delivery/write-back tolerance) covers a heavily loaded runner (e.g., CI
      // executing the full suite) delaying the DataNode -> StreamNode delivery by seconds; a
      // green case still exits the poll as soon as the row appears.
      stmt.execute("INSERT INTO t1(time, device_id, value) VALUES (" + base + ", 'd1', 1.0)");
      stmt.execute(
          "INSERT INTO t1(time, device_id, value) VALUES (" + (base + 300_000L) + ", 'd1', 2.0)");
      SYSTEM_TRIGGER_AWAIT.untilAsserted(
          () -> {
            final List<Double> sums = querySums(stmt, "sum_calc");
            assertEquals("window must close via the system-time trigger", 1, sums.size());
            assertEquals(3.0, sums.get(0), 1e-6);
          });
    }
  }

  /**
   * OPT-17 max_delay_window_close value type and unit constraints (negative). MAX_DELAY_
   * WINDOW_CLOSE accepts a time duration in s/m/h/d (ms is not a supported unit and values below 5s
   * violate the minimum); 1ms, true, 'abc' and 1mo must all make CREATE STREAM fail with an
   * explicit parameter error, and no stream task may be left behind.
   *
   * <p>Oracle: the requirements option table types the option as "time length" with unit set
   * {s,m,h,d} and a 5s minimum; anything else is rejected.
   */
  @Test
  public void testOPT17MaxDelayWindowCloseValueTypeConstraints() throws Exception {
    final String[] illegalValues = {"1ms", "true", "'abc'", "1mo"};
    final String[] streamNames = {"opt17_ms", "opt17_bool", "opt17_str", "opt17_mo"};
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      createDatabase(stmt, "opt17");
      stmt.execute(
          "CREATE TABLE t1(time TIMESTAMP TIME, device_id STRING TAG, value DOUBLE FIELD)");
      stmt.execute("CREATE TABLE sum_calc(time TIMESTAMP TIME, sum_value DOUBLE FIELD)");

      final long base = System.currentTimeMillis() + 2000;
      for (int i = 0; i < illegalValues.length; i++) {
        final String value = illegalValues[i];
        final String streamName = streamNames[i];
        try {
          stmt.execute(
              "CREATE STREAM "
                  + streamName
                  + " FROM t1 TUMBLE(10m, "
                  + base
                  + ") INTO sum_calc(time, sum_value) "
                  + "SELECT ${START_TIME}, sum(value) FROM ${ROWS} "
                  + "WITH (max_delay_window_close = "
                  + value
                  + ")");
          throw new AssertionError(
              "CREATE STREAM with max_delay_window_close=" + value + " must be rejected");
        } catch (SQLException expected) {
          // rejected as required
        }
      }

      final List<String> registered = new ArrayList<>();
      try (ResultSet rs = stmt.executeQuery("show streams")) {
        while (rs.next()) {
          registered.add(rs.getString("stream_name"));
        }
      }
      for (final String streamName : streamNames) {
        assertFalse(
            "stream " + streamName + " must not exist in metadata",
            registered.contains(streamName));
      }
    }
  }

  /**
   * OPT-18 max_delay_window_close with incremental computation. Set max_delay_window_close=5s and
   * send 10k rows in one Session tablet, above the 1000-row incremental trigger threshold. The
   * window engine must incorporate the entire tablet before a system-time tick can close it.
   * Consume with one poll thread so the subsequent event-time progress record cannot overtake the
   * bulk data. Before that record the target window must have no output; after it the result must
   * remain a single row with sum 10000.0.
   *
   * <p>Oracle: 10000 distinct timestamps with value 1.0 in the target window (sum 10000.0).
   */
  @Test
  public void testOPT18MaxDelayIgnoredDuringComputation() throws Exception {
    final int rowCount = 10_000;
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      createDatabase(stmt, "opt18");
      stmt.execute(
          "CREATE TABLE t1(time TIMESTAMP TIME, device_id STRING TAG, value DOUBLE FIELD)");
      stmt.execute("CREATE TABLE sum_calc(time TIMESTAMP TIME, sum_value DOUBLE FIELD)");

      final long base = System.currentTimeMillis() + 2000;

      stmt.execute(
          "CREATE STREAM opt18 FROM t1 TUMBLE(10m, "
              + base
              + ") INTO sum_calc(time, sum_value) "
              + "SELECT ${START_TIME}, sum(value) FROM ${ROWS} "
              + "WITH (max_delay_window_close = 5s, subscription_poll_thread_count = 1)");
      stmt.execute("START STREAM opt18");

      // A multi-row SQL INSERT becomes individual row tablets in the subscription converter.
      // Use the Session API to keep the bulk input in one tablet and avoid closing it partway.
      final Tablet tablet =
          new Tablet(
              "t1",
              List.of("device_id", "value"),
              List.of(TSDataType.STRING, TSDataType.DOUBLE),
              List.of(ColumnCategory.TAG, ColumnCategory.FIELD),
              rowCount);
      for (int i = 0; i < rowCount; i++) {
        tablet.addTimestamp(i, base + (long) i * 3);
        tablet.addValue("device_id", i, "d1");
        tablet.addValue("value", i, 1.0);
      }
      try (ITableSession session = env().getTableSessionConnectionWithDB("opt18")) {
        session.insert(tablet);
      }

      // Incremental computation must not publish a result while the window is still open.
      Thread.sleep(NEGATIVE_OBSERVE_MS);
      try (ResultSet rs = stmt.executeQuery("SELECT * FROM sum_calc")) {
        assertFalse("no result expected while/after the long computation", rs.next());
      }

      // Explicit event-time progress record closes the window: exactly one correct result. The
      // progress record also opens W2, which the same C=5s option force-closes ~5s later and
      // writes back a 0.0 row; only the TARGET window (time = window start) is counted so that
      // extra row cannot pollute the count (same narrowing as OPT-09/OPT-19). A premature
      // close is still caught by the value assertion and a re-execution by the row count.
      stmt.execute(
          "INSERT INTO t1(time, device_id, value) VALUES (" + (base + 600_000L) + ", 'd1', 0.0)");
      AWAIT.untilAsserted(
          () -> {
            final List<Double> sums = queryWindowSumsNoPartition(stmt, "sum_calc", base);
            assertEquals(1, sums.size());
            assertEquals((double) rowCount, sums.get(0), 1e-6);
          });

      // The complete result must stay stable after settling.
      Thread.sleep(NEGATIVE_OBSERVE_MS);
      assertSingleWindowSumNoPartition(stmt, "sum_calc", base, (double) rowCount);
    }
  }

  /**
   * OPT-19 max_idle_window_retain default value. Create a TUMBLE(10m) stream with
   * allowed_lateness=5m and max_delay_window_close=5s but WITHOUT max_idle_window_retain. Close the
   * window via MAX_DELAY_WINDOW_CLOSE (system time), write a late row, and observe for a bounded
   * silence window (6s, outlasting the 5s spec minimum so an erroneous 5s default fires inside it):
   * with default I=0 the system idle time never destroys the window (a default of 0 means the
   * system time is not considered after the window closes), so no revision may appear during the
   * observation. Only after the v=0 event-time progress record reaches the ALLOWED_LATENESS end
   * condition does the result include the late row (revision 3.0 -> 11.0).
   *
   * <p>Oracle: W1 = 1.0+2.0, late 8.0 included only at the event-time revision (11.0,
   * hand-counted).
   */
  @Test
  public void testOPT19MaxIdleWindowRetainDefaultValue() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      createDatabase(stmt, "opt19");
      stmt.execute(
          "CREATE TABLE t1(time TIMESTAMP TIME, device_id STRING TAG, value DOUBLE FIELD)");
      stmt.execute("CREATE TABLE sum_calc(time TIMESTAMP TIME, sum_value DOUBLE FIELD)");

      final long base = System.currentTimeMillis() + 2000;

      stmt.execute(
          "CREATE STREAM opt19 FROM t1 TUMBLE(10m, "
              + base
              + ") INTO sum_calc(time, sum_value) "
              + "SELECT ${START_TIME}, sum(value) FROM ${ROWS} "
              + "WITH (allowed_lateness = 5m, max_delay_window_close = 5s)");
      stmt.execute("START STREAM opt19");

      // Window data; the window must close via the system-time trigger (C=5s). The result only
      // appears after the wall-clock trigger fires plus delivery/write-back latency, so this
      // wait uses the system-trigger bound, not the event-time AWAIT.
      stmt.execute("INSERT INTO t1(time, device_id, value) VALUES (" + base + ", 'd1', 1.0)");
      stmt.execute(
          "INSERT INTO t1(time, device_id, value) VALUES (" + (base + 300_000L) + ", 'd1', 2.0)");
      SYSTEM_TRIGGER_AWAIT.untilAsserted(
          () -> {
            final List<Double> sums = querySums(stmt, "sum_calc");
            assertEquals(1, sums.size());
            assertEquals(3.0, sums.get(0), 1e-6);
          });

      // Late row for the closed window; default I=0: a bounded silence window (outlasting the 5s
      // spec minimum, so an erroneous 5s default fires inside it) must NOT revise.
      stmt.execute(
          "INSERT INTO t1(time, device_id, value) VALUES (" + (base + 350_000L) + ", 'd1', 8.0)");
      Thread.sleep(SYSTEM_SILENCE_MS);
      assertSingleSum(stmt, "sum_calc", 3.0);

      // Event time reaches the lateness end condition: revision to 11. The assertion is scoped to
      // the TARGET window (time = window start): the base+15m progress record opens W2, which the
      // C=5s force-close closes ~5s later and emits a 0.0 row — counting all rows would see 2 and
      // fail spuriously (same narrowing applied to OPT-09).
      stmt.execute(
          "INSERT INTO t1(time, device_id, value) VALUES (" + (base + 900_000L) + ", 'd1', 0.0)");
      AWAIT.untilAsserted(
          () -> {
            final List<Double> sums = queryWindowSumsNoPartition(stmt, "sum_calc", base);
            assertEquals(1, sums.size());
            assertEquals(11.0, sums.get(0), 1e-6);
          });
    }
  }

  /**
   * OPT-20 max_idle_window_retain forces window completion. Set allowed_lateness=5m,
   * max_delay_window_close=5s and max_idle_window_retain=5s. Close the window via the system time
   * trigger, then write one late row and keep polling: the result must NOT change immediately;
   * within a reasonable scheduling tolerance after 5s of idle time following the last late row, the
   * window is force-destroyed with a full recompute and the result must be revised to include the
   * late row (3.0 -> 11.0).
   *
   * <p>Oracle: W1 = 1.0+2.0, late 8.0 included at the idle-destroy revision (11.0, hand-counted);
   * both system-time waits use the system-trigger bound (trigger duration plus delivery and
   * write-back latency).
   */
  @Test
  public void testOPT20MaxIdleWindowRetainForcesCompletion() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      createDatabase(stmt, "opt20");
      stmt.execute(
          "CREATE TABLE t1(time TIMESTAMP TIME, device_id STRING TAG, value DOUBLE FIELD)");
      stmt.execute("CREATE TABLE sum_calc(time TIMESTAMP TIME, sum_value DOUBLE FIELD)");

      final long base = System.currentTimeMillis() + 2000;

      stmt.execute(
          "CREATE STREAM opt20 FROM t1 TUMBLE(10m, "
              + base
              + ") INTO sum_calc(time, sum_value) "
              + "SELECT ${START_TIME}, sum(value) FROM ${ROWS} "
              + "WITH (allowed_lateness = 5m, max_delay_window_close = 5s, max_idle_window_retain"
              + " = 5s)");
      stmt.execute("START STREAM opt20");

      // Window data; the window must close via the system-time trigger (C=5s): the result only
      // appears after the wall-clock trigger fires plus delivery/write-back latency, so this
      // wait uses the system-trigger bound, not the event-time AWAIT.
      stmt.execute("INSERT INTO t1(time, device_id, value) VALUES (" + base + ", 'd1', 1.0)");
      stmt.execute(
          "INSERT INTO t1(time, device_id, value) VALUES (" + (base + 300_000L) + ", 'd1', 2.0)");
      SYSTEM_TRIGGER_AWAIT.untilAsserted(
          () -> {
            final List<Double> sums = querySums(stmt, "sum_calc");
            assertEquals(1, sums.size());
            assertEquals(3.0, sums.get(0), 1e-6);
          });

      // One late row: no immediate revision.
      stmt.execute(
          "INSERT INTO t1(time, device_id, value) VALUES (" + (base + 350_000L) + ", 'd1', 8.0)");
      Thread.sleep(200L);
      assertSingleSum(stmt, "sum_calc", 3.0);

      // 5s idle after the late row: the forced destroy revises to 11 (system-trigger bound).
      SYSTEM_TRIGGER_AWAIT.untilAsserted(
          () -> {
            final List<Double> sums = querySums(stmt, "sum_calc");
            assertEquals(1, sums.size());
            assertEquals(11.0, sums.get(0), 1e-6);
          });
    }
  }

  /**
   * OPT-21 consecutive late rows reset the idle wait. Set allowed_lateness=5m (so the event-time
   * lateness line is far away and any revision can only come from the idle timer) and
   * max_idle_window_retain=5s. Close the window by event time (t0 = close moment), then write two
   * late rows at t0+1s and t0+3s. Query at the original deadline (t0+5s) and at the first row's
   * deadline (t0+6s): no revision allowed. Only 5s after the LAST late row (within the
   * system-trigger bound) does the window get force-destroyed and the result revised once to
   * include both late rows (3.0 -> 13.0), because each late row resets the
   * window-destroy-system-time to its receive time + MAX_IDLE_WINDOW_RETAIN.
   *
   * <p>Oracle: W1 = 1.0+2.0, late 8.0 and 2.0 both included at the one-shot revision (13.0,
   * hand-counted).
   */
  @Test
  public void testOPT21LateRowsResetIdleWait() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      createDatabase(stmt, "opt21");
      stmt.execute(
          "CREATE TABLE t1(time TIMESTAMP TIME, device_id STRING TAG, value DOUBLE FIELD)");
      stmt.execute("CREATE TABLE sum_calc(time TIMESTAMP TIME, sum_value DOUBLE FIELD)");

      final long base = System.currentTimeMillis() + 2000;

      stmt.execute(
          "CREATE STREAM opt21 FROM t1 TUMBLE(10m, "
              + base
              + ") INTO sum_calc(time, sum_value) "
              + "SELECT ${START_TIME}, sum(value) FROM ${ROWS} "
              + "WITH (allowed_lateness = 5m, max_idle_window_retain = 5s)");
      stmt.execute("START STREAM opt21");

      // Close the window by event time; t0 is the close moment.
      stmt.execute("INSERT INTO t1(time, device_id, value) VALUES (" + base + ", 'd1', 1.0)");
      stmt.execute(
          "INSERT INTO t1(time, device_id, value) VALUES (" + (base + 300_000L) + ", 'd1', 2.0)");
      stmt.execute(
          "INSERT INTO t1(time, device_id, value) VALUES (" + (base + 600_000L) + ", 'd1', 0.0)");
      AWAIT.untilAsserted(
          () -> {
            final List<Double> sums = querySums(stmt, "sum_calc");
            assertEquals(1, sums.size());
            assertEquals(3.0, sums.get(0), 1e-6);
          });

      final long t0 = System.currentTimeMillis();

      // First late row at t0+1s, second at t0+3s (event times inside the closed W1).
      Thread.sleep(Math.max(0, t0 + 500L - System.currentTimeMillis()));
      stmt.execute(
          "INSERT INTO t1(time, device_id, value) VALUES (" + (base + 350_000L) + ", 'd1', 8.0)");
      Thread.sleep(Math.max(0, t0 + 1_500L - System.currentTimeMillis()));
      stmt.execute(
          "INSERT INTO t1(time, device_id, value) VALUES (" + (base + 400_000L) + ", 'd1', 2.0)");

      // Original deadline (t0+5s) and first-row deadline (t0+6s): no revision.
      Thread.sleep(Math.max(0, t0 + 3_000L - System.currentTimeMillis()));
      assertSingleSum(stmt, "sum_calc", 3.0);

      // 5s after the LAST late row (t0+1.5s in the compressed skeleton): the one-shot
      // revision to 13 must arrive within the system-trigger bound (idle duration plus
      // delivery/write-back latency on a loaded runner).
      SYSTEM_TRIGGER_AWAIT.untilAsserted(
          () -> {
            final List<Double> sums = querySums(stmt, "sum_calc");
            assertEquals(1, sums.size());
            assertEquals(13.0, sums.get(0), 1e-6);
          });
    }
  }

  /**
   * OPT-22 max_idle_window_retain executable time units. Create TUMBLE(10m) streams with
   * max_idle_window_retain=5s and 1m (1h/1d need unreasonably long real waits and are excluded per
   * the classification), both with allowed_lateness=5m so any revision can only come from the idle
   * timer. For the 5s stream: close the window by event time (first output SUM=3), write a late
   * row, observe no revision before the configured duration, then the one-shot revision to 11.0
   * within 3s tolerance after the duration elapses from the late row.
   *
   * <p>Runtime note (same as OPT-15): the 1m stream does NOT wait out the full 60s idle duration; a
   * bounded 6s silence check proves the unit is accepted and not converted to a smaller magnitude,
   * then the window is destroyed by the event-time lateness line and the revision value is checked;
   * the exact idle timing stays covered at the 5s scale.
   *
   * <p>Oracle: each stream's W1 = 1.0+2.0 (+ late 8.0 at revision = 11.0, hand-counted).
   */
  @Test
  public void testOPT22MaxIdleWindowRetainTimeUnits() throws Exception {
    final String[] units = {"5s", "1m"};
    final long[] unitMillis = {5_000L, 60_000L};
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      createDatabase(stmt, "opt22");
      final long base = System.currentTimeMillis() + 2000;

      for (int i = 0; i < units.length; i++) {
        final String suffix = units[i];
        stmt.execute(
            "CREATE TABLE t_"
                + suffix
                + "(time TIMESTAMP TIME, device_id STRING TAG, value DOUBLE FIELD)");
        stmt.execute(
            "CREATE TABLE sum_" + suffix + "(time TIMESTAMP TIME, sum_value DOUBLE" + " FIELD)");
        stmt.execute(
            "CREATE STREAM opt22_"
                + suffix
                + " FROM t_"
                + suffix
                + " TUMBLE(10m, "
                + base
                + ") INTO sum_"
                + suffix
                + "(time, sum_value) "
                + "SELECT ${START_TIME}, sum(value) FROM ${ROWS} "
                + "WITH (allowed_lateness = 5m, max_idle_window_retain = "
                + suffix
                + ")");
        stmt.execute("START STREAM opt22_" + suffix);
      }

      for (int i = 0; i < units.length; i++) {
        final String suffix = units[i];
        final String table = "sum_" + suffix;
        final long unit = unitMillis[i];

        // Close the window by event time: first output SUM=3.
        stmt.execute(
            "INSERT INTO t_"
                + suffix
                + "(time, device_id, value) VALUES ("
                + base
                + ", 'd1',"
                + " 1.0)");
        stmt.execute(
            "INSERT INTO t_"
                + suffix
                + "(time, device_id, value) VALUES ("
                + (base + 300_000L)
                + ", 'd1', 2.0)");
        stmt.execute(
            "INSERT INTO t_"
                + suffix
                + "(time, device_id, value) VALUES ("
                + (base + 600_000L)
                + ", 'd1', 0.0)");
        AWAIT.untilAsserted(
            () -> {
              final List<Double> sums = querySums(stmt, table);
              assertEquals(1, sums.size());
              assertEquals(3.0, sums.get(0), 1e-6);
            });

        // Late row; no revision before the configured idle duration.
        final long lateWriteTime = System.currentTimeMillis();
        stmt.execute(
            "INSERT INTO t_"
                + suffix
                + "(time, device_id, value) VALUES ("
                + (base + 350_000L)
                + ", 'd1', 8.0)");
        if (unit > SYSTEM_SILENCE_MS) {
          // Executable-unit compression for 1m (see OPT-15): the bounded silence check proves the
          // idle unit is accepted and NOT interpreted at or below the 5s spec minimum, without
          // waiting out the full 60s; the exact idle timing stays covered at the 5s scale by the
          // other unit in this loop. Destroy by the event-time lateness line instead.
          Thread.sleep(SYSTEM_SILENCE_MS);
          assertSingleSum(stmt, table, 3.0);
          // 10:15(v=0) pushes the watermark onto the lateness line (10:10 + 5m): destroy after a
          // full recompute, revision to 11.
          stmt.execute(
              "INSERT INTO t_"
                  + suffix
                  + "(time, device_id, value) VALUES ("
                  + (base + 900_000L)
                  + ", 'd1', 0.0)");
          final String target = table;
          AWAIT.untilAsserted(
              () -> {
                final List<Double> sums = querySums(stmt, target);
                assertEquals(1, sums.size());
                assertEquals(11.0, sums.get(0), 1e-6);
              });
        } else {
          // No revision before the configured idle duration.
          Thread.sleep(Math.max(0, lateWriteTime + unit - 500L - System.currentTimeMillis()));
          assertSingleSum(stmt, table, 3.0);

          // The one-shot revision to 11 within the system-trigger bound after the idle
          // duration (idle trigger plus delivery/write-back latency on a loaded runner).
          SYSTEM_TRIGGER_AWAIT.untilAsserted(
              () -> {
                final List<Double> sums = querySums(stmt, table);
                assertEquals(1, sums.size());
                assertEquals(11.0, sums.get(0), 1e-6);
              });
        }
      }
    }
  }

  /**
   * OPT-23 max_idle_window_retain minimum value. Create one stream with max_idle_window_retain=5s
   * and one with 4s. The 5s stream must be created (and revise on time); the 4s stream must be
   * rejected (when set above 0 the minimum is 5 seconds) and leave no task in the stream metadata.
   *
   * <p>Oracle: 4s < 5s minimum is illegal by the requirements option table; the 5s stream's W1 =
   * 1.0+2.0 (+ late 8.0 at the idle revision = 11.0, hand-counted).
   */
  @Test
  public void testOPT23MaxIdleWindowRetainMinimum() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      createDatabase(stmt, "opt23");
      stmt.execute(
          "CREATE TABLE t1(time TIMESTAMP TIME, device_id STRING TAG, value DOUBLE FIELD)");
      stmt.execute("CREATE TABLE sum_calc(time TIMESTAMP TIME, sum_value DOUBLE FIELD)");

      final long base = System.currentTimeMillis() + 2000;

      // 5s stream: legal minimum, must be created.
      stmt.execute(
          "CREATE STREAM opt23_ok FROM t1 TUMBLE(10m, "
              + base
              + ") INTO sum_calc(time, sum_value) "
              + "SELECT ${START_TIME}, sum(value) FROM ${ROWS} "
              + "WITH (allowed_lateness = 5m, max_idle_window_retain = 5s)");
      stmt.execute("START STREAM opt23_ok");

      // 4s stream: below the 5s minimum, must be rejected.
      try {
        stmt.execute(
            "CREATE STREAM opt23_bad FROM t1 TUMBLE(10m, "
                + base
                + ") INTO sum_calc(time, sum_value) "
                + "SELECT ${START_TIME}, sum(value) FROM ${ROWS} "
                + "WITH (allowed_lateness = 5m, max_idle_window_retain = 4s)");
        throw new AssertionError("CREATE STREAM with max_idle_window_retain=4s must be rejected");
      } catch (SQLException expected) {
        // rejected as required
      }
      final List<String> registered = new ArrayList<>();
      try (ResultSet rs = stmt.executeQuery("show streams")) {
        while (rs.next()) {
          registered.add(rs.getString("stream_name"));
        }
      }
      assertFalse("stream opt23_bad must not exist in metadata", registered.contains("opt23_bad"));

      // 5s stream revises on time: close by event time, late row, idle revision to 11.
      stmt.execute("INSERT INTO t1(time, device_id, value) VALUES (" + base + ", 'd1', 1.0)");
      stmt.execute(
          "INSERT INTO t1(time, device_id, value) VALUES (" + (base + 300_000L) + ", 'd1', 2.0)");
      stmt.execute(
          "INSERT INTO t1(time, device_id, value) VALUES (" + (base + 600_000L) + ", 'd1', 0.0)");
      AWAIT.untilAsserted(
          () -> {
            final List<Double> sums = querySums(stmt, "sum_calc");
            assertEquals(1, sums.size());
            assertEquals(3.0, sums.get(0), 1e-6);
          });
      // 5s idle after the late row: the forced destroy revises to 11 (system-trigger bound).
      stmt.execute(
          "INSERT INTO t1(time, device_id, value) VALUES (" + (base + 350_000L) + ", 'd1', 8.0)");
      SYSTEM_TRIGGER_AWAIT.untilAsserted(
          () -> {
            final List<Double> sums = querySums(stmt, "sum_calc");
            assertEquals(1, sums.size());
            assertEquals(11.0, sums.get(0), 1e-6);
          });
    }
  }

  /**
   * OPT-24 max_idle_window_retain value type and unit constraints (negative). MAX_IDLE_WINDOW_
   * RETAIN accepts a time duration in s/m/h/d (ms is not a supported unit and values below 5s
   * violate the minimum); 1ms, true, 'abc' and 1mo must all make CREATE STREAM fail with an
   * explicit parameter error, and no stream task may be left behind.
   *
   * <p>Oracle: the requirements option table types the option as "time length" with unit set
   * {s,m,h,d} and a 5s minimum; anything else is rejected.
   */
  @Test
  public void testOPT24MaxIdleWindowRetainValueTypeConstraints() throws Exception {
    final String[] illegalValues = {"1ms", "true", "'abc'", "1mo"};
    final String[] streamNames = {"opt24_ms", "opt24_bool", "opt24_str", "opt24_mo"};
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      createDatabase(stmt, "opt24");
      stmt.execute(
          "CREATE TABLE t1(time TIMESTAMP TIME, device_id STRING TAG, value DOUBLE FIELD)");
      stmt.execute("CREATE TABLE sum_calc(time TIMESTAMP TIME, sum_value DOUBLE FIELD)");

      final long base = System.currentTimeMillis() + 2000;
      for (int i = 0; i < illegalValues.length; i++) {
        final String value = illegalValues[i];
        final String streamName = streamNames[i];
        try {
          stmt.execute(
              "CREATE STREAM "
                  + streamName
                  + " FROM t1 TUMBLE(10m, "
                  + base
                  + ") INTO sum_calc(time, sum_value) "
                  + "SELECT ${START_TIME}, sum(value) FROM ${ROWS} "
                  + "WITH (allowed_lateness = 5m, max_idle_window_retain = "
                  + value
                  + ")");
          throw new AssertionError(
              "CREATE STREAM with max_idle_window_retain=" + value + " must be rejected");
        } catch (SQLException expected) {
          // rejected as required
        }
      }

      final List<String> registered = new ArrayList<>();
      try (ResultSet rs = stmt.executeQuery("show streams")) {
        while (rs.next()) {
          registered.add(rs.getString("stream_name"));
        }
      }
      for (final String streamName : streamNames) {
        assertFalse(
            "stream " + streamName + " must not exist in metadata",
            registered.contains(streamName));
      }
    }
  }

  /**
   * OPT-25 max_idle_window_retain trigger ignored during a window revision. Set allowed_lateness=5m
   * and max_idle_window_retain=5s. Close the window by event time, then write a bulk of late rows
   * (10k rows, above the 1000-row incremental threshold) so that the window revision computation
   * runs for a long stretch overlapping the idle trigger point: a MAX_IDLE trigger arriving while a
   * computation is running is ignored. The window must produce exactly ONE valid revision (SUM = 3
   * + 10000 = 10003.0), no duplicate results during the computation, and no extra re-execution
   * afterwards.
   *
   * <p>Construction note (classification deletion clause): a public slow UDF is not available here,
   * so the "slow computation" is built from data volume (same approach as OPT-18). Oracle: W1 =
   * 1.0+2.0 plus 10000 late rows of 1.0 (sum 10003.0, hand-counted by construction).
   */
  @Test
  public void testOPT25MaxIdleIgnoredDuringRevision() throws Exception {
    final int lateRowCount = 10_000;
    final int batchSize = 1_000;
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      createDatabase(stmt, "opt25");
      stmt.execute(
          "CREATE TABLE t1(time TIMESTAMP TIME, device_id STRING TAG, value DOUBLE FIELD)");
      stmt.execute("CREATE TABLE sum_calc(time TIMESTAMP TIME, sum_value DOUBLE FIELD)");

      final long base = System.currentTimeMillis() + 2000;

      stmt.execute(
          "CREATE STREAM opt25 FROM t1 TUMBLE(10m, "
              + base
              + ") INTO sum_calc(time, sum_value) "
              + "SELECT ${START_TIME}, sum(value) FROM ${ROWS} "
              + "WITH (allowed_lateness = 5m, max_idle_window_retain = 5s)");
      stmt.execute("START STREAM opt25");

      // Close the window by event time: first output SUM=3.
      stmt.execute("INSERT INTO t1(time, device_id, value) VALUES (" + base + ", 'd1', 1.0)");
      stmt.execute(
          "INSERT INTO t1(time, device_id, value) VALUES (" + (base + 300_000L) + ", 'd1', 2.0)");
      stmt.execute(
          "INSERT INTO t1(time, device_id, value) VALUES (" + (base + 600_000L) + ", 'd1', 0.0)");
      AWAIT.untilAsserted(
          () -> {
            final List<Double> sums = querySums(stmt, "sum_calc");
            assertEquals(1, sums.size());
            assertEquals(3.0, sums.get(0), 1e-6);
          });

      // Bulk of late rows inside the closed W1: event times in [base+100s, base+300s).
      final long lateWriteTime = System.currentTimeMillis();
      for (int batchStart = 0; batchStart < lateRowCount; batchStart += batchSize) {
        final StringBuilder sql =
            new StringBuilder("INSERT INTO t1(time, device_id, value) VALUES ");
        for (int i = batchStart; i < batchStart + batchSize; i++) {
          if (i > batchStart) {
            sql.append(", ");
          }
          sql.append("(").append(base + 100_000L + (long) i * 20).append(", 'd1', 1.0)");
        }
        stmt.execute(sql.toString());
      }

      // The revision (idle destroy + full recompute) must happen exactly once; deadline = last
      // late write + 5s idle + 30s compute tolerance.
      final long deadline = lateWriteTime + 5_000L + 30_000L;
      List<Double> sums = querySums(stmt, "sum_calc");
      while ((sums.size() != 1 || Math.abs(sums.get(0) - 10003.0) > 1e-6)
          && System.currentTimeMillis() < deadline) {
        Thread.sleep(200L);
        sums = querySums(stmt, "sum_calc");
      }
      assertEquals(1, sums.size());
      assertEquals(10003.0, sums.get(0), 1e-6);

      // No extra re-execution or duplicates afterwards.
      Thread.sleep(NEGATIVE_OBSERVE_MS);
      assertSingleSum(stmt, "sum_calc", 10003.0);
    }
  }

  // --------------------------------------------------------------------------------------------
  // OPT-26 ~ OPT-35: LEFT_CLOSED semantics, the first system-time full-flow case, and the
  // opening cells of the five-option combination matrix. In the combination cases the E group
  // drives the event-time line and the S group the system-time line (PARTITION BY device_id,
  // per-partition watermark).
  // --------------------------------------------------------------------------------------------

  /** One aggregate output row: (window start, sum value). Used for boundary attribution. */
  private record SumRow(long windowStart, double sumValue) {}

  /** Reads (window start, sum) pairs from the output table, ordered by window start. */
  private static List<SumRow> querySumRows(final Statement statement, final String table)
      throws SQLException {
    final List<SumRow> rows = new ArrayList<>();
    try (ResultSet rs =
        statement.executeQuery("SELECT time, sum_value FROM " + table + " ORDER BY time")) {
      while (rs.next()) {
        rows.add(new SumRow(rs.getLong(1), rs.getDouble(2)));
      }
    }
    return rows;
  }

  /** Asserts the output table holds exactly the given (window start, sum) rows, in order. */
  private static void assertWindowRows(
      final Statement statement, final String table, final long[] starts, final double[] sums)
      throws SQLException {
    final List<SumRow> rows = querySumRows(statement, table);
    assertEquals(starts.length, rows.size());
    for (int i = 0; i < starts.length; i++) {
      assertEquals("window start of row " + i, starts[i], rows.get(i).windowStart());
      assertEquals("sum of row " + i, sums[i], rows.get(i).sumValue(), 1e-6);
    }
  }

  /**
   * OPT-26 LEFT_CLOSED default boundary attribution. Create a TUMBLE(10m) stream WITHOUT any WITH
   * clause, write the boundary rows 10:00(v=1), 10:10(v=2), 10:20(v=4) and then the v=0 progress
   * record 10:30. Per the default LEFT_CLOSED=true (left-closed-right-open) each boundary row
   * belongs to the window STARTING at its own timestamp: [10:00,10:10)=1.0, [10:10,10:20)=2.0,
   * [10:20,10:30)=4.0 — every boundary record appears exactly once, neither duplicated nor dropped.
   * The assertion checks the window START TIMES as well as the sums: a left-open-right -closed
   * mis-default yields the same three sums shifted one window earlier, which only the window-start
   * check can catch.
   *
   * <p>Known-defect coverage (B-? candidate, comparison table): with the whole WITH clause absent
   * the task takes the no-arg StreamProperties constructor whose Java default leftClosed=false,
   * which attributes the three sums to one window earlier.
   *
   * <p>Oracle: rows (base,1.0), (base+10m,2.0), (base+20m,4.0), hand-counted; the 10:30 progress
   * record belongs to the fourth window which never closes.
   */
  @Test
  public void testOPT26LeftClosedDefaultBoundary() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      createDatabase(stmt, "opt26");
      stmt.execute(
          "CREATE TABLE t1(time TIMESTAMP TIME, device_id STRING TAG, value DOUBLE FIELD)");
      stmt.execute("CREATE TABLE sum_calc(time TIMESTAMP TIME, sum_value DOUBLE FIELD)");

      final long base = System.currentTimeMillis() + 2000;

      stmt.execute(
          "CREATE STREAM opt26 FROM t1 TUMBLE(10m, "
              + base
              + ") INTO sum_calc(time, sum_value) "
              + "SELECT ${START_TIME}, sum(value) FROM ${ROWS}");
      stmt.execute("START STREAM opt26");

      // Three boundary rows, one per window boundary; then the v=0 progress record 10:30.
      stmt.execute("INSERT INTO t1(time, device_id, value) VALUES (" + base + ", 'd1', 1.0)");
      stmt.execute(
          "INSERT INTO t1(time, device_id, value) VALUES (" + (base + 600_000L) + ", 'd1', 2.0)");
      stmt.execute(
          "INSERT INTO t1(time, device_id, value) VALUES (" + (base + 1_200_000L) + ", 'd1', 4.0)");
      stmt.execute(
          "INSERT INTO t1(time, device_id, value) VALUES (" + (base + 1_800_000L) + ", 'd1', 0.0)");

      final long[] starts = {base, base + 600_000L, base + 1_200_000L};
      final double[] sums = {1.0, 2.0, 4.0};
      AWAIT.untilAsserted(() -> assertWindowRows(stmt, "sum_calc", starts, sums));

      // Each boundary record appears exactly once: no duplicates or late additions.
      Thread.sleep(NEGATIVE_OBSERVE_MS);
      assertWindowRows(stmt, "sum_calc", starts, sums);
    }
  }

  /**
   * OPT-27 LEFT_CLOSED=true is equivalent to the default. Create two otherwise-identical streams
   * over the same source table — one omits left_closed, the other sets left_closed=true — both with
   * watermark=2m and allowed_lateness=5m. Replay identical boundary / normal / late / progress data
   * into both: 10:00(v=1) boundary, 10:05(v=2) normal, 10:10(v=0) boundary row (belongs to W2 under
   * left-closed semantics), 10:12(v=0) progress closes W1 with SUM=3, late 10:06(v=8), 10:17(v=9)
   * progress pushes the watermark to the lateness line and the destroy-recompute revises W1 to 11.
   * Both streams must show identical target windows, values, revision counts (exactly one row each)
   * and timing (the same await windows).
   *
   * <p>Oracle: W1 = 1.0+2.0 first, revised to 11.0 with the late 8.0 (hand-counted; mirrors the
   * OPT-11 worked example).
   */
  @Test
  public void testOPT27LeftClosedExplicitTrueEqualsDefault() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      createDatabase(stmt, "opt27");
      stmt.execute(
          "CREATE TABLE t1(time TIMESTAMP TIME, device_id STRING TAG, value DOUBLE FIELD)");
      stmt.execute("CREATE TABLE sum_a(time TIMESTAMP TIME, sum_value DOUBLE FIELD)");
      stmt.execute("CREATE TABLE sum_b(time TIMESTAMP TIME, sum_value DOUBLE FIELD)");

      final long base = System.currentTimeMillis() + 2000;

      stmt.execute(
          "CREATE STREAM opt27a FROM t1 TUMBLE(10m, "
              + base
              + ") INTO sum_a(time, sum_value) "
              + "SELECT ${START_TIME}, sum(value) FROM ${ROWS} "
              + "WITH (watermark = 2m, allowed_lateness = 5m)");
      stmt.execute(
          "CREATE STREAM opt27b FROM t1 TUMBLE(10m, "
              + base
              + ") INTO sum_b(time, sum_value) "
              + "SELECT ${START_TIME}, sum(value) FROM ${ROWS} "
              + "WITH (watermark = 2m, allowed_lateness = 5m, left_closed = true)");
      stmt.execute("START STREAM opt27a");
      stmt.execute("START STREAM opt27b");

      // Boundary, normal, boundary and the closing progress record (watermark 10:10 >= end).
      stmt.execute("INSERT INTO t1(time, device_id, value) VALUES (" + base + ", 'd1', 1.0)");
      stmt.execute(
          "INSERT INTO t1(time, device_id, value) VALUES (" + (base + 300_000L) + ", 'd1', 2.0)");
      stmt.execute(
          "INSERT INTO t1(time, device_id, value) VALUES (" + (base + 600_000L) + ", 'd1', 0.0)");
      stmt.execute(
          "INSERT INTO t1(time, device_id, value) VALUES (" + (base + 720_000L) + ", 'd1', 0.0)");
      AWAIT.untilAsserted(
          () -> {
            assertSingleSum(stmt, "sum_a", 3.0);
            assertSingleSum(stmt, "sum_b", 3.0);
          });

      // Late row: neither stream may revise immediately.
      stmt.execute(
          "INSERT INTO t1(time, device_id, value) VALUES (" + (base + 360_000L) + ", 'd1', 8.0)");
      Thread.sleep(NEGATIVE_OBSERVE_MS);
      assertSingleSum(stmt, "sum_a", 3.0);
      assertSingleSum(stmt, "sum_b", 3.0);

      // 10:17 pushes the watermark to the lateness line: both streams revise identically.
      stmt.execute(
          "INSERT INTO t1(time, device_id, value) VALUES (" + (base + 1_020_000L) + ", 'd1', 9.0)");
      AWAIT.untilAsserted(
          () -> {
            assertSingleSum(stmt, "sum_a", 11.0);
            assertSingleSum(stmt, "sum_b", 11.0);
          });
    }
  }

  /**
   * OPT-28 LEFT_CLOSED=false boundary attribution. Create a TUMBLE(10m) stream WITH
   * left_closed=false and write the same boundary rows 10:00(v=1), 10:10(v=2), 10:20(v=4) plus the
   * v=0 progress record 10:30. Under left-open-right-closed each boundary row belongs to the window
   * ENDING at its own timestamp: 10:00 belongs to the PREVIOUS window (...,10:00] which holds v=1,
   * (10:00,10:10] holds v=2, (10:10,10:20] holds v=4; every boundary record appears exactly once.
   *
   * <p>Oracle: rows (base-10m,1.0), (base,2.0), (base+10m,4.0), hand-counted. Closing requires a
   * watermark strictly greater than the window end, which the write order exercises implicitly: the
   * (...,10:00] row is closed by the 10:10 write, not by the 10:00 write.
   */
  @Test
  public void testOPT28LeftClosedFalseBoundary() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      createDatabase(stmt, "opt28");
      stmt.execute(
          "CREATE TABLE t1(time TIMESTAMP TIME, device_id STRING TAG, value DOUBLE FIELD)");
      stmt.execute("CREATE TABLE sum_calc(time TIMESTAMP TIME, sum_value DOUBLE FIELD)");

      final long base = System.currentTimeMillis() + 2000;

      stmt.execute(
          "CREATE STREAM opt28 FROM t1 TUMBLE(10m, "
              + base
              + ") INTO sum_calc(time, sum_value) "
              + "SELECT ${START_TIME}, sum(value) FROM ${ROWS} "
              + "WITH (left_closed = false)");
      stmt.execute("START STREAM opt28");

      // Three boundary rows, one per window boundary; then the v=0 progress record 10:30.
      stmt.execute("INSERT INTO t1(time, device_id, value) VALUES (" + base + ", 'd1', 1.0)");
      stmt.execute(
          "INSERT INTO t1(time, device_id, value) VALUES (" + (base + 600_000L) + ", 'd1', 2.0)");
      stmt.execute(
          "INSERT INTO t1(time, device_id, value) VALUES (" + (base + 1_200_000L) + ", 'd1', 4.0)");
      stmt.execute(
          "INSERT INTO t1(time, device_id, value) VALUES (" + (base + 1_800_000L) + ", 'd1', 0.0)");

      final long[] starts = {base - 600_000L, base, base + 600_000L};
      final double[] sums = {1.0, 2.0, 4.0};
      AWAIT.untilAsserted(() -> assertWindowRows(stmt, "sum_calc", starts, sums));

      // Each boundary record appears exactly once: no duplicates or late additions.
      Thread.sleep(NEGATIVE_OBSERVE_MS);
      assertWindowRows(stmt, "sum_calc", starts, sums);
    }
  }

  /**
   * OPT-29 LEFT_CLOSED=false event-time and system-time boundaries. Under left-open-right-closed
   * the trigger comparisons flip from >= to strictly >: watermark > window end closes, watermark >
   * lateness line destroys.
   *
   * <p>Event-time path (stream opt29e with left_closed=false, watermark=0, allowed_lateness=5m and
   * NO system-time options, so no background timer can contaminate the 1ms attribution — with the
   * case's 10m/5m values that isolation is exactly what the long durations buy): open (10:00,10:10]
   * with 10:05(v=1); a progress record AT the window end (watermark = end) must NOT close; +1ms
   * must close (first result 1.0). Late 10:06(v=8) must not revise immediately; a progress record
   * AT the lateness line 10:15 (= end + 5m) must NOT destroy; +1ms must destroy after recompute and
   * revise to 9.0.
   *
   * <p>System-time path (stream opt29s, same options plus max_delay_window_close=5s and
   * max_idle_window_retain=5s — the seconds-scale executable form of the case's 10m/5m, the same
   * scale the case list itself prescribes for OPT-31): no result before the force-close deadline,
   * first result right after it; the late row is accepted without an immediate revision and the 5s
   * idle destroy revises 1.0 -> 9.0 (allowed_lateness=5m > 0, so the idle timer legitimately fires
   * here).
   *
   * <p>Oracle: W1 = 1.0, revised to 9.0 with the late 8.0 (hand-counted). The equal-point steps are
   * the red flags for an engine that still closes/destroys on >=.
   */
  @Test
  public void testOPT29LeftClosedFalseEventAndSystemTimeBoundary() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      createDatabase(stmt, "opt29");
      stmt.execute(
          "CREATE TABLE t_e(time TIMESTAMP TIME, device_id STRING TAG, value DOUBLE FIELD)");
      stmt.execute("CREATE TABLE sum_e(time TIMESTAMP TIME, sum_value DOUBLE FIELD)");
      stmt.execute(
          "CREATE TABLE t_s(time TIMESTAMP TIME, device_id STRING TAG, value DOUBLE FIELD)");
      stmt.execute("CREATE TABLE sum_s(time TIMESTAMP TIME, sum_value DOUBLE FIELD)");

      final long base = System.currentTimeMillis() + 2000;
      final long sBase = base + 3_600_000L; // 1h later, same 10m alignment grid

      stmt.execute(
          "CREATE STREAM opt29e FROM t_e TUMBLE(10m, "
              + base
              + ") INTO sum_e(time, sum_value) "
              + "SELECT ${START_TIME}, sum(value) FROM ${ROWS} "
              + "WITH (left_closed = false, watermark = 0, allowed_lateness = 5m)");
      stmt.execute(
          "CREATE STREAM opt29s FROM t_s TUMBLE(10m, "
              + sBase
              + ") INTO sum_s(time, sum_value) "
              + "SELECT ${START_TIME}, sum(value) FROM ${ROWS} "
              + "WITH (left_closed = false, watermark = 0, allowed_lateness = 5m, "
              + "max_delay_window_close = 5s, max_idle_window_retain = 5s)");
      stmt.execute("START STREAM opt29e");
      stmt.execute("START STREAM opt29s");

      // ---- Event-time path: equal point vs +1ms on the CLOSE boundary. ----
      // Open (base, base+10m] with 10:05(v=1).
      stmt.execute(
          "INSERT INTO t_e(time, device_id, value) VALUES (" + (base + 300_000L) + ", 'd1', 1.0)");
      // Progress record at exactly the window end: watermark = end, strict > not met -> no close.
      stmt.execute(
          "INSERT INTO t_e(time, device_id, value) VALUES (" + (base + 600_000L) + ", 'd1', 0.0)");
      assertEmptyAfterObservation(stmt, "sum_e");
      // +1ms: the watermark exceeds the end -> the window closes with the first result.
      stmt.execute(
          "INSERT INTO t_e(time, device_id, value) VALUES (" + (base + 600_001L) + ", 'd1', 0.0)");
      AWAIT.untilAsserted(() -> assertSingleSum(stmt, "sum_e", 1.0));

      // ---- Event-time path: equal point vs +1ms on the DESTROY boundary. ----
      // Late 10:06(v=8) for the closed window: no immediate revision.
      stmt.execute(
          "INSERT INTO t_e(time, device_id, value) VALUES (" + (base + 360_000L) + ", 'd1', 8.0)");
      Thread.sleep(NEGATIVE_OBSERVE_MS);
      assertSingleSum(stmt, "sum_e", 1.0);
      // Progress record at exactly the lateness line (end + 5m): watermark = lateness line,
      // strict > not met -> no destroy, no revision.
      stmt.execute(
          "INSERT INTO t_e(time, device_id, value) VALUES (" + (base + 900_000L) + ", 'd1', 0.0)");
      Thread.sleep(NEGATIVE_OBSERVE_MS);
      assertSingleSum(stmt, "sum_e", 1.0);
      // +1ms: the destroy after full recompute must include the late row.
      stmt.execute(
          "INSERT INTO t_e(time, device_id, value) VALUES (" + (base + 900_001L) + ", 'd1', 0.0)");
      AWAIT.untilAsserted(() -> assertSingleSum(stmt, "sum_e", 9.0));

      // ---- System-time path. ----
      final long t0 = System.currentTimeMillis();
      // Open (sBase, sBase+10m] with 20:05(v=1); the watermark stays below the end, so only the
      // force-close timer can close the window (deadline = t0 + 5s, anchored at the window open).
      stmt.execute(
          "INSERT INTO t_s(time, device_id, value) VALUES (" + (sBase + 300_000L) + ", 'd1', 1.0)");
      Thread.sleep(Math.max(0, t0 + 4_000L - System.currentTimeMillis()));
      assertTrue(
          "no result allowed before the force-close deadline", querySums(stmt, "sum_s").isEmpty());
      // First result within the system-trigger bound after t0+5s.
      SYSTEM_TRIGGER_AWAIT.untilAsserted(() -> assertSingleSum(stmt, "sum_s", null, 1.0));

      // Late row 1s after the first result: no immediate revision.
      Thread.sleep(200L);
      stmt.execute(
          "INSERT INTO t_s(time, device_id, value) VALUES (" + (sBase + 360_000L) + ", 'd1', 8.0)");
      Thread.sleep(200L);
      assertSingleSum(stmt, "sum_s", 1.0);
      // 5s idle after the late row: the forced destroy revises to 9.0 (system-trigger bound).
      SYSTEM_TRIGGER_AWAIT.untilAsserted(() -> assertSingleSum(stmt, "sum_s", null, 9.0));
    }
  }

  /**
   * OPT-31 both system-time options in one black-box flow. Set watermark=2m, allowed_lateness=5m,
   * max_delay_window_close=5s and max_idle_window_retain=5s (the case list's own values). At t0
   * write normal data whose watermark (10:05 - 2m = 10:03) stays below the window end, so the
   * event-time line cannot close the window: only the force-close timer can — no result before
   * t0+5s, first result within tolerance after it. One second after the first result write the late
   * row 10:06(v=8): no immediate revision; after 5s of idle time the forced destroy runs the full
   * recompute and revises 3.0 -> 11.0.
   *
   * <p>Oracle: W1 = 1.0+2.0 first, revised to 11.0 with the late 8.0 (hand-counted).
   */
  @Test
  public void testOPT31SystemTimeOptionsBlackBoxFlow() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      createDatabase(stmt, "opt31");
      stmt.execute(
          "CREATE TABLE t1(time TIMESTAMP TIME, device_id STRING TAG, value DOUBLE FIELD)");
      stmt.execute("CREATE TABLE sum_calc(time TIMESTAMP TIME, sum_value DOUBLE FIELD)");

      final long base = System.currentTimeMillis() + 2000;

      stmt.execute(
          "CREATE STREAM opt31 FROM t1 TUMBLE(10m, "
              + base
              + ") INTO sum_calc(time, sum_value) "
              + "SELECT ${START_TIME}, sum(value) FROM ${ROWS} "
              + "WITH (watermark = 2m, allowed_lateness = 5m, max_delay_window_close = 5s, "
              + "max_idle_window_retain = 5s)");
      stmt.execute("START STREAM opt31");

      final long t0 = System.currentTimeMillis();
      // Normal data: watermark 10:03 stays below the window end 10:10 -> no event-time close.
      stmt.execute("INSERT INTO t1(time, device_id, value) VALUES (" + base + ", 'd1', 1.0)");
      stmt.execute(
          "INSERT INTO t1(time, device_id, value) VALUES (" + (base + 300_000L) + ", 'd1', 2.0)");

      // No result before the force-close deadline t0+5s.
      Thread.sleep(Math.max(0, t0 + 4_000L - System.currentTimeMillis()));
      assertTrue(
          "no result allowed before the force-close deadline",
          querySums(stmt, "sum_calc").isEmpty());
      // First result within the system-trigger bound after t0+5s.
      SYSTEM_TRIGGER_AWAIT.untilAsserted(() -> assertSingleSum(stmt, "sum_calc", null, 3.0));

      // Late row 1s after the first result: no immediate revision.
      Thread.sleep(200L);
      stmt.execute(
          "INSERT INTO t1(time, device_id, value) VALUES (" + (base + 360_000L) + ", 'd1', 8.0)");
      Thread.sleep(200L);
      assertSingleSum(stmt, "sum_calc", 3.0);

      // 5s idle after the late row: the forced destroy revises to 11.0 (system-trigger bound).
      SYSTEM_TRIGGER_AWAIT.untilAsserted(() -> assertSingleSum(stmt, "sum_calc", null, 11.0));

      // No further changes.
      Thread.sleep(NEGATIVE_OBSERVE_MS);
      assertSingleSum(stmt, "sum_calc", 11.0);
    }
  }

  /**
   * OPT-32 combination-matrix baseline: all five options omitted. PARTITION BY device_id; the E
   * group drives the event-time line, the S group proves the system-time line stays disarmed. E:
   * 10:00(v=1), 10:05(v=2), then 10:10(v=4) (boundary row of W2 which also advances the watermark
   * to the W1 end) -> W1 [10:00,10:10) outputs SUM=3; the late 10:07(v=16) is dropped because
   * allowed_lateness=0 makes close and destroy atomic. S: rows at wall 0s/2s with the watermark
   * stuck below the end must stay silent for the observation period (max_delay_window_close=0: no
   * system-time close); the group's own 20:10(v=0) progress record closes AND destroys the window
   * in one step (SUM=10); the late 20:06(v=20) is dropped.
   *
   * <p>With the whole WITH clause absent this is also the B-? leftClosed-default case (if the
   * defect is live, 10:00 lands in the previous window and W1 does not sum to 3.0). Compare with
   * OPT-33, the same data WITH a WITH clause present.
   *
   * <p>Oracle: E group exactly [3.0]; S group exactly [10.0] (hand-counted).
   */
  @Test
  public void testOPT32CombinationAllDefaults() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      createDatabase(stmt, "opt32");
      stmt.execute(
          "CREATE TABLE t1(time TIMESTAMP TIME, device_id STRING TAG, value DOUBLE FIELD)");
      stmt.execute(
          "CREATE TABLE sum_calc(time TIMESTAMP TIME, device_id STRING TAG, sum_value DOUBLE"
              + " FIELD)");

      final long base = System.currentTimeMillis() + 2000;
      final long sBase = base + 3_600_000L; // 1h later, same 10m alignment grid

      stmt.execute(
          "CREATE STREAM opt32 FROM t1 PARTITION BY device_id TUMBLE(10m, "
              + base
              + ") INTO sum_calc(time, device_id, sum_value) "
              + "SELECT ${START_TIME}, ${1}, sum(value) FROM ${ROWS}");
      stmt.execute("START STREAM opt32");

      // ---- E group: event-time close, atomic destroy, late data dropped. ----
      stmt.execute("INSERT INTO t1(time, device_id, value) VALUES (" + base + ", 'e', 1.0)");
      stmt.execute(
          "INSERT INTO t1(time, device_id, value) VALUES (" + (base + 300_000L) + ", 'e', 2.0)");
      stmt.execute(
          "INSERT INTO t1(time, device_id, value) VALUES (" + (base + 600_000L) + ", 'e', 4.0)");
      AWAIT.untilAsserted(
          () -> {
            final List<Double> sums = querySums(stmt, "sum_calc", "e");
            assertEquals(1, sums.size());
            assertEquals(3.0, sums.get(0), 1e-6);
          });
      // Late row for the atomically destroyed window: dropped, no revision.
      stmt.execute(
          "INSERT INTO t1(time, device_id, value) VALUES (" + (base + 420_000L) + ", 'e', 16.0)");
      Thread.sleep(NEGATIVE_OBSERVE_MS);
      assertSingleSum(stmt, "sum_calc", "e", 3.0);

      // ---- S group: no system-time close while the watermark cannot advance. ----
      stmt.execute(
          "INSERT INTO t1(time, device_id, value) VALUES (" + (sBase + 300_000L) + ", 's', 10.0)");
      Thread.sleep(200L);
      stmt.execute(
          "INSERT INTO t1(time, device_id, value) VALUES (" + (sBase + 420_000L) + ", 's', 0.0)");
      Thread.sleep(SYSTEM_SILENCE_MS);
      assertTrue(
          "no system-time close with max_delay_window_close omitted",
          querySums(stmt, "sum_calc", "s").isEmpty());
      // The group's own progress record closes and destroys the window in one step.
      stmt.execute(
          "INSERT INTO t1(time, device_id, value) VALUES (" + (sBase + 600_000L) + ", 's', 0.0)");
      AWAIT.untilAsserted(
          () -> {
            final List<Double> sums = querySums(stmt, "sum_calc", "s");
            assertEquals(1, sums.size());
            assertEquals(10.0, sums.get(0), 1e-6);
          });
      // Late row after the atomic destroy: dropped, no revision in either group.
      stmt.execute(
          "INSERT INTO t1(time, device_id, value) VALUES (" + (sBase + 360_000L) + ", 's', 20.0)");
      Thread.sleep(NEGATIVE_OBSERVE_MS);
      assertSingleSum(stmt, "sum_calc", "s", 10.0);
      assertSingleSum(stmt, "sum_calc", "e", 3.0);
    }
  }

  /**
   * OPT-33 combination: max_idle_window_retain only. Same data and stream shape as OPT-32, but WITH
   * (max_idle_window_retain = 5m) is present. The idle timer takes effect ONLY when
   * allowed_lateness > 0; with allowed_lateness omitted (=0) close and destroy are atomic, so the
   * configured 5m can never matter and the observable behavior must be EXACTLY OPT-32's. This cell
   * also serves as the WITH-present control for the LEFT_CLOSED default: OPT-32 (WITH absent, the
   * B-? defect applies) and OPT-33 (WITH present, leftClosed properly defaults to true) contrast
   * with each other and localize the defect to the WITH-absent path.
   *
   * <p>Oracle: E group exactly [3.0]; S group exactly [10.0] (hand-counted, identical to OPT-32).
   */
  @Test
  public void testOPT33CombinationIdleRetainOnly() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      createDatabase(stmt, "opt33");
      stmt.execute(
          "CREATE TABLE t1(time TIMESTAMP TIME, device_id STRING TAG, value DOUBLE FIELD)");
      stmt.execute(
          "CREATE TABLE sum_calc(time TIMESTAMP TIME, device_id STRING TAG, sum_value DOUBLE"
              + " FIELD)");

      final long base = System.currentTimeMillis() + 2000;
      final long sBase = base + 3_600_000L;

      stmt.execute(
          "CREATE STREAM opt33 FROM t1 PARTITION BY device_id TUMBLE(10m, "
              + base
              + ") INTO sum_calc(time, device_id, sum_value) "
              + "SELECT ${START_TIME}, ${1}, sum(value) FROM ${ROWS} "
              + "WITH (max_idle_window_retain = 5m)");
      stmt.execute("START STREAM opt33");

      // ---- E group: identical to OPT-32 (the idle timer cannot intervene with A=0). ----
      stmt.execute("INSERT INTO t1(time, device_id, value) VALUES (" + base + ", 'e', 1.0)");
      stmt.execute(
          "INSERT INTO t1(time, device_id, value) VALUES (" + (base + 300_000L) + ", 'e', 2.0)");
      stmt.execute(
          "INSERT INTO t1(time, device_id, value) VALUES (" + (base + 600_000L) + ", 'e', 4.0)");
      AWAIT.untilAsserted(
          () -> {
            final List<Double> sums = querySums(stmt, "sum_calc", "e");
            assertEquals(1, sums.size());
            assertEquals(3.0, sums.get(0), 1e-6);
          });
      stmt.execute(
          "INSERT INTO t1(time, device_id, value) VALUES (" + (base + 420_000L) + ", 'e', 16.0)");
      Thread.sleep(NEGATIVE_OBSERVE_MS);
      assertSingleSum(stmt, "sum_calc", "e", 3.0);

      // ---- S group: identical to OPT-32 (no system-time close without max_delay_window_close;
      // the idle timer only applies AFTER a close, which never happens here). ----
      stmt.execute(
          "INSERT INTO t1(time, device_id, value) VALUES (" + (sBase + 300_000L) + ", 's', 10.0)");
      Thread.sleep(200L);
      stmt.execute(
          "INSERT INTO t1(time, device_id, value) VALUES (" + (sBase + 420_000L) + ", 's', 0.0)");
      Thread.sleep(SYSTEM_SILENCE_MS);
      assertTrue(
          "no system-time close with max_delay_window_close omitted",
          querySums(stmt, "sum_calc", "s").isEmpty());
      stmt.execute(
          "INSERT INTO t1(time, device_id, value) VALUES (" + (sBase + 600_000L) + ", 's', 0.0)");
      AWAIT.untilAsserted(
          () -> {
            final List<Double> sums = querySums(stmt, "sum_calc", "s");
            assertEquals(1, sums.size());
            assertEquals(10.0, sums.get(0), 1e-6);
          });
      stmt.execute(
          "INSERT INTO t1(time, device_id, value) VALUES (" + (sBase + 360_000L) + ", 's', 20.0)");
      Thread.sleep(NEGATIVE_OBSERVE_MS);
      assertSingleSum(stmt, "sum_calc", "s", 10.0);
      assertSingleSum(stmt, "sum_calc", "e", 3.0);
    }
  }

  /**
   * OPT-34 combination: max_delay_window_close only. E group: the event-time path behaves exactly
   * like OPT-32 (the E steps finish within seconds, long before the force-close deadline, so event
   * time wins the race and the late row is dropped; the E group's never-advanced W2 is force-closed
   * about 5s after it opens and gains a 4.0 row afterwards — expected; every assertion below is
   * scoped to the TARGET window (time = window start), so W2's row never disturbs a count). S
   * group: rows at wall 0s/2s keep the watermark below the window end, so ONLY the system-time line
   * can close the window — no result before the deadline, first result within tolerance after it.
   * With allowed_lateness omitted (=0) a window's close and its destroy are one atomic step: the
   * force-close destroys the window immediately, the expired line is pushed to the window end (one
   * millisecond short on this left-closed stream), and the late 20:06(v=20) row arriving afterwards
   * is dropped by that expired line. No revision output exists past the close: the 20:10(v=0)
   * event-time advance (which also opens W2, force-closed ~5s later with its row outside the target
   * window) must leave S at 10.0. If the engine instead kept the force-closed window alive and
   * revised it on the advance, the final negative observation below turns red.
   *
   * <p>Scale note: the 10m force-close duration is shortened to 5s (the minimum allowed value) to
   * keep the S-group wait bounded. The timer-anchor discrimination (window open vs latest row)
   * stays covered by OPT-14.
   *
   * <p>Oracle: E group [3.0]; S group [10.0] only — no revision ever (hand-counted).
   */
  @Test
  public void testOPT34CombinationMaxDelayCloseOnly() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      createDatabase(stmt, "opt34");
      stmt.execute(
          "CREATE TABLE t1(time TIMESTAMP TIME, device_id STRING TAG, value DOUBLE FIELD)");
      stmt.execute(
          "CREATE TABLE sum_calc(time TIMESTAMP TIME, device_id STRING TAG, sum_value DOUBLE"
              + " FIELD)");

      final long base = System.currentTimeMillis() + 2000;
      final long sBase = base + 3_600_000L;

      stmt.execute(
          "CREATE STREAM opt34 FROM t1 PARTITION BY device_id TUMBLE(10m, "
              + base
              + ") INTO sum_calc(time, device_id, sum_value) "
              + "SELECT ${START_TIME}, ${1}, sum(value) FROM ${ROWS} "
              + "WITH (max_delay_window_close = 5s)");
      stmt.execute("START STREAM opt34");

      // ---- E group: event time wins the race; the late row is dropped. ----
      stmt.execute("INSERT INTO t1(time, device_id, value) VALUES (" + base + ", 'e', 1.0)");
      stmt.execute(
          "INSERT INTO t1(time, device_id, value) VALUES (" + (base + 300_000L) + ", 'e', 2.0)");
      stmt.execute(
          "INSERT INTO t1(time, device_id, value) VALUES (" + (base + 600_000L) + ", 'e', 4.0)");
      AWAIT.untilAsserted(
          () -> {
            final List<Double> sums = queryWindowSums(stmt, "sum_calc", "e", base);
            assertEquals(1, sums.size());
            assertEquals(3.0, sums.get(0), 1e-6);
          });
      stmt.execute(
          "INSERT INTO t1(time, device_id, value) VALUES (" + (base + 420_000L) + ", 'e', 16.0)");
      Thread.sleep(NEGATIVE_OBSERVE_MS);
      assertSingleWindowSum(stmt, "sum_calc", "e", base, 3.0);

      // ---- S group: force close by system time, revision only by event time. ----
      final long ts0 = System.currentTimeMillis();
      // The window opens at wall ts0; the force-close deadline is ts0+5s (anchored at open).
      stmt.execute(
          "INSERT INTO t1(time, device_id, value) VALUES (" + (sBase + 300_000L) + ", 's', 10.0)");
      // Decoy row 2s later: the watermark stays below the end, so this row cannot close.
      Thread.sleep(200L);
      stmt.execute(
          "INSERT INTO t1(time, device_id, value) VALUES (" + (sBase + 420_000L) + ", 's', 0.0)");
      // Mid-observation before the deadline: still no result.
      Thread.sleep(Math.max(0, ts0 + 3_000L - System.currentTimeMillis()));
      assertTrue(
          "no result before the force-close deadline",
          queryWindowSums(stmt, "sum_calc", "s", sBase).isEmpty());
      // First result within the system-trigger bound after ts0+5s.
      SYSTEM_TRIGGER_AWAIT.untilAsserted(
          () -> assertSingleWindowSum(stmt, "sum_calc", "s", sBase, 10.0));

      // Late row 1s after the first result: dropped by the expired line (allowed_lateness=0 makes
      // the force-close destroy the window atomically) — no revision, no idle destroy.
      Thread.sleep(200L);
      stmt.execute(
          "INSERT INTO t1(time, device_id, value) VALUES (" + (sBase + 360_000L) + ", 's', 20.0)");
      Thread.sleep(SYSTEM_SILENCE_MS);
      assertSingleWindowSum(stmt, "sum_calc", "s", sBase, 10.0);

      // The event-time advance to the window end must NOT revive the destroyed window: no revision
      // output exists past the close (the 20:10 row opens W2, force-closed ~5s later, whose row
      // lands outside the target window).
      stmt.execute(
          "INSERT INTO t1(time, device_id, value) VALUES (" + (sBase + 600_000L) + ", 's', 0.0)");
      Thread.sleep(SYSTEM_SILENCE_MS);
      assertSingleWindowSum(stmt, "sum_calc", "s", sBase, 10.0);
    }
  }

  /**
   * OPT-35 combination: max_delay_window_close + max_idle_window_retain, allowed_lateness omitted.
   * SPEC NOTE: max_idle_window_retain takes effect ONLY when allowed_lateness > 0 — with
   * allowed_lateness omitted (=0) a window's close and its destroy are one atomic step, no late
   * data is received afterwards, and no revision output exists past the close. The E-path window is
   * closed and destroyed atomically (a late row cannot revise it), and on the S path the
   * force-close destroys the window at once, so the idle timer has no surviving window to act on:
   * the observable behavior must be EXACTLY OPT-34's (S stays 10.0 through the end). If the engine
   * ever fails to gate the idle timer on allowed_lateness, the S-group idle observation below turns
   * red — that discrimination is this cell's real value.
   *
   * <p>Scale note: 10m/5m -> 5s/5s executable units (same as OPT-31).
   *
   * <p>Oracle: E group [3.0]; S group [10.0] only — no revision ever (hand-counted).
   */
  @Test
  public void testOPT35CombinationMaxDelayCloseAndIdleRetain() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      createDatabase(stmt, "opt35");
      stmt.execute(
          "CREATE TABLE t1(time TIMESTAMP TIME, device_id STRING TAG, value DOUBLE FIELD)");
      stmt.execute(
          "CREATE TABLE sum_calc(time TIMESTAMP TIME, device_id STRING TAG, sum_value DOUBLE"
              + " FIELD)");

      final long base = System.currentTimeMillis() + 2000;
      final long sBase = base + 3_600_000L;

      stmt.execute(
          "CREATE STREAM opt35 FROM t1 PARTITION BY device_id TUMBLE(10m, "
              + base
              + ") INTO sum_calc(time, device_id, sum_value) "
              + "SELECT ${START_TIME}, ${1}, sum(value) FROM ${ROWS} "
              + "WITH (max_delay_window_close = 5s, max_idle_window_retain = 5s)");
      stmt.execute("START STREAM opt35");

      // ---- E group: event time wins the race; the late row is dropped (I is gated by A=0). ----
      stmt.execute("INSERT INTO t1(time, device_id, value) VALUES (" + base + ", 'e', 1.0)");
      stmt.execute(
          "INSERT INTO t1(time, device_id, value) VALUES (" + (base + 300_000L) + ", 'e', 2.0)");
      stmt.execute(
          "INSERT INTO t1(time, device_id, value) VALUES (" + (base + 600_000L) + ", 'e', 4.0)");
      AWAIT.untilAsserted(
          () -> {
            final List<Double> sums = queryWindowSums(stmt, "sum_calc", "e", base);
            assertEquals(1, sums.size());
            assertEquals(3.0, sums.get(0), 1e-6);
          });
      stmt.execute(
          "INSERT INTO t1(time, device_id, value) VALUES (" + (base + 420_000L) + ", 'e', 16.0)");
      Thread.sleep(NEGATIVE_OBSERVE_MS);
      assertSingleWindowSum(stmt, "sum_calc", "e", base, 3.0);

      // ---- S group: same as OPT-34 — the idle timer must NOT fire while A=0. ----
      final long ts0 = System.currentTimeMillis();
      stmt.execute(
          "INSERT INTO t1(time, device_id, value) VALUES (" + (sBase + 300_000L) + ", 's', 10.0)");
      Thread.sleep(200L);
      stmt.execute(
          "INSERT INTO t1(time, device_id, value) VALUES (" + (sBase + 420_000L) + ", 's', 0.0)");
      Thread.sleep(Math.max(0, ts0 + 3_000L - System.currentTimeMillis()));
      assertTrue(
          "no result before the force-close deadline",
          queryWindowSums(stmt, "sum_calc", "s", sBase).isEmpty());
      // First result within the system-trigger bound after ts0+5s.
      SYSTEM_TRIGGER_AWAIT.untilAsserted(
          () -> assertSingleWindowSum(stmt, "sum_calc", "s", sBase, 10.0));

      // Late row 1s after the first result: no immediate revision. If the engine fails to gate
      // max_idle_window_retain on allowed_lateness, the 5s idle timer fires inside this
      // observation and revises to 30.0 without the event-time advance (known defect). The
      // allowed_lateness=0 force-close also drops the row at the expired line either way.
      Thread.sleep(200L);
      stmt.execute(
          "INSERT INTO t1(time, device_id, value) VALUES (" + (sBase + 360_000L) + ", 's', 20.0)");
      Thread.sleep(SYSTEM_SILENCE_MS);
      assertSingleWindowSum(stmt, "sum_calc", "s", sBase, 10.0);

      // The event-time advance to the window end must NOT revive the destroyed window: no revision
      // output exists past the close (the 20:10 row opens W2, force-closed ~5s later, whose row
      // lands outside the target window).
      stmt.execute(
          "INSERT INTO t1(time, device_id, value) VALUES (" + (sBase + 600_000L) + ", 's', 0.0)");
      Thread.sleep(SYSTEM_SILENCE_MS);
      assertSingleWindowSum(stmt, "sum_calc", "s", sBase, 10.0);
    }
  }

  /**
   * OPT-36 combination: allowed_lateness only (LEFT_CLOSED / WATERMARK / MAX_DELAY_WINDOW_CLOSE /
   * MAX_IDLE_WINDOW_RETAIN all omitted). E group: the 10:10(v=4) row closes [10:00,10:10) (first
   * SUM=3.0); the late 10:06(v=8) row must not revise immediately (the lateness line 10:15 is above
   * the watermark 10:10); the 10:15(v=0) advance pushes the watermark onto the lateness line and
   * destroys the window after a full recompute (3.0 -> 11.0); the post-destroy 10:07(v=16) row is
   * dropped (event time <= expire line 10:10). S group: with max_delay_window_close omitted the
   * system time never closes the window — only the group's own 20:10(v=0) advance closes it
   * (SUM=10.0); the late 20:06(v=20) row waits without an immediate revision and without an idle
   * destroy (max_idle_window_retain omitted); only the 20:15(v=0) advance to the lateness line
   * revises the window (10.0 -> 30.0).
   *
   * <p>Oracle: E group [3.0] then [11.0]; S group [10.0] then [30.0] (all hand-counted).
   */
  @Test
  public void testOPT36CombinationAllowedLatenessOnly() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      createDatabase(stmt, "opt36");
      stmt.execute(
          "CREATE TABLE t1(time TIMESTAMP TIME, device_id STRING TAG, value DOUBLE FIELD)");
      stmt.execute(
          "CREATE TABLE sum_calc(time TIMESTAMP TIME, device_id STRING TAG, sum_value DOUBLE"
              + " FIELD)");

      final long base = System.currentTimeMillis() + 2000;
      final long sBase = base + 3_600_000L;

      stmt.execute(
          "CREATE STREAM opt36 FROM t1 PARTITION BY device_id TUMBLE(10m, "
              + base
              + ") INTO sum_calc(time, device_id, sum_value) "
              + "SELECT ${START_TIME}, ${1}, sum(value) FROM ${ROWS} "
              + "WITH (allowed_lateness = 5m)");
      stmt.execute("START STREAM opt36");

      // ---- E group: deferred revision at the lateness line, drop after destroy. ----
      stmt.execute("INSERT INTO t1(time, device_id, value) VALUES (" + base + ", 'e', 1.0)");
      stmt.execute(
          "INSERT INTO t1(time, device_id, value) VALUES (" + (base + 300_000L) + ", 'e', 2.0)");
      stmt.execute(
          "INSERT INTO t1(time, device_id, value) VALUES (" + (base + 600_000L) + ", 'e', 4.0)");
      AWAIT.untilAsserted(
          () -> {
            final List<Double> sums = querySums(stmt, "sum_calc", "e");
            assertEquals(1, sums.size());
            assertEquals(3.0, sums.get(0), 1e-6);
          });
      // Late 10:06(v=8): accepted into the full-recompute list, no immediate revision.
      stmt.execute(
          "INSERT INTO t1(time, device_id, value) VALUES (" + (base + 360_000L) + ", 'e', 8.0)");
      Thread.sleep(NEGATIVE_OBSERVE_MS);
      assertSingleSum(stmt, "sum_calc", "e", 3.0);
      // 10:15(v=0) pushes the watermark onto the lateness line (10:10+5m): destroy after a full
      // recompute; the revision must now include the late 8.0.
      stmt.execute(
          "INSERT INTO t1(time, device_id, value) VALUES (" + (base + 900_000L) + ", 'e', 0.0)");
      AWAIT.untilAsserted(
          () -> {
            final List<Double> sums = querySums(stmt, "sum_calc", "e");
            assertEquals(1, sums.size());
            assertEquals(11.0, sums.get(0), 1e-6);
          });
      // Post-destroy late 10:07(v=16): dropped (event time <= expire line 10:10).
      stmt.execute(
          "INSERT INTO t1(time, device_id, value) VALUES (" + (base + 420_000L) + ", 'e', 16.0)");
      Thread.sleep(NEGATIVE_OBSERVE_MS);
      assertSingleSum(stmt, "sum_calc", "e", 11.0);

      // ---- S group: event time drives close, revision and destroy; system time does nothing. --
      stmt.execute(
          "INSERT INTO t1(time, device_id, value) VALUES (" + (sBase + 300_000L) + ", 's', 10.0)");
      Thread.sleep(200L);
      stmt.execute(
          "INSERT INTO t1(time, device_id, value) VALUES (" + (sBase + 420_000L) + ", 's', 0.0)");
      Thread.sleep(SYSTEM_SILENCE_MS);
      assertTrue(
          "no system-time close with max_delay_window_close omitted",
          querySums(stmt, "sum_calc", "s").isEmpty());
      // The group's own advance closes the window: first result 10.0.
      stmt.execute(
          "INSERT INTO t1(time, device_id, value) VALUES (" + (sBase + 600_000L) + ", 's', 0.0)");
      AWAIT.untilAsserted(
          () -> {
            final List<Double> sums = querySums(stmt, "sum_calc", "s");
            assertEquals(1, sums.size());
            assertEquals(10.0, sums.get(0), 1e-6);
          });
      // Late 20:06(v=20): waits without an immediate revision and without an idle destroy.
      Thread.sleep(200L);
      stmt.execute(
          "INSERT INTO t1(time, device_id, value) VALUES (" + (sBase + 360_000L) + ", 's', 20.0)");
      Thread.sleep(NEGATIVE_OBSERVE_MS);
      assertSingleSum(stmt, "sum_calc", "s", 10.0);
      // 20:15(v=0) pushes the watermark onto the lateness line (20:10+5m): revision 30.0.
      stmt.execute(
          "INSERT INTO t1(time, device_id, value) VALUES (" + (sBase + 900_000L) + ", 's', 0.0)");
      AWAIT.untilAsserted(
          () -> {
            final List<Double> sums = querySums(stmt, "sum_calc", "s");
            assertEquals(1, sums.size());
            assertEquals(30.0, sums.get(0), 1e-6);
          });
    }
  }

  /**
   * OPT-37 combination: allowed_lateness + max_idle_window_retain (LEFT_CLOSED / WATERMARK /
   * MAX_DELAY_WINDOW_CLOSE omitted). E group: identical event-time script to OPT-36 — the E steps
   * complete within seconds, so the event-time lateness line always wins the race against the
   * executable 5s idle timer and the sequence stays 3.0 -> 11.0. S group: the group's own
   * 20:10(v=0) advance still closes the window (max_delay_window_close omitted, first SUM=10.0);
   * the late 20:06(v=20) row must not revise immediately, but this time NO further event-time
   * advance is written: the idle deadline (reset by the late row to its arrival + 5s) expires and
   * max_idle_window_retain forces the destroy, revising the window to 30.0 by system time alone.
   *
   * <p>Scale note: the case list prescribes max_idle_window_retain=5m; the executable form uses 5s
   * (the unit the case list itself executes in OPT-22) so the S-group idle wait stays bounded. The
   * mid-observation after the late row is deliberately 2s (< 5s) so it cannot race the reset idle
   * deadline.
   *
   * <p>Oracle: E group [3.0] then [11.0]; S group [10.0] then [30.0] (all hand-counted).
   */
  @Test
  public void testOPT37CombinationAllowedLatenessAndIdleRetain() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      createDatabase(stmt, "opt37");
      stmt.execute(
          "CREATE TABLE t1(time TIMESTAMP TIME, device_id STRING TAG, value DOUBLE FIELD)");
      stmt.execute(
          "CREATE TABLE sum_calc(time TIMESTAMP TIME, device_id STRING TAG, sum_value DOUBLE"
              + " FIELD)");

      final long base = System.currentTimeMillis() + 2000;
      final long sBase = base + 3_600_000L;

      stmt.execute(
          "CREATE STREAM opt37 FROM t1 PARTITION BY device_id TUMBLE(10m, "
              + base
              + ") INTO sum_calc(time, device_id, sum_value) "
              + "SELECT ${START_TIME}, ${1}, sum(value) FROM ${ROWS} "
              + "WITH (allowed_lateness = 5m, max_idle_window_retain = 5s)");
      stmt.execute("START STREAM opt37");

      // ---- E group: same script as OPT-36; the idle timer must not preempt the event-time path.
      stmt.execute("INSERT INTO t1(time, device_id, value) VALUES (" + base + ", 'e', 1.0)");
      stmt.execute(
          "INSERT INTO t1(time, device_id, value) VALUES (" + (base + 300_000L) + ", 'e', 2.0)");
      stmt.execute(
          "INSERT INTO t1(time, device_id, value) VALUES (" + (base + 600_000L) + ", 'e', 4.0)");
      AWAIT.untilAsserted(
          () -> {
            final List<Double> sums = querySums(stmt, "sum_calc", "e");
            assertEquals(1, sums.size());
            assertEquals(3.0, sums.get(0), 1e-6);
          });
      // Late 10:06(v=8): no immediate revision (2s check stays clear of the reset idle deadline).
      stmt.execute(
          "INSERT INTO t1(time, device_id, value) VALUES (" + (base + 360_000L) + ", 'e', 8.0)");
      Thread.sleep(200L);
      assertSingleSum(stmt, "sum_calc", "e", 3.0);
      // 10:15(v=0) pushes the watermark onto the lateness line BEFORE the idle deadline: destroy
      // after a full recompute; the revision must now include the late 8.0.
      stmt.execute(
          "INSERT INTO t1(time, device_id, value) VALUES (" + (base + 900_000L) + ", 'e', 0.0)");
      AWAIT.untilAsserted(
          () -> {
            final List<Double> sums = querySums(stmt, "sum_calc", "e");
            assertEquals(1, sums.size());
            assertEquals(11.0, sums.get(0), 1e-6);
          });
      // Post-destroy late 10:07(v=16): dropped (event time <= expire line 10:10).
      stmt.execute(
          "INSERT INTO t1(time, device_id, value) VALUES (" + (base + 420_000L) + ", 'e', 16.0)");
      Thread.sleep(NEGATIVE_OBSERVE_MS);
      assertSingleSum(stmt, "sum_calc", "e", 11.0);

      // ---- S group: close by event time; destroy/revision by the idle timer. ----
      stmt.execute(
          "INSERT INTO t1(time, device_id, value) VALUES (" + (sBase + 300_000L) + ", 's', 10.0)");
      Thread.sleep(200L);
      stmt.execute(
          "INSERT INTO t1(time, device_id, value) VALUES (" + (sBase + 420_000L) + ", 's', 0.0)");
      Thread.sleep(SYSTEM_SILENCE_MS);
      assertTrue(
          "no system-time close with max_delay_window_close omitted",
          querySums(stmt, "sum_calc", "s").isEmpty());
      stmt.execute(
          "INSERT INTO t1(time, device_id, value) VALUES (" + (sBase + 600_000L) + ", 's', 0.0)");
      AWAIT.untilAsserted(
          () -> {
            final List<Double> sums = querySums(stmt, "sum_calc", "s");
            assertEquals(1, sums.size());
            assertEquals(10.0, sums.get(0), 1e-6);
          });
      // Late 20:06(v=20): no immediate revision inside the reset idle window (2s < 5s).
      Thread.sleep(200L);
      stmt.execute(
          "INSERT INTO t1(time, device_id, value) VALUES (" + (sBase + 360_000L) + ", 's', 20.0)");
      Thread.sleep(200L);
      assertSingleSum(stmt, "sum_calc", "s", 10.0);
      // Idle expiry (late-row arrival + 5s) forces the destroy: revision to 30.0 WITHOUT any
      // further event-time advance. The revision only appears after the wall-clock idle trigger
      // fires plus delivery/write-back latency, so the wait uses the system-trigger bound.
      SYSTEM_TRIGGER_AWAIT.untilAsserted(
          () -> {
            final List<Double> sums = querySums(stmt, "sum_calc", "s");
            assertEquals(1, sums.size());
            assertEquals(30.0, sums.get(0), 1e-6);
          });
    }
  }

  /**
   * OPT-38 combination: allowed_lateness + max_delay_window_close (LEFT_CLOSED / WATERMARK /
   * MAX_IDLE_WINDOW_RETAIN omitted). E group: identical event-time script to OPT-36 — the E steps
   * finish within seconds, far before the force-close deadline, so event time wins the race and the
   * sequence stays 3.0 -> 11.0. S group: rows at wall 0s/2s keep the watermark below the window
   * end, so only the system-time line can close the window — no result before the deadline, first
   * result (10.0) within tolerance after it. The late 20:06(v=20) row waits in the full-recompute
   * list; with max_idle_window_retain omitted the idle observation must show no revision, and only
   * the 20:15(v=0) advance to the lateness line (20:10+5m) destroys and revises the window (10.0 ->
   * 30.0).
   *
   * <p>Scale note: the case list prescribes max_delay_window_close=10m; the executable form uses 5s
   * (the spec minimum, the same unit the case list itself executes in OPT-14/15/31) so the S-group
   * wait stays bounded. The timer-anchor discrimination stays covered by OPT-14. The E group's W2
   * (10:10-10:20, containing 4.0/0.0) is force-closed ~5s after it opens and gains a 4.0 row
   * afterwards — expected; every assertion is scoped to the target window (time = window start), so
   * W2's row never disturbs a count.
   *
   * <p>Oracle: E group [3.0] then [11.0]; S group [10.0] then [30.0] (all hand-counted).
   */
  @Test
  public void testOPT38CombinationAllowedLatenessAndMaxDelayClose() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      createDatabase(stmt, "opt38");
      stmt.execute(
          "CREATE TABLE t1(time TIMESTAMP TIME, device_id STRING TAG, value DOUBLE FIELD)");
      stmt.execute(
          "CREATE TABLE sum_calc(time TIMESTAMP TIME, device_id STRING TAG, sum_value DOUBLE"
              + " FIELD)");

      final long base = System.currentTimeMillis() + 2000;
      final long sBase = base + 3_600_000L;

      stmt.execute(
          "CREATE STREAM opt38 FROM t1 PARTITION BY device_id TUMBLE(10m, "
              + base
              + ") INTO sum_calc(time, device_id, sum_value) "
              + "SELECT ${START_TIME}, ${1}, sum(value) FROM ${ROWS} "
              + "WITH (allowed_lateness = 5m, max_delay_window_close = 5s)");
      stmt.execute("START STREAM opt38");

      // ---- E group: event time wins the race; deferred revision at the lateness line. ----
      stmt.execute("INSERT INTO t1(time, device_id, value) VALUES (" + base + ", 'e', 1.0)");
      stmt.execute(
          "INSERT INTO t1(time, device_id, value) VALUES (" + (base + 300_000L) + ", 'e', 2.0)");
      stmt.execute(
          "INSERT INTO t1(time, device_id, value) VALUES (" + (base + 600_000L) + ", 'e', 4.0)");
      AWAIT.untilAsserted(
          () -> {
            final List<Double> sums = queryWindowSums(stmt, "sum_calc", "e", base);
            assertEquals(1, sums.size());
            assertEquals(3.0, sums.get(0), 1e-6);
          });
      stmt.execute(
          "INSERT INTO t1(time, device_id, value) VALUES (" + (base + 360_000L) + ", 'e', 8.0)");
      Thread.sleep(NEGATIVE_OBSERVE_MS);
      assertSingleWindowSum(stmt, "sum_calc", "e", base, 3.0);
      stmt.execute(
          "INSERT INTO t1(time, device_id, value) VALUES (" + (base + 900_000L) + ", 'e', 0.0)");
      AWAIT.untilAsserted(
          () -> {
            final List<Double> sums = queryWindowSums(stmt, "sum_calc", "e", base);
            assertEquals(1, sums.size());
            assertEquals(11.0, sums.get(0), 1e-6);
          });
      stmt.execute(
          "INSERT INTO t1(time, device_id, value) VALUES (" + (base + 420_000L) + ", 'e', 16.0)");
      Thread.sleep(NEGATIVE_OBSERVE_MS);
      assertSingleWindowSum(stmt, "sum_calc", "e", base, 11.0);

      // ---- S group: force close by system time, revision only by event time. ----
      final long ts0 = System.currentTimeMillis();
      // The window opens at wall ts0; the force-close deadline is ts0+5s (anchored at open).
      stmt.execute(
          "INSERT INTO t1(time, device_id, value) VALUES (" + (sBase + 300_000L) + ", 's', 10.0)");
      // Decoy row 2s later: the watermark stays below the end, so this row cannot close.
      Thread.sleep(200L);
      stmt.execute(
          "INSERT INTO t1(time, device_id, value) VALUES (" + (sBase + 420_000L) + ", 's', 0.0)");
      // Mid-observation before the deadline: still no result.
      Thread.sleep(Math.max(0, ts0 + 3_000L - System.currentTimeMillis()));
      assertTrue(
          "no result before the force-close deadline",
          queryWindowSums(stmt, "sum_calc", "s", sBase).isEmpty());
      // First result within the system-trigger bound after ts0+5s.
      SYSTEM_TRIGGER_AWAIT.untilAsserted(
          () -> assertSingleWindowSum(stmt, "sum_calc", "s", sBase, 10.0));

      // Late row 1s after the first result: accepted into the full-recompute list, but no
      // immediate revision and no idle destroy (max_idle_window_retain omitted = 0).
      Thread.sleep(200L);
      stmt.execute(
          "INSERT INTO t1(time, device_id, value) VALUES (" + (sBase + 360_000L) + ", 's', 20.0)");
      Thread.sleep(SYSTEM_SILENCE_MS);
      assertSingleWindowSum(stmt, "sum_calc", "s", sBase, 10.0);

      // Only the event-time advance to the lateness line (20:10+5m) destroys and revises.
      stmt.execute(
          "INSERT INTO t1(time, device_id, value) VALUES (" + (sBase + 900_000L) + ", 's', 0.0)");
      AWAIT.untilAsserted(
          () -> {
            final List<Double> sums = queryWindowSums(stmt, "sum_calc", "s", sBase);
            assertEquals(1, sums.size());
            assertEquals(30.0, sums.get(0), 1e-6);
          });
    }
  }

  /**
   * OPT-39 combination: allowed_lateness + max_delay_window_close + max_idle_window_retain
   * (LEFT_CLOSED / WATERMARK omitted) — the full system-time relay. E group: identical to OPT-36
   * (all E steps complete within seconds; the event-time lateness line beats both system timers,
   * sequence 3.0 -> 11.0). S group: the event time is deliberately frozen at 20:07, so the whole
   * lifecycle runs on system time alone: max_delay_window_close force-closes the window at the
   * deadline (first result 10.0); the late 20:06(v=20) row waits without an immediate revision and
   * resets the idle deadline; max_idle_window_retain expires (late arrival + 5s) and forces the
   * destroy, revising the window to 30.0 — no event-time advance is ever written.
   *
   * <p>Scale note: 10m/5m -> 5s/5s executable units (same as OPT-31); the mid-observation after the
   * late row is 2s so it cannot race the reset idle deadline. The E group's W2 (10:10-10:20) is
   * force-closed ~5s after it opens and gains a 4.0 row afterwards — expected; every assertion is
   * scoped to the target window (time = window start), so W2's row never disturbs a count.
   *
   * <p>Oracle: E group [3.0] then [11.0]; S group [10.0] then [30.0] (all hand-counted).
   */
  @Test
  public void testOPT39CombinationAllowedLatenessMaxDelayCloseAndIdleRetain() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      createDatabase(stmt, "opt39");
      stmt.execute(
          "CREATE TABLE t1(time TIMESTAMP TIME, device_id STRING TAG, value DOUBLE FIELD)");
      stmt.execute(
          "CREATE TABLE sum_calc(time TIMESTAMP TIME, device_id STRING TAG, sum_value DOUBLE"
              + " FIELD)");

      final long base = System.currentTimeMillis() + 2000;
      final long sBase = base + 3_600_000L;

      stmt.execute(
          "CREATE STREAM opt39 FROM t1 PARTITION BY device_id TUMBLE(10m, "
              + base
              + ") INTO sum_calc(time, device_id, sum_value) "
              + "SELECT ${START_TIME}, ${1}, sum(value) FROM ${ROWS} "
              + "WITH (allowed_lateness = 5m, max_delay_window_close = 5s, max_idle_window_retain"
              + " = 5s)");
      stmt.execute("START STREAM opt39");

      // ---- E group: same script as OPT-36; the system timers must not preempt the event path. --
      stmt.execute("INSERT INTO t1(time, device_id, value) VALUES (" + base + ", 'e', 1.0)");
      stmt.execute(
          "INSERT INTO t1(time, device_id, value) VALUES (" + (base + 300_000L) + ", 'e', 2.0)");
      stmt.execute(
          "INSERT INTO t1(time, device_id, value) VALUES (" + (base + 600_000L) + ", 'e', 4.0)");
      AWAIT.untilAsserted(
          () -> {
            final List<Double> sums = queryWindowSums(stmt, "sum_calc", "e", base);
            assertEquals(1, sums.size());
            assertEquals(3.0, sums.get(0), 1e-6);
          });
      // Late 10:06(v=8): no immediate revision (2s check stays clear of the reset idle deadline).
      stmt.execute(
          "INSERT INTO t1(time, device_id, value) VALUES (" + (base + 360_000L) + ", 'e', 8.0)");
      Thread.sleep(2_000L);
      assertSingleWindowSum(stmt, "sum_calc", "e", base, 3.0);
      // 10:15(v=0) pushes the watermark onto the lateness line BEFORE both system deadlines:
      // destroy after a full recompute; the revision must now include the late 8.0.
      stmt.execute(
          "INSERT INTO t1(time, device_id, value) VALUES (" + (base + 900_000L) + ", 'e', 0.0)");
      AWAIT.untilAsserted(
          () -> {
            final List<Double> sums = queryWindowSums(stmt, "sum_calc", "e", base);
            assertEquals(1, sums.size());
            assertEquals(11.0, sums.get(0), 1e-6);
          });
      // Post-destroy late 10:07(v=16): dropped (event time <= expire line 10:10).
      stmt.execute(
          "INSERT INTO t1(time, device_id, value) VALUES (" + (base + 420_000L) + ", 'e', 16.0)");
      Thread.sleep(NEGATIVE_OBSERVE_MS);
      assertSingleWindowSum(stmt, "sum_calc", "e", base, 11.0);

      // ---- S group: the full system-time relay with the event time frozen. ----
      final long ts0 = System.currentTimeMillis();
      // The window opens at wall ts0; the force-close deadline is ts0+5s (anchored at open).
      stmt.execute(
          "INSERT INTO t1(time, device_id, value) VALUES (" + (sBase + 300_000L) + ", 's', 10.0)");
      // Decoy row 2s later: the watermark stays below the end, so this row cannot close.
      Thread.sleep(2_000L);
      stmt.execute(
          "INSERT INTO t1(time, device_id, value) VALUES (" + (sBase + 420_000L) + ", 's', 0.0)");
      // Mid-observation before the deadline: still no result.
      Thread.sleep(Math.max(0, ts0 + 3_000L - System.currentTimeMillis()));
      assertTrue(
          "no result before the force-close deadline",
          queryWindowSums(stmt, "sum_calc", "s", sBase).isEmpty());
      // First result within the system-trigger bound after ts0+5s.
      SYSTEM_TRIGGER_AWAIT.untilAsserted(
          () -> assertSingleWindowSum(stmt, "sum_calc", "s", sBase, 10.0));

      // Late row 1s after the first result: no immediate revision inside the reset idle window
      // (2s < 5s).
      Thread.sleep(1_000L);
      stmt.execute(
          "INSERT INTO t1(time, device_id, value) VALUES (" + (sBase + 360_000L) + ", 's', 20.0)");
      Thread.sleep(2_000L);
      assertSingleWindowSum(stmt, "sum_calc", "s", sBase, 10.0);
      // Idle expiry (late-row arrival + 5s) forces the destroy: revision to 30.0 WITHOUT any
      // event-time advance — max_delay_window_close and max_idle_window_retain relay the whole
      // lifecycle by system time alone. The revision only appears after the wall-clock idle
      // trigger fires plus delivery/write-back latency, so the wait uses the system-trigger
      // bound.
      SYSTEM_TRIGGER_AWAIT.untilAsserted(
          () -> {
            final List<Double> sums = queryWindowSums(stmt, "sum_calc", "s", sBase);
            assertEquals(1, sums.size());
            assertEquals(30.0, sums.get(0), 1e-6);
          });
    }
  }

  // -----------------------------------------------------------------------------------------------
  // OPT-40 .. OPT-47: LEFT_CLOSED omitted (default left-closed right-open) with WATERMARK=2m
  // -----------------------------------------------------------------------------------------------

  /**
   * OPT-40 E path (L omitted, W=2m, A/C/I all omitted): the target window [10:00,10:10) is expected
   * to hold 3 (10:00(v=1)+10:05(v=2)); the 10:07(v=16) written after the close adds no row and
   * causes no revision, so the window stays 3.
   */
  @Test
  public void testOPT40EventTimePath() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      final long base = System.currentTimeMillis() + 2000;
      createStream(stmt, "opt40", "opt40e", base, null, W_2M, NONE, NONE, NONE);
      runEventTimePath(stmt, base, null, W_2M, NONE);
    }
  }

  /**
   * OPT-40 S path (L omitted, W=2m, A/C/I all omitted): with C=0 the system time never closes the
   * window, so 10 appears once the event time reaches the close condition (writing 20:12); the
   * 20:06(v=20) written afterwards causes no revision.
   */
  @Test
  public void testOPT40SystemTimePath() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      final long base = System.currentTimeMillis() + 2000;
      final long sBase = base + OPT_WINDOW_MS;
      createStream(stmt, "opt40", "opt40s", base, null, W_2M, NONE, NONE, NONE);
      runSystemTimePath(stmt, sBase, null, W_2M, NONE, NONE, NONE);
    }
  }

  /**
   * OPT-41 E path (L omitted, W=2m, A/C omitted, I=5m): I takes no part in the event-time path, so
   * the expectation matches OPT-40 (3).
   */
  @Test
  public void testOPT41EventTimePath() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      final long base = System.currentTimeMillis() + 2000;
      createStream(stmt, "opt41", "opt41e", base, null, W_2M, NONE, NONE, I_5M);
      runEventTimePath(stmt, base, null, W_2M, NONE);
    }
  }

  /**
   * OPT-41 S path (L omitted, W=2m, A/C omitted, I=5m): with A=0 the idle timer does not take
   * effect, so the expectation matches OPT-40: the event time closes the window at 10, and the
   * 20:06(v=20) written afterwards causes no revision.
   */
  @Test
  public void testOPT41SystemTimePath() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      final long base = System.currentTimeMillis() + 2000;
      final long sBase = base + OPT_WINDOW_MS;
      createStream(stmt, "opt41", "opt41s", base, null, W_2M, NONE, NONE, I_5M);
      runSystemTimePath(stmt, sBase, null, W_2M, NONE, NONE, I_5M);
    }
  }

  /**
   * OPT-42 E path (L omitted, W=2m, A/I omitted, C=10m): C takes no part in the event-time path, so
   * the expectation matches OPT-40 (3).
   */
  @Test
  public void testOPT42EventTimePath() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      final long base = System.currentTimeMillis() + 2000;
      createStream(stmt, "opt42", "opt42e", base, null, W_2M, NONE, C_10M, NONE);
      runEventTimePath(stmt, base, null, W_2M, NONE);
    }
  }

  /**
   * OPT-42 S path (L omitted, W=2m, A/I omitted, C=10m): the first result appears only after the
   * system-time close deadline (window open + 10s) = 10. With A omitted (=0) that close destroys
   * the window in the same step, so the 20:06(v=20) written at 00:41 lands inside the expired line
   * and is dropped: nothing is revised at the 00:46 observation point, and the later probe at 20:12
   * does not revive the window either - the result stays 10.
   */
  @Test
  public void testOPT42SystemTimePath() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      final long base = System.currentTimeMillis() + 2000;
      final long sBase = base + OPT_WINDOW_MS;
      createStream(stmt, "opt42", "opt42s", base, null, W_2M, NONE, C_10M, NONE);
      runSystemTimePath(stmt, sBase, null, W_2M, NONE, C_10M, NONE);
    }
  }

  /**
   * OPT-43 E path (L omitted, W=2m, A omitted, C=10m, I=5m): the expectation matches OPT-40 (3).
   *
   * <p>The stream is created with ALLOWED_LATENESS set to 1m (this case's S expectation needs the
   * idle timer to be armed); the E path still follows the case text and stops at the close and
   * 10:07, without entering the retention steps.
   */
  @Test
  public void testOPT43EventTimePath() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      final long base = System.currentTimeMillis() + 2000;
      createStream(stmt, "opt43", "opt43e", base, null, W_2M, A_1M, C_10M, I_5M);
      runEventTimePath(stmt, base, null, W_2M, NONE);
    }
  }

  /**
   * OPT-43 S path (L omitted, W=2m, A=1m, C=10m, I=5m): the system time closes the window at 10;
   * the value does not change as soon as 20:06(v=20) arrives, and is revised to 30 once the idle
   * retention time (5s) after the last late row has elapsed.
   *
   * <p>ALLOWED_LATENESS is set to 1m to make the idle timer valid (it only takes effect when
   * ALLOWED_LATENESS &gt; 0), matching the case text's "query the revised result within a
   * reasonable scheduling tolerance after the full 5m".
   */
  @Test
  public void testOPT43SystemTimePath() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      final long base = System.currentTimeMillis() + 2000;
      final long sBase = base + OPT_WINDOW_MS;
      createStream(stmt, "opt43", "opt43s", base, null, W_2M, A_1M, C_10M, I_5M);
      runSystemTimePath(stmt, sBase, null, W_2M, A_1M, C_10M, I_5M);
    }
  }

  /**
   * OPT-44 E path (L omitted, W=2m, A=5m, C/I omitted): first 3; the value does not change as soon
   * as 10:06(v=8) arrives; after the retention period ends (the watermark reaching the lateness
   * line 20:15, probe 10:17) it is revised to 11; 10:07(v=16) no longer changes the result.
   */
  @Test
  public void testOPT44EventTimePath() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      final long base = System.currentTimeMillis() + 2000;
      createStream(stmt, "opt44", "opt44e", base, null, W_2M, A_5M, NONE, NONE);
      runEventTimePath(stmt, base, null, W_2M, A_5M);
    }
  }

  /**
   * OPT-44 S path (L omitted, W=2m, A=5m, C/I omitted): no first result during the system-time
   * observation, so 10 appears once the event time reaches the close condition (writing 20:12); the
   * value does not change as soon as 20:06(v=20) arrives, and is revised to 30 only after the
   * watermark passes the lateness line (probe 20:17).
   */
  @Test
  public void testOPT44SystemTimePath() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      final long base = System.currentTimeMillis() + 2000;
      final long sBase = base + OPT_WINDOW_MS;
      createStream(stmt, "opt44", "opt44s", base, null, W_2M, A_5M, NONE, NONE);
      runSystemTimePath(stmt, sBase, null, W_2M, A_5M, NONE, NONE);
    }
  }

  /**
   * OPT-45 E path (L omitted, W=2m, A=5m, C omitted, I=5m): I takes no part in the event-time path,
   * so the expectation matches OPT-44 (3 -> 11).
   */
  @Test
  public void testOPT45EventTimePath() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      final long base = System.currentTimeMillis() + 2000;
      createStream(stmt, "opt45", "opt45e", base, null, W_2M, A_5M, NONE, I_5M);
      runEventTimePath(stmt, base, null, W_2M, A_5M);
    }
  }

  /**
   * OPT-45 S path (L omitted, W=2m, A=5m, C omitted, I=5m): the event time closes the window at 10;
   * the value does not change as soon as 20:06(v=20) arrives, and is revised to 30 once the idle
   * retention time (5s) after the last late row has elapsed.
   */
  @Test
  public void testOPT45SystemTimePath() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      final long base = System.currentTimeMillis() + 2000;
      final long sBase = base + OPT_WINDOW_MS;
      createStream(stmt, "opt45", "opt45s", base, null, W_2M, A_5M, NONE, I_5M);
      runSystemTimePath(stmt, sBase, null, W_2M, A_5M, NONE, I_5M);
    }
  }

  /**
   * OPT-46 E path (L omitted, W=2m, A=5m, C=10m, I omitted): C takes no part in the event-time
   * path, so the expectation matches OPT-44 (3 -> 11 after the retention period ends).
   */
  @Test
  public void testOPT46EventTimePath() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      final long base = System.currentTimeMillis() + 2000;
      createStream(stmt, "opt46", "opt46e", base, null, W_2M, A_5M, C_10M, NONE);
      runEventTimePath(stmt, base, null, W_2M, A_5M);
    }
  }

  /**
   * OPT-46 S path (L omitted, W=2m, A=5m, C=10m, I omitted): the first result appears only after
   * the system-time close deadline = 10; it does not change as soon as 20:06(v=20) arrives, and the
   * system-time observation point shows no revision either, until the watermark passes the lateness
   * line (probe 20:17) and the result is revised to 30.
   */
  @Test
  public void testOPT46SystemTimePath() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      final long base = System.currentTimeMillis() + 2000;
      final long sBase = base + OPT_WINDOW_MS;
      createStream(stmt, "opt46", "opt46s", base, null, W_2M, A_5M, C_10M, NONE);
      runSystemTimePath(stmt, sBase, null, W_2M, A_5M, C_10M, NONE);
    }
  }

  /**
   * OPT-47 E path (L omitted, W=2m, A=5m, C=10m, I=5m): the expectation matches OPT-44 (3 -> 11).
   */
  @Test
  public void testOPT47EventTimePath() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      final long base = System.currentTimeMillis() + 2000;
      createStream(stmt, "opt47", "opt47e", base, null, W_2M, A_5M, C_10M, I_5M);
      runEventTimePath(stmt, base, null, W_2M, A_5M);
    }
  }

  /**
   * OPT-47 S path (L omitted, W=2m, A=5m, C=10m, I=5m): the system time closes the window at 10;
   * the value does not change as soon as 20:06(v=20) arrives, and is revised to 30 once the idle
   * retention time (5s) after the last late row has elapsed.
   */
  @Test
  public void testOPT47SystemTimePath() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      final long base = System.currentTimeMillis() + 2000;
      final long sBase = base + OPT_WINDOW_MS;
      createStream(stmt, "opt47", "opt47s", base, null, W_2M, A_5M, C_10M, I_5M);
      runSystemTimePath(stmt, sBase, null, W_2M, A_5M, C_10M, I_5M);
    }
  }

  // -----------------------------------------------------------------------------------------------
  // OPT-48 .. OPT-55: LEFT_CLOSED=true with WATERMARK omitted (same semantics as OPT-32..39)
  // -----------------------------------------------------------------------------------------------

  /**
   * OPT-48 E path (L=true, W/A/C/I all omitted): the target window [10:00,10:10) is expected to
   * hold 3; writing 10:10(v=4) closes the window, and the 10:07(v=16) written afterwards adds no
   * row and causes no revision.
   */
  @Test
  public void testOPT48EventTimePath() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      final long base = System.currentTimeMillis() + 2000;
      createStream(stmt, "opt48", "opt48e", base, Boolean.TRUE, NONE, NONE, NONE, NONE);
      runEventTimePath(stmt, base, Boolean.TRUE, NONE, NONE);
    }
  }

  /**
   * OPT-48 S path (L=true, W/A/C/I all omitted): no result during the system-time observation; 10
   * appears once the event time reaches the close condition (writing 20:10), and the same progress
   * step completes the window processing; the 20:06(v=20) written afterwards causes no revision.
   */
  @Test
  public void testOPT48SystemTimePath() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      final long base = System.currentTimeMillis() + 2000;
      final long sBase = base + OPT_WINDOW_MS;
      createStream(stmt, "opt48", "opt48s", base, Boolean.TRUE, NONE, NONE, NONE, NONE);
      runSystemTimePath(stmt, sBase, Boolean.TRUE, NONE, NONE, NONE, NONE);
    }
  }

  /**
   * OPT-49 E path (L=true, W/A/C omitted, I=5m): I takes no part in the event-time path, so the
   * expectation matches OPT-48 (3).
   */
  @Test
  public void testOPT49EventTimePath() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      final long base = System.currentTimeMillis() + 2000;
      createStream(stmt, "opt49", "opt49e", base, Boolean.TRUE, NONE, NONE, NONE, I_5M);
      runEventTimePath(stmt, base, Boolean.TRUE, NONE, NONE);
    }
  }

  /**
   * OPT-49 S path (L=true, W/A/C omitted, I=5m): with A=0 the idle timer does not take effect, so
   * the expectation matches OPT-48: the event time closes the window at 10, and the 20:06(v=20)
   * written afterwards causes no revision.
   */
  @Test
  public void testOPT49SystemTimePath() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      final long base = System.currentTimeMillis() + 2000;
      final long sBase = base + OPT_WINDOW_MS;
      createStream(stmt, "opt49", "opt49s", base, Boolean.TRUE, NONE, NONE, NONE, I_5M);
      runSystemTimePath(stmt, sBase, Boolean.TRUE, NONE, NONE, NONE, I_5M);
    }
  }

  /**
   * OPT-50 E path (L=true, W/A/I omitted, C=10m): C takes no part in the event-time path, so the
   * expectation matches OPT-48 (3).
   */
  @Test
  public void testOPT50EventTimePath() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      final long base = System.currentTimeMillis() + 2000;
      createStream(stmt, "opt50", "opt50e", base, Boolean.TRUE, NONE, NONE, C_10M, NONE);
      runEventTimePath(stmt, base, Boolean.TRUE, NONE, NONE);
    }
  }

  /**
   * OPT-50 S path (L=true, W/A/I omitted, C=10m): the first result appears only after the
   * system-time close deadline = 10. With A omitted (=0) that close destroys the window in the same
   * step, so the 20:06(v=20) written at 00:41 lands inside the expired line and is dropped: nothing
   * is revised at the 00:46 observation point, and the later probe at 20:10 (the window end) does
   * not revive the window either - the result stays 10.
   */
  @Test
  public void testOPT50SystemTimePath() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      final long base = System.currentTimeMillis() + 2000;
      final long sBase = base + OPT_WINDOW_MS;
      createStream(stmt, "opt50", "opt50s", base, Boolean.TRUE, NONE, NONE, C_10M, NONE);
      runSystemTimePath(stmt, sBase, Boolean.TRUE, NONE, NONE, C_10M, NONE);
    }
  }

  /**
   * OPT-51 E path (L=true, W/A omitted, C=10m, I=5m): the expectation matches OPT-48 (3).
   *
   * <p>The stream is created with ALLOWED_LATENESS set to 1m (this case's S expectation needs the
   * idle timer to be armed); the E path still follows the case text and stops at the close and
   * 10:07, without entering the retention steps.
   */
  @Test
  public void testOPT51EventTimePath() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      final long base = System.currentTimeMillis() + 2000;
      createStream(stmt, "opt51", "opt51e", base, Boolean.TRUE, NONE, A_1M, C_10M, I_5M);
      runEventTimePath(stmt, base, Boolean.TRUE, NONE, NONE);
    }
  }

  /**
   * OPT-51 S path (L=true, W/A omitted, C=10m, I=5m): the system time closes the window at 10; the
   * value does not change as soon as 20:06(v=20) arrives, and is revised to 30 once the idle
   * retention time (5s) after the last late row has elapsed.
   *
   * <p>ALLOWED_LATENESS is set to 1m to make the idle timer valid (it only takes effect when
   * ALLOWED_LATENESS &gt; 0), matching the case text's "query the revised result within a
   * reasonable scheduling tolerance after the full 5m".
   */
  @Test
  public void testOPT51SystemTimePath() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      final long base = System.currentTimeMillis() + 2000;
      final long sBase = base + OPT_WINDOW_MS;
      createStream(stmt, "opt51", "opt51s", base, Boolean.TRUE, NONE, A_1M, C_10M, I_5M);
      runSystemTimePath(stmt, sBase, Boolean.TRUE, NONE, A_1M, C_10M, I_5M);
    }
  }

  /**
   * OPT-52 E path (L=true, W omitted, A=5m, C/I omitted): first 3; the value does not change as
   * soon as 10:06(v=8) arrives; after the retention period ends (the watermark reaching the
   * lateness line 10:15) it is revised to 11; 10:07(v=16) no longer changes the result.
   */
  @Test
  public void testOPT52EventTimePath() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      final long base = System.currentTimeMillis() + 2000;
      createStream(stmt, "opt52", "opt52e", base, Boolean.TRUE, NONE, A_5M, NONE, NONE);
      runEventTimePath(stmt, base, Boolean.TRUE, NONE, A_5M);
    }
  }

  /**
   * OPT-52 S path (L=true, W omitted, A=5m, C/I omitted): no first result during the system-time
   * observation, so 10 appears once the event time reaches the close condition (writing 20:10); it
   * does not change as soon as 20:06(v=20) arrives, and the system-time observation point shows no
   * revision either, until the watermark reaches the lateness line (probe 20:15) and the result is
   * revised to 30.
   */
  @Test
  public void testOPT52SystemTimePath() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      final long base = System.currentTimeMillis() + 2000;
      final long sBase = base + OPT_WINDOW_MS;
      createStream(stmt, "opt52", "opt52s", base, Boolean.TRUE, NONE, A_5M, NONE, NONE);
      runSystemTimePath(stmt, sBase, Boolean.TRUE, NONE, A_5M, NONE, NONE);
    }
  }

  /**
   * OPT-53 E path (L=true, W omitted, A=5m, C omitted, I=5m): I takes no part in the event-time
   * path, so the expectation matches OPT-52 (3 -> 11).
   */
  @Test
  public void testOPT53EventTimePath() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      final long base = System.currentTimeMillis() + 2000;
      createStream(stmt, "opt53", "opt53e", base, Boolean.TRUE, NONE, A_5M, NONE, I_5M);
      runEventTimePath(stmt, base, Boolean.TRUE, NONE, A_5M);
    }
  }

  /**
   * OPT-53 S path (L=true, W omitted, A=5m, C omitted, I=5m): the event time closes the window at
   * 10; the value does not change as soon as 20:06(v=20) arrives, and is revised to 30 once the
   * idle retention time (5s) after the last late row has elapsed.
   */
  @Test
  public void testOPT53SystemTimePath() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      final long base = System.currentTimeMillis() + 2000;
      final long sBase = base + OPT_WINDOW_MS;
      createStream(stmt, "opt53", "opt53s", base, Boolean.TRUE, NONE, A_5M, NONE, I_5M);
      runSystemTimePath(stmt, sBase, Boolean.TRUE, NONE, A_5M, NONE, I_5M);
    }
  }

  /**
   * OPT-54 E path (L=true, W omitted, A=5m, C=10m, I omitted): C takes no part in the event-time
   * path, so the expectation matches OPT-52 (3 -> 11).
   */
  @Test
  public void testOPT54EventTimePath() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      final long base = System.currentTimeMillis() + 2000;
      createStream(stmt, "opt54", "opt54e", base, Boolean.TRUE, NONE, A_5M, C_10M, NONE);
      runEventTimePath(stmt, base, Boolean.TRUE, NONE, A_5M);
    }
  }

  /**
   * OPT-54 S path (L=true, W omitted, A=5m, C=10m, I omitted): the first result appears only after
   * the system-time close deadline = 10; it does not change as soon as 20:06(v=20) arrives, and the
   * system-time observation point shows no revision either, until the watermark reaches the
   * lateness line (probe 20:15) and the result is revised to 30.
   */
  @Test
  public void testOPT54SystemTimePath() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      final long base = System.currentTimeMillis() + 2000;
      final long sBase = base + OPT_WINDOW_MS;
      createStream(stmt, "opt54", "opt54s", base, Boolean.TRUE, NONE, A_5M, C_10M, NONE);
      runSystemTimePath(stmt, sBase, Boolean.TRUE, NONE, A_5M, C_10M, NONE);
    }
  }

  /**
   * OPT-55 E path (L=true, W omitted, A=5m, C=10m, I=5m): the expectation matches OPT-52 (3 -> 11).
   */
  @Test
  public void testOPT55EventTimePath() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      final long base = System.currentTimeMillis() + 2000;
      createStream(stmt, "opt55", "opt55e", base, Boolean.TRUE, NONE, A_5M, C_10M, I_5M);
      runEventTimePath(stmt, base, Boolean.TRUE, NONE, A_5M);
    }
  }

  /**
   * OPT-55 S path (L=true, W omitted, A=5m, C=10m, I=5m): the system time closes the window at 10;
   * the value does not change as soon as 20:06(v=20) arrives, and is revised to 30 once the idle
   * retention time (5s) after the last late row has elapsed.
   */
  @Test
  public void testOPT55SystemTimePath() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      final long base = System.currentTimeMillis() + 2000;
      final long sBase = base + OPT_WINDOW_MS;
      createStream(stmt, "opt55", "opt55s", base, Boolean.TRUE, NONE, A_5M, C_10M, I_5M);
      runSystemTimePath(stmt, sBase, Boolean.TRUE, NONE, A_5M, C_10M, I_5M);
    }
  }

  // -----------------------------------------------------------------------------------------------
  // OPT-56 .. OPT-63: LEFT_CLOSED=true with WATERMARK=2m (differs from OPT-40..47 only by the
  // explicit L=true)
  // -----------------------------------------------------------------------------------------------

  /**
   * OPT-56 E path (L=true, W=2m, A/C/I omitted): the target window [10:00,10:10) is expected to
   * hold 3; writing 10:12(v=0) closes the window, and the 10:07(v=16) written afterwards adds no
   * row and causes no revision.
   */
  @Test
  public void testOPT56EventTimePath() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      final long base = System.currentTimeMillis() + 2000;
      createStream(stmt, "opt56", "opt56e", base, Boolean.TRUE, W_2M, NONE, NONE, NONE);
      runEventTimePath(stmt, base, Boolean.TRUE, W_2M, NONE);
    }
  }

  /**
   * OPT-56 S path (L=true, W=2m, A/C/I omitted): with C=0 the system time never closes the window,
   * so 10 appears once the event time reaches the close condition (writing 20:12); the 20:06(v=20)
   * written afterwards causes no revision.
   */
  @Test
  public void testOPT56SystemTimePath() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      final long base = System.currentTimeMillis() + 2000;
      final long sBase = base + OPT_WINDOW_MS;
      createStream(stmt, "opt56", "opt56s", base, Boolean.TRUE, W_2M, NONE, NONE, NONE);
      runSystemTimePath(stmt, sBase, Boolean.TRUE, W_2M, NONE, NONE, NONE);
    }
  }

  /**
   * OPT-57 E path (L=true, W=2m, A/C omitted, I=5m): I takes no part in the event-time path, so the
   * expectation matches OPT-56 (3).
   */
  @Test
  public void testOPT57EventTimePath() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      final long base = System.currentTimeMillis() + 2000;
      createStream(stmt, "opt57", "opt57e", base, Boolean.TRUE, W_2M, NONE, NONE, I_5M);
      runEventTimePath(stmt, base, Boolean.TRUE, W_2M, NONE);
    }
  }

  /**
   * OPT-57 S path (L=true, W=2m, A/C omitted, I=5m): with A=0 the idle timer does not take effect,
   * so the expectation matches OPT-56: the event time closes the window at 10, and the 20:06(v=20)
   * written afterwards causes no revision.
   */
  @Test
  public void testOPT57SystemTimePath() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      final long base = System.currentTimeMillis() + 2000;
      final long sBase = base + OPT_WINDOW_MS;
      createStream(stmt, "opt57", "opt57s", base, Boolean.TRUE, W_2M, NONE, NONE, I_5M);
      runSystemTimePath(stmt, sBase, Boolean.TRUE, W_2M, NONE, NONE, I_5M);
    }
  }

  /**
   * OPT-58 E path (L=true, W=2m, A/I omitted, C=10m): C takes no part in the event-time path, so
   * the expectation matches OPT-56 (3).
   */
  @Test
  public void testOPT58EventTimePath() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      final long base = System.currentTimeMillis() + 2000;
      createStream(stmt, "opt58", "opt58e", base, Boolean.TRUE, W_2M, NONE, C_10M, NONE);
      runEventTimePath(stmt, base, Boolean.TRUE, W_2M, NONE);
    }
  }

  /**
   * OPT-58 S path (L=true, W=2m, A/I omitted, C=10m): the first result appears only after the
   * system-time close deadline = 10. With A omitted (=0) that close destroys the window in the same
   * step, so the 20:06(v=20) written at 00:41 lands inside the expired line and is dropped: nothing
   * is revised at the 00:46 observation point, and the later probe at 20:12 does not revive the
   * window either - the result stays 10.
   */
  @Test
  public void testOPT58SystemTimePath() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      final long base = System.currentTimeMillis() + 2000;
      final long sBase = base + OPT_WINDOW_MS;
      createStream(stmt, "opt58", "opt58s", base, Boolean.TRUE, W_2M, NONE, C_10M, NONE);
      runSystemTimePath(stmt, sBase, Boolean.TRUE, W_2M, NONE, C_10M, NONE);
    }
  }

  /**
   * OPT-59 E path (L=true, W=2m, A omitted, C=10m, I=5m): the expectation matches OPT-56 (3).
   *
   * <p>The stream is created with ALLOWED_LATENESS set to 1m (this case's S expectation needs the
   * idle timer to be armed); the E path still follows the case text and stops at the close and
   * 10:07, without entering the retention steps.
   */
  @Test
  public void testOPT59EventTimePath() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      final long base = System.currentTimeMillis() + 2000;
      createStream(stmt, "opt59", "opt59e", base, Boolean.TRUE, W_2M, A_1M, C_10M, I_5M);
      runEventTimePath(stmt, base, Boolean.TRUE, W_2M, NONE);
    }
  }

  /**
   * OPT-59 S path (L=true, W=2m, A omitted, C=10m, I=5m): the system time closes the window at 10;
   * the value does not change as soon as 20:06(v=20) arrives, and is revised to 30 once the idle
   * retention time (5s) after the last late row has elapsed.
   *
   * <p>ALLOWED_LATENESS is set to 1m to make the idle timer valid (it only takes effect when
   * ALLOWED_LATENESS &gt; 0), matching the case text's "query the revised result within a
   * reasonable scheduling tolerance after the full 5m".
   */
  @Test
  public void testOPT59SystemTimePath() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      final long base = System.currentTimeMillis() + 2000;
      final long sBase = base + OPT_WINDOW_MS;
      createStream(stmt, "opt59", "opt59s", base, Boolean.TRUE, W_2M, A_1M, C_10M, I_5M);
      runSystemTimePath(stmt, sBase, Boolean.TRUE, W_2M, A_1M, C_10M, I_5M);
    }
  }

  /**
   * OPT-60 E path (L=true, W=2m, A=5m, C/I omitted): first 3; the value does not change as soon as
   * 10:06(v=8) arrives; after the retention period ends (the watermark reaching the lateness line,
   * probe 10:17) it is revised to 11; 10:07(v=16) no longer changes the result.
   */
  @Test
  public void testOPT60EventTimePath() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      final long base = System.currentTimeMillis() + 2000;
      createStream(stmt, "opt60", "opt60e", base, Boolean.TRUE, W_2M, A_5M, NONE, NONE);
      runEventTimePath(stmt, base, Boolean.TRUE, W_2M, A_5M);
    }
  }

  /**
   * OPT-60 S path (L=true, W=2m, A=5m, C/I omitted): no first result during the system-time
   * observation, so 10 appears once the event time reaches the close condition (writing 20:12); it
   * does not change as soon as 20:06(v=20) arrives, and is revised to 30 only after the watermark
   * reaches the lateness line (probe 20:17).
   */
  @Test
  public void testOPT60SystemTimePath() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      final long base = System.currentTimeMillis() + 2000;
      final long sBase = base + OPT_WINDOW_MS;
      createStream(stmt, "opt60", "opt60s", base, Boolean.TRUE, W_2M, A_5M, NONE, NONE);
      runSystemTimePath(stmt, sBase, Boolean.TRUE, W_2M, A_5M, NONE, NONE);
    }
  }

  /**
   * OPT-61 E path (L=true, W=2m, A=5m, C omitted, I=5m): the expectation matches OPT-60 (3 -> 11).
   */
  @Test
  public void testOPT61EventTimePath() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      final long base = System.currentTimeMillis() + 2000;
      createStream(stmt, "opt61", "opt61e", base, Boolean.TRUE, W_2M, A_5M, NONE, I_5M);
      runEventTimePath(stmt, base, Boolean.TRUE, W_2M, A_5M);
    }
  }

  /**
   * OPT-61 S path (L=true, W=2m, A=5m, C omitted, I=5m): the event time closes the window at 10;
   * the value does not change as soon as 20:06(v=20) arrives, and is revised to 30 once the idle
   * retention time (5s) after the last late row has elapsed.
   */
  @Test
  public void testOPT61SystemTimePath() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      final long base = System.currentTimeMillis() + 2000;
      final long sBase = base + OPT_WINDOW_MS;
      createStream(stmt, "opt61", "opt61s", base, Boolean.TRUE, W_2M, A_5M, NONE, I_5M);
      runSystemTimePath(stmt, sBase, Boolean.TRUE, W_2M, A_5M, NONE, I_5M);
    }
  }

  /**
   * OPT-62 E path (L=true, W=2m, A=5m, C=10m, I omitted): C takes no part in the event-time path,
   * so the expectation matches OPT-60 (3 -> 11).
   */
  @Test
  public void testOPT62EventTimePath() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      final long base = System.currentTimeMillis() + 2000;
      createStream(stmt, "opt62", "opt62e", base, Boolean.TRUE, W_2M, A_5M, C_10M, NONE);
      runEventTimePath(stmt, base, Boolean.TRUE, W_2M, A_5M);
    }
  }

  /**
   * OPT-62 S path (L=true, W=2m, A=5m, C=10m, I omitted): the first result appears only after the
   * system-time close deadline = 10; it does not change as soon as 20:06(v=20) arrives, and the
   * system-time observation point shows no revision either, until the watermark reaches the
   * lateness line (probe 20:17) and the result is revised to 30.
   */
  @Test
  public void testOPT62SystemTimePath() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      final long base = System.currentTimeMillis() + 2000;
      final long sBase = base + OPT_WINDOW_MS;
      createStream(stmt, "opt62", "opt62s", base, Boolean.TRUE, W_2M, A_5M, C_10M, NONE);
      runSystemTimePath(stmt, sBase, Boolean.TRUE, W_2M, A_5M, C_10M, NONE);
    }
  }

  /** OPT-63 E path (L=true, W=2m, A=5m, C=10m, I=5m): the expectation matches OPT-60 (3 -> 11). */
  @Test
  public void testOPT63EventTimePath() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      final long base = System.currentTimeMillis() + 2000;
      createStream(stmt, "opt63", "opt63e", base, Boolean.TRUE, W_2M, A_5M, C_10M, I_5M);
      runEventTimePath(stmt, base, Boolean.TRUE, W_2M, A_5M);
    }
  }

  /**
   * OPT-63 S path (L=true, W=2m, A=5m, C=10m, I=5m): the system time closes the window at 10; the
   * value does not change as soon as 20:06(v=20) arrives, and is revised to 30 once the idle
   * retention time (5s) after the last late row has elapsed.
   */
  @Test
  public void testOPT63SystemTimePath() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      final long base = System.currentTimeMillis() + 2000;
      final long sBase = base + OPT_WINDOW_MS;
      createStream(stmt, "opt63", "opt63s", base, Boolean.TRUE, W_2M, A_5M, C_10M, I_5M);
      runSystemTimePath(stmt, sBase, Boolean.TRUE, W_2M, A_5M, C_10M, I_5M);
    }
  }

  // -----------------------------------------------------------------------------------------------
  // OPT-64 .. OPT-69: LEFT_CLOSED=false with WATERMARK omitted (OPT-70/71 are A=5m&C=10m from the
  // same group)
  // -----------------------------------------------------------------------------------------------

  /**
   * OPT-64 E path (L=false, W/A/C/I all omitted): the target window (10:00,10:10] is expected to
   * hold 6 (10:05(v=2)+10:10(v=4)); after the 10:10(v=4) write there is still no result, and only
   * 10:10.001(v=0) closes the window; the 10:07(v=16) written afterwards adds no row and causes no
   * revision.
   */
  @Test
  public void testOPT64EventTimePath() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      final long base = System.currentTimeMillis() + 2000;
      createStream(stmt, "opt64", "opt64e", base, Boolean.FALSE, NONE, NONE, NONE, NONE);
      runEventTimePath(stmt, base, Boolean.FALSE, NONE, NONE);
    }
  }

  /**
   * OPT-64 S path (L=false, W/A/C/I all omitted): with C=0 the system time never closes the window,
   * so 10 appears once the event time reaches the close condition (writing 20:10(v=0) still yields
   * no result, and 20:10.001(v=0) closes it); the 20:06(v=20) written afterwards causes no
   * revision.
   */
  @Test
  public void testOPT64SystemTimePath() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      final long base = System.currentTimeMillis() + 2000;
      final long sBase = base + OPT_WINDOW_MS;
      createStream(stmt, "opt64", "opt64s", base, Boolean.FALSE, NONE, NONE, NONE, NONE);
      runSystemTimePath(stmt, sBase, Boolean.FALSE, NONE, NONE, NONE, NONE);
    }
  }

  /**
   * OPT-65 E path (L=false, W/A/C omitted, I=5m): I takes no part in the event-time path, so the
   * expectation matches OPT-64 (6).
   */
  @Test
  public void testOPT65EventTimePath() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      final long base = System.currentTimeMillis() + 2000;
      createStream(stmt, "opt65", "opt65e", base, Boolean.FALSE, NONE, NONE, NONE, I_5M);
      runEventTimePath(stmt, base, Boolean.FALSE, NONE, NONE);
    }
  }

  /**
   * OPT-65 S path (L=false, W/A/C omitted, I=5m): with A=0 the idle timer does not take effect, so
   * the expectation matches OPT-64: the event time closes the window at 10, and the 20:06(v=20)
   * written afterwards causes no revision.
   */
  @Test
  public void testOPT65SystemTimePath() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      final long base = System.currentTimeMillis() + 2000;
      final long sBase = base + OPT_WINDOW_MS;
      createStream(stmt, "opt65", "opt65s", base, Boolean.FALSE, NONE, NONE, NONE, I_5M);
      runSystemTimePath(stmt, sBase, Boolean.FALSE, NONE, NONE, NONE, I_5M);
    }
  }

  /**
   * OPT-66 E path (L=false, W/A/I omitted, C=10m): C takes no part in the event-time path, so the
   * expectation matches OPT-64 (6).
   */
  @Test
  public void testOPT66EventTimePath() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      final long base = System.currentTimeMillis() + 2000;
      createStream(stmt, "opt66", "opt66e", base, Boolean.FALSE, NONE, NONE, C_10M, NONE);
      runEventTimePath(stmt, base, Boolean.FALSE, NONE, NONE);
    }
  }

  /**
   * OPT-66 S path (L=false, W/A/I omitted, C=10m): the first result appears only after the
   * system-time close deadline = 10. With A omitted (=0) that close destroys the window in the same
   * step, so the 20:06(v=20) written at 00:41 lands inside the expired line and is dropped: nothing
   * is revised at the 00:46 observation point, and neither the equal-point probe at 20:10 nor
   * 20:10.001 revives the window - the result stays 10.
   */
  @Test
  public void testOPT66SystemTimePath() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      final long base = System.currentTimeMillis() + 2000;
      final long sBase = base + OPT_WINDOW_MS;
      createStream(stmt, "opt66", "opt66s", base, Boolean.FALSE, NONE, NONE, C_10M, NONE);
      runSystemTimePath(stmt, sBase, Boolean.FALSE, NONE, NONE, C_10M, NONE);
    }
  }

  /**
   * OPT-67 E path (L=false, W/A omitted, C=10m, I=5m): the expectation matches OPT-64 (6).
   *
   * <p>The stream is created with ALLOWED_LATENESS set to 1m (this case's S expectation needs the
   * idle timer to be armed); the E path still follows the case text and stops at the close and
   * 10:07, without entering the retention steps.
   */
  @Test
  public void testOPT67EventTimePath() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      final long base = System.currentTimeMillis() + 2000;
      createStream(stmt, "opt67", "opt67e", base, Boolean.FALSE, NONE, A_1M, C_10M, I_5M);
      runEventTimePath(stmt, base, Boolean.FALSE, NONE, NONE);
    }
  }

  /**
   * OPT-67 S path (L=false, W/A omitted, C=10m, I=5m): the system time closes the window at 10; the
   * value does not change as soon as 20:06(v=20) arrives, and is revised to 30 once the idle
   * retention time (5s) after the last late row has elapsed.
   *
   * <p>ALLOWED_LATENESS is set to 1m to make the idle timer valid (it only takes effect when
   * ALLOWED_LATENESS &gt; 0), matching the case text's "query the revised result within a
   * reasonable scheduling tolerance after the full 5m".
   */
  @Test
  public void testOPT67SystemTimePath() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      final long base = System.currentTimeMillis() + 2000;
      final long sBase = base + OPT_WINDOW_MS;
      createStream(stmt, "opt67", "opt67s", base, Boolean.FALSE, NONE, A_1M, C_10M, I_5M);
      runSystemTimePath(stmt, sBase, Boolean.FALSE, NONE, A_1M, C_10M, I_5M);
    }
  }

  /**
   * OPT-68 E path (L=false, W omitted, A=5m, C/I omitted): the target window (10:00,10:10] first
   * holds 6; the value does not change as soon as 10:06(v=8) arrives; after the retention period
   * ends (writing 10:15(v=0) changes nothing, and 10:15.001(v=0) destroys the window) it is revised
   * to 14; 10:07(v=16) no longer changes the result.
   */
  @Test
  public void testOPT68EventTimePath() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      final long base = System.currentTimeMillis() + 2000;
      createStream(stmt, "opt68", "opt68e", base, Boolean.FALSE, NONE, A_5M, NONE, NONE);
      runEventTimePath(stmt, base, Boolean.FALSE, NONE, A_5M);
    }
  }

  /**
   * OPT-68 S path (L=false, W omitted, A=5m, C/I omitted): no first result during the system-time
   * observation, so 10 appears once the event time reaches the close condition (20:10.001); it does
   * not change as soon as 20:06(v=20) arrives, and the system-time observation point shows no
   * revision either, until the watermark passes the lateness line (writing 20:15(v=0) changes
   * nothing, and 20:15.001(v=0) destroys the window) and the result is revised to 30.
   */
  @Test
  public void testOPT68SystemTimePath() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      final long base = System.currentTimeMillis() + 2000;
      final long sBase = base + OPT_WINDOW_MS;
      createStream(stmt, "opt68", "opt68s", base, Boolean.FALSE, NONE, A_5M, NONE, NONE);
      runSystemTimePath(stmt, sBase, Boolean.FALSE, NONE, A_5M, NONE, NONE);
    }
  }

  /**
   * OPT-69 E path (L=false, W omitted, A=5m, C omitted, I=5m): I takes no part in the event-time
   * path, so the expectation matches OPT-68 (6 -> 14).
   */
  @Test
  public void testOPT69EventTimePath() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      final long base = System.currentTimeMillis() + 2000;
      createStream(stmt, "opt69", "opt69e", base, Boolean.FALSE, NONE, A_5M, NONE, I_5M);
      runEventTimePath(stmt, base, Boolean.FALSE, NONE, A_5M);
    }
  }

  /**
   * OPT-69 S path (L=false, W omitted, A=5m, C omitted, I=5m): the event time closes the window at
   * 10; the value does not change as soon as 20:06(v=20) arrives, and is revised to 30 once the
   * idle retention time (5s) after the last late row has elapsed.
   */
  @Test
  public void testOPT69SystemTimePath() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      final long base = System.currentTimeMillis() + 2000;
      final long sBase = base + OPT_WINDOW_MS;
      createStream(stmt, "opt69", "opt69s", base, Boolean.FALSE, NONE, A_5M, NONE, I_5M);
      runSystemTimePath(stmt, sBase, Boolean.FALSE, NONE, A_5M, NONE, I_5M);
    }
  }

  // -----------------------------------------------------------------------------------------------
  // OPT-70 .. OPT-79 (LEFT_CLOSED=false combination matrix): each case is split into an E and an S
  // method, each of which uses only its own group
  // -----------------------------------------------------------------------------------------------

  /**
   * OPT-70 E path (L=false, W omitted, A=5m, C=10m, I omitted): the target window (10:00,10:10]
   * hand-counts to 2+4 = 6 (10:00(v=1) falls into the previous window under left-open right-closed,
   * whose final result is 1); the 10:06(v=8) written after the close joins the full recomputation
   * once the watermark strictly passes the lateness line 10:15 -> 14; 10:07(v=16) falls inside the
   * expiry line 10:10 and is dropped.
   */
  @Test
  public void testOPT70EventTimePath() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      final long base = System.currentTimeMillis() + 2000;
      createStream(stmt, "opt70", "opt70e", base, Boolean.FALSE, NONE, A_5M, C_10M, NONE);
      runEventTimePath(stmt, base, Boolean.FALSE, NONE, A_5M);
    }
  }

  /**
   * OPT-70 S path (L=false, W omitted, A=5m, C=10m, I omitted): the S window (20:00,20:10] is
   * opened by 20:05(v=10) at system time 00:30, and the close system time is 00:40 (left-open
   * right-closed requires the system time to be strictly greater than that point) -> first result
   * 10; 20:06(v=20) is written at 00:41 as late data and must still not be revised at the 00:46
   * observation point, until the watermark strictly passes the lateness line 20:15 and the window
   * is destroyed and fully recomputed -> 30.
   */
  @Test
  public void testOPT70SystemTimePath() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      final long base = System.currentTimeMillis() + 2000;
      final long sBase = base + OPT_WINDOW_MS;
      createStream(stmt, "opt70", "opt70s", base, Boolean.FALSE, NONE, A_5M, C_10M, NONE);
      runSystemTimePath(stmt, sBase, Boolean.FALSE, NONE, A_5M, C_10M, NONE);
    }
  }

  /**
   * OPT-71 E path (L=false, W omitted, A=5m, C=10m, I=5m): I takes no part in the event-time path,
   * so the steps and the hand-counted oracle match OPT-70 (close 6 -> destroy and recompute 14),
   * which also confirms that MAX_IDLE_WINDOW_RETAIN does not affect an event-time close.
   */
  @Test
  public void testOPT71EventTimePath() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      final long base = System.currentTimeMillis() + 2000;
      createStream(stmt, "opt71", "opt71e", base, Boolean.FALSE, NONE, A_5M, C_10M, I_5M);
      runEventTimePath(stmt, base, Boolean.FALSE, NONE, A_5M);
    }
  }

  /**
   * OPT-71 S path (L=false, W omitted, A=5m, C=10m, I=5m): the system time closes the window at
   * 00:40 -> 10; the 20:06(v=20) of 00:41 is late data, and its receive system time + I (5m, i.e.
   * case mark 00:46) triggers idle destruction and a full recomputation -> 30.
   */
  @Test
  public void testOPT71SystemTimePath() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      final long base = System.currentTimeMillis() + 2000;
      final long sBase = base + OPT_WINDOW_MS;
      createStream(stmt, "opt71", "opt71s", base, Boolean.FALSE, NONE, A_5M, C_10M, I_5M);
      runSystemTimePath(stmt, sBase, Boolean.FALSE, NONE, A_5M, C_10M, I_5M);
    }
  }

  /**
   * OPT-72 E path (L=false, W=2m, A omitted, C omitted, I omitted): the close equal-point probe is
   * mark 10:12 (watermark = 10:10 = the window end, so the window still does not close), and only
   * 10:12.001 closes it -> 6; A=0 makes the lateness line equal the window end, which is already
   * passed at close time -> closing destroys the window, the 10:07(v=16) written afterwards is
   * dropped, and the result stays 6.
   */
  @Test
  public void testOPT72EventTimePath() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      final long base = System.currentTimeMillis() + 2000;
      createStream(stmt, "opt72", "opt72e", base, Boolean.FALSE, W_2M, NONE, NONE, NONE);
      runEventTimePath(stmt, base, Boolean.FALSE, W_2M, NONE);
    }
  }

  /**
   * OPT-72 S path (L=false, W=2m, A omitted, C omitted, I omitted): with C=0 the system time never
   * closes the window, so the target window still has no result at case mark 00:40 and during the
   * observation that follows; only the watermark can close it (mark 20:12 is the equal-point probe
   * and 20:12.001 closes it) -> 10; A=0 makes the window be destroyed at close time, so the
   * 20:06(v=20) written afterwards is dropped and the result stays 10.
   */
  @Test
  public void testOPT72SystemTimePath() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      final long base = System.currentTimeMillis() + 2000;
      final long sBase = base + OPT_WINDOW_MS;
      createStream(stmt, "opt72", "opt72s", base, Boolean.FALSE, W_2M, NONE, NONE, NONE);
      runSystemTimePath(stmt, sBase, Boolean.FALSE, W_2M, NONE, NONE, NONE);
    }
  }

  /**
   * OPT-73 E path (L=false, W=2m, A omitted, C omitted, I=5m): identical to the OPT-72 E path, both
   * the steps and the oracle being close 6, destroy on close, and 10:07(v=16) dropped.
   */
  @Test
  public void testOPT73EventTimePath() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      final long base = System.currentTimeMillis() + 2000;
      createStream(stmt, "opt73", "opt73e", base, Boolean.FALSE, W_2M, NONE, NONE, I_5M);
      runEventTimePath(stmt, base, Boolean.FALSE, W_2M, NONE);
    }
  }

  /**
   * OPT-73 S path (L=false, W=2m, A omitted, C omitted, I=5m): the steps and oracle match OPT-72
   * (10). With A=0 MAX_IDLE_WINDOW_RETAIN does not take effect, and the window is already destroyed
   * at close time, so I=5m does not change the result.
   */
  @Test
  public void testOPT73SystemTimePath() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      final long base = System.currentTimeMillis() + 2000;
      final long sBase = base + OPT_WINDOW_MS;
      createStream(stmt, "opt73", "opt73s", base, Boolean.FALSE, W_2M, NONE, NONE, I_5M);
      runSystemTimePath(stmt, sBase, Boolean.FALSE, W_2M, NONE, NONE, I_5M);
    }
  }

  /**
   * OPT-74 E path (L=false, W=2m, A omitted, C=10m, I omitted): C takes no part in the event-time
   * path, so the steps and oracle match OPT-72: 10:12.001 closes the window -> 6, and A=0 makes the
   * close destroy the window, so 10:07(v=16) is dropped.
   */
  @Test
  public void testOPT74EventTimePath() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      final long base = System.currentTimeMillis() + 2000;
      createStream(stmt, "opt74", "opt74e", base, Boolean.FALSE, W_2M, NONE, C_10M, NONE);
      runEventTimePath(stmt, base, Boolean.FALSE, W_2M, NONE);
    }
  }

  /**
   * OPT-74 S path (L=false, W=2m, A omitted, C=10m, I omitted): the system time closes the window
   * at 00:40 -> 10, and with A omitted (=0) that close destroys the window in the same step, so the
   * 20:06(v=20) written at 00:41 lands inside the expired line and is dropped: nothing is revised
   * at the 00:46 observation point, and neither the equal-point probe at 20:12 nor 20:12.001
   * revives the window - the result stays 10.
   */
  @Test
  public void testOPT74SystemTimePath() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      final long base = System.currentTimeMillis() + 2000;
      final long sBase = base + OPT_WINDOW_MS;
      createStream(stmt, "opt74", "opt74s", base, Boolean.FALSE, W_2M, NONE, C_10M, NONE);
      runSystemTimePath(stmt, sBase, Boolean.FALSE, W_2M, NONE, C_10M, NONE);
    }
  }

  /**
   * OPT-75 E path (L=false, W=2m, A=1m, C=10m, I=5m). Omitting A (=0) the way the case text does
   * would disarm MAX_IDLE_WINDOW_RETAIN, which contradicts the case text ("unchanged within 5m
   * after the late data, a revised result visible after 5m"), so this case sets A to 1m: 10:12.001
   * closes the window -> 6; after the late 10:06(v=8) the lateness-line probe falls on mark 10:13
   * (= window end + A + W) and only strictly passing it destroys the window and fully recomputes ->
   * 14.
   */
  @Test
  public void testOPT75EventTimePath() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      final long base = System.currentTimeMillis() + 2000;
      createStream(stmt, "opt75", "opt75e", base, Boolean.FALSE, W_2M, A_1M, C_10M, I_5M);
      runEventTimePath(stmt, base, Boolean.FALSE, W_2M, A_1M);
    }
  }

  /**
   * OPT-75 S path (L=false, W=2m, A=1m, C=10m, I=5m): A=1m &gt; 0 arms the idle timer, so the
   * system time closes the window at 00:40 -> 10; the 20:06(v=20) of 00:41 is late data and must
   * not be revised within 5m (case mark 00:46); after 5m the idle destruction triggers a full
   * recomputation -> 30.
   */
  @Test
  public void testOPT75SystemTimePath() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      final long base = System.currentTimeMillis() + 2000;
      final long sBase = base + OPT_WINDOW_MS;
      createStream(stmt, "opt75", "opt75s", base, Boolean.FALSE, W_2M, A_1M, C_10M, I_5M);
      runSystemTimePath(stmt, sBase, Boolean.FALSE, W_2M, A_1M, C_10M, I_5M);
    }
  }

  /**
   * OPT-76 E path (L=false, W=2m, A=5m, C omitted, I omitted): 10:12.001 closes the window -> 6;
   * 10:06(v=8) is late data and the lateness-line probe falls on mark 10:17 (= window end + A + W),
   * and only strictly passing it destroys the window and fully recomputes -> 14; 10:07(v=16) falls
   * inside the expiry line and is dropped.
   */
  @Test
  public void testOPT76EventTimePath() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      final long base = System.currentTimeMillis() + 2000;
      createStream(stmt, "opt76", "opt76e", base, Boolean.FALSE, W_2M, A_5M, NONE, NONE);
      runEventTimePath(stmt, base, Boolean.FALSE, W_2M, A_5M);
    }
  }

  /**
   * OPT-76 S path (L=false, W=2m, A=5m, C omitted, I omitted): with C=0 the system time never
   * closes the window, so 20:12.001 closes it by watermark -> 10; after 20:06(v=20) is written at
   * 00:41 the 00:46 observation point must still show no revision, until the watermark strictly
   * passes the lateness line 20:15 (probe 20:17.001) and the window is destroyed and fully
   * recomputed -> 30.
   */
  @Test
  public void testOPT76SystemTimePath() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      final long base = System.currentTimeMillis() + 2000;
      final long sBase = base + OPT_WINDOW_MS;
      createStream(stmt, "opt76", "opt76s", base, Boolean.FALSE, W_2M, A_5M, NONE, NONE);
      runSystemTimePath(stmt, sBase, Boolean.FALSE, W_2M, A_5M, NONE, NONE);
    }
  }

  /**
   * OPT-77 E path (L=false, W=2m, A=5m, C omitted, I=5m): I takes no part in the event-time path,
   * so the steps and oracle match OPT-76 (close 6 -> destroy and recompute 14).
   */
  @Test
  public void testOPT77EventTimePath() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      final long base = System.currentTimeMillis() + 2000;
      createStream(stmt, "opt77", "opt77e", base, Boolean.FALSE, W_2M, A_5M, NONE, I_5M);
      runEventTimePath(stmt, base, Boolean.FALSE, W_2M, A_5M);
    }
  }

  /**
   * OPT-77 S path (L=false, W=2m, A=5m, C omitted, I=5m): 20:12.001 closes the window by event time
   * -> 10; the 20:06(v=20) of 00:41 is late data and must not be revised within 5m (case mark
   * 00:46); after 5m the idle destruction triggers a full recomputation -> 30.
   */
  @Test
  public void testOPT77SystemTimePath() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      final long base = System.currentTimeMillis() + 2000;
      final long sBase = base + OPT_WINDOW_MS;
      createStream(stmt, "opt77", "opt77s", base, Boolean.FALSE, W_2M, A_5M, NONE, I_5M);
      runSystemTimePath(stmt, sBase, Boolean.FALSE, W_2M, A_5M, NONE, I_5M);
    }
  }

  /**
   * OPT-78 E path (L=false, W=2m, A=5m, C=10m, I omitted): C takes no part in the event-time path,
   * so the steps and oracle match OPT-76 (close 6 -> lateness-line probes 10:17/10:17.001 ->
   * destroy and recompute 14).
   */
  @Test
  public void testOPT78EventTimePath() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      final long base = System.currentTimeMillis() + 2000;
      createStream(stmt, "opt78", "opt78e", base, Boolean.FALSE, W_2M, A_5M, C_10M, NONE);
      runEventTimePath(stmt, base, Boolean.FALSE, W_2M, A_5M);
    }
  }

  /**
   * OPT-78 S path (L=false, W=2m, A=5m, C=10m, I omitted): the system time closes the window at
   * 00:40 -> 10; after 20:06(v=20) is written at 00:41 the 00:46 observation point must still show
   * no revision, until the watermark strictly passes the lateness line 20:15 (probe 20:17.001) and
   * the window is destroyed and fully recomputed -> 30.
   */
  @Test
  public void testOPT78SystemTimePath() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      final long base = System.currentTimeMillis() + 2000;
      final long sBase = base + OPT_WINDOW_MS;
      createStream(stmt, "opt78", "opt78s", base, Boolean.FALSE, W_2M, A_5M, C_10M, NONE);
      runSystemTimePath(stmt, sBase, Boolean.FALSE, W_2M, A_5M, C_10M, NONE);
    }
  }

  /**
   * OPT-79 E path (L=false, W=2m, A=5m, C=10m, I=5m): I takes no part in the event-time path, so
   * the steps and oracle match OPT-78 (close 6 -> destroy and recompute 14).
   */
  @Test
  public void testOPT79EventTimePath() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      final long base = System.currentTimeMillis() + 2000;
      createStream(stmt, "opt79", "opt79e", base, Boolean.FALSE, W_2M, A_5M, C_10M, I_5M);
      runEventTimePath(stmt, base, Boolean.FALSE, W_2M, A_5M);
    }
  }

  /**
   * OPT-79 S path (L=false, W=2m, A=5m, C=10m, I=5m): the system time closes the window at 00:40 ->
   * 10; the 20:06(v=20) of 00:41 is late data and must not be revised within 5m; after 5m the idle
   * destruction triggers a full recomputation -> 30.
   */
  @Test
  public void testOPT79SystemTimePath() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      final long base = System.currentTimeMillis() + 2000;
      final long sBase = base + OPT_WINDOW_MS;
      createStream(stmt, "opt79", "opt79s", base, Boolean.FALSE, W_2M, A_5M, C_10M, I_5M);
      runSystemTimePath(stmt, sBase, Boolean.FALSE, W_2M, A_5M, C_10M, I_5M);
    }
  }
}
