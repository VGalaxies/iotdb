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
import org.junit.*;
import org.junit.experimental.categories.Category;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * PERIOD (time driven) event window cases PER-01 .. PER-13, run on a 1C1D1S cluster (1 ConfigNode,
 * 1 DataNode, 1 StreamNode).
 *
 * <p>PERIOD windows are clock driven: {@code PeriodWindowEngine} emits one window per {@code
 * periodMs} regardless of incoming data. Boundaries are anchored at {@code origin + k * period},
 * and the sink {@code time} column carries {@code ${START_TIME}} (the window start). Every timing
 * assertion is therefore written on that value rather than on wall clock arrival time, which makes
 * the expected values exact instead of tolerance based.
 *
 * <p>Every origin is floored down to a whole second at CREATE STREAM analysis time ({@code value -
 * value % 1s}), whatever form it was written in - an integer, a DATETIME or ISO8601 string, or
 * {@code NOW()}; an omitted origin defaults to the analysis-time system clock, floored the same
 * way. The period must be a whole number of seconds, so every window boundary lies on a whole
 * second.
 *
 * <p>Two engine properties drive the design. For a past origin the first window is the smallest
 * {@code origin + k * period} strictly greater than the instant the engine is constructed (which
 * happens on {@code START STREAM}), so the partial window containing that instant is never
 * back-filled. For a future origin {@code T} the first window is {@code [T, T + period)}: its
 * trigger fires at {@code T + period}, while the {@code time} written to the sink is the window
 * start {@code T} itself.
 *
 * <p>Cases whose spec expectation is stricter than the current implementation are marked "Expected
 * RED until ..." in their javadoc.
 */
@Category({StreamClusterIT.class})
public class PeriodStreamIT {

  private static final String DB = "test";

  /** Source table shared by every case; PERIOD streams trigger from the clock, not from data. */
  private static final String SRC_DDL =
      "CREATE TABLE src(time TIMESTAMP TIME, device_id STRING TAG, value FLOAT FIELD)";

  private static final long SECOND = 1000L;
  private static final long MINUTE = 60_000L;
  private static final long HOUR = 3_600_000L;

  /**
   * Lead time used to build a future origin: it must keep the origin in the future when {@code
   * START STREAM} constructs the engine. The {@code CREATE STREAM} + {@code START STREAM} round
   * trip measures about 1.2s locally, so 5s leaves roughly 3.8s of slack.
   */
  private static final long FUTURE_ORIGIN_LEAD_MS = 5000L;

  /**
   * Sub-second remainder carried by an origin that must not be a whole second: PER-05 uses it on an
   * integer origin and PER-10 on the s/m/h periods, and PERIOD floors every origin down to a whole
   * second, so the value stored back must have this remainder removed while the {@code time}
   * written to the sink sits on the floored grid.
   */
  private static final int ORIGIN_REMAINDER_MS = 234;

  private static final Pattern PERIOD_MS_PATTERN = Pattern.compile("periodMs=(-?\\d+)ms");
  private static final Pattern ORIGIN_MS_PATTERN = Pattern.compile("originMs=(-?\\d+)ms");

  /** Default upper bound for waiting until the sink holds the trigger rows. */
  private static final long AWAIT_TIMEOUT_MS = 60_000L;

  /** Extra slack on top of the trigger instant, absorbing engine start and polling jitter. */
  private static final long AWAIT_SLACK_MS = 20_000L;

  private static BaseEnv env() {
    return StreamEnvFactory.getEnv();
  }

  @BeforeClass
  public static void setUp() throws Exception {
    // getEnv(int) must be called before getEnv(): the factory only builds the env on first use.
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
      try {
        statement.execute("DROP DATABASE " + DB);
      } catch (Exception ignored) {
        // database may not exist if a test failed before creating it
      }
    }
  }

  // --------------------------------------------------------------------------------------------
  // Shared helpers
  // --------------------------------------------------------------------------------------------

  private void createDatabase(final Statement statement) throws SQLException {
    statement.execute("CREATE DATABASE " + DB);
    statement.execute("USE " + DB);
  }

  /** Creates the source table plus a sink holding the trigger timestamp and the row count. */
  private void createSourceAndSink(final Statement statement, final String sinkTable)
      throws SQLException {
    statement.execute(SRC_DDL);
    createSink(statement, sinkTable);
  }

  /**
   * Creates only the sink table. Use this when a case needs several independent sinks (one per
   * stream): the source table is then created once through {@link #createSourceAndSink}.
   */
  private void createSink(final Statement statement, final String sinkTable) throws SQLException {
    statement.execute("CREATE TABLE " + sinkTable + "(time TIMESTAMP TIME, event_cnt INT64 FIELD)");
  }

  /**
   * Builds a PERIOD stream whose sink {@code time} column is the window start. {@code windowArgs}
   * is spliced verbatim between {@code PERIOD(} and {@code )}, e.g. {@code "1s"} or {@code "3s,
   * 1757900000001"}.
   */
  private static String periodStreamSql(
      final String streamName, final String windowArgs, final String sinkTable) {
    return "CREATE STREAM "
        + streamName
        + " PERIOD("
        + windowArgs
        + ") INTO "
        + sinkTable
        + "(time, event_cnt) SELECT ${START_TIME}, count(*) FROM src";
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
                + "*"
                + " FROM information_schema.streams WHERE stream_name = '"
                + streamName
                + "'")) {
      return rs.next() ? rs.getString(column) : null;
    }
  }

  /**
   * Asserts that executing {@code sql} fails. When {@code expectedSubstring} is non-null, the
   * failure message must contain it; otherwise any failure is accepted.
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

  /**
   * Reads the earliest {@code ${START_TIME}} written into {@code sinkTable} through {@code SELECT
   * min(time)}, i.e. the first window boundary the stream actually published. Returns {@code null}
   * while the sink holds no trigger row: a global aggregate then yields either no row at all or a
   * single SQL NULL row.
   *
   * <p>The value is read by index on purpose - the table model labels an unaliased aggregate {@code
   * _col0}, so a name based lookup would not resolve.
   */
  private Long readEarliestSinkTime(final Statement statement, final String sinkTable)
      throws SQLException {
    try (ResultSet rs = statement.executeQuery("SELECT min(time) FROM " + sinkTable)) {
      if (!rs.next()) {
        return null;
      }
      final long earliest = rs.getLong(1);
      // A NULL can also surface as 0 when the aggregate is materialized into the result's time
      // column, and a real trigger never sits at epoch 0.
      return rs.wasNull() || earliest <= 0 ? null : earliest;
    }
  }

  /** Reads the {@code ${START_TIME}} values written into the sink, ascending. */
  private List<Long> readSinkTimes(final Statement statement, final String sinkTable)
      throws SQLException {
    final List<Long> times = new ArrayList<>();
    final List<Long> event_cnt = new ArrayList<>();
    try (ResultSet rs = statement.executeQuery("SELECT * FROM " + sinkTable)) {
      while (rs.next()) {
        times.add(rs.getLong("time"));
        event_cnt.add(rs.getLong("event_cnt"));
      }
    }
    Collections.sort(times);
    return times;
  }

  /**
   * Waits until the sink holds at least {@code count} trigger rows, then returns the first ones.
   */
  private List<Long> awaitFirstTriggers(
      final Statement statement, final String sinkTable, final int count) throws SQLException {
    return awaitFirstTriggers(statement, sinkTable, count, AWAIT_TIMEOUT_MS);
  }

  /**
   * Waits until the sink holds at least {@code count} trigger rows, then returns the first ones.
   *
   * <p>The second trigger of a minute level period needs a full period, so callers widen {@code
   * timeoutMs} for those.
   */
  private List<Long> awaitFirstTriggers(
      final Statement statement, final String sinkTable, final int count, final long timeoutMs)
      throws SQLException {
    awaitWithin(timeoutMs)
        .untilAsserted(
            () ->
                assertTrue(
                    "sink "
                        + sinkTable
                        + " should have at least "
                        + count
                        + " periodic trigger rows",
                    readSinkTimes(statement, sinkTable).size() >= count));
    final List<Long> times = readSinkTimes(statement, sinkTable);
    assertTrue(
        "sink " + sinkTable + " should hold at least " + count + " rows", times.size() >= count);
    return new ArrayList<>(times.subList(0, count));
  }

  private static ConditionFactory awaitWithin(final long timeoutMs) {
    return Awaitility.await()
        .pollInSameThread()
        .pollDelay(500, TimeUnit.MILLISECONDS)
        .pollInterval(500, TimeUnit.MILLISECONDS)
        .atMost(timeoutMs, TimeUnit.MILLISECONDS);
  }

  /** Reads the event window metadata recorded for a stream, e.g. {@code PeriodWindow{...}}. */
  private String eventWindowOf(final Statement statement, final String streamName)
      throws SQLException {
    final String eventWindow = getStreamColumn(statement, streamName, "event_window");
    assertNotNull("event_window should be recorded for " + streamName, eventWindow);
    return eventWindow;
  }

  /**
   * Reads the persisted window anchor {@code originMs} of a stream. PERIOD freezes the origin at
   * CREATE STREAM analysis time (a defaulted origin is the analysis-time system clock), so this is
   * the value every trigger grid assertion must be anchored on - not the stream start instant.
   */
  private long readOriginMillis(final Statement statement, final String streamName)
      throws SQLException {
    return extractMillis(eventWindowOf(statement, streamName), "originMs");
  }

  private static long extractMillis(final String eventWindow, final String key) {
    final Matcher matcher =
        ("periodMs".equals(key) ? PERIOD_MS_PATTERN : ORIGIN_MS_PATTERN)
            .matcher(eventWindow == null ? "" : eventWindow);
    assertTrue("cannot find '" + key + "' in event_window: " + eventWindow, matcher.find());
    return Long.parseLong(matcher.group(1));
  }

  private void assertEventWindow(
      final Statement statement,
      final String streamName,
      final long expectedPeriodMs,
      final long expectedOriginMs)
      throws SQLException {
    final String eventWindow = eventWindowOf(statement, streamName);
    assertEquals(
        "unexpected periodMs in " + eventWindow,
        expectedPeriodMs,
        extractMillis(eventWindow, "periodMs"));
    assertEquals(
        "unexpected originMs in " + eventWindow,
        expectedOriginMs,
        extractMillis(eventWindow, "originMs"));
  }

  /** Asserts the {@code periodMs} recorded for a stream, ignoring the (defaulted) origin. */
  private void assertPeriodMs(
      final Statement statement, final String streamName, final long expectedPeriodMs)
      throws SQLException {
    final String eventWindow = eventWindowOf(statement, streamName);
    assertEquals(
        "unexpected periodMs in " + eventWindow,
        expectedPeriodMs,
        extractMillis(eventWindow, "periodMs"));
  }

  /**
   * Asserts that the origin recorded for a stream is a whole second, i.e. the form every PERIOD
   * origin is floored to - including a defaulted one (the analysis-time system clock).
   */
  private void assertOriginAlignedToSecond(final Statement statement, final String streamName)
      throws SQLException {
    final String eventWindow = eventWindowOf(statement, streamName);
    assertEquals(
        "the origin should be a whole second in " + eventWindow,
        0L,
        Math.floorMod(extractMillis(eventWindow, "originMs"), SECOND));
  }

  /** Truncates a timestamp down to a multiple of {@code unit}. */
  private static long floorToUnit(final long timestamp, final long unit) {
    return timestamp - Math.floorMod(timestamp, unit);
  }

  /** Returns the smallest epoch millis strictly greater than {@code base} ending in {@code rem}. */
  private static long nextAlignedEpochMillis(final long base, final int remainderMillis) {
    final long candidate = floorToUnit(base, SECOND) + remainderMillis;
    return candidate > base ? candidate : candidate + SECOND;
  }

  /**
   * Returns the smallest whole second strictly greater than {@code base} that does not land on the
   * {@code periodMs} grid, when such a second exists. A whole second can never be off a 1s grid, so
   * that case returns the floored value unchanged.
   */
  private static long nextOffGridWholeSecond(final long base, final long periodMs) {
    final long candidate = nextAlignedEpochMillis(base, 0);
    return periodMs <= SECOND || Math.floorMod(candidate, periodMs) != 0
        ? candidate
        : candidate + SECOND;
  }

  /**
   * Formats {@code epochMillis} as an offset date time, e.g. {@code 2026-09-15T10:23:45.001+08:00}.
   */
  private static String isoOffset(final long epochMillis, final ZoneOffset offset) {
    return DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSXXX")
        .withZone(offset)
        .format(Instant.ofEpochMilli(epochMillis));
  }

  /** Formats {@code epochMillis} as ISO8601 UTC, e.g. {@code 2026-09-15T02:23:45.001Z}. */
  private static String isoUtc(final long epochMillis) {
    return DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
        .withZone(ZoneOffset.UTC)
        .format(Instant.ofEpochMilli(epochMillis));
  }

  /** Asserts that every trigger sits on the {@code origin + k * period} grid, one period apart. */
  private void assertAlignedSeries(final List<Long> series, final long origin, final long period) {
    assertFalse("the trigger series should not be empty", series.isEmpty());
    for (int i = 0; i < series.size(); i++) {
      assertEquals(
          "trigger " + i + " should stay on the origin anchored grid",
          0L,
          Math.floorMod(series.get(i) - origin, period));
    }
    for (int i = 1; i < series.size(); i++) {
      assertEquals(
          "consecutive triggers should be exactly one period apart",
          period,
          (long) (series.get(i) - series.get(i - 1)));
    }
  }

  // --------------------------------------------------------------------------------------------
  // PER-01 .. PER-13
  // --------------------------------------------------------------------------------------------

  /**
   * PER-01 without origin: the origin defaults to the system time at {@code CREATE STREAM} analysis
   * time, floored down to a whole second, and triggers then continue once per period on that
   * anchor.
   *
   * <p>The anchor is the recorded {@code originMs}, not the stream start instant: the criteria are
   * that the defaulted origin is a whole second inside the {@code CREATE STREAM} statement's own
   * execution interval, that every trigger sits on the {@code origin + n * period} grid, and that
   * the first trigger is the origin itself or the window right after it. {@code START STREAM} runs
   * a moment after the origin was frozen, so the wall clock may already have moved past the
   * origin's second by then.
   */
  @Test
  public void testPER01PeriodWithoutOrigin() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      createDatabase(stmt);
      createSourceAndSink(stmt, "sink_01");

      final long period = 3 * SECOND;
      final long createBefore = System.currentTimeMillis();
      stmt.execute(periodStreamSql("per01", "3s", "sink_01"));
      final long createAfter = System.currentTimeMillis();

      // The defaulted origin is the CREATE STREAM execution instant, aligned down to the second.
      assertOriginAlignedToSecond(stmt, "per01");
      final long originMs = readOriginMillis(stmt, "per01");
      assertTrue(
          "the defaulted origin "
              + originMs
              + " should be the CREATE STREAM execution instant, in ["
              + (createBefore - SECOND)
              + ", "
              + (createAfter)
              + "]",
          originMs >= createBefore - SECOND && originMs <= createAfter);

      stmt.execute("START STREAM per01");
      awaitFirstTriggers(stmt, "sink_01", 3);
      final List<Long> triggers = readSinkTimes(stmt, "sink_01");

      // Every trigger sits on the origin anchored grid, exactly one period apart.
      assertAlignedSeries(triggers, originMs, period);

      // The engine is constructed one short moment after the origin was frozen, so the first window
      // is the origin itself or the one right after it.
      final long first = triggers.get(0);
      assertTrue(
          "the first trigger "
              + first
              + " should be the origin "
              + originMs
              + " or origin + period "
              + (originMs + period),
          first == originMs || first == originMs + period);
    }
  }

  /**
   * PER-02 origin later than the current time: the first window is {@code [origin, origin +
   * period)}, so the first {@code ${START_TIME}} written into the sink is the origin, then origin +
   * n * period.
   */
  @Test
  public void testPER02FutureOrigin() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      createDatabase(stmt);
      createSourceAndSink(stmt, "sink_02");

      final long tBefore = System.currentTimeMillis();
      // A whole second: PERIOD stores a sub-second origin only after flooring it, so an origin that
      // already sits on a second boundary is stored verbatim and the expectation stays exact.
      final long t = nextAlignedEpochMillis(tBefore + FUTURE_ORIGIN_LEAD_MS, 0);
      stmt.execute(periodStreamSql("per02", "3s, " + t, "sink_02"));
      assertEventWindow(stmt, "per02", 3 * SECOND, t);

      stmt.execute("START STREAM per02");
      assertEquals(
          "the first three triggers should be origin, origin + 3s and origin + 6s",
          Arrays.asList(t, t + 3 * SECOND, t + 6 * SECOND),
          awaitFirstTriggers(stmt, "sink_02", 3));
    }
  }

  /**
   * PER-04 historical origin: historical periods are not backfilled; the first window is the
   * smallest {@code origin + n * period} strictly later than the engine construction instant.
   *
   * <p>The grid is anchored on the recorded {@code originMs}, which is also asserted to be the
   * one-hour-back origin stated in the DDL. The engine is constructed while {@code START STREAM}
   * runs, so the construction instant lies inside {@code [streamStartBefore, streamStartAfter]};
   * the first window is the first grid point strictly later than that instant, hence {@code
   * streamStartBefore <= earliest <= streamStartAfter + period}. Back-filling the history would
   * publish one window per period from the origin, about 720 rows for the hour that lies behind an
   * origin of an hour ago with a 5s period - both the earliest bound and the row count upper bound
   * below catch that, by more than an order of magnitude, while the diagnostic records the real
   * series for failures.
   */
  @Test
  public void testPER04HistoricalOriginNoBackfill() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      createDatabase(stmt);
      createSourceAndSink(stmt, "sink_04");

      final long period = 5 * SECOND;

      // Grid anchor, taken before the stream is created and floored to a whole second: the origin
      // is
      // one hour earlier, so it is a whole second too and PERIOD stores it verbatim.
      final long anchor = floorToUnit(System.currentTimeMillis(), SECOND);
      final long origin = anchor - HOUR;

      final long createStart = System.currentTimeMillis();
      stmt.execute(periodStreamSql("per04", "5s, " + origin, "sink_04"));
      final long createEnd = System.currentTimeMillis();

      assertEventWindow(stmt, "per04", period, origin);
      final long originMs = readOriginMillis(stmt, "per04");

      // START STREAM constructs the window engine, so the construction instant lies inside this
      // interval.
      final long streamStartBefore = System.currentTimeMillis();
      stmt.execute("START STREAM per04");
      final long streamStartAfter = System.currentTimeMillis();

      // Wait for at least three triggers to confirm a continuous series, then read the real times.
      final List<Long> triggers = awaitFirstTriggers(stmt, "sink_04", 3);
      assertAlignedSeries(triggers, originMs, period);

      final Long earliest = readEarliestSinkTime(stmt, "sink_04");
      assertNotNull(
          "sink_04 should already hold a trigger row, but min(time) returned NULL", earliest);
      final int rows = readSinkTimes(stmt, "sink_04").size();
      final String diagnostic =
          "earliest="
              + earliest
              + " ("
              + isoUtc(earliest)
              + "), anchor="
              + anchor
              + " ("
              + isoUtc(anchor)
              + "), earliest - anchor="
              + (earliest - anchor)
              + "ms, originMs="
              + originMs
              + " ("
              + isoUtc(originMs)
              + "), createStart="
              + createStart
              + " ("
              + isoUtc(createStart)
              + "), createEnd="
              + createEnd
              + " (create latency "
              + (createEnd - createStart)
              + "ms), period="
              + period
              + "ms, streamStart=["
              + streamStartBefore
              + ", "
              + streamStartAfter
              + "], earliest - streamStartBefore="
              + (earliest - streamStartBefore)
              + "ms, earliest - (streamStartAfter + period)="
              + (earliest - (streamStartAfter + period))
              + "ms, rows="
              + rows;

      // The first window is the first grid point strictly later than the construction instant,
      // which lies inside [streamStartBefore, streamStartAfter]: it cannot precede the stream start
      // and cannot sit more than one period after the engine was built.
      assertTrue(
          "the first window should not predate the stream start: earliest - streamStartBefore = "
              + (earliest - streamStartBefore)
              + "ms; "
              + diagnostic,
          earliest >= streamStartBefore);
      assertTrue(
          "the first window should fall within one period after the stream start: earliest - "
              + "(streamStartAfter + period) = "
              + (earliest - (streamStartAfter + period))
              + "ms; "
              + diagnostic,
          earliest <= streamStartAfter + period);

      // Historical windows are not back-filled: with a 5s period the hour behind the origin would
      // publish ~720 rows, so a series of a few rows proves only the windows after the stream
      // started were emitted.
      assertTrue(
          "back-filling the hour behind the origin would publish ~720 rows, but the sink held "
              + rows
              + "; "
              + diagnostic,
          rows <= 6);
    }
  }

  /**
   * PER-05 Integer TimeValue: an integer timestamp given at database precision (ms by default) is
   * parsed as an epoch millis value and floored down to a whole second, and the stream then
   * triggers on the floored grid.
   */
  @Test
  public void testPER05IntegerTimeValueOrigin() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      createDatabase(stmt);
      createSourceAndSink(stmt, "sink_05");

      final long createStart = System.currentTimeMillis();
      // A whole second plus ORIGIN_REMAINDER_MS: the server must store floor(origin) and anchor the
      // grid there, not keep the sub-second remainder.
      final long originFloored = floorToUnit(createStart - 5 * SECOND, SECOND);
      final long origin = originFloored + ORIGIN_REMAINDER_MS;
      stmt.execute(periodStreamSql("per05", "1s, " + origin, "sink_05"));

      // An integer origin is used as an epoch millis value at ms precision, then floored.
      assertEventWindow(stmt, "per05", SECOND, originFloored);
      final long originMs = readOriginMillis(stmt, "per05");

      stmt.execute("START STREAM per05");
      final List<Long> times = awaitFirstTriggers(stmt, "sink_05", 3);
      assertAlignedSeries(times, originMs, SECOND);
      for (final long trigger : times) {
        assertEquals(
            "every trigger should sit on a whole second once the origin was floored",
            0L,
            Math.floorMod(trigger, SECOND));
      }
    }
  }

  /**
   * PER-06 DateTime origin and origin flooring: a future DateTime origin written with a .001
   * millisecond part must be floored down to the whole second T, and the first three {@code
   * ${START_TIME}} values written into the sink must then be T, T + 3s and T + 6s, all on whole
   * seconds.
   */
  @Test
  public void testPER06DateTimeOriginWithMillisRemainder() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      createDatabase(stmt);
      createSourceAndSink(stmt, "sink_06");

      final long tBefore = System.currentTimeMillis();
      // The whole second the origin is expected to be floored to; the statement writes it with a
      // .001 millisecond part.
      final long t = nextAlignedEpochMillis(tBefore + FUTURE_ORIGIN_LEAD_MS, 0);
      // An unquoted DATETIME_VALUE carries an explicit +08:00 offset, so the parsed instant does
      // not depend on the session time zone.
      final String origin = isoOffset(t + 1, ZoneOffset.ofHours(8));
      stmt.execute(periodStreamSql("per06", "3s, " + origin, "sink_06"));

      assertEventWindow(stmt, "per06", 3 * SECOND, t);

      stmt.execute("START STREAM per06");
      final List<Long> times = awaitFirstTriggers(stmt, "sink_06", 3);
      // Should the start latency push the first trigger onto the next grid point, relax the line
      // below to: assertTrue(times.get(0) == t || times.get(0) == t + 3 * SECOND)
      assertEquals(
          "the first three triggers should be T, T + 3s and T + 6s",
          Arrays.asList(t, t + 3 * SECOND, t + 6 * SECOND),
          times);
      for (final long trigger : times) {
        assertEquals(
            "every trigger should sit on the whole second the origin was floored to",
            0L,
            Math.floorMod(trigger, SECOND));
      }
    }
  }

  /**
   * PER-08 NOW(): uses the server time at statement execution, floored down to a whole second.
   *
   * <p>Criteria:
   *
   * <ul>
   *   <li>the {@code originMs} recorded in the stream's {@code event_window} is a whole second;
   *   <li>that {@code originMs} falls inside the {@code CREATE STREAM} statement's own execution
   *       interval, widened by one second on both sides: {@code NOW()} is folded into a literal at
   *       parse time, so the anchor follows the statement execution instant, not the stream start;
   *   <li>the trigger series the stream writes into the sink follows the {@code originMs + n *
   *       period} grid, one period apart - a past origin is never back-filled onto the grid in
   *       front of it.
   * </ul>
   */
  @Test
  public void testPER08NowOrigin() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      createDatabase(stmt);
      createSourceAndSink(stmt, "sink_08");

      final long period = 3 * SECOND;

      // The CREATE STREAM statement's own execution interval: NOW() is folded into a literal at
      // parse time, so the origin is anchored here rather than at the stream start.
      final long createBefore = System.currentTimeMillis();
      stmt.execute(periodStreamSql("per08", "3s, NOW()", "sink_08"));
      final long createAfter = System.currentTimeMillis();
      assertStreamExists(stmt, "per08");

      // The event_window stream info: periodMs is the configured value, originMs is the origin
      // recorded by NOW(), which is the anchor every grid assertion below is built on.
      final String eventWindow = eventWindowOf(stmt, "per08");
      assertPeriodMs(stmt, "per08", period);
      final long originMs = readOriginMillis(stmt, "per08");

      // Stream start interval: before START STREAM -> after it returns.
      Thread.sleep(2000);
      final long streamStartBefore = System.currentTimeMillis();
      stmt.execute("START STREAM per08");
      final long streamStartAfter = System.currentTimeMillis();

      // The trigger series written by the stream, ascending.
      final List<Long> times = awaitFirstTriggers(stmt, "sink_08", 3);

      final String diagnostic =
          "originMs="
              + originMs
              + " ("
              + isoUtc(originMs)
              + "), times="
              + times
              + ", streamStart=["
              + streamStartBefore
              + ", "
              + streamStartAfter
              + "], createStatement=["
              + createBefore
              + ", "
              + createAfter
              + "], period="
              + period
              + "ms, eventWindow="
              + eventWindow;

      // (a) The origin recorded by NOW() is floored down to a whole second.
      assertEquals(
          "the origin recorded by NOW() should be a whole second: " + diagnostic,
          0L,
          Math.floorMod(originMs, SECOND));

      // (b) The origin is anchored at the statement execution instant, not at the stream start.
      assertTrue(
          "the origin should fall within the CREATE STREAM execution interval: " + diagnostic,
          originMs >= createBefore - SECOND && originMs <= createAfter);

      // (c) A past origin is never back-filled: every trigger sits on the origin anchored grid, one
      //     period apart.
      assertAlignedSeries(times, originMs, period);
    }
  }

  /**
   * PER-09 invalid origin: values that are unparsable, out of range or not a TimeValue must fail
   * the creation and leave no task behind.
   *
   * <p>The non-TimeValue sub case is Expected RED until {@code EventWindow#parseOrigin} rejects
   * argument types outside {@code Integer | DateTime | ISO8601 | NOW()}; a numeric literal such as
   * {@code 1.5} currently degrades to {@code originMs = 0} without any error.
   */
  @Test
  public void testPER09InvalidOrigin() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      createDatabase(stmt);
      createSourceAndSink(stmt, "sink_09");

      // (a) unparsable datetime string
      assertCreateFails(
          stmt,
          periodStreamSql("per09a", "1h, 'c1'", "sink_09"),
          "Invalid argument type for period event window");
      assertStreamAbsent(stmt, "per09a");

      // (b) out of range datetime
      assertCreateFails(
          stmt,
          periodStreamSql("per09b", "1h, '2025-13-45T25:61:61'", "sink_09"),
          "Invalid argument type for period event window");
      assertStreamAbsent(stmt, "per09b");

      // (c) empty string
      assertCreateFails(stmt, periodStreamSql("per09c", "1h, ''", "sink_09"), null);
      assertStreamAbsent(stmt, "per09c");

      // (d) a value that fits no TimeValue alternative at all
      assertCreateFails(stmt, periodStreamSql("per09d", "1h, 1.5", "sink_09"), null);
      assertStreamAbsent(stmt, "per09d");
    }
  }

  /**
   * PER-10 second, minute and hour periods: streams are created with 1s, 1m and 1h, each origin
   * carrying a remainder of a unit finer than the period unit - it is never a whole second, and for
   * the minute and hour periods it is never a whole period either - and every {@code time} written
   * back to the sink must satisfy {@code time = recordedOrigin + n * period}.
   *
   * <p>Each segment's origin comes from {@link #startOffGridFutureOriginStream}: the instant before
   * the stream is created plus {@link #FUTURE_ORIGIN_LEAD_MS}, floored to a whole second, pushed
   * off the period grid by at most one second when such a second exists, and finally incremented by
   * {@link #ORIGIN_REMAINDER_MS}. PERIOD only floors the origin down to a whole second, so what it
   * records is that whole-second part: the sub-second remainder is dropped, while the second the
   * minute and hour periods carry on top of their grid stays. Asserting the recorded value keeps
   * both halves honest - a server that kept the sub-second remainder, or one that snapped the
   * anchor to the period boundary instead of only flooring it to the second, would move it.
   *
   * <p>The three segments differ in granularity, because a row carries the window start but is only
   * written once its window closes:
   *
   * <ul>
   *   <li>1s: the lead time (about 5s) exceeds one period, so the origin is still in the future
   *       when the engine is constructed and the first window is {@code [origin, origin + period)}
   *       - the first row equals the recorded origin, and several rows can be collected. The 1s
   *       grid is covered by the sub-second remainder alone, so the grid and the spacing are
   *       asserted;
   *   <li>1m: likewise a future origin, so the first row must equal the recorded origin exactly,
   *       which still sits off the minute grid; waiting for two rows also checks that the second
   *       row is exactly one period after the first, but the first row is only written one period
   *       after the origin, so the wait spans two periods;
   *   <li>1h: an hour long wait is out of reach of a test, so only the recorded metadata is checked
   *       - the stream is created and its {@code periodMs} / {@code originMs} are read back, the
   *       latter still off the hour grid - but it is never started.
   * </ul>
   */
  @Test
  public void testPER10SecondMinuteHourAlignment() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      createDatabase(stmt);
      createSourceAndSink(stmt, "sink_10_s");
      createSink(stmt, "sink_10_m");
      createSink(stmt, "sink_10_h");

      // (a) second period: the origin written into the statement is not a whole second, so the
      //     flooring rule drops its sub-second part; wait for three triggers and assert they all
      //     sit on the recorded origin grid.
      final long originSecond =
          startOffGridFutureOriginStream(stmt, "per10_s", "sink_10_s", "1s", SECOND);
      final List<Long> secondTriggers = awaitFirstTriggers(stmt, "sink_10_s", 3, AWAIT_TIMEOUT_MS);
      assertAlignedSeries(secondTriggers, originSecond, SECOND);

      // (b) minute period: the first row is the recorded origin, which still sits off the minute
      //     grid, and the second is origin + 1m. A row is written when its window closes, so the
      //     second row lands two periods after the origin.
      final long originMinute =
          startOffGridFutureOriginStream(stmt, "per10_m", "sink_10_m", "1m", MINUTE);
      assertFirstTriggerIsOrigin(
          stmt, "per10_m", "sink_10_m", originMinute, MINUTE, 2, 3 * MINUTE + AWAIT_SLACK_MS);

      // (c) hour period: waiting an hour is out of reach, so only the recorded metadata is checked
      //     (inside the helper, including that the origin still sits off the hour grid) and the
      //     stream is never started.
      createOffGridFutureOriginStream(stmt, "per10_h", "sink_10_h", "1h", HOUR);
      assertStreamExists(stmt, "per10_h");
    }
  }

  /**
   * Creates a stream whose origin lies in the future and carries a remainder of a unit finer than
   * the period unit; asserts the recorded metadata and returns the recorded origin.
   *
   * <p>The baseline is the instant before the stream is created plus {@link
   * #FUTURE_ORIGIN_LEAD_MS}, floored to a whole second and, when the period allows it, shifted by
   * at most one second so that this whole-second part never lands on the period grid. The statement
   * then writes that whole second plus {@link #ORIGIN_REMAINDER_MS}, so the origin is never a whole
   * second and, for the minute and hour periods, never a whole period either.
   *
   * <p>PERIOD floors the origin down to a whole second, so the recorded value is the whole-second
   * part alone: the sub-second remainder is dropped while the coarser one survives. A server that
   * kept the sub-second remainder, or one that snapped the anchor to the period boundary instead of
   * only flooring it to the second, is rejected by the assertions below.
   */
  private long createOffGridFutureOriginStream(
      final Statement statement,
      final String streamName,
      final String sinkTable,
      final String periodLiteral,
      final long periodMs)
      throws SQLException {
    final long createBefore = System.currentTimeMillis();
    final long originFloored =
        nextOffGridWholeSecond(createBefore + FUTURE_ORIGIN_LEAD_MS, periodMs);
    final long origin = originFloored + ORIGIN_REMAINDER_MS;
    assertTrue(
        "the origin "
            + origin
            + " should not be a whole second, but its sub-second remainder was "
            + Math.floorMod(origin, SECOND),
        Math.floorMod(origin, SECOND) == ORIGIN_REMAINDER_MS);
    if (periodMs > SECOND) {
      assertTrue(
          "the origin "
              + origin
              + " should not land on the "
              + periodLiteral
              + " grid, but its whole-second part "
              + originFloored
              + " had remainder "
              + Math.floorMod(originFloored, periodMs),
          Math.floorMod(originFloored, periodMs) != 0);
    }
    // For a 1s period no whole second is off the grid, so the sub-second remainder is the only form
    // the finer unit can take there.

    statement.execute(periodStreamSql(streamName, periodLiteral + ", " + origin, sinkTable));
    // The sub-second remainder must be gone and the coarser one must survive.
    assertEventWindow(statement, streamName, periodMs, originFloored);
    // Hand back the persisted anchor: every grid assertion of the caller is anchored on it.
    final long originMs = readOriginMillis(statement, streamName);
    if (periodMs > SECOND) {
      assertTrue(
          "the recorded origin "
              + originMs
              + " should still sit off the "
              + periodLiteral
              + " grid, but its remainder was "
              + Math.floorMod(originMs, periodMs),
          Math.floorMod(originMs, periodMs) != 0);
    }
    return originMs;
  }

  /** Creates and starts such a stream, returning its recorded origin. */
  private long startOffGridFutureOriginStream(
      final Statement statement,
      final String streamName,
      final String sinkTable,
      final String periodLiteral,
      final long periodMs)
      throws SQLException {
    final long origin =
        createOffGridFutureOriginStream(statement, streamName, sinkTable, periodLiteral, periodMs);
    statement.execute("START STREAM " + streamName);
    return origin;
  }

  /**
   * Checks a segment whose period is longer than the test time limit: the stream carries a future
   * recorded origin, so its first window is {@code [origin, origin + period)} and the first row
   * written back to the sink must equal that recorded origin exactly; once {@code triggerCount}
   * rows are present, all of them must sit on the origin grid and be exactly one period apart.
   */
  private void assertFirstTriggerIsOrigin(
      final Statement statement,
      final String streamName,
      final String sinkTable,
      final long origin,
      final long period,
      final int triggerCount,
      final long timeoutMs)
      throws SQLException {
    final List<Long> triggers = awaitFirstTriggers(statement, sinkTable, triggerCount, timeoutMs);
    assertEquals(
        "the first ${START_TIME} written back should be the origin itself",
        origin,
        (long) triggers.get(0));
    assertAlignedSeries(triggers, origin, period);
  }

  /**
   * PER-11 mixing month and non-month units: a {@code period_time} containing a month unit (y/mo)
   * must not also carry a non-month unit - such forms are ambiguous and must fail the creation
   * without leaving a task behind; pure non-month units may be mixed, but the unit segments must be
   * separated by spaces. Note that {@code m} is minutes and {@code mo} is months, so {@code 1d 1m}
   * is 1 day 1 minute and legal, while {@code 1mo 1d} is not.
   *
   * <p>The three illegal forms are rejected at different layers:
   *
   * <ul>
   *   <li>{@code 1mo 1d}: passes the grammar and is stopped semantically by {@code
   *       EventWindow.validateMonthNonMonthMixing};
   *   <li>{@code 1d1h} / {@code 1h1d}: rejected at the lexical layer. {@code RelationalSql.g4}
   *       enables {@code caseInsensitive} and {@code IDENTIFIER} is {@code (LETTER | '_') (LETTER |
   *       DIGIT | '_')*}, so {@code d1h} matches as a whole IDENTIFIER (3 characters) and beats
   *       {@code DAY}'s {@code 'D'} (1 character); the token stream no longer matches {@code
   *       timeDuration}. A space blocks that swallowing, hence the requirement to separate unit
   *       segments;
   *   <li>{@code 1d+1h}: {@code +} is not a legal connector for {@code timeDuration}, so the
   *       grammar layer rejects it.
   * </ul>
   */
  @Test
  public void testPER11MonthNonMonthMixing() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      createDatabase(stmt);
      createSourceAndSink(stmt, "sink_11");

      // (1) month + non-month: ambiguous, must be rejected
      assertCreateFails(
          stmt,
          periodStreamSql("per11a", "1mo 1d", "sink_11"),
          "Simultaneous setting of monthly and non-monthly intervals is not supported.");
      assertStreamAbsent(stmt, "per11a");

      // (4) no space between the units: d1h is swallowed as an IDENTIFIER, so the token stream no
      //     longer matches timeDuration
      assertCreateFails(stmt, periodStreamSql("per11d", "1d1h", "sink_11"), null);
      assertStreamAbsent(stmt, "per11d");

      // (5) '+' is not a connector for timeDuration: rejected at the grammar layer
      assertCreateFails(stmt, periodStreamSql("per11e", "1d+1h", "sink_11"), null);
      assertStreamAbsent(stmt, "per11e");

      // (6) the same unit run in reverse order: same as (4)
      assertCreateFails(stmt, periodStreamSql("per11f", "1h1d", "sink_11"), null);
      assertStreamAbsent(stmt, "per11f");

      // (2) pure non-month mix: legal, periodMs = 24h + 1h. The origin is omitted, so it defaults
      // to
      //     the analysis-time system clock, floored down to a whole second.
      createSink(stmt, "sink_11b");
      stmt.execute(periodStreamSql("per11b", "1d 1h", "sink_11b"));
      assertStreamExists(stmt, "per11b");
      assertPeriodMs(stmt, "per11b", 24 * HOUR + HOUR);
      assertOriginAlignedToSecond(stmt, "per11b");

      // (3) pure non-month mix: legal, periodMs = 24h + 1m (m is minutes)
      createSink(stmt, "sink_11c");
      stmt.execute(periodStreamSql("per11c", "1d 1m", "sink_11c"));
      assertStreamExists(stmt, "per11c");
      assertPeriodMs(stmt, "per11c", 24 * HOUR + MINUTE);
      assertOriginAlignedToSecond(stmt, "per11c");
    }
  }

  /**
   * PER-12 triggering without data: no data is written to the source table during the observation
   * period, yet the stream still runs automatically and continuously on system time.
   */
  @Test
  public void testPER12TriggerWithoutData() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      createDatabase(stmt);
      createSourceAndSink(stmt, "sink_12");

      final long createStart = System.currentTimeMillis();
      final long origin = floorToUnit(createStart, SECOND);
      stmt.execute(periodStreamSql("per12", "3s, " + origin, "sink_12"));
      assertEventWindow(stmt, "per12", 3 * SECOND, origin);
      final long originMs = readOriginMillis(stmt, "per12");

      stmt.execute("START STREAM per12");
      // src is never written to: the clock alone must keep the stream running.
      final List<Long> times = awaitFirstTriggers(stmt, "sink_12", 3);
      assertAlignedSeries(times, originMs, 3 * SECOND);

      try (ResultSet rs = stmt.executeQuery("SELECT event_cnt FROM sink_12")) {
        while (rs.next()) {
          assertEquals(
              "a trigger computed over an untouched source table should count 0 rows",
              0L,
              rs.getLong("event_cnt"));
        }
      }
    }
  }

  /**
   * PER-13 PERIOD does not support a millisecond period: the rest of the syntax and objects in
   * {@code PERIOD(1ms, future_origin)} are valid, but the official period units are only
   * s/m/h/d/w/mo, so the creation must fail and leave no task behind.
   *
   * <p>Expected RED until a minimum period unit validation is implemented: {@code
   * TableConfigTaskVisitor} converts any duration unit without a lower bound, so the statement is
   * accepted today. The stream is deliberately not started, since a 1ms period would flood the
   * sink.
   */
  @Test
  public void testPER13MillisecondPeriodRejected() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      createDatabase(stmt);
      createSourceAndSink(stmt, "sink_13");

      final long tBefore = System.currentTimeMillis();
      final long t = nextAlignedEpochMillis(tBefore + 2 * SECOND, 1);
      assertCreateFails(stmt, periodStreamSql("per13", "100ms, " + t, "sink_13"), null);
      assertStreamAbsent(stmt, "per13");
    }
  }
}
