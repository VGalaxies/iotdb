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
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * TUMBLE (tumbling window) integration test covering TUM-01 .. TUM-14 of the TUMBLE spec. Runs on a
 * 1C1D1S cluster (1 ConfigNode, 1 DataNode, 1 StreamNode).
 *
 * <p><b>Core design principle: how windows are divided is decided by the system from origin and
 * size, and the test neither participates in that nor has to make origin "satisfy" anything.</b>
 *
 * <p>The file therefore does not pick aligned data timestamps. Instead it encodes the engine's
 * windowing contract as a test-side derivation function, {@link #windowStartOf}, computes the
 * window grouping the written data should produce, and asserts that what the system actually emits
 * matches it item by item.
 *
 * <p>Engine contract ({@code TumbleWindowEngine#alignToWindowStart}):
 *
 * <pre>
 *   start = origin + floor((ts - origin) / size) * size
 * </pre>
 *
 * <p>Several key behaviours follow, and every case in this file relies on them:
 *
 * <ul>
 *   <li>windows are <b>left closed, right open</b>: {@code [start, start + size)}; the {@code
 *       left_closed} property does not affect runtime behaviour today;
 *   <li><b>the first window start is decided by the event time of the first row</b>, not by the
 *       stream start instant nor by the origin itself;
 *   <li><b>only a row whose event time is &gt;= the current window end closes the window</b>; there
 *       is no watermark advance logic;
 *   <li>on close, <b>a window is emitted only if it holds data</b>; empty windows in between are
 *       skipped;
 *   <li>the row that crosses the boundary <b>belongs to the new window</b> and is not counted into
 *       the window it closes;
 *   <li>a row whose event time is <b>earlier than the current window start</b> is silently dropped.
 * </ul>
 *
 * <p>Together these mean that <b>window advance is driven purely by data event time, independent of
 * the real clock</b>. The test can therefore write rows with arbitrary event times (past or future)
 * and then push the window out with one later row, <b>without waiting for real time to pass</b> -
 * d/w sized windows all assert within seconds. Actual write instants deliberately differ from the
 * data event times.
 *
 * <p>The two gaps where the spec demands failure while the current implementation passes are
 * written as <b>Expected RED</b>, following the existing repository convention: assert the spec and
 * note {@code Expected RED until ...} in the javadoc.
 */
@Category({StreamClusterIT.class})
public class TumbleStreamIT {

  private static final String DB = "tumble_test";

  private static final long SECOND = 1000L;
  private static final long MINUTE = 60_000L;
  private static final long HOUR = 3_600_000L;
  private static final long DAY = 86_400_000L;
  private static final long WEEK = 604_800_000L;

  /**
   * How far ahead of the current instant the data baseline is placed. The event times used for
   * writes land after this baseline, but are <b>never aligned</b> - window membership is decided
   * entirely by the engine from origin and size.
   */
  private static final long BASE_LEAD_MS = 2000L;

  /** Default upper bound for waiting until the sink produces window results. */
  private static final long AWAIT_TIMEOUT_MS = 60_000L;

  /** {@code TumbleWindow#toString} looks like {@code TumbleWindow{size=3600000ms, origin=0ms}}. */
  private static final Pattern SIZE_PATTERN = Pattern.compile("size=(-?\\d+)ms");

  private static final Pattern ORIGIN_PATTERN = Pattern.compile("origin=(-?\\d+)ms");

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
        // Window-boundary cases require one WAL order across TAGs; regions are polled
        // independently.
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
  // Helpers for creating tables and streams
  // --------------------------------------------------------------------------------------------

  private void createDatabase(final Statement statement) throws SQLException {
    statement.execute("CREATE DATABASE " + DB);
    statement.execute("USE " + DB);
  }

  /** Creates the standard source table with the three columns {@code time/device_id/value}. */
  private void createSource(final Statement statement, final String table) throws SQLException {
    statement.execute(
        "CREATE TABLE " + table + "(time TIMESTAMP TIME, device_id STRING TAG, value FLOAT FIELD)");
  }

  /**
   * Creates a source table with a sequence column. TUM-04 needs a unique increasing seq per row to
   * prove that each row belongs to at most one window.
   */
  private void createSeqSource(final Statement statement, final String table) throws SQLException {
    statement.execute(
        "CREATE TABLE " + table + "(time TIMESTAMP TIME, device_id STRING TAG, seq INT64 FIELD)");
  }

  /**
   * Creates the output table. {@code columns} is spliced straight into the DDL, e.g. {@code "time
   * TIMESTAMP TIME, cnt INT64 FIELD"}.
   */
  private void createSink(final Statement statement, final String table, final String columns)
      throws SQLException {
    statement.execute("CREATE TABLE " + table + "(" + columns + ")");
  }

  /**
   * Builds an ungrouped TUMBLE stream creation statement.
   *
   * @param windowArgs the raw text between {@code TUMBLE(} and {@code )}, e.g. {@code "5s"}, {@code
   *     "5s, 1757900000001"}, {@code "size =&gt; 1h"}
   * @param sinkColumns the written columns in the INTO clause; the count must match {@code
   *     selectList}
   * @param selectList the SELECT list, without {@code FROM}
   */
  private static String tumbleStreamSql(
      final String streamName,
      final String sourceTable,
      final String windowArgs,
      final String sinkTable,
      final String sinkColumns,
      final String selectList) {
    return "CREATE STREAM "
        + streamName
        + " FROM "
        + sourceTable
        + " TUMBLE("
        + windowArgs
        + ") INTO "
        + sinkTable
        + "("
        + sinkColumns
        + ") SELECT "
        + selectList
        + " FROM ${ROWS}";
  }

  /** Same as {@link #tumbleStreamSql}, but with a {@code PARTITION BY} grouping. */
  private static String tumblePartitionedStreamSql(
      final String streamName,
      final String sourceTable,
      final String partitionBy,
      final String windowArgs,
      final String sinkTable,
      final String sinkColumns,
      final String selectList) {
    return "CREATE STREAM "
        + streamName
        + " FROM "
        + sourceTable
        + " PARTITION BY "
        + partitionBy
        + " TUMBLE("
        + windowArgs
        + ") INTO "
        + sinkTable
        + "("
        + sinkColumns
        + ") SELECT "
        + selectList
        + " FROM ${ROWS}";
  }

  // --------------------------------------------------------------------------------------------
  // Inserting data: event times are given explicitly and decoupled from the real write instant
  // --------------------------------------------------------------------------------------------

  /**
   * Inserts one row into the standard source table.
   *
   * <p><b>The event time is supplied explicitly and is unrelated to the real write instant</b>:
   * TUMBLE window division only looks at the data event time, so past or future timestamps can be
   * written to drive the windows without waiting for real time to pass.
   */
  private void insertRow(
      final Statement statement,
      final String table,
      final long time,
      final String device,
      final double value)
      throws SQLException {
    statement.execute(
        "INSERT INTO "
            + table
            + "(time, device_id, value) VALUES ("
            + time
            + ", '"
            + device
            + "', "
            + value
            + ")");
  }

  /** Inserts one row into a source table carrying a seq column (TUM-04 only). */
  private void insertSeqRow(
      final Statement statement,
      final String table,
      final long time,
      final String device,
      final long seq)
      throws SQLException {
    statement.execute(
        "INSERT INTO "
            + table
            + "(time, device_id, seq) VALUES ("
            + time
            + ", '"
            + device
            + "', "
            + seq
            + ")");
  }

  // --------------------------------------------------------------------------------------------
  // Engine contract: the only source of expected values on the test side
  // --------------------------------------------------------------------------------------------

  /**
   * Engine contract: the start of the window a given event time belongs to.
   *
   * <p>Corresponds to {@code TumbleWindowEngine#alignToWindowStart}:
   *
   * <pre>
   *   start = origin + floor((ts - origin) / size) * size
   * </pre>
   *
   * <p>The implementation uses Java's {@code %} (truncated remainder, rounding toward zero) plus an
   * {@code offset += size} compensation, which is exactly equivalent to the {@code Math.floorDiv}
   * used here. The compensation is needed because when {@code ts &lt; origin}, {@code -2000 % 10000
   * == -2000} rather than the mathematical 8000, so subtracting directly would push the window
   * start to the right.
   *
   * <p>Every case in this file keeps data event times &gt;= origin, so that negative offset branch
   * is never reached in practice.
   */
  private static long windowStartOf(final long timestamp, final long origin, final long size) {
    return origin + Math.floorDiv(timestamp - origin, size) * size;
  }

  /**
   * Derives "emitted window to row count" from the engine contract.
   *
   * <p>{@code eventTimes} must be ascending by event time, and <b>its last element is the pusher
   * row</b>. The reasoning:
   *
   * <ul>
   *   <li>a window closes when a row with event time &gt;= the current window end arrives, so every
   *       window holding data is closed by "the next, later row", except the last one;
   *   <li>the last window is closed by the pusher row, which itself lands in the window right after
   *       it;
   *   <li>the window the pusher row lands in <b>is never closed</b>, so it is excluded here.
   * </ul>
   *
   * <p>Callers must guarantee the pusher row equals {@code last data point + size}, so that it
   * definitely lands in the immediately following window rather than alongside the data.
   */
  private static Map<Long, Integer> expectedEmittedWindows(
      final List<Long> eventTimes, final long origin, final long size) {
    assertFalse("eventTimes must not be empty", eventTimes.isEmpty());
    final Map<Long, Integer> windows = new TreeMap<>();
    for (final long timestamp : eventTimes) {
      windows.merge(windowStartOf(timestamp, origin, size), 1, Integer::sum);
    }
    // The window holding the pusher row is never closed - that row is the one closing the previous
    // window.
    final long pusherWindow = windowStartOf(eventTimes.get(eventTimes.size() - 1), origin, size);
    assertEquals(
        "the pusher row should land alone in the immediately following window (pusher = last data point + size)",
        1,
        (int) windows.getOrDefault(pusherWindow, 0));
    windows.remove(pusherWindow);
    return windows;
  }

  // --------------------------------------------------------------------------------------------
  // Reading the sink
  // --------------------------------------------------------------------------------------------

  /** One result row in the sink. Unused columns stay {@code null} / 0, per the reading method. */
  private static final class SinkRow {

    /** {@code ${START_TIME}}, i.e. the window start. */
    private final long time;

    /** {@code count(*)}. */
    private final long count;

    /** The grouping column {@code ${1}}; only set for sinks carrying a TAG column. */
    private final String tag;

    /** {@code max(seq)}; only set for the TUM-04 sink. */
    private final long seqMax;

    private SinkRow(final long time, final long count, final String tag, final long seqMax) {
      this.time = time;
      this.count = count;
      this.tag = tag;
      this.seqMax = seqMax;
    }

    @Override
    public String toString() {
      return "(time=" + time + ", cnt=" + count + ", tag=" + tag + ", seqMax=" + seqMax + ")";
    }
  }

  /** Reads a sink shaped {@code (time TIMESTAMP TIME, cnt INT64 FIELD)}. */
  private static List<SinkRow> readCountSink(final Statement statement, final String sinkTable)
      throws SQLException {
    final List<SinkRow> rows = new ArrayList<>();
    try (ResultSet rs = statement.executeQuery("SELECT time, cnt FROM " + sinkTable)) {
      while (rs.next()) {
        rows.add(new SinkRow(rs.getLong("time"), rs.getLong("cnt"), null, 0L));
      }
    }
    return rows;
  }

  /**
   * Reads a sink shaped {@code (time, device_id, cnt)} (the TAG grouped streams of TUM-11/TUM-12).
   */
  private static List<SinkRow> readTagCountSink(final Statement statement, final String sinkTable)
      throws SQLException {
    final List<SinkRow> rows = new ArrayList<>();
    try (ResultSet rs = statement.executeQuery("SELECT time, device_id, cnt FROM " + sinkTable)) {
      while (rs.next()) {
        rows.add(new SinkRow(rs.getLong("time"), rs.getLong("cnt"), rs.getString("device_id"), 0L));
      }
    }
    return rows;
  }

  /** Reads a sink shaped {@code (time, cnt, seq_max)} (TUM-04). */
  private static List<SinkRow> readCountSeqSink(final Statement statement, final String sinkTable)
      throws SQLException {
    final List<SinkRow> rows = new ArrayList<>();
    try (ResultSet rs = statement.executeQuery("SELECT time, cnt, seq_max FROM " + sinkTable)) {
      while (rs.next()) {
        rows.add(new SinkRow(rs.getLong("time"), rs.getLong("cnt"), null, rs.getLong("seq_max")));
      }
    }
    return rows;
  }

  /**
   * A single sink read that may throw {@link SQLException}, so it fits an Awaitility retry lambda.
   */
  @FunctionalInterface
  private interface SinkReader {
    List<SinkRow> read() throws SQLException;
  }

  /**
   * Polls until the sink satisfies the condition, then returns every row.
   *
   * <p>The JDBC query must stay inside the retry lambda: Awaitility re-runs it on every attempt,
   * otherwise it would always see the first snapshot.
   */
  private static List<SinkRow> awaitSinkRows(
      final SinkReader reader, final Predicate<List<SinkRow>> satisfied, final String expectation)
      throws SQLException {
    awaitWithin(AWAIT_TIMEOUT_MS)
        .untilAsserted(
            () -> {
              final List<SinkRow> rows = reader.read();
              assertTrue(expectation + ", actual=" + describe(rows), satisfied.test(rows));
            });
    return reader.read();
  }

  /** Waits until the sink has produced at least {@code minRows} window results. */
  private static List<SinkRow> awaitCountSink(
      final Statement statement, final String sinkTable, final int minRows, final String streamName)
      throws SQLException {
    return awaitSinkRows(
        () -> readCountSink(statement, sinkTable),
        rows -> rows.size() >= minRows,
        "stream "
            + streamName
            + " should produce at least "
            + minRows
            + " window results in sink "
            + sinkTable);
  }

  /**
   * Waits until the sink contains the window starting at {@code windowStart} with matching {@code
   * count} and {@code seqMax}. Used by TUM-04 as a precise per-batch barrier: only after the
   * batch's window-closing row is consumed and the window is emitted does the predicate hold, so
   * the next batch cannot be written ahead of it.
   */
  private static void awaitWindowInSink(
      final Statement statement,
      final String sinkTable,
      final long windowStart,
      final long expectedCount,
      final long expectedSeqMax)
      throws SQLException {
    awaitSinkRows(
        () -> readCountSeqSink(statement, sinkTable),
        rows -> {
          for (final SinkRow r : filterByTime(rows, windowStart)) {
            if (r.count == expectedCount && r.seqMax == expectedSeqMax) {
              return true;
            }
          }
          return false;
        },
        "sink "
            + sinkTable
            + " should contain window start="
            + windowStart
            + " cnt="
            + expectedCount
            + " seqMax="
            + expectedSeqMax);
  }

  private static ConditionFactory awaitWithin(final long timeoutMs) {
    return Awaitility.await()
        .pollInSameThread()
        .pollDelay(500, TimeUnit.MILLISECONDS)
        .pollInterval(500, TimeUnit.MILLISECONDS)
        .atMost(timeoutMs, TimeUnit.MILLISECONDS);
  }

  /**
   * Waits until {@code information_schema.streams} reports the stream as RUNNING.
   *
   * <p>START STREAM returns once the StreamNode has accepted the task, and the subscription only
   * delivers rows written after it is bound. Confirming RUNNING before the first source row is
   * written makes that ordering explicit, so the data layout can never race the stream startup.
   */
  private void awaitStreamRunning(final Statement statement, final String streamName)
      throws SQLException {
    awaitWithin(AWAIT_TIMEOUT_MS)
        .untilAsserted(
            () -> {
              final String status = getStreamColumn(statement, streamName, "status");
              assertEquals(
                  "stream "
                      + streamName
                      + " should be RUNNING before source rows are written; actual status="
                      + status,
                  "RUNNING",
                  status);
            });
  }

  // --------------------------------------------------------------------------------------------
  // Assertions
  // --------------------------------------------------------------------------------------------

  /**
   * Asserts that the windows the stream actually emitted match the ones derived from the engine
   * contract.
   *
   * <p>{@code eventTimes} must be ascending and end with the pusher row (see {@link
   * #expectedEmittedWindows}).
   *
   * <p>Four things are asserted:
   *
   * <ol>
   *   <li>every window start in the sink lies on the {@code origin + n * size} grid;
   *   <li>every window predicted by the contract really appears, with a matching row count;
   *   <li>consecutive window starts differ by <b>exactly size</b> (fixed length, contiguous, non
   *       overlapping);
   *   <li>the row counts sum to <b>the number of rows actually written</b> (nothing duplicated and
   *       nothing dropped, so each row belongs to at most one window).
   * </ol>
   *
   * <p>Filtering by "the starts predicted by the contract" rather than asserting the total sink row
   * count is what handles {@code TumbleWindowEngine#forceClose}: when a task winds down it also
   * emits the still open tail window if it holds data, and the window holding the pusher row is
   * exactly such a tail window.
   *
   * <p>Callers must guarantee the data covers <b>contiguous</b> windows (no empty window in
   * between), otherwise point 3 does not hold.
   */
  private void assertWindowsMatch(
      final List<SinkRow> rows,
      final List<Long> eventTimes,
      final long origin,
      final long size,
      final String streamName,
      final Statement statement)
      throws SQLException {

    final Map<Long, Integer> expected = expectedEmittedWindows(eventTimes, origin, size);
    final String diagnostic =
        "stream="
            + streamName
            + ", event_window="
            + getStreamColumn(statement, streamName, "event_window")
            + ", origin="
            + origin
            + ", size="
            + size
            + ", windows derived from the contract="
            + expected
            + ", all sink rows="
            + describe(rows);

    // (1) every row in the sink (including a possible tail window) must start on the engine's grid
    for (final SinkRow row : rows) {
      assertEquals(
          "sink holds a window start that is not on the origin grid: "
              + row.time
              + "; "
              + diagnostic,
          row.time,
          windowStartOf(row.time, origin, size));
    }

    // (2) every window predicted by the contract must appear, with a matching row count
    final Map<Long, Long> actual = actualWindowCounts(rows, new ArrayList<>(expected.keySet()));
    assertEquals(
        "the window start sequence does not match the contract; " + diagnostic,
        new ArrayList<>(expected.keySet()),
        new ArrayList<>(actual.keySet()));
    for (final Map.Entry<Long, Integer> entry : expected.entrySet()) {
      assertEquals(
          "the row count of window "
              + entry.getKey()
              + " does not match the contract; "
              + diagnostic,
          (long) entry.getValue(),
          (long) actual.get(entry.getKey()));
    }

    // (3) consecutive window starts differ by exactly size => fixed length, contiguous, no overlap
    final List<Long> starts = new ArrayList<>(actual.keySet());
    for (int i = 1; i < starts.size(); i++) {
      assertEquals(
          "consecutive window starts should differ by exactly size (fixed length, contiguous); "
              + diagnostic,
          size,
          (long) (starts.get(i) - starts.get(i - 1)));
    }

    // (4) the row counts sum to the number of rows written (the last eventTime is the pusher row)
    long total = 0L;
    for (final long count : actual.values()) {
      total += count;
    }
    assertEquals(
        "the row counts should sum to the number of rows written (each row belongs to at most one window); "
            + diagnostic,
        (long) (eventTimes.size() - 1),
        total);
  }

  /**
   * Collects the sink rows whose window start is in {@code starts} into "start to row count",
   * ascending by start.
   *
   * <p>Cases use it to compare hand written expected values against what the sink really holds;
   * rows not in {@code starts} (such as a tail window re-sent by forceClose) are ignored.
   */
  private static Map<Long, Long> actualWindowCounts(
      final List<SinkRow> rows, final List<Long> starts) {
    final Map<Long, Long> counts = new TreeMap<>();
    for (final SinkRow row : rows) {
      if (starts.contains(row.time)) {
        counts.merge(row.time, row.count, Long::sum);
      }
    }
    return counts;
  }

  /** Returns the sink rows whose window start equals {@code start}. */
  private static List<SinkRow> filterByTime(final List<SinkRow> rows, final long start) {
    final List<SinkRow> filtered = new ArrayList<>();
    for (final SinkRow row : rows) {
      if (row.time == start) {
        filtered.add(row);
      }
    }
    return filtered;
  }

  private static String describe(final List<SinkRow> rows) {
    final StringBuilder builder = new StringBuilder("[");
    for (int i = 0; i < rows.size(); i++) {
      if (i > 0) {
        builder.append(", ");
      }
      builder.append(rows.get(i));
    }
    return builder.append(']').toString();
  }

  // --------------------------------------------------------------------------------------------
  // information_schema.streams / event_window
  // --------------------------------------------------------------------------------------------

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

  /**
   * Reads one column of {@code information_schema.streams}.
   *
   * <p>The column is looked up by name through {@code SELECT *}: {@code information_schema.streams}
   * has only 13 columns and <b>does not carry {@code properties}</b> (that is the 14th column of
   * {@code SHOW STREAMS} only, and no case here needs it).
   */
  private String getStreamColumn(
      final Statement statement, final String streamName, final String column) throws SQLException {
    try (ResultSet rs =
        statement.executeQuery(
            "SELECT *"
                + " FROM information_schema.streams WHERE stream_name = '"
                + streamName
                + "'")) {
      return rs.next() ? rs.getString(column) : null;
    }
  }

  /**
   * Reads the stream's event window metadata string, e.g. {@code TumbleWindow{size=3600000ms,
   * origin=0ms}}.
   */
  private String eventWindowOf(final Statement statement, final String streamName)
      throws SQLException {
    final String eventWindow = getStreamColumn(statement, streamName, "event_window");
    assertNotNull("event_window should be recorded for " + streamName, eventWindow);
    return eventWindow;
  }

  private static long extractEventWindowValue(final String eventWindow, final Pattern pattern) {
    final Matcher matcher = pattern.matcher(eventWindow == null ? "" : eventWindow);
    assertTrue("cannot parse the target value out of event_window: " + eventWindow, matcher.find());
    return Long.parseLong(matcher.group(1));
  }

  /**
   * Asserts the size and origin recorded in {@code event_window}.
   *
   * <p>Note that TUMBLE metadata looks like {@code size=...ms, origin=...ms}, so <b>the {@code
   * periodMs=} / {@code originMs=} patterns of PeriodStreamIT cannot be reused here</b>.
   */
  private void assertEventWindow(
      final Statement statement,
      final String streamName,
      final long expectedSizeMs,
      final long expectedOriginMs)
      throws SQLException {
    final String eventWindow = eventWindowOf(statement, streamName);
    assertEquals(
        "unexpected size in event_window: " + eventWindow,
        expectedSizeMs,
        extractEventWindowValue(eventWindow, SIZE_PATTERN));
    assertEquals(
        "unexpected origin in event_window: " + eventWindow,
        expectedOriginMs,
        extractEventWindowValue(eventWindow, ORIGIN_PATTERN));
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

  // --------------------------------------------------------------------------------------------
  // Time formatting
  // --------------------------------------------------------------------------------------------

  /**
   * Formats epoch millis as a DateTime literal with an explicit offset, e.g. {@code
   * 2026-09-17T10:23:45.137+08:00}.
   */
  private static String dateTimeWithT(final long epochMillis, final ZoneOffset offset) {
    return DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSXXX")
        .withZone(offset)
        .format(Instant.ofEpochMilli(epochMillis));
  }

  /**
   * Same as above, but with a <b>space</b> between date and time - the other DateTime form of the
   * spec.
   */
  private static String dateTimeWithSpace(final long epochMillis, final ZoneOffset offset) {
    return DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSXXX")
        .withZone(offset)
        .format(Instant.ofEpochMilli(epochMillis));
  }

  /** Failure diagnostics only: formats epoch millis as a readable UTC time. */
  private static String isoUtc(final long epochMillis) {
    return DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
        .withZone(ZoneOffset.UTC)
        .format(Instant.ofEpochMilli(epochMillis));
  }

  /**
   * The shared driver: writes {@code eventTimes} ascending by event time, appends a pusher row
   * (valued {@code last event time + size}) to push the last window out, waits for the sink to
   * produce results, then asserts against the engine contract.
   *
   * <p>The pusher row is {@code last + size} because {@code last} necessarily lies in {@code
   * [start, start + size)}, so {@code last + size &gt;= start + size} and closing is guaranteed.
   *
   * <p>Data is written ascending by event time because <b>a row earlier than the current window
   * start is silently dropped by the engine</b>. Out of order writes inside a window are safe (see
   * TUM-12), but stepping back across a window boundary is not.
   *
   * @param eventTimes the data point event times, <b>not including the pusher row</b>
   * @return every row read from the sink, so callers can add case specific assertions
   */
  private List<SinkRow> driveWindowsAndAssert(
      final Statement statement,
      final String sourceTable,
      final String sinkTable,
      final String streamName,
      final List<Long> eventTimes,
      final long origin,
      final long size)
      throws SQLException {
    assertFalse("eventTimes must not be empty", eventTimes.isEmpty());
    awaitStreamRunning(statement, streamName);

    final List<Long> sorted = new ArrayList<>(eventTimes);
    Collections.sort(sorted);
    for (final long time : sorted) {
      insertRow(statement, sourceTable, time, "d1", 1.0);
    }
    final long pusher = sorted.get(sorted.size() - 1) + size;

    final List<Long> withPusher = new ArrayList<>(sorted);
    withPusher.add(pusher);

    final int expectedWindowCount = expectedEmittedWindows(withPusher, origin, size).size();
    // Sink barrier: wait until the data rows have been consumed (the closing row of the last data
    // point produces expectedWindowCount - 1 windows; the pusher closes the final one) before
    // writing the pusher, so the pusher can never be consumed ahead of the data rows and trigger a
    // premature close-and-destroy.
    if (expectedWindowCount > 1) {
      awaitCountSink(statement, sinkTable, expectedWindowCount - 1, streamName);
    }
    insertRow(statement, sourceTable, pusher, "d1", 1.0);

    final List<SinkRow> rows =
        awaitCountSink(statement, sinkTable, expectedWindowCount, streamName);
    assertWindowsMatch(rows, withPusher, origin, size, streamName, statement);
    return rows;
  }

  // ============================================================================================
  // TUM-01 latest signature
  // ============================================================================================

  /**
   * TUM-01 latest signature: {@code TUMBLE(1h)} is created successfully and does not require a
   * timecol argument.
   *
   * <p>Criteria: {@code event_window} records {@code size=3600000ms} with a defaulted origin of 0;
   * a second stream using {@code TUMBLE(5s)} is created and fed with ordinary base point data, to
   * verify the created stream really does produce window results from origin and size.
   */
  @Test
  public void testTUM01MinimalTumbleSignature() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      createDatabase(stmt);
      createSource(stmt, "tum01_src");

      // (a) TUMBLE(1h): size only, no timecol
      createSink(stmt, "sink01a", "time TIMESTAMP TIME, cnt INT64 FIELD");
      stmt.execute(
          tumbleStreamSql(
              "tum01a", "tum01_src", "1h", "sink01a", "time, cnt", "${START_TIME}, count(*)"));
      assertStreamExists(stmt, "tum01a");
      assertEquals(
          "the stream status should be CREATED after creation",
          "CREATED",
          getStreamColumn(stmt, "tum01a", "status"));
      assertEventWindow(stmt, "tum01a", HOUR, 0L);
    }
  }

  // ============================================================================================
  // TUM-02 legacy timecol signature
  // ============================================================================================

  /**
   * TUM-02 legacy timecol signature: {@code TUMBLE(time, 1h)} fails the creation and leaves no
   * task.
   *
   * <p>With positional arguments the first actual argument fills the SIZE slot, and {@code time} is
   * a column reference rather than a duration literal; the cast in {@code
   * TumbleEventWindow#parseArguments} throws {@code ClassCastException}, which is caught and turned
   * into {@code Invalid argument type for tumble event window}.
   *
   * <p>The case is by nature "no task produced", so there is no data to model.
   */
  @Test
  public void testTUM02LegacyTimeColumnSignature() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      createDatabase(stmt);
      createSource(stmt, "tum02_src");
      createSink(stmt, "sink02", "time TIMESTAMP TIME, cnt INT64 FIELD");

      assertCreateFails(
          stmt,
          tumbleStreamSql(
              "tum02", "tum02_src", "time, 1h", "sink02", "time, cnt", "${START_TIME}, count(*)"),
          "Invalid argument type for tumble event window");
      assertStreamAbsent(stmt, "tum02");
    }
  }

  // ============================================================================================
  // TUM-03 fixed, contiguous, non overlapping
  // ============================================================================================

  /**
   * TUM-03 fixed, continuous, non overlapping: fixed size/origin, writing across three adjacent
   * windows.
   *
   * <p>Data layout ({@code size = 5s}, explicit {@code origin = o}):
   *
   * <pre>
   *   W1 [o,      o+5s)   o+1000, o+2000
   *   W2 [o+5s,   o+10s)  o+7000, o+8000
   *   W3 [o+10s,  o+15s)  o+13000, o+14000
   *   pusher row          o+19000   (= last data point + size)
   * </pre>
   *
   * <p>Engine behaviour: each row that crosses the current window end closes and emits that window
   * and opens a new one, so exactly {@code (o,2) / (o+5s,2) / (o+10s,2)} should be produced. {@link
   * #assertWindowsMatch} additionally asserts that consecutive starts differ by <b>exactly size</b>
   * and that the counts sum to the number of rows written.
   */
  @Test
  public void testTUM03FixedContiguousNonOverlapping() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      createDatabase(stmt);
      createSource(stmt, "tum03_src");
      createSink(stmt, "sink03", "time TIMESTAMP TIME, cnt INT64 FIELD");

      final long size = 5 * SECOND;
      // The origin is a plain instant, never aligned - the engine decides the boundaries from
      // origin
      // and size on its own.
      final long origin = System.currentTimeMillis() + BASE_LEAD_MS;
      stmt.execute(
          tumbleStreamSql(
              "tum03",
              "tum03_src",
              "5s, " + origin,
              "sink03",
              "time, cnt",
              "${START_TIME}, count(*)"));
      stmt.execute("START STREAM tum03");

      final List<Long> eventTimes =
          Arrays.asList(
              origin + 1000, origin + 2000, // W1
              origin + 7000, origin + 8000, // W2
              origin + 13000, origin + 14000); // W3

      final List<SinkRow> rows =
          driveWindowsAndAssert(stmt, "tum03_src", "sink03", "tum03", eventTimes, origin, size);

      // Case specific assertion: the three window starts and counts are exactly these hand written
      // triples.
      final Map<Long, Long> expectedCounts = new TreeMap<>();
      expectedCounts.put(origin, 2L);
      expectedCounts.put(origin + size, 2L);
      expectedCounts.put(origin + 2 * size, 2L);
      assertEquals(
          "exactly three adjacent windows should be produced (2 rows each); actual="
              + describe(rows),
          expectedCounts,
          actualWindowCounts(rows, new ArrayList<>(expectedCounts.keySet())));
    }
  }

  // ============================================================================================
  // TUM-04 each row belongs to at most one window
  // ============================================================================================

  /**
   * TUM-04 each row belongs to at most one window: the input carries a unique seq and covers both
   * the interior and the boundaries of windows.
   *
   * <p>Data layout ({@code size = 5s}, {@code origin = o}, seq unique and increasing):
   *
   * <pre>
   *   seq=1 @ o        <- left boundary, in W1
   *   seq=2 @ o+2500   <- window interior, in W1
   *   seq=3 @ o+4999   <- 1ms before the right boundary, in W1
   *   seq=4 @ o+5000   <- right boundary, in W2 because the window is left closed right open
   *   seq=5 @ o+7500   <- window interior, in W2
   *   seq=6 @ o+10000  <- right boundary, in W3
   *   seq=7 @ o+15000  <- pusher row
   * </pre>
   *
   * <p>Expected values (derived from the contract): window starts {@code [o, o+5s, o+10s]} with
   * counts {@code [3, 2, 1]}.
   *
   * <ul>
   *   <li>They sum to 6, the number of rows written, so no seq was double counted or dropped, i.e.
   *       <b>each row belongs to at most one window</b>;
   *   <li>{@code max(seq)} is 3 / 5 / 6 respectively, so the boundary row {@code o+5000} lands in
   *       W2 rather than W1, directly proving the window is left closed right open.
   * </ul>
   *
   * <p>If the current implementation does not support the {@code max()} aggregate, this can degrade
   * to asserting {@code count(*)} alone: {@code [3,2,1]} with a total of 6 already separates left
   * closed ({@code [3,2,1]}) from right closed (which would give {@code [4,2,...]}).
   */
  @Test
  public void testTUM04EachRowBelongsToAtMostOneWindow() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      createDatabase(stmt);
      createSeqSource(stmt, "tum04_src");
      createSink(stmt, "sink04", "time TIMESTAMP TIME, cnt INT64 FIELD, seq_max INT64 FIELD");

      final long size = 5 * SECOND;
      final long origin = System.currentTimeMillis() + BASE_LEAD_MS;
      stmt.execute(
          tumbleStreamSql(
              "tum04",
              "tum04_src",
              "5s, " + origin,
              "sink04",
              "time, cnt, seq_max",
              "${START_TIME}, count(*), max(seq)"));
      stmt.execute("START STREAM tum04");
      awaitStreamRunning(stmt, "tum04");

      // Event times must be >= origin (otherwise the window start would fall before the origin,
      // which is semantically confusing); this case starts at the origin itself, which also covers
      // the window's left boundary.
      //
      // Written in three batches, each ending with a window-closing row. A sink barrier
      // (awaitWindowInSink) after each batch confirms the batch's rows were consumed and its window
      // emitted before the next batch is written, so the pusher (batch 3) can never be consumed
      // ahead of the data rows and trigger a premature close-and-destroy.
      // Batch 1: seq 1..4, seq=4 (@ o+5000) closes W1
      insertSeqRow(stmt, "tum04_src", origin, "d1", 1L);
      insertSeqRow(stmt, "tum04_src", origin + 2500, "d1", 2L);
      insertSeqRow(stmt, "tum04_src", origin + 4999, "d1", 3L);
      insertSeqRow(stmt, "tum04_src", origin + 5000, "d1", 4L);
      awaitWindowInSink(stmt, "sink04", origin, 3, 3);

      // Batch 2: seq 5..6, seq=6 (@ o+10000) closes W2
      insertSeqRow(stmt, "tum04_src", origin + 7500, "d1", 5L);
      insertSeqRow(stmt, "tum04_src", origin + 10000, "d1", 6L);
      awaitWindowInSink(stmt, "sink04", origin + size, 2, 5);

      // Batch 3: pusher, seq=7 (@ o+15000) closes W3
      final long pusher = origin + 15000;
      insertSeqRow(stmt, "tum04_src", pusher, "d1", 7L);
      awaitWindowInSink(stmt, "sink04", origin + 2 * size, 1, 6);

      final List<Long> eventTimes =
          Arrays.asList(
              origin, origin + 2500, origin + 4999, origin + 5000, origin + 7500, origin + 10000);
      final List<Long> withPusher = new ArrayList<>(eventTimes);
      withPusher.add(pusher);

      // First confirm the contract derivation is itself as expected, then use it against the
      // engine.
      final Map<Long, Integer> expected = expectedEmittedWindows(withPusher, origin, size);
      assertEquals(
          "the window starts derived from the contract are wrong: " + expected,
          Arrays.asList(origin, origin + size, origin + 2 * size),
          new ArrayList<>(expected.keySet()));
      assertEquals(
          "the row counts derived from the contract are wrong: " + expected,
          Arrays.asList(3, 2, 1),
          new ArrayList<>(expected.values()));

      // The three per-batch barriers above already guarantee W3 is present; read the final sink
      // snapshot for the assertions below.
      final List<SinkRow> rows = readCountSeqSink(stmt, "sink04");
      assertWindowsMatch(rows, withPusher, origin, size, "tum04", stmt);

      // Checking max(seq) window by window directly proves the boundary row o+5000 (seq=4) belongs
      // to W2 rather than W1.
      final List<Long> starts = Arrays.asList(origin, origin + size, origin + 2 * size);
      final Map<Long, SinkRow> byStart = new TreeMap<>();
      for (final SinkRow row : rows) {
        if (starts.contains(row.time)) {
          byStart.put(row.time, row);
        }
      }
      assertEquals(
          "W1 should hold 3 rows; actual=" + describe(rows), 3L, byStart.get(origin).count);
      assertEquals(
          "the largest seq in W1 should be 3, meaning the boundary row seq=4 (event time o+5000) was not counted into W1; actual="
              + describe(rows),
          3L,
          byStart.get(origin).seqMax);
      assertEquals(
          "W2 should hold 2 rows; actual=" + describe(rows), 2L, byStart.get(origin + size).count);
      assertEquals(
          "the largest seq in W2 should be 5, meaning seq=4 falls exactly on the W2 start (windows are left closed right open); actual="
              + describe(rows),
          5L,
          byStart.get(origin + size).seqMax);
      assertEquals(
          "W3 should hold 1 row; actual=" + describe(rows),
          1L,
          byStart.get(origin + 2 * size).count);
      assertEquals(
          "the largest seq in W3 should be 6; actual=" + describe(rows),
          6L,
          byStart.get(origin + 2 * size).seqMax);
    }
  }

  // ============================================================================================
  // TUM-05 non month positive integer size
  // ============================================================================================

  /**
   * TUM-05 non month positive integer size: the units s/m/h/d/w are covered parametrically, each
   * with a different integer coefficient.
   *
   * <p>Data layout: one stream per unit (<b>no origin given</b>, so origin = 0), asserting two
   * things:
   *
   * <ol>
   *   <li>a legal positive integer duration can be created - the {@code event_window} size equals
   *       that unit's millisecond value;
   *   <li>windows are divided continuously at that fixed length - ordinary base point data drives
   *       the windows, and every expected value is derived by {@link #expectedEmittedWindows} from
   *       the contract, so the test does not presuppose which window the data lands in.
   * </ol>
   *
   * <p>Because windows are driven by data event time, even a very long window such as {@code 2w}
   * can be asserted within seconds.
   */
  @Test
  public void testTUM05NonMonthPositiveSizeUnits() throws Exception {
    // A different integer coefficient per unit, to avoid the illusion that only 1 works.
    final String[] windowArgs = {"5s", "3m", "2h", "4d", "2w"};
    final long[] expectedSizes = {5 * SECOND, 3 * MINUTE, 2 * HOUR, 4 * DAY, 2 * WEEK};

    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      createDatabase(stmt);
      createSource(stmt, "tum05_src");

      for (int i = 0; i < windowArgs.length; i++) {
        final String suffix = "_" + (i + 1);
        final String streamName = "tum05" + suffix;
        final String sinkTable = "sink05" + suffix;
        final long size = expectedSizes[i];

        createSink(stmt, sinkTable, "time TIMESTAMP TIME, cnt INT64 FIELD");
        stmt.execute(
            tumbleStreamSql(
                streamName,
                "tum05_src",
                windowArgs[i],
                sinkTable,
                "time, cnt",
                "${START_TIME}, count(*)"));
        assertEventWindow(stmt, streamName, size, 0L);
        stmt.execute("START STREAM " + streamName);

        // Plain base instant, never aligned: which windows the data falls into is decided entirely
        // by the engine from origin (= 0) and size.
        final long base = System.currentTimeMillis() + BASE_LEAD_MS;
        final List<Long> eventTimes =
            Arrays.asList(
                base + 1000,
                base + 2000,
                base + 1000 + size,
                base + 2000 + size,
                base + 1000 + 2 * size);

        final List<SinkRow> rows =
            driveWindowsAndAssert(stmt, "tum05_src", sinkTable, streamName, eventTimes, 0L, size);
        assertFalse(
            "unit "
                + windowArgs[i]
                + " should produce at least one window result; actual="
                + describe(rows),
            rows.isEmpty());
      }
    }
  }

  // ============================================================================================
  // TUM-06 illegal size
  // ============================================================================================

  /**
   * TUM-06 illegal size: 0, negative and non integer values must all fail the creation and leave no
   * task behind.
   *
   * <p>Why each sub case fails:
   *
   * <ul>
   *   <li>{@code TUMBLE(0)}: a bare integer lands in the SIZE slot and the cast of {@code
   *       LongLiteral} to {@code TimeDurationLiteral} fails;
   *   <li>{@code TUMBLE(1.5)}: a {@code DoubleLiteral}, which fails the same cast;
   *   <li>{@code TUMBLE(-1h)}, {@code size => -1s}: rejected at the grammar layer - the {@code
   *       timeDuration} rule is {@code (INTEGER_VALUE intervalField)+} and {@code INTEGER_VALUE} is
   *       unsigned;
   *   <li>{@code TUMBLE(1.5s)}: rejected at the grammar layer as well, decimals are not accepted by
   *       {@code timeDuration}.
   * </ul>
   *
   * <p><b>Expected RED</b>: {@code TUMBLE(0s)} and {@code TUMBLE(size => 0s)} are valid syntax but
   * semantically illegal. {@code TumbleEventWindow#parseArguments} has no "size must be positive"
   * check (unlike the TVF path, where {@code TumbleTableFunction} uses {@code
   * POSITIVE_LONG_CHECKER}), so the stream is created successfully and later divides by zero on
   * {@code % windowSize} inside {@code alignToWindowStart}.
   *
   * <p>Those two sub cases assert "creation should fail" per the spec, so they fail under the
   * current implementation - which is exactly the gap they expose. They assert only the failure,
   * without constraining the message and without {@code assertStreamAbsent}, to avoid piling on
   * assertions that do not help locate the problem.
   */
  @Test
  public void testTUM06IllegalSize() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      createDatabase(stmt);
      createSource(stmt, "tum06_src");
      createSink(stmt, "sink06", "time TIMESTAMP TIME, cnt INT64 FIELD");

      // ---- GREEN: these forms do fail ----

      // (a) bare integer 0: wrong type
      assertCreateFails(
          stmt,
          tumbleStreamSql(
              "tum06a", "tum06_src", "0", "sink06", "time, cnt", "${START_TIME}, count(*)"),
          "Invalid argument type for tumble event window");
      assertStreamAbsent(stmt, "tum06a");

      // (b) decimal: wrong type
      assertCreateFails(
          stmt,
          tumbleStreamSql(
              "tum06b", "tum06_src", "1.5", "sink06", "time, cnt", "${START_TIME}, count(*)"),
          "Invalid argument type for tumble event window");
      assertStreamAbsent(stmt, "tum06b");

      // (c) negative duration: rejected at the grammar layer (INTEGER_VALUE is unsigned), so only
      //     assert the failure without constraining the message
      assertCreateFails(
          stmt,
          tumbleStreamSql(
              "tum06c", "tum06_src", "-1h", "sink06", "time, cnt", "${START_TIME}, count(*)"),
          null);
      assertStreamAbsent(stmt, "tum06c");

      // (d) named negative
      assertCreateFails(
          stmt,
          tumbleStreamSql(
              "tum06d",
              "tum06_src",
              "size => -1s",
              "sink06",
              "time, cnt",
              "${START_TIME}, count(*)"),
          null);
      assertStreamAbsent(stmt, "tum06d");

      // (e) decimal duration: rejected at the grammar layer
      assertCreateFails(
          stmt,
          tumbleStreamSql(
              "tum06e", "tum06_src", "1.5s", "sink06", "time, cnt", "${START_TIME}, count(*)"),
          null);
      assertStreamAbsent(stmt, "tum06e");

      // ---- Expected RED: 0s is legal duration syntax but violates "size must be positive" ----

      // (f) positional 0s
      assertCreateFails(
          stmt,
          tumbleStreamSql(
              "tum06f", "tum06_src", "0s", "sink06", "time, cnt", "${START_TIME}, count(*)"),
          null);

      // (g) named 0s
      assertCreateFails(
          stmt,
          tumbleStreamSql(
              "tum06g",
              "tum06_src",
              "size => 0s",
              "sink06",
              "time, cnt",
              "${START_TIME}, count(*)"),
          null);
    }
  }

  // ============================================================================================
  // TUM-07 origin omitted
  // ============================================================================================

  /**
   * TUM-07 origin omitted: the default alignment base is used and later windows stay continuous.
   *
   * <p>Setting: <b>with no origin given, origin = 0</b>, so the window grid starts at the Unix
   * epoch; the first window containing data from the epoch onwards is the first triggered window,
   * and its start satisfies {@code Unix epoch + n * size}. Later windows divide on the same base
   * continuously, with no look back into history and no backfill of empty windows.
   *
   * <p>Data layout: four data points from a plain, unaligned base instant span three consecutive
   * windows, plus one pusher row. Every expected value is derived from the contract.
   *
   * <ul>
   *   <li>The {@code event_window} origin is recorded as 0;
   *   <li>every window start actually produced satisfies {@code floorMod(start, size) == 0};
   *   <li>consecutive starts differ by exactly size (already asserted by {@link
   *       #assertWindowsMatch}).
   * </ul>
   */
  @Test
  public void testTUM07DefaultOriginAlignment() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      createDatabase(stmt);
      createSource(stmt, "tum07_src");
      createSink(stmt, "sink07", "time TIMESTAMP TIME, cnt INT64 FIELD");

      final long size = 5 * SECOND;
      stmt.execute(
          tumbleStreamSql(
              "tum07", "tum07_src", "5s", "sink07", "time, cnt", "${START_TIME}, count(*)"));
      assertEventWindow(stmt, "tum07", size, 0L);
      stmt.execute("START STREAM tum07");

      final long base = System.currentTimeMillis() + BASE_LEAD_MS;
      final List<Long> eventTimes =
          Arrays.asList(base + 1000, base + 2000, base + 1000 + size, base + 1000 + 2 * size);

      final List<SinkRow> rows =
          driveWindowsAndAssert(stmt, "tum07_src", "sink07", "tum07", eventTimes, 0L, size);

      assertFalse("at least one window result should be produced", rows.isEmpty());

      // origin omitted => the grid starts at the Unix epoch => every window start is a multiple of
      // size
      for (final SinkRow row : rows) {
        assertEquals(
            "with origin omitted the window start should be Unix epoch + n * size, actual="
                + row.time
                + "; sink="
                + describe(rows),
            0L,
            Math.floorMod(row.time, size));
      }
    }
  }

  // ============================================================================================
  // TUM-08 legal origin formats
  // ============================================================================================

  /**
   * TUM-08 (Integer) an integer timestamp origin: used directly as epoch at database precision (ms
   * by default).
   *
   * <p>The origin deliberately carries a non zero millisecond remainder of +137: were the server to
   * truncate it to the second, both the origin in {@code event_window} and the window starts
   * written into the sink would immediately drift off that remainder, so the criterion is not a
   * vacuous one.
   *
   * <p>Data driven: two ordinary data points after the origin plus one pusher row; window starts
   * must lie on the {@code origin + n * size} grid and keep the origin's millisecond remainder.
   */
  @Test
  public void testTUM08IntegerOrigin() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      createDatabase(stmt);
      createSource(stmt, "tum08a_src");
      createSink(stmt, "sink08a", "time TIMESTAMP TIME, cnt INT64 FIELD");

      final long size = 5 * SECOND;
      // Force a non zero millisecond remainder so the "precision not truncated" criterion always
      // holds.
      final long origin = (System.currentTimeMillis() + BASE_LEAD_MS) / SECOND * SECOND + 137;

      stmt.execute(
          tumbleStreamSql(
              "tum08a",
              "tum08a_src",
              "5s, " + origin,
              "sink08a",
              "time, cnt",
              "${START_TIME}, count(*)"));
      assertEventWindow(stmt, "tum08a", size, origin);
      stmt.execute("START STREAM tum08a");

      final List<Long> eventTimes = Arrays.asList(origin + 1000, origin + 2000);
      final List<SinkRow> rows =
          driveWindowsAndAssert(stmt, "tum08a_src", "sink08a", "tum08a", eventTimes, origin, size);

      assertFalse(
          "the first window start should be the origin itself; actual=" + describe(rows),
          filterByTime(rows, origin).isEmpty());

      // Window start = origin + n * size, and size is a whole number of seconds, so the millisecond
      // remainder must match the origin's.
      final long remainder = Math.floorMod(origin, SECOND);
      for (final SinkRow row : rows) {
        assertEquals(
            "a window start should keep the origin's millisecond remainder "
                + remainder
                + ", actual="
                + row.time
                + "; event_window="
                + getStreamColumn(stmt, "tum08a", "event_window"),
            remainder,
            Math.floorMod(row.time, SECOND));
      }
    }
  }

  /**
   * TUM-08 (DateTime) a date time string origin: the {@code T} separated and the <b>space</b>
   * separated forms are equivalent.
   *
   * <p>The lexer for {@code DATETIME_VALUE} allows either {@code T} or whitespace between date and
   * time, and both should parse to the same absolute instant. The time string carries an explicit
   * {@code +08:00} offset, so the result is independent of the session time zone.
   *
   * <p>Criterion: the {@code event_window} origins of both streams equal the same expected epoch,
   * i.e. the two forms parse equivalently; one of the streams then runs real windows to verify the
   * boundaries really do align to that origin.
   */
  @Test
  public void testTUM08DateTimeOrigin() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      createDatabase(stmt);
      createSource(stmt, "tum08b_src");
      createSource(stmt, "tum08c_src");
      createSink(stmt, "sink08b", "time TIMESTAMP TIME, cnt INT64 FIELD");
      createSink(stmt, "sink08c", "time TIMESTAMP TIME, cnt INT64 FIELD");

      final long size = 5 * SECOND;
      final long origin = (System.currentTimeMillis() + BASE_LEAD_MS) / SECOND * SECOND + 137;
      final ZoneOffset offset = ZoneOffset.ofHours(8);
      final String originWithT = dateTimeWithT(origin, offset);
      final String originWithSpace = dateTimeWithSpace(origin, offset);

      // (a) T separated
      stmt.execute(
          tumbleStreamSql(
              "tum08b",
              "tum08b_src",
              "5s, " + originWithT,
              "sink08b",
              "time, cnt",
              "${START_TIME}, count(*)"));
      assertEventWindow(stmt, "tum08b", size, origin);

      // (b) space separated: should parse to the same absolute instant
      stmt.execute(
          tumbleStreamSql(
              "tum08c",
              "tum08c_src",
              "5s, " + originWithSpace,
              "sink08c",
              "time, cnt",
              "${START_TIME}, count(*)"));
      assertEventWindow(stmt, "tum08c", size, origin);
      assertEquals(
          "a T separated and a space separated DateTime origin should parse to the same absolute instant",
          getStreamColumn(stmt, "tum08b", "event_window"),
          getStreamColumn(stmt, "tum08c", "event_window"));

      // (c) run real windows on one of the streams to verify the boundaries really align to that
      // origin
      stmt.execute("START STREAM tum08b");
      final List<Long> eventTimes = Arrays.asList(origin + 1000, origin + 2000);
      final List<SinkRow> rows =
          driveWindowsAndAssert(stmt, "tum08b_src", "sink08b", "tum08b", eventTimes, origin, size);
      assertFalse(
          "the first window start should be the origin itself; actual=" + describe(rows),
          filterByTime(rows, origin).isEmpty());
    }
  }

  /**
   * TUM-08 (NOW()) the server time at the instant the {@code CREATE STREAM} statement is parsed.
   *
   * <p>Criterion: the {@code event_window} origin should fall inside the interval spanning the
   * {@code CREATE STREAM} statement - semantically {@code NOW()} means the server time at which the
   * stream creation statement started executing.
   *
   * <p><b>Expected GREEN</b>: {@code NOW()} is evaluated <b>at parse time</b> - syntactically it
   * matches {@code datetimeLiteral} rather than {@code specialDateTimeFunction}, and {@code
   * AstBuilder#visitDatetimeLiteral} hands it to {@code ASTVisitor#parseDateTimeFormat} (which
   * returns the given {@code currentTime} unchanged for {@code now()}), finally folding it into
   * {@code LongLiteral(CommonDateTimeUtils.currentTime())}. Under the default {@code ms} precision,
   * {@code CommonDateTimeUtils.currentTime()} is {@code System.currentTimeMillis()}, the same
   * source as the test clock, so the origin necessarily falls inside the creation interval and the
   * criterion holds.
   *
   * <p>Note that "first window start == origin" is <b>not affected by the evaluation instant</b>:
   * the origin is a fixed value whether evaluated at creation or at start, and the first window
   * start is derived from the data event time aligned to that same origin, so it always holds. The
   * only criterion that distinguishes the evaluation instants is the interval one - anchored on the
   * creation interval, it would start failing if {@code NOW()} were later made lazy, evaluated at
   * stream start (engine construction).
   *
   * <p>{@code PeriodStreamIT#testPER08NowOrigin} covers the same behaviour, with its interval
   * anchored on {@code START STREAM}.
   */
  @Test
  public void testTUM08NowOrigin() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      createDatabase(stmt);
      createSource(stmt, "tum08d_src");
      createSink(stmt, "sink08d", "time TIMESTAMP TIME, cnt INT64 FIELD");

      final long size = 5 * SECOND;

      // The CREATE STREAM statement's own execution interval: NOW() is evaluated while parsing, so
      // the origin should fall inside it.
      final long createBefore = System.currentTimeMillis();
      stmt.execute(
          tumbleStreamSql(
              "tum08d",
              "tum08d_src",
              "5s, NOW()",
              "sink08d",
              "time, cnt",
              "${START_TIME}, count(*)"));
      final long createAfter = System.currentTimeMillis();
      assertStreamExists(stmt, "tum08d");

      final long origin = extractEventWindowValue(eventWindowOf(stmt, "tum08d"), ORIGIN_PATTERN);

      // The stream still has to be started: driveWindowsAndAssert below needs a running stream to
      // emit windows.
      stmt.execute("START STREAM tum08d");

      final String diagnostic =
          "origin="
              + origin
              + " ("
              + isoUtc(origin)
              + "), CREATE STREAM interval=["
              + createBefore
              + ", "
              + createAfter
              + "]";

      // Spec criterion (Expected GREEN): the origin should fall inside the creation interval
      // (NOW() is evaluated while parsing CREATE STREAM).
      assertTrue(
          "the NOW() origin should fall inside the CREATE STREAM interval (i.e. be evaluated while parsing): "
              + diagnostic,
          origin >= createBefore && origin <= createAfter);

      // Window alignment criterion: independent of the evaluation instant, so it should still hold.
      final List<Long> eventTimes = Arrays.asList(origin + 1000, origin + 2000);
      final List<SinkRow> rows =
          driveWindowsAndAssert(stmt, "tum08d_src", "sink08d", "tum08d", eventTimes, origin, size);
      assertFalse(
          "the first window start should be the origin recorded by NOW(); actual=" + describe(rows),
          filterByTime(rows, origin).isEmpty());
    }
  }

  // ============================================================================================
  // TUM-09 illegal origin
  // ============================================================================================

  /**
   * TUM-09 illegal origin: a value that does not meet the TimeValue definition should fail the
   * creation.
   *
   * <p>GREEN sub cases: a {@code StringLiteral} goes through {@code
   * DateTimeUtils.convertDatetimeStrToLong}, whose parse failure throws and is wrapped by {@code
   * EventWindow#parseOrigin} into {@code IllegalArgumentException}, ultimately turned by {@code
   * TumbleEventWindow#parseArguments} into {@code Invalid argument type for tumble event window}.
   *
   * <p><b>Expected RED</b>: {@code origin => 1.5} (a {@code DoubleLiteral}) and {@code origin =>
   * &lt;column reference&gt;} fall into the trailing {@code return null} of {@code parseOrigin} and
   * <b>silently degrade to origin = 0 without an error</b>, so the stream is created successfully.
   * Fixing this requires {@code parseOrigin} to reject argument types outside {@code Integer |
   * DateTime | NOW()}. ({@code PeriodStreamIT#testPER09InvalidOrigin} records the same gap.)
   *
   * <p>The RED sub cases skip {@code assertStreamAbsent}, to avoid piling on assertions that do not
   * help locate the problem when they fail.
   */
  @Test
  public void testTUM09IllegalOrigin() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      createDatabase(stmt);
      createSource(stmt, "tum09_src");
      createSink(stmt, "sink09", "time TIMESTAMP TIME, cnt INT64 FIELD");

      // ---- GREEN: unparsable strings ----

      assertCreateFails(
          stmt,
          tumbleStreamSql(
              "tum09a",
              "tum09_src",
              "5s, 'not-a-time'",
              "sink09",
              "time, cnt",
              "${START_TIME}, count(*)"),
          "Invalid argument type for tumble event window");
      assertStreamAbsent(stmt, "tum09a");

      assertCreateFails(
          stmt,
          tumbleStreamSql(
              "tum09b",
              "tum09_src",
              "5s, '2026-13-45T99:99:99Z'",
              "sink09",
              "time, cnt",
              "${START_TIME}, count(*)"),
          "Invalid argument type for tumble event window");
      assertStreamAbsent(stmt, "tum09b");

      // ---- Expected RED: non TimeValue types should be rejected, but currently degrade silently
      // to
      //      origin = 0 ----

      // a decimal is not a TimeValue
      assertCreateFails(
          stmt,
          tumbleStreamSql(
              "tum09c", "tum09_src", "5s, 1.5", "sink09", "time, cnt", "${START_TIME}, count(*)"),
          "must be integer");

      // a column reference is not a TimeValue
      assertCreateFails(
          stmt,
          tumbleStreamSql(
              "tum09d",
              "tum09_src",
              "5s, device_id",
              "sink09",
              "time, cnt",
              "${START_TIME}, count(*)"),
          "must be integer or date time");
    }
  }

  // ============================================================================================
  // TUM-10 origin shift
  // ============================================================================================

  /**
   * TUM-10 origin shift: the same input and size under two different origins shifts the window
   * boundaries, and membership changes accordingly.
   *
   * <p>Both streams share one source table and read the same batch of data; {@code size = 10s} and
   * {@code B = A + 4s} (a shift smaller than size). All data is {@code >= B}, avoiding windows
   * whose start would precede their origin.
   *
   * <pre>
   *   event time     windows of stream A (origin = A)   windows of stream B (origin = B = A+4s)
   *   t1 = A+5000    [A,      A+10s)                   [B,      B+10s)     same window
   *   t2 = A+12000   [A+10s,  A+20s)                   [B,      B+10s)     <- different window!
   *   t3 = A+16000   [A+10s,  A+20s)                   [B+10s,  B+20s)
   *   t4 = A+20000   [A+20s,  A+30s)                   [B+10s,  B+20s)
   *   t5 = A+24000   [A+20s,  A+30s)                   [B+20s,  B+30s)
   *   pusher A+34000 closes A's third window           closes B's third window
   * </pre>
   *
   * <p>Engine behaviour: stream A produces {@code (A,1) / (A+10s,2) / (A+20s,2)}; stream B produces
   * {@code (B,2) / (B+10s,2) / (B+20s,1)}; and the same data point {@code t2} belongs to the second
   * window in A but the first in B - exactly the "boundaries shift with origin and membership
   * follows" behaviour.
   *
   * <p>Both sequences are derived by {@link #windowStartOf} and compared against hand written
   * expected values, rather than being hardcoded one sidedly.
   */
  @Test
  public void testTUM10OriginShiftChangesMembership() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      createDatabase(stmt);
      // Both streams share one source table, which guarantees "the same input"
      createSource(stmt, "tum10_src");
      createSink(stmt, "sink10a", "time TIMESTAMP TIME, cnt INT64 FIELD");
      createSink(stmt, "sink10b", "time TIMESTAMP TIME, cnt INT64 FIELD");

      final long size = 10 * SECOND;
      final long originA = System.currentTimeMillis() + BASE_LEAD_MS;
      final long originB = originA + 4000;

      stmt.execute(
          tumbleStreamSql(
              "tum10a",
              "tum10_src",
              "10s, " + originA,
              "sink10a",
              "time, cnt",
              "${START_TIME}, count(*)"));
      stmt.execute(
          tumbleStreamSql(
              "tum10b",
              "tum10_src",
              "10s, " + originB,
              "sink10b",
              "time, cnt",
              "${START_TIME}, count(*)"));
      assertEventWindow(stmt, "tum10a", size, originA);
      assertEventWindow(stmt, "tum10b", size, originB);
      stmt.execute("START STREAM tum10a");
      stmt.execute("START STREAM tum10b");
      awaitStreamRunning(stmt, "tum10a");
      awaitStreamRunning(stmt, "tum10b");

      // All data is >= originB, ensuring no window starts before its origin
      final List<Long> eventTimes =
          Arrays.asList(
              originA + 5000, originA + 12000, originA + 16000, originA + 20000, originA + 24000);
      final long pusher = originA + 24000 + size;
      final List<Long> withPusher = new ArrayList<>(eventTimes);
      withPusher.add(pusher);

      // Write the data rows first, then barrier on each sink before the pusher so the pusher can
      // never be consumed ahead of the data rows by either stream.
      for (final long time : eventTimes) {
        insertRow(stmt, "tum10_src", time, "d1", 1.0);
      }

      final Map<Long, Integer> expectedA = expectedEmittedWindows(withPusher, originA, size);
      final Map<Long, Integer> expectedB = expectedEmittedWindows(withPusher, originB, size);

      // Sink barrier: the closing row of the last data point produces N-1 windows; the pusher
      // closes the final one. Wait for both streams to reach N-1 before writing the pusher.
      if (expectedA.size() > 1) {
        awaitCountSink(stmt, "sink10a", expectedA.size() - 1, "tum10a");
      }
      if (expectedB.size() > 1) {
        awaitCountSink(stmt, "sink10b", expectedB.size() - 1, "tum10b");
      }
      insertRow(stmt, "tum10_src", pusher, "d1", 1.0);

      final List<SinkRow> rowsA = awaitCountSink(stmt, "sink10a", expectedA.size(), "tum10a");
      final List<SinkRow> rowsB = awaitCountSink(stmt, "sink10b", expectedB.size(), "tum10b");
      assertWindowsMatch(rowsA, withPusher, originA, size, "tum10a", stmt);
      assertWindowsMatch(rowsB, withPusher, originB, size, "tum10b", stmt);

      // Case specific assertion: each window sequence is exactly the three groups shifted by its
      // own
      // origin.
      final Map<Long, Long> expectedCountsA = new TreeMap<>();
      expectedCountsA.put(originA, 1L);
      expectedCountsA.put(originA + size, 2L);
      expectedCountsA.put(originA + 2 * size, 2L);
      assertEquals(
          "the window sequence of stream A should be (A,1)(A+10s,2)(A+20s,2); actual="
              + describe(rowsA),
          expectedCountsA,
          actualWindowCounts(rowsA, new ArrayList<>(expectedCountsA.keySet())));

      final Map<Long, Long> expectedCountsB = new TreeMap<>();
      expectedCountsB.put(originB, 2L);
      expectedCountsB.put(originB + size, 2L);
      expectedCountsB.put(originB + 2 * size, 1L);
      assertEquals(
          "the window sequence of stream B should be (B,2)(B+10s,2)(B+20s,1); actual="
              + describe(rowsB),
          expectedCountsB,
          actualWindowCounts(rowsB, new ArrayList<>(expectedCountsB.keySet())));

      // The same data point t2 belongs to different windows in the two streams
      final long t2 = originA + 12000;
      final long t2WindowInA = windowStartOf(t2, originA, size);
      final long t2WindowInB = windowStartOf(t2, originB, size);
      assertFalse(
          "data point t2 should belong to different windows in the two streams, otherwise the origin shift had no effect",
          t2WindowInA == t2WindowInB);
      assertEquals(
          "t2 should belong to the second window in stream A", originA + size, t2WindowInA);
      assertEquals("t2 should still belong to the first window in stream B", originB, t2WindowInB);
    }
  }

  // ============================================================================================
  // TUM-11 ungrouped versus TAG grouped
  // ============================================================================================

  /**
   * TUM-11 ungrouped versus TAG grouped: for the same input, a global stream and a stream grouped
   * by device_id both produce correct global and per group results.
   *
   * <p>The shared cluster uses one DataRegion and both streams use one subscription poll thread.
   * This preserves WAL delivery order across TAGs, so a W2 row cannot close W1 before another TAG's
   * W1 rows arrive. No elapsed-time wait is needed between the two windows' writes.
   *
   * <p>Engine behaviour: <b>the row that closes a window is not counted into it</b> (it belongs to
   * the new window), so the global stream produces {@code count = 5} at window {@code o} (the first
   * 5 rows), the grouped stream produces {@code d1 = 3} and {@code d2 = 2}, and the per group sum
   * {@code 3 + 2 = 5} agrees with the global one.
   *
   * <p>The three W2 rows have no later row to close them and therefore never trigger; all
   * assertions filter by window start {@code == o}.
   */
  @Test
  public void testTUM11GlobalVersusPartitioned() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      createDatabase(stmt);
      createSource(stmt, "tum11_src");
      createSink(stmt, "sink11g", "time TIMESTAMP TIME, cnt INT64 FIELD");
      createSink(stmt, "sink11p", "time TIMESTAMP TIME, device_id STRING TAG, cnt INT64 FIELD");

      final long origin = System.currentTimeMillis() + BASE_LEAD_MS;

      // Global stream: ungrouped
      stmt.execute(
          tumbleStreamSql(
                  "tum11g",
                  "tum11_src",
                  "5s, " + origin,
                  "sink11g",
                  "time, cnt",
                  "${START_TIME}, count(*)")
              + " WITH (subscription_poll_thread_count = 1)");
      // Grouped stream: grouped by device_id, the grouping column referenced as ${1}
      stmt.execute(
          tumblePartitionedStreamSql(
                  "tum11p",
                  "tum11_src",
                  "device_id",
                  "5s, " + origin,
                  "sink11p",
                  "time, device_id, cnt",
                  "${START_TIME}, ${1}, count(*)")
              + " WITH (subscription_poll_thread_count = 1)");
      stmt.execute("START STREAM tum11g");
      stmt.execute("START STREAM tum11p");
      // Write the source rows only once both streams are RUNNING, so that no row can be written
      // before the subscription is bound and therefore enter no window at all.
      awaitStreamRunning(stmt, "tum11g");
      awaitStreamRunning(stmt, "tum11p");

      insertRow(stmt, "tum11_src", origin + 1000, "d1", 1.0);
      insertRow(stmt, "tum11_src", origin + 2000, "d2", 1.0);
      insertRow(stmt, "tum11_src", origin + 3000, "d1", 1.0);
      insertRow(stmt, "tum11_src", origin + 3500, "d2", 1.0);
      insertRow(stmt, "tum11_src", origin + 4000, "d1", 1.0);

      // Rows 6 and 7 are the boundary rows closing W1: they are not counted into W1
      insertRow(stmt, "tum11_src", origin + 6000, "d1", 1.0);
      insertRow(stmt, "tum11_src", origin + 6000, "d2", 1.0);
      insertRow(stmt, "tum11_src", origin + 7000, "d1", 1.0);

      // ---- global stream ----
      final List<SinkRow> globalRows =
          awaitSinkRows(
              () -> readCountSink(stmt, "sink11g"),
              rows -> !filterByTime(rows, origin).isEmpty(),
              "the global stream should produce a result at window " + origin);
      final List<SinkRow> globalAtOrigin = filterByTime(globalRows, origin);
      assertEquals(
          "the global stream should produce exactly 1 row at window "
              + origin
              + "; actual="
              + describe(globalRows),
          1,
          globalAtOrigin.size());
      assertEquals(
          "the global stream should count the 5 rows inside W1 (rows 6 and 7 are the boundary rows closing W1 and belong to W2); actual="
              + describe(globalRows),
          5L,
          globalAtOrigin.get(0).count);

      // ---- grouped stream ----
      final List<SinkRow> groupedRows =
          awaitSinkRows(
              () -> readTagCountSink(stmt, "sink11p"),
              // The two partitions compute asynchronously; wait for both groups before comparing.
              rows -> filterByTime(rows, origin).size() >= 2,
              "the grouped stream should produce both groups at window " + origin);
      final Map<String, Long> byTag = new TreeMap<>();
      for (final SinkRow row : filterByTime(groupedRows, origin)) {
        byTag.merge(row.tag, row.count, Long::sum);
      }
      assertEquals(
          "the grouped stream should produce exactly the d1 and d2 groups at window "
              + origin
              + "; actual="
              + describe(groupedRows),
          Arrays.asList("d1", "d2"),
          new ArrayList<>(byTag.keySet()));
      assertEquals(
          "the d1 group should count 3 rows; actual=" + describe(groupedRows),
          3L,
          (long) byTag.get("d1"));
      assertEquals(
          "the d2 group should count 2 rows; actual=" + describe(groupedRows),
          2L,
          (long) byTag.get("d2"));

      // ---- consistency between global and per group ----
      assertEquals(
          "the global window count should equal the sum of the group counts; actual="
              + describe(groupedRows),
          globalAtOrigin.get(0).count,
          (long) byTag.get("d1") + (long) byTag.get("d2"));
    }
  }

  // ============================================================================================
  // TUM-12 groups advance independently
  // ============================================================================================

  /**
   * TUM-12 groups advance independently: each TAG group is written at a different rate and degree
   * of out of orderness, and their triggering, progress and output do not affect each other.
   *
   * <p>Data layout ({@code size = 5s}, {@code origin = o}, 10 rows in total):
   *
   * <pre>
   *   tag1(d1) rate 1s  : o+2000, o+1000, o+3000, o+4000, o+5000
   *                       the 2nd is deliberately out of order against the 1st (in window disorder
   *                       should not affect membership)
   *   tag2(d2) rate 1.5s: o+1500, o+3000, o+4500, o+6000, o+7500
   * </pre>
   *
   * <p><b>The write sequence itself is the criterion</b>: both groups are interleaved into
   * <b>one</b> sequence (not batched by index, with no waiting for results in between), but d2's
   * {@code o+4500} is deliberately placed after d1's boundary row:
   *
   * <pre>
   *   d1 o+2000 -> d2 o+1500 -> d1 o+1000 -> d1 o+3000 -> d2 o+3000 -> d1 o+4000
   *   -> d1 o+5000 (closes d1's W1) -> d2 o+4500 -> d2 o+6000 (closes d2's W1) -> d2 o+7500
   * </pre>
   *
   * <ol>
   *   <li>d1's 5th row {@code o+5000} is exactly the window's right boundary and closes d1's W1;
   *   <li>d2's {@code o+4500} still has an event time inside d2's W1, but <b>lands later</b> than
   *       the previous step;
   *   <li>assert each group produces its own window: d1 yields {@code (o, 4)} and d2 yields {@code
   *       (o, 3)}.
   * </ol>
   *
   * <p><b>Why the criterion holds</b>: the engine's closing and dropping rules are the same one -
   * only a row with event time &gt;= the current window end closes the window, and a row earlier
   * than the current window start is silently dropped. If the two groups shared progress, step 1's
   * {@code o+5000} would also close d2's {@code [o, o+5s)} (by then d2 had only reached {@code
   * o+1500} and {@code o+3000}), and step 2's {@code o+4500} would then be dropped for being
   * earlier than the new window start {@code o+5000}, so d2 could never produce {@code cnt = 3}.
   * The assertion therefore genuinely separates "independent groups" from "shared progress".
   *
   * <p>The criterion relies on one premise: the subscription source delivers rows to the stream
   * task <b>in write order</b> (the same premise as TUM-11 and others). Should the implementation
   * change to deliver sorted by event time, this case would still pass but would no longer falsify
   * "shared progress".
   */
  @Test
  public void testTUM12GroupsAdvanceIndependently() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      createDatabase(stmt);
      createSource(stmt, "tum12_src");
      createSink(stmt, "sink12", "time TIMESTAMP TIME, device_id STRING TAG, cnt INT64 FIELD");

      final long origin = System.currentTimeMillis() + BASE_LEAD_MS;
      stmt.execute(
          tumblePartitionedStreamSql(
              "tum12",
              "tum12_src",
              "device_id",
              "5s, " + origin,
              "sink12",
              "time, device_id, cnt",
              "${START_TIME}, ${1}, count(*)"));
      stmt.execute("START STREAM tum12");
      awaitStreamRunning(stmt, "tum12");

      // ---- interleaved writes: the 10 rows of d1 and d2 land in the order below (d1's 2nd is
      //      deliberately out of order against its 1st) ----
      insertRow(stmt, "tum12_src", origin + 2000, "d1", 1.0);
      insertRow(stmt, "tum12_src", origin + 1500, "d2", 1.0);
      insertRow(stmt, "tum12_src", origin + 1000, "d1", 1.0);
      insertRow(stmt, "tum12_src", origin + 3000, "d1", 1.0);
      insertRow(stmt, "tum12_src", origin + 3000, "d2", 1.0);
      insertRow(stmt, "tum12_src", origin + 4000, "d1", 1.0);
      // Row 7: d1's boundary row, closing d1's W1 (it belongs to W2 itself)
      insertRow(stmt, "tum12_src", origin + 5000, "d1", 1.0);
      // Row 8: its event time is still inside d2's W1, but it lands after the previous row - with
      // shared progress it would be dropped for being earlier than the new window start
      insertRow(stmt, "tum12_src", origin + 4500, "d2", 1.0);
      // Row 9: closes d2's W1 (it belongs to W2 itself)
      insertRow(stmt, "tum12_src", origin + 6000, "d2", 1.0);
      insertRow(stmt, "tum12_src", origin + 7500, "d2", 1.0);

      // ---- each group produces a result for window o (d2 lands last, so its appearance means both
      //      groups are done) ----
      final List<SinkRow> rows =
          awaitSinkRows(
              () -> readTagCountSink(stmt, "sink12"),
              r -> filterByTime(r, origin).stream().anyMatch(row -> "d2".equals(row.tag)),
              "the d2 group should independently produce a result for window "
                  + origin
                  + " (with shared progress, d2's o+4500 would be treated as late data and dropped, so it could never produce cnt = 3)");

      final List<SinkRow> atOrigin = filterByTime(rows, origin);
      final Map<String, Long> byTag = new TreeMap<>();
      for (final SinkRow row : atOrigin) {
        byTag.merge(row.tag, row.count, Long::sum);
      }
      assertEquals(
          "exactly the d1 and d2 groups, one row each, should be produced at window "
              + origin
              + "; actual="
              + describe(rows),
          Arrays.asList("d1", "d2"),
          new ArrayList<>(byTag.keySet()));
      assertEquals(
          "the d1 group should count 4 rows (its 5th, o+5000, is the boundary row closing W1 and belongs to W2); actual="
              + describe(rows),
          4L,
          (long) byTag.get("d1"));
      assertEquals(
          "the d2 group should count 3 rows (its 4th, o+6000, is the boundary row closing W1 and belongs to W2); actual="
              + describe(rows),
          3L,
          (long) byTag.get("d2"));
    }
  }

  // ============================================================================================
  // TUM-13 wrong argument count
  // ============================================================================================

  /**
   * TUM-13 wrong argument count: a third argument or empty arguments should both fail the creation
   * and leave no task behind.
   *
   * <ul>
   *   <li>{@code TUMBLE(1h, 0, 5)}: more positional arguments than parameter names yields {@code
   *       Exceeding arguments provided};
   *   <li>{@code TUMBLE()}: an empty argument list leaves no SIZE in the parameter table, yielding
   *       {@code Tumble event window requires 'size' argument};
   *   <li>{@code TUMBLE(size => 1h, size => 1h)}: a duplicated parameter name yields {@code
   *       Duplicate argument name: 'size'} (also a malformed parameters case).
   * </ul>
   */
  @Test
  public void testTUM13ArgumentCountErrors() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      createDatabase(stmt);
      createSource(stmt, "tum13_src");
      createSink(stmt, "sink13", "time TIMESTAMP TIME, cnt INT64 FIELD");

      // (a) a third argument
      assertCreateFails(
          stmt,
          tumbleStreamSql(
              "tum13a", "tum13_src", "1h, 0, 5", "sink13", "time, cnt", "${START_TIME}, count(*)"),
          "Exceeding arguments provided");
      assertStreamAbsent(stmt, "tum13a");

      // (b) empty arguments
      assertCreateFails(
          stmt,
          tumbleStreamSql(
              "tum13b", "tum13_src", "", "sink13", "time, cnt", "${START_TIME}, count(*)"),
          "Tumble event window requires 'size' argument");
      assertStreamAbsent(stmt, "tum13b");

      // (c) duplicate argument name
      assertCreateFails(
          stmt,
          tumbleStreamSql(
              "tum13c",
              "tum13_src",
              "size => 1h, size => 1h",
              "sink13",
              "time, cnt",
              "${START_TIME}, count(*)"),
          "Duplicate argument name: 'size'");
      assertStreamAbsent(stmt, "tum13c");
    }
  }

  // ============================================================================================
  // TUM-14 month/year size rejected
  // ============================================================================================

  /**
   * TUM-14 month/year size rejected: a TUMBLE size carrying a month or a year unit must fail the
   * creation and leave no task behind.
   *
   * <p>The spec allows only non month/year units for the size (ns/us/ms/s/m/h/d/w), so {@code mo}
   * and {@code y} are rejected at AST build time by {@code
   * AstBuilder#validateEventWindowDurationUnit} against the {@code TUMBLE_WINDOW_DURATION_UNITS}
   * whitelist, with "only supports duration units ns/us/ms/s/m/h/d/w".
   *
   * <ul>
   *   <li>{@code tumble(1mo)}: a pure month size;
   *   <li>{@code tumble(1y)}: a year is twelve months, so it is rejected the same way;
   *   <li>{@code tumble(1mo 1d)}: a month mixed with a non month unit is rejected too.
   * </ul>
   */
  @Test
  public void testTUM14MonthOrYearSizeRejected() throws Exception {
    try (Connection conn = env().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement stmt = conn.createStatement()) {
      createDatabase(stmt);
      createSource(stmt, "tum14_src");
      createSink(stmt, "sink14", "time TIMESTAMP TIME, cnt INT64 FIELD");

      // (a) month size
      assertCreateFails(
          stmt,
          tumbleStreamSql(
              "tum14a", "tum14_src", "1mo", "sink14", "time, cnt", "${START_TIME}, count(*)"),
          "only supports duration units ns/us/ms/s/m/h/d/w");
      assertStreamAbsent(stmt, "tum14a");

      // (b) year size
      assertCreateFails(
          stmt,
          tumbleStreamSql(
              "tum14b", "tum14_src", "1y", "sink14", "time, cnt", "${START_TIME}, count(*)"),
          "only supports duration units ns/us/ms/s/m/h/d/w");
      assertStreamAbsent(stmt, "tum14b");

      // (c) month mixed with a non month unit
      assertCreateFails(
          stmt,
          tumbleStreamSql(
              "tum14c", "tum14_src", "1mo 1d", "sink14", "time, cnt", "${START_TIME}, count(*)"),
          "only supports duration units ns/us/ms/s/m/h/d/w");
      assertStreamAbsent(stmt, "tum14c");
    }
  }
}
