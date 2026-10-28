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

package org.apache.iotdb.streamnode.engine.window;

import org.apache.iotdb.calc.execution.operator.Operator;
import org.apache.iotdb.commons.stream.PeriodWindow;
import org.apache.iotdb.streamnode.engine.scheduler.task.StreamDriverContext;
import org.apache.iotdb.streamnode.engine.sink.CommitTracker;
import org.apache.iotdb.streamnode.engine.task.StreamSubTask.DataSlice;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.Assert;
import org.junit.Test;

import java.time.Instant;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

import static org.apache.iotdb.streamnode.engine.window.WindowEngineTestUtils.createDriverContext;
import static org.apache.iotdb.streamnode.engine.window.WindowEngineTestUtils.printWindowData;

public class PeriodWindowEngineTest {

  private static final long PERIOD_MS = 200;

  private PeriodWindowEngine createEngine() {
    PeriodWindow window = new PeriodWindow(0, PERIOD_MS, 0L);
    window.setEventInputColumns(Collections.singletonList("col0"));
    return new PeriodWindowEngine(window, createDriverContext(), null);
  }

  private PeriodWindowEngine createEngine(long periodMs, long originMs) {
    PeriodWindow window = new PeriodWindow(0, periodMs, originMs);
    window.setEventInputColumns(Collections.singletonList("col0"));
    return new PeriodWindowEngine(window, createDriverContext(), null);
  }

  private DataSlice createDataSlice(int rowCount) {
    Tablet tablet =
        new Tablet(
            "root.test.device",
            Collections.singletonList(new MeasurementSchema("col0", TSDataType.INT64)),
            rowCount);
    for (int i = 0; i < rowCount; i++) {
      tablet.getTimestamps()[i] = i * 100L;
      tablet.addValue(i, "col0", (long) i);
    }
    tablet.setRowSize(rowCount);
    return new DataSlice(null, tablet, 0, rowCount, -1L);
  }

  private ListenableFuture<?> push(
      WindowEngine engine, DataSlice dataSlice, int startRow, int length, long tabletId) {
    if (dataSlice == null) {
      return engine.push(null);
    }
    return engine.push(
        Collections.singletonList(
            new DataSlice(
                dataSlice.getPartitionKey(),
                dataSlice.getTablet(),
                startRow,
                startRow + length,
                tabletId)));
  }

  private List<IEventInfo> getEventsFromQueue(PeriodWindowEngine engine) {
    List<IEventInfo> events = new ArrayList<>();
    while (engine.hasPendingComputeEvent()) {
      events.add(engine.pollComputeEvent());
    }
    return events;
  }

  @Test
  public void testPushThrowsUnsupportedOperation() {
    PeriodWindowEngine engine = createEngine();
    // The period window no longer accepts data; push must reject it uniformly.
    Assert.assertThrows(UnsupportedOperationException.class, () -> push(engine, null, 0, 0, 1L));
    DataSlice block = createDataSlice(2);
    Assert.assertThrows(UnsupportedOperationException.class, () -> push(engine, block, 0, 2, 1L));
    engine.forceClose();
    Assert.assertFalse(getEventsFromQueue(engine).isEmpty());
  }

  @Test
  public void testPeriodicTrigger() throws InterruptedException {
    PeriodWindowEngine engine = createEngine();

    // With the epoch origin (past origin) the first window is the next grid point, so the first
    // trigger is at most 2 periods (400ms) out; sleep past that.
    Thread.sleep(600);

    IEventInfo event = engine.peekComputeEvent();
    Assert.assertNotNull(event);
    printWindowData(Collections.singletonList(event));
    // Period windows skip CLOSE and emit DESTROY events directly (EOF, so the source commit can
    // advance once the event is flushed).
    Assert.assertTrue(event.isEOF());
    Assert.assertEquals(WindowEventType.WINDOW_DESTROY, event.getEventType());
    engine.forceClose();
  }

  @Test
  public void testPeriodicTriggerWithoutData() throws InterruptedException {
    PeriodWindowEngine engine = createEngine();
    Thread.sleep(600);

    Assert.assertTrue(
        "Period window should trigger even without data", engine.hasPendingComputeEvent());
    engine.forceClose();
  }

  @Test
  public void testMultiplePeriodsTrigger() throws InterruptedException {
    PeriodWindowEngine engine = createEngine();
    Thread.sleep(800);

    List<IEventInfo> events = getEventsFromQueue(engine);
    printWindowData(events);
    Assert.assertTrue("Expected multiple periodic events", events.size() >= 2);
    engine.forceClose();
  }

  /**
   * Period events carry {@code windowId = -1} and no window ever opens, so they can never gate
   * source commit: the driver's flush skips them entirely, and even a defensive {@code
   * onWindowDestroy(-1)} is a harmless no-op that fires nothing. Driven with a controlled clock so
   * it is fully deterministic.
   */
  @Test
  public void testPeriodicEventNeverGatesCommit() {
    List<Long> committed = new ArrayList<>();
    CommitTracker tracker = new CommitTracker(committed::add);
    StreamDriverContext driverContext = createDriverContext();
    driverContext.getSubTaskContext().setCommitTracker(tracker);

    long origin = epochMillisAt(2025, 1, 1, 0, 0) + 1L;
    long threeDays = TimeUnit.DAYS.toMillis(3);
    // Created just after the origin: past origin, so the first window is the next grid point
    // [origin+3d, origin+6d) - the partial window containing the creation instant is skipped.
    long[] clockHolder = {origin + 1L};
    PeriodWindowEngine engine = createMsEngine(threeDays, origin, clockHolder, driverContext);

    clockHolder[0] = origin + 2 * threeDays + 1L;
    engine.triggerPeriodicEvent();
    List<IEventInfo> events = getEventsFromQueue(engine);
    Assert.assertEquals(1, events.size());
    IEventInfo event = events.get(0);
    Assert.assertTrue(event.isEOF());
    // Period events carry no window id...
    Assert.assertEquals(-1L, event.getWindowId());
    // ...so flushing them through the tracker releases nothing and fires nothing.
    tracker.onWindowDestroy(event.getWindowId());
    Assert.assertTrue("period events must not gate source commit", committed.isEmpty());
    engine.forceClose();
  }

  @Test
  public void testForceCloseWithNoData() {
    PeriodWindowEngine engine = createEngine();
    ListenableFuture<?> future = engine.forceClose();
    Assert.assertSame(Operator.NOT_BLOCKED, future);
    Assert.assertFalse(getEventsFromQueue(engine).isEmpty());
  }

  @Test
  public void testEventQueueOperations() throws InterruptedException {
    PeriodWindowEngine engine = createEngine();
    Thread.sleep(600);

    Assert.assertTrue(engine.hasPendingComputeEvent());
    Assert.assertNotNull(engine.peekComputeEvent());
    Assert.assertNotNull(engine.pollComputeEvent());
    engine.forceClose();
  }

  @Test
  public void testPeriodicEventHasNullRowsIterator() throws InterruptedException {
    PeriodWindowEngine engine = createEngine();

    Thread.sleep(600);

    List<IEventInfo> events = getEventsFromQueue(engine);
    Assert.assertFalse(events.isEmpty());
    IEventInfo event = events.get(0);
    // The period window no longer accepts data; events carry a time range only (rowsIterator null).
    Assert.assertNull(event.getRowsIterator());
    printWindowData(events);
    engine.forceClose();
  }

  @Test
  public void testGetBlockedFuture() throws InterruptedException {
    PeriodWindowEngine engine = createEngine();
    // New engine with empty queue should return a blocking future (not done)
    Assert.assertFalse(engine.getBlockedFuture().isDone());

    // After scheduler triggers an event, future should be done
    Thread.sleep(600);
    Assert.assertTrue(engine.getBlockedFuture().isDone());
    engine.forceClose();
  }

  @Test
  public void testOriginAlignment() throws InterruptedException {
    PeriodWindowEngine originEngine = createEngine(200L, 100L);

    Thread.sleep(600);

    List<IEventInfo> events = new ArrayList<>();
    while (originEngine.hasPendingComputeEvent()) {
      events.add(originEngine.pollComputeEvent());
    }
    printWindowData(events);
    Assert.assertFalse(events.isEmpty());
    originEngine.forceClose();
  }

  // ===== Month-mode window (periodMonth) tests =====

  /** System-default time zone, used consistently by both the engine and the test expectations. */
  private static final TimeZone TZ = TimeZone.getDefault();

  /** Epoch millis for midnight on the given date in the test time zone. */
  private static long epochMillis(int year, int month, int day) {
    return LocalDateTime.of(year, month, day, 0, 0)
        .atZone(TZ.toZoneId())
        .toInstant()
        .toEpochMilli();
  }

  /** Day-of-month of an epoch-millis instant in the test time zone. */
  private static int dayOfMonth(long epochMillis) {
    return LocalDateTime.ofInstant(Instant.ofEpochMilli(epochMillis), TZ.toZoneId())
        .getDayOfMonth();
  }

  /** Epoch millis for the given local date/time in the test time zone. */
  private static long epochMillisAt(int year, int month, int day, int hour, int minute) {
    return LocalDateTime.of(year, month, day, hour, minute)
        .atZone(TZ.toZoneId())
        .toInstant()
        .toEpochMilli();
  }

  /**
   * Builds a month-mode engine with a controllable clock and the real scheduler disabled, so tests
   * can advance time and call {@link PeriodWindowEngine#triggerPeriodicEvent()} deterministically.
   */
  private PeriodWindowEngine createMonthEngine(
      int periodMonth, long periodMs, long originMs, long[] clockHolder) {
    PeriodWindow window = new PeriodWindow(periodMonth, periodMs, originMs);
    window.setEventInputColumns(Collections.singletonList("col0"));
    return new PeriodWindowEngine(window, createDriverContext(), null, () -> clockHolder[0]) {
      @Override
      protected void startScheduler() {
        // No-op: tests drive triggerPeriodicEvent() manually with a controlled clock.
      }
    };
  }

  /**
   * Ms-mode engine with a controllable clock and the real scheduler disabled, for deterministic
   * ms-window tests.
   */
  private PeriodWindowEngine createMsEngine(long periodMs, long originMs, long[] clockHolder) {
    return createMsEngine(periodMs, originMs, clockHolder, createDriverContext());
  }

  /** Variant of {@link #createMsEngine} with a caller-supplied driver context. */
  private PeriodWindowEngine createMsEngine(
      long periodMs, long originMs, long[] clockHolder, StreamDriverContext driverContext) {
    PeriodWindow window = new PeriodWindow(0, periodMs, originMs);
    window.setEventInputColumns(Collections.singletonList("col0"));
    return new PeriodWindowEngine(window, driverContext, null, () -> clockHolder[0]) {
      @Override
      protected void startScheduler() {
        // No-op: tests drive triggerPeriodicEvent() manually with a controlled clock.
      }
    };
  }

  /**
   * Origin on the last day of a month: every boundary stays on the last day of its month (Jan 31 ->
   * Feb 29 leap -> Mar 31 -> Apr 30 -> May 31).
   */
  @Test
  public void testMonthBoundaryLastDayPersistence() {
    long origin = epochMillis(2024, 1, 31); // Jan 31 2024 (leap year)
    Assert.assertEquals(
        epochMillis(2024, 2, 29), PeriodWindowEngine.monthBoundary(origin, 1, 0, 1, TZ));
    Assert.assertEquals(
        epochMillis(2024, 3, 31), PeriodWindowEngine.monthBoundary(origin, 1, 0, 2, TZ));
    Assert.assertEquals(
        epochMillis(2024, 4, 30), PeriodWindowEngine.monthBoundary(origin, 1, 0, 3, TZ));
    Assert.assertEquals(
        epochMillis(2024, 5, 31), PeriodWindowEngine.monthBoundary(origin, 1, 0, 4, TZ));
  }

  /**
   * Origin on the 30th of a 31-day month: 30/31-day months fire on the 30th; February clamps to its
   * last day.
   */
  @Test
  public void testMonthBoundary30thOfBigMonth() {
    long origin = epochMillis(2024, 1, 30); // Jan 30 (30th of a 31-day month)
    // Feb has no 30th -> clamped to Feb 29 (leap)
    Assert.assertEquals(
        epochMillis(2024, 2, 29), PeriodWindowEngine.monthBoundary(origin, 1, 0, 1, TZ));
    // Mar (31-day) restores the 30th
    Assert.assertEquals(
        epochMillis(2024, 3, 30), PeriodWindowEngine.monthBoundary(origin, 1, 0, 2, TZ));
    // Apr (30-day small month) fires on the 30th (= its last day)
    Assert.assertEquals(
        epochMillis(2024, 4, 30), PeriodWindowEngine.monthBoundary(origin, 1, 0, 3, TZ));
    // Jun (30-day small month) fires on the 30th
    Assert.assertEquals(
        epochMillis(2024, 6, 30), PeriodWindowEngine.monthBoundary(origin, 1, 0, 5, TZ));
  }

  /** February clamps to 29 in a leap year and 28 in a common year. */
  @Test
  public void testMonthBoundaryFebLeapVsCommon() {
    long originCommon = epochMillis(2023, 1, 31);
    Assert.assertEquals(
        epochMillis(2023, 2, 28), PeriodWindowEngine.monthBoundary(originCommon, 1, 0, 1, TZ));
    long originLeap = epochMillis(2024, 1, 31);
    Assert.assertEquals(
        epochMillis(2024, 2, 29), PeriodWindowEngine.monthBoundary(originLeap, 1, 0, 1, TZ));
  }

  /**
   * Requirement case 1 - an origin on Feb 29 of a leap year IS a month end, so every boundary takes
   * the last day of its month: 2024-02-29 -> 03-31 -> 04-30 -> 05-31 (time-of-day preserved).
   */
  @Test
  public void testMonthBoundaryLeapFeb29OriginAnchorsToMonthEnd() {
    long origin = epochMillisAt(2024, 2, 29, 14, 0);
    Assert.assertEquals(
        epochMillisAt(2024, 2, 29, 14, 0), PeriodWindowEngine.monthBoundary(origin, 1, 0, 0, TZ));
    Assert.assertEquals(
        epochMillisAt(2024, 3, 31, 14, 0), PeriodWindowEngine.monthBoundary(origin, 1, 0, 1, TZ));
    Assert.assertEquals(
        epochMillisAt(2024, 4, 30, 14, 0), PeriodWindowEngine.monthBoundary(origin, 1, 0, 2, TZ));
    Assert.assertEquals(
        epochMillisAt(2024, 5, 31, 14, 0), PeriodWindowEngine.monthBoundary(origin, 1, 0, 3, TZ));
  }

  /**
   * Requirement case 2 - Feb 28 of a leap year is NOT a month end (February has 29 days), so the
   * day-of-month is simply preserved: 2024-02-28 -> 03-28 -> 04-28 -> 05-28.
   */
  @Test
  public void testMonthBoundaryLeapFeb28OriginKeepsDayOfMonth() {
    long origin = epochMillis(2024, 2, 28);
    Assert.assertEquals(
        epochMillis(2024, 3, 28), PeriodWindowEngine.monthBoundary(origin, 1, 0, 1, TZ));
    Assert.assertEquals(
        epochMillis(2024, 4, 28), PeriodWindowEngine.monthBoundary(origin, 1, 0, 2, TZ));
    Assert.assertEquals(
        epochMillis(2024, 5, 28), PeriodWindowEngine.monthBoundary(origin, 1, 0, 3, TZ));
  }

  /**
   * Requirement case 5 - the 29th of a 31-day month (not a month end): day-of-month preserved, with
   * February (29 days in 2024) keeping the 29th: 2024-01-29 -> 02-29 -> 03-29 -> 04-29 -> 05-29.
   */
  @Test
  public void testMonthBoundary29thOfBigMonth() {
    long origin = epochMillis(2024, 1, 29);
    Assert.assertEquals(
        epochMillis(2024, 2, 29), PeriodWindowEngine.monthBoundary(origin, 1, 0, 1, TZ));
    Assert.assertEquals(
        epochMillis(2024, 3, 29), PeriodWindowEngine.monthBoundary(origin, 1, 0, 2, TZ));
    Assert.assertEquals(
        epochMillis(2024, 4, 29), PeriodWindowEngine.monthBoundary(origin, 1, 0, 3, TZ));
    Assert.assertEquals(
        epochMillis(2024, 5, 29), PeriodWindowEngine.monthBoundary(origin, 1, 0, 4, TZ));
  }

  /**
   * An origin on the 30th of a 30-day (small) month IS a month end, so every boundary takes the
   * last day of its month: 2024-04-30 -> 05-31 -> 06-30 -> 07-31.
   */
  @Test
  public void testMonthBoundarySmallMonth30thOriginAnchorsToMonthEnd() {
    long origin = epochMillis(2024, 4, 30);
    Assert.assertEquals(
        epochMillis(2024, 5, 31), PeriodWindowEngine.monthBoundary(origin, 1, 0, 1, TZ));
    Assert.assertEquals(
        epochMillis(2024, 6, 30), PeriodWindowEngine.monthBoundary(origin, 1, 0, 2, TZ));
    Assert.assertEquals(
        epochMillis(2024, 7, 31), PeriodWindowEngine.monthBoundary(origin, 1, 0, 3, TZ));
  }

  /**
   * Feb 28 of a common year IS a month end, so every boundary takes the last day of its month:
   * 2025-02-28 -> 03-31 -> 04-30 -> 05-31.
   */
  @Test
  public void testMonthBoundaryCommonYearFeb28OriginAnchorsToMonthEnd() {
    long origin = epochMillis(2025, 2, 28);
    Assert.assertEquals(
        epochMillis(2025, 3, 31), PeriodWindowEngine.monthBoundary(origin, 1, 0, 1, TZ));
    Assert.assertEquals(
        epochMillis(2025, 4, 30), PeriodWindowEngine.monthBoundary(origin, 1, 0, 2, TZ));
    Assert.assertEquals(
        epochMillis(2025, 5, 31), PeriodWindowEngine.monthBoundary(origin, 1, 0, 3, TZ));
  }

  /** Multi-month period (quarterly) and the epoch origin (windows on the 1st). */
  @Test
  public void testMonthBoundaryQuarterlyAndEpochOrigin() {
    long origin = epochMillis(2024, 1, 31);
    // periodMonth = 3: +1 period (3 months) -> Apr 30; +2 -> Jul 31
    Assert.assertEquals(
        epochMillis(2024, 4, 30), PeriodWindowEngine.monthBoundary(origin, 3, 0, 1, TZ));
    Assert.assertEquals(
        epochMillis(2024, 7, 31), PeriodWindowEngine.monthBoundary(origin, 3, 0, 2, TZ));
    // origin = 0 (the default when unspecified, i.e. the epoch): boundaries land on the 1st of each
    // month. (Asserted by day-of-month since epoch 0 is midnight UTC, which is a non-midnight local
    // instant in non-UTC zones.)
    Assert.assertEquals(1, dayOfMonth(PeriodWindowEngine.monthBoundary(0, 1, 0, 1, TZ)));
    Assert.assertEquals(1, dayOfMonth(PeriodWindowEngine.monthBoundary(0, 1, 0, 2, TZ)));
  }

  /** A non-month offset (periodMs) is added on top of each month advance. */
  @Test
  public void testMonthBoundaryWithNonMonthOffset() {
    long oneDay = TimeUnit.DAYS.toMillis(1);
    long origin = epochMillis(2024, 1, 15);
    // +1 month + 1 day: Feb 15 + 1 day = Feb 16
    Assert.assertEquals(
        epochMillis(2024, 2, 16), PeriodWindowEngine.monthBoundary(origin, 1, oneDay, 1, TZ));
  }

  /**
   * PERIOD(1mo, '2024-01-28T14:00:00'): trigger on the 28th at 14:00 each month. Day 28 is valid in
   * every month, so no clamping occurs and both day-of-month and time-of-day are preserved across
   * all five triggers.
   */
  @Test
  public void testMonthBoundary28thAt14Monthly() {
    long origin = epochMillisAt(2024, 1, 28, 14, 0);
    Assert.assertEquals(
        epochMillisAt(2024, 1, 28, 14, 0), PeriodWindowEngine.monthBoundary(origin, 1, 0, 0, TZ));
    Assert.assertEquals(
        epochMillisAt(2024, 2, 28, 14, 0), PeriodWindowEngine.monthBoundary(origin, 1, 0, 1, TZ));
    Assert.assertEquals(
        epochMillisAt(2024, 3, 28, 14, 0), PeriodWindowEngine.monthBoundary(origin, 1, 0, 2, TZ));
    Assert.assertEquals(
        epochMillisAt(2024, 4, 28, 14, 0), PeriodWindowEngine.monthBoundary(origin, 1, 0, 3, TZ));
    Assert.assertEquals(
        epochMillisAt(2024, 5, 28, 14, 0), PeriodWindowEngine.monthBoundary(origin, 1, 0, 4, TZ));
  }

  /**
   * PERIOD(1mo, '2024-01-30T14:00:00'): trigger on the 30th at 14:00 each month. The anchor is the
   * 30th of a 31-day month. February (29 days in 2024) clamps the 30th down to the 29th; subsequent
   * months restore the 30th because each boundary is computed from the origin (non-accumulating) -
   * so March is the 30th, not the 29th that accumulating from Feb 29 would yield. April (a 30-day
   * month) fires on the 30th = its last day.
   */
  @Test
  public void testMonthBoundary30thAt14Monthly() {
    long origin = epochMillisAt(2024, 1, 30, 14, 0);
    Assert.assertEquals(
        epochMillisAt(2024, 1, 30, 14, 0), PeriodWindowEngine.monthBoundary(origin, 1, 0, 0, TZ));
    Assert.assertEquals(
        epochMillisAt(2024, 2, 29, 14, 0), PeriodWindowEngine.monthBoundary(origin, 1, 0, 1, TZ));
    Assert.assertEquals(
        epochMillisAt(2024, 3, 30, 14, 0), PeriodWindowEngine.monthBoundary(origin, 1, 0, 2, TZ));
    Assert.assertEquals(
        epochMillisAt(2024, 4, 30, 14, 0), PeriodWindowEngine.monthBoundary(origin, 1, 0, 3, TZ));
    Assert.assertEquals(
        epochMillisAt(2024, 5, 30, 14, 0), PeriodWindowEngine.monthBoundary(origin, 1, 0, 4, TZ));
  }

  /**
   * PERIOD(1mo, '2024-01-31T14:00:00'): month-end trigger at 14:00. The Jan-31 anchor is the last
   * day of a 31-day month, so every boundary stays on the last day of its month (Feb 29 leap / Mar
   * 31 / Apr 30 / May 31), and the 14:00 time-of-day is preserved throughout.
   */
  @Test
  public void testMonthBoundaryLastDayAt14Monthly() {
    long origin = epochMillisAt(2024, 1, 31, 14, 0);
    Assert.assertEquals(
        epochMillisAt(2024, 1, 31, 14, 0), PeriodWindowEngine.monthBoundary(origin, 1, 0, 0, TZ));
    Assert.assertEquals(
        epochMillisAt(2024, 2, 29, 14, 0), PeriodWindowEngine.monthBoundary(origin, 1, 0, 1, TZ));
    Assert.assertEquals(
        epochMillisAt(2024, 3, 31, 14, 0), PeriodWindowEngine.monthBoundary(origin, 1, 0, 2, TZ));
    Assert.assertEquals(
        epochMillisAt(2024, 4, 30, 14, 0), PeriodWindowEngine.monthBoundary(origin, 1, 0, 3, TZ));
    Assert.assertEquals(
        epochMillisAt(2024, 5, 31, 14, 0), PeriodWindowEngine.monthBoundary(origin, 1, 0, 4, TZ));
  }

  // ----- Non-leap-year (2025) counterparts: February has 28 days -----

  /**
   * Non-leap-year counterpart of {@link #testMonthBoundary28thAt14Monthly}: PERIOD(1mo,
   * '2025-01-28T14:00:00'). Day 28 is valid in February of a non-leap year (28 days) too, so no
   * clamping occurs and the 28th at 14:00 is preserved across all five triggers.
   */
  @Test
  public void testMonthBoundary28thAt14MonthlyNonLeap() {
    long origin = epochMillisAt(2025, 1, 28, 14, 0);
    Assert.assertEquals(
        epochMillisAt(2025, 1, 28, 14, 0), PeriodWindowEngine.monthBoundary(origin, 1, 0, 0, TZ));
    Assert.assertEquals(
        epochMillisAt(2025, 2, 28, 14, 0), PeriodWindowEngine.monthBoundary(origin, 1, 0, 1, TZ));
    Assert.assertEquals(
        epochMillisAt(2025, 3, 28, 14, 0), PeriodWindowEngine.monthBoundary(origin, 1, 0, 2, TZ));
    Assert.assertEquals(
        epochMillisAt(2025, 4, 28, 14, 0), PeriodWindowEngine.monthBoundary(origin, 1, 0, 3, TZ));
    Assert.assertEquals(
        epochMillisAt(2025, 5, 28, 14, 0), PeriodWindowEngine.monthBoundary(origin, 1, 0, 4, TZ));
  }

  /**
   * Non-leap-year counterpart of {@link #testMonthBoundary30thAt14Monthly}: PERIOD(1mo,
   * '2025-01-30T14:00:00'). February of a non-leap year has only 28 days, so the 30th clamps down
   * to Feb 28 (its last day); subsequent months restore the 30th (non-accumulating), so March is
   * the 30th, not the 28th that accumulating from Feb 28 would yield.
   */
  @Test
  public void testMonthBoundary30thAt14MonthlyNonLeap() {
    long origin = epochMillisAt(2025, 1, 30, 14, 0);
    Assert.assertEquals(
        epochMillisAt(2025, 1, 30, 14, 0), PeriodWindowEngine.monthBoundary(origin, 1, 0, 0, TZ));
    Assert.assertEquals(
        epochMillisAt(2025, 2, 28, 14, 0), PeriodWindowEngine.monthBoundary(origin, 1, 0, 1, TZ));
    Assert.assertEquals(
        epochMillisAt(2025, 3, 30, 14, 0), PeriodWindowEngine.monthBoundary(origin, 1, 0, 2, TZ));
    Assert.assertEquals(
        epochMillisAt(2025, 4, 30, 14, 0), PeriodWindowEngine.monthBoundary(origin, 1, 0, 3, TZ));
    Assert.assertEquals(
        epochMillisAt(2025, 5, 30, 14, 0), PeriodWindowEngine.monthBoundary(origin, 1, 0, 4, TZ));
  }

  /**
   * Non-leap-year counterpart of {@link #testMonthBoundaryLastDayAt14Monthly}: PERIOD(1mo,
   * '2025-01-31T14:00:00'). The anchor is the last day of a 31-day month; February of a non-leap
   * year clamps to its last day (the 28th), while March/May keep the 31st and April the 30th - each
   * the last day of its month.
   */
  @Test
  public void testMonthBoundaryLastDayAt14MonthlyNonLeap() {
    long origin = epochMillisAt(2025, 1, 31, 14, 0);
    Assert.assertEquals(
        epochMillisAt(2025, 1, 31, 14, 0), PeriodWindowEngine.monthBoundary(origin, 1, 0, 0, TZ));
    Assert.assertEquals(
        epochMillisAt(2025, 2, 28, 14, 0), PeriodWindowEngine.monthBoundary(origin, 1, 0, 1, TZ));
    Assert.assertEquals(
        epochMillisAt(2025, 3, 31, 14, 0), PeriodWindowEngine.monthBoundary(origin, 1, 0, 2, TZ));
    Assert.assertEquals(
        epochMillisAt(2025, 4, 30, 14, 0), PeriodWindowEngine.monthBoundary(origin, 1, 0, 3, TZ));
    Assert.assertEquals(
        epochMillisAt(2025, 5, 31, 14, 0), PeriodWindowEngine.monthBoundary(origin, 1, 0, 4, TZ));
  }

  /**
   * Drives a month-mode engine with a controllable clock (real scheduler disabled): verifies events
   * fire at month boundaries and the window advances with correct last-day / leap-year semantics.
   * Constructed inside [Jan 31, Feb 29): past origin, so the first window is the next grid point
   * [Feb 29, Mar 31) - the partial window containing the creation instant never fires.
   */
  @Test
  public void testMonthModeEngineAdvancesAcrossBoundaries() {
    long origin = epochMillis(2024, 1, 31); // Jan 31 2024 (leap year)
    long[] clockHolder = {epochMillis(2024, 2, 1)}; // inside the first window [Jan 31, Feb 29)
    PeriodWindowEngine engine = createMonthEngine(1, 0, origin, clockHolder);

    // Advance the clock past the Mar 31 boundary: the first window [Feb 29, Mar 31) fires.
    clockHolder[0] = epochMillis(2024, 4, 1);
    engine.triggerPeriodicEvent();

    List<IEventInfo> firstEvents = getEventsFromQueue(engine);
    Assert.assertFalse("first month window should have fired", firstEvents.isEmpty());
    IEventInfo firstEvent = firstEvents.get(0);
    Assert.assertEquals(epochMillis(2024, 2, 29), firstEvent.getStartTime().getAsLong());
    Assert.assertEquals(epochMillis(2024, 3, 31), firstEvent.getEndTime().getAsLong());

    // Advance past the Apr 30 boundary. The next window fires an empty event for [Mar 31, Apr 30).
    clockHolder[0] = epochMillis(2024, 5, 1);
    engine.triggerPeriodicEvent();
    List<IEventInfo> secondEvents = getEventsFromQueue(engine);
    Assert.assertFalse("second month window should have fired", secondEvents.isEmpty());
    IEventInfo secondEvent = secondEvents.get(0);
    Assert.assertEquals(epochMillis(2024, 3, 31), secondEvent.getStartTime().getAsLong());
    Assert.assertEquals(epochMillis(2024, 4, 30), secondEvent.getEndTime().getAsLong());

    engine.forceClose();
  }

  /**
   * A regular (ms-mode) 3-day window: {@code PERIOD(3d, '2025-01-01T00:00:00.001')} created before
   * the origin. The requirement makes origin the first execution PERIOD, so the first window is
   * [origin, origin+3d) - the grid never extends before the origin - and each trigger fires at its
   * window's end: origin+3d, origin+6d, origin+9d.
   */
  @Test
  public void testRegular3DayWindowTriggers() {
    long origin = epochMillisAt(2025, 1, 1, 0, 0) + 1L; // 2025-01-01T00:00:00.001
    long threeDays = TimeUnit.DAYS.toMillis(3);
    // Clock starts 1ms before the origin (origin in the future).
    long[] clockHolder = {origin - 1L};
    PeriodWindowEngine engine = createMsEngine(threeDays, origin, clockHolder);

    // The origin has not arrived yet: nothing fires.
    engine.triggerPeriodicEvent();
    Assert.assertTrue("no fire before the origin arrives", getEventsFromQueue(engine).isEmpty());

    // At the origin itself nothing fires either: the first window [origin, origin+3d) has not
    // closed yet (the preceding period is not a window of this stream).
    clockHolder[0] = origin + 1L;
    engine.triggerPeriodicEvent();
    Assert.assertTrue(
        "no fire at the origin: the first window [origin, origin+3d) is still open",
        getEventsFromQueue(engine).isEmpty());

    // Window 1 [origin, origin+3d), window 2 [origin+3d, origin+6d),
    // window 3 [origin+6d, origin+9d).
    long[] expectedStart = {origin, origin + threeDays, origin + 2 * threeDays};
    long[] expectedEnd = {origin + threeDays, origin + 2 * threeDays, origin + 3 * threeDays};
    for (int i = 0; i < 3; i++) {
      // Advance the clock just past the i-th window end and trigger.
      clockHolder[0] = expectedEnd[i] + 1L;
      engine.triggerPeriodicEvent();
      List<IEventInfo> events = getEventsFromQueue(engine);
      Assert.assertEquals(1, events.size());
      Assert.assertEquals(expectedStart[i], events.get(0).getStartTime().getAsLong());
      Assert.assertEquals(expectedEnd[i], events.get(0).getEndTime().getAsLong());
    }
    engine.forceClose();
  }

  /**
   * Requirement: for a past origin the first execution period is the smallest {@code origin + n *
   * period} STRICTLY greater than the current time. Created in the middle of the [origin,
   * origin+3d) grid cell, the first window is therefore [origin+3d, origin+6d) - the partial window
   * containing the creation instant is never emitted, mirroring PER-01's "the first trigger should
   * not predate the stream start".
   */
  @Test
  public void testPastOriginFirstWindowIsNextGridPoint() {
    long origin = epochMillisAt(2025, 1, 1, 0, 0) + 1L;
    long threeDays = TimeUnit.DAYS.toMillis(3);
    long[] clockHolder = {origin + threeDays / 2}; // middle of [origin, origin+3d)
    PeriodWindowEngine engine = createMsEngine(threeDays, origin, clockHolder);

    // Just past the first grid point after creation (origin+3d): the window [origin+3d,
    // origin+6d) is still open, so nothing fires yet - in particular not the partial window
    // [origin, origin+3d).
    clockHolder[0] = origin + threeDays + 1L;
    engine.triggerPeriodicEvent();
    Assert.assertTrue(
        "past origin: nothing fires while the first full window is still open",
        getEventsFromQueue(engine).isEmpty());

    // The first event fires at origin+6d covering [origin+3d, origin+6d); prev_time is NULL on
    // the first emitted window.
    clockHolder[0] = origin + 2 * threeDays + 1L;
    engine.triggerPeriodicEvent();
    List<IEventInfo> events = getEventsFromQueue(engine);
    Assert.assertEquals(1, events.size());
    Assert.assertEquals(origin + threeDays, events.get(0).getStartTime().getAsLong());
    Assert.assertEquals(origin + 2 * threeDays, events.get(0).getEndTime().getAsLong());
    Assert.assertFalse(events.get(0).getPrevTime().isPresent());

    // The grid continues: the next trigger covers [origin+6d, origin+9d).
    clockHolder[0] = origin + 3 * threeDays + 1L;
    engine.triggerPeriodicEvent();
    events = getEventsFromQueue(engine);
    Assert.assertEquals(1, events.size());
    Assert.assertEquals(origin + 2 * threeDays, events.get(0).getStartTime().getAsLong());
    Assert.assertEquals(origin + 3 * threeDays, events.get(0).getEndTime().getAsLong());
    engine.forceClose();
  }

  /**
   * stop() pauses periodic emission (the trigger task keeps ticking but emits nothing); start()
   * resumes it and the next trigger catches up the window that elapsed during the pause. Driven
   * with a controlled clock so it is fully deterministic (no real-time waits).
   */
  @Test
  public void testStopPausesEmissionAndStartResumes() {
    long origin = epochMillisAt(2025, 1, 1, 0, 0) + 1L;
    long threeDays = TimeUnit.DAYS.toMillis(3);
    // Created just after the origin: past origin, so the first window is the next grid point
    // [origin+3d, origin+6d) - the partial window containing the creation instant is skipped.
    long[] clockHolder = {origin + 1L};
    PeriodWindowEngine engine = createMsEngine(threeDays, origin, clockHolder);

    // First window closes: not paused -> emits [origin+3d, origin+6d).
    clockHolder[0] = origin + 2 * threeDays + 1L;
    engine.triggerPeriodicEvent();
    Assert.assertEquals(1, getEventsFromQueue(engine).size());

    // stop() pauses emission: advancing past the next window close emits nothing.
    engine.stop();
    clockHolder[0] = origin + 3 * threeDays + 1L;
    engine.triggerPeriodicEvent();
    Assert.assertFalse("stop() should pause emission", engine.hasPendingComputeEvent());

    // start() resumes: the missed window is emitted on the next trigger (catch-up).
    engine.start();
    engine.triggerPeriodicEvent();
    List<IEventInfo> events = getEventsFromQueue(engine);
    Assert.assertEquals("start() should resume emission", 1, events.size());
    Assert.assertEquals(origin + 2 * threeDays, events.get(0).getStartTime().getAsLong());
    Assert.assertEquals(origin + 3 * threeDays, events.get(0).getEndTime().getAsLong());
    engine.forceClose();
  }

  /**
   * Month mode paused past its window end must not busy-loop the scheduler thread. The paused
   * trigger emits nothing and re-arms; re-arming at the (already elapsed) stale boundary means
   * delay 0 - an immediate re-schedule that spins a full CPU core. Uses the REAL scheduler with an
   * injected counting clock: stop() while the first window is open, push the clock past its end,
   * then re-arm exactly as {@code onMonthTrigger}'s finally-block would and measure how often the
   * clock is read. (Under the strictly-after-now first-window semantics the construction-time
   * one-shot is always at least one month out, so the stale-boundary re-arm is driven directly.)
   */
  @Test(timeout = 30_000)
  public void testPausedMonthModeSchedulerDoesNotBusyLoop() throws InterruptedException {
    long origin = epochMillisAt(2024, 1, 31, 0, 0);
    long firstStart = PeriodWindowEngine.monthBoundary(origin, 1, 0, 1, TZ); // Feb 29
    long firstEnd = PeriodWindowEngine.monthBoundary(origin, 1, 0, 2, TZ); // Mar 31
    // Inside [Jan 31, Feb 29): past origin, so the first window is [firstStart, firstEnd).
    long[] clockHolder = {firstStart - 50L};
    AtomicLong clockCalls = new AtomicLong();
    LongSupplier clock =
        () -> {
          clockCalls.incrementAndGet();
          return clockHolder[0];
        };

    PeriodWindow window = new PeriodWindow(1, 0, origin);
    window.setEventInputColumns(Collections.singletonList("col0"));
    PeriodWindowEngine engine = new PeriodWindowEngine(window, createDriverContext(), null, clock);

    // Pause while the first window [firstStart, firstEnd) is open, then let its end pass.
    engine.stop();
    clockHolder[0] = firstEnd + 1000L;
    // Re-arm as onMonthTrigger would: the boundary is already stale (delay 0), which must re-arm
    // at the bounded interval instead of spinning.
    engine.scheduleNextMonthTrigger();

    // With the busy loop, the clock is read millions of times per second; a well-behaved paused
    // scheduler re-arms at a bounded interval and reads it a handful of times.
    Thread.sleep(500);
    long callsAfterPause = clockCalls.get();

    Assert.assertTrue(
        "paused month-mode scheduler must not busy-loop (clock reads in 500ms: "
            + callsAfterPause
            + ")",
        callsAfterPause < 1000);

    // Resume: the window that elapsed during the pause is caught up within the bounded re-check
    // interval (1s) and emitted as an event for [firstStart, firstEnd).
    engine.start();
    IEventInfo caughtUp = null;
    long deadline = System.currentTimeMillis() + 5000;
    while (System.currentTimeMillis() < deadline) {
      caughtUp = engine.pollComputeEvent();
      if (caughtUp != null) {
        break;
      }
      Thread.sleep(20);
    }
    engine.drop();
    Assert.assertNotNull("start() after a paused month window should catch up its event", caughtUp);
    Assert.assertEquals(firstStart, caughtUp.getStartTime().getAsLong());
    Assert.assertEquals(firstEnd, caughtUp.getEndTime().getAsLong());
  }

  /**
   * Requirement: {@code origin >= now} makes origin itself the first execution period - month
   * mode's first window is [origin, next boundary); nothing fires at the origin itself (the month
   * preceding a future origin is not a window of this stream). The stream is created one day before
   * origin.
   */
  @Test
  public void testFutureOriginMonthModeFirstWindowStartsAtOrigin() {
    long origin = epochMillis(2024, 1, 31); // Jan 31 2024 (leap year, month-end origin)
    long[] clockHolder = {origin - TimeUnit.DAYS.toMillis(1)};
    PeriodWindowEngine engine = createMonthEngine(1, 0, origin, clockHolder);

    // At the origin itself nothing fires: the first window [Jan 31, Feb 29) is still open.
    clockHolder[0] = origin + 1L;
    engine.triggerPeriodicEvent();
    Assert.assertTrue(
        "origin in the future: no event at the origin itself (the first window is still open)",
        getEventsFromQueue(engine).isEmpty());

    // The first event fires at the boundary covering [Jan 31, Feb 29); prev_time is NULL on the
    // first emitted window.
    clockHolder[0] = epochMillis(2024, 2, 29) + 1L;
    engine.triggerPeriodicEvent();
    List<IEventInfo> events = getEventsFromQueue(engine);
    Assert.assertEquals(1, events.size());
    Assert.assertEquals(origin, events.get(0).getStartTime().getAsLong());
    Assert.assertEquals(epochMillis(2024, 2, 29), events.get(0).getEndTime().getAsLong());
    Assert.assertFalse(events.get(0).getPrevTime().isPresent());

    // The next trigger is one month out covering [Feb 29, Mar 31); prev_time = origin (the
    // previous window's start).
    clockHolder[0] = epochMillis(2024, 3, 31) + 1L;
    engine.triggerPeriodicEvent();
    events = getEventsFromQueue(engine);
    Assert.assertEquals(1, events.size());
    Assert.assertEquals(epochMillis(2024, 2, 29), events.get(0).getStartTime().getAsLong());
    Assert.assertEquals(epochMillis(2024, 3, 31), events.get(0).getEndTime().getAsLong());
    Assert.assertEquals(origin, events.get(0).getPrevTime().getAsLong());
    engine.forceClose();
  }

  /**
   * Requirement: {@code origin >= now} makes origin itself the first execution period - ms mode's
   * first window is [origin, origin+period) and its trigger fires at origin+period, never at the
   * origin itself and never on the backwards-extrapolated grid in front of it. The stream is
   * created 2.5 periods before origin.
   */
  @Test
  public void testFutureOriginMsModeFirstWindowStartsAtOrigin() {
    long periodMs = TimeUnit.DAYS.toMillis(3);
    long origin = epochMillisAt(2025, 1, 5, 0, 0);
    long[] clockHolder = {origin - 5 * periodMs / 2};
    PeriodWindowEngine engine = createMsEngine(periodMs, origin, clockHolder);

    // At origin nothing fires: the extrapolated windows in front of origin must not execute, and
    // the first window [origin, origin+3d) is still open at the origin itself.
    clockHolder[0] = origin + 1L;
    engine.triggerPeriodicEvent();
    List<IEventInfo> events = getEventsFromQueue(engine);
    Assert.assertTrue(
        "origin in the future: no event at the origin itself (the first window is still open)",
        events.isEmpty());

    // The first trigger fires one period out covering [origin, origin+3d); prev_time is NULL on
    // the first emitted window.
    clockHolder[0] = origin + periodMs + 1L;
    engine.triggerPeriodicEvent();
    events = getEventsFromQueue(engine);
    Assert.assertEquals(1, events.size());
    Assert.assertEquals(origin, events.get(0).getStartTime().getAsLong());
    Assert.assertEquals(origin + periodMs, events.get(0).getEndTime().getAsLong());
    Assert.assertFalse(events.get(0).getPrevTime().isPresent());

    // The grid continues from origin: the next trigger covers [origin+3d, origin+6d) with
    // prev_time = origin (the previous window's start).
    clockHolder[0] = origin + 2 * periodMs + 1L;
    engine.triggerPeriodicEvent();
    events = getEventsFromQueue(engine);
    Assert.assertEquals(1, events.size());
    Assert.assertEquals(origin + periodMs, events.get(0).getStartTime().getAsLong());
    Assert.assertEquals(origin + 2 * periodMs, events.get(0).getEndTime().getAsLong());
    Assert.assertEquals(origin, events.get(0).getPrevTime().getAsLong());
    engine.forceClose();
  }

  @Test
  public void testDropStopsScheduler() throws InterruptedException {
    PeriodWindowEngine engine = createEngine();
    engine.drop();
    // The scheduler is shut down; no periodic events fire afterwards.
    Thread.sleep(500);
    Assert.assertTrue("drop() should stop the scheduler", getEventsFromQueue(engine).isEmpty());
  }

  @Test
  public void testStartAfterDropIsNoOp() throws InterruptedException {
    PeriodWindowEngine engine = createEngine();
    engine.drop();
    // start() must not restart the shut-down scheduler (drop is terminal).
    engine.start();
    Thread.sleep(500);
    Assert.assertTrue(
        "start() after drop() should not restart the scheduler",
        getEventsFromQueue(engine).isEmpty());
  }

  @Test
  public void testPrevTimeNullOnFirstWindowThenPresent() {
    long origin = epochMillisAt(2025, 1, 1, 0, 0) + 1L;
    long threeDays = TimeUnit.DAYS.toMillis(3);
    // Created just after the origin: past origin, so the first window is the next grid point
    // [origin+3d, origin+6d) - the partial window containing the creation instant is skipped.
    long[] clockHolder = {origin + 1L};
    PeriodWindowEngine engine = createMsEngine(threeDays, origin, clockHolder);

    // First window [origin+3d, origin+6d) closes: prev_time is NULL (no previous trigger).
    clockHolder[0] = origin + 2 * threeDays + 1L;
    engine.triggerPeriodicEvent();
    List<IEventInfo> events = getEventsFromQueue(engine);
    Assert.assertEquals(1, events.size());
    Assert.assertEquals(origin + threeDays, events.get(0).getStartTime().getAsLong());
    Assert.assertFalse(
        "prev_time should be NULL on the first window", events.get(0).getPrevTime().isPresent());

    // Second window [origin+6d, origin+9d) closes: prev_time = previous window's start.
    clockHolder[0] = origin + 3 * threeDays + 1L;
    engine.triggerPeriodicEvent();
    List<IEventInfo> events2 = getEventsFromQueue(engine);
    Assert.assertEquals(1, events2.size());
    Assert.assertEquals(origin + 2 * threeDays, events2.get(0).getStartTime().getAsLong());
    Assert.assertTrue(events2.get(0).getPrevTime().isPresent());
    Assert.assertEquals(origin + threeDays, events2.get(0).getPrevTime().getAsLong());
    engine.forceClose();
  }
}
