/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.iotdb.commons.concurrent.threadpool.ScheduledExecutorUtil;
import org.apache.iotdb.commons.stream.PeriodWindow;
import org.apache.iotdb.commons.stream.StreamProperties;
import org.apache.iotdb.commons.stream.StreamWindow;
import org.apache.iotdb.streamnode.engine.scheduler.task.StreamDriverContext;
import org.apache.iotdb.streamnode.engine.task.StreamSubTask.DataSlice;
import org.apache.iotdb.streamnode.i18n.StreamNodeMessages;
import org.apache.iotdb.streamnode.metrics.StreamWindowEngineMetrics;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.tsfile.utils.TimeDuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.YearMonth;
import java.time.ZoneId;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongSupplier;

public class PeriodWindowEngine extends WindowEngine {

  private static final Logger LOGGER = LoggerFactory.getLogger(PeriodWindowEngine.class);

  /**
   * When {@code true}, the window advances by calendar months ({@code periodMonth > 0}) and takes
   * priority over {@code periodMs}; otherwise the window advances by a fixed {@code periodMs}.
   */
  private final boolean monthMode;

  private final int periodMonth;
  private final long periodMs;
  private final long originMs;

  /**
   * Time zone used for month-mode calendar arithmetic. {@link PeriodWindow} carries no zone, so the
   * system default is used.
   */
  private final TimeZone timeZone;

  /** Clock source so tests can drive time forward without waiting for real month boundaries. */
  private final LongSupplier clock;

  private volatile long currentWindowStart;
  private volatile long currentWindowEnd;

  /** Month mode only: number of periods elapsed since {@code originMs}; 0 in ms mode. */
  private volatile int windowIndex;

  // Scheduled executor - implements time-driven window triggering. Named after the owning task
  // (mirroring WindowEngineTickScheduler's "WindowEngine-Tick-<taskName>") so scheduler threads of
  // concurrent stream tasks are distinguishable in thread dumps.
  private final ScheduledExecutorService scheduler =
      Executors.newSingleThreadScheduledExecutor(
          r -> {
            Thread t = new Thread(r, schedulerThreadName());
            t.setDaemon(true);
            return t;
          });
  private final AtomicBoolean running = new AtomicBoolean(false);

  /**
   * When {@code true}, periodic event emission is paused ({@link #stop}) but the scheduler keeps
   * running.
   */
  private volatile boolean paused;

  // Whether at least one window has been emitted; prev_time is NULL on the very first emission.
  private volatile boolean hasEmittedWindow;

  public PeriodWindowEngine(
      StreamWindow window, StreamDriverContext driverContext, StreamProperties streamProperties) {
    this(window, driverContext, streamProperties, System::currentTimeMillis);
  }

  /** Package-private constructor that injects a clock, used by tests. */
  PeriodWindowEngine(
      StreamWindow window,
      StreamDriverContext driverContext,
      StreamProperties streamProperties,
      LongSupplier clock) {
    // The period window drives itself with its own timer; it never registers for shared ticks.
    super(driverContext, streamProperties, window.getEventInputColumns(), null);
    PeriodWindow periodWindow = (PeriodWindow) window;
    this.periodMonth = periodWindow.getPeriodMonth();
    this.monthMode = periodMonth > 0;
    this.originMs = periodWindow.getOriginMs();
    this.periodMs = monthMode ? 0 : periodWindow.getPeriodMs();
    this.timeZone = TimeZone.getDefault();
    this.clock = clock;
    long now = clock.getAsLong();
    if (monthMode) {
      int index = windowIndexFor(now);
      this.windowIndex = index;
      this.currentWindowStart = monthBoundary(index);
      this.currentWindowEnd = monthBoundary(index + 1);
    } else if (originMs >= now) {
      // Requirement: an origin at or after the current time makes origin itself the first
      // execution PERIOD (window), not the first execution instant:
      // the first window is [origin, origin + period) and its trigger fires at origin + period.
      // The grid must never extend before the origin: the period preceding a future origin is not
      // a window of this stream, so no event may be emitted for it at the origin.
      this.currentWindowStart = originMs;
      this.currentWindowEnd = originMs + periodMs;
    } else {
      // Requirement: for a past origin the first execution
      // period is the smallest grid point STRICTLY greater than the current time (origin + n *
      // period > now) - the partial window containing the current time is not a window of this
      // stream and is never back-filled. alignToWindowStart(now) floors to that partial window's
      // start (or to now itself when now sits exactly on a grid point), so the smallest grid point
      // strictly greater than now is always alignToWindowStart(now) + periodMs.
      this.currentWindowStart = alignToWindowStart(now) + periodMs;
      this.currentWindowEnd = currentWindowStart + periodMs;
    }
    // Requirement: the constructor performs initialization and starts the window.
    start();
  }

  /** Scheduler thread name carrying the owning task's name (the lambda runs after construction). */
  private String schedulerThreadName() {
    return "PeriodWindowEngine-Scheduler-" + driverContext.getSubTaskContext().getTaskName();
  }

  private long alignToWindowStart(long timestamp) {
    long offset = (timestamp - originMs) % periodMs;
    return timestamp - offset;
  }

  /**
   * Computes the {@code index}-th month-mode window boundary, i.e. the instant {@code periodMonth *
   * index} months plus {@code periodMs * index} after {@code originMs}.
   *
   * <p>This is computed directly from {@code originMs} (not accumulated from the previous
   * boundary), mirroring {@link TimeDuration#calcPositiveIntervalByMonth}'s non-accumulating
   * semantics (its Javadoc: {@code 1.30 + 2mo = 3.30}).
   *
   * <p>Requirement (mo row): when the origin is the LAST day of its month (29/28 of February, 31 of
   * a 31-day month, 30 of a 30-day month), every boundary takes the last day of its corresponding
   * month - e.g. origins on Jan 31, leap Feb 29, common Feb 28, Apr 30 all yield month-end-anchored
   * boundaries (leap Feb 29, Apr 30, May 31 ...). {@link TimeDuration#calcPositiveIntervalByMonth}
   * alone does NOT provide this for day &lt;= 30 origins: it merely preserves the day-of-month
   * (leap Feb 29 -> Mar 29 -> Apr 29 ...), so month-end origins are anchored explicitly here. A
   * non-month-end origin keeps the plain semantics: the day-of-month is preserved, with days beyond
   * a month's length clamped down (the 30th of a 31-day month yields the 30th of 30/31-day months
   * and the last day of February).
   */
  static long monthBoundary(
      long originMs, int periodMonth, long periodMs, int index, TimeZone timeZone) {
    ZoneId zoneId = timeZone.toZoneId();
    LocalDateTime originLdt = LocalDateTime.ofInstant(Instant.ofEpochMilli(originMs), zoneId);
    if (periodMs == 0 && originLdt.getDayOfMonth() == originLdt.toLocalDate().lengthOfMonth()) {
      // Month-end origin: take the last day of the target month, preserving the time-of-day. The
      // target year-month of origin + periodMonth*index months is exact integer-month arithmetic,
      // so plusMonths (which only clamps the day) lands in the right month.
      LocalDateTime target = originLdt.plusMonths((long) periodMonth * index);
      return target
          .withDayOfMonth(YearMonth.from(target).lengthOfMonth())
          .atZone(zoneId)
          .toInstant()
          .toEpochMilli();
    }
    return TimeDuration.calcPositiveIntervalByMonth(
        originMs,
        new TimeDuration(periodMonth, periodMs).multiple(index),
        timeZone,
        TimeUnit.MILLISECONDS);
  }

  private long monthBoundary(int index) {
    return monthBoundary(originMs, periodMonth, periodMs, index, timeZone);
  }

  /**
   * Finds the largest window index {@code i} such that {@code monthBoundary(i) <= now}, i.e. the
   * window containing {@code now}. Starts from a calendar-based estimate then adjusts a couple of
   * steps; returns 0 when {@code originMs} is still in the future (the first window has not begun).
   */
  private int findMonthWindowIndex(long now) {
    ZoneId zoneId = timeZone.toZoneId();
    LocalDateTime originLdt = LocalDateTime.ofInstant(Instant.ofEpochMilli(originMs), zoneId);
    LocalDateTime nowLdt = LocalDateTime.ofInstant(Instant.ofEpochMilli(now), zoneId);
    long monthDiff =
        (nowLdt.getYear() - originLdt.getYear()) * 12L
            + (nowLdt.getMonthValue() - originLdt.getMonthValue());
    int index = (int) Math.max(0, monthDiff / Math.max(1, periodMonth));
    while (monthBoundary(index + 1) <= now) {
      index++;
    }
    while (index > 0 && monthBoundary(index) > now) {
      index--;
    }
    return index;
  }

  /**
   * Window index for the state at {@code now}: the smallest boundary strictly greater than {@code
   * now}, except that while {@code originMs} is still in the future the index is {@code 0} - the
   * first window is [origin, next boundary) and its trigger fires at the boundary (requirement:
   * {@code origin >= now} makes origin itself the first execution period; the month preceding a
   * future origin is not a window of this stream). For a past origin the containing (possibly
   * already partially elapsed) window is skipped - the first execution period is the smallest
   * {@code origin + n * period} strictly greater than {@code now}, so no partial window is ever
   * back-filled. Used at construction and by {@link #forceClose}'s realignment.
   */
  private int windowIndexFor(long now) {
    return originMs >= now ? 0 : findMonthWindowIndex(now) + 1;
  }

  @Override
  public ListenableFuture<?> push(List<DataSlice> dataSlices) {
    // The period window is time-driven and no longer accepts pushed data. Downstream computation
    // obtains data by scanning the source time range ([startTime, endTime)) carried by each event.
    throw new UnsupportedOperationException(
        StreamNodeMessages.EXCEPTION_PERIOD_WINDOW_DOES_NOT_ACCEPT_DATA_F936860F);
  }

  @Override
  protected void processDataSlices(List<DataSlice> dataSlices) {
    // Unreachable: push() rejects data before any batch can be buffered or processed. The base
    // contract still requires the implementation.
    throw new UnsupportedOperationException(
        StreamNodeMessages.EXCEPTION_PERIOD_WINDOW_DOES_NOT_ACCEPT_DATA_F936860F);
  }

  private void advanceWindow() {
    if (monthMode) {
      // Recompute from origin (non-accumulating) so month-end anchors are preserved across months.
      windowIndex++;
      currentWindowStart = monthBoundary(windowIndex);
      currentWindowEnd = monthBoundary(windowIndex + 1);
    } else {
      currentWindowStart += periodMs;
      currentWindowEnd = currentWindowStart + periodMs;
    }
  }

  /** Start the scheduled executor - implements time-driven window triggering. */
  protected void startScheduler() {
    // After drop() the scheduler is shut down permanently; never attempt to restart it.
    if (scheduler.isShutdown()) {
      return;
    }
    if (monthMode) {
      // Month boundaries are variable-length, so each trigger is scheduled as a one-shot at the
      // exact upcoming boundary (see scheduleNextMonthTrigger) rather than at a fixed rate.
      if (running.compareAndSet(false, true)) {
        LOGGER.debug(StreamNodeMessages.LOG_PERIOD_WINDOW_MONTH_SCHEDULER_STARTING_8277C994);
        scheduleNextMonthTrigger();
      }
      return;
    }

    if (periodMs <= 0) {
      LOGGER.warn(StreamNodeMessages.LOG_PERIOD_WINDOW_PERIODMS_NOT_SET_B688316F);
      return;
    }

    if (running.compareAndSet(false, true)) {
      // Calculate the delay for the first execution (the injected clock, not the system clock, so
      // tests driving time deterministically schedule against the same time they read).
      long now = clock.getAsLong();
      long delay = currentWindowEnd - now;
      if (delay < 0) {
        // If already expired, calculate the delay for the next window
        long missedPeriods = (now - currentWindowEnd) / periodMs + 1;
        delay = periodMs - ((now - currentWindowEnd) % periodMs);
        currentWindowStart += missedPeriods * periodMs;
        currentWindowEnd = currentWindowStart + periodMs;
      }

      LOGGER.debug(
          StreamNodeMessages.LOG_PERIOD_WINDOW_SCHEDULER_STARTING_FIRST_TRIGGER_6DF9AD81,
          delay,
          currentWindowStart,
          currentWindowEnd);

      ScheduledExecutorUtil.safelyScheduleAtFixedRate(
          scheduler, this::triggerPeriodicEvent, delay, periodMs, TimeUnit.MILLISECONDS);
    }
  }

  /**
   * While paused ({@link #stop}), a month-mode one-shot trigger whose window already elapsed
   * re-arms after this interval: re-arming at the stale boundary's delay 0 would spin the scheduler
   * thread - a full CPU core. A paused engine thereby wakes at most once per second, which also
   * bounds the catch-up latency after {@link #start()} to the same interval.
   */
  private static final long PAUSED_RECHECK_INTERVAL_MS = 1000L;

  /**
   * Month mode only: schedule a one-shot trigger at the next month boundary, then re-schedule after
   * each fire. Unlike ms mode (fixed rate), month boundaries vary in length, so the engine
   * schedules the next trigger for the exact upcoming {@code currentWindowEnd} rather than ticking
   * at a fixed interval.
   */
  // Package-private: tests drive the stale-boundary paused re-arm directly (under the
  // strictly-after-now first-window semantics the construction-time one-shot is always at least
  // one period out, so the paused elapsed-boundary path is no longer reachable through it alone).
  void scheduleNextMonthTrigger() {
    long now = clock.getAsLong();
    long delay = Math.max(0, currentWindowEnd - now);
    if (paused && delay == 0) {
      // The window already elapsed during the pause: re-arm at a bounded interval instead of
      // immediately, which would busy-loop the scheduler thread. The catch-up loop in
      // triggerPeriodicEvent emits every window that elapsed in the meantime once the pause ends
      // (or on the next re-check).
      delay = PAUSED_RECHECK_INTERVAL_MS;
    }
    try {
      scheduler.schedule(this::onMonthTrigger, delay, TimeUnit.MILLISECONDS);
    } catch (RejectedExecutionException e) {
      // Scheduler was shut down between the check and the schedule; safe to ignore.
      LOGGER.debug(StreamNodeMessages.LOG_PERIOD_WINDOW_MONTH_RESCHEDULE_REJECTED_18C381FF, e);
    }
  }

  private void onMonthTrigger() {
    try {
      triggerPeriodicEvent();
    } finally {
      if (running.get() && !scheduler.isShutdown()) {
        scheduleNextMonthTrigger();
      }
    }
  }

  /** Stop the scheduled executor */
  private void stopScheduler() {
    if (running.compareAndSet(true, false)) {
      scheduler.shutdown();
      try {
        if (!scheduler.awaitTermination(1, TimeUnit.SECONDS)) {
          scheduler.shutdownNow();
        }
      } catch (InterruptedException e) {
        scheduler.shutdownNow();
        Thread.currentThread().interrupt();
      }
      LOGGER.info(StreamNodeMessages.LOG_PERIOD_WINDOW_SCHEDULER_STOPPED_0E3BC3D5);
    }
  }

  /** Periodically trigger events - core method for time-driven triggering */
  void triggerPeriodicEvent() {
    if (paused) {
      // Processing is paused (stop()); keep ticking but emit nothing. On resume the catch-up loop
      // below emits events for any windows that elapsed during the pause.
      return;
    }
    synchronized (lock) {
      long now = clock.getAsLong();

      // Handle potentially missed multiple periods
      while (currentWindowEnd <= now) {
        // Create and add IEventInfo directly (triggers even without data)
        addEventInfo(currentWindowStart, currentWindowEnd);
        advanceWindow();

        LOGGER.debug(
            StreamNodeMessages.LOG_PERIOD_WINDOW_PERIODIC_TRIGGER_ADVANCED_D214BE38,
            currentWindowStart,
            currentWindowEnd);
      }
    }
  }

  private void addEventInfo(long startTime, long endTime) {
    // The period window no longer accepts pushed data, so events carry only a time range
    // ([startTime, endTime)); the downstream SessionScanOperator fetches data for that range.
    // rowsIterator is therefore null and rowCount is 0. And the commitIds are ignored.
    StreamWindowEngineMetrics.getInstance().incCloseEvent();
    WindowEventInfo.Builder builder =
        new WindowEventInfo.Builder()
            .startTime(startTime)
            .endTime(endTime)
            .partitionKey(driverContext.getSubTaskContext().getPartitionKey())
            .nextTime(endTime)
            .rowCount(0)
            .rowsIterator(null)
            .windowId(-1)
            .eventType(WindowEventType.WINDOW_DESTROY)
            .needsCompute(true);
    // prev_time is NULL for the first emitted window (no previous trigger); for later windows it
    // is the previous window's start.
    if (hasEmittedWindow) {
      builder.prevTime(monthMode ? monthBoundary(windowIndex - 1) : startTime - periodMs);
    }
    IEventInfo eventInfo = builder.build();
    hasEmittedWindow = true;

    try {
      tryOfferComputeEvent(eventInfo);
    } catch (Exception e) {
      LOGGER.warn(StreamNodeMessages.LOG_FAILED_TO_ADD_EVENT_TO_QUEUE_62D83E1B, e);
    }
  }

  @Override
  public void start() {
    // Clear any pause and (re)arm the scheduler. Idempotent: once the scheduler is running, later
    // calls just clear the pause flag; after drop the scheduler is shut down and stays stopped.
    paused = false;
    startScheduler();
  }

  @Override
  public void stop() {
    // Pause periodic emission without stopping the scheduler, so reception is unaffected. The
    // trigger task keeps ticking but emits nothing while paused; on start() it catches up.
    paused = true;
  }

  @Override
  public void drop() {
    // Mark dropped and unregister from the memory controller's release listeners (the period
    // engine never memory-pauses, but its listener must not outlive the engine).
    markDropped();
    stopScheduler();
    synchronized (lock) {
      windowIndex = 0;
      currentWindowStart = 0;
      currentWindowEnd = 0;
    }
    clearAllPending();
    paused = false;
    LOGGER.info(StreamNodeMessages.LOG_PERIOD_WINDOW_DROPPED_870E46A8);
  }

  @Override
  public ListenableFuture<?> forceClose() {
    // Stop the scheduler first
    stopScheduler();

    synchronized (lock) {
      addEventInfo(currentWindowStart, currentWindowEnd);
      // Realign the window to the current time. (The period window accepts no pushed data, so
      // there is never any pending row data to fold in here.)
      long now = clock.getAsLong();
      if (monthMode) {
        int index = windowIndexFor(now);
        windowIndex = index;
        currentWindowStart = monthBoundary(index);
        currentWindowEnd = monthBoundary(index + 1);
      } else if (originMs >= now) {
        // Same future-origin semantics as the constructor: the first window is [origin, origin +
        // period) - never the backwards-extrapolated [origin - period, origin).
        currentWindowStart = originMs;
        currentWindowEnd = originMs + periodMs;
      } else {
        // Same past-origin semantics as the constructor: the next window starts at the smallest
        // grid point strictly greater than now - never the containing partial window.
        currentWindowStart = alignToWindowStart(now) + periodMs;
        currentWindowEnd = currentWindowStart + periodMs;
      }
    }
    LOGGER.info(StreamNodeMessages.LOG_PERIOD_WINDOW_FORCE_CLOSED_1650B493);
    return Operator.NOT_BLOCKED;
  }
}
