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
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.stream.StreamLiteral;
import org.apache.iotdb.commons.stream.StreamProperties;
import org.apache.iotdb.commons.stream.StreamWindow;
import org.apache.iotdb.commons.stream.TumbleWindow;
import org.apache.iotdb.streamnode.engine.scheduler.task.StreamDriverContext;
import org.apache.iotdb.streamnode.engine.task.StreamSubTask.DataSlice;
import org.apache.iotdb.streamnode.i18n.StreamNodeMessages;
import org.apache.iotdb.streamnode.metrics.StreamWindowEngineMetrics;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.tsfile.common.constant.TsFileConstant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.TreeMap;

/**
 * Event-driven tumbling-window engine implementing the stream-computation control-options model.
 *
 * <p>Per-partition timelines:
 *
 * <ul>
 *   <li><b>watermark</b> ({@code maxEventTime - watermarkOption}, init -INF): advancing past a
 *       window's end closes it; advancing past a closed window's delay line destroys it.
 *   <li><b>expiredLine</b> (init -INF): data at or before this line is discarded. Advanced past a
 *       destroyed window's owned range (windowEnd - 1 when left-closed, since the end timestamp
 *       belongs to the next window; windowEnd when left-open) so its timestamps can never be
 *       re-opened.
 * </ul>
 *
 * <p>Per-window timelines (on {@link TumbleWindowState}):
 *
 * <ul>
 *   <li><b>computeLine</b> (init windowStart - 1): the boundary between already-emitted rows and
 *       pending rows.
 *   <li><b>delayLine</b> ({@code windowEnd + allowedLateness}, init +INF): watermark past this line
 *       destroys the (closed) window.
 *   <li><b>closeSystemTime / destroySystemTime</b>: system-time force-close / force-destroy
 *       deadlines for MAX_DELAY_WINDOW_CLOSE / MAX_IDLE_WINDOW_RETAIN.
 * </ul>
 *
 * <p>Emission contract (one compute event per window, plus destroy events):
 *
 * <ul>
 *   <li>Each window produces exactly <b>one</b> {@link WindowEventInfo} for its row data: created
 *       on the window's first emission (a pre-close <i>incremental</i> chunk, or the close itself
 *       if no chunk was emitted) with its rows appended incrementally thereafter. The event is
 *       typed {@code WINDOW_OPEN} while the window is open and flips to {@code WINDOW_CLOSE} on
 *       close.
 *   <li>Pre-close incremental chunks (every {@link #INCREMENTAL_COMPUTE_THRESHOLD} rows below the
 *       watermark) carry a subset of rows; the compute layer accumulates them as partial results.
 *   <li>On close the final (un-emitted) rows are appended to the same event and its type flips to
 *       {@code WINDOW_CLOSE}. In full-compute mode there are no incremental chunks, so the single
 *       event carries all rows on close.
 *   <li>Destroying a window emits a destroy-typed event (EOF): a <b>dirty</b> cached window (late
 *       data since close) emits a full-recompute event - all rows, needs-compute - onto the
 *       recompute queue; a <b>clean</b> window emits a compute-free {@code WINDOW_DESTROY} marker
 *       (no rows) onto the recompute queue. The marker never overtakes its close event: when both
 *       slots hold the same window the driver gives the compute event priority, and when the close
 *       event is still queued in the compute queue (possible with a compute backlog and a destroy
 *       order diverging from the close order - MAX_IDLE destroys per window) the driver parks the
 *       marker until the window's queued compute events drain, so the close event's result is
 *       always flushed first. With no delayed-destroy configuration (allowedLateness 0) the
 *       watermark sweep closes and destroys in the same pass, so a window emits its close event and
 *       the destroy marker together.
 *   <li>Source-commit gating: a window registers <b>every</b> tablet that contributes rows to it
 *       ({@code onWindowOpen} per slice and window, not only the opener - a late smaller tablet
 *       must lower the open allow, and the destroy allow covers every contributing tablet); rows
 *       dropped as expired gate nothing (their window was already destroyed and released). Each
 *       processed data slice fires {@code markTabletGroupDone}. The window's destroy event, once
 *       flushed by the sink, releases the commit gate behind the window.
 * </ul>
 *
 * <p>Compute mode is selected once from {@link StreamWindow#getLiteralPlaceholders()}: containing
 * rowNum → full compute (no pre-close incremental events), else → incremental compute.
 *
 * <p>When the calc plan never references the {@code ${rows}} placeholder, the compute layer never
 * scans event rows: each window then caches only the time column (see {@link #rowStoreColumns}),
 * trading event payload for memory while timestamps still drive windowing, dedup and row counts.
 *
 * <p>With all options at their defaults (watermark 0, allowedLateness 0, maxDelay 0, maxIdle 0,
 * leftClosed true) the engine reproduces the original single-window advance-on-boundary behavior:
 * each window accumulates rows and emits exactly one {@code closed=true} event when a later row
 * crosses its end.
 */
public class TumbleWindowEngine extends WindowEngine {

  private static final Logger LOGGER = LoggerFactory.getLogger(TumbleWindowEngine.class);

  /** Number of pending rows below the watermark that trigger a pre-close incremental emission. */
  private static final int INCREMENTAL_COMPUTE_THRESHOLD = 1000;

  private final long windowSize;
  private final long origin;

  // ===== Resolved stream control options (null StreamProperties -> doc defaults) =====
  private final long watermarkOption;
  private final long allowedLateness;
  private final long maxDelayWindowCloseMs;
  private final long maxIdleWindowRetainMs;
  private final boolean leftClosed;

  // ===== Compute mode =====
  private final boolean fullCompute;

  /**
   * Columns cached per row. When the calc plan never references the {@code ${rows}} placeholder
   * ({@code window.containsRowsPlaceholder() == false}), the compute layer never scans event rows,
   * so only the time column is cached - the per-device time-sorted row store still provides dedup
   * and ordering - and every event carries just the timestamp column.
   */
  private final List<String> rowStoreColumns;

  /**
   * Owns the scan-column mapping: upserts pushed tablet rows directly into the windows' row caches
   * and (via the event iterators) materializes cached rows back into TsBlocks.
   */
  private final WindowRowConverter rowConverter;

  // ===== Partition-level timelines =====
  private long maxEventTime = Long.MIN_VALUE;
  private long watermark = Long.MIN_VALUE;
  private long expiredLine = Long.MIN_VALUE;

  /**
   * Open and cached (closed-but-not-destroyed) windows, keyed by window start and <b>ordered</b> by
   * it: a watermark jump that closes several windows at once creates and offers their events in
   * window-start order, so the event stream stays ordered and {@code prev_time} (tracked via {@link
   * #lastEmittedStart}) chains along the window sequence. The order also lets the watermark-driven
   * sweep stop at the first window beyond the watermark instead of scanning the whole map on every
   * new maximum.
   */
  private final TreeMap<Long, TumbleWindowState> windows = new TreeMap<>();

  /**
   * Timestamps of rows that entered an open window above the (then-current) watermark and are not
   * yet counted in their window's {@link TumbleWindowState#pendingCount} - a row becomes countable
   * exactly when the watermark passes it, so the drain after every watermark advance converts each
   * below-the-watermark entry into a count in O(log n) (pop) + O(log n) (window lookup). Engine
   * scratch (not data-pool charged; at most 8 bytes per row above the watermark), cleared on
   * drop/forceClose. Guarded by {@link #lock}.
   */
  private final UncountedTimestamps uncounted = new UncountedTimestamps();

  // Whether at least one window has been emitted; prev_time is NULL on the very first emission.
  private volatile boolean hasEmittedWindow;
  // Start of the most recently emitted window; becomes prev_time of the next tracked emission.
  private volatile long lastEmittedStart;

  /**
   * Window start the slice currently being processed has already registered its tablet on
   * (per-slice scratch, reset by {@link #processDataSlices} for every slice): {@code onWindowOpen}
   * fires once per (slice, window) pair, so every contributing tablet registers without
   * re-registering the same tablet on every row. Only touched under {@link #lock}.
   */
  private long registeredWindowStartOfSlice = Long.MIN_VALUE;

  public TumbleWindowEngine(StreamWindow window, StreamDriverContext driverContext) {
    this(window, driverContext, null, null);
  }

  public TumbleWindowEngine(
      StreamWindow window,
      StreamDriverContext driverContext,
      StreamProperties streamProperties,
      WindowEngineTickScheduler tickScheduler) {
    super(driverContext, streamProperties, window.getEventInputColumns(), tickScheduler);
    TumbleWindow tumbleWindow = (TumbleWindow) window;
    this.windowSize = tumbleWindow.getSize();
    this.origin = tumbleWindow.getOrigin();
    if (streamProperties != null) {
      this.watermarkOption = streamProperties.getWatermark();
      this.allowedLateness = streamProperties.getAllowedLateness();
      this.maxDelayWindowCloseMs = streamProperties.getMaxDelayWindowCloseMs();
      this.maxIdleWindowRetainMs = streamProperties.getMaxIdleWindowRetainMs();
      this.leftClosed = streamProperties.isLeftClosed();
    } else {
      // Doc defaults: all timings 0, LEFT_CLOSED true.
      this.watermarkOption = 0;
      this.allowedLateness = 0;
      this.maxDelayWindowCloseMs = 0;
      this.maxIdleWindowRetainMs = 0;
      this.leftClosed = true;
    }
    // Resolve the compute mode
    this.fullCompute =
        window.getLiteralPlaceholders() != null
            && window.getLiteralPlaceholders().contains(StreamLiteral.Type.ROW_NUM);
    // Without ${rows} the event row values are never read downstream: cache the time column only.
    // The null check also covers a calc plan without an EventScanNode at all (trigger table
    // differs from the compute table): eventInputColumns is then null, and the time column is all
    // the window logic needs.
    this.rowStoreColumns =
        window.containsRowsPlaceholder() && eventScanInputColumns != null
            ? eventScanInputColumns
            : Collections.singletonList(TsFileConstant.TIME_COLUMN_ID);
    this.rowConverter =
        new WindowRowConverter(
            rowStoreColumns, window.getEventInputColumnTypes(), MAX_WINDOW_BLOCK_ROWS);
    // Requirement: the constructor performs initialization and starts the window.
    start();
  }

  /**
   * Align the timestamp to the window boundary. For left-closed {@code [start, end)} this is the
   * floor; for left-open {@code (start, end]} a timestamp exactly on a boundary belongs to the
   * previous window, so the start is shifted back by one window.
   */
  private long alignToWindowStart(long timestamp) {
    long offset = (timestamp - origin) % windowSize;
    if (offset < 0) {
      offset += windowSize;
    }
    long start = timestamp - offset;
    if (!leftClosed && offset == 0) {
      start -= windowSize;
    }
    return start;
  }

  /** Watermark crossing the window end closes an open window. */
  private boolean shouldClose(long wm, long windowEnd) {
    return leftClosed ? wm >= windowEnd : wm > windowEnd;
  }

  /** Watermark crossing the delay line destroys a closed window. */
  private boolean shouldDestroy(long wm, long delayLine) {
    return leftClosed ? wm >= delayLine : wm > delayLine;
  }

  @Override
  public ListenableFuture<?> push(List<DataSlice> dataSlices) {
    // Reception + inline processing on the ingestion thread: rows go directly into the windows'
    // columnar caches and events are emitted here. While processing is inactive the batch is only
    // buffered, and folded in on the next start() / resume. Input-pool exhaustion backpressures
    // the producer via the returned future.
    ListenableFuture<?> blocked = pushAndProcess(dataSlices);
    return blocked != null ? blocked : Operator.NOT_BLOCKED;
  }

  @Override
  protected void processDataSlices(List<DataSlice> dataSlices) {
    long now = nowMillis();
    int rowCount = 0;
    for (DataSlice dataSlice : dataSlices) {
      registeredWindowStartOfSlice = Long.MIN_VALUE;
      for (int row = dataSlice.getStartRow(); row < dataSlice.getEndRow(); row++) {
        processRow(dataSlice.getTablet().getTimestamps()[row], dataSlice, row, now);
        rowCount++;
      }
      markTabletGroupDone(dataSlice.getTabletId());
    }
    if (rowCount > 0) {
      LOGGER.debug(
          StreamNodeMessages.LOG_TUMBLE_WINDOW_PROCESSED_ROWS_OPEN_WINDOWS_59851869,
          rowCount,
          windows.size());
    }
    // Reconcile the row caches' data-pool charges with their post-batch size estimates (no-op for
    // stores that did not grow). A failed reservation memory-pauses the engine.
    for (TumbleWindowState state : windows.values()) {
      if (!state.rowStore.reconcileDataMemory()) {
        pauseOnMemoryPressure(false);
      }
    }
  }

  /**
   * Per-row state machine per the control-options flow: expired discard → open window → upsert →
   * advance watermark (close/destroy sweep + incremental threshold) / late-data-to-cached /
   * out-of-order incremental threshold / late-data-in-open-window.
   *
   * <p>Both in-order rows (new maximum, which advances the watermark) and out-of-order rows (below
   * the maximum but above the compute line) check the 1000-row incremental threshold: once that
   * many rows accumulate below the watermark in {@code (computeLine, watermark)}, they are appended
   * to the window's event.
   */
  private void processRow(long t, DataSlice dataSlice, int row, long now) {
    // 0. Resolve the per-tablet column mapping (cheap: cached by tablet identity); the first
    //    tablet establishes the output column types, which every window's row store needs.
    rowConverter.ensureColumnMapping(dataSlice);
    // 1. Expired discard: data at or before the expired line is dropped.
    if (t <= expiredLine) {
      return;
    }
    // 2. Open the window for this timestamp if it does not yet exist.
    long windowStart = alignToWindowStart(t);
    TumbleWindowState state = windows.get(windowStart);
    if (state == null) {
      state = new TumbleWindowState(windowStart, windowStart + windowSize, now);
      windows.put(windowStart, state);
      // MAX_DELAY defensive check at open time (fires only if the deadline already passed).
      if (maxDelayWindowCloseMs > 0 && now >= state.closeSystemTime) {
        StreamWindowEngineMetrics.getInstance().incForceCloseByMaxDelay();
        closeWindow(state, now);
      }
    }
    // 2b. Register EVERY tablet that contributes rows to the window, once per (slice, window):
    //     registering only the opener would leave a late smaller tablet unregistered - it could
    //     commit (open allow = min tablet - 1 stays too high) while its rows are still unflushed in
    //     the window - and would understate the max tablet, delaying the tablets' release after
    //     the destroy marker flushes.
    if (windowStart != registeredWindowStartOfSlice) {
      onWindowOpen(state.windowId, dataSlice.getTabletId());
      registeredWindowStartOfSlice = windowStart;
    }
    // 3. Upsert the row: merge non-null columns into the stored row (never a full-row replace).
    //    Before close this writes the main view; after close (store frozen) the row diverts to the
    //    store's late list - a late write to a cached window marks it dirty (branch below) and is
    //    corrected by the destroy-time recompute over the merged copy.
    boolean inserted = rowConverter.upsertInto(state.rowStore, dataSlice, row);
    // 3b. Pending-row bookkeeping (incremental mode, open window): a new row strictly above the
    //     compute line becomes countable exactly when the watermark passes it. Below the current
    //     watermark it counts immediately; at or above it the timestamp parks in the uncounted
    //     heap until a later maximum advances the watermark past it (drain below). Merged rows
    //     (same device + timestamp) never count; frozen windows keep their count fixed at close.
    if (inserted && !fullCompute && state.isOpen() && t > state.computeLine) {
      if (t < watermark) {
        state.pendingCount++;
      } else {
        uncounted.push(t);
      }
    }
    // 4. Advance partition timelines / branch on event time.
    boolean newMaxEventTime = t > maxEventTime;
    if (newMaxEventTime) {
      // New maximum (in-order arrival): advance the watermark, sweep close/destroy. The drain
      // runs BEFORE the sweep: rows of windows the sweep is about to close/destroy must be
      // counted first - close resets pendingCount, and a destroyed window's stale entries are
      // dropped by the drain's window lookup.
      maxEventTime = t;
      watermark = maxEventTime - watermarkOption;
      drainUncounted();
      closeAndDestroyWindows(now);
    }
    if (!state.isOpen()) {
      // Late data to a cached (closed) window: mark dirty and refresh its idle-retain deadline.
      // This also covers new-maximum rows: a MAX_DELAY force-close happens below the watermark,
      // so an in-order row landing in the force-closed window still arrives after its close event
      // was emitted - it joins the full-recompute list and defers the idle-retain destroy (doc
      // MAX_DELAY_WINDOW_CLOSE / MAX_IDLE_WINDOW_RETAIN example, data 4).
      state.dirty = true;
      if (allowedLateness > 0 && maxIdleWindowRetainMs > 0) {
        state.destroySystemTime = now + maxIdleWindowRetainMs;
      }
    } else if (newMaxEventTime || t > state.computeLine) {
      // In-window row below the watermark: incremental-list row. Check the batch threshold.
      maybeIncrementalEmit(state);
    } else {
      // t <= computeLine: late data within an open window (full-list row).
      state.dirty = true;
    }
  }

  /**
   * Close and destroy windows whose watermark lines have been crossed. The map is ordered by window
   * start, so the windows whose end the watermark has reached form a sorted prefix: the scan stops
   * at the first window beyond it (no full map scan on every new maximum), and the events of a
   * multi-window close are created and offered in window-start order.
   */
  private void closeAndDestroyWindows(long now) {
    Iterator<Map.Entry<Long, TumbleWindowState>> it = windows.entrySet().iterator();
    while (it.hasNext()) {
      TumbleWindowState state = it.next().getValue();
      if (!shouldClose(watermark, state.windowEnd)) {
        // Sorted by start: every remaining window's end lies even further in the future, and so
        // does its delay line - the sweep is done.
        return;
      }
      if (state.isOpen()) {
        closeWindow(state, now);
      }
      if (state.isClosed() && shouldDestroy(watermark, state.delayLine)) {
        destroyWindow(state);
        it.remove();
      }
    }
  }

  /**
   * Converts uncounted-heap entries the watermark has passed into counts in their windows' {@link
   * TumbleWindowState#pendingCount}. Popped entries of windows that no longer exist (destroyed) or
   * are no longer open are skipped - their pending rows were fixed or finalized at close/destroy,
   * so a leftover entry must not revive them. Every entry's window lookup is one TreeMap descent.
   */
  private void drainUncounted() {
    while (!uncounted.isEmpty() && uncounted.peek() < watermark) {
      long t = uncounted.pop();
      TumbleWindowState state = windows.get(alignToWindowStart(t));
      if (state != null && state.isOpen() && t > state.computeLine) {
        state.pendingCount++;
      }
    }
  }

  /** Append the pre-close incremental batch if enough rows have accumulated below the watermark. */
  private void maybeIncrementalEmit(TumbleWindowState state) {
    if (fullCompute || !state.isOpen() || watermark == Long.MIN_VALUE) {
      return;
    }
    if (state.pendingCount >= INCREMENTAL_COMPUTE_THRESHOLD) {
      // The pending rows are those with timestamps in (computeLine, watermark); record them as
      // one emission batch on the event's iterator (materialized lazily by its next()).
      appendToEvent(state, state.computeLine, watermark, false);
      // Advance computeLine past the emitted range. Rows stay in the store: in incremental mode
      // the close event does NOT re-send them (it carries only rows above the line), so the
      // store is what backs the destroy-time full recompute. The new line is the maximum
      // timestamp below the watermark, so exactly no row below the watermark stays uncounted;
      // the heap's entries all sit at or above it.
      state.computeLine = state.rowStore.maxTimestampBelow(watermark);
      state.pendingCount = 0;
    }
  }

  /**
   * Close a window and arm the destroy timelines. The single per-window event's rows are finalized
   * here: in full-compute mode all rows are appended; in incremental mode only the un-emitted rows
   * (above {@code computeLine}) are appended as the final increment. The event's {@code closed}
   * flag then flips to {@code true}. In incremental mode, rows at/below the compute line that
   * arrived after their emission stay dirty and are recovered by the destroy-time full recompute.
   */
  private void closeWindow(TumbleWindowState state, long now) {
    if (!state.isOpen()) {
      return;
    }
    state.state = WindowState.CLOSED;
    state.closeSystemTime = now;
    state.delayLine = state.windowEnd + allowedLateness;
    // Idle-retain deadline applies only when late data is allowed; otherwise destroy is driven
    // purely by the watermark reaching the delay line.
    state.destroySystemTime =
        (allowedLateness > 0 && maxIdleWindowRetainMs > 0)
            ? now + maxIdleWindowRetainMs
            : Long.MAX_VALUE;
    // Freeze the store's read view: the close emission below counts exactly the rows present now,
    // and the (lazily materializing) close event must keep seeing exactly those rows - late data
    // arriving after close diverts to the store's late list and surfaces only in the destroy-time
    // full recompute, never in this close event's result.
    state.rowStore.freeze();
    if (fullCompute) {
      // Full compute: the single event carries all rows; nothing remains un-emitted.
      if (!state.rowStore.isEmpty()) {
        appendToEvent(state, Long.MIN_VALUE, Long.MAX_VALUE, true);
      }
      state.dirty = false;
    } else if (!state.rowStore.isEmpty()) {
      // Incremental compute: append only the un-emitted rows above the compute line as the final
      // increment. Even when the remainder is empty, the close must still finalize the event so the
      // compute layer receives the window-finalization signal.
      appendToEvent(state, state.computeLine, Long.MAX_VALUE, true);
    }
    // After close no further incremental emission happens for this window.
    state.computeLine = state.rowStore.maxTimestamp();
    state.pendingCount = 0;
  }

  /**
   * Destroy a window. If late data modified it since close, regenerate its event (full recompute)
   * onto the {@code recomputeEventQueue} before discarding it and advancing the expired line. A
   * clean destroy instead emits a compute-free destroy marker (typed {@code WINDOW_DESTROY}) onto
   * the recompute queue, queued behind the window's close event so the sink flushes it last - its
   * flush is what lets the source commit advance past the window's tablets.
   */
  private void destroyWindow(TumbleWindowState state) {
    // The late-list check matters for a window closed empty (e.g. MAX_DELAY force-close at open)
    // that only received rows after close: they live in the late list and the recompute is their
    // only way out.
    if (state.dirty && (!state.rowStore.isEmpty() || state.rowStore.hasLateRows())) {
      // Charge the store's current size before discarding it: a window created and destroyed within
      // one batch never reaches the batch-end reconcile, and a pinned store (outstanding event or
      // recompute iterator) must keep a charge matching the rows it still backs.
      if (!state.rowStore.reconcileDataMemory()) {
        pauseOnMemoryPressure(false);
      }
      // Dirty cached window: emit a recompute event to the recompute queue. Emission tracking is
      // NOT advanced - the window was already tracked when first emitted, and advancing would
      // corrupt prev_time for later windows.
      tryOfferRecomputeEvent(buildRecomputeEvent(state, true));
      state.dirty = false;
    } else {
      // Clean destroy: emit the destroy marker (no rows, no compute) so the sink can advance the
      // source commit behind this window once the marker is flushed.
      tryOfferRecomputeEvent(buildRecomputeEvent(state, false));
    }
    state.state = WindowState.DESTROYED;
    // Drop the engine's owner reference to the row cache: a store still pinned by an outstanding
    // event iterator keeps its data-pool charge until that iterator is done with it.
    state.rowStore.releaseOwner();
    // Left-closed windows own [start, end): the end timestamp belongs to the NEXT window, so the
    // expired line must stop one millisecond short of it, or a row at exactly windowEnd (whose own
    // window may still be open) would be dropped. Left-open windows own (start, end], so expiring
    // through end is exact.
    long expiredThrough = leftClosed ? state.windowEnd - 1 : state.windowEnd;
    expiredLine = Math.max(expiredLine, expiredThrough);
  }

  @Override
  protected void onTick() {
    long now = nowMillis();
    Iterator<Map.Entry<Long, TumbleWindowState>> it = windows.entrySet().iterator();
    while (it.hasNext()) {
      TumbleWindowState state = it.next().getValue();
      // MAX_DELAY_WINDOW_CLOSE: force-close open windows whose system-time deadline has passed.
      if (state.isOpen() && maxDelayWindowCloseMs > 0 && now >= state.closeSystemTime) {
        StreamWindowEngineMetrics.getInstance().incForceCloseByMaxDelay();
        closeWindow(state, now);
        // No delayed-destroy config (allowedLateness == 0): the closed window cannot receive late
        // data anyway, so destroy it right away - the close event is immediately followed by its
        // (possibly compute-free) destroy event. The watermark-driven sweep gets this for free
        // (delayLine == windowEnd), but a system-time force-close fires below the delay line.
        if (allowedLateness == 0) {
          destroyWindow(state);
          it.remove();
          continue;
        }
      }
      // MAX_IDLE_WINDOW_RETAIN: force-destroy cached (closed) windows whose idle-retain deadline
      // has passed. Effective only when late data is allowed.
      if (state.isClosed()
          && allowedLateness > 0
          && maxIdleWindowRetainMs > 0
          && now >= state.destroySystemTime) {
        StreamWindowEngineMetrics.getInstance().incForceDestroyByMaxIdle();
        destroyWindow(state);
        it.remove();
      }
    }
  }

  /**
   * Last-resort self-rescue under an unresolved data-pool memory pause (see {@code tick()}):
   * force-releases exactly one window per call. Cached (closed) windows go first - their rows are
   * already emitted, so destroying them costs only the late-data recompute and covers the
   * MAX_DELAY-closed-but-not-destroyed case (allowedLateness > 0 without MAX_IDLE config, which
   * would retain them forever). Only when no closed window remains is the oldest open window
   * force-closed, accepting the same premature-close semantics as MAX_DELAY_WINDOW_CLOSE. Either
   * way the window is destroyed right away: retaining it for late data would keep holding the very
   * memory the rescue must return. Caller must hold {@link #lock}.
   */
  @Override
  protected boolean onMemoryPressureRescue() {
    // Closed windows first (cheapest): their close events are already emitted.
    Iterator<Map.Entry<Long, TumbleWindowState>> it = windows.entrySet().iterator();
    while (it.hasNext()) {
      TumbleWindowState state = it.next().getValue();
      if (state.isClosed()) {
        StreamWindowEngineMetrics.getInstance().incForceDestroyByMemoryPressure();
        LOGGER.warn(
            StreamNodeMessages.LOG_WINDOW_FORCE_DESTROY_BY_MEMORY_PRESSURE_731C3E95,
            getClass().getSimpleName(),
            state.windowStart,
            DATA_POOL_PAUSE_RESCUE_GRACE_MS);
        destroyWindow(state);
        it.remove();
        return true;
      }
    }
    // Oldest open window: force-close then destroy, exactly like a MAX_DELAY force-close with
    // allowedLateness == 0 (the closed event is immediately followed by the destroy marker /
    // recompute event; rows arriving later for this window are late and dropped or recovered by
    // the recompute).
    it = windows.entrySet().iterator();
    while (it.hasNext()) {
      TumbleWindowState state = it.next().getValue();
      if (state.isOpen()) {
        StreamWindowEngineMetrics.getInstance().incForceCloseByMemoryPressure();
        LOGGER.warn(
            StreamNodeMessages.LOG_WINDOW_FORCE_CLOSE_BY_MEMORY_PRESSURE_961D31B5,
            getClass().getSimpleName(),
            state.windowStart,
            DATA_POOL_PAUSE_RESCUE_GRACE_MS);
        closeWindow(state, nowMillis());
        destroyWindow(state);
        it.remove();
        return true;
      }
    }
    // Nothing left to release here: remaining data-pool charges belong to stores pinned by
    // outstanding event iterators and return when the driver consumes them.
    return false;
  }

  /**
   * Reconciles and drops the engine's owner reference for every window's row cache (drop /
   * forceClose): a store still pinned by an outstanding event iterator keeps its data-pool charge
   * until that iterator is done with it. Caller must hold {@link #lock}.
   */
  private void releaseAllWindowStores() {
    for (TumbleWindowState state : windows.values()) {
      if (!state.rowStore.reconcileDataMemory()) {
        pauseOnMemoryPressure(false);
      }
      state.rowStore.releaseOwner();
    }
  }

  /** Returns (creating and offering if needed) the window's single event and its live iterator. */
  private WindowEventInfo ensureEvent(TumbleWindowState state, boolean close) {
    if (state.eventInfo == null) {
      // The iterator binds the window's row store: it materializes TsBlocks lazily from it, and
      // keeps reading it even after the engine discards the window.
      state.rowsIterator = new WindowEventRowsIterator(rowConverter, state.rowStore, lock, this);
      state.eventInfo = createEvent(state, state.rowsIterator, close);
      tryOfferComputeEvent(state.eventInfo);
    }
    return state.eventInfo;
  }

  /**
   * Records one emission batch on the window's event: the rows whose timestamps lie in {@code
   * (fromExclusive, toExclusive)} are materialized lazily by the event's iterator ({@code
   * toExclusive == Long.MAX_VALUE} means through the last row), while the event's row count is
   * extended right away. With {@code close} the batch is the event's final one: the event flips to
   * {@code WINDOW_CLOSE} and the iterator is then marked finished (a consumer completing the rows
   * always observes the closed type).
   */
  private void appendToEvent(
      TumbleWindowState state, long fromExclusive, long toExclusive, boolean close) {
    WindowEventInfo event = ensureEvent(state, close);
    // The batch's row count is derived from the same per-device timestamp predicates the
    // iterator's generation walk evaluates, so the event's row count matches its delivery.
    int rowCount =
        toExclusive == Long.MAX_VALUE
            ? state.rowStore.countAbove(fromExclusive)
            : state.rowStore.countBetween(fromExclusive, toExclusive);
    if (rowCount > 0) {
      state.rowsIterator.emitRange(fromExclusive, toExclusive);
      event.addRows(rowCount);
      if (!close) {
        StreamWindowEngineMetrics.getInstance().incIncrementalEvent();
      }
    }
    if (close) {
      event.markClosed();
      state.rowsIterator.markFinished();
      StreamWindowEngineMetrics.getInstance().incCloseEvent();
    }
  }

  /**
   * Creates the window's single event, typed {@code WINDOW_CLOSE} when it is created by the close
   * itself and {@code WINDOW_OPEN} when created by a pre-close incremental chunk (it flips to close
   * via {@link WindowEventInfo#markClosed}). {@code prev_time} is NULL for the first emitted
   * window; otherwise it is the start of the most recently emitted window.
   */
  private WindowEventInfo createEvent(
      TumbleWindowState state, WindowEventRowsIterator iterator, boolean close) {
    WindowEventInfo.Builder builder =
        new WindowEventInfo.Builder()
            .startTime(state.windowStart)
            .endTime(state.windowEnd)
            .partitionKey(driverContext.getSubTaskContext().getPartitionKey())
            .nextTime(state.windowEnd)
            .windowId(state.windowId)
            .eventType(close ? WindowEventType.WINDOW_CLOSE : WindowEventType.WINDOW_OPEN)
            .needsCompute(true)
            .rowsIterator(iterator);
    // Snapshot the prev_time this window's event carries: the destroy-time full recompute reuses
    // the same value instead of recomputing a structural predecessor, which may name a window
    // that never existed (the slot before the first window, or an empty window that was never
    // created).
    state.eventPrevTime =
        hasEmittedWindow ? OptionalLong.of(lastEmittedStart) : OptionalLong.empty();
    if (state.eventPrevTime.isPresent()) {
      builder.prevTime(state.eventPrevTime.getAsLong());
    }
    WindowEventInfo event = builder.build();
    hasEmittedWindow = true;
    lastEmittedStart = state.windowStart;
    return event;
  }

  /**
   * Builds a full-recompute event for a dirty cached window. Untracked: its {@code prev_time}
   * reuses the snapshot of the window's own close event (NULL for the first window) and it never
   * advances the emission tracking fields. It is destroy-typed (EOF): its flush is the window's
   * terminal event and lets the source commit advance behind it.
   */
  private IEventInfo buildRecomputeEvent(TumbleWindowState state, boolean needCompute) {
    StreamWindowEngineMetrics.getInstance().incRecomputeEvent();
    // The full recompute must see every row, including late ones diverted after the close froze the
    // store's read view. When any arrived, it reads from a merged copy so a possibly
    // still-unconsumed close event (same store, frozen view) stays untouched.
    boolean merged = needCompute && state.rowStore.hasLateRows();
    WindowRowStore store = merged ? state.rowStore.mergedCopy() : state.rowStore;
    if (merged && !store.reconcileDataMemory()) {
      pauseOnMemoryPressure(false);
    }
    int size = store.size();
    WindowEventRowsIterator iterator =
        needCompute ? new WindowEventRowsIterator(rowConverter, store, lock, this) : null;
    if (iterator != null) {
      // Full recompute: the event carries all rows of the cached window as one final batch.
      iterator.emitRange(Long.MIN_VALUE, Long.MAX_VALUE);
      iterator.markFinished();
      if (merged) {
        // Drop the engine's transient owner reference to the copy; the iterator's pin keeps its
        // data-pool charge alive until the recompute event is consumed.
        store.releaseOwner();
      }
    }
    WindowEventInfo.Builder builder =
        new WindowEventInfo.Builder()
            .startTime(state.windowStart)
            .endTime(state.windowEnd)
            .partitionKey(driverContext.getSubTaskContext().getPartitionKey())
            .nextTime(state.windowEnd)
            .rowCount(needCompute ? size : 0)
            .rowsIterator(iterator)
            .windowId(state.windowId)
            .eventType(WindowEventType.WINDOW_DESTROY)
            .needsCompute(needCompute);
    // prev_time must match what the window's own close event carried: the recompute revises that
    // close event and must not surface a different predecessor (the structural predecessor may
    // name a window that never existed - the slot before the first window, or an empty window
    // that was never created). A never-emitted window (its compute-free destroy marker is the
    // only event it ever gets) has no snapshot and keeps NULL; no compute reads it.
    if (state.eventPrevTime != null && state.eventPrevTime.isPresent()) {
      builder.prevTime(state.eventPrevTime.getAsLong());
    }
    return builder.build();
  }

  @Override
  public void start() {
    startProcessing();
  }

  @Override
  public void stop() {
    pauseProcessing();
  }

  @Override
  public void drop() {
    markDropped();
    stopProcessing();
    synchronized (lock) {
      releaseAllWindowStores();
      windows.clear();
      // Stale uncounted entries must not leak into a restarted engine's counts.
      uncounted.clear();
    }
    maxEventTime = Long.MIN_VALUE;
    watermark = Long.MIN_VALUE;
    expiredLine = Long.MIN_VALUE;
    hasEmittedWindow = false;
    lastEmittedStart = 0;
    clearAllPending();
    LOGGER.info(StreamNodeMessages.LOG_TUMBLE_WINDOW_DROPPED_C71A44A2);
  }

  @Override
  public ListenableFuture<?> forceClose() {
    synchronized (lock) {
      // Stop processing and fold in any batches buffered while paused so that no data is lost
      // before emitting the final closed events. The final flush ignores a memory pause: charging
      // is best-effort, but no buffered row may be dropped.
      stopProcessing();
      drainAllBufferedBatches();
      // Emit every window that still has data: open windows get their final close event (all rows
      // in full-compute mode, the remaining un-emitted rows in incremental mode), then every
      // non-destroyed window is destroyed - a dirty window via a full-recompute event, a clean one
      // via its destroy marker. Empty windows are skipped (they never emitted or gated anything).
      for (TumbleWindowState state : windows.values()) {
        if (state.isDestroyed()) {
          continue;
        }
        if (state.isOpen() && !state.rowStore.isEmpty()) {
          closeWindow(state, nowMillis());
        }
        destroyWindow(state);
      }
      windows.clear();
      // Stale uncounted entries must not leak into a restarted engine's counts.
      uncounted.clear();
      maxEventTime = Long.MIN_VALUE;
      watermark = Long.MIN_VALUE;
      expiredLine = Long.MIN_VALUE;
    }
    LOGGER.info(StreamNodeMessages.LOG_TUMBLE_WINDOW_FORCE_CLOSED_31857093);
    return Operator.NOT_BLOCKED;
  }

  /**
   * Primitive long min-heap of uncounted row timestamps (boxed {@code Long} heaps would pay 16+
   * bytes and pointer-chasing per row above the watermark). Grows by doubling; {@code clear()}
   * drops the array reference entirely so drop/forceClose release the scratch in one step.
   * Single-threaded under the owning engine's lock; not thread-safe on its own.
   */
  private static final class UncountedTimestamps {

    private long[] heap = new long[0];
    private int size;

    boolean isEmpty() {
      return size == 0;
    }

    long peek() {
      return heap[0];
    }

    void push(long value) {
      if (size == heap.length) {
        heap = Arrays.copyOf(heap, Math.max(16, heap.length * 2));
      }
      heap[size] = value;
      siftUp(size++);
    }

    long pop() {
      long top = heap[0];
      heap[0] = heap[--size];
      if (size > 0) {
        siftDown(0);
      }
      return top;
    }

    void clear() {
      heap = new long[0];
      size = 0;
    }

    private void siftUp(int i) {
      long value = heap[i];
      while (i > 0) {
        int parent = (i - 1) >>> 1;
        if (heap[parent] <= value) {
          break;
        }
        heap[i] = heap[parent];
        i = parent;
      }
      heap[i] = value;
    }

    private void siftDown(int i) {
      long value = heap[i];
      while (true) {
        int child = 2 * i + 1;
        if (child >= size) {
          break;
        }
        int right = child + 1;
        if (right < size && heap[right] < heap[child]) {
          child = right;
        }
        if (heap[child] >= value) {
          break;
        }
        heap[i] = heap[child];
        i = child;
      }
      heap[i] = value;
    }
  }

  /** Window lifecycle: a window is opened on its first row, closed by watermark/time, destroyed. */
  private enum WindowState {
    OPEN,
    CLOSED,
    DESTROYED
  }

  /**
   * Mutable per-window state. Non-static so it can read the engine's scan columns and block cap.
   */
  private final class TumbleWindowState {
    final long windowStart;
    final long windowEnd;
    final WindowRowStore rowStore;

    /**
     * Task-scoped window id assigned when the window opens; carried by every event and destroy
     * marker of this window. Also becomes the engine's in-progress window (dropped rows gate on it)
     * until the next window opens.
     */
    final long windowId;

    WindowState state = WindowState.OPEN;

    /** The window's single event and its live row iterator; null until the first emission. */
    WindowEventInfo eventInfo;

    WindowEventRowsIterator rowsIterator;

    /**
     * The prev_time this window's own event carries, snapshotted when the event is created: empty
     * (NULL) when this is the first emitted window, otherwise the most recently emitted window's
     * start at that moment. The destroy-time full recompute reuses this snapshot so it revises the
     * close event with the same prev_time instead of a recomputed structural predecessor. Null
     * until the window's first (only) emission.
     */
    OptionalLong eventPrevTime;

    /**
     * Max event time already emitted via a pre-close incremental event; rows strictly above the
     * line are pending. Initialized to {@code windowStart - 1} so every row of the window starts
     * pending. Rows at or below the line remain in the store: in incremental mode the close event
     * emits only rows above the line, so below-line late modifications are recovered by the
     * destroy-time full recompute.
     */
    long computeLine;

    /**
     * Rows in {@code (computeLine, watermark)} - the pending rows {@link #maybeIncrementalEmit}
     * counts against {@link #INCREMENTAL_COMPUTE_THRESHOLD}. Maintained incrementally: a main-view
     * insert below the watermark (or an uncounted-heap entry, once the watermark passes it) counts;
     * an emission or the close resets it (the new compute line is the maximum timestamp below the
     * watermark, so exactly zero rows remain below it). Incremental mode only; the O(devices)
     * per-row {@code countBetween} this replaces was quadratic in the same-timestamp many-devices
     * shape.
     */
    int pendingCount;

    /**
     * {@code windowEnd + allowedLateness}; watermark past this line destroys the window. +INF until
     * close.
     */
    long delayLine;

    /**
     * {@code openTime + maxDelayWindowCloseMs}; system-time force-close deadline. +INF if disabled.
     */
    long closeSystemTime;

    /**
     * {@code closeTime + maxIdleWindowRetainMs}; system-time force-destroy deadline. +INF if
     * disabled.
     */
    long destroySystemTime;

    /** Late data arrived since the last emission; drives a full-recompute emission on destroy. */
    boolean dirty;

    TumbleWindowState(long windowStart, long windowEnd, long openTime) {
      this.windowStart = windowStart;
      this.windowEnd = windowEnd;
      this.rowStore = new WindowRowStore(rowConverter.outputTypes());
      this.windowId = allocateWindowId();
      // One below windowStart: no row is emitted yet, and every row of the window (including a
      // row at exactly windowStart) starts strictly above the line as pending.
      this.computeLine = windowStart - 1;
      this.delayLine = Long.MAX_VALUE;
      this.closeSystemTime =
          maxDelayWindowCloseMs > 0 ? openTime + maxDelayWindowCloseMs : Long.MAX_VALUE;
      this.destroySystemTime = Long.MAX_VALUE;
    }

    boolean isOpen() {
      return state == WindowState.OPEN;
    }

    boolean isClosed() {
      return state == WindowState.CLOSED;
    }

    boolean isDestroyed() {
      return state == WindowState.DESTROYED;
    }
  }
}
