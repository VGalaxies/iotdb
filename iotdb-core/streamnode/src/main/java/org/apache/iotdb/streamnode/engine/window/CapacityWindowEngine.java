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
import org.apache.iotdb.commons.stream.CapacityWindow;
import org.apache.iotdb.commons.stream.StreamProperties;
import org.apache.iotdb.commons.stream.StreamWindow;
import org.apache.iotdb.streamnode.engine.scheduler.task.StreamDriverContext;
import org.apache.iotdb.streamnode.engine.task.StreamSubTask.DataSlice;
import org.apache.iotdb.streamnode.i18n.StreamNodeMessages;
import org.apache.iotdb.streamnode.metrics.StreamWindowEngineMetrics;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.tsfile.common.constant.TsFileConstant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;

/**
 * Count-based (capacity) window engine: {@link #push} receives <i>and processes</i> data on the
 * ingestion thread - rows are written directly into the current window's columnar row cache (no
 * intermediate row representation) and each filled window is emitted as one event. While processing
 * is inactive ({@code stop()}/{@code drop()}) the pushed batches are only buffered and folded in on
 * the next {@code start()}.
 *
 * <p>Window semantics: rows accumulate in a single current window (a {@link WindowRowStore}); when
 * it reaches the configured capacity it is emitted as one event and the next window starts. The
 * event's row data is materialized lazily by its {@link WindowEventRowsIterator} (one full-store
 * range recorded at emission). Unlike the time-driven engines there is no watermark, no late data
 * and no recompute path; a filled window is closed and destroyed at once, emitting a {@code
 * WINDOW_CLOSE} event immediately followed by its compute-free {@code WINDOW_DESTROY} marker. With
 * {@code MAX_DELAY_WINDOW_CLOSE} configured, a partially-filled window that stops receiving rows
 * does not wait forever: the shared tick scheduler force-closes it once its system-time deadline
 * (armed at window open, a hard cap like in {@code TumbleWindowEngine}) passes.
 */
public class CapacityWindowEngine extends WindowEngine {

  private static final Logger LOGGER = LoggerFactory.getLogger(CapacityWindowEngine.class);

  private final int capacity;

  /**
   * MAX_DELAY_WINDOW_CLOSE option: the maximum system time (ms) a partially-filled window may stay
   * open. {@code 0} disables the system-time force-close: the window then only closes when it fills
   * or on {@code forceClose()}.
   */
  private final long maxDelayWindowCloseMs;

  /**
   * Owns the scan-column mapping: upserts pushed tablet rows directly into the current window's row
   * cache and (via the event iterators) materializes cached rows back into TsBlocks.
   */
  private final WindowRowConverter rowConverter;

  /** System time (ms) at which the current (still filling) window started. */
  private long currentWindowStart;

  /**
   * Row cache of the current window; created lazily on its first row (the converter's column types
   * are only known after the first push) and discarded when the window fills up. Guarded by {@link
   * #lock}.
   */
  private WindowRowStore rowStore;

  /**
   * Rows accumulated in the current window, counting MERGED rows: a same-device duplicate
   * timestamp's columns merge into the stored row and do not advance the count, while a row of
   * another device (same timestamp or not) is a distinct row and does - device identity is derived
   * per row, see {@link #rowStore}. So while the window is open this always equals {@link
   * #rowStore}.size(). The event's row count stays accurate under duplicate timestamps (data
   * merged, count merged too). Guarded by {@link #lock}.
   */
  private int currentRowCount;

  /** In-progress window id; {@code -1} when no window is open. */
  protected long currentWindowId = -1;

  /**
   * System-time deadline at which the shared tick scheduler force-closes the current (still
   * filling) window; armed at window open, {@link Long#MAX_VALUE} while no window/deadline is
   * armed. Guarded by {@link #lock}.
   */
  private long closeSystemTime = Long.MAX_VALUE;

  /**
   * Whether the slice currently being processed has already registered its tablet on the
   * in-progress window (per-slice scratch, reset by {@link #processDataSlices} for every slice):
   * {@code onWindowOpen} fires once per (slice, window) pair, so every contributing tablet
   * registers without re-registering the same tablet on every row. Only touched under {@link
   * #lock}.
   */
  private boolean registeredCurrentWindowOfSlice;

  public CapacityWindowEngine(
      StreamWindow window,
      StreamDriverContext driverContext,
      StreamProperties streamProperties,
      WindowEngineTickScheduler tickScheduler) {
    super(driverContext, streamProperties, window.getEventInputColumns(), tickScheduler);
    CapacityWindow capacityWindow = (CapacityWindow) window;
    this.capacity = capacityWindow.getCapacity();
    this.maxDelayWindowCloseMs =
        streamProperties != null ? streamProperties.getMaxDelayWindowCloseMs() : 0;
    // Without ${rows} (containsRowsPlaceholder false, or no EventScanNode in the calc plan at all -
    // trigger table differs from the compute table, so eventInputColumns is null) the cached row
    // values are never read downstream: cache the time column only, which still advances the
    // capacity-window logic. Mirrors TumbleWindowEngine's rowStoreColumns selection.
    this.rowConverter =
        new WindowRowConverter(
            window.containsRowsPlaceholder() && eventScanInputColumns != null
                ? eventScanInputColumns
                : Collections.singletonList(TsFileConstant.TIME_COLUMN_ID),
            window.getEventInputColumnTypes(),
            MAX_WINDOW_BLOCK_ROWS);
    this.currentWindowStart = nowMillis();
    // Requirement: the constructor performs initialization and starts the window.
    start();
  }

  @Override
  public ListenableFuture<?> push(List<DataSlice> dataSlices) {
    // Reception + inline processing on the ingestion thread: rows go directly into the current
    // window's columnar cache and filled windows are emitted here. While processing is inactive
    // the batch is only buffered, and folded in on the next start() / resume. Input-pool
    // exhaustion backpressures the producer via the returned future.
    ListenableFuture<?> blocked = pushAndProcess(dataSlices);
    return blocked != null ? blocked : Operator.NOT_BLOCKED;
  }

  @Override
  protected void processDataSlices(List<DataSlice> dataSlices) {
    long now = nowMillis();
    for (DataSlice dataSlice : dataSlices) {
      registeredCurrentWindowOfSlice = false;
      for (int row = dataSlice.getStartRow(); row < dataSlice.getEndRow(); row++) {
        processRow(dataSlice, row, now);
      }
      markTabletGroupDone(dataSlice.getTabletId());
    }
    if (currentRowCount > 0) {
      LOGGER.debug(
          StreamNodeMessages.LOG_CAPACITY_WINDOW_PROCESSED_BATCH_ROWS_ARG_1309F37E,
          currentRowCount);
    }
    // Reconcile the current window's row-cache charge with its post-batch size estimate; a failed
    // reservation memory-pauses the engine.
    if (rowStore != null && !rowStore.reconcileDataMemory()) {
      pauseOnMemoryPressure(false);
    }
  }

  /**
   * Buffers one row into the current window's cache, emitting the window when it reaches the
   * configured capacity. Caller must hold {@link #lock}.
   */
  private void processRow(DataSlice dataSlice, int row, long now) {
    // The per-tablet column mapping is resolved here (cheap: cached by tablet identity); the
    // first tablet establishes the output column types, which the (lazily created) row store
    // needs.
    rowConverter.ensureColumnMapping(dataSlice);
    if (rowStore == null) {
      rowStore = new WindowRowStore(rowConverter.outputTypes());
    }
    // Upsert shares TumbleWindowEngine's data-layer semantics: same-device same-timestamp rows
    // merge by column (the common in-order case is a plain append). The window therefore fills per
    // MERGED row: a duplicate timestamp's columns merge into the stored row and do not advance the
    // count; rows of other devices are distinct rows (device identity is derived per row from the
    // tablet's TAG columns). The store keeps no per-row tablet ids; every contributing tablet
    // registers on the window (onWindowOpen, once per slice) when its rows enter, so a merge
    // cannot lose a tablet's commit gate.
    int sizeBeforeUpsert = rowStore.size();
    rowConverter.upsertInto(rowStore, dataSlice, row);
    long tabletId = dataSlice.getTabletId();
    if (sizeBeforeUpsert == 0) {
      currentWindowId = allocateWindowId();
      onWindowOpen(currentWindowId, tabletId);
      registeredCurrentWindowOfSlice = true;
      // Arm the MAX_DELAY_WINDOW_CLOSE deadline at window open. It is a hard cap: later rows do
      // not refresh it (mirroring TumbleWindowEngine).
      closeSystemTime = maxDelayWindowCloseMs > 0 ? now + maxDelayWindowCloseMs : Long.MAX_VALUE;
    } else if (!registeredCurrentWindowOfSlice) {
      // Later tablet feeding the in-progress window (once per slice): register it too - a late
      // smaller tablet must lower the open allow (min tablet - 1), or it could commit while its
      // rows are still unflushed in the window, and the destroy allow must cover every
      // contributing tablet's max.
      onWindowOpen(currentWindowId, tabletId);
      registeredCurrentWindowOfSlice = true;
    }
    currentRowCount = rowStore.size();
    if (currentRowCount >= capacity) {
      closeWindow(now);
    }
  }

  /**
   * Emits the filled window as one event and starts the next window where this one ended. The
   * event's rows are materialized lazily: one full-store range is recorded on the iterator, which
   * keeps its own reference to the (about-to-be-discarded) row store. Caller must hold {@link
   * #lock}.
   */
  private void closeWindow(long now) {
    // Charge the store's full size before handing it to the event: a window that fills within one
    // batch never reaches the batch-end reconcile, and the emitted event's iterator (which pins
    // the store) must not outlive its data-pool charge.
    if (!rowStore.reconcileDataMemory()) {
      pauseOnMemoryPressure(false);
    }
    emitWindowEvent(now);
    // Reset: the next window starts where this one ended, with a fresh window id. The emitted
    // event's iterator keeps its own owner reference to the store, so its data-pool charge lives
    // until the event is consumed.
    resetCurrentWindowState();
    rowStore.releaseOwner();
    rowStore = null;
    currentRowCount = 0;
    closeSystemTime = Long.MAX_VALUE;
    currentWindowStart = now;
  }

  /**
   * Emits the current window (full store, all pushed rows) as one close event followed by its
   * compute-free destroy marker, recording the full-store range on a lazily-materializing iterator.
   * Both carry the current window id; the close event goes to the compute queue and the marker to
   * the recompute queue. The marker never overtakes the close event: when both slots hold the same
   * window the driver gives the compute event priority, and when the close event is still queued in
   * the compute queue the driver parks the marker until the window's queued compute events drain,
   * so the close event's result is always flushed first - the marker's flush is what lets the
   * source commit advance behind the window. Caller must hold {@link #lock} and the store must be
   * non-empty.
   */
  private void emitWindowEvent(long now) {
    StreamWindowEngineMetrics.getInstance().incCloseEvent();

    WindowEventRowsIterator iterator =
        new WindowEventRowsIterator(rowConverter, rowStore, lock, this);
    iterator.emitRange(Long.MIN_VALUE, Long.MAX_VALUE);
    // The window is data-complete at emission: mark the iterator finished (its isFinished() only
    // turns true once the recorded range is also fully generated).
    iterator.markFinished();
    IEventInfo event =
        new WindowEventInfo.Builder()
            .startTime(currentWindowStart)
            .endTime(now)
            .partitionKey(driverContext.getSubTaskContext().getPartitionKey())
            .rowCount(currentRowCount)
            .rowsIterator(iterator)
            .windowId(currentWindowId)
            .eventType(WindowEventType.WINDOW_CLOSE)
            .needsCompute(true)
            .build();
    tryOfferComputeEvent(event);

    // window is destroyed right after it is closed
    StreamWindowEngineMetrics.getInstance().incDestroyEvent();
    IEventInfo destroyWindowEvent =
        new WindowEventInfo.Builder()
            .startTime(currentWindowStart)
            .endTime(now)
            .partitionKey(driverContext.getSubTaskContext().getPartitionKey())
            .rowCount(currentRowCount)
            .rowsIterator(null)
            .windowId(currentWindowId)
            .eventType(WindowEventType.WINDOW_DESTROY)
            .needsCompute(false)
            .build();
    tryOfferRecomputeEvent(destroyWindowEvent);
  }

  /**
   * MAX_DELAY_WINDOW_CLOSE: a partially-filled window cannot wait forever - when the system-time
   * deadline armed at its open passes, the shared tick scheduler force-closes it here, emitting its
   * partial rows as a close event followed by the destroy marker (same emission path as a window
   * that filled normally). The next window arms a fresh deadline at its first row. Caller must hold
   * {@link #lock}.
   */
  @Override
  protected void onTick() {
    if (maxDelayWindowCloseMs <= 0 || rowStore == null || currentRowCount == 0) {
      return;
    }
    long now = nowMillis();
    if (now >= closeSystemTime) {
      StreamWindowEngineMetrics.getInstance().incForceCloseByMaxDelay();
      closeWindow(now);
    }
  }

  /**
   * Last-resort self-rescue under an unresolved data-pool memory pause (see {@code tick()}):
   * force-closes the partially-filled current window through the regular close path (close event +
   * destroy marker, store owner released) - exactly like a MAX_DELAY force-close, but triggered by
   * memory pressure instead of a configured deadline. The next window arms fresh at its first row.
   * Caller must hold {@link #lock}.
   */
  @Override
  protected boolean onMemoryPressureRescue() {
    if (rowStore == null || currentRowCount == 0) {
      // Nothing held here: a remaining data-pool charge belongs to a store pinned by an
      // outstanding event iterator and returns when the driver consumes it.
      return false;
    }
    StreamWindowEngineMetrics.getInstance().incForceCloseByMemoryPressure();
    LOGGER.warn(
        StreamNodeMessages.LOG_WINDOW_FORCE_CLOSE_BY_MEMORY_PRESSURE_961D31B5,
        getClass().getSimpleName(),
        currentWindowStart,
        DATA_POOL_PAUSE_RESCUE_GRACE_MS);
    closeWindow(nowMillis());
    return true;
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
      discardRowStore();
      currentRowCount = 0;
      closeSystemTime = Long.MAX_VALUE;
      currentWindowStart = nowMillis();
      resetCurrentWindowState();
    }
    clearAllPending();
    LOGGER.info(StreamNodeMessages.LOG_CAPACITY_WINDOW_DROPPED_488FBEBE);
  }

  @Override
  public ListenableFuture<?> forceClose() {
    synchronized (lock) {
      // Stop processing and fold in any batches buffered while paused so that no data is lost
      // before emitting the final closed event. The final flush ignores a memory pause: charging
      // is best-effort, but no buffered row may be dropped.
      stopProcessing();
      drainAllBufferedBatches();
      if (rowStore != null && !rowStore.isEmpty()) {
        // The current window never reached its capacity: emit its partial rows as the final
        // closed event.
        closeWindow(nowMillis());
      }
    }
    LOGGER.info(StreamNodeMessages.LOG_CAPACITY_WINDOW_FORCE_CLOSED_57A2067F);
    return Operator.NOT_BLOCKED;
  }

  private void resetCurrentWindowState() {
    currentWindowId = -1;
  }

  /**
   * Drops the engine's owner reference to the current window's row cache (drop / forceClose). A
   * store still pinned by an outstanding event iterator keeps its data-pool charge until that
   * iterator is done with it. Caller must hold {@link #lock}.
   */
  private void discardRowStore() {
    if (rowStore != null) {
      rowStore.releaseOwner();
      rowStore = null;
    }
  }
}
