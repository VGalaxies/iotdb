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
import org.apache.iotdb.commons.stream.StreamProperties;
import org.apache.iotdb.commons.stream.StreamWindow;
import org.apache.iotdb.streamnode.engine.scheduler.task.StreamDriverContext;
import org.apache.iotdb.streamnode.engine.sink.CommitTracker;
import org.apache.iotdb.streamnode.engine.task.StreamSubTask.DataSlice;
import org.apache.iotdb.streamnode.engine.task.StreamSubTaskContext;
import org.apache.iotdb.streamnode.i18n.StreamNodeMessages;
import org.apache.iotdb.streamnode.metrics.StreamWindowEngineMetrics;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;

public abstract class WindowEngine {

  private static final Logger LOGGER = LoggerFactory.getLogger(WindowEngine.class);

  protected static final int MAX_WINDOW_BLOCK_ROWS = 1024;

  /**
   * Grace period of an unresolved data-pool memory pause before the engine force-releases its own
   * windows (see {@code tick()}): long enough that a concurrent release elsewhere can still lift
   * the pause without any premature window close, short enough that the default configuration
   * (MAX_DELAY_WINDOW_CLOSE / MAX_IDLE_WINDOW_RETAIN both 0) cannot stall the task forever.
   */
  static final long DATA_POOL_PAUSE_RESCUE_GRACE_MS = 10_000;

  protected final StreamDriverContext driverContext;
  protected final StreamProperties streamProperties;
  protected final List<String> eventScanInputColumns;

  /**
   * Shared per-stream-task tick scheduler driving this engine's system-time triggers ({@link
   * #onTick}). Null in unit tests or when no system-time trigger is needed; then ticks can still be
   * driven manually via {@link #tick()}.
   */
  private final WindowEngineTickScheduler tickScheduler;

  /**
   * Queue of compute events (incremental + close) waiting to be consumed by the downstream driver's
   * compute path. Unbounded and FIFO: memory control for backpressure is applied out-of-band (see
   * the technical design doc), not via queue bounds.
   */
  protected final BlockingQueue<IEventInfo> computeEventQueue = new LinkedBlockingQueue<>();

  /**
   * Number of compute events per window id currently waiting in {@link #computeEventQueue}:
   * incremented <b>before</b> the queue offer and decremented after the poll, so a concurrent
   * poll's decrement cannot race past the matching increment (the queue's own offer-to-poll
   * happens-before then carries the visibility). Lets the driver defer a destroy marker until its
   * window's close event has left the queue (see {@link #hasQueuedComputeEventsForWindow}).
   */
  private final Map<Long, Integer> queuedComputeEventCounts = new ConcurrentHashMap<>();

  /**
   * Queue of recompute events (dirty cached windows destroyed after close), consumed by the
   * driver's recompute path. Unbounded and FIFO, decoupled from the compute queue.
   */
  protected final BlockingQueue<IEventInfo> recomputeEventQueue = new LinkedBlockingQueue<>();

  /** Lock guarding window state mutations across concurrent process/tick/forceClose invocations. */
  protected final Object lock = new Object();

  /**
   * Future used to block the driver when there are no events available. When an event is added to
   * the queue (or pending data becomes available), this future is completed to wake up the driver.
   */
  protected final AtomicReference<SettableFuture<?>> driverBlockedFuture = new AtomicReference<>();

  /**
   * Future used to block the driver when there are no recompute events available. Completed when a
   * recompute event is added to {@link #recomputeEventQueue}.
   */
  protected final AtomicReference<SettableFuture<?>> recomputeDriverBlockedFuture =
      new AtomicReference<>();

  /**
   * Batches buffered while processing was inactive ({@code stop()}/{@code drop()} or a <b>memory
   * pause</b>): pushed raw DataSlices are kept here reception-only, and folded in on the next
   * {@code start()} / resume (or in the next active {@link #pushAndProcess(List)}) so no data is
   * lost across a pause. Each batch carries its charge in the global window <b>input</b> memory
   * pool, returned when the batch is processed or discarded.
   */
  protected final BlockingQueue<BufferedBatch> bufferedBatches = new LinkedBlockingQueue<>();

  /** One batch buffered while processing was inactive, plus its input-pool charge. */
  protected static final class BufferedBatch {
    final List<DataSlice> slices;
    final long reservedBytes;

    BufferedBatch(List<DataSlice> slices, long reservedBytes) {
      this.slices = slices;
      this.reservedBytes = reservedBytes;
    }
  }

  /**
   * Group-done obligations (one entry per slice, counted per tablet) of the batch currently inside
   * {@link #processBatchProtected}: {@link #markTabletGroupDone} consumes its tablet's entry as
   * {@code processDataSlices} advances, and the failure path reports what is left. Non-null only
   * while a batch is being processed. Guarded by {@link #lock} (processBatchProtected callers hold
   * it).
   */
  private Map<Long, Integer> pendingGroupDoneTablets;

  /**
   * When {@code false}, pushed data is buffered into the pending-data queue instead of processed.
   */
  private volatile boolean processingRunning;

  /** When {@code true}, pushed batches are only buffered instead of processed inline. */
  private volatile boolean processingPaused;

  /**
   * Memory-driven pause, distinct from the user-driven {@link #processingPaused}: set when a
   * <b>data</b> or <b>output</b> pool reservation fails (the store cannot grow, or a materialized
   * block cannot be charged). While set, pushed batches are only buffered (charging the input pool)
   * and ticks skip window advancement; a data/output release retries the reservation via the memory
   * controller's release listener (plus the tick as a safety net), resuming processing or
   * re-pausing if the pool is still exhausted.
   */
  private volatile boolean memoryPaused;

  /**
   * Which pool caused the current memory pause: {@code true} = the <b>output</b> pool (a
   * materialized block could not be charged), {@code false} = the <b>data</b> pool (a store could
   * not grow). The resume attempt re-verifies this pool's state: an output pause is only lifted
   * when the output pool has room again - a data release does not relieve output pressure.
   */
  private volatile boolean memoryPausedOnOutput;

  /**
   * System time (from {@link #nowMillis()}) when the current unresolved data-pool memory pause was
   * first observed by a tick; 0 while no stall is tracked. Guards {@link
   * #rescueFromDataPoolPauseIfStalled()}, which re-arms it after each successful rescue and resets
   * it as soon as the pause is lifted. Only touched under {@link #lock}.
   */
  private long dataPoolPauseSinceMs;

  /**
   * The engine's hook in the global memory controller, invoked (asynchronously) after a data or
   * output release. Stored as a field so {@link #markDropped()} can unregister the same instance.
   */
  private final Runnable memoryResumeListener = this::tryResumeFromMemoryPressure;

  /**
   * Once set, the engine has been {@link #drop() dropped} and can no longer be restarted; a fresh
   * engine instance is required instead. Guards {@link #startProcessing()} so that {@code start()}
   * after {@code drop()} is a no-op.
   */
  private volatile boolean dropped;

  protected WindowEngine(
      StreamDriverContext driverContext,
      StreamProperties streamProperties,
      List<String> eventScanInputColumns,
      WindowEngineTickScheduler tickScheduler) {
    this.driverContext = driverContext;
    this.streamProperties = streamProperties;
    this.eventScanInputColumns = eventScanInputColumns;
    this.tickScheduler = tickScheduler;
    // A data/output release anywhere in the node may lift this engine's memory pause.
    GlobalWindowMemoryController.getInstance().registerMemoryReleaseListener(memoryResumeListener);
  }

  public StreamDriverContext getDriverContext() {
    return driverContext;
  }

  /**
   * Accepts rows from the given tablet slices on the ingestion thread and processes them inline
   * (event-driven engines). Reception itself never loses data: a batch that cannot be processed
   * right now is buffered (charging the global window <b>input</b> pool), and when even the input
   * pool is exhausted the returned future stays incomplete until memory frees - <b>backpressure to
   * the producer</b>.
   *
   * @param dataSlices tablet slices to push
   * @return {@link Operator#NOT_BLOCKED}, or a future that completes when the producer may push
   *     again
   */
  public abstract ListenableFuture<?> push(List<DataSlice> dataSlices);

  /**
   * Push skeleton shared by the event-driven engines: while processing is inactive (paused,
   * stopped, dropped or <b>memory-paused</b>) the batch is only <i>buffered</i> (reception stays
   * unaffected, and a later resume or {@code start()} folds the buffered batches in); while active,
   * any batches buffered earlier are processed first, then this batch - each under {@link #lock}
   * with per-batch exception protection. A failed input reservation turns into input backpressure
   * (see {@link #bufferBatch(List)}).
   *
   * @return null when the batch was processed or buffered with its input charge; otherwise a future
   *     that completes when the input pool frees memory (the producer is throttled)
   */
  protected ListenableFuture<?> pushAndProcess(List<DataSlice> dataSlices) {
    if (dataSlices == null || dataSlices.isEmpty()) {
      return null;
    }
    long pushRowCount = 0;
    for (DataSlice slice : dataSlices) {
      pushRowCount += slice.getEndRow() - slice.getStartRow();
    }
    StreamWindowEngineMetrics.getInstance().incPush(pushRowCount);
    if (!isProcessingActive()) {
      // A data/output release may have lifted the memory pause since the last check: retry the
      // resume (which also drains the buffered batches) before buffering this one.
      if (memoryPaused) {
        tryResumeFromMemoryPressure();
      }
      if (!isProcessingActive()) {
        return bufferBatch(dataSlices);
      }
    }
    synchronized (lock) {
      drainBufferedBatches();
      if (memoryPaused || !checkDataMemoryHeadroom(dataSlices)) {
        return bufferBatch(dataSlices);
      }
      processBatchProtected(dataSlices);
    }
    return null;
  }

  /**
   * Pre-checks the <b>data</b> pool for one batch's worth of row growth before processing it: on
   * exhaustion the engine memory-pauses (further processing halts; pushes buffer into the input
   * pool) and the batch is buffered unprocessed. The probe reservation is returned immediately -
   * the real charge happens in the batch-end reconcile - so this is a watermark, not an exact gate.
   */
  private boolean checkDataMemoryHeadroom(List<DataSlice> batch) {
    long headroom = estimateInputBytes(batch);
    if (GlobalWindowMemoryController.getInstance().tryReserveDataMemory(headroom)) {
      GlobalWindowMemoryController.getInstance().releaseDataMemory(headroom);
      return true;
    }
    pauseOnMemoryPressure(false);
    return false;
  }

  /**
   * Buffers one batch reception-only (processing inactive) and charges its estimated bytes to the
   * global window <b>input</b> memory pool. On pool exhaustion the batch is still buffered
   * (reception must not lose data, so it stays uncharged) and a future completing on the next input
   * release is returned, throttling the producer.
   *
   * @return null when the input charge succeeded; otherwise the input backpressure future
   */
  private ListenableFuture<?> bufferBatch(List<DataSlice> dataSlices) {
    long bytes = estimateInputBytes(dataSlices);
    boolean reserved = GlobalWindowMemoryController.getInstance().tryReserveInputMemory(bytes);
    bufferedBatches.add(new BufferedBatch(dataSlices, reserved ? bytes : 0));
    return reserved ? null : GlobalWindowMemoryController.getInstance().newInputBlockedFuture();
  }

  /**
   * Rough heap estimate of one pushed batch's pending input: each slice's row share of its tablet's
   * arrays - the time column, one value column per measurement and the carried commit id, 8 bytes
   * each. Binary payloads are underestimated by design; the estimate only steers the input memory
   * watermark.
   */
  private static long estimateInputBytes(List<DataSlice> dataSlices) {
    long bytes = 0;
    for (DataSlice slice : dataSlices) {
      int columnCount = slice.getTablet().getSchemas().size();
      bytes += (long) (slice.getEndRow() - slice.getStartRow()) * (2 + columnCount) * Long.BYTES;
    }
    return bytes;
  }

  /**
   * Processes one pushed batch under {@link #lock}: per-row window state updates and event
   * emission, on the ingestion thread (or on the caller of {@code start()} for batches buffered
   * while paused). The period window does not accept data and throws {@link
   * UnsupportedOperationException}.
   */
  protected abstract void processDataSlices(List<DataSlice> dataSlices);

  /**
   * Processes batches buffered while processing was inactive, returning each batch's input-pool
   * charge as it leaves the buffer. Stops at the head batch when the data pool is exhausted (the
   * engine re-pauses and the batch stays buffered with its charge); the tick's resume retry or a
   * later release picks it up. Caller must hold {@link #lock}.
   */
  protected void drainBufferedBatches() {
    while (!memoryPaused) {
      // Peek before polling: a batch that fails the headroom check stays at the head (order is
      // preserved and its input charge is only returned once it is actually processed).
      BufferedBatch batch = bufferedBatches.peek();
      if (batch == null) {
        return;
      }
      if (!checkDataMemoryHeadroom(batch.slices)) {
        return;
      }
      bufferedBatches.poll();
      if (batch.reservedBytes > 0) {
        GlobalWindowMemoryController.getInstance().releaseInputMemory(batch.reservedBytes);
      }
      processBatchProtected(batch.slices);
    }
  }

  /**
   * Drains <i>all</i> buffered batches regardless of the memory pause (the final flush of {@code
   * forceClose}: charging is best-effort, but no buffered row may be dropped). Caller must hold
   * {@link #lock}.
   */
  protected void drainAllBufferedBatches() {
    BufferedBatch batch;
    while ((batch = bufferedBatches.poll()) != null) {
      if (batch.reservedBytes > 0) {
        GlobalWindowMemoryController.getInstance().releaseInputMemory(batch.reservedBytes);
      }
      processBatchProtected(batch.slices);
    }
  }

  /**
   * Exception protection (design doc): a failing batch must not break the ingestion (or driver)
   * path. Log the error and drop the failed work - the batch's unprocessed rows and its
   * not-yet-offered events. No retry by design: a persistent error would degrade into a hot loop,
   * and the exception itself is the alarm.
   *
   * <p>Group-done compensation: {@code markTabletGroupDone} fires per processed slice inside {@code
   * processDataSlices}, so a mid-batch failure would leave the batch's not-yet-marked tablets
   * unreported - {@link CommitTracker}'s consecutive-completion watermark would stall before them
   * and source commit would never advance again (subscriptions/WAL backlog). The remaining tablets
   * are therefore reported in {@code finally}: the batch's rows are dropped either way, so from the
   * tracker's perspective their scans are finished. Data that already reached a window stays
   * protected by that window's commit allow.
   */
  private void processBatchProtected(List<DataSlice> batch) {
    // One pending group-done obligation per slice (slices of the same tablet each carry one);
    // consumed by markTabletGroupDone as processDataSlices advances through the batch.
    Map<Long, Integer> pendingGroupDoneTablets = new LinkedHashMap<>();
    for (DataSlice slice : batch) {
      if (slice.getTabletId() >= 0) {
        pendingGroupDoneTablets.merge(slice.getTabletId(), 1, Integer::sum);
      }
    }
    this.pendingGroupDoneTablets = pendingGroupDoneTablets;
    try {
      processDataSlices(batch);
    } catch (Exception e) {
      StreamWindowEngineMetrics.getInstance().incDroppedBatch();
      LOGGER.error(
          StreamNodeMessages.LOG_FAILED_TO_PROCESS_PUSHED_BATCH_CD3572E3,
          getClass().getSimpleName(),
          e);
    } finally {
      this.pendingGroupDoneTablets = null;
      reportRemainingGroupDones(pendingGroupDoneTablets);
    }
  }

  /**
   * Reports the per-slice group-done obligations a failed batch never got to. Best-effort per
   * tablet: one tablet's tracker failure must not stop the others from unblocking source commit.
   */
  private void reportRemainingGroupDones(Map<Long, Integer> pendingGroupDoneTablets) {
    for (Map.Entry<Long, Integer> entry : pendingGroupDoneTablets.entrySet()) {
      for (int i = 0; i < entry.getValue(); i++) {
        try {
          markTabletGroupDone(entry.getKey());
        } catch (Exception e) {
          LOGGER.error(
              StreamNodeMessages.LOG_FAILED_TO_REPORT_GROUP_DONE_FOR_TABLET_A82B9F2A,
              entry.getKey(),
              e);
        }
      }
    }
  }

  /**
   * Starts (or restarts) the window's processing logic. Called once from the subclass constructor
   * (after initialization) and again by the owning stream task when it transitions to RUNNING, so
   * implementations must be idempotent. For period windows this activates the internal timer; for
   * event-driven windows this unblocks pending-data processing (and drains data buffered while
   * paused).
   */
  public abstract void start();

  /**
   * Pauses the window's processing logic while leaving data reception ({@link #push}) unaffected.
   * Pushed data continues to be buffered; a subsequent {@link #start()} resumes processing of the
   * buffered data.
   */
  public abstract void stop();

  /**
   * Stops processing and cleans up all internal state (queued data, pending events, window buffers,
   * timers/threads). After {@code drop} the engine cannot be restarted.
   */
  public abstract void drop();

  /**
   * Forces the current window to close and emits any pending data as a final event.
   *
   * @return a ListenableFuture that completes when the forced close is done
   */
  public abstract ListenableFuture<?> forceClose();

  /**
   * System-time-driven hook invoked by the shared per-task tick scheduler (see {@link
   * WindowEngineTickScheduler}). Subclasses override it to advance windows on system time
   * (MAX_DELAY_WINDOW_CLOSE force-close, MAX_IDLE_WINDOW_RETAIN force-destroy).
   */
  protected void onTick() {}

  /**
   * One tick of the shared per-task tick scheduler: runs {@link #onTick} under {@link #lock} and
   * then wakes any driver that may have been waiting on an open window's event. No-op while the
   * engine is paused, stopped or dropped. While <b>memory-paused</b>, the tick first retries the
   * resume (safety net behind the memory controller's release listeners). An <b>output</b>-pool
   * pause then skips window advancement - producing new events would only worsen the output
   * pressure. A <b>data</b>-pool pause is the opposite: the time-driven force-close / force-destroy
   * only releases data-pool memory (events are materialized lazily), so {@link #onTick} still runs
   * - otherwise an exhausted data pool deadlocks: with processing halted the watermark cannot
   * advance either, and force-close would be the engine's only self-recovery. That recovery
   * requires a configured MAX_DELAY_WINDOW_CLOSE / MAX_IDLE_WINDOW_RETAIN, though: at their default
   * 0 {@link #onTick} is a no-op and the pause would stall the task forever. A data-pool pause that
   * outlives {@link #DATA_POOL_PAUSE_RESCUE_GRACE_MS} therefore also triggers {@link
   * #onMemoryPressureRescue} - the engine force-releases its own windows as the last resort.
   */
  final void tick() {
    if (!processingRunning || processingPaused || dropped) {
      return;
    }
    if (memoryPaused && !tryResumeFromMemoryPressure()) {
      if (memoryPausedOnOutput) {
        dataPoolPauseSinceMs = 0L;
        return;
      }
      // Data-pool pause: run the time-driven window advancement anyway (it only releases memory).
      synchronized (lock) {
        if (!processingRunning || processingPaused || dropped) {
          return;
        }
        onTick();
        rescueFromDataPoolPauseIfStalled();
        completeDriverBlockedFuture();
      }
      return;
    }
    dataPoolPauseSinceMs = 0L;
    synchronized (lock) {
      if (!processingRunning || processingPaused || dropped || memoryPaused) {
        return;
      }
      onTick();
      // A tick may have appended rows to an already-offered open window's event (unblocking it) or
      // offered new events; wake the driver in either case.
      completeDriverBlockedFuture();
    }
  }

  /**
   * Safety net for a data-pool memory pause that outlives every configured recovery (see {@code
   * tick()}): arms a grace timer on the first paused tick and, once {@link
   * #DATA_POOL_PAUSE_RESCUE_GRACE_MS} has elapsed without a resume, asks the engine to
   * force-release one window via {@link #onMemoryPressureRescue}. The timer is re-armed after each
   * successful rescue, so windows are released gradually (one per grace period) and a concurrent
   * release elsewhere can still lift the pause without further premature closes. The timer resets
   * as soon as the pause is lifted. Caller must hold {@link #lock}.
   */
  private void rescueFromDataPoolPauseIfStalled() {
    long now = nowMillis();
    if (dataPoolPauseSinceMs == 0L) {
      dataPoolPauseSinceMs = now;
      return;
    }
    if (now - dataPoolPauseSinceMs < DATA_POOL_PAUSE_RESCUE_GRACE_MS) {
      return;
    }
    if (onMemoryPressureRescue()) {
      dataPoolPauseSinceMs = now;
    }
  }

  /**
   * Last-resort self-rescue under an unresolved data-pool memory pause, invoked by {@code tick()}
   * under {@link #lock}. Implementations force-release one window (destroying it regardless of
   * retention options - retaining it would keep holding the very memory the rescue must return) and
   * return true; return false when the engine holds nothing it can release (e.g. all stores are
   * pinned by outstanding event iterators, whose memory returns when the driver consumes them).
   * Default: nothing to release.
   */
  protected boolean onMemoryPressureRescue() {
    return false;
  }

  /** Current system time in milliseconds. Factored out so tests can override for determinism. */
  protected long nowMillis() {
    return System.currentTimeMillis();
  }

  /**
   * Offers an event to the (unbounded) compute event queue, waking the driver.
   *
   * @param eventInfo the event to offer
   * @return true (the unbounded queue always accepts the event)
   */
  protected boolean tryOfferComputeEvent(IEventInfo eventInfo) {
    // Count BEFORE the offer: a concurrent poll's decrement (computeIfPresent) could otherwise run
    // between the offer and this merge and miss it, leaking a count that gates the window's destroy
    // marker forever.
    if (eventInfo.getWindowId() >= 0) {
      queuedComputeEventCounts.merge(eventInfo.getWindowId(), 1, Integer::sum);
    }
    boolean offered = computeEventQueue.offer(eventInfo);
    if (offered) {
      completeDriverBlockedFuture();
    } else {
      // Rejected (unreachable for the unbounded queue): undo the count so it cannot outlive the
      // event.
      uncountQueuedComputeEvent(eventInfo.getWindowId());
    }
    return offered;
  }

  /**
   * Tries to offer a recompute event to the recompute queue. The recompute queue is unbounded, so
   * this always succeeds and never blocks the producer.
   *
   * @param eventInfo the recompute event to offer
   * @return true (the unbounded queue always accepts the event)
   */
  protected boolean tryOfferRecomputeEvent(IEventInfo eventInfo) {
    boolean offered = recomputeEventQueue.offer(eventInfo);
    if (offered) {
      completeRecomputeDriverBlockedFuture();
    }
    return offered;
  }

  /**
   * Completes the recompute driver blocked future (if any), waking a driver waiting for a recompute
   * event. Called when a recompute event is added to the queue.
   */
  protected void completeRecomputeDriverBlockedFuture() {
    SettableFuture<?> future = recomputeDriverBlockedFuture.getAndSet(null);
    if (future != null && !future.isDone()) {
      future.set(null);
    }
  }

  /**
   * Completes the driver blocked future (if any), waking up any waiting driver. This should be
   * called when an event is added to the queue or when pending data becomes available.
   */
  protected void completeDriverBlockedFuture() {
    SettableFuture<?> future = driverBlockedFuture.getAndSet(null);
    if (future != null && !future.isDone()) {
      future.set(null);
    }
  }

  /**
   * @return true if the engine is running and not paused, memory-paused or dropped; only then
   *     should pending data be drained and processed.
   */
  protected final boolean isProcessingActive() {
    return processingRunning && !processingPaused && !dropped && !memoryPaused;
  }

  /**
   * @return whether the engine is currently memory-paused (data/output pool exhaustion).
   */
  boolean isMemoryPaused() {
    return memoryPaused;
  }

  /**
   * Enters the memory pause: window processing halts (pushes buffer into the input pool; ticks keep
   * running the time-driven force-close/force-destroy on a data-pool pause but skip window
   * advancement on an output-pool pause) until a data/output release lifts it. Idempotent; all
   * callers hold {@link #lock}.
   *
   * @param outputPool whether the pause was caused by the <b>output</b> pool (as opposed to the
   *     <b>data</b> pool); decides which pool the resume attempt re-verifies
   */
  protected void pauseOnMemoryPressure(boolean outputPool) {
    if (memoryPaused) {
      return;
    }
    memoryPaused = true;
    memoryPausedOnOutput = outputPool;
    StreamWindowEngineMetrics.getInstance().incMemoryPause();
    GlobalWindowMemoryController controller = GlobalWindowMemoryController.getInstance();
    // One WARN per pause entry (not per tick): an exhaustion stall must be visible in the logs,
    // not only in the metrics - processing halts until a memory release frees the pool.
    LOGGER.warn(
        StreamNodeMessages.LOG_MEMORY_PAUSED_BY_POOL_EXHAUSTION_8BB73B2A,
        getClass().getSimpleName(),
        outputPool ? "output" : "data",
        controller.getDataUsedBytes(),
        controller.getDataTotalBytes(),
        controller.getOutputUsedBytes(),
        controller.getOutputTotalBytes());
    // The controller only fires its release listeners while some engine is paused.
    controller.engineMemoryPaused();
  }

  /**
   * Tries to lift the memory pause and drain the batches buffered while paused. The pause cause is
   * re-verified first (an output pause requires the output pool to have room again; a data pause
   * requires the head buffered batch to fit the data pool again - an exhausted pool keeps the pause
   * flag set instead of clearing and immediately re-entering it, which would re-fire the pause WARN
   * and metric on every retry). Still-insufficient memory leaves the engine paused with the
   * remaining batches buffered until the next release. Invoked from the memory controller's notify
   * thread (after a data/output release), from the tick thread (safety net) and opportunistically
   * from {@code push}.
   *
   * @return whether processing is currently active (pause lifted and not re-entered)
   */
  private boolean tryResumeFromMemoryPressure() {
    if (!memoryPaused) {
      return isProcessingActive();
    }
    if (dropped || !processingRunning || processingPaused) {
      return false;
    }
    synchronized (lock) {
      if (!memoryPaused) {
        return isProcessingActive();
      }
      if (dropped || !processingRunning || processingPaused) {
        return false;
      }
      if (memoryPausedOnOutput && !GlobalWindowMemoryController.getInstance().hasOutputHeadroom()) {
        // Output pressure is not relieved (e.g. the release was on the data pool): stay paused.
        return false;
      }
      if (!memoryPausedOnOutput && !hasHeadroomForHeadBatch()) {
        // Data pressure is not relieved: stay paused (the flag stays set, so the WARN and metric
        // are not re-fired; the drain would only fail on the same head batch anyway).
        return false;
      }
      clearMemoryPaused();
      drainBufferedBatches();
      return !memoryPaused;
    }
  }

  /**
   * Pre-checks whether a data-pause resume retry could make progress: the head buffered batch
   * (which the drain re-verifies per batch) must fit the data pool again. Pure probe - the
   * reservation is returned immediately, and {@code checkDataMemoryHeadroom}'s pause branch is a
   * no-op here because the engine is memory-paused. With no buffered batch nothing blocks the
   * drain. Caller must hold {@link #lock}.
   */
  private boolean hasHeadroomForHeadBatch() {
    BufferedBatch head = bufferedBatches.peek();
    return head == null || checkDataMemoryHeadroom(head.slices);
  }

  /** Leaves the memory-paused state (transition only; caller holds {@link #lock}). */
  private void clearMemoryPaused() {
    memoryPaused = false;
    memoryPausedOnOutput = false;
    GlobalWindowMemoryController.getInstance().engineMemoryResumed();
  }

  /**
   * Starts (or resumes) processing. Idempotent. Unblocks pending-data processing, registers the
   * engine with the shared per-task tick scheduler, and synchronously processes any batches
   * buffered while paused. A lingering memory pause is cleared here (a still-exhausted pool
   * re-pauses on the first drained batch).
   */
  protected void startProcessing() {
    // A dropped engine is terminal: do not (re)arm processing. Callers must create a new engine
    // instance instead of calling start() after drop().
    if (dropped) {
      return;
    }
    processingPaused = false;
    processingRunning = true;
    if (tickScheduler != null) {
      tickScheduler.register(this);
    }
    synchronized (lock) {
      if (memoryPaused) {
        clearMemoryPaused();
      }
      drainBufferedBatches();
    }
  }

  /** Pauses processing: pushes are buffered instead of processed, and ticks stop firing. */
  protected void pauseProcessing() {
    processingPaused = true;
  }

  /**
   * Stops processing: pushes are buffered instead of processed, and ticks stop firing. Idempotent.
   * After this returns pending data is no longer processed until a subsequent {@code start()}. A
   * lingering memory pause is cleared (it is re-armed on demand by the next reservation failure).
   */
  protected void stopProcessing() {
    processingRunning = false;
    processingPaused = false;
    if (memoryPaused) {
      synchronized (lock) {
        if (memoryPaused) {
          clearMemoryPaused();
        }
      }
    }
  }

  /**
   * Marks the engine as dropped so {@link #startProcessing()} (and thus {@code start()}) becomes a
   * no-op, and unregisters it from the shared tick scheduler and the memory controller's release
   * listeners. Intended to be called once at the beginning of a subclass {@code drop()}
   * implementation.
   */
  protected void markDropped() {
    dropped = true;
    if (memoryPaused) {
      synchronized (lock) {
        if (memoryPaused) {
          clearMemoryPaused();
        }
      }
    }
    if (tickScheduler != null) {
      tickScheduler.unregister(this);
    }
    GlobalWindowMemoryController.getInstance()
        .unregisterMemoryReleaseListener(memoryResumeListener);
  }

  /** Clears all buffered data and pending events, returning the buffered batches' input charges. */
  protected void clearAllPending() {
    synchronized (lock) {
      BufferedBatch batch;
      while ((batch = bufferedBatches.poll()) != null) {
        if (batch.reservedBytes > 0) {
          GlobalWindowMemoryController.getInstance().releaseInputMemory(batch.reservedBytes);
        }
      }
      releasePendingEvents(computeEventQueue);
      releasePendingEvents(recomputeEventQueue);
      // The queues are drained above; forget their per-window counts with them.
      queuedComputeEventCounts.clear();
    }
  }

  /**
   * Polls every event from the queue and releases it. The purged events will never be consumed:
   * releasing each one lets its iterator unpin the window's row store (returning the store's
   * data-pool charge when it was the last owner) instead of leaking the charge.
   */
  private static void releasePendingEvents(BlockingQueue<IEventInfo> queue) {
    IEventInfo event;
    while ((event = queue.poll()) != null) {
      event.release();
    }
  }

  /**
   * Allocates the next task-scoped window id and records it as the in-progress window. Returns
   * {@code -1} when no sub-task context is attached (unit tests without commit tracking).
   */
  protected long allocateWindowId() {
    StreamSubTaskContext subTaskContext = getSubTaskContext();
    if (subTaskContext == null) {
      return -1;
    }
    return subTaskContext.allocateWindowId();
  }

  /**
   * Notifies {@link CommitTracker#onWindowOpen} that {@code tabletId} entered the window with the
   * given id, so the tablet's source commit stays gated behind that window's flush. No-op without a
   * tracker or for unassigned ids ({@code < 0}).
   */
  protected void onWindowOpen(long windowId, long tabletId) {
    if (windowId < 0 || tabletId < 0) {
      return;
    }
    CommitTracker tracker = getCommitTracker();
    if (tracker != null) {
      tracker.onWindowOpen(windowId, tabletId);
    }
  }

  /**
   * Notifies {@link CommitTracker} that this partition finished scanning {@code tabletId}. Must be
   * called once for the last slice of a batch even when all rows were dropped (e.g. late data in a
   * tumble window) so the tablet does not permanently block source commit.
   */
  protected void markTabletGroupDone(long tabletId) {
    if (tabletId < 0) {
      return;
    }
    // While a batch is inside processBatchProtected, consume its pending obligation for this
    // tablet (one per slice): the failure path only reports the slices processing never reached.
    if (pendingGroupDoneTablets != null) {
      pendingGroupDoneTablets.computeIfPresent(
          tabletId, (id, count) -> count > 1 ? count - 1 : null);
    }
    CommitTracker tracker = getCommitTracker();
    if (tracker == null) {
      return;
    }
    tracker.onTabletGroupDone(tabletId);
  }

  private CommitTracker getCommitTracker() {
    StreamSubTaskContext subTaskContext = getSubTaskContext();
    return subTaskContext == null ? null : subTaskContext.getCommitTracker();
  }

  private StreamSubTaskContext getSubTaskContext() {
    if (driverContext == null) {
      return null;
    }
    return driverContext.getSubTaskContext();
  }

  /**
   * @return true if there is at least one compute event waiting in the queue.
   */
  public boolean hasPendingComputeEvent() {
    return !computeEventQueue.isEmpty();
  }

  /**
   * @return the next compute event without removing it, or null if the queue is empty.
   */
  public IEventInfo peekComputeEvent() {
    return computeEventQueue.peek();
  }

  /**
   * @return and removes the next compute event, or null if the queue is empty.
   */
  public IEventInfo pollComputeEvent() {
    IEventInfo event = computeEventQueue.poll();
    if (event != null) {
      uncountQueuedComputeEvent(event.getWindowId());
    }
    return event;
  }

  /**
   * Whether {@link #computeEventQueue} still holds events of {@code windowId}. The driver defers a
   * destroy-typed recompute head (a clean marker or a dirty full-recompute event) while this is
   * true: pushing it to the sink first would flush its EOF entry and let the source commit advance
   * past the window's close event whose result has not been written back yet - a crash in between
   * loses those rows.
   */
  public boolean hasQueuedComputeEventsForWindow(long windowId) {
    return windowId >= 0 && queuedComputeEventCounts.containsKey(windowId);
  }

  /**
   * Drops one queued-event count of {@code windowId} (the event was polled, or the offer was
   * rejected); removing the last count un-gates the window's destroy marker.
   */
  private void uncountQueuedComputeEvent(long windowId) {
    if (windowId >= 0) {
      queuedComputeEventCounts.computeIfPresent(
          windowId, (id, count) -> count > 1 ? count - 1 : null);
    }
  }

  /**
   * @return true if there is at least one recompute event waiting in the recompute queue.
   */
  public boolean hasPendingRecomputeEvent() {
    return !recomputeEventQueue.isEmpty();
  }

  /**
   * @return the next recompute event without removing it, or null if the recompute queue is empty.
   */
  public IEventInfo peekRecomputeEvent() {
    return recomputeEventQueue.peek();
  }

  /**
   * @return and removes the next recompute event, or null if the recompute queue is empty.
   */
  public IEventInfo pollRecomputeEvent() {
    return recomputeEventQueue.poll();
  }

  /**
   * Returns a future that completes when a compute event becomes available. If one is already
   * queued, a pre-completed future is returned so the driver can proceed immediately. (Pushes are
   * processed inline on the ingestion thread, so there is never pending-but-unprocessed data while
   * processing is active.)
   *
   * <p>The emptiness check and the future's installation share no lock with {@link
   * #tryOfferComputeEvent}'s completion, so a producer may offer an event (completing as a no-op -
   * no future installed yet) in between. The installation therefore re-checks the queue and
   * completes the fresh future itself, closing that lost wakeup - the driver would otherwise sleep
   * until the next offer or tick, and the period engine never ticks (a month-period would sleep a
   * month).
   *
   * @return listenable future used to block the driver when no compute events are available
   */
  public ListenableFuture<?> getBlockedFuture() {
    if (!computeEventQueue.isEmpty()) {
      // Events are available, return a completed future.
      SettableFuture<Void> future = SettableFuture.create();
      future.set(null);
      return future;
    }
    // No events available, return a blocking future that will be completed when an event is added.
    StreamWindowEngineMetrics.getInstance().incDriverBlocked();
    while (true) {
      SettableFuture<?> existing = driverBlockedFuture.get();
      if (existing != null) {
        // A future installed by an earlier call is completed by the next offer (the completion
        // path swaps it out of the field), so it cannot miss a wakeup.
        return existing;
      }
      // Create a new blocking future and install it atomically.
      SettableFuture<Void> newFuture = SettableFuture.create();
      if (driverBlockedFuture.compareAndSet(null, newFuture)) {
        // Lost-wakeup guard: an offer may have completed (as a no-op) between the emptiness check
        // above and this installation. Re-check the queue and complete the fresh future ourselves;
        // an offer arriving after the installation still completes it via the completion path.
        if (!computeEventQueue.isEmpty()) {
          driverBlockedFuture.compareAndSet(newFuture, null);
          newFuture.set(null);
        }
        return newFuture;
      }
      // Another thread won the installation; its future may already be completed and cleared
      // meanwhile, so retry instead of returning null.
    }
  }

  /**
   * Returns a future that completes when a recompute event becomes available. If a recompute event
   * is already queued, a pre-completed future is returned so the driver can proceed immediately.
   *
   * <p>Carries the same lost-wakeup guard as {@link #getBlockedFuture()}: the queue is re-checked
   * after the future's installation, so a recompute offer completing as a no-op (no future
   * installed yet) cannot put the driver to sleep with an event already queued.
   *
   * @return listenable future used to block the driver when no recompute events are available
   */
  public ListenableFuture<?> getRecomputeBlockedFuture() {
    while (true) {
      if (!recomputeEventQueue.isEmpty()) {
        SettableFuture<Void> future = SettableFuture.create();
        future.set(null);
        return future;
      }
      SettableFuture<?> existing = recomputeDriverBlockedFuture.get();
      if (existing != null) {
        return existing;
      }
      SettableFuture<Void> newFuture = SettableFuture.create();
      if (recomputeDriverBlockedFuture.compareAndSet(null, newFuture)) {
        // Lost-wakeup guard, as in getBlockedFuture().
        if (!recomputeEventQueue.isEmpty()) {
          recomputeDriverBlockedFuture.compareAndSet(newFuture, null);
          newFuture.set(null);
        }
        return newFuture;
      }
    }
  }

  /**
   * Factory method that creates the concrete window engine for the given window definition.
   *
   * @param window stream window definition
   * @param driverContext context for this window
   * @param streamProperties stream properties
   * @param tickScheduler per-stream-task shared tick scheduler driving system-time triggers (may be
   *     null, e.g. in unit tests)
   * @return concrete WindowEngine implementation
   */
  public static WindowEngine create(
      StreamWindow window,
      StreamDriverContext driverContext,
      StreamProperties streamProperties,
      WindowEngineTickScheduler tickScheduler) {
    switch (window.getType()) {
      case PERIOD:
        // The period window drives itself with its own timer; no shared tick is needed.
        return new PeriodWindowEngine(window, driverContext, streamProperties);
      case TUMBLE:
        return new TumbleWindowEngine(window, driverContext, streamProperties, tickScheduler);
      case CAPACITY:
        return new CapacityWindowEngine(window, driverContext, streamProperties, tickScheduler);
      default:
        throw new UnsupportedOperationException(
            String.format(
                StreamNodeMessages.EXCEPTION_UNSUPPORTED_WINDOW_TYPE_E2440AEF, window.getType()));
    }
  }
}
