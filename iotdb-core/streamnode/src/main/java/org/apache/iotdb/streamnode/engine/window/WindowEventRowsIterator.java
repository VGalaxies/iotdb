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
import org.apache.iotdb.streamnode.engine.window.datastructure.WindowRowList;
import org.apache.iotdb.streamnode.metrics.StreamWindowEngineMetrics;
import org.apache.iotdb.streamnode.utils.IEventRowsIterator;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;

import java.util.ArrayDeque;

/**
 * Iterates over the data rows of a window event, <i>lazily</i>: no TsBlock is materialized when the
 * event is created. Instead, each emission point of the engine (a pre-close incremental batch, the
 * window close, or the destroy-time full recompute) records a <b>row range</b> on this iterator via
 * {@link #emitRange(long, long)}; the rows are only turned into TsBlocks when the consumer calls
 * {@link #next()}.
 *
 * <p>Recorded ranges are bounded by <b>event timestamps</b>, not store indices: late rows can
 * insert below an already-recorded range and shift physical indices, while timestamp bounds stay
 * valid. A range covers the rows whose timestamps lie in {@code (fromExclusive, toExclusive)},
 * where {@code toExclusive == Long.MAX_VALUE} means "through the last row". A to-the-end range
 * resolves against the store's <b>frozen view</b> (the store freezes at window close and diverts
 * late upserts to a separate map), so a close event always materializes exactly the rows counted at
 * close no matter how late the consumer runs; the late rows surface only in the destroy-time full
 * recompute, which reads a merged copy.
 *
 * <p>Each {@link #next()} materializes at most {@code maxWindowBlockRows} rows of the head range
 * into one TsBlock, walking the store's <b>device-major order</b> from a generation-progress
 * cursor: devices before the cursor device are fully decided, the cursor device resumes strictly
 * after its last emitted row (the row's timestamp identifies it exactly - per-device timestamps are
 * unique and indexed rows are never removed or re-keyed, so the seek plus one advance is a
 * node-identity resume), and later devices are walked from their first row with the range's
 * timestamp predicate. Resuming by row identity is what fixes the tail loss when a block boundary
 * falls inside an equal-timestamp run (which spans devices in the device-major order). Rows of the
 * cursor device arriving at or below it are skipped (late data, corrected by the destroy-time
 * recompute); rows of later devices are emitted whenever their timestamp satisfies the range. Once
 * a range is fully generated, {@link #next()} returns {@code null} for it (the consumer parks on
 * {@link #isBlocked()} or spins once and continues with the next range).
 *
 * <p>All state is guarded by the owning engine's lock ({@code dataLock}): the engine records ranges
 * under that lock while processing pushes, and the consumer materializes under the same lock, so
 * index resolution and reads never race a concurrent upsert. The iterator keeps its own reference
 * to the window's {@link WindowRowStore}, so its rows stay readable after the engine discards or
 * resets the window.
 *
 * <p>{@code finished} defaults to {@code false} and is set on window close ({@link
 * #markFinished()}); {@link #isFinished()} reports {@code true} once the window is closed and all
 * recorded ranges have been generated. While no range is recorded and the window is still open,
 * {@link #isBlocked()} returns a future that the producer completes on the next {@link
 * #emitRange(long, long)} (or on {@link #markFinished()}), unblocking a consumer waiting for rows.
 */
public class WindowEventRowsIterator implements IEventRowsIterator {

  /** Sentinel {@code toExclusive} meaning "through the last row of the store". */
  static final long TO_END = Long.MAX_VALUE;

  private final WindowRowConverter converter;

  /** The window's row cache this iterator reads (and materializes) from. */
  private final WindowRowStore rowStore;

  /** The owning engine's lock; guards all iterator state and the materialization reads. */
  private final Object dataLock;

  /**
   * The owning engine, paused when the <b>output</b> pool cannot charge a materialized block: the
   * pressure propagates back (no further events are generated; pushes buffer into the input pool)
   * instead of growing the output demand.
   */
  private final WindowEngine engine;

  /** FIFO of ranges recorded at the engine's emission points, awaiting materialization. */
  private final ArrayDeque<Range> ranges = new ArrayDeque<>();

  // ===== generation progress cursor within the head range =====

  /**
   * Device (in the store's device-major order) up to which the head range has been generated.
   * Devices before it are fully decided; the cursor device itself resumes at the timestamp below,
   * devices after it are walked from their first row. Reset when a range is popped.
   */
  private int cursorDevice;

  /**
   * Last emitted timestamp within the cursor device - the resume identity. Per-device timestamps
   * are unique and indexed rows are never removed or re-keyed, so a seek to this timestamp lands on
   * exactly the last emitted row and one advance resumes past it; a row of the cursor device
   * arriving at or below it is skipped, matching the eagerly-materialized model (such late data is
   * corrected by the destroy-time recompute). Only meaningful while {@link #cursorMidDevice} is
   * set. An equal-timestamp run spans several devices in the device-major order, so this row-level
   * resume no longer skips the run's tail when a block boundary falls mid-run.
   */
  private long cursorTs = Long.MIN_VALUE;

  /**
   * Whether the cursor device is resumed mid-segment (a block cap cut it) - i.e. whether {@link
   * #cursorTs} is a valid resume identity. {@code false} when the cursor device starts fresh.
   */
  private boolean cursorMidDevice;

  /** Set to {@code true} on window close ({@link #markFinished}); no more ranges will be added. */
  private boolean finished;

  /**
   * Bytes charged to the global window <b>output</b> memory pool for the TsBlock returned by the
   * most recent {@link #next()} (one outstanding block, the compute module's queue contract). The
   * charge is returned when the consumer asks for the next block or closes this iterator.
   */
  private long reservedOutputBytes;

  /** Whether this iterator already dropped its owner reference to the row store. */
  private boolean storeReleased;

  /** Future returned by {@link #isBlocked()} while no range is recorded and the window is open. */
  private SettableFuture<Void> blocked = SettableFuture.create();

  public WindowEventRowsIterator(
      WindowRowConverter converter, WindowRowStore rowStore, Object dataLock, WindowEngine engine) {
    this.converter = converter;
    this.rowStore = rowStore;
    this.dataLock = dataLock;
    this.engine = engine;
    // Pin the store: it stays charged to the window data pool while this iterator can still
    // materialize from it, even after the engine discards the window.
    rowStore.retain();
  }

  /**
   * Records one emission batch - the rows whose timestamps lie in {@code (fromExclusive,
   * toExclusive)} - and unblocks any consumer waiting on {@link #isBlocked()}. Called by the engine
   * under {@code dataLock}. {@code toExclusive == Long.MAX_VALUE} means through the last row (used
   * by close / full-recompute emissions).
   */
  public void emitRange(long fromExclusive, long toExclusive) {
    synchronized (dataLock) {
      ranges.add(new Range(fromExclusive, toExclusive));
      unblock();
    }
  }

  /** Marks the window as closed (no more ranges will be recorded) and unblocks any consumer. */
  public void markFinished() {
    synchronized (dataLock) {
      finished = true;
      unblock();
    }
  }

  @Override
  public TsBlock next() {
    synchronized (dataLock) {
      // The block returned by the previous call is done from this iterator's perspective: hand its
      // output-pool charge back before producing the next block.
      releaseOutstandingOutput();
      Range range = ranges.peek();
      if (range == null) {
        return null;
      }
      // Walk the store's device-major order from the progress cursor, appending the head range's
      // rows (its timestamp predicate evaluated per device at delivery time) into one block.
      int deviceCount = rowStore.deviceCount();
      int device = Math.min(cursorDevice, deviceCount);
      int remaining = converter.maxWindowBlockRows();
      TsBlockBuilder builder = converter.newBlockBuilder();
      int appended = 0;
      int lastDevice = -1;
      long lastEmittedTs = Long.MIN_VALUE;
      boolean midDevice = false;
      while (device < deviceCount && remaining > 0) {
        WindowRowList list = rowStore.deviceList(device);
        WindowRowList.Cursor walk = list.newCursor();
        if (device == cursorDevice && cursorMidDevice) {
          // Resume strictly after the cursor row: the seek resolves to exactly that row (unique
          // timestamp, never removed) and one advance lands on its successor. The resumed row is
          // always above the range's lower bound (the cursor row was emitted for THIS range), so
          // no lower-bound clamp is needed here.
          if (!walk.seekAtOrAfter(cursorTs) || !walk.advance()) {
            device++;
            continue;
          }
        } else {
          // Fresh device (or the cursor device starting over): begin at its first row above the
          // range's lower bound.
          if (!walk.seekAfter(range.fromExclusive)) {
            device++;
            continue;
          }
        }
        while (walk.isValid()
            && remaining > 0
            && (range.toExclusive == TO_END || walk.timestamp() < range.toExclusive)) {
          converter.appendRow(builder, walk);
          appended++;
          remaining--;
          lastDevice = device;
          lastEmittedTs = walk.timestamp();
          walk.advance();
        }
        if (remaining == 0
            && walk.isValid()
            && (range.toExclusive == TO_END || walk.timestamp() < range.toExclusive)) {
          // The block cap cut this device's segment: the cursor stays here, resuming strictly
          // after the last emitted row (an equal-timestamp run spans devices in the device-major
          // order, so the run's tail in the later devices is delivered on the next blocks).
          midDevice = true;
          break;
        }
        device++;
      }
      if (appended == 0) {
        // This recorded batch has been fully generated: pop it and return null. The next call
        // starts the next recorded range (if any); the consumer meanwhile parks on isBlocked(),
        // which is NOT_BLOCKED while a range remains queued.
        ranges.poll();
        resetCursor();
        maybeReleaseStore();
        return null;
      }
      TsBlock tsBlock = builder.build();
      if (device >= deviceCount) {
        // The walk reached the last device's end: the range is fully generated.
        ranges.poll();
        resetCursor();
        maybeReleaseStore();
      } else if (midDevice) {
        // The block cap cut a device segment: resume strictly after its last emitted row.
        cursorDevice = lastDevice;
        cursorTs = lastEmittedTs;
        cursorMidDevice = true;
      } else {
        // The block cap hit exactly a device boundary: resume at the next un-walked device's first
        // in-range row (its lower bound is re-derived from the range predicate on the next walk).
        resetCursor();
        cursorDevice = device;
      }
      chargeOutput(tsBlock);
      StreamWindowEngineMetrics.getInstance().incMaterialized(tsBlock.getPositionCount());
      return tsBlock;
    }
  }

  private void resetCursor() {
    cursorDevice = 0;
    cursorTs = Long.MIN_VALUE;
    cursorMidDevice = false;
  }

  /**
   * Charges one materialized TsBlock to the window <b>output</b> memory pool. On pool exhaustion
   * the block is still returned uncharged (the consumer must not stall), but the owning engine is
   * memory-paused: event generation halts until a data/output release lifts the pause, so the
   * pressure propagates back to the input buffer instead of growing the output demand.
   */
  private void chargeOutput(TsBlock tsBlock) {
    long bytes = tsBlock.getRetainedSizeInBytes();
    if (bytes <= 0) {
      return;
    }
    if (GlobalWindowMemoryController.getInstance().tryReserveOutputMemory(bytes)) {
      reservedOutputBytes = bytes;
    } else {
      engine.pauseOnMemoryPressure(true);
    }
  }

  /** Returns the outstanding block's output-pool charge, if any. */
  private void releaseOutstandingOutput() {
    if (reservedOutputBytes > 0) {
      GlobalWindowMemoryController.getInstance().releaseOutputMemory(reservedOutputBytes);
      reservedOutputBytes = 0;
    }
  }

  /**
   * Drops this iterator's owner reference once it can no longer read the store (window closed and
   * all recorded ranges generated), so the store's data-pool charge can be returned by its last
   * owner even without an explicit {@link #close()}.
   */
  private void maybeReleaseStore() {
    if (!storeReleased && finished && ranges.isEmpty()) {
      storeReleased = true;
      rowStore.releaseOwner();
    }
  }

  @Override
  public ListenableFuture<?> isBlocked() {
    synchronized (dataLock) {
      if (!ranges.isEmpty() || finished) {
        return Operator.NOT_BLOCKED;
      }
      return blocked;
    }
  }

  @Override
  public boolean isFinished() {
    synchronized (dataLock) {
      return finished && ranges.isEmpty();
    }
  }

  @Override
  public void close() {
    synchronized (dataLock) {
      finished = true;
      ranges.clear();
      releaseOutstandingOutput();
      if (!storeReleased) {
        storeReleased = true;
        rowStore.releaseOwner();
      }
      unblock();
    }
  }

  private void unblock() {
    SettableFuture<Void> current = blocked;
    if (current != null && !current.isDone()) {
      current.set(null);
    }
    // A settled future cannot be reused; hand out a fresh one for the next empty-and-open wait.
    blocked = SettableFuture.create();
  }

  /** One emission batch recorded at an engine emission point. */
  private static final class Range {

    /** Rows with timestamps strictly greater than this bound are covered. */
    final long fromExclusive;

    /** Rows with timestamps strictly below this bound are covered; {@link #TO_END} = last row. */
    final long toExclusive;

    Range(long fromExclusive, long toExclusive) {
      this.fromExclusive = fromExclusive;
      this.toExclusive = toExclusive;
    }
  }
}
