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
import org.apache.iotdb.commons.stream.CapacityWindow;
import org.apache.iotdb.commons.stream.StreamProperties;
import org.apache.iotdb.commons.stream.StreamWindow;
import org.apache.iotdb.streamnode.engine.scheduler.task.StreamDriverContext;
import org.apache.iotdb.streamnode.engine.sink.CommitTracker;
import org.apache.iotdb.streamnode.engine.task.StreamSubTask.DataSlice;
import org.apache.iotdb.streamnode.utils.IEventRowsIterator;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.iotdb.streamnode.engine.window.WindowEngineTestUtils.createDriverContext;
import static org.apache.iotdb.streamnode.engine.window.WindowEngineTestUtils.flushDestroyMarkers;
import static org.apache.iotdb.streamnode.engine.window.WindowEngineTestUtils.printWindowData;

public class CapacityWindowEngineTest {

  private static final int CAPACITY = 3;

  private CapacityWindowEngine createEngine() {
    CapacityWindow window = new CapacityWindow(CAPACITY, null);
    window.setEventInputColumns(Collections.singletonList("col0"));
    // Value caching (these tests assert cached column values) only happens with ${rows}.
    window.setContainsRowsPlaceholder(true);
    return new CapacityWindowEngine(window, createDriverContext(), null, null);
  }

  private CapacityWindowEngine createEngine(int capacity) {
    CapacityWindow window = new CapacityWindow(capacity, null);
    window.setEventInputColumns(Collections.singletonList("col0"));
    // Value caching (these tests assert cached column values) only happens with ${rows}.
    window.setContainsRowsPlaceholder(true);
    return new CapacityWindowEngine(window, createDriverContext(), null, null);
  }

  private DataSlice createDataSlice(int rowCount) {
    return createDataSlice(rowCount, 0L);
  }

  /**
   * {@link #createDataSlice(int)} with an explicit first timestamp; use distinct starts across
   * pushes unless the test intentionally exercises duplicate-timestamp merging (a duplicate
   * timestamp merges into the stored row and does not fill the window).
   */
  private DataSlice createDataSlice(int rowCount, long firstTimestamp) {
    Tablet tablet =
        new Tablet(
            "root.test.device",
            Collections.singletonList(new MeasurementSchema("col0", TSDataType.INT64)),
            rowCount);
    for (int i = 0; i < rowCount; i++) {
      tablet.getTimestamps()[i] = firstTimestamp + i * 100L;
      tablet.addValue(i, "col0", (long) i);
    }
    tablet.setRowSize(rowCount);
    return new DataSlice(null, tablet, 0, rowCount, -1L);
  }

  private DataSlice createDataSliceWithValues(long[] values) {
    Tablet tablet =
        new Tablet(
            "root.test.device",
            Collections.singletonList(new MeasurementSchema("col0", TSDataType.INT64)),
            values.length);
    for (int i = 0; i < values.length; i++) {
      tablet.getTimestamps()[i] = i * 100L;
      tablet.addValue(i, "col0", values[i]);
    }
    tablet.setRowSize(values.length);
    return new DataSlice(null, tablet, 0, values.length, -1L);
  }

  /** Like {@link #createDataSliceWithValues(long[])} but with explicit timestamps (may repeat). */
  private DataSlice createDataSliceWithTimestampsAndValues(long[] timestamps, long[] values) {
    Tablet tablet =
        new Tablet(
            "root.test.device",
            Collections.singletonList(new MeasurementSchema("col0", TSDataType.INT64)),
            timestamps.length);
    for (int i = 0; i < timestamps.length; i++) {
      tablet.getTimestamps()[i] = timestamps[i];
      tablet.addValue(i, "col0", values[i]);
    }
    tablet.setRowSize(timestamps.length);
    return new DataSlice(null, tablet, 0, timestamps.length, -1L);
  }

  /**
   * Builds a one-row slice whose tablet carries the given schema order; a {@code null} value leaves
   * the column out of the tablet entirely (mirroring {@code INSERT INTO t(time, ...) ...}).
   */
  private DataSlice createTwoColumnSlice(
      boolean s2First, long timestamp, Long s1Value, Long s2Value, long tabletId) {
    List<IMeasurementSchema> schemas = new ArrayList<>(2);
    if (s2First) {
      if (s2Value != null) {
        schemas.add(new MeasurementSchema("s2", TSDataType.INT64));
      }
      if (s1Value != null) {
        schemas.add(new MeasurementSchema("s1", TSDataType.INT64));
      }
    } else {
      if (s1Value != null) {
        schemas.add(new MeasurementSchema("s1", TSDataType.INT64));
      }
      if (s2Value != null) {
        schemas.add(new MeasurementSchema("s2", TSDataType.INT64));
      }
    }
    Tablet tablet = new Tablet("root.test.device", schemas, 1);
    tablet.getTimestamps()[0] = timestamp;
    if (s1Value != null) {
      tablet.addValue(0, "s1", s1Value);
    }
    if (s2Value != null) {
      tablet.addValue(0, "s2", s2Value);
    }
    tablet.setRowSize(1);
    return new DataSlice(null, tablet, 0, 1, tabletId);
  }

  private CapacityWindowEngine createTwoColumnEngine(int capacity) {
    CapacityWindow window = new CapacityWindow(capacity, null);
    window.setEventInputColumns(Arrays.asList("s1", "s2"));
    // Value caching (these tests assert cached column values) only happens with ${rows}.
    window.setContainsRowsPlaceholder(true);
    return new CapacityWindowEngine(window, createDriverContext(), null, null);
  }

  /** Drains one event's rows iterator into its single materialized TsBlock. */
  private TsBlock readSingleBlock(IEventInfo event) {
    IEventRowsIterator iterator = event.getRowsIterator();
    TsBlock block = null;
    while (!iterator.isFinished()) {
      TsBlock next = iterator.next();
      if (next == null) {
        if (!iterator.isBlocked().isDone()) {
          break;
        }
        continue;
      }
      Assert.assertNull("expected a single TsBlock", block);
      block = next;
    }
    return block;
  }

  /**
   * Subscription tablets follow each INSERT's column order, so a later tablet may carry [s2, s1]
   * after the first carried [s1, s2]: the mapping must resolve per tablet instead of reusing the
   * first tablet's indexes (which would silently swap the values).
   */
  @Test
  public void testColumnMappingFollowsEachTabletSchema() {
    CapacityWindowEngine engine = createTwoColumnEngine(2);
    engine.push(Collections.singletonList(createTwoColumnSlice(false, 0L, 1L, 2L, 1L)));
    engine.push(Collections.singletonList(createTwoColumnSlice(true, 100L, 10L, 20L, 2L)));

    List<IEventInfo> events = getEventsFromQueue(engine);
    Assert.assertEquals(2, events.size());
    TsBlock block = readSingleBlock(events.get(0));
    Assert.assertNotNull(block);
    Assert.assertEquals(2, block.getPositionCount());
    Assert.assertEquals(1L, block.getColumn(0).getLong(0));
    Assert.assertEquals(2L, block.getColumn(1).getLong(0));
    Assert.assertEquals(10L, block.getColumn(0).getLong(1));
    Assert.assertEquals(20L, block.getColumn(1).getLong(1));
  }

  /**
   * The trigger table differs from the compute table and the query carries no {@code ${rows}}: the
   * calc plan then has no EventScanNode, so the window's eventInputColumns is null. The engine must
   * cache the pushed rows' time column only, still filling and emitting windows normally.
   */
  @Test
  public void testNullEventInputColumnsCachesTimeOnly() {
    CapacityWindow window = new CapacityWindow(CAPACITY, null);
    CapacityWindowEngine engine =
        new CapacityWindowEngine(window, createDriverContext(), null, null);

    engine.push(Collections.singletonList(createDataSlice(CAPACITY)));

    // The window fills and is closed, then destroyed right after it is closed.
    List<IEventInfo> events = getEventsFromQueue(engine);
    Assert.assertEquals(2, events.size());
    Assert.assertEquals(WindowEventType.WINDOW_CLOSE, events.get(0).getEventType());
    Assert.assertEquals(WindowEventType.WINDOW_DESTROY, events.get(1).getEventType());
    TsBlock block = readSingleBlock(events.get(0));
    Assert.assertNotNull(block);
    Assert.assertEquals(CAPACITY, block.getPositionCount());
    // Time-only cache: the single materialized column carries the store's true event timestamps.
    Assert.assertEquals(0L, block.getColumn(0).getLong(0));
    Assert.assertEquals(100L, block.getColumn(0).getLong(1));
    Assert.assertEquals(200L, block.getColumn(0).getLong(2));
  }

  /**
   * A later tablet may omit a scan column entirely ({@code INSERT INTO t(time, s2) ...}); its rows
   * upsert the missing column as null instead of crashing on a stale column index.
   */
  @Test
  public void testTabletMissingScanColumnUpsertsNull() {
    CapacityWindowEngine engine = createTwoColumnEngine(2);
    engine.push(Collections.singletonList(createTwoColumnSlice(false, 0L, 1L, 2L, 1L)));
    engine.push(Collections.singletonList(createTwoColumnSlice(true, 100L, null, 20L, 2L)));

    List<IEventInfo> events = getEventsFromQueue(engine);
    Assert.assertEquals(2, events.size());
    TsBlock block = readSingleBlock(events.get(0));
    Assert.assertNotNull(block);
    Assert.assertEquals(2, block.getPositionCount());
    Assert.assertEquals(1L, block.getColumn(0).getLong(0));
    Assert.assertEquals(2L, block.getColumn(1).getLong(0));
    Assert.assertTrue(block.getColumn(0).isNull(1));
    Assert.assertEquals(20L, block.getColumn(1).getLong(1));
  }

  /**
   * Alternating INSERTs each carrying one scan column ({@code INSERT INTO t(time, s1) ...} then
   * {@code INSERT INTO t(time, s2) ...}) never produce a tablet with the full scan column set, so
   * the output types cannot be derived from any single tablet. The stream-creation-resolved types
   * must make every such batch processable: no batch is dropped, and each row's missing column
   * upserts as null.
   */
  @Test
  public void testTabletsEachCarryingASubsetOfScanColumnsAreNotDropped() {
    CapacityWindow window = new CapacityWindow(2, null);
    window.setEventInputColumns(Arrays.asList("s1", "s2"));
    window.setEventInputColumnTypes(Arrays.asList(TSDataType.INT64, TSDataType.INT64));
    window.setContainsRowsPlaceholder(true);
    CapacityWindowEngine engine =
        new CapacityWindowEngine(window, createDriverContext(), null, null);

    engine.push(Collections.singletonList(createTwoColumnSlice(false, 0L, 1L, null, 1L)));
    engine.push(Collections.singletonList(createTwoColumnSlice(true, 100L, null, 2L, 2L)));
    engine.push(Collections.singletonList(createTwoColumnSlice(false, 200L, 3L, null, 3L)));
    engine.push(Collections.singletonList(createTwoColumnSlice(true, 300L, null, 4L, 4L)));

    // 4 rows at capacity 2 close 2 windows. getEventsFromQueue drains the compute queue first,
    // then the recompute queue: [close1, close2, marker1, marker2].
    List<IEventInfo> events = getEventsFromQueue(engine);
    Assert.assertEquals(4, events.size());
    Assert.assertEquals(WindowEventType.WINDOW_CLOSE, events.get(0).getEventType());
    Assert.assertEquals(WindowEventType.WINDOW_CLOSE, events.get(1).getEventType());
    Assert.assertEquals(WindowEventType.WINDOW_DESTROY, events.get(2).getEventType());
    Assert.assertEquals(WindowEventType.WINDOW_DESTROY, events.get(3).getEventType());
    TsBlock block = readSingleBlock(events.get(0));
    Assert.assertNotNull(block);
    Assert.assertEquals(2, block.getPositionCount());
    Assert.assertEquals(1L, block.getColumn(0).getLong(0));
    Assert.assertTrue(block.getColumn(1).isNull(0));
    Assert.assertTrue(block.getColumn(0).isNull(1));
    Assert.assertEquals(2L, block.getColumn(1).getLong(1));
  }

  private ListenableFuture<?> push(
      WindowEngine engine, DataSlice dataSlice, int startRow, int length, long tabletId) {
    ListenableFuture<?> future;
    if (dataSlice == null) {
      future = engine.push(null);
    } else {
      future =
          engine.push(
              Collections.singletonList(
                  new DataSlice(
                      dataSlice.getPartitionKey(),
                      dataSlice.getTablet(),
                      startRow,
                      startRow + length,
                      tabletId)));
    }
    // push() processes the batch inline on this (ingestion) thread; when processing is paused the
    // batch is only buffered and a later start() folds it in.
    return future;
  }

  /**
   * Drains the compute queue first, then the recompute queue, so for multiple windows the order is
   * {@code [close1, close2, ..., marker1, marker2, ...]}. Use {@link #awaitEvents} when the
   * per-window interleaved order {@code [close1, marker1, close2, marker2, ...]} is asserted.
   */
  private List<IEventInfo> getEventsFromQueue(CapacityWindowEngine engine) {
    List<IEventInfo> events = new ArrayList<>();
    while (engine.hasPendingComputeEvent()) {
      events.add(engine.pollComputeEvent());
    }
    while (engine.hasPendingRecomputeEvent()) {
      events.add(engine.pollRecomputeEvent());
    }
    return events;
  }

  /**
   * Waits for at least {@code minCount} events, polling one compute and one recompute event per
   * round (so a window's close event is always collected before its destroy marker), then drains
   * any remaining. Push processing is synchronous, so capacity-triggered events are already queued
   * by the time this is called; unlike {@link #getEventsFromQueue} this yields the per-window
   * interleaved order {@code [close1, marker1, close2, marker2, ...]}.
   */
  private List<IEventInfo> awaitEvents(CapacityWindowEngine engine, int minCount) {
    List<IEventInfo> events = new ArrayList<>();
    long deadline = System.currentTimeMillis() + 5000;
    try {
      while (System.currentTimeMillis() < deadline && events.size() < minCount) {
        IEventInfo event = engine.pollComputeEvent();
        if (event != null) {
          events.add(event);
        }
        IEventInfo reevent = engine.pollRecomputeEvent();
        if (reevent != null) {
          events.add(reevent);
        }
        if (events.size() < minCount) {
          Thread.sleep(5);
        }
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    while (engine.hasPendingComputeEvent()) {
      events.add(engine.pollComputeEvent());
    }
    while (engine.hasPendingRecomputeEvent()) {
      events.add(engine.pollRecomputeEvent());
    }
    return events;
  }

  /** Waits for the async processing thread to produce at least one event (without draining it). */
  private void awaitPendingEvent(CapacityWindowEngine engine) {
    long deadline = System.currentTimeMillis() + 5000;
    try {
      while (System.currentTimeMillis() < deadline && !engine.hasPendingComputeEvent()) {
        Thread.sleep(5);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  /**
   * Asserts that no event is produced within {@code waitMs}. Used to verify that {@code stop()}
   * paused processing: if processing were active, a capacity-reaching batch would produce an event
   * within milliseconds.
   */
  private void assertNoEventsShortly(CapacityWindowEngine engine, long waitMs) {
    long deadline = System.currentTimeMillis() + waitMs;
    try {
      while (System.currentTimeMillis() < deadline) {
        Assert.assertFalse(
            "processing should be paused, but an event appeared", engine.hasPendingComputeEvent());
        Thread.sleep(5);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    Assert.assertFalse(engine.hasPendingComputeEvent());
  }

  @Test
  public void testNullTsBlockReturnsNotBlocked() {
    CapacityWindowEngine engine = createEngine();
    ListenableFuture<?> future = push(engine, null, 0, 0, 1L);
    Assert.assertSame(Operator.NOT_BLOCKED, future);
    Assert.assertTrue(getEventsFromQueue(engine).isEmpty());
  }

  @Test
  public void testBelowCapacityNoTrigger() {
    CapacityWindowEngine engine = createEngine();
    DataSlice block = createDataSlice(2);
    ListenableFuture<?> future = push(engine, block, 0, 2, 1L);
    Assert.assertSame(Operator.NOT_BLOCKED, future);
    Assert.assertTrue(getEventsFromQueue(engine).isEmpty());
  }

  @Test
  public void testExactCapacityTriggersWindow() {
    CapacityWindowEngine engine = createEngine();
    DataSlice block = createDataSlice(3);
    ListenableFuture<?> future = push(engine, block, 0, 3, 1L);
    Assert.assertSame(Operator.NOT_BLOCKED, future);

    List<IEventInfo> events = awaitEvents(engine, 2);
    Assert.assertEquals(2, events.size());
    Assert.assertEquals(WindowEventType.WINDOW_CLOSE, events.get(0).getEventType());
    // the filled window is destroyed right after it is closed
    Assert.assertEquals(WindowEventType.WINDOW_DESTROY, events.get(1).getEventType());
    printWindowData(events);
  }

  @Test
  public void testExceedCapacityTriggersMultipleWindows() {
    CapacityWindowEngine engine = createEngine();
    DataSlice block = createDataSlice(7);
    ListenableFuture<?> future = push(engine, block, 0, 7, 1L);
    Assert.assertSame(Operator.NOT_BLOCKED, future);

    // Two filled windows, each emitting its close event followed by its destroy marker.
    List<IEventInfo> events = awaitEvents(engine, 4);
    Assert.assertEquals(4, events.size());
    Assert.assertEquals(WindowEventType.WINDOW_CLOSE, events.get(0).getEventType());
    Assert.assertEquals(WindowEventType.WINDOW_DESTROY, events.get(1).getEventType());
    Assert.assertEquals(WindowEventType.WINDOW_CLOSE, events.get(2).getEventType());
    Assert.assertEquals(WindowEventType.WINDOW_DESTROY, events.get(3).getEventType());
    printWindowData(events);
  }

  @Test
  public void testOptimizationFastPathExactCapacity() {
    CapacityWindowEngine engine = createEngine();
    DataSlice block1 = createDataSlice(1);
    ListenableFuture<?> future1 = push(engine, block1, 0, 1, 1L);
    Assert.assertSame(Operator.NOT_BLOCKED, future1);
    Assert.assertTrue(getEventsFromQueue(engine).isEmpty());

    DataSlice block2 = createDataSlice(2, 100L);
    ListenableFuture<?> future2 = push(engine, block2, 0, 2, 2L);
    Assert.assertSame(Operator.NOT_BLOCKED, future2);

    List<IEventInfo> events = awaitEvents(engine, 2);
    Assert.assertEquals(2, events.size());
    Assert.assertEquals(WindowEventType.WINDOW_CLOSE, events.get(0).getEventType());
    Assert.assertEquals(WindowEventType.WINDOW_DESTROY, events.get(1).getEventType());
    printWindowData(events);
  }

  @Test
  public void testOptimizationFastPathBelowCapacity() {
    CapacityWindowEngine engine = createEngine();
    DataSlice block1 = createDataSlice(1);
    push(engine, block1, 0, 1, 1L);

    DataSlice block2 = createDataSlice(1);
    ListenableFuture<?> future = push(engine, block2, 0, 1, 2L);
    Assert.assertSame(Operator.NOT_BLOCKED, future);
    Assert.assertTrue(getEventsFromQueue(engine).isEmpty());
  }

  @Test
  public void testAccumulativeRowsAcrossMultiplePush() {
    CapacityWindowEngine engine = createEngine();
    DataSlice block1 = createDataSlice(1);
    push(engine, block1, 0, 1, 1L);

    DataSlice block2 = createDataSlice(1, 100L);
    push(engine, block2, 0, 1, 2L);

    DataSlice block3 = createDataSlice(1, 200L);
    ListenableFuture<?> future = push(engine, block3, 0, 1, 3L);
    Assert.assertSame(Operator.NOT_BLOCKED, future);

    List<IEventInfo> events = awaitEvents(engine, 2);
    Assert.assertEquals(2, events.size());
    Assert.assertEquals(WindowEventType.WINDOW_CLOSE, events.get(0).getEventType());
    Assert.assertEquals(WindowEventType.WINDOW_DESTROY, events.get(1).getEventType());
    printWindowData(events);
  }

  @Test
  public void testForceCloseWithPendingData() {
    CapacityWindowEngine engine = createEngine();
    DataSlice block = createDataSlice(2);
    push(engine, block, 0, 2, 1L);

    ListenableFuture<?> future = engine.forceClose();
    Assert.assertSame(Operator.NOT_BLOCKED, future);

    List<IEventInfo> events = getEventsFromQueue(engine);
    Assert.assertEquals(2, events.size());
    Assert.assertEquals(WindowEventType.WINDOW_CLOSE, events.get(0).getEventType());
    Assert.assertEquals(WindowEventType.WINDOW_DESTROY, events.get(1).getEventType());
    printWindowData(events);
  }

  @Test
  public void testForceCloseWithNoData() {
    CapacityWindowEngine engine = createEngine();
    ListenableFuture<?> future = engine.forceClose();
    Assert.assertSame(Operator.NOT_BLOCKED, future);
    // No window ever received data: nothing to close or destroy, so no event is emitted.
    List<IEventInfo> events = getEventsFromQueue(engine);
    Assert.assertTrue(events.isEmpty());
  }

  @Test
  public void testForceCloseAfterWindowTrigger() {
    CapacityWindowEngine engine = createEngine();
    DataSlice block = createDataSlice(3);
    push(engine, block, 0, 3, 1L);

    List<IEventInfo> triggerEvents = awaitEvents(engine, 2);
    Assert.assertEquals(2, triggerEvents.size());

    ListenableFuture<?> future = engine.forceClose();
    Assert.assertSame(Operator.NOT_BLOCKED, future);
    // The triggered window was already closed and destroyed (its events drained above); with no
    // pending data forceClose emits nothing further.
    List<IEventInfo> forceCloseEvents = getEventsFromQueue(engine);
    Assert.assertTrue(forceCloseEvents.isEmpty());
  }

  /**
   * Slices carrying no tablet id ({@code -1}, e.g. from sources without commit ids) never engage
   * the commit tracker: the capacity window's destroy marker carries a real windowId, but since
   * {@code onWindowOpen} saw no tablet it was never registered - flushing the marker is a harmless
   * no-op and source commit never advances.
   */
  @Test
  public void testNegativeTabletIdNotTracked() {
    List<Long> committed = new ArrayList<>();
    CommitTracker tracker = new CommitTracker(committed::add);
    StreamDriverContext driverContext = createDriverContext();
    driverContext.getSubTaskContext().setCommitTracker(tracker);

    CapacityWindow window = new CapacityWindow(CAPACITY, null);
    window.setEventInputColumns(Collections.singletonList("col0"));
    // Value caching (these tests assert cached column values) only happens with ${rows}.
    window.setContainsRowsPlaceholder(true);
    CapacityWindowEngine engine = new CapacityWindowEngine(window, driverContext, null, null);

    tracker.registerCommitId(1L, 1);

    // All rows carry tabletId -1: the window still fills, closes and is destroyed normally...
    push(engine, createDataSlice(CAPACITY), 0, CAPACITY, -1L);
    IEventInfo closeEvent = engine.pollComputeEvent();
    Assert.assertNotNull(closeEvent);
    IEventInfo marker = engine.pollRecomputeEvent();
    Assert.assertNotNull(marker);
    // ...but the marker's windowId (a real one) was never registered, since onWindowOpen saw
    // tabletId -1: flushing it releases nothing and nothing ever fires.
    Assert.assertTrue(marker.getWindowId() >= 0);
    tracker.onWindowDestroy(marker.getWindowId());
    flushDestroyMarkers(engine, tracker);
    Assert.assertTrue("negative tabletIds must not engage the tracker", committed.isEmpty());
  }

  /**
   * The tumble engine's {@code onWindowOpen} contract applies here too: it must fire for every
   * tablet that contributes rows to a window, not only the tablet that opened it. A late, smaller
   * tablet feeding the in-progress window must lower the open allow ({@code minTabletId - 1}), or
   * source commit may pass the tablet while its rows still sit unflushed in the window.
   */
  @Test
  public void testLateSmallerTabletLowersOpenAllow() {
    List<Long> committed = new ArrayList<>();
    CommitTracker tracker = new CommitTracker(committed::add);
    StreamDriverContext driverContext = createDriverContext();
    driverContext.getSubTaskContext().setCommitTracker(tracker);

    CapacityWindow window = new CapacityWindow(3, null);
    window.setEventInputColumns(Collections.singletonList("col0"));
    // Value caching (these tests assert cached column values) only happens with ${rows}.
    window.setContainsRowsPlaceholder(true);
    CapacityWindowEngine engine = new CapacityWindowEngine(window, driverContext, null, null);

    tracker.registerCommitId(1L, 1);
    tracker.registerCommitId(2L, 1);
    tracker.registerCommitId(3L, 1);

    // Tablet 2 opens the window; tablet 1 arrives late (smaller id) and feeds the same window
    // (capacity 3 not yet reached).
    push(
        engine,
        createDataSliceWithTimestampsAndValues(new long[] {100L}, new long[] {10L}),
        0,
        1,
        2L);
    push(
        engine,
        createDataSliceWithTimestampsAndValues(new long[] {200L}, new long[] {10L}),
        0,
        1,
        1L);

    // Both tablets' scans are done, but their rows are still unflushed in the open window: with
    // tablet 1 registered the open allow is min(1, 2) - 1 = 0 and nothing may commit. Had the late
    // tablet not registered, the allow would stay at 1 and tablet 1 would be committed unflushed.
    Assert.assertTrue("late smaller tablet must gate the open allow", committed.isEmpty());

    // Tablet 3's row fills the window (its tablet registers too), the close event and destroy
    // marker are emitted; flushing the marker releases the window's whole tablet range.
    push(
        engine,
        createDataSliceWithTimestampsAndValues(new long[] {300L}, new long[] {10L}),
        0,
        1,
        3L);
    flushDestroyMarkers(engine, tracker);
    Assert.assertEquals(Collections.singletonList(3L), committed);
  }

  /**
   * A batch that fails mid-processing must still report group-done for its not-yet-marked tablets:
   * the batch's rows are dropped by design (exception protection), so from the commit tracker's
   * perspective those tablets' scans are finished; with their group-done missing, the tracker's
   * consecutive-completion watermark stalls before the failed tablet forever and source commit
   * never advances again.
   */
  @Test
  public void testFailedBatchStillMarksRemainingTabletsGroupDone() {
    List<Long> committed = new ArrayList<>();
    CommitTracker tracker = new CommitTracker(committed::add);
    StreamDriverContext driverContext = createDriverContext();
    driverContext.getSubTaskContext().setCommitTracker(tracker);

    CapacityWindow window = new CapacityWindow(1, null);
    window.setEventInputColumns(Collections.singletonList("col0"));
    // Value caching (these tests assert cached column values) only happens with ${rows}.
    window.setContainsRowsPlaceholder(true);
    CapacityWindowEngine engine = new CapacityWindowEngine(window, driverContext, null, null);

    tracker.registerCommitId(1L, 1);
    tracker.registerCommitId(2L, 1);
    tracker.registerCommitId(3L, 1);

    // One batch, two slices: tablet 1's INT64 "col0" row fills the capacity-1 window (its
    // group-done fires at the end of its slice), but tablet 2's "col0" column is TEXT - a type
    // conflicting with the output types the first tablet established, so the row converter fails
    // on its first row (ClassCastException in readValue) and the rest of the batch is dropped.
    Assert.assertSame(
        Operator.NOT_BLOCKED,
        engine.push(
            Arrays.asList(
                new DataSlice(
                    null,
                    createDataSliceWithTimestampsAndValues(new long[] {100L}, new long[] {1L})
                        .getTablet(),
                    0,
                    1,
                    1L),
                textCol0Slice(200L, "boom", 2L))));

    // Tablet 3 (INT64 again) fills its own window.
    Assert.assertSame(
        Operator.NOT_BLOCKED,
        push(
            engine,
            createDataSliceWithTimestampsAndValues(new long[] {300L}, new long[] {3L}),
            0,
            1,
            3L));
    Assert.assertTrue(engine.forceClose().isDone());
    flushDestroyMarkers(engine, tracker);

    // Commit must advance past the failed tablet 2 to 3: [2 (window w0 flushed: its tablet-1
    // allow releases, and the still-gating open window w1 cascades the watermark to tablet 3 - 1),
    // 3 (window w1 flushed: tablet 3)]. Committing 2 covers the failed tablet 2's prefix; had its
    // group-done been lost with the dropped batch, the tablet prefix would pin commit at 1 forever.
    Assert.assertEquals(Arrays.asList(2L, 3L), committed);
  }

  /**
   * One one-row slice whose "col0" column is {@link TSDataType#TEXT} - conflicting with the INT64
   * "col0" established by an earlier tablet of the same engine.
   */
  private DataSlice textCol0Slice(long timestamp, String value, long tabletId) {
    Tablet tablet =
        new Tablet(
            "root.test.device",
            Collections.singletonList(new MeasurementSchema("col0", TSDataType.TEXT)),
            1);
    tablet.getTimestamps()[0] = timestamp;
    tablet.addValue(0, "col0", value);
    tablet.setRowSize(1);
    return new DataSlice(null, tablet, 0, 1, tabletId);
  }

  @Test
  public void testEventQueueOperations() {
    CapacityWindowEngine engine = createEngine();
    DataSlice block = createDataSlice(3);
    push(engine, block, 0, 3, 1L);

    awaitPendingEvent(engine);
    Assert.assertTrue(engine.hasPendingComputeEvent());
    Assert.assertNotNull(engine.peekComputeEvent());
    Assert.assertNotNull(engine.pollComputeEvent());
    Assert.assertTrue(engine.hasPendingRecomputeEvent());
    Assert.assertNotNull(engine.pollRecomputeEvent());
    Assert.assertFalse(engine.hasPendingRecomputeEvent());
  }

  @Test
  public void testRowsIteratorInEvent() {
    CapacityWindowEngine engine = createEngine();
    DataSlice block = createDataSliceWithValues(new long[] {10L, 20L, 30L});
    ListenableFuture<?> future = push(engine, block, 0, 3, 1L);
    Assert.assertSame(Operator.NOT_BLOCKED, future);

    List<IEventInfo> events = awaitEvents(engine, 2);
    Assert.assertEquals(2, events.size());
    IEventInfo closeEvent = events.get(0);
    Assert.assertNotNull(closeEvent.getRowsIterator());
    Assert.assertFalse(closeEvent.getRowsIterator().isFinished());
    // the destroy event carries no rows
    Assert.assertNull(events.get(1).getRowsIterator());
    printWindowData(events);
  }

  @Test
  public void testDuplicateTimestampsMergeIntoOneCountedRow() {
    // Duplicate timestamps merge by column in the row store; the window must fill per MERGED row:
    // the close event's row count matches the merged store (3 rows), not the pushed row count (4).
    // The non-null duplicate value wins the merge (col0=99 overwrites col0=10 at ts=100).
    CapacityWindowEngine engine = createEngine(3);
    DataSlice block =
        createDataSliceWithTimestampsAndValues(
            new long[] {100L, 100L, 200L, 300L}, new long[] {10L, 99L, 20L, 30L});
    push(engine, block, 0, 4, 1L);

    List<IEventInfo> events = awaitEvents(engine, 2);
    Assert.assertEquals(2, events.size());
    IEventInfo closeEvent = events.get(0);
    Assert.assertTrue(WindowEventType.WINDOW_CLOSE.equals(closeEvent.getEventType()));
    Assert.assertEquals(3L, closeEvent.getRowCount().getAsLong());
    TsBlock tsBlock = readSingleBlock(closeEvent);
    Assert.assertNotNull(tsBlock);
    Assert.assertEquals(3, tsBlock.getPositionCount());
    Assert.assertEquals(99L, tsBlock.getColumn(0).getLong(0));
    Assert.assertEquals(20L, tsBlock.getColumn(0).getLong(1));
    Assert.assertEquals(30L, tsBlock.getColumn(0).getLong(2));
  }

  @Test
  public void testGetBlockedFuture() {
    CapacityWindowEngine engine = createEngine();
    Assert.assertFalse(engine.getBlockedFuture().isDone());

    DataSlice block = createDataSlice(1);
    push(engine, block, 0, 1, 1L);
    Assert.assertFalse(engine.getBlockedFuture().isDone());
  }

  @Test
  public void testWindowResetAfterTrigger() {
    CapacityWindowEngine engine = createEngine();
    DataSlice block1 = createDataSlice(3);
    push(engine, block1, 0, 3, 1L);
    List<IEventInfo> events1 = awaitEvents(engine, 2);
    Assert.assertEquals(2, events1.size());
    printWindowData(events1);

    DataSlice block2 = createDataSlice(3);
    ListenableFuture<?> future = push(engine, block2, 0, 3, 2L);
    Assert.assertSame(Operator.NOT_BLOCKED, future);

    List<IEventInfo> events2 = awaitEvents(engine, 1);
    Assert.assertEquals(2, events2.size());
    printWindowData(events2);
  }

  @Test
  public void testStopPausesProcessingWhilePushStillReceives() {
    CapacityWindowEngine engine = createEngine();
    engine.stop();

    // push() must still accept data (reception is unaffected by stop), even though the batch
    // reaches the window capacity and would normally trigger an event.
    DataSlice block = createDataSlice(CAPACITY);
    ListenableFuture<?> future = push(engine, block, 0, CAPACITY, 1L);
    Assert.assertSame(Operator.NOT_BLOCKED, future);
    // Processing is paused: the capacity-reaching batch produces no event.
    assertNoEventsShortly(engine, 150);

    // start() resumes processing; the queued batch is now processed and emits its event.
    engine.start();
    List<IEventInfo> events = awaitEvents(engine, 2);
    Assert.assertEquals(2, events.size());
  }

  @Test
  public void testStartIsIdempotent() {
    CapacityWindowEngine engine = createEngine();
    // The constructor already called start(); calling it again must be harmless.
    engine.start();
    engine.start();

    DataSlice block = createDataSlice(CAPACITY);
    push(engine, block, 0, CAPACITY, 1L);
    List<IEventInfo> events = awaitEvents(engine, 2);
    Assert.assertEquals(2, events.size());
  }

  @Test
  public void testDropStopsProcessing() {
    CapacityWindowEngine engine = createEngine();
    engine.drop();

    // After drop processing is disabled; pushed data is received but never processed.
    DataSlice block = createDataSlice(CAPACITY);
    ListenableFuture<?> future = push(engine, block, 0, CAPACITY, 1L);
    Assert.assertSame(Operator.NOT_BLOCKED, future);
    assertNoEventsShortly(engine, 150);
  }

  @Test
  public void testStartAfterDropIsNoOp() {
    CapacityWindowEngine engine = createEngine();
    engine.drop();
    // start() after drop() must NOT restart the processing thread (drop is terminal).
    engine.start();

    DataSlice block = createDataSlice(CAPACITY);
    push(engine, block, 0, CAPACITY, 1L);
    assertNoEventsShortly(engine, 150);
  }

  @Test
  public void testPrevAndNextTimeAlwaysEmpty() {
    // Capacity (count-based) windows carry no prev_time/next_time: those placeholders are not
    // applicable, so they must resolve to NULL rather than a sentinel like -1.
    CapacityWindowEngine engine = createEngine();
    DataSlice block = createDataSlice(CAPACITY);
    push(engine, block, 0, CAPACITY, 1L);
    List<IEventInfo> events = awaitEvents(engine, 2);
    Assert.assertEquals(2, events.size());
    IEventInfo event = events.get(0);
    Assert.assertFalse(
        "prev_time should be NULL for capacity windows", event.getPrevTime().isPresent());
    Assert.assertFalse(
        "next_time should be NULL for capacity windows", event.getNextTime().isPresent());
  }

  /**
   * A CapacityWindowEngine whose system clock is test-controlled, for MAX_DELAY_WINDOW_CLOSE tests:
   * without it the deadline would depend on wall-clock sleeps.
   */
  private static final class ControllableTimeEngine extends CapacityWindowEngine {
    private volatile long currentMillis;

    ControllableTimeEngine(
        StreamWindow window, StreamProperties streamProperties, long startMillis) {
      super(window, createDriverContext(), streamProperties, null);
      this.currentMillis = startMillis;
    }

    @Override
    protected long nowMillis() {
      return currentMillis;
    }

    void advanceTo(long millis) {
      this.currentMillis = millis;
    }
  }

  private ControllableTimeEngine createMaxDelayEngine(
      long maxDelayWindowCloseMs, long startMillis) {
    CapacityWindow window = new CapacityWindow(CAPACITY, null);
    window.setEventInputColumns(Collections.singletonList("col0"));
    // Value caching (these tests assert cached column values) only happens with ${rows}.
    window.setContainsRowsPlaceholder(true);
    StreamProperties streamProperties = new StreamProperties(0, 0, maxDelayWindowCloseMs, 0, true);
    return new ControllableTimeEngine(window, streamProperties, startMillis);
  }

  /** MAX_DELAY_WINDOW_CLOSE: a partial window that stops receiving rows is force-closed by tick. */
  @Test
  public void testMaxDelayForceClosesPartialWindow() {
    long t0 = 1_000_000L;
    ControllableTimeEngine engine = createMaxDelayEngine(100, t0);

    // One row below capacity: the window opens with closeSystemTime = t0 + 100.
    push(engine, createDataSlice(1), 0, 1, 1L);
    Assert.assertTrue(getEventsFromQueue(engine).isEmpty());

    // Not yet at the deadline: the tick must not close the partial window.
    engine.advanceTo(t0 + 99);
    engine.tick();
    Assert.assertTrue(getEventsFromQueue(engine).isEmpty());

    // Past the deadline: the partial window is force-closed - close event with its 1 row,
    // followed by the destroy marker.
    engine.advanceTo(t0 + 100);
    engine.tick();
    List<IEventInfo> events = getEventsFromQueue(engine);
    Assert.assertEquals(2, events.size());
    Assert.assertEquals(WindowEventType.WINDOW_CLOSE, events.get(0).getEventType());
    Assert.assertEquals(WindowEventType.WINDOW_DESTROY, events.get(1).getEventType());
    Assert.assertEquals(1L, events.get(0).getRowCount().getAsLong());
    Assert.assertEquals(events.get(0).getWindowId(), events.get(1).getWindowId());
    printWindowData(events);
  }

  /**
   * With MAX_DELAY_WINDOW_CLOSE at its default 0, a data-pool memory pause has no configured
   * recovery (onTick is a no-op) and the partially-filled current window would hold its data-pool
   * charge forever - the task stalls with only a WARN. Past the rescue grace period the engine must
   * force-close it through the regular close path, exactly like a MAX_DELAY force-close.
   */
  @Test
  public void testDataPoolExhaustionRescuesWithDefaultControlOptions() throws Exception {
    long t0 = 1_000_000L;
    ControllableTimeEngine engine = createMaxDelayEngine(0, t0);
    GlobalWindowMemoryController controller = GlobalWindowMemoryController.getInstance();

    // Partial window (1 row below capacity), charged to the data pool.
    push(engine, createDataSlice(1), 0, 1, 1L);
    Assert.assertTrue(getEventsFromQueue(engine).isEmpty());

    // Exhaust the data pool, then push one more batch: the engine memory-pauses and buffers it.
    long held = controller.getDataTotalBytes() - controller.getDataUsedBytes();
    Assert.assertTrue(controller.tryReserveDataMemory(held));
    try {
      push(engine, createDataSlice(1, 50L), 0, 1, 2L);
      Assert.assertTrue(engine.isMemoryPaused());

      // Within the grace period: no premature close (a concurrent release may still lift the
      // pause without any data loss).
      engine.advanceTo(t0 + 1000);
      engine.tick();
      Assert.assertTrue(engine.isMemoryPaused());
      Assert.assertTrue(getEventsFromQueue(engine).isEmpty());

      // Beyond the grace period: the partial window is force-closed (close event + destroy
      // marker) and its data-pool charge returned.
      engine.advanceTo(t0 + 1000 + WindowEngine.DATA_POOL_PAUSE_RESCUE_GRACE_MS + 1);
      engine.tick();
      List<IEventInfo> events = getEventsFromQueue(engine);
      Assert.assertEquals(2, events.size());
      Assert.assertEquals(WindowEventType.WINDOW_CLOSE, events.get(0).getEventType());
      Assert.assertEquals(WindowEventType.WINDOW_DESTROY, events.get(1).getEventType());

      // The rescue returned the window's memory; the released pool lets the (asynchronous)
      // resume drain the buffered batch and leave the pause.
      Assert.assertTrue(controller.releaseDataMemory(held));
      long deadline = System.currentTimeMillis() + 5000;
      while (engine.isMemoryPaused() && System.currentTimeMillis() < deadline) {
        Thread.sleep(5);
      }
      Assert.assertFalse(
          "the engine should resume after its self-rescue returned the window's memory",
          engine.isMemoryPaused());
    } finally {
      controller.releaseDataMemory(held);
      engine.drop();
    }
  }

  /**
   * The deadline is armed once at window open and is a hard cap: rows arriving later do not refresh
   * it, so the window still force-closes at open+maxDelay with the rows received so far.
   */
  @Test
  public void testMaxDelayDeadlineNotRefreshedByLaterRows() {
    long t0 = 1_000_000L;
    ControllableTimeEngine engine = createMaxDelayEngine(100, t0);

    push(engine, createDataSlice(1), 0, 1, 1L);
    // A second row arrives at t0+50, still below capacity; the deadline stays t0+100.
    engine.advanceTo(t0 + 50);
    push(engine, createDataSlice(1, 100L), 0, 1, 2L);
    Assert.assertTrue(getEventsFromQueue(engine).isEmpty());

    engine.advanceTo(t0 + 100);
    engine.tick();
    List<IEventInfo> events = getEventsFromQueue(engine);
    Assert.assertEquals(2, events.size());
    Assert.assertEquals(WindowEventType.WINDOW_CLOSE, events.get(0).getEventType());
    Assert.assertEquals(WindowEventType.WINDOW_DESTROY, events.get(1).getEventType());
    Assert.assertEquals(2L, events.get(0).getRowCount().getAsLong());
  }

  /** After a force-close the next window arms a fresh deadline at its own open. */
  @Test
  public void testMaxDelayCloseArmsFreshDeadlineForNextWindow() {
    long t0 = 1_000_000L;
    ControllableTimeEngine engine = createMaxDelayEngine(100, t0);

    // First window: 1 row, force-closed at t0+100.
    push(engine, createDataSlice(1), 0, 1, 1L);
    engine.advanceTo(t0 + 100);
    engine.tick();
    List<IEventInfo> firstEvents = getEventsFromQueue(engine);
    Assert.assertEquals(2, firstEvents.size());
    long firstWindowId = firstEvents.get(0).getWindowId();

    // Second window opens at t0+150: its deadline is t0+250, not the previous window's.
    engine.advanceTo(t0 + 150);
    push(engine, createDataSlice(1), 0, 1, 2L);
    engine.advanceTo(t0 + 200);
    engine.tick();
    Assert.assertTrue(
        "new window must keep its own fresh deadline", getEventsFromQueue(engine).isEmpty());

    engine.advanceTo(t0 + 250);
    engine.tick();
    List<IEventInfo> secondEvents = getEventsFromQueue(engine);
    Assert.assertEquals(2, secondEvents.size());
    Assert.assertEquals(WindowEventType.WINDOW_CLOSE, secondEvents.get(0).getEventType());
    Assert.assertEquals(firstWindowId + 1, secondEvents.get(0).getWindowId());
  }

  /**
   * {@code maxDelayWindowCloseMs = 0} disables the system-time force-close: a partial window
   * survives ticks arbitrarily far in the future and only closes when it fills or on forceClose.
   */
  @Test
  public void testMaxDelayDisabledNoTickClose() {
    long t0 = 1_000_000L;
    ControllableTimeEngine engine = createMaxDelayEngine(0, t0);

    push(engine, createDataSlice(CAPACITY - 1), 0, CAPACITY - 1, 1L);
    engine.advanceTo(t0 + Long.MAX_VALUE / 2);
    engine.tick();
    engine.tick();
    Assert.assertTrue(getEventsFromQueue(engine).isEmpty());

    // The window still closes normally when it fills.
    push(engine, createDataSlice(1, (CAPACITY - 1) * 100L), 0, 1, 2L);
    List<IEventInfo> events = getEventsFromQueue(engine);
    Assert.assertEquals(2, events.size());
    Assert.assertEquals(WindowEventType.WINDOW_CLOSE, events.get(0).getEventType());
  }

  @Test
  public void testLargeWindowGeneratesMultipleTsBlocks() {
    // A filled window of more than MAX_WINDOW_BLOCK_ROWS rows is materialized lazily by next()
    // in successive <=cap blocks (progress recorded between calls); after the last block next()
    // returns null and isFinished() turns true.
    int blockCap = WindowEngine.MAX_WINDOW_BLOCK_ROWS;
    int rowCount = blockCap + 100;
    CapacityWindowEngine engine = createEngine(rowCount);
    push(engine, createDataSlice(rowCount), 0, rowCount, 1L);

    List<IEventInfo> events = getEventsFromQueue(engine);
    Assert.assertEquals(2, events.size());
    IEventInfo event = events.get(0);
    Assert.assertFalse(event.getRowsIterator().isFinished());
    Assert.assertEquals(rowCount, event.getRowCount().getAsLong());

    IEventRowsIterator it = event.getRowsIterator();
    Assert.assertFalse(it.isFinished());
    TsBlock block1 = it.next();
    Assert.assertEquals(blockCap, block1.getPositionCount());
    Assert.assertEquals(0L, block1.getColumn(0).getLong(0));
    Assert.assertEquals((long) (blockCap - 1), block1.getColumn(0).getLong(blockCap - 1));

    TsBlock block2 = it.next();
    Assert.assertEquals(rowCount - blockCap, block2.getPositionCount());
    Assert.assertEquals((long) blockCap, block2.getColumn(0).getLong(0));
    Assert.assertEquals(
        (long) (rowCount - 1), block2.getColumn(0).getLong(block2.getPositionCount() - 1));

    Assert.assertNull(it.next());
    Assert.assertTrue(it.isFinished());
  }
}
