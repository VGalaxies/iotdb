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
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.stream.StreamLiteral;
import org.apache.iotdb.commons.stream.StreamProperties;
import org.apache.iotdb.commons.stream.StreamWindow;
import org.apache.iotdb.commons.stream.TumbleWindow;
import org.apache.iotdb.streamnode.engine.scheduler.task.StreamDriverContext;
import org.apache.iotdb.streamnode.engine.sink.CommitTracker;
import org.apache.iotdb.streamnode.engine.task.StreamSubTask.DataSlice;
import org.apache.iotdb.streamnode.i18n.StreamNodeMessages;
import org.apache.iotdb.streamnode.utils.IEventRowsIterator;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.tsfile.common.constant.TsFileConstant;
import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.apache.iotdb.streamnode.engine.window.WindowEngineTestUtils.createDriverContext;
import static org.apache.iotdb.streamnode.engine.window.WindowEngineTestUtils.flushDestroyMarkers;
import static org.apache.iotdb.streamnode.engine.window.WindowEngineTestUtils.printWindowData;

public class TumbleWindowEngineTest {

  private static final long WINDOW_SIZE_MS = 1000;
  private static final long ORIGIN_MS = 0;

  private TumbleWindowEngine createEngine() {
    TumbleWindow window = new TumbleWindow(WINDOW_SIZE_MS, ORIGIN_MS);
    window.setEventInputColumns(Collections.singletonList("time"));
    window.setContainsRowsPlaceholder(true);
    return new TumbleWindowEngine(window, createDriverContext(), null, null);
  }

  private TumbleWindowEngine createEngine(long windowSizeMs, long originMs) {
    TumbleWindow window = new TumbleWindow(windowSizeMs, originMs);
    window.setEventInputColumns(Collections.singletonList("time"));
    window.setContainsRowsPlaceholder(true);
    return new TumbleWindowEngine(window, createDriverContext(), null, null);
  }

  private DataSlice createDataSlice(long[] timestamps, long[] values) {
    Tablet tablet =
        new Tablet(
            "root.test.device",
            Arrays.asList(
                new MeasurementSchema("time", TSDataType.INT64),
                new MeasurementSchema("value", TSDataType.INT64)),
            timestamps.length);
    for (int i = 0; i < timestamps.length; i++) {
      tablet.getTimestamps()[i] = timestamps[i];
      tablet.addValue(i, "time", timestamps[i]);
      tablet.addValue(i, "value", values[i]);
    }
    tablet.setRowSize(timestamps.length);
    return new DataSlice(null, tablet, 0, timestamps.length, -1L);
  }

  private DataSlice createDataSlice(long[] timestamps) {
    Tablet tablet =
        new Tablet(
            "root.test.device",
            Collections.singletonList(new MeasurementSchema("time", TSDataType.INT64)),
            timestamps.length);
    for (int i = 0; i < timestamps.length; i++) {
      tablet.getTimestamps()[i] = timestamps[i];
      tablet.addValue(i, "time", timestamps[i]);
    }
    tablet.setRowSize(timestamps.length);
    return new DataSlice(null, tablet, 0, timestamps.length, -1L);
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
   * {@code [close1, close2, ..., marker1, marker2, ...]}: a window's close event always precedes
   * its marker, but markers of earlier windows may follow close events of later ones. Current tests
   * only assert single-window scenarios, where this equals the chronological order.
   */
  private List<IEventInfo> getEventsFromQueue(TumbleWindowEngine engine) {
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
   * Waits for at least {@code minCount} events, draining them as they appear. Push processing is
   * synchronous, so most events are already queued; use this in place of {@link
   * #getEventsFromQueue} when events are produced asynchronously instead (by a tick or by a resumed
   * drain). Each round drains all compute events first, then all recompute events - the same
   * cross-queue ordering caveat as {@link #getEventsFromQueue} applies.
   */
  private List<IEventInfo> awaitEvents(TumbleWindowEngine engine, int minCount) {
    List<IEventInfo> events = new ArrayList<>();
    long deadline = System.currentTimeMillis() + 5000;
    try {
      while (System.currentTimeMillis() < deadline && events.size() < minCount) {
        while (engine.hasPendingComputeEvent()) {
          events.add(engine.pollComputeEvent());
        }
        while (engine.hasPendingRecomputeEvent()) {
          events.add(engine.pollRecomputeEvent());
        }

        if (events.size() < minCount) {
          Thread.sleep(5);
        }
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    return events;
  }

  private List<IEventInfo> getRecomputeEventsFromQueue(TumbleWindowEngine engine) {
    List<IEventInfo> events = new ArrayList<>();
    while (engine.hasPendingRecomputeEvent()) {
      events.add(engine.pollRecomputeEvent());
    }
    return events;
  }

  /**
   * Waits for at least {@code minCount} recompute events, draining them from the recompute queue as
   * they appear.
   */
  private List<IEventInfo> awaitRecomputeEvents(TumbleWindowEngine engine, int minCount) {
    List<IEventInfo> events = new ArrayList<>();
    long deadline = System.currentTimeMillis() + 5000;
    try {
      while (System.currentTimeMillis() < deadline && events.size() < minCount) {
        IEventInfo event = engine.pollRecomputeEvent();
        if (event != null) {
          events.add(event);
        } else {
          Thread.sleep(5);
        }
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    while (engine.hasPendingRecomputeEvent()) {
      events.add(engine.pollRecomputeEvent());
    }
    return events;
  }

  /** Waits for at least one event to be queued (without draining it). */
  private void awaitPendingEvent(TumbleWindowEngine engine) {
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
   * paused processing: if processing were active, a boundary-crossing batch would produce an event
   * within milliseconds.
   */
  private void assertNoEventsShortly(TumbleWindowEngine engine, long waitMs) {
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
    TumbleWindowEngine engine = createEngine();
    ListenableFuture<?> future = push(engine, null, 0, 0, 1L);
    Assert.assertSame(Operator.NOT_BLOCKED, future);
    Assert.assertTrue(getEventsFromQueue(engine).isEmpty());
  }

  @Test
  public void testSingleRowWithinWindow() {
    TumbleWindowEngine engine = createEngine();
    DataSlice block = createDataSlice(new long[] {100L});
    ListenableFuture<?> future = push(engine, block, 0, 1, 1L);
    Assert.assertSame(Operator.NOT_BLOCKED, future);
    Assert.assertTrue(getEventsFromQueue(engine).isEmpty());
    Assert.assertFalse(engine.hasPendingComputeEvent());
  }

  @Test
  public void testAllRowsInSameWindow() {
    TumbleWindowEngine engine = createEngine();
    long[] timestamps = {100L, 200L, 300L, 400L, 500L};
    DataSlice block = createDataSlice(timestamps);
    ListenableFuture<?> future = push(engine, block, 0, timestamps.length, 1L);
    Assert.assertSame(Operator.NOT_BLOCKED, future);
    Assert.assertTrue(getEventsFromQueue(engine).isEmpty());
  }

  @Test
  public void testRowCrossesWindowBoundary() {
    TumbleWindowEngine engine = createEngine();
    DataSlice block1 = createDataSlice(new long[] {100L, 500L});
    ListenableFuture<?> future1 = push(engine, block1, 0, 2, 1L);
    Assert.assertSame(Operator.NOT_BLOCKED, future1);
    Assert.assertTrue(getEventsFromQueue(engine).isEmpty());
    printWindowData(getEventsFromQueue(engine));

    DataSlice block2 = createDataSlice(new long[] {1001L});
    ListenableFuture<?> future2 = push(engine, block2, 0, 1, 2L);
    Assert.assertSame(Operator.NOT_BLOCKED, future2);

    List<IEventInfo> events = awaitEvents(engine, 2);
    Assert.assertEquals(2, events.size());

    IEventInfo closeEvent = events.get(0);
    Assert.assertEquals(WindowEventType.WINDOW_CLOSE, closeEvent.getEventType());
    Assert.assertEquals(0L, closeEvent.getStartTime().getAsLong());
    Assert.assertEquals(1000L, closeEvent.getEndTime().getAsLong());
    // window is destroyed right after it is closed
    Assert.assertEquals(WindowEventType.WINDOW_DESTROY, events.get(1).getEventType());
    printWindowData(events);
  }

  /**
   * The trigger table differs from the compute table and the query carries no {@code ${rows}}: the
   * calc plan then has no EventScanNode, so the window's eventInputColumns is null. The engine must
   * cache the pushed rows' time column only, still advancing and closing windows normally.
   */
  @Test
  public void testNullEventInputColumnsCachesTimeOnly() {
    TumbleWindow window = new TumbleWindow(WINDOW_SIZE_MS, ORIGIN_MS);
    TumbleWindowEngine engine = new TumbleWindowEngine(window, createDriverContext(), null, null);

    DataSlice block1 = createDataSlice(new long[] {100L, 200L});
    push(engine, block1, 0, 2, 1L);
    DataSlice block2 = createDataSlice(new long[] {1001L});
    push(engine, block2, 0, 1, 2L);

    List<IEventInfo> events = awaitEvents(engine, 2);
    Assert.assertEquals(2, events.size());
    Assert.assertEquals(WindowEventType.WINDOW_CLOSE, events.get(0).getEventType());
    Assert.assertEquals(0L, events.get(0).getStartTime().getAsLong());
    Assert.assertEquals(1000L, events.get(0).getEndTime().getAsLong());

    // Time-only cache: the closed window's rows materialize as the true event timestamps.
    List<Long> timestamps = new ArrayList<>();
    IEventRowsIterator iterator = events.get(0).getRowsIterator();
    while (iterator != null && !iterator.isFinished()) {
      TsBlock block = iterator.next();
      if (block == null) {
        if (!iterator.isBlocked().isDone()) {
          break;
        }
        continue;
      }
      for (int i = 0; i < block.getPositionCount(); i++) {
        timestamps.add(block.getColumn(0).getLong(i));
      }
    }
    Assert.assertEquals(Arrays.asList(100L, 200L), timestamps);
  }

  /** One-row slice whose tablet carries only {@code time} and the given measurement column. */
  private DataSlice createSingleColumnSlice(String column, long timestamp, long value) {
    Tablet tablet =
        new Tablet(
            "root.test.device",
            Collections.singletonList(new MeasurementSchema(column, TSDataType.INT64)),
            1);
    tablet.getTimestamps()[0] = timestamp;
    tablet.addValue(0, column, value);
    tablet.setRowSize(1);
    return new DataSlice(null, tablet, 0, 1, -1L);
  }

  /**
   * Alternating INSERTs each carrying one scan column ({@code INSERT INTO t(time, s1) ...} then
   * {@code INSERT INTO t(time, s2) ...}) never produce a tablet with the full scan column set, so
   * the output types cannot be derived from any single tablet. The stream-creation-resolved types
   * keep every such batch processable: no batch is dropped, and each row's missing column upserts
   * as null.
   */
  @Test
  public void testTabletsEachCarryingASubsetOfScanColumnsAreNotDropped() {
    TumbleWindow window = new TumbleWindow(WINDOW_SIZE_MS, ORIGIN_MS);
    window.setEventInputColumns(Arrays.asList(TsFileConstant.TIME_COLUMN_ID, "s1", "s2"));
    window.setEventInputColumnTypes(
        Arrays.asList(TSDataType.TIMESTAMP, TSDataType.INT64, TSDataType.INT64));
    window.setContainsRowsPlaceholder(true);
    TumbleWindowEngine engine = new TumbleWindowEngine(window, createDriverContext(), null, null);

    push(engine, createSingleColumnSlice("s1", 100L, 1L), 0, 1, 1L);
    push(engine, createSingleColumnSlice("s2", 200L, 2L), 0, 1, 2L);
    // A row past the window boundary closes [0, 1000).
    push(engine, createSingleColumnSlice("s1", 1001L, 3L), 0, 1, 3L);

    List<IEventInfo> events = awaitEvents(engine, 2);
    Assert.assertEquals(2, events.size());
    Assert.assertEquals(WindowEventType.WINDOW_CLOSE, events.get(0).getEventType());
    Assert.assertEquals(0L, events.get(0).getStartTime().getAsLong());
    Assert.assertEquals(1000L, events.get(0).getEndTime().getAsLong());

    // The closed window's rows carry each tablet's column and null for the column it omitted.
    IEventRowsIterator iterator = events.get(0).getRowsIterator();
    TsBlock block = null;
    while (iterator != null && !iterator.isFinished()) {
      TsBlock next = iterator.next();
      if (next == null) {
        if (!iterator.isBlocked().isDone()) {
          break;
        }
        continue;
      }
      block = next;
    }
    Assert.assertNotNull(block);
    Assert.assertEquals(2, block.getPositionCount());
    Assert.assertEquals(100L, block.getColumn(0).getLong(0));
    Assert.assertEquals(1L, block.getColumn(1).getLong(0));
    Assert.assertTrue(block.getColumn(2).isNull(0));
    Assert.assertEquals(200L, block.getColumn(0).getLong(1));
    Assert.assertTrue(block.getColumn(1).isNull(1));
    Assert.assertEquals(2L, block.getColumn(2).getLong(1));
  }

  @Test
  public void testOptimizationEntireTsBlockInSameWindow() {
    TumbleWindowEngine engine = createEngine();
    DataSlice initBlock = createDataSlice(new long[] {100L});
    push(engine, initBlock, 0, 1, 0L);

    long[] timestamps = {200L, 300L, 400L, 500L, 600L};
    DataSlice block = createDataSlice(timestamps);
    ListenableFuture<?> future = push(engine, block, 0, timestamps.length, 1L);
    Assert.assertSame(Operator.NOT_BLOCKED, future);
    Assert.assertTrue(getEventsFromQueue(engine).isEmpty());
  }

  @Test
  public void testMultipleWindowsTriggered() {
    TumbleWindowEngine engine = createEngine();
    DataSlice block1 = createDataSlice(new long[] {100L});
    push(engine, block1, 0, 1, 1L);

    DataSlice block2 = createDataSlice(new long[] {1500L});
    ListenableFuture<?> future2 = push(engine, block2, 0, 1, 2L);
    Assert.assertSame(Operator.NOT_BLOCKED, future2);

    List<IEventInfo> events2 = awaitEvents(engine, 2);
    Assert.assertEquals(2, events2.size());
    Assert.assertEquals(WindowEventType.WINDOW_CLOSE, events2.get(0).getEventType());
    Assert.assertEquals(0L, events2.get(0).getStartTime().getAsLong());
    Assert.assertEquals(1000L, events2.get(0).getEndTime().getAsLong());
    Assert.assertEquals(WindowEventType.WINDOW_DESTROY, events2.get(1).getEventType());
    printWindowData(events2);

    DataSlice block3 = createDataSlice(new long[] {2500L});
    ListenableFuture<?> future3 = push(engine, block3, 0, 1, 3L);
    Assert.assertSame(Operator.NOT_BLOCKED, future3);

    List<IEventInfo> events3 = awaitEvents(engine, 2);
    Assert.assertEquals(2, events3.size());
    Assert.assertEquals(WindowEventType.WINDOW_CLOSE, events3.get(0).getEventType());
    Assert.assertEquals(1000L, events3.get(0).getStartTime().getAsLong());
    Assert.assertEquals(2000L, events3.get(0).getEndTime().getAsLong());
    Assert.assertEquals(WindowEventType.WINDOW_DESTROY, events3.get(1).getEventType());
    printWindowData(events3);
  }

  @Test
  public void testSkipEmptyWindows() {
    TumbleWindowEngine engine = createEngine();
    DataSlice block1 = createDataSlice(new long[] {100L});
    push(engine, block1, 0, 1, 1L);

    DataSlice block2 = createDataSlice(new long[] {5500L});
    ListenableFuture<?> future = push(engine, block2, 0, 1, 2L);
    Assert.assertSame(Operator.NOT_BLOCKED, future);

    List<IEventInfo> events = awaitEvents(engine, 2);
    Assert.assertEquals(2, events.size());
    Assert.assertEquals(WindowEventType.WINDOW_CLOSE, events.get(0).getEventType());
    Assert.assertEquals(0L, events.get(0).getStartTime().getAsLong());
    Assert.assertEquals(1000L, events.get(0).getEndTime().getAsLong());
    Assert.assertEquals(WindowEventType.WINDOW_DESTROY, events.get(1).getEventType());
    printWindowData(events);
  }

  @Test
  public void testOriginAlignment() {
    TumbleWindowEngine originEngine = createEngine(WINDOW_SIZE_MS, 500L);

    DataSlice block1 = createDataSlice(new long[] {600L});
    push(originEngine, block1, 0, 1, 1L);

    DataSlice block2 = createDataSlice(new long[] {1600L});
    ListenableFuture<?> future = push(originEngine, block2, 0, 1, 2L);
    Assert.assertSame(Operator.NOT_BLOCKED, future);

    List<IEventInfo> events = awaitEvents(originEngine, 2);

    Assert.assertEquals(2, events.size());
    Assert.assertEquals(WindowEventType.WINDOW_CLOSE, events.get(0).getEventType());
    Assert.assertEquals(500L, events.get(0).getStartTime().getAsLong());
    Assert.assertEquals(1500L, events.get(0).getEndTime().getAsLong());
    Assert.assertEquals(WindowEventType.WINDOW_DESTROY, events.get(1).getEventType());
    printWindowData(events);
  }

  @Test
  public void testForceCloseWithPendingData() {
    TumbleWindowEngine engine = createEngine();
    DataSlice block = createDataSlice(new long[] {100L, 500L});
    push(engine, block, 0, 2, 1L);

    ListenableFuture<?> future = engine.forceClose();
    Assert.assertSame(Operator.NOT_BLOCKED, future);

    List<IEventInfo> events = getEventsFromQueue(engine);
    Assert.assertEquals(2, events.size());
    Assert.assertEquals(WindowEventType.WINDOW_CLOSE, events.get(0).getEventType());
    Assert.assertEquals(0L, events.get(0).getStartTime().getAsLong());
    Assert.assertEquals(1000L, events.get(0).getEndTime().getAsLong());
    Assert.assertEquals(WindowEventType.WINDOW_DESTROY, events.get(1).getEventType());
    printWindowData(events);
  }

  @Test
  public void testForceCloseWithNoData() {
    TumbleWindowEngine engine = createEngine();
    ListenableFuture<?> future = engine.forceClose();
    Assert.assertSame(Operator.NOT_BLOCKED, future);
    Assert.assertTrue(getEventsFromQueue(engine).isEmpty());
  }

  /**
   * A window fed by several tablets registers them all ({@code onWindowOpen} tracks the window's
   * min/max tablet range) and every processed slice fires {@code markTabletGroupDone}: after the
   * window's destroy marker is flushed, source commit advances through the window's max tablet.
   */
  @Test
  public void testCommitTabletsTrackedThroughWindowLifecycle() {
    List<Long> committed = new ArrayList<>();
    CommitTracker tracker = new CommitTracker(committed::add);
    StreamDriverContext driverContext = createDriverContext();
    driverContext.getSubTaskContext().setCommitTracker(tracker);

    TumbleWindow window = new TumbleWindow(WINDOW_SIZE_MS, ORIGIN_MS);
    window.setEventInputColumns(Collections.singletonList("time"));
    TumbleWindowEngine engine = new TumbleWindowEngine(window, driverContext, null, null);

    // Tablets 1..3, one partition group each.
    tracker.registerCommitId(1L, 1);
    tracker.registerCommitId(2L, 1);
    tracker.registerCommitId(3L, 1);

    // Tablet 1 opens [0,1000); tablet 2 feeds the SAME window (a later slice's onWindowOpen widens
    // the existing window's range to max tablet 2).
    push(engine, createDataSlice(new long[] {100L}), 0, 1, 1L);
    push(engine, createDataSlice(new long[] {300L}), 0, 1, 2L);
    // Tablet 3 crosses the boundary: [0,1000) closes and (lateness 0) is destroyed immediately.
    push(engine, createDataSlice(new long[] {1001L}), 0, 1, 3L);

    // Flushing the destroy marker (as the driver does) releases the window's whole tablet range:
    // commit advances to 2 - only possible because both tablets' group-done fired AND the window
    // tracked max tablet 2 (had the second tablet not registered, destroy would allow only 1,
    // pinning commit until the next window's own gate caught up).
    flushDestroyMarkers(engine, tracker);
    Assert.assertEquals(Collections.singletonList(2L), committed);

    // The still-open [1000,2000) (tablet 3) gates commit until its own destroy is flushed.
    Assert.assertTrue(engine.forceClose().isDone());
    flushDestroyMarkers(engine, tracker);
    Assert.assertEquals(Arrays.asList(2L, 3L), committed);
  }

  /**
   * {@code onWindowOpen} must fire for every tablet that contributes rows to a window, not only the
   * tablet that opened it (the old implementation's contract - "a late smaller tablet can lower the
   * open allow watermark"). A late, out-of-order slice with a smaller tablet id must lower the open
   * window's allow ({@code minTabletId - 1}), or source commit may pass the tablet while its rows
   * still sit unflushed in the window - a crash right then loses them. Under-registering also
   * understates the window's max tablet, delaying the tablets' commit after the destroy marker
   * flushes.
   */
  @Test
  public void testLateSmallerTabletLowersOpenAllow() {
    List<Long> committed = new ArrayList<>();
    CommitTracker tracker = new CommitTracker(committed::add);
    StreamDriverContext driverContext = createDriverContext();
    driverContext.getSubTaskContext().setCommitTracker(tracker);

    TumbleWindow window = new TumbleWindow(WINDOW_SIZE_MS, ORIGIN_MS);
    window.setEventInputColumns(Collections.singletonList("time"));
    TumbleWindowEngine engine = new TumbleWindowEngine(window, driverContext, null, null);

    tracker.registerCommitId(1L, 1);
    tracker.registerCommitId(2L, 1);

    // Tablet 2 opens [0,1000); tablet 1 arrives late (smaller id, out-of-order event time) and
    // feeds the same window.
    push(engine, createDataSlice(new long[] {300L}), 0, 1, 2L);
    push(engine, createDataSlice(new long[] {100L}), 0, 1, 1L);

    // Both tablets' scans are done (group-done fired per slice) and the consecutive prefix would
    // reach 2, but their rows are still unflushed in the open window: with tablet 1 registered the
    // open allow is min(1, 2) - 1 = 0 and nothing may commit. Had the late tablet not registered,
    // the allow would stay at 1 and tablet 1 would be committed with its rows unflushed.
    Assert.assertTrue("late smaller tablet must gate the open allow", committed.isEmpty());

    // Flushing the final destroy marker (all rows already flushed) releases the window's max
    // tablet: commit advances to 2.
    Assert.assertTrue(engine.forceClose().isDone());
    flushDestroyMarkers(engine, tracker);
    Assert.assertEquals(Collections.singletonList(2L), committed);
  }

  /**
   * Slices carrying no tablet id ({@code -1}, e.g. from sources without commit ids) never engage
   * the commit tracker: no window registers and no group-done fires, so flushing their destroy
   * markers is a harmless no-op and source commit never advances.
   */
  @Test
  public void testNegativeTabletIdNotTracked() {
    List<Long> committed = new ArrayList<>();
    CommitTracker tracker = new CommitTracker(committed::add);
    StreamDriverContext driverContext = createDriverContext();
    driverContext.getSubTaskContext().setCommitTracker(tracker);

    TumbleWindow window = new TumbleWindow(WINDOW_SIZE_MS, ORIGIN_MS);
    window.setEventInputColumns(Collections.singletonList("time"));
    TumbleWindowEngine engine = new TumbleWindowEngine(window, driverContext, null, null);

    tracker.registerCommitId(1L, 1);

    // All slices carry tabletId -1: the window still opens, closes and is destroyed normally...
    push(engine, createDataSlice(new long[] {100L, 500L}), 0, 2, -1L);
    push(engine, createDataSlice(new long[] {1001L}), 0, 1, -1L);
    IEventInfo closeEvent = engine.pollComputeEvent();
    Assert.assertNotNull(closeEvent);
    Assert.assertTrue(closeEvent.getWindowId() >= 0);

    // ...but the destroy marker's windowId was never registered (onWindowOpen saw tabletId -1):
    // flushing it touches no window state, and nothing ever fires.
    flushDestroyMarkers(engine, tracker);
    Assert.assertTrue("negative tabletIds must not engage the tracker", committed.isEmpty());
  }

  @Test
  public void testEventQueueOperations() {
    TumbleWindowEngine engine = createEngine();
    DataSlice block1 = createDataSlice(new long[] {100L});
    push(engine, block1, 0, 1, 1L);

    Assert.assertFalse(engine.hasPendingComputeEvent());
    Assert.assertNull(engine.peekComputeEvent());
    Assert.assertNull(engine.pollComputeEvent());

    DataSlice block2 = createDataSlice(new long[] {1001L});
    ListenableFuture<?> future = push(engine, block2, 0, 1, 2L);
    Assert.assertSame(Operator.NOT_BLOCKED, future);

    awaitPendingEvent(engine);
    Assert.assertTrue(engine.hasPendingComputeEvent());
    Assert.assertNotNull(engine.peekComputeEvent());
    // A closed window emits a close event followed by a destroy event
    Assert.assertNotNull(engine.pollComputeEvent());
    Assert.assertTrue(engine.hasPendingRecomputeEvent());
    Assert.assertNotNull(engine.pollRecomputeEvent());
    Assert.assertFalse(engine.hasPendingRecomputeEvent());
  }

  @Test
  public void testRowsIteratorInEvent() {
    TumbleWindowEngine engine = createEngine();
    DataSlice block1 = createDataSlice(new long[] {100L, 500L}, new long[] {10L, 20L});
    push(engine, block1, 0, 2, 1L);

    DataSlice block2 = createDataSlice(new long[] {1001L}, new long[] {30L});
    ListenableFuture<?> future = push(engine, block2, 0, 1, 2L);
    Assert.assertSame(Operator.NOT_BLOCKED, future);

    List<IEventInfo> events = awaitEvents(engine, 2);
    Assert.assertEquals(2, events.size());
    IEventInfo event = events.get(0);
    Assert.assertNotNull(event.getRowsIterator());
    Assert.assertFalse(event.getRowsIterator().isFinished());
    // the destroy event carries no rows
    Assert.assertNull(events.get(1).getRowsIterator());
    printWindowData(events);
  }

  @Test
  public void testGetBlockedFuture() {
    TumbleWindowEngine engine = createEngine();
    Assert.assertFalse(engine.getBlockedFuture().isDone());

    DataSlice block1 = createDataSlice(new long[] {100L});
    push(engine, block1, 0, 1, 1L);

    DataSlice block2 = createDataSlice(new long[] {1001L});
    push(engine, block2, 0, 1, 2L);

    awaitPendingEvent(engine);
    Assert.assertTrue(engine.getBlockedFuture().isDone());
  }

  @Test
  public void testStopPausesProcessingWhilePushStillReceives() {
    TumbleWindowEngine engine = createEngine();
    engine.stop();

    // push() must still accept data (reception is unaffected by stop), even though the batch
    // crosses
    // a window boundary and would normally trigger an event.
    DataSlice block = createDataSlice(new long[] {100L, 1001L});
    ListenableFuture<?> future = push(engine, block, 0, 2, 1L);
    Assert.assertSame(Operator.NOT_BLOCKED, future);
    // Processing is paused: the boundary-crossing batch produces no event.
    assertNoEventsShortly(engine, 150);

    // start() resumes processing; the queued batch is now processed and emits its close event
    // followed by the immediate destroy marker (allowedLateness 0).
    engine.start();
    List<IEventInfo> events = awaitEvents(engine, 1);
    Assert.assertEquals(2, events.size());
    Assert.assertEquals(0L, events.get(0).getStartTime().getAsLong());
    Assert.assertEquals(1000L, events.get(0).getEndTime().getAsLong());
  }

  @Test
  public void testStartIsIdempotent() {
    TumbleWindowEngine engine = createEngine();
    // The constructor already called start(); calling it again must be harmless.
    engine.start();
    engine.start();

    DataSlice block = createDataSlice(new long[] {100L, 1001L});
    push(engine, block, 0, 2, 1L);
    List<IEventInfo> events = awaitEvents(engine, 1);
    Assert.assertEquals(2, events.size());
  }

  @Test
  public void testDropStopsProcessing() {
    TumbleWindowEngine engine = createEngine();
    engine.drop();

    // After drop processing is disabled; pushed data is received but never processed.
    DataSlice block = createDataSlice(new long[] {100L, 1001L});
    ListenableFuture<?> future = push(engine, block, 0, 2, 1L);
    Assert.assertSame(Operator.NOT_BLOCKED, future);
    assertNoEventsShortly(engine, 150);
  }

  @Test
  public void testStartAfterDropIsNoOp() {
    TumbleWindowEngine engine = createEngine();
    engine.drop();
    // start() after drop() must NOT re-enable processing (drop is terminal).
    engine.start();

    DataSlice block = createDataSlice(new long[] {100L, 1001L});
    push(engine, block, 0, 2, 1L);
    assertNoEventsShortly(engine, 150);
  }

  @Test
  public void testPrevTimeNullOnFirstWindowThenPresent() {
    TumbleWindowEngine engine = createEngine();
    // Window [0, 1000): seed it with a row.
    DataSlice block1 = createDataSlice(new long[] {100L});
    push(engine, block1, 0, 1, 1L);

    // Cross into [1000, 2000) -> emit the first window [0, 1000): close event + destroy marker.
    DataSlice block2 = createDataSlice(new long[] {1001L});
    push(engine, block2, 0, 1, 2L);
    List<IEventInfo> events = awaitEvents(engine, 1);
    Assert.assertEquals(2, events.size());
    IEventInfo firstEvent = events.get(0);
    Assert.assertEquals(0L, firstEvent.getStartTime().getAsLong());
    // The first emitted window has no previous trigger -> prev_time is NULL.
    Assert.assertFalse(
        "prev_time should be NULL on the first window", firstEvent.getPrevTime().isPresent());

    // Cross into [2000, 3000) -> emit the second window [1000, 2000): close event + destroy marker.
    DataSlice block3 = createDataSlice(new long[] {2001L});
    push(engine, block3, 0, 1, 3L);
    List<IEventInfo> events2 = awaitEvents(engine, 1);
    Assert.assertEquals(2, events2.size());
    IEventInfo secondEvent = events2.get(0);
    Assert.assertEquals(1000L, secondEvent.getStartTime().getAsLong());
    // prev_time of the second window = previous window's start = 0.
    Assert.assertTrue(secondEvent.getPrevTime().isPresent());
    Assert.assertEquals(0L, secondEvent.getPrevTime().getAsLong());
  }

  // ===== Helpers for the control-options tests =====

  private TumbleWindowEngine createEngine(StreamProperties props) {
    TumbleWindow window = new TumbleWindow(WINDOW_SIZE_MS, ORIGIN_MS);
    window.setEventInputColumns(Collections.singletonList("time"));
    window.setContainsRowsPlaceholder(true);
    return new TumbleWindowEngine(window, createDriverContext(), props, null);
  }

  private TumbleWindowEngine createEngineWithColumns(StreamProperties props, List<String> columns) {
    TumbleWindow window = new TumbleWindow(WINDOW_SIZE_MS, ORIGIN_MS);
    window.setEventInputColumns(columns);
    window.setContainsRowsPlaceholder(true);
    return new TumbleWindowEngine(window, createDriverContext(), props, null);
  }

  /**
   * Reads {@code [timestamp, value]} pairs from an event whose scan columns are {@code [time,
   * value]}. The row store writes the event timestamp into column 0 and the value into column 1.
   */
  private List<long[]> readTimestampValuePairs(IEventInfo event) {
    List<long[]> pairs = new ArrayList<>();
    IEventRowsIterator it = event.getRowsIterator();
    while (it != null && !it.isFinished()) {
      TsBlock block = it.next();
      if (block == null) {
        // next() returns null between the TsBlocks of one recorded batch (and between recorded
        // ranges); keep pulling while more data can be generated, and stop when nothing more can
        // (the window is still open with no range recorded).
        if (!it.isBlocked().isDone()) {
          break;
        }
        continue;
      }
      for (int i = 0; i < block.getPositionCount(); i++) {
        pairs.add(new long[] {block.getColumn(0).getLong(i), block.getColumn(1).getLong(i)});
      }
    }
    return pairs;
  }

  /** A TumbleWindowEngine whose system clock is test-controlled, for MAX_DELAY / MAX_IDLE tests. */
  private static final class ControllableTimeEngine extends TumbleWindowEngine {
    private volatile long currentMillis;

    ControllableTimeEngine(
        StreamWindow window,
        StreamDriverContext driverContext,
        StreamProperties props,
        long start) {
      super(window, driverContext, props, null);
      this.currentMillis = start;
    }

    @Override
    protected long nowMillis() {
      return currentMillis;
    }

    void advanceTo(long millis) {
      this.currentMillis = millis;
    }
  }

  private ControllableTimeEngine createTimedEngine(StreamProperties props, long startMillis) {
    TumbleWindow window = new TumbleWindow(WINDOW_SIZE_MS, ORIGIN_MS);
    window.setEventInputColumns(Collections.singletonList("time"));
    window.setContainsRowsPlaceholder(true);
    return new ControllableTimeEngine(window, createDriverContext(), props, startMillis);
  }

  // ===== Control-options tests =====

  @Test
  public void testWatermarkOptionDelaysClose() {
    // watermark=1000: the watermark lags maxEventTime by 1000, so a row at 1500 (watermark=500)
    // does NOT yet close window [0,1000). Both windows coexist open until a row at 2500
    // (watermark=1500) finally closes [0,1000).
    StreamProperties props = new StreamProperties(1000, 0, 0, 0, true);
    TumbleWindowEngine engine = createEngine(props);

    push(engine, createDataSlice(new long[] {100L}), 0, 1, 1L);
    push(engine, createDataSlice(new long[] {1500L}), 0, 1, 2L);
    // Both [0,1000) and [1000,2000) are still open: watermark (500) has not reached 1000.
    Assert.assertTrue(getEventsFromQueue(engine).isEmpty());

    push(engine, createDataSlice(new long[] {2500L}), 0, 1, 3L);
    List<IEventInfo> events = awaitEvents(engine, 1);
    Assert.assertEquals(2, events.size());
    Assert.assertEquals(0L, events.get(0).getStartTime().getAsLong());
    Assert.assertEquals(1000L, events.get(0).getEndTime().getAsLong());
    Assert.assertTrue(WindowEventType.WINDOW_CLOSE.equals(events.get(0).getEventType()));
    // allowedLateness 0: the closed window is immediately destroyed -> destroy marker follows.
    Assert.assertTrue(WindowEventType.WINDOW_DESTROY.equals(events.get(1).getEventType()));
  }

  @Test
  public void testMultiWindowCloseEmitsEventsInWindowOrder() {
    // A watermark jump closing many windows at once must offer the close events in window-start
    // order: a hash-ordered `windows` map emitted them as (+0, +2000, +4000, +1000, +3000, +5000)
    // and prev_time (tracked via lastEmittedStart) followed the same scrambled order - window
    // +1000's prev_time was +4000. Events must be created and offered in window-start order.
    StreamProperties props = new StreamProperties(10000, 0, 0, 0, true);
    TumbleWindowEngine engine = createEngine(props);

    // Rows at 500..5500 open six windows [0,1000) .. [5000,6000); the watermark (max event time
    // - 10s) stays below every window end, so nothing closes yet.
    push(engine, createDataSlice(new long[] {500L, 1500L, 2500L, 3500L, 4500L, 5500L}), 0, 6, 1L);
    Assert.assertTrue(getEventsFromQueue(engine).isEmpty());

    // One watermark jump (row 16000 -> watermark 6000) closes and destroys all six windows at once.
    push(engine, createDataSlice(new long[] {16000L}), 0, 1, 2L);
    List<IEventInfo> events = getEventsFromQueue(engine);
    // Six close events (compute queue) followed by six destroy markers (recompute queue), each in
    // window-start order.
    Assert.assertEquals(12, events.size());
    for (int i = 0; i < 12; i++) {
      Assert.assertEquals(
          "event " + i + " must follow window-start order",
          (long) i % 6 * 1000L,
          events.get(i).getStartTime().getAsLong());
    }
    // prev_time chains along the emission order: NULL on the first window, then each window's
    // predecessor's start.
    Assert.assertFalse(
        "prev_time should be NULL on the first window", events.get(0).getPrevTime().isPresent());
    for (int i = 1; i < 6; i++) {
      Assert.assertTrue(events.get(i).getPrevTime().isPresent());
      Assert.assertEquals((i - 1) * 1000L, events.get(i).getPrevTime().getAsLong());
    }
  }

  @Test
  public void testLeftOpenWindowBoundaryBelongsPreviousWindow() {
    // leftClosed=false: the window is (start, end]; a row at 1000 belongs to (0,1000], so the
    // emitted window starts at 0 (under left-closed it would start at 1000).
    StreamProperties props = new StreamProperties(0, 0, 0, 0, false);
    TumbleWindowEngine engine = createEngine(props);

    push(engine, createDataSlice(new long[] {1000L}), 0, 1, 1L);
    push(engine, createDataSlice(new long[] {2001L}), 0, 1, 2L);
    // Row 2001 (wm 2001) closes and immediately destroys (0,1000]: close event + destroy marker.
    List<IEventInfo> events = awaitEvents(engine, 1);
    Assert.assertEquals(2, events.size());
    Assert.assertEquals(0L, events.get(0).getStartTime().getAsLong());
    Assert.assertEquals(1000L, events.get(0).getEndTime().getAsLong());
    Assert.assertTrue(WindowEventType.WINDOW_CLOSE.equals(events.get(0).getEventType()));
    Assert.assertTrue(WindowEventType.WINDOW_DESTROY.equals(events.get(1).getEventType()));
  }

  @Test
  public void testIncrementalEmitThreshold() {
    // Incremental mode (usesRowNum=false): seed a high row to set the watermark, then push 1000
    // out-of-order rows below it. The 1000th triggers a pre-close incremental emission, offered as
    // a WINDOW_OPEN event (the window is still open).
    StreamProperties props = new StreamProperties(0, 0, 0, 0, true);
    TumbleWindow window = new TumbleWindow(100000, ORIGIN_MS);
    window.setEventInputColumns(Collections.singletonList("time"));
    window.setContainsRowsPlaceholder(true);
    TumbleWindowEngine engine = new TumbleWindowEngine(window, createDriverContext(), props, null);

    // Row 99999 sets maxEventTime=watermark=99999; window [0,100000) stays open.
    push(engine, createDataSlice(new long[] {99999L}), 0, 1, 1L);
    // 1000 out-of-order rows (timestamps 1..1000) accumulate in (computeLine=0, watermark=99999).
    long[] late = new long[1000];
    for (int i = 0; i < 1000; i++) {
      late[i] = i + 1;
    }
    push(engine, createDataSlice(late), 0, 1000, 2L);

    List<IEventInfo> events = awaitEvents(engine, 1);
    Assert.assertEquals(1, events.size());
    Assert.assertFalse(WindowEventType.WINDOW_CLOSE.equals(events.get(0).getEventType()));
    Assert.assertEquals(0L, events.get(0).getStartTime().getAsLong());
    Assert.assertEquals(100000L, events.get(0).getEndTime().getAsLong());
    Assert.assertEquals(1000, events.get(0).getRowCount().getAsLong());
  }

  @Test
  public void testIncrementalEmitThresholdInOrder() {
    // In-order rows also check the 1000-row threshold: with watermarkOption=0 the watermark equals
    // maxEventTime, so as each new maximum arrives every earlier row falls below the watermark.
    // The 1001st in-order row pushes the pending count to 1000, triggering a pre-close incremental
    // emission (a WINDOW_OPEN event) over (computeLine=0, watermark=1001) = rows 1..1000.
    StreamProperties props = new StreamProperties(0, 0, 0, 0, true);
    TumbleWindow window = new TumbleWindow(100000, ORIGIN_MS);
    window.setEventInputColumns(Collections.singletonList("time"));
    window.setContainsRowsPlaceholder(true);
    TumbleWindowEngine engine = new TumbleWindowEngine(window, createDriverContext(), props, null);

    long[] rows = new long[1001];
    for (int i = 0; i < 1001; i++) {
      rows[i] = i + 1;
    }
    push(engine, createDataSlice(rows), 0, 1001, 1L);

    List<IEventInfo> events = awaitEvents(engine, 1);
    Assert.assertEquals(1, events.size());
    Assert.assertFalse(WindowEventType.WINDOW_CLOSE.equals(events.get(0).getEventType()));
    Assert.assertEquals(0L, events.get(0).getStartTime().getAsLong());
    Assert.assertEquals(100000L, events.get(0).getEndTime().getAsLong());
    Assert.assertEquals(1000, events.get(0).getRowCount().getAsLong());
  }

  @Test
  public void testIncrementalEmitThresholdViaWatermarkAdvance() {
    // The pending count is no longer recomputed per row (the O(devices) countBetween): a row
    // arriving at/above the watermark parks its timestamp in the uncounted heap and is counted
    // exactly when a later maximum advances the watermark past it. With watermark option 500, row
    // 1500 advances the watermark to 1000 and counts rows 1..999 (999 pending); row 1501 counts
    // the 1000th, triggering the pre-close incremental emission over rows 1..1000.
    StreamProperties props = new StreamProperties(500, 0, 0, 0, true);
    TumbleWindow window = new TumbleWindow(100000, ORIGIN_MS);
    window.setEventInputColumns(Collections.singletonList("time"));
    window.setContainsRowsPlaceholder(true);
    TumbleWindowEngine engine = new TumbleWindowEngine(window, createDriverContext(), props, null);

    long[] rows = new long[1500];
    for (int i = 0; i < 1500; i++) {
      rows[i] = i + 1;
    }
    push(engine, createDataSlice(rows), 0, 1500, 1L);
    Assert.assertTrue(getEventsFromQueue(engine).isEmpty());

    push(engine, createDataSlice(new long[] {1501L}), 0, 1, 2L);
    List<IEventInfo> events = awaitEvents(engine, 1);
    Assert.assertEquals(1, events.size());
    Assert.assertFalse(WindowEventType.WINDOW_CLOSE.equals(events.get(0).getEventType()));
    Assert.assertEquals(0L, events.get(0).getStartTime().getAsLong());
    Assert.assertEquals(100000L, events.get(0).getEndTime().getAsLong());
    Assert.assertEquals(1000, events.get(0).getRowCount().getAsLong());
  }

  @Test
  public void testUncountedRowsOfDestroyedWindowAreNotCountedIntoLaterWindow() {
    // Stale uncounted-heap entries (rows of a MAX_DELAY force-closed and destroyed window whose
    // timestamps never fell below the watermark) must be skipped by the drain's window lookup,
    // never counted into a later window: 501 stale entries + 1000 fresh rows would otherwise
    // reach the 1000-row threshold prematurely and emit a corrupted (too small) increment.
    StreamProperties props = new StreamProperties(500, 0, 100, 0, true);
    long t0 = 1_000_000L;
    // A 100000ms window (not the helper's 1000ms default): the 1499 rows must all land in ONE
    // window, so the force-close produces exactly one close event + one destroy marker.
    TumbleWindow window = new TumbleWindow(100000, ORIGIN_MS);
    window.setEventInputColumns(Collections.singletonList("time"));
    window.setContainsRowsPlaceholder(true);
    ControllableTimeEngine engine =
        new ControllableTimeEngine(window, createDriverContext(), props, t0);

    // Window [0,100000): rows 1..1499 count 998 pending (below the threshold), and rows 999..1499
    // (501 timestamps) sit in the uncounted heap above the watermark 999.
    long[] rows = new long[1499];
    for (int i = 0; i < 1499; i++) {
      rows[i] = i + 1;
    }
    push(engine, createDataSlice(rows), 0, 1499, 1L);
    Assert.assertTrue(getEventsFromQueue(engine).isEmpty());

    // Past the MAX_DELAY deadline: the open window is force-closed (all 1499 rows as the close
    // increment) and destroyed immediately (allowedLateness 0) - its 501 heap entries go stale.
    engine.advanceTo(t0 + 300);
    engine.tick();
    List<IEventInfo> events = awaitEvents(engine, 1);
    Assert.assertEquals(2, events.size());
    Assert.assertTrue(WindowEventType.WINDOW_CLOSE.equals(events.get(0).getEventType()));
    Assert.assertEquals(1499, events.get(0).getRowCount().getAsLong());
    Assert.assertTrue(WindowEventType.WINDOW_DESTROY.equals(events.get(1).getEventType()));

    // A later window [200000,300000) receives 1000 rows. Correct behavior: the stale entries are
    // dropped by the drain and the fresh count stays at 499 (rows 200001..200499 below the final
    // watermark 200500) - below the threshold, no event. Had the stale entries leaked into this
    // window, the 1000-row threshold would have fired exactly at this batch's last row with a
    // corrupted (too small) increment.
    long[] fresh = new long[1000];
    for (int i = 0; i < 1000; i++) {
      fresh[i] = 200001L + i;
    }
    push(engine, createDataSlice(fresh), 0, 1000, 2L);
    Assert.assertTrue(getEventsFromQueue(engine).isEmpty());
    Assert.assertFalse(engine.hasPendingComputeEvent());

    // The fresh window still emits normally at its own 1000th pending row: the second batch's
    // last row (201501) advances the watermark to 201001, counting rows 200001..201000 as the
    // emission's increment.
    long[] more = new long[501];
    for (int i = 0; i < 501; i++) {
      more[i] = 201001L + i;
    }
    push(engine, createDataSlice(more), 0, 501, 3L);
    List<IEventInfo> later = awaitEvents(engine, 1);
    Assert.assertEquals(1, later.size());
    Assert.assertFalse(WindowEventType.WINDOW_CLOSE.equals(later.get(0).getEventType()));
    Assert.assertEquals(200000L, later.get(0).getStartTime().getAsLong());
    Assert.assertEquals(300000L, later.get(0).getEndTime().getAsLong());
    Assert.assertEquals(1000, later.get(0).getRowCount().getAsLong());
    engine.drop();
  }

  @Test
  public void testFullComputeModeSuppressesIncrementalEmit() {
    // Full compute mode (usesRowNum=true): no pre-close incremental emission even at the threshold;
    // the window emits all rows only on close.
    StreamDriverContext driverContext = createDriverContext();
    TumbleWindow window = new TumbleWindow(100000, ORIGIN_MS);
    window.getLiteralPlaceholders().add(StreamLiteral.Type.ROW_NUM);
    window.setEventInputColumns(Collections.singletonList("time"));
    window.setContainsRowsPlaceholder(true);
    TumbleWindowEngine engine =
        new TumbleWindowEngine(window, driverContext, new StreamProperties(0, 0, 0, 0, true), null);

    push(engine, createDataSlice(new long[] {99999L}), 0, 1, 1L);
    long[] late = new long[1000];
    for (int i = 0; i < 1000; i++) {
      late[i] = i + 1;
    }
    push(engine, createDataSlice(late), 0, 1000, 2L);
    // Full compute: the 1000-row threshold produces no pre-close event.
    Assert.assertTrue(getEventsFromQueue(engine).isEmpty());

    // Crossing the window end emits all 1001 rows (99999 + 1..1000) as a single closed event,
    // followed by the immediate destroy marker (allowedLateness 0).
    push(engine, createDataSlice(new long[] {100001L}), 0, 1, 3L);
    List<IEventInfo> events = awaitEvents(engine, 1);
    Assert.assertEquals(2, events.size());
    Assert.assertTrue(WindowEventType.WINDOW_CLOSE.equals(events.get(0).getEventType()));
    Assert.assertEquals(1001, events.get(0).getRowCount().getAsLong());
    Assert.assertTrue(WindowEventType.WINDOW_DESTROY.equals(events.get(1).getEventType()));
  }

  @Test
  public void testIncrementalModeCloseEventCarriesOnlyRemainingRows() {
    // Incremental mode: one window produces exactly ONE event. The pre-close increment (rows
    // 1..1000) creates it as a WINDOW_OPEN event; on close the SAME event flips to WINDOW_CLOSE
    // (markClosed) and appends only the un-emitted remainder (row 99999 above computeLine=1000),
    // accumulating rowCount to 1001.
    StreamProperties props = new StreamProperties(0, 0, 0, 0, true);
    TumbleWindow window = new TumbleWindow(100000, ORIGIN_MS);
    window.setEventInputColumns(Collections.singletonList("time"));
    window.setContainsRowsPlaceholder(true);
    TumbleWindowEngine engine = new TumbleWindowEngine(window, createDriverContext(), props, null);

    push(engine, createDataSlice(new long[] {99999L}), 0, 1, 1L);
    long[] late = new long[1000];
    for (int i = 0; i < 1000; i++) {
      late[i] = i + 1;
    }
    push(engine, createDataSlice(late), 0, 1000, 2L);
    List<IEventInfo> events = awaitEvents(engine, 1);
    Assert.assertEquals(1, events.size());
    IEventInfo event = events.get(0);
    Assert.assertFalse(WindowEventType.WINDOW_CLOSE.equals(event.getEventType()));
    Assert.assertEquals(1000, event.getRowCount().getAsLong());

    // Row 100001 closes [0,100000): no new compute event is queued - the single event flips to
    // WINDOW_CLOSE and appends the remaining un-emitted row (99999). The immediate destroy
    // (allowedLateness 0) queues the window's compute-free destroy marker behind it.
    push(engine, createDataSlice(new long[] {100001L}), 0, 1, 3L);
    IEventInfo destroyMarker = engine.pollRecomputeEvent();
    Assert.assertNotNull(destroyMarker);
    Assert.assertTrue(WindowEventType.WINDOW_DESTROY.equals(destroyMarker.getEventType()));
    Assert.assertFalse(destroyMarker.needsCompute());
    Assert.assertFalse(engine.hasPendingComputeEvent());
    Assert.assertTrue(WindowEventType.WINDOW_CLOSE.equals(event.getEventType()));
    Assert.assertEquals(0L, event.getStartTime().getAsLong());
    Assert.assertEquals(100000L, event.getEndTime().getAsLong());
    Assert.assertEquals(1001, event.getRowCount().getAsLong());
  }

  @Test
  public void testIncrementalModeLateRowBelowComputeLineSurvivesCloseForRecompute() {
    // Incremental mode with lateness: one window produces exactly ONE event. A late row at/below
    // computeLine (already-emitted territory) stays un-emitted and dirty through close (the close
    // does not append it); the destroy-time full recompute then delivers it in an all-rows event.
    StreamProperties props = new StreamProperties(0, 500, 0, 0, true);
    TumbleWindow window = new TumbleWindow(100000, ORIGIN_MS);
    window.setEventInputColumns(Arrays.asList("time", "value"));
    window.setContainsRowsPlaceholder(true);
    TumbleWindowEngine engine = new TumbleWindowEngine(window, createDriverContext(), props, null);

    // Increment over rows 1..1000 (computeLine -> 1000); row 99999 stays un-emitted.
    push(engine, createDataSlice(new long[] {99999L}, new long[] {1L}), 0, 1, 1L);
    long[] late = new long[1000];
    long[] values = new long[1000];
    for (int i = 0; i < 1000; i++) {
      late[i] = i + 1;
      values[i] = 10L;
    }
    push(engine, createDataSlice(late, values), 0, 1000, 2L);
    List<IEventInfo> events = awaitEvents(engine, 1);
    Assert.assertEquals(1, events.size());
    IEventInfo event = events.get(0);
    Assert.assertFalse(WindowEventType.WINDOW_CLOSE.equals(event.getEventType()));

    // Late row at t=500 (<= computeLine 1000): replaces the stored value, dirty, NOT emitted.
    push(engine, createDataSlice(new long[] {500L}, new long[] {999L}), 0, 1, 3L);
    assertNoEventsShortly(engine, 200); // no event for the below-line late row

    // Row 100001 closes [0,100000): no new event - the single event flips to WINDOW_CLOSE,
    // appending only
    // row 99999 (the below-line late row stays dirty, recovered by the destroy-time recompute).
    push(engine, createDataSlice(new long[] {100001L}, new long[] {2L}), 0, 1, 4L);
    Assert.assertFalse(engine.hasPendingComputeEvent());
    Assert.assertTrue(WindowEventType.WINDOW_CLOSE.equals(event.getEventType()));
    Assert.assertEquals(1001, event.getRowCount().getAsLong());

    // Watermark 100501 >= delayLine 100500 destroys the window; dirty -> all-rows recompute
    // including the late row (t=500 -> 999) that close never carried. The recompute event is the
    // window's terminal (destroy-typed, EOF) event.
    push(engine, createDataSlice(new long[] {100501L}, new long[] {3L}), 0, 1, 5L);
    List<IEventInfo> recomputeEvents = awaitRecomputeEvents(engine, 1);
    Assert.assertEquals(1, recomputeEvents.size());
    IEventInfo recomputeEvent = recomputeEvents.get(0);
    Assert.assertTrue(WindowEventType.WINDOW_DESTROY.equals(recomputeEvent.getEventType()));
    Assert.assertTrue(recomputeEvent.needsCompute());
    Assert.assertEquals(1001, recomputeEvent.getRowCount().getAsLong());
    List<long[]> recomputePairs = readTimestampValuePairs(recomputeEvent);
    Assert.assertEquals(1001, recomputePairs.size());
    Assert.assertArrayEquals(new long[] {500L, 999L}, recomputePairs.get(499));
  }

  @Test
  public void testCachedWindowLateDataReplacesAndRecomputesOnDestroy() {
    // allowedLateness=500: after [0,1000) closes it stays cached until watermark >= 1500. Late data
    // at an existing timestamp REPLACES the row (not appends); the dirty window is then fully
    // recomputed when destroyed.
    StreamProperties props = new StreamProperties(0, 500, 0, 0, true);
    TumbleWindowEngine engine = createEngineWithColumns(props, Arrays.asList("time", "value"));

    // Seed [0,1000) with (100->10, 500->20).
    push(engine, createDataSlice(new long[] {100L, 500L}, new long[] {10L, 20L}), 0, 2, 1L);
    // Row 1001 closes [0,1000) (watermark 1001 >= 1000); cached because lateness=500.
    push(engine, createDataSlice(new long[] {1001L}, new long[] {30L}), 0, 1, 2L);
    List<IEventInfo> closeEvents = awaitEvents(engine, 1);
    Assert.assertEquals(1, closeEvents.size());
    List<long[]> closePairs = readTimestampValuePairs(closeEvents.get(0));
    Assert.assertEquals(2, closePairs.size());
    Assert.assertArrayEquals(new long[] {100L, 10L}, closePairs.get(0));
    Assert.assertArrayEquals(new long[] {500L, 20L}, closePairs.get(1));

    // Late data at timestamp 100 (already in the cached window) -> replaces 10 with 999, dirty.
    push(engine, createDataSlice(new long[] {100L}, new long[] {999L}), 0, 1, 3L);
    // Row 1601 advances watermark to 1601 >= delayLine 1500 -> destroy; dirty -> recompute.
    push(engine, createDataSlice(new long[] {1601L}, new long[] {40L}), 0, 1, 4L);
    List<IEventInfo> recomputeEvents = awaitRecomputeEvents(engine, 1);
    Assert.assertEquals(1, recomputeEvents.size());
    // The recompute event (the window's terminal, destroy-typed event) carries the replaced value
    // (999), and the unchanged row (500->20).
    IEventInfo recomputeEvent = recomputeEvents.get(0);
    Assert.assertTrue(WindowEventType.WINDOW_DESTROY.equals(recomputeEvent.getEventType()));
    Assert.assertEquals(0L, recomputeEvent.getStartTime().getAsLong());
    Assert.assertEquals(2, recomputeEvent.getRowCount().getAsLong());
    List<long[]> recomputePairs = readTimestampValuePairs(recomputeEvent);
    Assert.assertEquals(2, recomputePairs.size());
    Assert.assertArrayEquals(new long[] {100L, 999L}, recomputePairs.get(0));
    Assert.assertArrayEquals(new long[] {500L, 20L}, recomputePairs.get(1));
  }

  @Test
  public void testCloseEventExcludesLateRowsArrivingAfterClose() {
    // A TO_END range must not resolve its upper bound (rowStore.size()) at next() time.
    // The close event's rowCount is fixed at close, so a consumer slower than a late upsert would
    // materialize post-close late rows into the close event - ${row_num} then disagrees with
    // count(*). The close event must carry exactly the rows counted at close; late rows belong to
    // the destroy-time full recompute only. Full-compute (${row_num}) mode is the cleanest repro:
    // the close event claims all rows of the window.
    TumbleWindow window = new TumbleWindow(WINDOW_SIZE_MS, ORIGIN_MS);
    window.getLiteralPlaceholders().add(StreamLiteral.Type.ROW_NUM);
    window.setEventInputColumns(Arrays.asList("time", "value"));
    window.setContainsRowsPlaceholder(true);
    TumbleWindowEngine engine =
        new TumbleWindowEngine(
            window, createDriverContext(), new StreamProperties(0, 500, 0, 0, true), null);

    // Seed [0,1000) with (100->10); row 1001 closes it (watermark 1001 >= 1000), cached for
    // lateness 500.
    push(engine, createDataSlice(new long[] {100L, 500L}, new long[] {10L, 20L}), 0, 2, 1L);
    push(engine, createDataSlice(new long[] {1001L}, new long[] {30L}), 0, 1, 2L);
    List<IEventInfo> closeEvents = awaitEvents(engine, 1);
    Assert.assertEquals(1, closeEvents.size());
    IEventInfo closeEvent = closeEvents.get(0);
    Assert.assertTrue(WindowEventType.WINDOW_CLOSE.equals(closeEvent.getEventType()));
    Assert.assertEquals(2, closeEvent.getRowCount().getAsLong());

    // Late rows (new timestamp 300, replacement at 500) enter the cached window while the close
    // event sits unconsumed.
    push(engine, createDataSlice(new long[] {300L, 500L}, new long[] {77L, 99L}), 0, 2, 3L);

    // Only now does the consumer materialize the close event: exactly the 2 rows counted at close
    // (100->10, 500->20), none of the late ones.
    List<long[]> closePairs = readTimestampValuePairs(closeEvent);
    Assert.assertEquals(2, closePairs.size());
    Assert.assertArrayEquals(new long[] {100L, 10L}, closePairs.get(0));
    Assert.assertArrayEquals(new long[] {500L, 20L}, closePairs.get(1));

    // Watermark 1601 >= delayLine 1500 destroys the window; the dirty full recompute carries ALL
    // rows, including both late ones (300->77, replaced 500->99).
    push(engine, createDataSlice(new long[] {1601L}, new long[] {40L}), 0, 1, 4L);
    List<IEventInfo> recomputeEvents = awaitRecomputeEvents(engine, 1);
    Assert.assertEquals(1, recomputeEvents.size());
    IEventInfo recomputeEvent = recomputeEvents.get(0);
    Assert.assertEquals(3, recomputeEvent.getRowCount().getAsLong());
    List<long[]> recomputePairs = readTimestampValuePairs(recomputeEvent);
    Assert.assertEquals(3, recomputePairs.size());
    Assert.assertArrayEquals(new long[] {100L, 10L}, recomputePairs.get(0));
    Assert.assertArrayEquals(new long[] {300L, 77L}, recomputePairs.get(1));
    Assert.assertArrayEquals(new long[] {500L, 99L}, recomputePairs.get(2));
  }

  @Test
  public void testRecomputeEventGoesToSeparateQueue() {
    // The close event goes to the compute queue (pollEvent); the dirty-destroy recompute event goes
    // to the separate recompute queue (pollRecomputeEvent). The two queues never cross-contaminate.
    StreamProperties props = new StreamProperties(0, 500, 0, 0, true);
    TumbleWindowEngine engine = createEngineWithColumns(props, Arrays.asList("time", "value"));

    push(engine, createDataSlice(new long[] {100L}, new long[] {10L}), 0, 1, 1L);
    push(engine, createDataSlice(new long[] {1001L}, new long[] {20L}), 0, 1, 2L); // close [0,1000)
    List<IEventInfo> closeEvents = awaitEvents(engine, 1);
    Assert.assertEquals(1, closeEvents.size());
    Assert.assertTrue(WindowEventType.WINDOW_CLOSE.equals(closeEvents.get(0).getEventType()));
    // No recompute event yet.
    Assert.assertFalse(engine.hasPendingRecomputeEvent());

    // Late data dirties the cached window; destroying it produces a recompute event.
    push(engine, createDataSlice(new long[] {100L}, new long[] {99L}), 0, 1, 3L);
    push(
        engine,
        createDataSlice(new long[] {1601L}, new long[] {30L}),
        0,
        1,
        4L); // destroy [0,1000)

    List<IEventInfo> recomputeEvents = awaitRecomputeEvents(engine, 1);
    Assert.assertEquals(1, recomputeEvents.size());
    // The recompute event is the window's terminal (destroy-typed, EOF) event.
    Assert.assertTrue(WindowEventType.WINDOW_DESTROY.equals(recomputeEvents.get(0).getEventType()));
    // The compute queue stays empty: the recompute event did not leak into it.
    Assert.assertFalse(engine.hasPendingComputeEvent());
  }

  @Test
  public void testMaxDelayWindowCloseForceCloses() {
    // maxDelayWindowCloseMs=100: an open window whose system-time deadline passes is force-closed
    // by onTick even though no further data has advanced the watermark. There is no per-engine
    // thread anymore: the tick is driven by the shared per-task scheduler in production, and by
    // an explicit tick() call here.
    StreamProperties props = new StreamProperties(0, 0, 100, 0, true);
    long t0 = 1_000_000L;
    ControllableTimeEngine engine = createTimedEngine(props, t0);

    push(engine, createDataSlice(new long[] {100L}), 0, 1, 1L);
    // Processing is synchronous: the window is already open with closeSystemTime = t0 + 100.
    Assert.assertTrue(getEventsFromQueue(engine).isEmpty());

    engine.advanceTo(t0 + 300); // past the close deadline
    engine.tick();
    // allowedLateness=0: the force-closed window is destroyed immediately, its close event
    // followed by the compute-free destroy marker.
    List<IEventInfo> events = awaitEvents(engine, 1);
    Assert.assertEquals(2, events.size());
    Assert.assertTrue(WindowEventType.WINDOW_CLOSE.equals(events.get(0).getEventType()));
    Assert.assertEquals(0L, events.get(0).getStartTime().getAsLong());
    Assert.assertEquals(1000L, events.get(0).getEndTime().getAsLong());
    Assert.assertTrue(WindowEventType.WINDOW_DESTROY.equals(events.get(1).getEventType()));
  }

  @Test
  public void testDataPoolExhaustionDoesNotBlockMaxDelayForceClose() throws Exception {
    // A data-pool exhaustion memory-pauses the engine, and tick() used to skip onTick entirely
    // while paused. But MAX_DELAY force-close / MAX_IDLE force-destroy only RELEASE data-pool
    // memory (their events are materialized lazily), and they are the engine's only self-recovery
    // when all pool memory is held by open windows - the watermark cannot advance either while
    // processing is halted. Skipping them deadlocks the exhausted pool.
    StreamProperties props = new StreamProperties(0, 0, 100, 0, true);
    long t0 = 1_000_000L;
    ControllableTimeEngine engine = createTimedEngine(props, t0);
    GlobalWindowMemoryController controller = GlobalWindowMemoryController.getInstance();

    // Open window [0,1000) holding one row, charged to the data pool.
    push(engine, createDataSlice(new long[] {100L}), 0, 1, 1L);
    Assert.assertTrue(getEventsFromQueue(engine).isEmpty());

    // Exhaust the data pool, then push one more batch: the engine memory-pauses and buffers it.
    long held = controller.getDataTotalBytes() - controller.getDataUsedBytes();
    Assert.assertTrue(controller.tryReserveDataMemory(held));
    try {
      push(engine, createDataSlice(new long[] {150L}), 0, 1, 2L);
      Assert.assertTrue(engine.isMemoryPaused());

      // Past the MAX_DELAY_WINDOW_CLOSE deadline: the tick must still force-close the open
      // window - its events are the engine's only path out of the exhausted data pool.
      engine.advanceTo(t0 + 300);
      engine.tick();
      List<IEventInfo> events = awaitEvents(engine, 1);
      Assert.assertEquals(2, events.size());
      Assert.assertTrue(WindowEventType.WINDOW_CLOSE.equals(events.get(0).getEventType()));
      Assert.assertEquals(0L, events.get(0).getStartTime().getAsLong());
      Assert.assertEquals(1000L, events.get(0).getEndTime().getAsLong());
      Assert.assertTrue(WindowEventType.WINDOW_DESTROY.equals(events.get(1).getEventType()));

      // Full self-recovery: releasing the held memory lets the (asynchronous) resume drain the
      // buffered batch - its row is late for the destroyed window and dropped, but the engine
      // must leave the memory pause.
      Assert.assertTrue(controller.releaseDataMemory(held));
      long deadline = System.currentTimeMillis() + 5000;
      while (engine.isMemoryPaused() && System.currentTimeMillis() < deadline) {
        Thread.sleep(5);
      }
      Assert.assertFalse(
          "the engine should resume after the data-pool release", engine.isMemoryPaused());
    } finally {
      controller.releaseDataMemory(held);
      engine.drop();
    }
  }

  @Test
  public void testDataPoolExhaustionRescuesWithDefaultControlOptions() throws Exception {
    // With MAX_DELAY_WINDOW_CLOSE and MAX_IDLE_WINDOW_RETAIN both at their default 0 (disabled),
    // the previous self-recovery does not exist: tick()'s onTick() is a no-op, the watermark
    // cannot advance while processing is halted, so no window ever closes or destroys and the
    // data-pool memory held by open windows is never returned - the task stalls forever with only
    // a WARN. The engine must fall back to force-releasing its own windows once the data-pool
    // pause has outlived a grace period.
    StreamProperties props = new StreamProperties(0, 0, 0, 0, true);
    long t0 = 1_000_000L;
    ControllableTimeEngine engine = createTimedEngine(props, t0);
    GlobalWindowMemoryController controller = GlobalWindowMemoryController.getInstance();

    // Open window [0,1000) holding one row, charged to the data pool.
    push(engine, createDataSlice(new long[] {100L}), 0, 1, 1L);
    Assert.assertTrue(getEventsFromQueue(engine).isEmpty());

    // Exhaust the data pool, then push one more batch: the engine memory-pauses and buffers it.
    long held = controller.getDataTotalBytes() - controller.getDataUsedBytes();
    Assert.assertTrue(controller.tryReserveDataMemory(held));
    try {
      push(engine, createDataSlice(new long[] {150L}), 0, 1, 2L);
      Assert.assertTrue(engine.isMemoryPaused());

      // Ticks within the grace period must not force-close anything: a concurrent release
      // elsewhere may still lift the pause without any premature window close.
      engine.advanceTo(t0 + 1000);
      engine.tick();
      Assert.assertTrue(engine.isMemoryPaused());
      Assert.assertTrue(getEventsFromQueue(engine).isEmpty());
      Assert.assertFalse(engine.hasPendingRecomputeEvent());

      // Beyond the grace period: the engine must rescue itself by force-closing (and, unlike a
      // configured MAX_DELAY with allowedLateness > 0, destroying right away - retaining the
      // closed window would keep holding the very memory the rescue must return) its window.
      engine.advanceTo(t0 + 1000 + WindowEngine.DATA_POOL_PAUSE_RESCUE_GRACE_MS + 1);
      engine.tick();
      List<IEventInfo> events = awaitEvents(engine, 1);
      Assert.assertEquals(2, events.size());
      Assert.assertTrue(WindowEventType.WINDOW_CLOSE.equals(events.get(0).getEventType()));
      Assert.assertEquals(0L, events.get(0).getStartTime().getAsLong());
      Assert.assertEquals(1000L, events.get(0).getEndTime().getAsLong());
      Assert.assertTrue(WindowEventType.WINDOW_DESTROY.equals(events.get(1).getEventType()));

      // The rescue returned the window's data-pool charge; the released memory lets the
      // (asynchronous) resume drain the buffered batch and leave the pause.
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

  @Test
  public void testDataPoolExhaustionRescueDestroysClosedWindowWithLateness() throws Exception {
    // With ALLOWED_LATENESS > 0 but MAX_IDLE_WINDOW_RETAIN at its default 0, a
    // MAX_DELAY-force-closed window stays CLOSED (waiting for late data) and keeps
    // holding its data-pool charge until the task is dropped. The memory-pressure rescue must
    // destroy such cached windows first - cheaper than force-closing an open window, since their
    // close events are already emitted.
    StreamProperties props = new StreamProperties(0, 1000, 100, 0, true);
    long t0 = 1_000_000L;
    ControllableTimeEngine engine = createTimedEngine(props, t0);
    GlobalWindowMemoryController controller = GlobalWindowMemoryController.getInstance();

    // MAX_DELAY force-close: window [0,1000) enters CLOSED (lateness 1000, no MAX_IDLE destroy).
    push(engine, createDataSlice(new long[] {100L}), 0, 1, 1L);
    engine.advanceTo(t0 + 300);
    engine.tick();
    Assert.assertEquals(1, getEventsFromQueue(engine).size());
    Assert.assertFalse(engine.hasPendingRecomputeEvent());

    // Exhaust the data pool, then push one more batch: the engine memory-pauses and buffers it.
    long held = controller.getDataTotalBytes() - controller.getDataUsedBytes();
    Assert.assertTrue(controller.tryReserveDataMemory(held));
    try {
      push(engine, createDataSlice(new long[] {150L}), 0, 1, 2L);
      Assert.assertTrue(engine.isMemoryPaused());

      // First paused tick arms the grace timer; no premature destroy yet.
      engine.advanceTo(t0 + 400);
      engine.tick();
      Assert.assertTrue(engine.isMemoryPaused());
      Assert.assertFalse(engine.hasPendingRecomputeEvent());

      // Beyond the grace period: the CLOSED window is destroyed (clean here: its destroy marker
      // is compute-free) and its data-pool charge returned.
      engine.advanceTo(t0 + 400 + WindowEngine.DATA_POOL_PAUSE_RESCUE_GRACE_MS + 1);
      engine.tick();
      List<IEventInfo> recomputeEvents = awaitRecomputeEvents(engine, 1);
      Assert.assertEquals(1, recomputeEvents.size());
      Assert.assertTrue(
          WindowEventType.WINDOW_DESTROY.equals(recomputeEvents.get(0).getEventType()));
    } finally {
      controller.releaseDataMemory(held);
      engine.drop();
    }
  }

  @Test
  public void testDataPoolExhaustionPauseWarnsOncePerEpisode() throws Exception {
    // While the data pool stays exhausted, every tick's resume retry must not clear the pause
    // flag and re-enter it through the buffered head batch's failed headroom probe - doing so
    // fired the exhaustion WARN (and the memoryPause metric) once per retry (~20 per second at
    // TICK_INTERVAL_MS) instead of once per pause entry. A retry that cannot make progress must
    // keep the pause flag set, so the WARN and the metric count pause episodes, not retries.
    StreamProperties props = new StreamProperties(0, 0, 0, 0, true);
    long t0 = 1_000_000L;
    ControllableTimeEngine engine = createTimedEngine(props, t0);
    GlobalWindowMemoryController controller = GlobalWindowMemoryController.getInstance();

    Logger windowEngineLogger =
        (Logger)
            LoggerFactory.getLogger(org.apache.iotdb.streamnode.engine.window.WindowEngine.class);
    ListAppender<ILoggingEvent> appender = new ListAppender<>();
    appender.start();
    windowEngineLogger.addAppender(appender);
    try {
      push(engine, createDataSlice(new long[] {100L}), 0, 1, 1L);

      // Exhaust the data pool and pause the engine on the next batch: one WARN (pause entry).
      long held = controller.getDataTotalBytes() - controller.getDataUsedBytes();
      Assert.assertTrue(controller.tryReserveDataMemory(held));
      try {
        push(engine, createDataSlice(new long[] {150L}), 0, 1, 2L);
        Assert.assertTrue(engine.isMemoryPaused());

        // Sustained exhaustion: several resume-retry ticks all fail on the same head batch. None
        // may re-fire the WARN - it is still the same pause episode (and no release happened).
        for (int i = 1; i <= 5; i++) {
          engine.advanceTo(t0 + 100L * i);
          engine.tick();
          Assert.assertTrue(engine.isMemoryPaused());
        }
        long pauseWarnCount =
            new ArrayList<>(appender.list)
                .stream()
                    .filter(event -> event.getLevel() == Level.WARN)
                    .filter(
                        event ->
                            StreamNodeMessages.LOG_MEMORY_PAUSED_BY_POOL_EXHAUSTION_8BB73B2A.equals(
                                event.getMessage()))
                    .count();
        Assert.assertEquals(
            "one pause WARN per pause episode, not per resume retry", 1, pauseWarnCount);
      } finally {
        controller.releaseDataMemory(held);
        engine.drop();
      }
    } finally {
      windowEngineLogger.detachAppender(appender);
      appender.stop();
    }
  }

  @Test
  public void testNoRowsPlaceholderCachesOnlyTimeColumn() {
    // containsRowsPlaceholder=false (default): even though the window declares [time, value] scan
    // columns, the calc plan never reads ${rows}, so the store caches only the time column - the
    // close event carries a single timestamp column and correct rowCount/commitIds.
    StreamProperties props = new StreamProperties(0, 0, 0, 0, true);
    TumbleWindow window = new TumbleWindow(WINDOW_SIZE_MS, ORIGIN_MS);
    window.setEventInputColumns(Arrays.asList("time", "value"));
    TumbleWindowEngine engine = new TumbleWindowEngine(window, createDriverContext(), props, null);

    push(engine, createDataSlice(new long[] {100L, 500L}, new long[] {10L, 20L}), 0, 2, 1L);
    push(engine, createDataSlice(new long[] {1001L}, new long[] {30L}), 0, 1, 2L);
    List<IEventInfo> events = awaitEvents(engine, 1);
    Assert.assertEquals(2, events.size());
    IEventInfo event = events.get(0);
    Assert.assertTrue(WindowEventType.WINDOW_CLOSE.equals(event.getEventType()));
    Assert.assertEquals(0L, event.getStartTime().getAsLong());
    Assert.assertEquals(2, event.getRowCount().getAsLong());

    // The event carries only the time column: column 0 holds the event timestamps, and there is
    // no value column - the values (10, 20) were never cached.
    TsBlock block = event.getRowsIterator().next();
    Assert.assertEquals(2, block.getPositionCount());
    Assert.assertEquals(1, block.getValueColumnCount());
    Assert.assertEquals(100L, block.getColumn(0).getLong(0));
    Assert.assertEquals(500L, block.getColumn(0).getLong(1));
  }

  @Test
  public void testRecomputeEventPrevTimeMatchesCloseEvent() {
    // A recompute event can fire long after later windows were emitted (here W2 [5000,6000) with
    // allowedLateness=5500 keeps W1 cached until wm 6500). Its prev_time must match what W1's own
    // close event carried - NULL for the first window - not lastEmittedStart at destroy time
    // (5000, which points after the recomputed window's own end) and not the structural
    // predecessor (-1000, a window that never existed).
    StreamProperties props = new StreamProperties(0, 5500, 0, 0, true);
    TumbleWindowEngine engine = createEngineWithColumns(props, Arrays.asList("time", "value"));

    // W1 [0,1000) seeded; row 5500 opens W2 [5000,6000) and (wm 5500) closes W1.
    push(engine, createDataSlice(new long[] {100L}, new long[] {10L}), 0, 1, 1L);
    push(engine, createDataSlice(new long[] {5500L}, new long[] {20L}), 0, 1, 2L);
    Assert.assertEquals(1, awaitEvents(engine, 1).size());
    // Row 6000 (wm 6000) closes W2; W1 stays cached (delayLine 6500).
    push(engine, createDataSlice(new long[] {6000L}, new long[] {30L}), 0, 1, 3L);
    List<IEventInfo> closeEvents = awaitEvents(engine, 1);
    Assert.assertEquals(1, closeEvents.size());
    Assert.assertEquals(5000L, closeEvents.get(0).getStartTime().getAsLong());
    // prev_time of W2's close event = the most recently emitted window's start = W1's start.
    Assert.assertTrue(closeEvents.get(0).getPrevTime().isPresent());
    Assert.assertEquals(0L, closeEvents.get(0).getPrevTime().getAsLong());

    // Late data dirties cached W1; wm 6501 >= delayLine 6500 destroys it -> recompute event.
    push(engine, createDataSlice(new long[] {100L}, new long[] {99L}), 0, 1, 4L);
    push(engine, createDataSlice(new long[] {6501L}, new long[] {40L}), 0, 1, 5L);
    List<IEventInfo> recomputeEvents = awaitRecomputeEvents(engine, 1);
    Assert.assertEquals(1, recomputeEvents.size());
    IEventInfo recomputeEvent = recomputeEvents.get(0);
    Assert.assertTrue(WindowEventType.WINDOW_DESTROY.equals(recomputeEvent.getEventType()));
    Assert.assertEquals(0L, recomputeEvent.getStartTime().getAsLong());
    // W1 is the first window: its close event carried prev_time NULL, and the recompute that
    // revises it must carry the same NULL - not the structural predecessor [-1000,0).
    Assert.assertFalse(recomputeEvent.getPrevTime().isPresent());
  }

  @Test
  public void testRecomputeEventPrevTimeReusesCloseEventValueAcrossSkippedWindow() {
    // W2 [1000,2000) gets no data and is never created, so W3's close event carries prev_time =
    // W1's start (0) - the most recently EMITTED window. The dirty destroy of W3 must reuse that
    // same value; the structural predecessor (1000) would name a window that never existed.
    StreamProperties props = new StreamProperties(0, 5500, 0, 0, true);
    TumbleWindowEngine engine = createEngineWithColumns(props, Arrays.asList("time", "value"));

    // W1 [0,1000) and W3 [2000,3000) seeded; row 3500 (W4) closes both (wm 3500).
    push(engine, createDataSlice(new long[] {100L}, new long[] {10L}), 0, 1, 1L);
    push(engine, createDataSlice(new long[] {2500L}, new long[] {20L}), 0, 1, 2L);
    push(engine, createDataSlice(new long[] {3500L}, new long[] {30L}), 0, 1, 3L);
    List<IEventInfo> closeEvents = awaitEvents(engine, 2);
    Assert.assertEquals(2, closeEvents.size());
    Assert.assertEquals(0L, closeEvents.get(0).getStartTime().getAsLong());
    Assert.assertFalse(closeEvents.get(0).getPrevTime().isPresent());
    Assert.assertEquals(2000L, closeEvents.get(1).getStartTime().getAsLong());
    // prev_time of W3's close = W1's start: W2 was empty and never emitted.
    Assert.assertTrue(closeEvents.get(1).getPrevTime().isPresent());
    Assert.assertEquals(0L, closeEvents.get(1).getPrevTime().getAsLong());

    // wm 6501 destroys the cached W1 cleanly (compute-free marker on the recompute queue).
    push(engine, createDataSlice(new long[] {6501L}, new long[] {40L}), 0, 1, 4L);
    List<IEventInfo> markers = awaitRecomputeEvents(engine, 1);
    Assert.assertEquals(1, markers.size());
    Assert.assertEquals(0L, markers.get(0).getStartTime().getAsLong());

    // Late data dirties the cached W3; wm 8501 >= delayLine 8500 destroys it -> recompute event
    // whose prev_time is W3's own close-event value (0), not the structural predecessor (1000).
    push(engine, createDataSlice(new long[] {2500L}, new long[] {99L}), 0, 1, 5L);
    push(engine, createDataSlice(new long[] {8501L}, new long[] {50L}), 0, 1, 6L);
    List<IEventInfo> recomputeEvents = awaitRecomputeEvents(engine, 1);
    Assert.assertEquals(1, recomputeEvents.size());
    IEventInfo recomputeEvent = recomputeEvents.get(0);
    Assert.assertEquals(2000L, recomputeEvent.getStartTime().getAsLong());
    Assert.assertTrue(recomputeEvent.getPrevTime().isPresent());
    Assert.assertEquals(0L, recomputeEvent.getPrevTime().getAsLong());
  }

  @Test
  public void testMaxIdleRetainForceDestroysDirtyWindowWithRecompute() {
    // allowedLateness=1000, maxIdleWindowRetainMs=100: a cached window whose idle-retain deadline
    // passes is force-destroyed by onTick; if late data made it dirty, a full-recompute event is
    // emitted before destruction. Ticks are driven explicitly (by the shared scheduler in
    // production).
    StreamProperties props = new StreamProperties(0, 1000, 0, 100, true);
    long t0 = 1_000_000L;
    ControllableTimeEngine engine = createTimedEngine(props, t0);

    push(engine, createDataSlice(new long[] {100L}), 0, 1, 1L);
    push(engine, createDataSlice(new long[] {1001L}), 0, 1, 2L); // close [0,1000); cached
    List<IEventInfo> closeEvents = awaitEvents(engine, 1);
    Assert.assertEquals(1, closeEvents.size());
    Assert.assertTrue(WindowEventType.WINDOW_CLOSE.equals(closeEvents.get(0).getEventType()));

    // Late data to the cached window: sets dirty and refreshes the destroy deadline
    // (destroySystemTime = t0+50+100).
    engine.advanceTo(t0 + 50);
    push(engine, createDataSlice(new long[] {100L}), 0, 1, 3L);

    engine.advanceTo(t0 + 300); // past the (refreshed) destroy deadline
    engine.tick();
    List<IEventInfo> events = awaitRecomputeEvents(engine, 1);
    Assert.assertEquals(1, events.size());
    Assert.assertTrue(
        WindowEventType.WINDOW_DESTROY.equals(
            events.get(0).getEventType())); // recompute event for the dirty cached window
    Assert.assertEquals(0L, events.get(0).getStartTime().getAsLong());
  }

  @Test
  public void testNewMaxRowIntoForceClosedWindowDefersDestroyAndRecomputes() {
    // Doc MAX_DELAY_WINDOW_CLOSE / MAX_IDLE_WINDOW_RETAIN example table, data 4: after a
    // MAX_DELAY force-close the watermark still sits below windowEnd, so an in-order row (a NEW
    // MAXIMUM event time) landing in the force-closed window arrives late relative to its
    // already-emitted close event. It must join the full-recompute list (dirty) and defer the
    // idle-retain destroy; the destroy then emits a full recompute carrying the row. Scaled:
    // window [0,1000), watermark=200, allowedLateness=500, maxDelayWindowCloseMs=100,
    // maxIdleWindowRetainMs=50; system time is advanced explicitly, ticks are manual.
    StreamProperties props = new StreamProperties(200, 500, 100, 50, true);
    long t0 = 1_000_000L;
    TumbleWindow window = new TumbleWindow(WINDOW_SIZE_MS, ORIGIN_MS);
    window.setEventInputColumns(Arrays.asList("time", "value"));
    window.setContainsRowsPlaceholder(true);
    ControllableTimeEngine engine =
        new ControllableTimeEngine(window, createDriverContext(), props, t0);

    // Doc data 1 + 2 (10:00:50 and 10:03:00): open W1, maxEventTime=300, watermark=100 - still
    // below windowEnd, so the window stays open.
    push(engine, createDataSlice(new long[] {100L}, new long[] {10L}), 0, 1, 1L);
    push(engine, createDataSlice(new long[] {300L}, new long[] {20L}), 0, 1, 2L);
    Assert.assertTrue(getEventsFromQueue(engine).isEmpty());

    // Doc row at 00:40:00: no data arrived since 00:35:00, watermark 100 < windowEnd 1000, so the
    // system-time deadline (t0+100) force-closes W1. allowedLateness>0: the window is cached,
    // destroySystemTime = (t0+150) + 50 = t0+200.
    engine.advanceTo(t0 + 150);
    engine.tick();
    List<IEventInfo> closeEvents = awaitEvents(engine, 1);
    Assert.assertEquals(1, closeEvents.size());
    Assert.assertTrue(WindowEventType.WINDOW_CLOSE.equals(closeEvents.get(0).getEventType()));
    Assert.assertEquals(0L, closeEvents.get(0).getStartTime().getAsLong());

    // Doc data 4 at 00:41:00 (10:05:00): a new-maximum event time row in the force-closed window.
    // Expected: the watermark advances to 300 and the row joins the full-recompute list (dirty),
    // with destroySystemTime refreshed to (t0+160) + 50 = t0+210.
    engine.advanceTo(t0 + 160);
    push(engine, createDataSlice(new long[] {500L}, new long[] {60L}), 0, 1, 3L);

    // Past the NOT-refreshed deadline (t0+200) but before the refreshed one (t0+210): the window
    // must still be cached - no destroy event yet.
    engine.advanceTo(t0 + 205);
    engine.tick();
    Assert.assertTrue(getRecomputeEventsFromQueue(engine).isEmpty());

    // The refreshed idle-retain deadline passes: the dirty window is force-destroyed with a
    // full-recompute event carrying all rows, including the late new-maximum row 500.
    engine.advanceTo(t0 + 210);
    engine.tick();
    List<IEventInfo> recomputeEvents = awaitRecomputeEvents(engine, 1);
    Assert.assertEquals(1, recomputeEvents.size());
    IEventInfo recomputeEvent = recomputeEvents.get(0);
    Assert.assertTrue(WindowEventType.WINDOW_DESTROY.equals(recomputeEvent.getEventType()));
    Assert.assertTrue(recomputeEvent.needsCompute());
    Assert.assertEquals(0L, recomputeEvent.getStartTime().getAsLong());
    List<long[]> pairs = readTimestampValuePairs(recomputeEvent);
    Assert.assertEquals(3, pairs.size());
    Assert.assertArrayEquals(new long[] {100L, 10L}, pairs.get(0));
    Assert.assertArrayEquals(new long[] {300L, 20L}, pairs.get(1));
    Assert.assertArrayEquals(new long[] {500L, 60L}, pairs.get(2));
  }

  @Test
  public void testDuplicateTimestampsMergeIntoOneCountedRow() {
    // Same-timestamp rows merge by column in the row store (a non-null duplicate value wins): the
    // merged row is counted and delivered ONCE - the close event's row count stays accurate under
    // duplicate timestamps. The event's rowCount is index-based (addRows), so no dedup is needed.
    StreamProperties props = new StreamProperties(0, 0, 0, 0, true);
    TumbleWindowEngine engine = createEngineWithColumns(props, Arrays.asList("time", "value"));

    // ts=100 twice (the second, non-null value 99 wins the merge), then 1001 closes [0,1000).
    push(engine, createDataSlice(new long[] {100L}, new long[] {10L}), 0, 1, 1L);
    push(engine, createDataSlice(new long[] {100L}, new long[] {99L}), 0, 1, 2L);
    push(engine, createDataSlice(new long[] {1001L}, new long[] {20L}), 0, 1, 3L);
    List<IEventInfo> events = awaitEvents(engine, 1);
    Assert.assertEquals(2, events.size());
    IEventInfo closeEvent = events.get(0);
    Assert.assertTrue(WindowEventType.WINDOW_CLOSE.equals(closeEvent.getEventType()));
    Assert.assertEquals(1L, closeEvent.getRowCount().getAsLong());
    List<long[]> pairs = readTimestampValuePairs(closeEvent);
    Assert.assertEquals(1, pairs.size());
    Assert.assertArrayEquals(new long[] {100L, 99L}, pairs.get(0));
  }

  @Test
  public void testLeftClosedExpiredLineKeepsNextWindowBoundaryRow() {
    // Left-closed: destroying [0,1000) must expire only [0,1000); a row at exactly 1000 belongs to
    // the still-open [1000,2000) and must NOT be dropped by the expired line (which stops at 999).
    StreamProperties props = new StreamProperties(0, 500, 0, 0, true);
    TumbleWindowEngine engine = createEngineWithColumns(props, Arrays.asList("time", "value"));

    push(engine, createDataSlice(new long[] {100L}, new long[] {10L}), 0, 1, 1L);
    // 1001 closes [0,1000) (wm 1001 >= 1000); cached because lateness=500 (delayLine 1500).
    push(engine, createDataSlice(new long[] {1001L}, new long[] {20L}), 0, 1, 2L);
    // only one close event here
    List<IEventInfo> events1 = awaitEvents(engine, 1);
    Assert.assertEquals(1, events1.size());
    Assert.assertEquals(0L, events1.get(0).getStartTime().getAsLong());
    Assert.assertEquals(1000L, events1.get(0).getEndTime().getAsLong());
    Assert.assertTrue(WindowEventType.WINDOW_CLOSE.equals(events1.get(0).getEventType()));
    // 1601 destroys [0,1000) (wm 1601 >= 1500): expiredLine = 999.
    push(engine, createDataSlice(new long[] {1601L}, new long[] {30L}), 0, 1, 3L);

    // A row at exactly 1000 belongs to the still-open [1000,2000) and must survive.
    push(engine, createDataSlice(new long[] {1000L}, new long[] {99L}), 0, 1, 4L);
    // 2001 closes [1000,2000) carrying 1000, 1001 and 1601 (1601 also lives in this window). The
    // helper polls the compute queue first, so the close event is collected before the clean
    // destroy marker of [0,1000) (queued at 1601 on the recompute queue).
    push(engine, createDataSlice(new long[] {2001L}, new long[] {40L}), 0, 1, 5L);
    List<IEventInfo> events = awaitEvents(engine, 2);
    Assert.assertEquals(2, events.size());
    IEventInfo event = events.get(0);
    Assert.assertTrue(WindowEventType.WINDOW_CLOSE.equals(event.getEventType()));
    Assert.assertEquals(1000L, event.getStartTime().getAsLong());
    Assert.assertEquals(2000L, event.getEndTime().getAsLong());
    List<long[]> pairs = readTimestampValuePairs(event);
    Assert.assertEquals(3, pairs.size());
    Assert.assertArrayEquals(new long[] {1000L, 99L}, pairs.get(0));
    Assert.assertArrayEquals(new long[] {1001L, 20L}, pairs.get(1));
    Assert.assertArrayEquals(new long[] {1601L, 30L}, pairs.get(2));
    // The destroy marker of [0,1000) carries no rows and needs no compute.
    IEventInfo marker = events.get(1);
    Assert.assertTrue(WindowEventType.WINDOW_DESTROY.equals(marker.getEventType()));
    Assert.assertFalse(marker.needsCompute());
  }

  @Test
  public void testLeftOpenExpiredLineExpiresWindowEndRow() {
    // Left-open: the destroyed window owns (0,1000], so the expired line goes through 1000 exactly
    // and a row at 1000 IS dropped (it belongs to the destroyed window, not the next one).
    StreamProperties props = new StreamProperties(0, 500, 0, 0, false);
    TumbleWindowEngine engine = createEngineWithColumns(props, Arrays.asList("time", "value"));

    push(engine, createDataSlice(new long[] {100L}, new long[] {10L}), 0, 1, 1L);
    // 1001 closes (0,1000] (wm 1001 > 1000); cached because lateness=500 (delayLine 1500).
    push(engine, createDataSlice(new long[] {1001L}, new long[] {20L}), 0, 1, 2L);
    Assert.assertEquals(1, awaitEvents(engine, 1).size());
    // 1601 destroys (0,1000] (wm 1601 > 1500): expiredLine = 1000.
    push(engine, createDataSlice(new long[] {1601L}, new long[] {30L}), 0, 1, 3L);

    // A row at exactly 1000 belongs to the DESTROYED (0,1000] and must be dropped.
    push(engine, createDataSlice(new long[] {1000L}, new long[] {99L}), 0, 1, 4L);
    // 2001 closes (1000,2000] carrying 1001 and 1601 (1000 was dropped; both live in this window).
    // The helper polls the compute queue first, so the close event is collected before the clean
    // destroy marker of (0,1000] (queued at 1601 on the recompute queue).
    push(engine, createDataSlice(new long[] {2001L}, new long[] {40L}), 0, 1, 5L);
    List<IEventInfo> events = awaitEvents(engine, 1);
    Assert.assertEquals(2, events.size());
    IEventInfo event = events.get(0);
    Assert.assertTrue(WindowEventType.WINDOW_CLOSE.equals(event.getEventType()));
    Assert.assertEquals(1000L, event.getStartTime().getAsLong());
    Assert.assertEquals(2000L, event.getEndTime().getAsLong());
    List<long[]> pairs = readTimestampValuePairs(event);
    Assert.assertEquals(2, pairs.size());
    Assert.assertArrayEquals(new long[] {1001L, 20L}, pairs.get(0));
    Assert.assertArrayEquals(new long[] {1601L, 30L}, pairs.get(1));
    // The destroy marker of (0,1000] carries no rows and needs no compute.
    IEventInfo marker = events.get(1);
    Assert.assertTrue(WindowEventType.WINDOW_DESTROY.equals(marker.getEventType()));
    Assert.assertFalse(marker.needsCompute());
  }

  // ===== Lazy TsBlock generation (WindowEventRowsIterator) tests =====

  @Test
  public void testClosedEventGeneratesMultipleTsBlocksAcrossNextCalls() {
    // One recorded emission batch larger than the TsBlock row cap is generated by next() one
    // block at a time (progress recorded between calls): next() returns null only after the
    // whole batch has been generated, and isFinished() only turns true then. No TsBlock exists
    // before the first next() call (materialization is lazy).
    int blockCap = WindowEngine.MAX_WINDOW_BLOCK_ROWS;
    StreamDriverContext driverContext = createDriverContext();
    TumbleWindow window = new TumbleWindow(100000, ORIGIN_MS);
    window.getLiteralPlaceholders().add(StreamLiteral.Type.ROW_NUM);
    window.setEventInputColumns(Collections.singletonList("time"));
    window.setContainsRowsPlaceholder(true);
    TumbleWindowEngine engine =
        new TumbleWindowEngine(window, driverContext, new StreamProperties(0, 0, 0, 0, true), null);

    // Full-compute mode: no pre-close incremental emission, so close carries ALL rows as ONE
    // recorded batch - 2 caps plus a remainder, generating three TsBlocks.
    int rowCount = 2 * blockCap + 453;
    long[] rows = new long[rowCount];
    for (int i = 0; i < rowCount; i++) {
      rows[i] = i + 1; // timestamps 1..rowCount, all inside [0,100000)
    }
    push(engine, createDataSlice(rows), 0, rowCount, 1L);
    Assert.assertTrue(getEventsFromQueue(engine).isEmpty());

    push(engine, createDataSlice(new long[] {100001L}), 0, 1, 2L);
    List<IEventInfo> events = awaitEvents(engine, 1);
    Assert.assertEquals(2, events.size());
    IEventInfo event = events.get(0);
    Assert.assertTrue(WindowEventType.WINDOW_CLOSE.equals(event.getEventType()));
    Assert.assertEquals(rowCount, event.getRowCount().getAsLong());

    IEventRowsIterator it = event.getRowsIterator();
    // The batch is recorded but not yet generated: the event is not finished until next() has
    // produced every block.
    Assert.assertFalse(it.isFinished());
    TsBlock block1 = it.next();
    Assert.assertEquals(blockCap, block1.getPositionCount());
    Assert.assertEquals(1L, block1.getColumn(0).getLong(0));
    Assert.assertEquals((long) blockCap, block1.getColumn(0).getLong(blockCap - 1));
    Assert.assertFalse(it.isFinished());

    TsBlock block2 = it.next();
    Assert.assertEquals(blockCap, block2.getPositionCount());
    Assert.assertEquals(blockCap + 1L, block2.getColumn(0).getLong(0));
    Assert.assertEquals(2L * blockCap, block2.getColumn(0).getLong(blockCap - 1));

    TsBlock block3 = it.next();
    Assert.assertEquals(rowCount - 2 * blockCap, block3.getPositionCount());
    Assert.assertEquals(2L * blockCap + 1, block3.getColumn(0).getLong(0));
    Assert.assertEquals(
        (long) rowCount, block3.getColumn(0).getLong(block3.getPositionCount() - 1));

    // The whole batch is generated: next() returns null and the event is finished.
    Assert.assertNull(it.next());
    Assert.assertTrue(it.isFinished());
  }

  @Test
  public void testIncrementalEventIteratesOnlyEmittedRange() {
    // The iterator is non-blocked only at emission points, and next() materializes only the rows
    // recorded at that point: the pre-close increment (rows below the watermark), then the close
    // remainder - never the whole window at once. Between emission points the open event blocks.
    StreamProperties props = new StreamProperties(0, 0, 0, 0, true);
    TumbleWindow window = new TumbleWindow(100000, ORIGIN_MS);
    window.setEventInputColumns(Collections.singletonList("time"));
    window.setContainsRowsPlaceholder(true);
    TumbleWindowEngine engine = new TumbleWindowEngine(window, createDriverContext(), props, null);

    // Seed the watermark at 99999 (window [0,100000) stays open), then 1000 out-of-order rows:
    // the threshold fires a pre-close incremental emission of exactly (computeLine, watermark).
    push(engine, createDataSlice(new long[] {99999L}), 0, 1, 1L);
    long[] late = new long[1000];
    for (int i = 0; i < 1000; i++) {
      late[i] = i + 1;
    }
    push(engine, createDataSlice(late), 0, 1000, 2L);
    List<IEventInfo> events = awaitEvents(engine, 1);
    Assert.assertEquals(1, events.size());
    IEventInfo event = events.get(0);
    Assert.assertFalse(WindowEventType.WINDOW_CLOSE.equals(event.getEventType()));

    IEventRowsIterator it = event.getRowsIterator();
    // The emission point unblocked the iterator: the increment is available for generation.
    Assert.assertTrue(it.isBlocked().isDone());
    Assert.assertFalse(it.isFinished());
    TsBlock increment = it.next();
    Assert.assertEquals(1000, increment.getPositionCount());
    // The increment covers exactly rows 1..1000; the watermark row (99999) is NOT part of it.
    Assert.assertEquals(1L, increment.getColumn(0).getLong(0));
    Assert.assertEquals(1000L, increment.getColumn(0).getLong(999));
    // The recorded batch is fully generated: next() returns null. The window is still open, so
    // the event is not finished and the iterator blocks again until the next emission point.
    Assert.assertNull(it.next());
    Assert.assertFalse(it.isFinished());
    Assert.assertFalse(it.isBlocked().isDone());

    // Closing the window records the final remainder: the un-emitted watermark row.
    push(engine, createDataSlice(new long[] {100001L}), 0, 1, 3L);
    Assert.assertTrue(WindowEventType.WINDOW_CLOSE.equals(event.getEventType()));
    Assert.assertTrue(it.isBlocked().isDone());
    TsBlock remainder = it.next();
    Assert.assertEquals(1, remainder.getPositionCount());
    Assert.assertEquals(99999L, remainder.getColumn(0).getLong(0));
    Assert.assertNull(it.next());
    Assert.assertTrue(it.isFinished());
  }

  // ===== block-cap truncation across the device-major order (generation cursor resume) =====

  /**
   * The block cap cutting a device's segment <b>mid-way</b> and the next block resuming strictly
   * after that device's last emitted row had no test coverage - every IT device holds 1 row, so
   * cuts always landed on device boundaries. Full-compute close: the 2100 rows of 3 interleaved
   * devices form ONE to-the-end range, cut twice by the 1024-row block cap.
   */
  @Test
  public void testBlockCapCutMidDeviceResumesAtNextRow() {
    TumbleWindow window = new TumbleWindow(100000, ORIGIN_MS);
    window.getLiteralPlaceholders().add(StreamLiteral.Type.ROW_NUM);
    window.setEventInputColumns(Arrays.asList("time", "value"));
    window.setEventInputColumnTypes(Arrays.asList(TSDataType.TIMESTAMP, TSDataType.INT64));
    window.setContainsRowsPlaceholder(true);
    TumbleWindowEngine engine =
        new TumbleWindowEngine(
            window, createDriverContext(), new StreamProperties(0, 0, 0, 0, true), null);

    int rowCount = 2100; // 700 rows per device, interleaved
    long[] timestamps = new long[rowCount];
    int[] deviceIndexes = new int[rowCount];
    for (int i = 0; i < rowCount; i++) {
      timestamps[i] = i + 1;
      deviceIndexes[i] = i % 3;
    }
    push(engine, createDeviceSlice(timestamps, deviceIndexes), 0, rowCount, 1L);

    // A row past the window end closes [0,100000): the close event carries ALL rows as one range.
    push(engine, createDataSlice(new long[] {100001L}), 0, 1, 2L);
    List<IEventInfo> events = awaitEvents(engine, 1);
    Assert.assertEquals(2, events.size());
    IEventInfo event = events.get(0);
    Assert.assertTrue(WindowEventType.WINDOW_CLOSE.equals(event.getEventType()));
    Assert.assertEquals(rowCount, event.getRowCount().getAsLong());

    IEventRowsIterator it = event.getRowsIterator();
    int blockCap = WindowEngine.MAX_WINDOW_BLOCK_ROWS;
    List<Long> delivered = new ArrayList<>();
    TsBlock block1 = it.next();
    Assert.assertEquals(blockCap, block1.getPositionCount());
    // Device 0 holds 700 rows, so the cap cuts device 1 mid-segment at its 324th row (ts 971).
    Assert.assertEquals(971L, block1.getColumn(0).getLong(blockCap - 1));
    delivered.addAll(readRowKeys(block1));
    TsBlock block2 = it.next();
    Assert.assertEquals(blockCap, block2.getPositionCount());
    // The resume is strictly after the cut row: device 1's next row (ts 974) opens the block.
    Assert.assertEquals(974L, block2.getColumn(0).getLong(0));
    delivered.addAll(readRowKeys(block2));
    TsBlock block3 = it.next();
    Assert.assertEquals(rowCount - 2 * blockCap, block3.getPositionCount());
    delivered.addAll(readRowKeys(block3));
    Assert.assertNull(it.next());
    Assert.assertTrue(it.isFinished());
    Assert.assertEquals(expectedDeviceMajor(timestamps, deviceIndexes), delivered);
    engine.drop();
  }

  /**
   * The cap landing exactly ON a device boundary (the cut device's last row fills the block): the
   * resume starts the next device fresh, with no mid-device resume state - the boundary branch of
   * the generation cursor.
   */
  @Test
  public void testBlockCapCutAtDeviceBoundaryStartsNextDeviceFresh() {
    TumbleWindow window = new TumbleWindow(100000, ORIGIN_MS);
    window.getLiteralPlaceholders().add(StreamLiteral.Type.ROW_NUM);
    window.setEventInputColumns(Arrays.asList("time", "value"));
    window.setEventInputColumnTypes(Arrays.asList(TSDataType.TIMESTAMP, TSDataType.INT64));
    window.setContainsRowsPlaceholder(true);
    TumbleWindowEngine engine =
        new TumbleWindowEngine(
            window, createDriverContext(), new StreamProperties(0, 0, 0, 0, true), null);

    // Device 0 holds exactly one block's worth of rows (ts 1..1024), device 1 the rest
    // (1025..1029).
    int rowCount = 1029;
    long[] timestamps = new long[rowCount];
    int[] deviceIndexes = new int[rowCount];
    for (int i = 0; i < rowCount; i++) {
      timestamps[i] = i + 1;
      deviceIndexes[i] = i < 1024 ? 0 : 1;
    }
    push(engine, createDeviceSlice(timestamps, deviceIndexes), 0, rowCount, 1L);
    push(engine, createDataSlice(new long[] {100001L}), 0, 1, 2L);
    List<IEventInfo> events = awaitEvents(engine, 1);
    Assert.assertEquals(2, events.size());
    IEventInfo event = events.get(0);
    Assert.assertTrue(WindowEventType.WINDOW_CLOSE.equals(event.getEventType()));
    Assert.assertEquals(rowCount, event.getRowCount().getAsLong());

    IEventRowsIterator it = event.getRowsIterator();
    int blockCap = WindowEngine.MAX_WINDOW_BLOCK_ROWS;
    List<Long> delivered = new ArrayList<>();
    TsBlock block1 = it.next();
    Assert.assertEquals(blockCap, block1.getPositionCount());
    // The cut device ends exactly with the block: its last row (device 0's ts 1024) fills it.
    Assert.assertEquals(1024L, block1.getColumn(0).getLong(blockCap - 1));
    delivered.addAll(readRowKeys(block1));
    TsBlock block2 = it.next();
    // Device 1 starts fresh: its first row (ts 1025) opens the block - no resume row is skipped.
    Assert.assertEquals(rowCount - blockCap, block2.getPositionCount());
    Assert.assertEquals(1025L, block2.getColumn(0).getLong(0));
    delivered.addAll(readRowKeys(block2));
    Assert.assertNull(it.next());
    Assert.assertTrue(it.isFinished());
    Assert.assertEquals(expectedDeviceMajor(timestamps, deviceIndexes), delivered);
    engine.drop();
  }

  /**
   * 1100 devices x 1 row, all at the SAME timestamp: the cap cuts inside the equal-timestamp run,
   * which spans devices in the device-major order. The old global-time-order cursor resumed
   * strictly past the run's timestamp and lost its tail; the device cursor must deliver all 1100
   * rows across the blocks.
   */
  @Test
  public void testBlockCapCutInsideEqualTimestampRunAcrossDevices() {
    TumbleWindow window = new TumbleWindow(100000, ORIGIN_MS);
    window.getLiteralPlaceholders().add(StreamLiteral.Type.ROW_NUM);
    window.setEventInputColumns(Arrays.asList("time", "value"));
    window.setEventInputColumnTypes(Arrays.asList(TSDataType.TIMESTAMP, TSDataType.INT64));
    window.setContainsRowsPlaceholder(true);
    TumbleWindowEngine engine =
        new TumbleWindowEngine(
            window, createDriverContext(), new StreamProperties(0, 0, 0, 0, true), null);

    int deviceCount = 1100;
    long[] timestamps = new long[deviceCount];
    int[] deviceIndexes = new int[deviceCount];
    for (int i = 0; i < deviceCount; i++) {
      timestamps[i] = 500L;
      deviceIndexes[i] = i;
    }
    push(engine, createDeviceSlice(timestamps, deviceIndexes), 0, deviceCount, 1L);
    push(engine, createDataSlice(new long[] {100001L}), 0, 1, 2L);
    List<IEventInfo> events = awaitEvents(engine, 1);
    Assert.assertEquals(2, events.size());
    IEventInfo event = events.get(0);
    Assert.assertTrue(WindowEventType.WINDOW_CLOSE.equals(event.getEventType()));
    Assert.assertEquals(deviceCount, event.getRowCount().getAsLong());

    IEventRowsIterator it = event.getRowsIterator();
    int blockCap = WindowEngine.MAX_WINDOW_BLOCK_ROWS;
    List<Long> delivered = new ArrayList<>();
    TsBlock block1 = it.next();
    Assert.assertEquals(blockCap, block1.getPositionCount());
    // The run's head: devices 0..1023, every row at the shared timestamp.
    Assert.assertEquals(500L, block1.getColumn(0).getLong(0));
    Assert.assertEquals(500L, block1.getColumn(0).getLong(blockCap - 1));
    delivered.addAll(readRowKeys(block1));
    TsBlock block2 = it.next();
    Assert.assertEquals(deviceCount - blockCap, block2.getPositionCount());
    // The run's tail survives the cut: the very next device's row (device 1024) opens the block.
    Assert.assertEquals(500L, block2.getColumn(0).getLong(0));
    delivered.addAll(readRowKeys(block2));
    Assert.assertNull(it.next());
    Assert.assertTrue(it.isFinished());
    Assert.assertEquals(expectedDeviceMajor(timestamps, deviceIndexes), delivered);
    engine.drop();
  }

  /**
   * Rows written between two blocks of one still-open recorded range. The first block cuts device 1
   * mid-segment; rows then pushed to the cursor device (above and below its resume row), to the
   * already-decided device 0, to the later device 2 and to a brand-new device must reach exactly
   * the documented destinations: above-resume and later/new-device rows join the remaining blocks;
   * below-resume and already-decided-device rows are skipped (late data, recovered by the
   * destroy-time recompute).
   */
  @Test
  public void testRowsWrittenBetweenBlocksOfAnOpenRange() {
    TumbleWindow window = new TumbleWindow(100000, ORIGIN_MS);
    window.setEventInputColumns(Arrays.asList("time", "value"));
    window.setEventInputColumnTypes(Arrays.asList(TSDataType.TIMESTAMP, TSDataType.INT64));
    window.setContainsRowsPlaceholder(true);
    // watermarkOption 2000 keeps the in-order stream's pending count below the 1000-row threshold
    // until the watermark jump: row 5000 drains rows 999..2999 at once (pending 2999), so one
    // emission covers the range (0, 3000) = 2999 rows - enough for mid-device cuts by the
    // 1024-row block cap, on a still-open event.
    TumbleWindowEngine engine =
        new TumbleWindowEngine(
            window, createDriverContext(), new StreamProperties(2000, 0, 0, 0, true), null);

    int rowCount = 2999; // ~1000 rows per device, interleaved
    long[] timestamps = new long[rowCount];
    int[] deviceIndexes = new int[rowCount];
    for (int i = 0; i < rowCount; i++) {
      timestamps[i] = i + 1;
      deviceIndexes[i] = i % 3;
    }
    push(engine, createDeviceSlice(timestamps, deviceIndexes), 0, rowCount, 1L);
    Assert.assertTrue(getEventsFromQueue(engine).isEmpty());
    // Row 5000 (device 0): the watermark jumps to 3000 and the threshold fires - the range
    // (0, 3000) becomes the open event's first recorded batch. Row 5000 is outside it.
    push(engine, createDeviceSlice(new long[] {5000L}, new int[] {0}), 0, 1, 2L);
    List<IEventInfo> events = awaitEvents(engine, 1);
    Assert.assertEquals(1, events.size());
    IEventInfo event = events.get(0);
    Assert.assertFalse(WindowEventType.WINDOW_CLOSE.equals(event.getEventType()));
    Assert.assertEquals(rowCount, event.getRowCount().getAsLong());

    IEventRowsIterator it = event.getRowsIterator();
    int blockCap = WindowEngine.MAX_WINDOW_BLOCK_ROWS;
    List<Long> delivered = new ArrayList<>();
    TsBlock block1 = it.next();
    Assert.assertEquals(blockCap, block1.getPositionCount());
    // Device 0 holds 1000 rows, so the cap cuts device 1 mid-segment at its 24th row (ts 71).
    Assert.assertEquals(71L, block1.getColumn(0).getLong(blockCap - 1));
    delivered.addAll(readRowKeys(block1));

    // Rows written between the blocks: 2001 to the cursor device ABOVE its resume row (71); 49 to
    // the cursor device BELOW it (device 1's own timestamps are 2 mod 3, so both are fresh rows);
    // 1500 to the already-decided device 0; 2500 to the later device 2; 2800 to a new device 3.
    push(
        engine,
        createDeviceSlice(new long[] {2001L, 49L, 1500L, 2500L, 2800L}, new int[] {1, 1, 0, 2, 3}),
        0,
        5,
        3L);

    // The remaining blocks deliver the above-resume row (2001), the later-device row (2500) and
    // the new device's row (2800); the below-resume row (49) and the decided device 0's row
    // (1500) never appear - 2999 + 3 rows, no loss, no duplication.
    TsBlock block;
    while ((block = it.next()) != null) {
      delivered.addAll(readRowKeys(block));
    }
    Assert.assertEquals(rowCount + 3, delivered.size());
    long[] deliveredTimestamps = new long[rowCount + 3];
    int[] deliveredDevices = new int[rowCount + 3];
    System.arraycopy(timestamps, 0, deliveredTimestamps, 0, rowCount);
    System.arraycopy(deviceIndexes, 0, deliveredDevices, 0, rowCount);
    deliveredTimestamps[rowCount] = 2001L;
    deliveredDevices[rowCount] = 1;
    deliveredTimestamps[rowCount + 1] = 2500L;
    deliveredDevices[rowCount + 1] = 2;
    deliveredTimestamps[rowCount + 2] = 2800L;
    deliveredDevices[rowCount + 2] = 3;
    Assert.assertEquals(expectedDeviceMajor(deliveredTimestamps, deliveredDevices), delivered);
    // The window is still open: the event is not finished (more ranges may still be recorded).
    Assert.assertFalse(it.isFinished());
    engine.drop();
  }

  /**
   * One table-model slice whose per-row device identity comes from a TAG column: row {@code i}
   * belongs to {@code device_<deviceIndexes[i]>} (derived per row via {@code
   * Tablet.getDeviceID(i)}), so the store's device-major order is the devices' first-appearance
   * order. The INT64 {@code value} column encodes the row's device, so content assertions can check
   * both the timestamp and the device a delivered row came from.
   */
  private DataSlice createDeviceSlice(long[] timestamps, int[] deviceIndexes) {
    // The scan column "time" resolves by NAME in the tablet schema (TIME_COLUMN_ID is the empty
    // string, so only that would map to the tablet's built-in timestamp array): like every other
    // helper here, the tablet carries an explicit "time" measurement column whose value IS the
    // row's timestamp - without it the column would upsert as null on every row.
    Tablet tablet =
        new Tablet(
            "test_table",
            Arrays.asList("device_id", "time", "value"),
            Arrays.asList(TSDataType.STRING, TSDataType.INT64, TSDataType.INT64),
            Arrays.asList(ColumnCategory.TAG, ColumnCategory.FIELD, ColumnCategory.FIELD),
            timestamps.length);
    for (int i = 0; i < timestamps.length; i++) {
      tablet.addTimestamp(i, timestamps[i]);
      tablet.addValue(i, 0, "device_" + deviceIndexes[i]);
      tablet.addValue(i, 1, timestamps[i]);
      tablet.addValue(i, 2, deviceIndexes[i] * 1_000_000L + timestamps[i]);
    }
    tablet.setRowSize(timestamps.length);
    return new DataSlice(null, tablet, 0, timestamps.length, -1L);
  }

  /** The block's rows as assertion keys: device index and timestamp packed into one value. */
  private static List<Long> readRowKeys(TsBlock block) {
    List<Long> keys = new ArrayList<>(block.getPositionCount());
    for (int i = 0; i < block.getPositionCount(); i++) {
      keys.add(rowKey(block.getColumn(0).getLong(i), block.getColumn(1).getLong(i)));
    }
    return keys;
  }

  /**
   * The expected device-major delivery of the given rows: devices in first-appearance order, each
   * device's rows in time order - exactly what a fully drained iterator must deliver (no loss, no
   * duplication, no reordering within a device).
   */
  private static List<Long> expectedDeviceMajor(long[] timestamps, int[] deviceIndexes) {
    Map<Integer, TreeMap<Long, Integer>> rowsByDevice = new LinkedHashMap<>();
    for (int i = 0; i < timestamps.length; i++) {
      rowsByDevice
          .computeIfAbsent(deviceIndexes[i], key -> new TreeMap<>())
          .put(timestamps[i], deviceIndexes[i]);
    }
    List<Long> expected = new ArrayList<>(timestamps.length);
    for (TreeMap<Long, Integer> deviceRows : rowsByDevice.values()) {
      for (Map.Entry<Long, Integer> row : deviceRows.entrySet()) {
        expected.add(row.getValue() * 10_000_000_000L + row.getKey());
      }
    }
    return expected;
  }

  /**
   * Packs one delivered row into its assertion key: the device (decoded from the value column's
   * encoding) and the timestamp. Must match {@link #expectedDeviceMajor}.
   */
  private static long rowKey(long timestamp, long value) {
    return value / 1_000_000L * 10_000_000_000L + timestamp;
  }

  /**
   * A slice whose every row is dropped by the expired-line check must still mark tablet-group-done;
   * otherwise {@link CommitTracker} keeps the remaining count &gt; 0 for that tablet and source
   * commit stalls forever.
   */
  @Test
  public void testLateDroppedTabletStillMarksGroupDone() {
    List<Long> committed = new ArrayList<>();
    CommitTracker tracker = new CommitTracker(committed::add);
    StreamDriverContext driverContext = createDriverContext();
    driverContext.getSubTaskContext().setCommitTracker(tracker);

    TumbleWindow window = new TumbleWindow(WINDOW_SIZE_MS, ORIGIN_MS);
    window.setEventInputColumns(Collections.singletonList("time"));
    TumbleWindowEngine engine = new TumbleWindowEngine(window, driverContext, null, null);

    tracker.registerCommitId(1L, 1);
    tracker.registerCommitId(2L, 1);
    tracker.registerCommitId(3L, 1);
    tracker.registerCommitId(4L, 1);

    // Tablet 1 opens [1000,2000).
    push(engine, createDataSlice(new long[] {1500L}), 0, 1, 1L);
    // Tablet 2 crosses the boundary: [1000,2000) closes and (lateness 0) is destroyed, advancing
    // the expired line to 1999; [2000,3000) opens.
    push(engine, createDataSlice(new long[] {2500L}), 0, 1, 2L);
    // Tablet 3's row is entirely expired (1500 <= 1999): the whole slice is dropped, but its
    // group-done must still fire. (There is no too-late check - without the expired line this row
    // would open a new window; the destroy above is what makes the drop happen.)
    push(engine, createDataSlice(new long[] {1500L}), 0, 1, 3L);
    // Tablet 4 closes and destroys [2000,3000), opening [3000,4000).
    push(engine, createDataSlice(new long[] {3500L}), 0, 1, 4L);

    // Flushing the destroy markers of [1000,2000) and [2000,3000) advances commit to 1, then 3.
    // Reaching 3 is only possible because the DROPPED tablet 3's group-done fired: with its
    // remaining count still at 1, the tablet prefix would pin commit at 2.
    flushDestroyMarkers(engine, tracker);
    Assert.assertEquals(Arrays.asList(1L, 3L), committed);

    // The last window follows: commit advances to 4.
    Assert.assertTrue(engine.forceClose().isDone());
    flushDestroyMarkers(engine, tracker);
    Assert.assertEquals(Arrays.asList(1L, 3L, 4L), committed);
  }

  /**
   * A batch that fails mid-processing must still report group-done for its not-yet-marked tablets.
   * The failing batch's rows are dropped by design (exception protection), so from the commit
   * tracker's perspective those tablets' scans are finished; with their group-done missing, the
   * tracker's consecutive-completion watermark stalls before the failed tablet forever and source
   * commit never advances again.
   */
  @Test
  public void testFailedBatchStillMarksRemainingTabletsGroupDone() {
    List<Long> committed = new ArrayList<>();
    CommitTracker tracker = new CommitTracker(committed::add);
    StreamDriverContext driverContext = createDriverContext();
    driverContext.getSubTaskContext().setCommitTracker(tracker);

    TumbleWindow window = new TumbleWindow(WINDOW_SIZE_MS, ORIGIN_MS);
    window.setEventInputColumns(Collections.singletonList("value"));
    window.setContainsRowsPlaceholder(true);
    TumbleWindowEngine engine = new TumbleWindowEngine(window, driverContext, null, null);

    tracker.registerCommitId(1L, 1);
    tracker.registerCommitId(2L, 1);
    tracker.registerCommitId(3L, 1);

    // One batch, two slices: tablet 1's INT64 "value" row is processed fine (its group-done fires
    // at the end of its slice), but tablet 2's "value" column is TEXT - a type conflicting with
    // the output types the first tablet established, so the row converter fails on its first row
    // (ClassCastException in readValue) and the rest of the batch is dropped.
    Assert.assertSame(
        Operator.NOT_BLOCKED,
        engine.push(
            Arrays.asList(
                new DataSlice(
                    null,
                    createDataSlice(new long[] {100L}, new long[] {1L}).getTablet(),
                    0,
                    1,
                    1L),
                textValueSlice(300L, "boom", 2L))));

    // Tablet 3 (INT64 again) opens [1000,2000) and closes [0,1000) (lateness 0: destroyed).
    push(engine, createDataSlice(new long[] {1500L}, new long[] {3L}), 0, 1, 3L);
    Assert.assertTrue(engine.forceClose().isDone());
    flushDestroyMarkers(engine, tracker);

    // Commit must advance past the failed tablet 2: flushing both destroy markers yields
    // [2 (window [0,1000) flushed: its tablet-1 allow releases, and the still-gating open window
    // [1000,2000) cascades the watermark to tablet 3 - 1), 3 (window [1000,2000) flushed: tablet
    // 3)]. Committing 2 includes the failed tablet 2's prefix; had its group-done been lost with
    // the dropped batch, the tablet prefix would pin commit at 1 forever.
    Assert.assertEquals(Arrays.asList(2L, 3L), committed);
  }

  /**
   * One one-row slice whose "value" column is {@link TSDataType#TEXT} - conflicting with the INT64
   * "value" established by an earlier tablet of the same engine.
   */
  private DataSlice textValueSlice(long timestamp, String value, long tabletId) {
    Tablet tablet =
        new Tablet(
            "root.test.device",
            Arrays.asList(
                new MeasurementSchema("time", TSDataType.INT64),
                new MeasurementSchema("value", TSDataType.TEXT)),
            1);
    tablet.getTimestamps()[0] = timestamp;
    tablet.addValue(0, "time", timestamp);
    tablet.addValue(0, "value", value);
    tablet.setRowSize(1);
    return new DataSlice(null, tablet, 0, 1, tabletId);
  }
}
