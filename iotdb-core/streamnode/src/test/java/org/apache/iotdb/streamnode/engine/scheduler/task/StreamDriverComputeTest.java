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

package org.apache.iotdb.streamnode.engine.scheduler.task;

import org.apache.iotdb.commons.stream.ListPartitionKey;
import org.apache.iotdb.streamnode.engine.computation.IStreamComputeTask;
import org.apache.iotdb.streamnode.engine.computation.StreamComputeTaskContext;
import org.apache.iotdb.streamnode.engine.sink.IStreamSinkTask;
import org.apache.iotdb.streamnode.engine.task.StreamSubTask;
import org.apache.iotdb.streamnode.engine.task.StreamSubTaskContext;
import org.apache.iotdb.streamnode.engine.window.IEventInfo;
import org.apache.iotdb.streamnode.engine.window.WindowEngine;
import org.apache.iotdb.streamnode.utils.IEventRowsIterator;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.units.Duration;
import org.apache.tsfile.read.common.block.TsBlock;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Collections;
import java.util.OptionalLong;
import java.util.concurrent.TimeUnit;

import static org.apache.iotdb.calc.execution.operator.Operator.NOT_BLOCKED;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Verifies how {@link StreamDriver} interleaves the two compute tasks (current-progress window and
 * recompute window), including recompute priority, fallback, sink backpressure and the combination
 * of the two slots' blocking futures.
 */
public class StreamDriverComputeTest {

  @Mock private StreamSubTask mockSubTask;
  @Mock private StreamDriverContext mockDriverContext;
  @Mock private WindowEngine mockWindowEngine;
  @Mock private IStreamComputeTask mockComputeTask;
  @Mock private IStreamComputeTask mockRecomputeTask;
  @Mock private StreamComputeTaskContext mockComputeContext;
  @Mock private StreamComputeTaskContext mockRecomputeContext;
  @Mock private IStreamSinkTask mockSinkTask;
  @Mock private StreamSubTaskContext mockSubTaskContext;

  private StreamDriver driver;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    final DriverTaskId taskId =
        new DriverTaskId("test-stream", new ListPartitionKey(Collections.emptyList()));

    when(mockSubTask.getWindowEngine()).thenReturn(mockWindowEngine);
    when(mockSubTask.getComputeTask()).thenReturn(mockComputeTask);
    when(mockSubTask.getRecomputeTask()).thenReturn(mockRecomputeTask);
    when(mockSubTask.getSink()).thenReturn(mockSinkTask);
    when(mockComputeTask.getStreamComputeTaskContext()).thenReturn(mockComputeContext);
    when(mockRecomputeTask.getStreamComputeTaskContext()).thenReturn(mockRecomputeContext);
    when(mockDriverContext.getDriverTaskId()).thenReturn(taskId);
    when(mockDriverContext.isDone()).thenReturn(false);
    when(mockDriverContext.getSubTaskContext()).thenReturn(mockSubTaskContext);
    doReturn(NOT_BLOCKED).when(mockWindowEngine).getBlockedFuture();
    doReturn(NOT_BLOCKED).when(mockWindowEngine).getRecomputeBlockedFuture();
    doReturn(NOT_BLOCKED).when(mockComputeTask).isBlocked();
    doReturn(NOT_BLOCKED).when(mockRecomputeTask).isBlocked();

    driver = new StreamDriver(mockSubTask, mockDriverContext);
  }

  private static TsBlock nonEmptyBlock() {
    final TsBlock block = mock(TsBlock.class);
    when(block.isEmpty()).thenReturn(false);
    return block;
  }

  private static IEventInfo newEvent(final long windowId) {
    final IEventInfo event = mock(IEventInfo.class);
    when(event.getWindowId()).thenReturn(windowId);
    when(event.getRowsIterator()).thenReturn(mock(IEventRowsIterator.class));
    when(event.isEOF()).thenReturn(windowId >= 0);
    when(event.needsCompute()).thenReturn(true);
    when(event.getStartTime()).thenReturn(OptionalLong.of(0L));
    when(event.getEndTime()).thenReturn(OptionalLong.of(1L));
    return event;
  }

  private static IEventInfo newEvent() {
    return newEvent(-1L);
  }

  /** Same-window events must be prepared before the driver decides which task to run. */
  @Test
  public void testSameWindowComputeRunsBeforeRecompute() throws Exception {
    final IEventInfo recomputeEvent = newEvent(10L);
    final IEventInfo currentEvent = newEvent(10L);
    final TsBlock currentBlock = nonEmptyBlock();
    when(mockWindowEngine.pollRecomputeEvent()).thenReturn(recomputeEvent);
    when(mockWindowEngine.pollComputeEvent()).thenReturn(currentEvent);
    when(mockComputeTask.compute()).thenReturn(currentBlock);
    when(mockComputeTask.isFinished()).thenReturn(false);
    doReturn(NOT_BLOCKED).when(mockSinkTask).push(any(), anyLong(), anyBoolean());

    driver.processFor(new Duration(20, TimeUnit.MILLISECONDS));

    verify(mockComputeTask, atLeastOnce()).compute();
    verify(mockComputeTask, atLeastOnce()).bindEventInfo(currentEvent);
    verify(mockRecomputeTask, never()).bindEventInfo(any());
    verify(mockRecomputeTask, never()).compute();
  }

  /** While recompute can keep advancing, the current-progress task must never be touched. */
  @Test
  public void testRecomputePrioritySkipsCurrentWhileRecomputeCanProgress() throws Exception {
    final IEventInfo recomputeEvent = newEvent();
    final TsBlock resultBlock = nonEmptyBlock();
    when(mockWindowEngine.pollRecomputeEvent()).thenReturn(recomputeEvent);
    when(mockRecomputeTask.compute()).thenReturn(resultBlock);
    when(mockRecomputeTask.isFinished()).thenReturn(false);
    doReturn(NOT_BLOCKED).when(mockSinkTask).push(any(), anyLong(), anyBoolean());

    driver.processFor(new Duration(20, TimeUnit.MILLISECONDS));

    verify(mockRecomputeTask, atLeastOnce()).compute();
    verify(mockComputeTask, never()).compute();
  }

  /** After the recompute event finishes, the driver falls back to the current-progress task. */
  @Test
  public void testRecomputeFallsBackToCurrentAfterFinishing() throws Exception {
    final IEventInfo recomputeEvent = newEvent();
    final IEventInfo currentEvent = newEvent();
    final TsBlock recomputeBlock = nonEmptyBlock();
    final TsBlock currentBlock = nonEmptyBlock();
    when(mockWindowEngine.pollRecomputeEvent()).thenReturn(recomputeEvent, (IEventInfo) null);
    when(mockRecomputeTask.compute()).thenReturn(recomputeBlock);
    when(mockRecomputeTask.isFinished()).thenReturn(true);
    when(mockWindowEngine.pollComputeEvent()).thenReturn(currentEvent);
    when(mockComputeTask.compute()).thenReturn(currentBlock);
    when(mockComputeTask.isFinished()).thenReturn(false);
    doReturn(NOT_BLOCKED).when(mockSinkTask).push(any(), anyLong(), anyBoolean());

    driver.processFor(new Duration(20, TimeUnit.MILLISECONDS));

    verify(mockRecomputeTask).reset();
    verify(mockComputeTask, atLeastOnce()).compute();
  }

  /**
   * When the recompute operator tree is blocked, the current-progress task is processed instead.
   */
  @Test
  public void testRecomputeComputeBlockedFallsBackToCurrent() throws Exception {
    final IEventInfo recomputeEvent = newEvent();
    final IEventInfo currentEvent = newEvent();
    final TsBlock currentBlock = nonEmptyBlock();
    when(mockWindowEngine.pollRecomputeEvent()).thenReturn(recomputeEvent);
    doReturn(SettableFuture.<Void>create()).when(mockRecomputeTask).isBlocked();
    when(mockWindowEngine.pollComputeEvent()).thenReturn(currentEvent);
    when(mockComputeTask.compute()).thenReturn(currentBlock);
    when(mockComputeTask.isFinished()).thenReturn(false);
    doReturn(NOT_BLOCKED).when(mockSinkTask).push(any(), anyLong(), anyBoolean());

    driver.processFor(new Duration(20, TimeUnit.MILLISECONDS));

    verify(mockRecomputeTask, never()).compute();
    verify(mockComputeTask, atLeastOnce()).compute();
  }

  /**
   * Recompute compute blocked + current window empty: the driver blocks on a combined future that
   * wakes when the recompute compute future completes.
   */
  @Test
  public void testCombinedBlockingFutureWokenByRecompute() throws Exception {
    final IEventInfo recomputeEvent = newEvent();
    when(mockWindowEngine.pollRecomputeEvent()).thenReturn(recomputeEvent);
    final SettableFuture<Void> recomputeBlocked = SettableFuture.create();
    final SettableFuture<Void> currentBlocked = SettableFuture.create();
    doReturn(recomputeBlocked).when(mockRecomputeTask).isBlocked();
    doReturn(currentBlocked).when(mockWindowEngine).getBlockedFuture();

    final ListenableFuture<?> result = driver.processFor(new Duration(50, TimeUnit.MILLISECONDS));
    assertFalse(result.isDone());

    recomputeBlocked.set(null);
    assertTrue(result.isDone());
  }

  /**
   * Recompute compute blocked + current window empty: the driver blocks on a combined future that
   * wakes when the current window future completes.
   */
  @Test
  public void testCombinedBlockingFutureWokenByCurrent() throws Exception {
    final IEventInfo recomputeEvent = newEvent();
    when(mockWindowEngine.pollRecomputeEvent()).thenReturn(recomputeEvent);
    final SettableFuture<Void> recomputeBlocked = SettableFuture.create();
    final SettableFuture<Void> currentBlocked = SettableFuture.create();
    doReturn(recomputeBlocked).when(mockRecomputeTask).isBlocked();
    doReturn(currentBlocked).when(mockWindowEngine).getBlockedFuture();

    final ListenableFuture<?> result = driver.processFor(new Duration(50, TimeUnit.MILLISECONDS));
    assertFalse(result.isDone());

    currentBlocked.set(null);
    assertTrue(result.isDone());
  }

  /**
   * Recompute window empty + current compute blocked: the two blocking futures are combined the
   * same way, regardless of which slot produced them.
   */
  @Test
  public void testRecomputeWindowEmptyAndCurrentComputeBlockedCombinesFutures() throws Exception {
    final SettableFuture<Void> recomputeWindow = SettableFuture.create();
    final SettableFuture<Void> currentBlocked = SettableFuture.create();
    doReturn(recomputeWindow).when(mockWindowEngine).getRecomputeBlockedFuture();
    doReturn(currentBlocked).when(mockComputeTask).isBlocked();
    final IEventInfo currentEvent = newEvent();
    when(mockWindowEngine.pollComputeEvent()).thenReturn(currentEvent);

    final ListenableFuture<?> result = driver.processFor(new Duration(50, TimeUnit.MILLISECONDS));
    assertFalse(result.isDone());

    recomputeWindow.set(null);
    assertTrue(result.isDone());
  }

  /** Sink backpressure produced by recompute blocks the whole driver and skips current. */
  @Test
  public void testSinkBackpressureFromRecomputeReturnsSinkFutureAndSkipsCurrent() throws Exception {
    final IEventInfo recomputeEvent = newEvent();
    final TsBlock resultBlock = nonEmptyBlock();
    when(mockWindowEngine.pollRecomputeEvent()).thenReturn(recomputeEvent);
    when(mockRecomputeTask.compute()).thenReturn(resultBlock);
    when(mockRecomputeTask.isFinished()).thenReturn(false);
    final SettableFuture<Void> sinkFuture = SettableFuture.create();
    doReturn(sinkFuture).when(mockSinkTask).push(any(), anyLong(), anyBoolean());

    final ListenableFuture<?> result = driver.processFor(new Duration(50, TimeUnit.MILLISECONDS));
    assertFalse(result.isDone());
    verify(mockComputeTask, never()).compute();
  }

  /**
   * A finished recompute event pushes its commit ids, resets the task and clears the slot so the
   * next pass polls for another recompute event.
   */
  @Test
  public void testRecomputeFinishedPushesCommitIdsResetsAndClearsSlot() throws Exception {
    final TsBlock resultBlock = nonEmptyBlock();
    final IEventInfo recomputeEvent = newEvent(10L);
    when(mockRecomputeTask.compute()).thenReturn(resultBlock);
    when(mockRecomputeTask.isFinished()).thenReturn(true);
    when(mockWindowEngine.pollRecomputeEvent()).thenReturn(recomputeEvent, (IEventInfo) null);
    doReturn(NOT_BLOCKED).when(mockSinkTask).push(any(), anyLong(), anyBoolean());
    doReturn(SettableFuture.<Void>create()).when(mockWindowEngine).getBlockedFuture();

    driver.processFor(new Duration(50, TimeUnit.MILLISECONDS));

    verify(mockSinkTask).push(resultBlock, 10L, true);
    verify(mockRecomputeTask).reset();
    verify(mockWindowEngine, atLeast(2)).pollRecomputeEvent();
  }

  /**
   * A finished current-progress event pushes its window close ids, resets the task and clears the
   * slot so the next pass polls for another current event.
   */
  @Test
  public void testCurrentFinishedPushesCommitIdsResetsAndClearsSlot() throws Exception {
    final TsBlock resultBlock = nonEmptyBlock();
    final IEventInfo currentEvent = newEvent(10L);
    when(mockComputeTask.compute()).thenReturn(resultBlock);
    when(mockComputeTask.isFinished()).thenReturn(true);
    when(mockWindowEngine.pollComputeEvent()).thenReturn(currentEvent, (IEventInfo) null);
    doReturn(NOT_BLOCKED).when(mockSinkTask).push(any(), anyLong(), anyBoolean());
    // The first two calls report the window as ready (so the event is fetched, then polled again
    // after the slot is cleared); the third call blocks so the driver stops instead of spinning.
    doReturn(NOT_BLOCKED, NOT_BLOCKED, SettableFuture.<Void>create())
        .when(mockWindowEngine)
        .getBlockedFuture();

    driver.processFor(new Duration(50, TimeUnit.MILLISECONDS));

    verify(mockSinkTask).push(resultBlock, 10L, true);
    verify(mockComputeTask).reset();
    verify(mockWindowEngine, atLeast(2)).pollComputeEvent();
  }

  /**
   * A compute-free lifecycle event (e.g. WINDOW_DESTROY) must not reach the compute task: the
   * driver pushes it directly as an EOF so the sink can advance source commit after flush. The
   * decision is driven by the event's explicit compute flag, not by the absence of a rows iterator.
   */
  @Test
  public void testRowsLessEventSkipsComputeAndPushesEof() throws Exception {
    final IEventInfo destroyEvent = newEvent(10L);
    when(destroyEvent.needsCompute()).thenReturn(false);
    when(destroyEvent.getRowsIterator()).thenReturn(null);
    when(mockWindowEngine.pollComputeEvent()).thenReturn(destroyEvent, (IEventInfo) null);
    doReturn(NOT_BLOCKED).when(mockSinkTask).push(any(), anyLong(), anyBoolean());
    doReturn(NOT_BLOCKED, NOT_BLOCKED, SettableFuture.<Void>create())
        .when(mockWindowEngine)
        .getBlockedFuture();

    driver.processFor(new Duration(50, TimeUnit.MILLISECONDS));

    verify(mockSinkTask).push(null, 10L, true);
    verify(mockComputeTask, never()).bindEventInfo(any());
    verify(mockComputeTask, never()).compute();
  }

  /**
   * A destroy-typed recompute head (a clean marker here) must not be dispatched while its own
   * window still has compute events queued. With a compute backlog and a destroy order diverging
   * from the close order (per-window MAX_IDLE destroy), the recompute slot can hold window 10's
   * marker while window 10's close event is still queued behind window 5's - isSameWindow only
   * covers the heads-aligning case, and recompute priority would push the marker's EOF entry to the
   * sink, whose flush advances the source commit past the close result that has not been written
   * back yet (a crash in between loses those rows). The marker waits in its slot; the compute slot
   * keeps draining.
   */
  @Test
  public void testDestroyMarkerWaitsForItsWindowsQueuedCloseEvent() throws Exception {
    final IEventInfo marker = newEvent(10L);
    when(marker.needsCompute()).thenReturn(false);
    when(marker.getRowsIterator()).thenReturn(null);
    when(mockWindowEngine.pollRecomputeEvent()).thenReturn(marker, (IEventInfo) null);
    // Window 10's close event is still queued behind window 5's.
    when(mockWindowEngine.hasQueuedComputeEventsForWindow(10L)).thenReturn(true);
    final IEventInfo earlierClose = newEvent(5L);
    when(mockWindowEngine.pollComputeEvent()).thenReturn(earlierClose, (IEventInfo) null);
    final TsBlock block = nonEmptyBlock();
    when(mockComputeTask.compute()).thenReturn(block);
    when(mockComputeTask.isFinished()).thenReturn(true);
    doReturn(NOT_BLOCKED).when(mockSinkTask).push(any(), anyLong(), anyBoolean());

    driver.processFor(new Duration(50, TimeUnit.MILLISECONDS));

    // The earlier window's close result is pushed and the compute slot keeps draining...
    verify(mockSinkTask).push(block, 5L, true);
    verify(mockWindowEngine, atLeast(2)).pollComputeEvent();
    // ...but window 10's marker is NOT: its own close event is still queued, and flushing the
    // marker would release the window's commit gate over the unflushed close result.
    verify(mockSinkTask, never()).push(null, 10L, true);
  }

  /**
   * When the compute task finishes an event without draining its rows iterator (e.g. a plan without
   * ${rows} has no EventScanOperator), clearing the slot must release the event so its iterator
   * unpins the window's row store instead of leaking the store's data-pool charge.
   */
  @Test
  public void testFinishedEventReleasedWhenSlotClears() throws Exception {
    final IEventInfo currentEvent = newEvent(10L);
    final TsBlock resultBlock = nonEmptyBlock();
    when(mockWindowEngine.pollComputeEvent()).thenReturn(currentEvent, (IEventInfo) null);
    when(mockComputeTask.compute()).thenReturn(resultBlock);
    when(mockComputeTask.isFinished()).thenReturn(true);
    doReturn(NOT_BLOCKED).when(mockSinkTask).push(any(), anyLong(), anyBoolean());
    doReturn(NOT_BLOCKED, NOT_BLOCKED, SettableFuture.<Void>create())
        .when(mockWindowEngine)
        .getBlockedFuture();

    driver.processFor(new Duration(50, TimeUnit.MILLISECONDS));

    verify(mockComputeTask).reset();
    verify(currentEvent).release();
  }

  /**
   * Driver destruction drains the engine's event queues and releases every event (forceClose()
   * emitted final events nobody will consume) along with any event still held by a compute slot: an
   * unreleased event's iterator would pin its window's row store forever.
   */
  @Test
  public void testUnconsumedEventsReleasedOnDriverDestroy() throws Exception {
    final IEventInfo queuedEvent = newEvent(10L);
    when(mockWindowEngine.pollComputeEvent()).thenReturn(queuedEvent, (IEventInfo) null);

    driver.close();

    verify(mockWindowEngine).forceClose();
    verify(queuedEvent).release();
    verify(mockComputeTask).close();
    verify(mockRecomputeTask).close();
  }

  /**
   * A failed session scan surfaces as an exception thrown by the compute task's isBlocked() (the
   * shared TsBlock queue reports its abort): the driver must treat it as an event-level failure
   * (mark the event, reset the task, release and drop the window) instead of letting it abort the
   * whole sub task, and keep computing the next window.
   */
  @Test
  public void testComputeBlockedExceptionDropsWindowAndContinues() throws Exception {
    doReturn(SettableFuture.<Void>create()).when(mockWindowEngine).getRecomputeBlockedFuture();
    final IEventInfo failedEvent = newEvent(10L);
    when(failedEvent.getStartTime()).thenReturn(OptionalLong.of(1000L));
    when(failedEvent.getEndTime()).thenReturn(OptionalLong.of(2000L));
    final IEventInfo nextEvent = newEvent(20L);
    final TsBlock nextResult = nonEmptyBlock();
    when(mockWindowEngine.pollComputeEvent()).thenReturn(failedEvent, nextEvent, (IEventInfo) null);
    doThrow(new IllegalStateException("session scan aborted"))
        .doReturn(NOT_BLOCKED)
        .when(mockComputeTask)
        .isBlocked();
    when(mockComputeTask.compute()).thenReturn(nextResult);
    when(mockComputeTask.isFinished()).thenReturn(false);
    doReturn(NOT_BLOCKED).when(mockSinkTask).push(any(), anyLong(), anyBoolean());
    doReturn(NOT_BLOCKED, NOT_BLOCKED, SettableFuture.<Void>create())
        .when(mockWindowEngine)
        .getBlockedFuture();

    driver.processFor(new Duration(50, TimeUnit.MILLISECONDS));

    verify(mockComputeTask).reset();
    verify(failedEvent).handleComputeException(any());
    verify(failedEvent).release();
    // The next window is still bound and computed.
    verify(mockComputeTask, atLeastOnce()).bindEventInfo(nextEvent);
    verify(mockComputeTask, atLeastOnce()).compute();
  }

  /**
   * A session scan submission failure (bindEventInfo) must also be treated as an event-level
   * failure: the affected window is dropped but the driver keeps running for the next window.
   */
  @Test
  public void testBindEventExceptionDropsWindowAndContinues() throws Exception {
    doReturn(SettableFuture.<Void>create()).when(mockWindowEngine).getRecomputeBlockedFuture();
    final IEventInfo failedEvent = newEvent(10L);
    when(failedEvent.getStartTime()).thenReturn(OptionalLong.of(1000L));
    when(failedEvent.getEndTime()).thenReturn(OptionalLong.of(2000L));
    final IEventInfo nextEvent = newEvent(20L);
    final TsBlock nextResult = nonEmptyBlock();
    when(mockWindowEngine.pollComputeEvent()).thenReturn(failedEvent, nextEvent, (IEventInfo) null);
    doThrow(new RuntimeException("session scan service not started"))
        .doNothing()
        .when(mockComputeTask)
        .bindEventInfo(any());
    when(mockComputeTask.compute()).thenReturn(nextResult);
    when(mockComputeTask.isFinished()).thenReturn(false);
    doReturn(NOT_BLOCKED).when(mockSinkTask).push(any(), anyLong(), anyBoolean());
    doReturn(NOT_BLOCKED, NOT_BLOCKED, SettableFuture.<Void>create())
        .when(mockWindowEngine)
        .getBlockedFuture();

    driver.processFor(new Duration(50, TimeUnit.MILLISECONDS));

    verify(mockComputeTask).reset();
    verify(failedEvent).handleComputeException(any());
    verify(failedEvent).release();
    // The next window is still bound and computed.
    verify(mockComputeTask, atLeastOnce()).bindEventInfo(nextEvent);
    verify(mockComputeTask, atLeastOnce()).compute();
  }
}
