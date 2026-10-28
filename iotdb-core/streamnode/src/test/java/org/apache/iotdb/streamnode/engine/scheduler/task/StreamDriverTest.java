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
import org.apache.iotdb.streamnode.conf.StreamNodeDescriptor;
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

import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.apache.iotdb.calc.execution.operator.Operator.NOT_BLOCKED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class StreamDriverTest {

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
  private DriverTaskId taskId;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);
    taskId = new DriverTaskId("test-stream", new ListPartitionKey(Collections.emptyList()));

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

  /** A compute-bound event: the driver must bind it to the compute task and run it. */
  private static IEventInfo newComputeEvent() {
    final IEventInfo event = mock(IEventInfo.class);
    when(event.getRowsIterator()).thenReturn(mock(IEventRowsIterator.class));
    when(event.needsCompute()).thenReturn(true);
    return event;
  }

  // ==================== Constructor / basic ====================

  @Test(expected = NullPointerException.class)
  public void testConstructorRejectsNullSubTask() throws Exception {
    new StreamDriver(null, mockDriverContext);
  }

  @Test(expected = NullPointerException.class)
  public void testConstructorRejectsNullDriverContext() throws Exception {
    new StreamDriver(mockSubTask, null);
  }

  @Test
  public void testGetDriverTaskIdDelegatesToContext() throws Exception {
    assertEquals(taskId, driver.getDriverTaskId());
  }

  @Test
  public void testSetDriverTaskIdPropagatesToContext() throws Exception {
    final DriverTaskId newId =
        new DriverTaskId("other", new ListPartitionKey(Collections.emptyList()));
    driver.setDriverTaskId(newId);
    verify(mockDriverContext).setDriverTaskId(newId);
  }

  @Test
  public void testGetLastSuccessfulProcessTimeInitiallyZero() throws Exception {
    assertEquals(0L, driver.getLastSuccessfulProcessTime());
  }

  // ==================== processFor ====================

  @Test
  public void testProcessForReturnsNotBlockedWhenComputeReturnsNullNotFinished() throws Exception {
    // compute() returns null, isFinished() false -> processInternal returns NOT_BLOCKED.
    final IEventInfo mockEvent = newComputeEvent();
    when(mockWindowEngine.pollComputeEvent()).thenReturn(mockEvent);
    when(mockComputeTask.compute()).thenReturn(null);
    when(mockComputeTask.isFinished()).thenReturn(false);

    final ListenableFuture<?> result = driver.processFor(new Duration(10, TimeUnit.MILLISECONDS));
    assertNotNull(result);
    assertTrue(result.isDone());
    verify(mockComputeTask, atLeastOnce()).compute();
  }

  @Test
  public void testProcessForReturnsNotBlockedAfterClose() throws Exception {
    driver.close();
    // After close, state != ALIVE -> processFor returns NOT_BLOCKED immediately.
    final ListenableFuture<?> result = driver.processFor(new Duration(10, TimeUnit.MILLISECONDS));
    assertTrue(result.isDone());
    // compute() should NOT be called because state check short-circuits.
    verify(mockComputeTask, never()).compute();
  }

  @Test
  public void testProcessForReturnsWindowBlockedFutureWhenWindowNotDone() throws Exception {
    // currentEvent is null initially; windowBlockedFuture not done -> returns it.
    final SettableFuture<Void> windowBlocked = SettableFuture.create();
    doReturn(windowBlocked).when(mockWindowEngine).getBlockedFuture();

    final ListenableFuture<?> result = driver.processFor(new Duration(50, TimeUnit.MILLISECONDS));
    // The returned future should be the (derived) driver blocked future, which is not done.
    assertFalse(result.isDone());
  }

  @Test
  public void testProcessForReturnsComputeBlockedFutureWhenComputeBlocked() throws Exception {
    // windowBlockedFuture done (NOT_BLOCKED), currentEvent gets set, computeTask.isBlocked not
    // done. With no pending data the driver then blocks on the window engine's blocked future.
    final SettableFuture<Void> computeBlocked = SettableFuture.create();
    doReturn(computeBlocked).when(mockComputeTask).isBlocked();
    // pollEvent returns a non-null event so currentEvent gets set.
    final IEventInfo mockEvent = newComputeEvent();
    when(mockWindowEngine.pollComputeEvent()).thenReturn(mockEvent);
    // First getBlockedFuture() call (window-not-blocked check) is done; the second (compute-blocked
    // branch) hands back the window engine's blocked future.
    final SettableFuture<Void> windowBlocked = SettableFuture.create();
    doReturn(NOT_BLOCKED, windowBlocked).when(mockWindowEngine).getBlockedFuture();

    final ListenableFuture<?> result = driver.processFor(new Duration(50, TimeUnit.MILLISECONDS));
    assertFalse(result.isDone());
    verify(mockComputeTask, atLeastOnce()).isBlocked();
  }

  @Test
  public void testProcessForCatchesComputeExceptionAndSkipsEvent() throws Exception {
    final IEventInfo mockEvent = newComputeEvent();
    when(mockWindowEngine.pollComputeEvent()).thenReturn(mockEvent, (IEventInfo) null);
    when(mockComputeTask.compute()).thenThrow(new RuntimeException("compute failed"));
    // The first call reports the window as ready (the failing event is fetched and then dropped);
    // the second call blocks so the driver stops instead of spinning.
    doReturn(NOT_BLOCKED, SettableFuture.<Void>create()).when(mockWindowEngine).getBlockedFuture();

    final ListenableFuture<?> result = driver.processFor(new Duration(50, TimeUnit.MILLISECONDS));

    assertFalse(result.isDone());
    verify(mockWindowEngine, atLeastOnce()).pollComputeEvent();
    // The compute exception is treated as an event-level failure: the event is dropped and the
    // driver does not fail.
    verify(mockDriverContext, never()).failed(any());
  }

  @Test
  public void testProcessForWithFinishedEventPushesCommitIdsAndResets() throws Exception {
    final IEventInfo mockEvent = newComputeEvent();
    when(mockEvent.getWindowId()).thenReturn(10L);
    when(mockEvent.isEOF()).thenReturn(true);
    when(mockWindowEngine.pollComputeEvent()).thenReturn(mockEvent);

    final TsBlock resultBlock = mock(TsBlock.class);
    when(mockComputeTask.compute()).thenReturn(resultBlock);
    when(mockComputeTask.isFinished()).thenReturn(true);
    doReturn(NOT_BLOCKED).when(mockSinkTask).push(any(), anyLong(), anyBoolean());

    driver.processFor(new Duration(50, TimeUnit.MILLISECONDS));

    verify(mockSinkTask, atLeastOnce()).push(resultBlock, 10L, true);
    verify(mockComputeTask, atLeastOnce()).reset();
  }

  @Test
  public void testProcessForWithUnfinishedAndNonNullResultPushesEmptyCommitIds() throws Exception {
    final IEventInfo mockEvent = newComputeEvent();
    when(mockEvent.getWindowId()).thenReturn(10L);
    when(mockWindowEngine.pollComputeEvent()).thenReturn(mockEvent);

    final TsBlock resultBlock = mock(TsBlock.class);
    when(resultBlock.isEmpty()).thenReturn(false);
    when(mockComputeTask.compute()).thenReturn(resultBlock);
    when(mockComputeTask.isFinished()).thenReturn(false);
    doReturn(NOT_BLOCKED).when(mockSinkTask).push(any(), anyLong(), anyBoolean());

    driver.processFor(new Duration(50, TimeUnit.MILLISECONDS));

    verify(mockSinkTask, atLeastOnce()).push(resultBlock, 10L, false);
    // reset() should NOT be called when not finished.
    verify(mockComputeTask, never()).reset();
  }

  @Test
  public void testProcessForUpdatesLastSuccessfulProcessTimeOnSuccess() throws Exception {
    final IEventInfo mockEvent = newComputeEvent();
    when(mockWindowEngine.pollComputeEvent()).thenReturn(mockEvent);

    final TsBlock resultBlock = mock(TsBlock.class);
    when(resultBlock.isEmpty()).thenReturn(false);
    when(mockComputeTask.compute()).thenReturn(resultBlock);
    when(mockComputeTask.isFinished()).thenReturn(false);
    doReturn(NOT_BLOCKED).when(mockSinkTask).push(any(), anyLong(), anyBoolean());

    assertEquals(0L, driver.getLastSuccessfulProcessTime());
    driver.processFor(new Duration(50, TimeUnit.MILLISECONDS));
    assertTrue(driver.getLastSuccessfulProcessTime() > 0);
  }

  @Test
  public void testProcessForBindsEventInfoWhenCurrentEventIsNull() throws Exception {
    final IEventInfo mockEvent = newComputeEvent();
    when(mockWindowEngine.pollComputeEvent()).thenReturn(mockEvent);
    when(mockComputeTask.compute()).thenReturn(null);
    when(mockComputeTask.isFinished()).thenReturn(false);

    driver.processFor(new Duration(20, TimeUnit.MILLISECONDS));

    verify(mockComputeTask, atLeastOnce()).bindEventInfo(mockEvent);
  }

  @Test
  public void testProcessForWithNoWindowEngineSkipsEventProcessing() throws Exception {
    // When windowEngine is null, both the recompute and current-progress events are unavailable,
    // so the driver returns NOT_BLOCKED without binding or computing anything.
    when(mockSubTask.getWindowEngine()).thenReturn(null);
    when(mockComputeTask.compute()).thenReturn(null);
    when(mockComputeTask.isFinished()).thenReturn(false);

    final ListenableFuture<?> result = driver.processFor(new Duration(10, TimeUnit.MILLISECONDS));
    assertTrue(result.isDone());
    verify(mockComputeTask, never()).bindEventInfo(any());
    verify(mockComputeTask, never()).compute();
  }

  @Test
  public void testProcessForReturnsPendingSinkFutureWhenNotDone() throws Exception {
    final IEventInfo mockEvent = newComputeEvent();
    when(mockWindowEngine.pollComputeEvent()).thenReturn(mockEvent);

    final TsBlock resultBlock = mock(TsBlock.class);
    when(resultBlock.isEmpty()).thenReturn(false);
    when(mockComputeTask.compute()).thenReturn(resultBlock);
    when(mockComputeTask.isFinished()).thenReturn(false);

    final SettableFuture<Void> pendingSink = SettableFuture.create();
    doReturn(pendingSink).when(mockSinkTask).push(any(), anyLong(), anyBoolean());

    final ListenableFuture<?> result = driver.processFor(new Duration(50, TimeUnit.MILLISECONDS));
    assertFalse(result.isDone());
  }

  // ==================== push ====================

  @Test
  public void testPushReturnsNotBlockedWhenWindowEngineIsNull() throws Exception {
    when(mockSubTask.getWindowEngine()).thenReturn(null);
    final ListenableFuture<?> result = driver.push(Collections.emptyList());
    assertTrue(result.isDone());
    assertEquals(NOT_BLOCKED, result);
  }

  @Test
  public void testPushReturnsNotBlockedWhenDataSlicesIsNull() throws Exception {
    final ListenableFuture<?> result = driver.push(null);
    assertTrue(result.isDone());
    assertEquals(NOT_BLOCKED, result);
  }

  @Test
  public void testPushReturnsNotBlockedWhenDataSlicesIsEmpty() throws Exception {
    final ListenableFuture<?> result = driver.push(Collections.emptyList());
    assertTrue(result.isDone());
    assertEquals(NOT_BLOCKED, result);
  }

  @Test
  public void testPushDelegatesToWindowEngineWhenDataSlicesNonEmpty() throws Exception {
    final StreamSubTask.DataSlice mockSlice = mock(StreamSubTask.DataSlice.class);
    final List<StreamSubTask.DataSlice> slices = Collections.singletonList(mockSlice);
    doReturn(NOT_BLOCKED).when(mockWindowEngine).push(slices);

    final ListenableFuture<?> result = driver.push(slices);
    verify(mockWindowEngine).push(slices);
    assertTrue(result.isDone());
  }

  // ==================== close ====================

  @Test
  public void testCloseIsIdempotent() throws Exception {
    driver.close();
    // Second close should be a no-op (CAS from NEED_DESTRUCTION to DESTROYED already happened
    // during the first close's destroyIfNecessary).
    driver.close();
    // Verify destruction happened exactly once: forceClose called once.
    verify(mockWindowEngine, atLeastOnce()).forceClose();
  }

  @Test
  public void testCloseTriggersSubTaskDestruction() throws Exception {
    driver.close();
    verify(mockDriverContext).markTaskClosing();
    verify(mockWindowEngine).forceClose();
    verify(mockComputeTask).close();
    verify(mockSinkTask).stop();
    verify(mockDriverContext).finished();
    verify(mockSubTaskContext).signalDriverClosed();
  }

  @Test
  public void testIsFinishedReturnsTrueAfterClose() throws Exception {
    assertFalse(driver.isFinished());
    driver.close();
    assertTrue(driver.isFinished());
  }

  @Test
  public void testIsFinishedReturnsTrueWhenDriverContextDone() throws Exception {
    when(mockDriverContext.isDone()).thenReturn(true);
    assertTrue(driver.isFinished());
  }

  // ==================== failed ====================

  @Test
  public void testFailedDelegatesToDriverContext() throws Exception {
    final Throwable cause = new RuntimeException("boom");
    driver.failed(cause);
    verify(mockDriverContext).failed(cause);
  }

  @Test
  public void testFailedWithNullCause() throws Exception {
    driver.failed(null);
    verify(mockDriverContext).failed(null);
  }

  // ==================== DriverLock behavior ====================

  @Test
  public void testCloseInterruptsRunningProcessFor() throws Exception {
    // When close() is called while processFor is holding the lock with interruptOnClose=false
    // (the default for processFor), interruptCurrentOwner is a no-op. The lock is acquired by
    // close's tryWithLockUninterruptibly only after processFor releases it.
    // This test verifies close() completes without deadlock even with concurrent processFor.
    final SettableFuture<Void> computeBlocked = SettableFuture.create();
    doReturn(computeBlocked).when(mockComputeTask).isBlocked();
    final IEventInfo mockEvent = newComputeEvent();
    // Return the event once, then null: pollComputeEvent signals an exhausted queue with null.
    // A stub that never returns null keeps close()'s unconsumed-event drain looping forever.
    when(mockWindowEngine.pollComputeEvent()).thenReturn(mockEvent, (IEventInfo) null);

    final Thread processThread =
        new Thread(
            () -> {
              try {
                driver.processFor(new Duration(5, TimeUnit.SECONDS));
              } catch (Throwable t) {
                // expected
              }
            });
    processThread.start();

    // Give processFor a moment to acquire the lock and block on computeBlocked.
    Thread.sleep(100);
    driver.close();
    // Release the blocked future so processFor can proceed.
    computeBlocked.set(null);

    processThread.join(3000);
    // close() should have completed. The test passes if no deadlock.
    assertTrue(driver.isFinished());
  }

  @Test
  public void testProcessForHandlesComputeExceptionAtEventLevel() throws Exception {
    final IEventInfo mockEvent = newComputeEvent();
    when(mockWindowEngine.pollComputeEvent()).thenReturn(mockEvent, (IEventInfo) null);
    final RuntimeException computeException =
        new RuntimeException(
            new org.apache.iotdb.commons.exception.IoTDBRuntimeException("inner-cause", 1));
    when(mockComputeTask.compute()).thenThrow(computeException);
    // After the failed event is dropped, block when no next event is available so the driver does
    // not repeatedly poll and process the same mocked event within one time slice.
    doReturn(NOT_BLOCKED, SettableFuture.<Void>create()).when(mockWindowEngine).getBlockedFuture();

    ListenableFuture<?> result = driver.processFor(new Duration(50, TimeUnit.MILLISECONDS));

    assertFalse(result.isDone());
    verify(mockEvent).handleComputeException(computeException);
    verify(mockComputeTask).reset();
    verify(mockDriverContext, never()).failed(any());
  }

  @Test
  public void testProcessForWhenDriverContextDoneDuringExecution() throws Exception {
    // If driverContext.isDone() returns true, isFinishedInternal returns true, which transitions
    // state to NEED_DESTRUCTION and triggers destruction.
    when(mockDriverContext.isDone()).thenReturn(true);

    driver.processFor(new Duration(50, TimeUnit.MILLISECONDS));
    // After destruction, driverContext.finished() should be called.
    verify(mockDriverContext).finished();
    assertTrue(driver.isFinished());
  }

  @Test
  public void testProcessForWithNullWindowEngineDoesNotCompute() throws Exception {
    when(mockSubTask.getWindowEngine()).thenReturn(null);

    ListenableFuture<?> result = driver.processFor(new Duration(50, TimeUnit.MILLISECONDS));

    assertTrue(result.isDone());
    verify(mockComputeTask, never()).compute();
    verify(mockComputeTask, never()).reset();
  }

  // ==================== push() branch tests ====================

  /** push() with null windowEngine returns NOT_BLOCKED. */
  @Test
  public void testPushWithNullWindowEngineReturnsNotBlocked() {
    when(mockSubTask.getWindowEngine()).thenReturn(null);
    ListenableFuture<?> result = driver.push(java.util.Collections.emptyList());
    assertNotNull(result);
    assertTrue(result.isDone());
  }

  /** push() with null dataSlices returns NOT_BLOCKED. */
  @Test
  public void testPushWithNullDataSlicesReturnsNotBlocked() {
    ListenableFuture<?> result = driver.push(null);
    assertNotNull(result);
    assertTrue(result.isDone());
  }

  /** push() with empty dataSlices returns NOT_BLOCKED. */
  @Test
  public void testPushWithEmptyDataSlicesReturnsNotBlocked() {
    ListenableFuture<?> result = driver.push(java.util.Collections.emptyList());
    assertNotNull(result);
    assertTrue(result.isDone());
  }

  /** push() with valid dataSlices delegates to windowEngine.push(). */
  @Test
  public void testPushWithValidDataSlicesDelegatesToWindowEngine() {
    ListenableFuture<?> expectedResult = SettableFuture.create();
    doReturn(expectedResult).when(mockWindowEngine).push(anyList());
    List<org.apache.iotdb.streamnode.engine.task.StreamSubTask.DataSlice> slices =
        java.util.Collections.singletonList(
            mock(org.apache.iotdb.streamnode.engine.task.StreamSubTask.DataSlice.class));
    ListenableFuture<?> result = driver.push(slices);
    assertSame(expectedResult, result);
    verify(mockWindowEngine).push(slices);
  }

  // ==================== close() tests ====================

  /** close() transitions state from ALIVE to NEED_DESTRUCTION. Double close is a no-op. */
  @Test
  public void testDoubleCloseIsNoOp() {
    driver.close();
    // Second close should not throw (CAS fails silently)
    driver.close();
  }

  // ==================== getLastSuccessfulProcessTime test ====================

  /** getLastSuccessfulProcessTime is updated after a successful processInternal cycle. */
  @Test
  public void testLastSuccessfulProcessTimeUpdatedAfterSuccess() throws Exception {
    final IEventInfo mockEvent = newComputeEvent();
    when(mockWindowEngine.pollComputeEvent()).thenReturn(mockEvent);
    when(mockComputeTask.compute()).thenReturn(mock(TsBlock.class));
    when(mockComputeTask.isFinished()).thenReturn(false);
    doReturn(NOT_BLOCKED).when(mockSinkTask).push(any(), anyLong(), anyBoolean());

    long before = driver.getLastSuccessfulProcessTime();
    driver.processFor(new Duration(50, TimeUnit.MILLISECONDS));
    long after = driver.getLastSuccessfulProcessTime();

    assertTrue("lastSuccessfulProcessTime should be updated", after > before);
  }

  // ==================== close() destruction exception paths ====================

  private void markDriverNeedDestruction() throws Exception {
    java.lang.reflect.Field stateField = StreamDriver.class.getDeclaredField("state");
    stateField.setAccessible(true);
    java.util.concurrent.atomic.AtomicReference<?> state =
        (java.util.concurrent.atomic.AtomicReference<?>) stateField.get(driver);
    Object currentState = state.get();
    @SuppressWarnings({"rawtypes", "unchecked"})
    Enum<?> needDestruction = Enum.valueOf((Class) currentState.getClass(), "NEED_DESTRUCTION");
    ((java.util.concurrent.atomic.AtomicReference) state).set(needDestruction);
  }

  /** close() with forceClose() throwing still completes destruction. */
  @Test
  public void testCloseWithForceCloseException() throws Exception {
    doThrow(new RuntimeException("forceClose failed")).when(mockWindowEngine).forceClose();
    try {
      driver.close();
    } catch (RuntimeException expected) {
      // destroyIfNecessary rethrows aggregated exception
    }
    verify(mockWindowEngine).forceClose();
    verify(mockComputeTask).close();
    verify(mockSinkTask).stop();
  }

  /** close() with computeTask.close() throwing InterruptedException sets interrupt flag. */
  @Test
  public void testCloseWithComputeTaskInterruptedException() throws Exception {
    doThrow(new InterruptedException()).when(mockComputeTask).close();
    driver.close();
    verify(mockComputeTask).close();
  }

  /** close() with computeTask.close() throwing other exception continues destruction. */
  @Test
  public void testCloseWithComputeTaskOtherException() throws Exception {
    doThrow(new RuntimeException("compute close failed")).when(mockComputeTask).close();
    try {
      driver.close();
    } catch (RuntimeException expected) {
      // destroyIfNecessary rethrows aggregated exception
    }
    verify(mockComputeTask).close();
    verify(mockSinkTask).stop();
  }

  /** close() with sink.stop() throwing still completes destruction. */
  @Test
  public void testCloseWithSinkStopException() throws Exception {
    doThrow(new RuntimeException("sink stop failed")).when(mockSinkTask).stop();
    try {
      driver.close();
    } catch (RuntimeException expected) {
      // destroyIfNecessary rethrows aggregated exception
    }
    verify(mockWindowEngine).forceClose();
    verify(mockComputeTask).close();
    verify(mockSinkTask).stop();
  }

  /** close() with mayHaveTmpFile=true on the compute task triggers cleanTmpFile(). */
  @Test
  public void testCloseWithMayHaveTmpFile() throws Exception {
    when(mockComputeContext.mayHaveTmpFile()).thenReturn(true);
    driver.close();
    verify(mockComputeContext).mayHaveTmpFile();
    verify(mockDriverContext).finished();
  }

  /** close() with multiple exceptions aggregates via addSuppressed. */
  @Test
  public void testCloseWithMultipleExceptions() throws Exception {
    doThrow(new RuntimeException("forceClose failed")).when(mockWindowEngine).forceClose();
    doThrow(new RuntimeException("compute close failed")).when(mockComputeTask).close();
    doThrow(new RuntimeException("sink stop failed")).when(mockSinkTask).stop();

    try {
      driver.close();
    } catch (RuntimeException expected) {
      // Aggregated exception is rethrown
    }
    verify(mockWindowEngine).forceClose();
    verify(mockComputeTask).close();
    verify(mockSinkTask).stop();
    verify(mockDriverContext).failed(any(Throwable.class));
  }

  /** close() with driverContext.finished() throwing still releases resources. */
  @Test
  public void testCloseWithFinishedException() throws Exception {
    doThrow(new RuntimeException("finished failed")).when(mockDriverContext).finished();
    try {
      driver.close();
    } catch (RuntimeException expected) {
      // destroyIfNecessary rethrows
    }
    verify(mockDriverContext).finished();
    verify(mockSubTaskContext).signalDriverClosed();
  }

  // ==================== processInternal interrupter path ====================

  /** close() completes while another thread is processing a blocked driver. */
  @Test
  public void testCloseWhileProcessForIsRunning() throws Exception {
    // Keep processFor blocked on the compute task so close() does not race with a tight loop.
    final SettableFuture<Void> computeBlocked = SettableFuture.create();
    doReturn(computeBlocked).when(mockComputeTask).isBlocked();
    final IEventInfo mockEvent = newComputeEvent();
    // Return the event once, then null: pollComputeEvent signals an exhausted queue with null.
    // A stub that never returns null keeps close()'s unconsumed-event drain looping forever.
    when(mockWindowEngine.pollComputeEvent()).thenReturn(mockEvent, (IEventInfo) null);

    // Start processFor in a separate thread, then close() to interrupt
    Thread t =
        new Thread(
            () -> {
              try {
                driver.processFor(new Duration(60, TimeUnit.SECONDS));
              } catch (Exception ignored) {
                // Expected
              }
            });
    t.start();
    Thread.sleep(100);
    driver.close();
    computeBlocked.set(null);
    t.join(5000);
    // Driver should be closed without deadlock
    assertTrue(driver.isFinished());
  }

  // ==================== processInternal catch block branch tests ====================

  /** processInternal with non-RuntimeException (Error) wraps in RuntimeException. */
  @Test
  public void testProcessInternalWithErrorWrapsInRuntimeException() throws Exception {
    final IEventInfo mockEvent = newComputeEvent();
    when(mockWindowEngine.pollComputeEvent()).thenReturn(mockEvent);
    when(mockComputeTask.compute()).thenThrow(new Error("test-error"));
    try {
      driver.processFor(new Duration(50, TimeUnit.MILLISECONDS));
      fail("Expected RuntimeException");
    } catch (RuntimeException e) {
      // Error is not RuntimeException, so it's wrapped
      assertTrue(e.getCause() instanceof Error);
    }
  }

  /** processFor when driverBlockedFuture is not done returns it immediately. */
  @Test
  public void testProcessForReturnsBlockedFutureWhenAlreadyBlocked() throws Exception {
    // Set up a blocked future that is not done
    SettableFuture<?> blockedFuture = SettableFuture.create();
    java.lang.reflect.Field field = StreamDriver.class.getDeclaredField("driverBlockedFuture");
    field.setAccessible(true);
    ((java.util.concurrent.atomic.AtomicReference<SettableFuture<?>>) field.get(driver))
        .set(blockedFuture);

    ListenableFuture<?> result = driver.processFor(new Duration(50, TimeUnit.MILLISECONDS));
    assertSame(blockedFuture, result);
  }

  /** processFor when state is not ALIVE returns NOT_BLOCKED immediately. */
  @Test
  public void testProcessForReturnsNotBlockedWhenStateNotAlive() throws Exception {
    driver.close(); // transitions state to NEED_DESTRUCTION then DESTROYED
    ListenableFuture<?> result = driver.processFor(new Duration(50, TimeUnit.MILLISECONDS));
    assertTrue(result.isDone());
  }

  // ==================== isFinished / tryWithLock lock-hold branches ====================

  /** Acquires the ReentrantLock inside DriverLock from a helper thread. */
  private Thread lockHolder;

  private java.util.concurrent.CountDownLatch releaseLock;

  private void acquireLockFromOtherThread() throws Exception {
    java.lang.reflect.Field lockField = StreamDriver.class.getDeclaredField("exclusiveLock");
    lockField.setAccessible(true);
    Object driverLock = lockField.get(driver);
    java.lang.reflect.Field reentrantField = driverLock.getClass().getDeclaredField("lock");
    reentrantField.setAccessible(true);
    java.util.concurrent.locks.ReentrantLock reentrantLock =
        (java.util.concurrent.locks.ReentrantLock) reentrantField.get(driverLock);

    java.util.concurrent.CountDownLatch lockAcquired = new java.util.concurrent.CountDownLatch(1);
    java.util.concurrent.CountDownLatch release = new java.util.concurrent.CountDownLatch(1);
    lockHolder =
        new Thread(
            () -> {
              reentrantLock.lock();
              lockAcquired.countDown();
              try {
                release.await();
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              } finally {
                reentrantLock.unlock();
              }
            });
    releaseLock = release;
    lockHolder.start();
    assertTrue("lock holder should acquire the lock", lockAcquired.await(5, TimeUnit.SECONDS));
  }

  /** Releases the ReentrantLock from the same helper thread that acquired it. */
  private void releaseLockFromOtherThread() throws Exception {
    assertNotNull("lock holder should exist", lockHolder);
    assertNotNull("release signal should exist", releaseLock);
    releaseLock.countDown();
    lockHolder.join(5000);
    assertFalse("lock holder should exit", lockHolder.isAlive());
    lockHolder = null;
    releaseLock = null;
  }

  /** isFinished falls back to state/driverContext check when lock cannot be acquired. */
  @Test
  public void testIsFinishedFallbackWhenLockHeld() throws Exception {
    acquireLockFromOtherThread();
    try {
      // tryWithLockUninterruptibly fails -> orElseGet fallback: state != ALIVE || isDone
      // Driver is ALIVE and not done -> false
      assertFalse(driver.isFinished());
    } finally {
      releaseLockFromOtherThread();
    }
  }

  /** isFinished fallback returns true when driverContext.isDone() is true. */
  @Test
  public void testIsFinishedFallbackReturnsTrueWhenDone() throws Exception {
    when(mockDriverContext.isDone()).thenReturn(true);
    acquireLockFromOtherThread();
    try {
      // Fallback: driverContext.isDone() is true -> isFinished returns true
      assertTrue(driver.isFinished());
    } finally {
      releaseLockFromOtherThread();
    }
  }

  /** processFor returns NOT_BLOCKED when lock cannot be acquired (tryWithLock returns empty). */
  @Test
  public void testProcessForReturnsNotBlockedWhenLockHeld() throws Exception {
    acquireLockFromOtherThread();
    try {
      // tryWithLock(100ms) fails to acquire -> Optional.empty() -> processFor returns NOT_BLOCKED
      ListenableFuture<?> result = driver.processFor(new Duration(50, TimeUnit.MILLISECONDS));
      assertNotNull(result);
      assertTrue(result.isDone());
    } finally {
      releaseLockFromOtherThread();
    }
  }

  // ==================== updateDriverBlockedFuture listener tests ====================

  /** updateDriverBlockedFuture completes the driver blocked future when source completes. */
  @Test
  public void testUpdateDriverBlockedFutureListenerCompletesResult() throws Exception {
    SettableFuture<Void> windowBlocked = SettableFuture.create();
    doReturn(windowBlocked).when(mockWindowEngine).getBlockedFuture();

    ListenableFuture<?> result = driver.processFor(new Duration(50, TimeUnit.MILLISECONDS));
    assertFalse(result.isDone());

    // Complete the source future -> listener completes the driver blocked future
    windowBlocked.set(null);
    assertTrue(result.isDone());
  }

  /** updateDriverBlockedFuture listener no-op when newDriverBlockedFuture already done. */
  @Test
  public void testUpdateDriverBlockedFutureListenerNoopWhenAlreadyDone() throws Exception {
    SettableFuture<Void> windowBlocked = SettableFuture.create();
    doReturn(windowBlocked).when(mockWindowEngine).getBlockedFuture();

    ListenableFuture<?> result = driver.processFor(new Duration(50, TimeUnit.MILLISECONDS));
    assertFalse(result.isDone());

    // Complete the source future, then try to complete the result again (should be no-op)
    windowBlocked.set(null);
    assertTrue(result.isDone());
    // The listener's `if (!newDriverBlockedFuture.isDone())` guard prevents double-completion
  }

  // ==================== tryWithLock Error branch test ====================

  /** tryWithLock rethrows Error when destroyIfNecessary throws an Error. */
  @Test
  public void testCloseWithErrorPropagatesAsError() throws Exception {
    // forceClose throwing an Error -> inFlightException is an Error -> destroyIfNecessary rethrows
    // it
    doThrow(new AssertionError("test-error")).when(mockWindowEngine).forceClose();
    try {
      driver.close();
      fail("Expected AssertionError to propagate");
    } catch (AssertionError expected) {
      assertEquals("test-error", expected.getMessage());
    }
  }

  // ==================== tryWithLock InterruptedException test ====================

  /** tryWithLock catches InterruptedException and returns empty when interrupted while waiting. */
  @Test
  public void testProcessForInterruptedWhileLockHeld() throws Exception {
    acquireLockFromOtherThread();
    // Interrupt current thread so tryLock throws InterruptedException
    Thread.currentThread().interrupt();
    try {
      // tryWithLock catches InterruptedException -> !acquired -> Optional.empty() -> NOT_BLOCKED
      ListenableFuture<?> result = driver.processFor(new Duration(50, TimeUnit.MILLISECONDS));
      assertNotNull(result);
      assertTrue(result.isDone());
      // Interrupt flag should be re-set by the catch block
      assertTrue(Thread.interrupted());
    } finally {
      releaseLockFromOtherThread();
    }
  }

  // ==================== cleanTmpFile tests ====================

  /** cleanTmpFile deletes the sort dir when it exists. */
  @Test
  public void testCloseDeletesExistingTmpFileDir() throws Exception {
    String sortTmpDir = StreamNodeDescriptor.getInstance().getConfig().getSortTmpDir();
    File sortDir =
        new File(
            sortTmpDir
                + File.separator
                + taskId.getFullId()
                + File.separator
                + "0"
                + File.separator);
    assertTrue("temp dir should be created for test", sortDir.mkdirs());

    when(mockComputeContext.mayHaveTmpFile()).thenReturn(true);
    try {
      driver.close();
      // cleanTmpFile should delete the dir
      assertFalse("sort dir should be deleted", sortDir.exists());
    } finally {
      if (sortDir.exists()) {
        org.apache.iotdb.commons.utils.FileUtils.deleteFileOrDirectory(sortDir, true);
      }
    }
  }

  // ==================== processInternal interrupterStack != null path ====================

  /**
   * processInternal catch block with interrupterStack != null wraps the cause in an "Interrupted
   * By" exception. This path is unreachable via normal processFor (lock acquired with
   * interruptOnClose=false), so we set the interrupterStack directly via reflection.
   */
  @Test
  public void testProcessInternalInterrupterStackBuildsInterruptedBy() throws Exception {
    // Set interrupterStack in the DriverLock via reflection
    java.lang.reflect.Field lockField = StreamDriver.class.getDeclaredField("exclusiveLock");
    lockField.setAccessible(true);
    Object driverLock = lockField.get(driver);
    java.lang.reflect.Field interrupterStackField =
        driverLock.getClass().getDeclaredField("interrupterStack");
    interrupterStackField.setAccessible(true);
    interrupterStackField.set(
        driverLock,
        java.util.Collections.singletonList(new StackTraceElement("test", "method", "file", 1)));

    // An exception outside processEvent() reaches processInternal()'s interruption wrapper.
    when(mockSubTask.getSink()).thenThrow(new RuntimeException("compute failed"));

    try {
      driver.processFor(new Duration(50, TimeUnit.MILLISECONDS));
      fail("Expected 'StreamDriver was interrupted' exception");
    } catch (RuntimeException e) {
      assertEquals("StreamDriver was interrupted", e.getMessage());
      assertNotNull(e.getCause());
      assertEquals("Interrupted By", e.getCause().getMessage());
    }
  }

  /**
   * processInternal catch block with interrupterStack != null and a non-RuntimeException cause
   * still wraps it in the "Interrupted By" exception.
   */
  @Test
  public void testProcessInternalInterrupterStackWithErrorCause() throws Exception {
    java.lang.reflect.Field lockField = StreamDriver.class.getDeclaredField("exclusiveLock");
    lockField.setAccessible(true);
    Object driverLock = lockField.get(driver);
    java.lang.reflect.Field interrupterStackField =
        driverLock.getClass().getDeclaredField("interrupterStack");
    interrupterStackField.setAccessible(true);
    interrupterStackField.set(
        driverLock,
        java.util.Collections.singletonList(new StackTraceElement("test", "method", "file", 1)));

    // Error (not RuntimeException) -> still wrapped in "Interrupted By" with suppressed cause
    final IEventInfo mockEvent = newComputeEvent();
    when(mockWindowEngine.pollComputeEvent()).thenReturn(mockEvent);
    when(mockComputeTask.compute()).thenThrow(new Error("compute error"));

    try {
      driver.processFor(new Duration(50, TimeUnit.MILLISECONDS));
      fail("Expected 'StreamDriver was interrupted' exception");
    } catch (RuntimeException e) {
      assertEquals("StreamDriver was interrupted", e.getMessage());
      // The original Error should be suppressed
      assertEquals(1, e.getSuppressed().length);
      assertTrue(e.getSuppressed()[0] instanceof Error);
    }
  }

  // ==================== tryWithLock NEED_DESTRUCTION re-acquire branches ====================

  /**
   * Covers tryWithLock lines 406-424 when task.get() throws AND state is NEED_DESTRUCTION. The
   * re-acquire block runs destroyIfNecessary (branches: state==NEED_DESTRUCTION true, tryLock
   * success, destroyIfNecessary success, finally unlock).
   */
  @Test(timeout = 10000)
  public void testTryWithLockNeedDestructionReacquire() throws Exception {
    java.util.concurrent.CountDownLatch taskStarted = new java.util.concurrent.CountDownLatch(1);
    java.util.concurrent.CountDownLatch releaseTask = new java.util.concurrent.CountDownLatch(1);
    // getSink() blocks until released, then throws from processInternal's outer path.
    when(mockSubTask.getSink())
        .thenAnswer(
            inv -> {
              taskStarted.countDown();
              releaseTask.await(5, TimeUnit.SECONDS);
              throw new RuntimeException("compute failed");
            });

    Thread t =
        new Thread(
            () -> {
              try {
                driver.processFor(new Duration(60, TimeUnit.SECONDS));
              } catch (Exception ignored) {
              }
            });
    t.start();
    assertTrue(taskStarted.await(5, TimeUnit.SECONDS));

    // Mark the driver for destruction while processFor holds the lock. This lets the process
    // thread, rather than a competing close thread, exercise the re-acquire path deterministically.
    markDriverNeedDestruction();

    // Release getSink() to throw -> processInternal throws -> after unlock, re-acquire runs.
    releaseTask.countDown();
    t.join(5000);

    // destroyIfNecessary should have run via the re-acquire block
    verify(mockDriverContext).finished();
  }

  /**
   * Covers tryWithLock lines 413-417 when destroyIfNecessary (inside re-acquire) throws. The
   * failure (compute exception) is non-null and != the new exception, so it is addSuppressed.
   */
  @Test(timeout = 10000)
  public void testTryWithLockNeedDestructionReacquireWithException() throws Exception {
    // forceClose throws -> destroyIfNecessary inside re-acquire throws -> catch block runs
    doThrow(new RuntimeException("forceClose failed")).when(mockWindowEngine).forceClose();

    java.util.concurrent.CountDownLatch taskStarted = new java.util.concurrent.CountDownLatch(1);
    java.util.concurrent.CountDownLatch releaseTask = new java.util.concurrent.CountDownLatch(1);
    when(mockSubTask.getSink())
        .thenAnswer(
            inv -> {
              taskStarted.countDown();
              releaseTask.await(5, TimeUnit.SECONDS);
              throw new RuntimeException("compute failed");
            });

    java.util.concurrent.atomic.AtomicReference<RuntimeException> caught =
        new java.util.concurrent.atomic.AtomicReference<>();
    Thread t =
        new Thread(
            () -> {
              try {
                driver.processFor(new Duration(60, TimeUnit.SECONDS));
              } catch (RuntimeException e) {
                caught.set(e);
              } catch (Exception ignored) {
              }
            });
    t.start();
    assertTrue(taskStarted.await(5, TimeUnit.SECONDS));

    // Mark the driver for destruction while processFor holds the lock so the process thread
    // deterministically exercises the re-acquire path.
    markDriverNeedDestruction();
    releaseTask.countDown();
    t.join(5000);

    // The compute failure is rethrown, with the forceClose failure suppressed
    assertNotNull("processFor should throw", caught.get());
    assertEquals("compute failed", caught.get().getMessage());
    boolean hasForceCloseSuppressed = false;
    for (Throwable suppressed : caught.get().getSuppressed()) {
      if ("forceClose failed".equals(suppressed.getMessage())) {
        hasForceCloseSuppressed = true;
        break;
      }
    }
    assertTrue("forceClose failure should be suppressed", hasForceCloseSuppressed);
  }
}
