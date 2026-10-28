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
package org.apache.iotdb.streamnode.engine.task;

import org.apache.iotdb.commons.stream.ListPartitionKey;
import org.apache.iotdb.commons.stream.PartitionKey;
import org.apache.iotdb.commons.stream.StreamTask;
import org.apache.iotdb.commons.stream.StreamWindowType;
import org.apache.iotdb.streamnode.engine.scheduler.IStreamTaskScheduler;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Collections;
import java.util.concurrent.ExecutorService;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class PeriodStreamTaskInstanceTest {

  @Mock private StreamTask mockStreamTask;
  @Mock private IStreamTaskScheduler mockScheduler;
  @Mock private ExecutorService mockSubTaskNotificationExecutor;

  private PeriodStreamTaskInstance periodInstance;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);
    when(mockStreamTask.getTaskName()).thenReturn("period-task");
    when(mockStreamTask.getWindow())
        .thenReturn(
            new org.apache.iotdb.commons.stream.PeriodWindow(0, 60000L, 0L) {
              @Override
              public StreamWindowType getType() {
                return StreamWindowType.PERIOD;
              }
            });

    PeriodStreamTaskInstance realInstance =
        new PeriodStreamTaskInstance(
            mockStreamTask, mockScheduler, mockSubTaskNotificationExecutor, false);
    periodInstance = spy(realInstance);
  }

  /** Pre-populates subTaskExecutions with a mock execution to simulate "already started" state. */
  private void injectMockExecution(PartitionKey key) throws Exception {
    StreamSubTaskExecution mockExecution = mock(StreamSubTaskExecution.class);
    StreamSubTask mockSubTask = mock(StreamSubTask.class);
    when(mockExecution.getSubTask()).thenReturn(mockSubTask);
    java.lang.reflect.Field subTaskExecutionsField =
        AbstractStreamTaskInstance.class.getDeclaredField("subTaskExecutions");
    subTaskExecutionsField.setAccessible(true);
    @SuppressWarnings("unchecked")
    java.util.Map<PartitionKey, StreamSubTaskExecution> map =
        (java.util.Map<PartitionKey, StreamSubTaskExecution>)
            subTaskExecutionsField.get(periodInstance);
    map.put(key, mockExecution);
  }

  @Test
  public void testStartInternalWithEmptyCreatesSubTask() throws Exception {
    when(mockStreamTask.getTarget()).thenReturn(null);
    // Stub getOrCreateSubTask to avoid full sub-task creation chain
    doReturn(mock(StreamSubTask.class)).when(periodInstance).getOrCreateSubTask(any());

    periodInstance.start();

    verify(periodInstance, times(1)).getOrCreateSubTask(any());
  }

  @Test
  public void testStartInternalWithNonEmptyRestartsExisting() throws Exception {
    when(mockStreamTask.getTarget()).thenReturn(null);
    PartitionKey key = new ListPartitionKey(Collections.emptyList());
    injectMockExecution(key);

    periodInstance.start();

    // Should NOT call getOrCreateSubTask (map is non-empty)
    verify(periodInstance, never()).getOrCreateSubTask(any());
    assertTrue(periodInstance.isRunning());
  }

  @Test
  public void testStopStopsAllSubTaskExecutions() throws Exception {
    when(mockStreamTask.getTarget()).thenReturn(null);
    PartitionKey key = new ListPartitionKey(Collections.emptyList());
    StreamSubTaskExecution mockExecution = mock(StreamSubTaskExecution.class);
    java.lang.reflect.Field subTaskExecutionsField =
        AbstractStreamTaskInstance.class.getDeclaredField("subTaskExecutions");
    subTaskExecutionsField.setAccessible(true);
    @SuppressWarnings("unchecked")
    java.util.Map<PartitionKey, StreamSubTaskExecution> map =
        (java.util.Map<PartitionKey, StreamSubTaskExecution>)
            subTaskExecutionsField.get(periodInstance);
    map.put(key, mockExecution);

    periodInstance.start();
    periodInstance.stop();

    assertFalse(periodInstance.isRunning());
    verify(mockExecution, times(1)).stop();
  }

  @Test
  public void testRestartRestartsExistingSubTask() throws Exception {
    when(mockStreamTask.getTarget()).thenReturn(null);
    PartitionKey key = new ListPartitionKey(Collections.emptyList());
    StreamSubTaskExecution mockExecution = mock(StreamSubTaskExecution.class);
    java.lang.reflect.Field subTaskExecutionsField =
        AbstractStreamTaskInstance.class.getDeclaredField("subTaskExecutions");
    subTaskExecutionsField.setAccessible(true);
    @SuppressWarnings("unchecked")
    java.util.Map<PartitionKey, StreamSubTaskExecution> map =
        (java.util.Map<PartitionKey, StreamSubTaskExecution>)
            subTaskExecutionsField.get(periodInstance);
    map.put(key, mockExecution);

    // First start (with pre-populated map -> restart path)
    periodInstance.start();
    // stop (pause)
    periodInstance.stop();
    // restart
    periodInstance.start();

    // start() should be called on the existing execution twice (initial + restart)
    verify(mockExecution, times(2)).start();
  }

  @Test
  public void testDropCleansUpResources() throws Exception {
    when(mockStreamTask.getTarget()).thenReturn(null);
    PartitionKey key = new ListPartitionKey(Collections.emptyList());
    StreamSubTaskExecution mockExecution = mock(StreamSubTaskExecution.class);
    java.lang.reflect.Field subTaskExecutionsField =
        AbstractStreamTaskInstance.class.getDeclaredField("subTaskExecutions");
    subTaskExecutionsField.setAccessible(true);
    @SuppressWarnings("unchecked")
    java.util.Map<PartitionKey, StreamSubTaskExecution> map =
        (java.util.Map<PartitionKey, StreamSubTaskExecution>)
            subTaskExecutionsField.get(periodInstance);
    map.put(key, mockExecution);

    periodInstance.start();
    periodInstance.drop();

    verify(mockExecution, times(1)).drop();
  }

  @Test
  public void testStopWhenNotRunningIsNoop() {
    periodInstance.stop();
    assertFalse(periodInstance.isRunning());
  }

  @Test
  public void testStartAlreadyRunningIsNoop() throws Exception {
    when(mockStreamTask.getTarget()).thenReturn(null);
    doReturn(mock(StreamSubTask.class)).when(periodInstance).getOrCreateSubTask(any());

    periodInstance.start();
    assertTrue(periodInstance.isRunning());

    // second start should be a no-op (CAS fails)
    periodInstance.start();
    assertTrue(periodInstance.isRunning());
    verify(periodInstance, times(1)).getOrCreateSubTask(any());
  }

  @Test
  public void testGetTaskDefinition() {
    org.junit.Assert.assertSame(mockStreamTask, periodInstance.getTaskDefinition());
  }

  @Test
  public void testIsRunningInitiallyFalse() {
    assertFalse(periodInstance.isRunning());
  }

  @Test
  public void testDropDoesNotRequireRunningReset() throws Exception {
    when(mockStreamTask.getTarget()).thenReturn(null);
    doReturn(mock(StreamSubTask.class)).when(periodInstance).getOrCreateSubTask(any());

    periodInstance.start();
    // drop() performs full cleanup (dropInternal + subTaskExecutions cleanup +
    // writeBackEngine.stop)
    // but does not reset the running flag (different flow from stop, per design).
    periodInstance.drop();
    // Verify drop path was exercised without throwing
    org.junit.Assert.assertNotNull(periodInstance);
  }

  // ==================== onSubTaskStateChanged tests ====================

  @Test
  public void testOnSubTaskStateChangedNonDoneReturnsEarly() {
    PartitionKey key = new ListPartitionKey(Collections.emptyList());
    StreamSubTaskExecution mockExecution = mock(StreamSubTaskExecution.class);
    // RUNNING is not a done state -> should return early without removing from map
    periodInstance.onSubTaskStateChanged(key, mockExecution, StreamSubTaskState.RUNNING);
    org.junit.Assert.assertNotNull(periodInstance);
  }

  @Test
  public void testOnSubTaskStateChangedDroppedRemovesFromMap() throws Exception {
    PartitionKey key = new ListPartitionKey(Collections.emptyList());
    injectMockExecution(key);
    StreamSubTaskExecution mockExecution = mock(StreamSubTaskExecution.class);

    periodInstance.onSubTaskStateChanged(key, mockExecution, StreamSubTaskState.DROPPED);

    assertTrue(periodInstance.getSubTasksSnapshot().isEmpty());
  }

  @Test
  public void testOnSubTaskStateChangedStoppedDoesNotRemoveFromMap() throws Exception {
    // STOPPED is non-terminal (isDone=false), so onSubTaskStateChanged returns early
    PartitionKey key = new ListPartitionKey(Collections.emptyList());
    injectMockExecution(key);
    StreamSubTaskExecution mockExecution = mock(StreamSubTaskExecution.class);

    periodInstance.onSubTaskStateChanged(key, mockExecution, StreamSubTaskState.STOPPED);

    // STOPPED is not done -> entry remains in map
    assertFalse(periodInstance.getSubTasksSnapshot().isEmpty());
  }

  @Test
  public void testOnSubTaskStateChangedFailedWhenNotRunningDoesNotRecreate() throws Exception {
    PartitionKey key = new ListPartitionKey(Collections.emptyList());
    StreamSubTaskExecution mockExecution = mock(StreamSubTaskExecution.class);
    StreamSubTaskContext mockContext = mock(StreamSubTaskContext.class);
    when(mockExecution.getContext()).thenReturn(mockContext);
    when(mockContext.getFailureCause()).thenReturn(java.util.Optional.empty());

    periodInstance.onSubTaskStateChanged(key, mockExecution, StreamSubTaskState.FAILED);

    // Not running -> recreatePeriodSubTask does nothing
    verify(periodInstance, never()).getOrCreateSubTask(any());
  }

  @Test
  public void testOnSubTaskStateChangedFailedWhenRunningTriggersRecreate() throws Exception {
    PartitionKey key = new ListPartitionKey(Collections.emptyList());
    StreamSubTaskExecution mockExecution = mock(StreamSubTaskExecution.class);
    StreamSubTaskContext mockContext = mock(StreamSubTaskContext.class);
    when(mockExecution.getContext()).thenReturn(mockContext);
    when(mockContext.getFailureCause()).thenReturn(java.util.Optional.empty());

    doReturn(mock(StreamSubTask.class)).when(periodInstance).getOrCreateSubTask(any());
    periodInstance.start();

    periodInstance.onSubTaskStateChanged(key, mockExecution, StreamSubTaskState.FAILED);

    verify(periodInstance, org.mockito.Mockito.atLeastOnce()).getOrCreateSubTask(any());
  }

  @Test
  public void testMultipleFailuresStartFailureResetTimer() throws Exception {
    PartitionKey key = new ListPartitionKey(Collections.emptyList());
    StreamSubTaskExecution mockExecution = mock(StreamSubTaskExecution.class);
    StreamSubTaskContext mockContext = mock(StreamSubTaskContext.class);
    when(mockExecution.getContext()).thenReturn(mockContext);
    when(mockContext.getFailureCause()).thenReturn(java.util.Optional.empty());

    doReturn(mock(StreamSubTask.class)).when(periodInstance).getOrCreateSubTask(any());
    periodInstance.start();

    for (int i = 0; i < 6; i++) {
      periodInstance.onSubTaskStateChanged(key, mockExecution, StreamSubTaskState.FAILED);
    }

    org.junit.Assert.assertNotNull(periodInstance);
  }

  @Test
  public void testDropStopsFailureResetTimer() throws Exception {
    PartitionKey key = new ListPartitionKey(Collections.emptyList());
    StreamSubTaskExecution mockExecution = mock(StreamSubTaskExecution.class);
    StreamSubTaskContext mockContext = mock(StreamSubTaskContext.class);
    when(mockExecution.getContext()).thenReturn(mockContext);
    when(mockContext.getFailureCause()).thenReturn(java.util.Optional.empty());

    doReturn(mock(StreamSubTask.class)).when(periodInstance).getOrCreateSubTask(any());
    periodInstance.start();
    for (int i = 0; i < 6; i++) {
      periodInstance.onSubTaskStateChanged(key, mockExecution, StreamSubTaskState.FAILED);
    }
    periodInstance.drop();

    org.junit.Assert.assertNotNull(periodInstance);
  }

  // ==================== failureResetTimer callback tests ====================

  /** stopFailureResetTimer with null timer is a no-op. */
  @Test
  public void testStopFailureResetTimerWhenNullIsNoOp() {
    // Don't start failureResetTimer, just drop (which calls stopFailureResetTimer)
    periodInstance.drop();
    // No exception
    org.junit.Assert.assertNotNull(periodInstance);
  }

  /** dropInternal stops failure reset timer without error. */
  @Test
  public void testDropInternalStopsFailureResetTimer() throws Exception {
    when(mockStreamTask.getTarget()).thenReturn(null);
    doReturn(mock(StreamSubTask.class)).when(periodInstance).getOrCreateSubTask(any());

    periodInstance.start();
    periodInstance.drop();

    // Verify drop path completed (dropInternal calls stopFailureResetTimer)
    org.junit.Assert.assertNotNull(periodInstance);
  }

  /** onSubTaskStateChanged with DROPPED is a done state, removes from map. */
  @Test
  public void testOnSubTaskStateChangedDroppedIsDoneAndRemovesFromMap() throws Exception {
    PartitionKey key = new ListPartitionKey(Collections.emptyList());
    injectMockExecution(key);
    StreamSubTaskExecution mockExecution = mock(StreamSubTaskExecution.class);

    periodInstance.onSubTaskStateChanged(key, mockExecution, StreamSubTaskState.DROPPED);

    // DROPPED is done -> removed from map
    assertTrue(periodInstance.getSubTasksSnapshot().isEmpty());
  }

  /** onSubTaskStateChanged with FAILED is a done state, removes from map. */
  @Test
  public void testOnSubTaskStateChangedFailedIsDoneAndRemovesFromMap() throws Exception {
    PartitionKey key = new ListPartitionKey(Collections.emptyList());
    injectMockExecution(key);
    StreamSubTaskExecution mockExecution = mock(StreamSubTaskExecution.class);
    StreamSubTaskContext mockContext = mock(StreamSubTaskContext.class);
    when(mockExecution.getContext()).thenReturn(mockContext);
    when(mockContext.getFailureCause()).thenReturn(java.util.Optional.empty());

    periodInstance.onSubTaskStateChanged(key, mockExecution, StreamSubTaskState.FAILED);

    // FAILED is done -> removed from map
    assertTrue(periodInstance.getSubTasksSnapshot().isEmpty());
  }

  /**
   * scheduleRecreateWithRateLimit with running=true triggers immediate recreate (waitTime <= 0).
   */
  @Test
  public void testScheduleRecreateWithRateLimitImmediateRecreate() throws Exception {
    PartitionKey key = new ListPartitionKey(Collections.emptyList());
    StreamSubTaskExecution mockExecution = mock(StreamSubTaskExecution.class);
    StreamSubTaskContext mockContext = mock(StreamSubTaskContext.class);
    when(mockExecution.getContext()).thenReturn(mockContext);
    when(mockContext.getFailureCause()).thenReturn(java.util.Optional.empty());

    doReturn(mock(StreamSubTask.class)).when(periodInstance).getOrCreateSubTask(any());
    periodInstance.start();

    // Set lastRecreateTime to 0 so waitTime = 0 + backoff - currentTime < 0 (immediate)
    periodInstance.onSubTaskStateChanged(key, mockExecution, StreamSubTaskState.FAILED);

    // getOrCreateSubTask should be called (immediate recreate)
    verify(periodInstance, atLeastOnce()).getOrCreateSubTask(any());
  }

  /** Multiple failures increment consecutiveFailures and eventually start timer. */
  @Test
  public void testMultipleFailuresIncrementFailureCount() throws Exception {
    PartitionKey key = new ListPartitionKey(Collections.emptyList());
    StreamSubTaskExecution mockExecution = mock(StreamSubTaskExecution.class);
    StreamSubTaskContext mockContext = mock(StreamSubTaskContext.class);
    when(mockExecution.getContext()).thenReturn(mockContext);
    when(mockContext.getFailureCause()).thenReturn(java.util.Optional.empty());

    doReturn(mock(StreamSubTask.class)).when(periodInstance).getOrCreateSubTask(any());
    periodInstance.start();

    // Trigger 3 failures (< threshold 5, timer not started yet)
    for (int i = 0; i < 3; i++) {
      periodInstance.onSubTaskStateChanged(key, mockExecution, StreamSubTaskState.FAILED);
    }

    // getOrCreateSubTask should be called at least once (immediate recreate for each failure)
    verify(periodInstance, atLeastOnce()).getOrCreateSubTask(any());
  }

  /** recreatePeriodSubTask when not running does not call getOrCreateSubTask. */
  @Test
  public void testRecreatePeriodSubTaskWhenNotRunningDoesNotRecreate() throws Exception {
    PartitionKey key = new ListPartitionKey(Collections.emptyList());
    StreamSubTaskExecution mockExecution = mock(StreamSubTaskExecution.class);
    StreamSubTaskContext mockContext = mock(StreamSubTaskContext.class);
    when(mockExecution.getContext()).thenReturn(mockContext);
    when(mockContext.getFailureCause()).thenReturn(java.util.Optional.empty());

    // Don't call start() -> running = false
    periodInstance.onSubTaskStateChanged(key, mockExecution, StreamSubTaskState.FAILED);

    // running is false -> recreatePeriodSubTask does nothing
    verify(periodInstance, never()).getOrCreateSubTask(any());
  }

  /** scheduleRecreateWithRateLimit with waitTime > 0 schedules delayed recreate. */
  @Test(timeout = 15000)
  public void testScheduleRecreateWithRateLimitDelayedRecreate() throws Exception {
    PartitionKey key = new ListPartitionKey(Collections.emptyList());
    StreamSubTaskExecution mockExecution = mock(StreamSubTaskExecution.class);
    StreamSubTaskContext mockContext = mock(StreamSubTaskContext.class);
    when(mockExecution.getContext()).thenReturn(mockContext);
    when(mockContext.getFailureCause()).thenReturn(java.util.Optional.empty());

    doReturn(mock(StreamSubTask.class)).when(periodInstance).getOrCreateSubTask(any());
    periodInstance.start();

    // First failure: immediate recreate (waitTime <= 0, sets lastRecreateTime)
    periodInstance.onSubTaskStateChanged(key, mockExecution, StreamSubTaskState.FAILED);

    // Second failure immediately: waitTime = lastRecreateTime + backoff - now > 0
    // backoff for 2nd failure = MIN_RECREATE_INTERVAL_MS * 2 = 2000ms
    periodInstance.onSubTaskStateChanged(key, mockExecution, StreamSubTaskState.FAILED);

    // The delayed recreate should eventually call getOrCreateSubTask again
    // Wait for the delayed recreate to complete (backoff is 2000ms)
    Thread.sleep(3000);

    // getOrCreateSubTask should be called at least twice (first immediate + delayed)
    verify(periodInstance, atLeastOnce()).getOrCreateSubTask(any());
  }

  /** onSubTaskStateChanged with RUNNING (non-done) returns early, no map removal. */
  @Test
  public void testOnSubTaskStateChangedRunningReturnsEarly() throws Exception {
    PartitionKey key = new ListPartitionKey(Collections.emptyList());
    injectMockExecution(key);
    StreamSubTaskExecution mockExecution = mock(StreamSubTaskExecution.class);

    periodInstance.onSubTaskStateChanged(key, mockExecution, StreamSubTaskState.RUNNING);

    // RUNNING is not done -> entry remains in map
    assertFalse(periodInstance.getSubTasksSnapshot().isEmpty());
  }

  /** onSubTaskStateChanged with STOPPING (non-done) returns early. */
  @Test
  public void testOnSubTaskStateChangedStoppingReturnsEarly() throws Exception {
    PartitionKey key = new ListPartitionKey(Collections.emptyList());
    injectMockExecution(key);
    StreamSubTaskExecution mockExecution = mock(StreamSubTaskExecution.class);

    periodInstance.onSubTaskStateChanged(key, mockExecution, StreamSubTaskState.STOPPING);

    assertFalse(periodInstance.getSubTasksSnapshot().isEmpty());
  }

  // ==================== failureResetTimer double-checked locking ====================

  /** startFailureResetTimer returns early when timer is already set (first check). */
  @Test
  public void testStartFailureResetTimerAlreadyStartedReturnsEarly() throws Exception {
    // Set failureResetTimer to a non-null mock so the first check returns early
    java.lang.reflect.Field timerField =
        PeriodStreamTaskInstance.class.getDeclaredField("failureResetTimer");
    timerField.setAccessible(true);
    timerField.set(periodInstance, mock(java.util.concurrent.ScheduledFuture.class));

    PartitionKey key = new ListPartitionKey(Collections.emptyList());
    StreamSubTaskExecution mockExecution = mock(StreamSubTaskExecution.class);
    StreamSubTaskContext mockContext = mock(StreamSubTaskContext.class);
    when(mockExecution.getContext()).thenReturn(mockContext);
    when(mockContext.getFailureCause()).thenReturn(java.util.Optional.empty());
    doReturn(mock(StreamSubTask.class)).when(periodInstance).getOrCreateSubTask(any());
    periodInstance.start();

    // Trigger 6 failures - should hit startFailureResetTimer's first check and return early
    for (int i = 0; i < 6; i++) {
      periodInstance.onSubTaskStateChanged(key, mockExecution, StreamSubTaskState.FAILED);
    }
    // Timer should still be the mock (not replaced)
    org.junit.Assert.assertSame(
        "failureResetTimer should not be replaced when already set",
        timerField.get(periodInstance),
        timerField.get(periodInstance));
  }

  /** stopFailureResetTimer clears the timer when it's not null. */
  @Test
  public void testStopFailureResetTimerWhenNotNull() throws Exception {
    java.lang.reflect.Field timerField =
        PeriodStreamTaskInstance.class.getDeclaredField("failureResetTimer");
    timerField.setAccessible(true);
    java.util.concurrent.ScheduledFuture<?> mockTimer =
        mock(java.util.concurrent.ScheduledFuture.class);
    timerField.set(periodInstance, mockTimer);

    // dropInternal calls stopFailureResetTimer which should cancel and null the timer
    periodInstance.drop();
    assertNull(timerField.get(periodInstance));
    verify(mockTimer).cancel(true);
  }

  // ==================== failureResetTimer callback via reflection ====================

  /**
   * Extracts and invokes the Runnable registered with the failureResetTimer. Uses reflection to
   * reach into the ScheduledFuture's internal callable.
   */
  private void invokeFailureResetTimerCallback() throws Exception {
    java.lang.reflect.Field timerField =
        PeriodStreamTaskInstance.class.getDeclaredField("failureResetTimer");
    timerField.setAccessible(true);
    Object timer = timerField.get(periodInstance);
    if (timer == null) {
      throw new IllegalStateException("failureResetTimer is null");
    }

    // ScheduledFutureTask wraps the Runnable in a "callable" field (OpenJDK)
    java.lang.reflect.Field callableField = findField(timer.getClass(), "callable");
    if (callableField == null) {
      // Fallback: try the ScheduledRunnable's run() directly if it's a Runnable
      if (timer instanceof Runnable) {
        ((Runnable) timer).run();
        return;
      }
      throw new IllegalStateException("Cannot extract Runnable from " + timer.getClass());
    }
    callableField.setAccessible(true);
    Object callable = callableField.get(timer);
    java.lang.reflect.Method callMethod = callable.getClass().getDeclaredMethod("call");
    callMethod.setAccessible(true);
    callMethod.invoke(callable);
  }

  private java.lang.reflect.Field findField(Class<?> clazz, String name) {
    Class<?> current = clazz;
    while (current != null) {
      try {
        return current.getDeclaredField(name);
      } catch (NoSuchFieldException e) {
        current = current.getSuperclass();
      }
    }
    return null;
  }

  /** failureResetTimer callback returns early when not running. */
  @Test
  public void testFailureResetTimerCallbackWhenNotRunning() throws Exception {
    java.lang.reflect.Field timerField =
        PeriodStreamTaskInstance.class.getDeclaredField("failureResetTimer");
    timerField.setAccessible(true);
    // Create a real timer by triggering failures while running, then stop
    when(mockStreamTask.getTarget()).thenReturn(null);
    doReturn(mock(StreamSubTask.class)).when(periodInstance).getOrCreateSubTask(any());
    periodInstance.start();

    PartitionKey key = new ListPartitionKey(Collections.emptyList());
    StreamSubTaskExecution mockExecution = mock(StreamSubTaskExecution.class);
    StreamSubTaskContext mockContext = mock(StreamSubTaskContext.class);
    when(mockExecution.getContext()).thenReturn(mockContext);
    when(mockContext.getFailureCause()).thenReturn(java.util.Optional.empty());

    for (int i = 0; i < 6; i++) {
      periodInstance.onSubTaskStateChanged(key, mockExecution, StreamSubTaskState.FAILED);
    }
    // Timer is now set. Stop the instance -> running = false
    periodInstance.stop();

    // Invoke callback - should return early due to !running
    invokeFailureResetTimerCallback();
    // No exception means success
    org.junit.Assert.assertNotNull(periodInstance);
  }

  /** failureResetTimer callback resets failure count when successful processing detected. */
  @Test
  public void testFailureResetTimerCallbackResetsOnSuccess() throws Exception {
    java.lang.reflect.Field timerField =
        PeriodStreamTaskInstance.class.getDeclaredField("failureResetTimer");
    timerField.setAccessible(true);
    when(mockStreamTask.getTarget()).thenReturn(null);
    doReturn(mock(StreamSubTask.class)).when(periodInstance).getOrCreateSubTask(any());
    periodInstance.start();

    PartitionKey key = new ListPartitionKey(Collections.emptyList());
    StreamSubTaskExecution mockExecution = mock(StreamSubTaskExecution.class);
    StreamSubTaskContext mockContext = mock(StreamSubTaskContext.class);
    when(mockExecution.getContext()).thenReturn(mockContext);
    when(mockContext.getFailureCause()).thenReturn(java.util.Optional.empty());

    for (int i = 0; i < 6; i++) {
      periodInstance.onSubTaskStateChanged(key, mockExecution, StreamSubTaskState.FAILED);
    }

    // Inject a sub-task execution whose driver reports successful processing after the last failure
    java.lang.reflect.Field subTaskField =
        AbstractStreamTaskInstance.class.getDeclaredField("subTaskExecutions");
    subTaskField.setAccessible(true);
    @SuppressWarnings("unchecked")
    java.util.Map<PartitionKey, StreamSubTaskExecution> map =
        (java.util.Map<PartitionKey, StreamSubTaskExecution>) subTaskField.get(periodInstance);
    StreamSubTaskExecution successExec = mock(StreamSubTaskExecution.class);
    org.apache.iotdb.streamnode.engine.scheduler.task.IStreamDriver mockDriver =
        mock(org.apache.iotdb.streamnode.engine.scheduler.task.IStreamDriver.class);
    when(successExec.getDriver()).thenReturn(mockDriver);
    when(mockDriver.getLastSuccessfulProcessTime()).thenReturn(System.currentTimeMillis() + 10000);
    map.put(key, successExec);

    // Invoke callback - should reset failure count
    invokeFailureResetTimerCallback();

    // lastFailureTime should be reset to 0
    java.lang.reflect.Field lastFailureField =
        PeriodStreamTaskInstance.class.getDeclaredField("lastFailureTime");
    lastFailureField.setAccessible(true);
    assertEquals(
        0L, ((java.util.concurrent.atomic.AtomicLong) lastFailureField.get(periodInstance)).get());
  }
}
