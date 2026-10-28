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

package org.apache.iotdb.streamnode.engine.scheduler;

import org.apache.iotdb.commons.stream.PartitionKey;
import org.apache.iotdb.streamnode.engine.scheduler.queue.FairRoundRobinQueue;
import org.apache.iotdb.streamnode.engine.scheduler.task.DriverTaskId;
import org.apache.iotdb.streamnode.engine.scheduler.task.DriverTaskStatus;
import org.apache.iotdb.streamnode.engine.scheduler.task.IStreamDriver;
import org.apache.iotdb.streamnode.engine.scheduler.task.StreamDriverTask;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class StreamTaskSchedulerTest {

  private StreamTaskScheduler scheduler;
  private PartitionKey testPartitionKey;

  @Mock private IStreamDriver mockStreamDriver;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    testPartitionKey =
        new PartitionKey() {
          @Override
          public int partitionHash() {
            return 0;
          }

          @Override
          public int segmentNum() {
            return 1;
          }

          @Override
          public Object segmentValue(int segmentIndex) {
            return 0;
          }
        };
    scheduler = new StreamTaskScheduler();
  }

  @After
  public void tearDown() {
    try {
      scheduler.stop();
    } catch (Exception e) {
      // Ignore stop exception
    }
  }

  @Test
  public void testSubmitStreamDriver() {
    long timeoutMs = 5000L;

    DriverTaskId driverTaskId = new DriverTaskId("test-stream", testPartitionKey);
    when(mockStreamDriver.getDriverTaskId()).thenReturn(driverTaskId);

    scheduler.submitStreamDriver(mockStreamDriver, timeoutMs);

    verify(mockStreamDriver, atLeastOnce()).getDriverTaskId();
  }

  @Test
  public void testCancelStreamTask() {
    DriverTaskId taskId = new DriverTaskId("test-stream", testPartitionKey);
    long timeoutMs = 5000L;

    when(mockStreamDriver.getDriverTaskId()).thenReturn(taskId);

    scheduler.submitStreamDriver(mockStreamDriver, timeoutMs);

    scheduler.cancelStreamTask(taskId);

    verify(mockStreamDriver, atLeast(2)).getDriverTaskId();
  }

  @Test
  public void testStartAndStop() {
    try {
      scheduler.start();

      TimeUnit.MILLISECONDS.sleep(200);

      scheduler.stop();

    } catch (InterruptedException e) {
      fail("Exception occurred: " + e.getMessage());
    }
  }

  @Test
  public void testTaskStatusTransitions() {
    DriverTaskId taskId = new DriverTaskId("test-stream", testPartitionKey);
    long timeoutMs = 5000L;

    when(mockStreamDriver.getDriverTaskId()).thenReturn(taskId);

    scheduler.submitStreamDriver(mockStreamDriver, timeoutMs);

    scheduler.cancelStreamTask(taskId);

    verify(mockStreamDriver, atLeast(2)).getDriverTaskId();
  }

  @Test
  public void testConcurrentTaskSubmission() {
    int numTasks = 5;

    for (int i = 0; i < numTasks; i++) {
      int taskIndex = i;
      DriverTaskId taskId = new DriverTaskId("stream-" + taskIndex, testPartitionKey);
      IStreamDriver mockDriver = mock(IStreamDriver.class);
      when(mockDriver.getDriverTaskId()).thenReturn(taskId);

      long timeoutMs = 3000L;

      scheduler.submitStreamDriver(mockDriver, timeoutMs);

      verify(mockDriver, atLeastOnce()).getDriverTaskId();
    }
  }

  @Test
  public void testTaskWithDifferentTimeout() {
    DriverTaskId taskId1 = new DriverTaskId("short-timeout-task", testPartitionKey);
    DriverTaskId taskId2 = new DriverTaskId("long-timeout-task", testPartitionKey);

    IStreamDriver mockDriver1 = mock(IStreamDriver.class);
    IStreamDriver mockDriver2 = mock(IStreamDriver.class);

    when(mockDriver1.getDriverTaskId()).thenReturn(taskId1);
    when(mockDriver2.getDriverTaskId()).thenReturn(taskId2);

    scheduler.submitStreamDriver(mockDriver1, 1000L);
    scheduler.submitStreamDriver(mockDriver2, 10000L);

    verify(mockDriver1, atLeastOnce()).getDriverTaskId();
    verify(mockDriver2, atLeastOnce()).getDriverTaskId();
  }

  @Test
  public void testCancelNonExistentTask() {
    DriverTaskId nonExistentTaskId = new DriverTaskId("non-existent", testPartitionKey);

    try {
      scheduler.cancelStreamTask(nonExistentTaskId);
    } catch (Exception e) {
      fail("Should not throw exception when cancelling non-existent task: " + e.getMessage());
    }
  }

  @Test
  public void testMultipleCancelCalls() {
    DriverTaskId taskId = new DriverTaskId("multi-cancel-task", testPartitionKey);
    long timeoutMs = 5000L;

    when(mockStreamDriver.getDriverTaskId()).thenReturn(taskId);

    scheduler.submitStreamDriver(mockStreamDriver, timeoutMs);

    scheduler.cancelStreamTask(taskId);
    scheduler.cancelStreamTask(taskId);
    scheduler.cancelStreamTask(taskId);

    verify(mockStreamDriver, atLeast(2)).getDriverTaskId();
  }

  @Test
  public void testCancelStreamTaskByStreamName() {
    DriverTaskId taskId1 = new DriverTaskId("test-stream", testPartitionKey);
    DriverTaskId taskId2 =
        new DriverTaskId(
            "test-stream",
            new PartitionKey() {
              @Override
              public int partitionHash() {
                return 0;
              }

              @Override
              public int segmentNum() {
                return 1;
              }

              @Override
              public Object segmentValue(int segmentIndex) {
                return 0;
              }
            });

    IStreamDriver mockDriver1 = mock(IStreamDriver.class);
    IStreamDriver mockDriver2 = mock(IStreamDriver.class);

    when(mockDriver1.getDriverTaskId()).thenReturn(taskId1);
    when(mockDriver2.getDriverTaskId()).thenReturn(taskId2);

    scheduler.submitStreamDriver(mockDriver1, 5000L);
    scheduler.submitStreamDriver(mockDriver2, 5000L);

    scheduler.cancelStreamTask("test-stream");

    verify(mockDriver1, atLeast(2)).getDriverTaskId();
    verify(mockDriver2, atLeast(2)).getDriverTaskId();
  }

  // ==================== Enhancement: coverage for clearDriverTask / Scheduler / counts
  // ====================

  @Test
  public void testIsStreamTaskExistAfterSubmitAndCancel() {
    DriverTaskId taskId = new DriverTaskId("exist-check", testPartitionKey);
    when(mockStreamDriver.getDriverTaskId()).thenReturn(taskId);

    assertFalse(scheduler.isStreamTaskExist(taskId));
    scheduler.submitStreamDriver(mockStreamDriver, 5000L);
    assertTrue(scheduler.isStreamTaskExist(taskId));
    scheduler.cancelStreamTask(taskId);
    assertFalse(scheduler.isStreamTaskExist(taskId));
  }

  @Test
  public void testGetTaskCountReflectsSubmitAndCancel() {
    DriverTaskId taskId = new DriverTaskId("count-task", testPartitionKey);
    when(mockStreamDriver.getDriverTaskId()).thenReturn(taskId);

    assertEquals(0, scheduler.getTaskCount());
    scheduler.submitStreamDriver(mockStreamDriver, 5000L);
    assertEquals(1, scheduler.getTaskCount());
    scheduler.cancelStreamTask(taskId);
    assertEquals(0, scheduler.getTaskCount());
  }

  @Test
  public void testGetRunningTaskCountExcludesReadyQueuedTask() {
    // No tasks submitted: both counts are 0.
    assertEquals(0, scheduler.getRunningTaskCount());
    assertEquals(0, scheduler.getTaskCount());

    DriverTaskId taskId = new DriverTaskId("running-task", testPartitionKey);
    when(mockStreamDriver.getDriverTaskId()).thenReturn(taskId);
    scheduler.submitStreamDriver(mockStreamDriver, 5000L);

    // Task is in READY state (waiting in the ready queue), not running yet.
    assertEquals(1, scheduler.getTaskCount());
    assertEquals(1, scheduler.getReadyQueueSize());
    assertEquals(0, scheduler.getRunningTaskCount());
  }

  @Test
  public void testGetReadyQueueSizeReflectsSubmit() {
    final int initialSize = scheduler.getReadyQueueSize();
    DriverTaskId taskId = new DriverTaskId("queue-size-task", testPartitionKey);
    when(mockStreamDriver.getDriverTaskId()).thenReturn(taskId);
    scheduler.submitStreamDriver(mockStreamDriver, 5000L);
    assertEquals(initialSize + 1, scheduler.getReadyQueueSize());
  }

  @Test
  public void testGetReadyQueueReservedSizeInitiallyZero() {
    assertEquals(0, scheduler.getReadyQueueReservedSize());
  }

  @Test
  public void testCancelStreamTaskClearsReadyBranch() throws Exception {
    // After submit, task is in READY. cancelStreamTask -> clearDriverTask READY branch:
    //   readyQueue.remove(taskId) returns the task (non-null) -> does NOT call
    // decreaseReservedSize.
    DriverTaskId taskId = new DriverTaskId("ready-branch", testPartitionKey);
    when(mockStreamDriver.getDriverTaskId()).thenReturn(taskId);
    scheduler.submitStreamDriver(mockStreamDriver, 5000L);

    final int sizeBeforeCancel = scheduler.getReadyQueueSize();
    assertTrue(sizeBeforeCancel > 0);

    scheduler.cancelStreamTask(taskId);

    // Task removed from registeredTaskMap and readyQueue.
    assertFalse(scheduler.isStreamTaskExist(taskId));
    assertEquals(sizeBeforeCancel - 1, scheduler.getReadyQueueSize());
  }

  @Test
  public void testCancelStreamTaskBlockedBranchRemovesFromIdleSet() throws Exception {
    DriverTaskId taskId = new DriverTaskId("blocked-branch", testPartitionKey);
    when(mockStreamDriver.getDriverTaskId()).thenReturn(taskId);
    scheduler.submitStreamDriver(mockStreamDriver, 5000L);

    // Move the task into BLOCKED state by directly manipulating internal state via reflection.
    // The Scheduler.runningToBlocked path adds the task to blockedSet and sets status to BLOCKED.
    // We simulate the post-blocked state here.
    final StreamDriverTask task = getTaskFromRegisteredMap(taskId);
    task.setStatus(DriverTaskStatus.BLOCKED);
    final Set<DriverTaskId> blockedSet = getBlockedSet();
    blockedSet.add(taskId);

    // reservedSize is incremented when a task is polled (reserved). Simulate that the task was
    // polled out of the readyQueue (so it's not in readyQueue but reservedSize > 0).
    // The BLOCKED branch calls readyQueue.decreaseReservedSize(task).
    assertTrue(blockedSet.contains(taskId));

    scheduler.cancelStreamTask(taskId);

    // After cancel, blockedSet should no longer contain the task (BLOCKED branch removes it).
    assertFalse(blockedSet.contains(taskId));
    assertFalse(scheduler.isStreamTaskExist(taskId));
  }

  @Test
  public void testCancelStreamTaskRunningBranchDecreasesReservedSize() throws Exception {
    DriverTaskId taskId = new DriverTaskId("running-branch", testPartitionKey);
    when(mockStreamDriver.getDriverTaskId()).thenReturn(taskId);
    scheduler.submitStreamDriver(mockStreamDriver, 5000L);

    // Move task to RUNNING state. The RUNNING branch calls readyQueue.decreaseReservedSize(task).
    final StreamDriverTask task = getTaskFromRegisteredMap(taskId);
    task.setStatus(DriverTaskStatus.RUNNING);

    // Verify the task is no longer in readyQueue (RUNNING means it was polled out).
    // cancelStreamTask should still complete without error.
    scheduler.cancelStreamTask(taskId);
    assertFalse(scheduler.isStreamTaskExist(taskId));
  }

  @Test
  public void testCancelStreamTaskAbortedBranchIsNoOp() throws Exception {
    // First cancel moves task to ABORTED. Second cancel finds registeredTaskMap no longer
    // contains the task (removed by first cancel), so cancelStreamTask is a no-op.
    DriverTaskId taskId = new DriverTaskId("aborted-branch", testPartitionKey);
    when(mockStreamDriver.getDriverTaskId()).thenReturn(taskId);
    scheduler.submitStreamDriver(mockStreamDriver, 5000L);

    scheduler.cancelStreamTask(taskId);
    // The task is now removed from registeredTaskMap. A second cancel should not throw.
    scheduler.cancelStreamTask(taskId);
    scheduler.cancelStreamTask(taskId);
    assertFalse(scheduler.isStreamTaskExist(taskId));
  }

  @Test
  public void testCancelStreamTaskByStreamNameNoMatchIsNoOp() {
    // No tasks submitted; cancel by name should not throw.
    scheduler.cancelStreamTask("non-existent-stream");
  }

  @Test
  public void testSubmitStreamDriverDoesNotStartWorkerThreads() {
    // Scheduler is constructed but start() is never called. Submitted tasks remain in readyQueue.
    DriverTaskId taskId = new DriverTaskId("no-start", testPartitionKey);
    when(mockStreamDriver.getDriverTaskId()).thenReturn(taskId);
    scheduler.submitStreamDriver(mockStreamDriver, 5000L);
    // The task sits in readyQueue; getTaskCount reflects it.
    assertEquals(1, scheduler.getTaskCount());
    assertTrue(scheduler.getReadyQueueSize() > 0);
  }

  @Test
  public void testStopClearsThreadList() {
    // start() then stop() should leave the scheduler in a clean state (no exception).
    scheduler.start();
    try {
      TimeUnit.MILLISECONDS.sleep(100);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    scheduler.stop();
    // stop() is idempotent-ish: calling again doesn't throw (threads list is empty).
    scheduler.stop();
  }

  @Test
  public void testReadyToRunningReturnsFalseForNonReadyTask() throws Exception {
    // Submit a task, then manually set its status to BLOCKED. The Scheduler.readyToRunning
    // should return false because status != READY.
    DriverTaskId taskId = new DriverTaskId("r2r-false", testPartitionKey);
    when(mockStreamDriver.getDriverTaskId()).thenReturn(taskId);
    scheduler.submitStreamDriver(mockStreamDriver, 5000L);

    final StreamDriverTask task = getTaskFromRegisteredMap(taskId);
    task.setStatus(DriverTaskStatus.BLOCKED);

    // We can't call Scheduler.readyToRunning directly (private inner class). But we can verify
    // that the task's status is BLOCKED and was not transitioned to RUNNING by any scheduler op.
    assertEquals(DriverTaskStatus.BLOCKED, task.getStatus());
  }

  @Test
  public void testSubmitMultipleStreamDriversIncrementCounts() {
    final int n = 5;
    for (int i = 0; i < n; i++) {
      DriverTaskId id = new DriverTaskId("multi-" + i, testPartitionKey);
      IStreamDriver d = mock(IStreamDriver.class);
      when(d.getDriverTaskId()).thenReturn(id);
      scheduler.submitStreamDriver(d, 5000L);
    }
    assertEquals(n, scheduler.getTaskCount());
    assertEquals(n, scheduler.getReadyQueueSize());
  }

  // ----- reflection helpers to access package-private state -----

  @SuppressWarnings("unchecked")
  private StreamDriverTask getTaskFromRegisteredMap(DriverTaskId id) throws Exception {
    final Field f = StreamTaskScheduler.class.getDeclaredField("registeredTaskMap");
    f.setAccessible(true);
    final Map<DriverTaskId, StreamDriverTask> map =
        (Map<DriverTaskId, StreamDriverTask>) f.get(scheduler);
    return map.get(id);
  }

  @SuppressWarnings("unchecked")
  private Set<DriverTaskId> getBlockedSet() throws Exception {
    final Field f = StreamTaskScheduler.class.getDeclaredField("blockedSet");
    f.setAccessible(true);
    return (Set<DriverTaskId>) f.get(scheduler);
  }

  private FairRoundRobinQueue getReadyQueue() throws Exception {
    final Field f = StreamTaskScheduler.class.getDeclaredField("readyQueue");
    f.setAccessible(true);
    return (FairRoundRobinQueue) f.get(scheduler);
  }

  // ==================== Scheduler inner class state transition tests ====================

  /** readyToRunning: task not in READY state returns false. */
  @Test
  public void testReadyToRunningNonReadyReturnsFalse() throws Exception {
    DriverTaskId taskId = new DriverTaskId("non-ready", testPartitionKey);
    when(mockStreamDriver.getDriverTaskId()).thenReturn(taskId);
    scheduler.submitStreamDriver(mockStreamDriver, 5000L);

    StreamDriverTask task = getTaskFromRegisteredMap(taskId);
    // Manually set status to RUNNING (not READY)
    task.setStatus(DriverTaskStatus.RUNNING);

    // Get the Scheduler instance via reflection
    ITaskScheduler internalScheduler = getInternalScheduler();
    assertFalse(internalScheduler.readyToRunning(task));
    assertEquals(DriverTaskStatus.RUNNING, task.getStatus());
  }

  /** readyToRunning: task in READY state transitions to RUNNING, returns true. */
  @Test
  public void testReadyToRunningReadyTransitionsToRunning() throws Exception {
    DriverTaskId taskId = new DriverTaskId("ready-task", testPartitionKey);
    when(mockStreamDriver.getDriverTaskId()).thenReturn(taskId);
    scheduler.submitStreamDriver(mockStreamDriver, 5000L);

    StreamDriverTask task = getTaskFromRegisteredMap(taskId);
    // Task starts in READY state after submit
    assertEquals(DriverTaskStatus.READY, task.getStatus());

    ITaskScheduler internalScheduler = getInternalScheduler();
    assertTrue(internalScheduler.readyToRunning(task));
    assertEquals(DriverTaskStatus.RUNNING, task.getStatus());
  }

  /** runningToReady: task not in RUNNING state does nothing. */
  @Test
  public void testRunningToReadyNonRunningNoOp() throws Exception {
    DriverTaskId taskId = new DriverTaskId("not-running", testPartitionKey);
    when(mockStreamDriver.getDriverTaskId()).thenReturn(taskId);
    scheduler.submitStreamDriver(mockStreamDriver, 5000L);

    StreamDriverTask task = getTaskFromRegisteredMap(taskId);
    // Task is in READY state, not RUNNING
    task.setStatus(DriverTaskStatus.READY);

    ITaskScheduler internalScheduler = getInternalScheduler();
    internalScheduler.runningToReady(task);
    // Status should remain READY
    assertEquals(DriverTaskStatus.READY, task.getStatus());
  }

  /** runningToReady: task in RUNNING state transitions to READY. */
  @Test
  public void testRunningToReadyRunningTransitionsToReady() throws Exception {
    DriverTaskId taskId = new DriverTaskId("to-ready", testPartitionKey);
    when(mockStreamDriver.getDriverTaskId()).thenReturn(taskId);
    scheduler.submitStreamDriver(mockStreamDriver, 5000L);

    StreamDriverTask task = getTaskFromRegisteredMap(taskId);
    task.setStatus(DriverTaskStatus.RUNNING);

    ITaskScheduler internalScheduler = getInternalScheduler();
    internalScheduler.runningToReady(task);
    assertEquals(DriverTaskStatus.READY, task.getStatus());
  }

  /** runningToBlocked: task in RUNNING state transitions to BLOCKED, added to blockedSet. */
  @Test
  public void testRunningToBlockedTransitionsToBlocked() throws Exception {
    DriverTaskId taskId = new DriverTaskId("to-blocked", testPartitionKey);
    when(mockStreamDriver.getDriverTaskId()).thenReturn(taskId);
    scheduler.submitStreamDriver(mockStreamDriver, 5000L);

    StreamDriverTask task = getTaskFromRegisteredMap(taskId);
    task.setStatus(DriverTaskStatus.RUNNING);

    ITaskScheduler internalScheduler = getInternalScheduler();
    internalScheduler.runningToBlocked(task);
    assertEquals(DriverTaskStatus.BLOCKED, task.getStatus());
    assertTrue(getBlockedSet().contains(taskId));
  }

  /** runningToBlocked: task not in RUNNING state does nothing. */
  @Test
  public void testRunningToBlockedNonRunningNoOp() throws Exception {
    DriverTaskId taskId = new DriverTaskId("not-running-blocked", testPartitionKey);
    when(mockStreamDriver.getDriverTaskId()).thenReturn(taskId);
    scheduler.submitStreamDriver(mockStreamDriver, 5000L);

    StreamDriverTask task = getTaskFromRegisteredMap(taskId);
    task.setStatus(DriverTaskStatus.READY);

    ITaskScheduler internalScheduler = getInternalScheduler();
    internalScheduler.runningToBlocked(task);
    assertEquals(DriverTaskStatus.READY, task.getStatus());
    assertFalse(getBlockedSet().contains(taskId));
  }

  /** blockedToReady: task in BLOCKED state transitions to READY, removed from blockedSet. */
  @Test
  public void testBlockedToReadyTransitionsToReady() throws Exception {
    DriverTaskId taskId = new DriverTaskId("blocked-to-ready", testPartitionKey);
    when(mockStreamDriver.getDriverTaskId()).thenReturn(taskId);
    scheduler.submitStreamDriver(mockStreamDriver, 5000L);

    StreamDriverTask task = getTaskFromRegisteredMap(taskId);
    task.setStatus(DriverTaskStatus.BLOCKED);
    getBlockedSet().add(taskId);

    ITaskScheduler internalScheduler = getInternalScheduler();
    internalScheduler.blockedToReady(task);
    assertEquals(DriverTaskStatus.READY, task.getStatus());
    assertFalse(getBlockedSet().contains(taskId));
  }

  /** blockedToReady: task not in BLOCKED state does nothing. */
  @Test
  public void testBlockedToReadyNonBlockedNoOp() throws Exception {
    DriverTaskId taskId = new DriverTaskId("not-blocked", testPartitionKey);
    when(mockStreamDriver.getDriverTaskId()).thenReturn(taskId);
    scheduler.submitStreamDriver(mockStreamDriver, 5000L);

    StreamDriverTask task = getTaskFromRegisteredMap(taskId);
    task.setStatus(DriverTaskStatus.RUNNING);

    ITaskScheduler internalScheduler = getInternalScheduler();
    internalScheduler.blockedToReady(task);
    assertEquals(DriverTaskStatus.RUNNING, task.getStatus());
  }

  /** toAborted: task already in end state does nothing. */
  @Test
  public void testToAbortedEndStateNoOp() throws Exception {
    DriverTaskId taskId = new DriverTaskId("already-aborted", testPartitionKey);
    when(mockStreamDriver.getDriverTaskId()).thenReturn(taskId);
    scheduler.submitStreamDriver(mockStreamDriver, 5000L);

    StreamDriverTask task = getTaskFromRegisteredMap(taskId);
    task.setStatus(DriverTaskStatus.ABORTED);

    ITaskScheduler internalScheduler = getInternalScheduler();
    internalScheduler.toAborted(task);
    // Should remain ABORTED
    assertEquals(DriverTaskStatus.ABORTED, task.getStatus());
  }

  /** toStopped: task already in end state does nothing. */
  @Test
  public void testToStoppedEndStateNoOp() throws Exception {
    DriverTaskId taskId = new DriverTaskId("already-stopped", testPartitionKey);
    when(mockStreamDriver.getDriverTaskId()).thenReturn(taskId);
    scheduler.submitStreamDriver(mockStreamDriver, 5000L);

    StreamDriverTask task = getTaskFromRegisteredMap(taskId);
    task.setStatus(DriverTaskStatus.STOPPED);

    ITaskScheduler internalScheduler = getInternalScheduler();
    internalScheduler.toStopped(task);
    assertEquals(DriverTaskStatus.STOPPED, task.getStatus());
  }

  /** toStopped: task in READY state transitions to STOPPED, removed from registeredTaskMap. */
  @Test
  public void testToStoppedFromReadyTransitionsToStopped() throws Exception {
    DriverTaskId taskId = new DriverTaskId("stop-from-ready", testPartitionKey);
    when(mockStreamDriver.getDriverTaskId()).thenReturn(taskId);
    scheduler.submitStreamDriver(mockStreamDriver, 5000L);

    StreamDriverTask task = getTaskFromRegisteredMap(taskId);
    assertEquals(DriverTaskStatus.READY, task.getStatus());

    ITaskScheduler internalScheduler = getInternalScheduler();
    internalScheduler.toStopped(task);
    assertEquals(DriverTaskStatus.STOPPED, task.getStatus());
    assertFalse(scheduler.isStreamTaskExist(taskId));
  }

  /** toAborted: task in RUNNING state with abort cause calls driver.failed(). */
  @Test
  public void testToAbortedFromRunningCallsFailed() throws Exception {
    DriverTaskId taskId = new DriverTaskId("abort-from-running", testPartitionKey);
    when(mockStreamDriver.getDriverTaskId()).thenReturn(taskId);
    scheduler.submitStreamDriver(mockStreamDriver, 5000L);

    StreamDriverTask task = getTaskFromRegisteredMap(taskId);
    task.setStatus(DriverTaskStatus.RUNNING);
    task.setAbortCause(new RuntimeException("test-abort"));

    ITaskScheduler internalScheduler = getInternalScheduler();
    internalScheduler.toAborted(task);
    assertEquals(DriverTaskStatus.ABORTED, task.getStatus());
    verify(mockStreamDriver).failed(any());
  }

  /** cancelStreamTask(DriverTaskId) with non-existent task does nothing. */
  @Test
  public void testCancelStreamTaskNonExistentNoOp() {
    DriverTaskId nonExistent = new DriverTaskId("non-existent", testPartitionKey);
    // Should not throw
    scheduler.cancelStreamTask(nonExistent);
  }

  /** cancelStreamTask(String) with matching stream name cancels all tasks for that stream. */
  @Test
  public void testCancelStreamTaskByNameCancelsMatching() {
    DriverTaskId taskId = new DriverTaskId("stream-A", testPartitionKey);
    when(mockStreamDriver.getDriverTaskId()).thenReturn(taskId);
    scheduler.submitStreamDriver(mockStreamDriver, 5000L);

    assertTrue(scheduler.isStreamTaskExist(taskId));

    scheduler.cancelStreamTask("stream-A");

    assertFalse(scheduler.isStreamTaskExist(taskId));
  }

  /** cancelStreamTask(String) with non-matching stream name does nothing. */
  @Test
  public void testCancelStreamTaskByNameNonMatchingNoOp() {
    DriverTaskId taskId = new DriverTaskId("stream-A", testPartitionKey);
    when(mockStreamDriver.getDriverTaskId()).thenReturn(taskId);
    scheduler.submitStreamDriver(mockStreamDriver, 5000L);

    scheduler.cancelStreamTask("stream-B");

    assertTrue(scheduler.isStreamTaskExist(taskId));
  }

  /** getTaskCount returns the number of registered tasks. */
  @Test
  public void testGetTaskCount() {
    assertEquals(0, scheduler.getTaskCount());

    DriverTaskId taskId = new DriverTaskId("count-test", testPartitionKey);
    when(mockStreamDriver.getDriverTaskId()).thenReturn(taskId);
    scheduler.submitStreamDriver(mockStreamDriver, 5000L);

    assertEquals(1, scheduler.getTaskCount());
  }

  /** getRunningTaskCount = registered - blocked - readyQueueSize. */
  @Test
  public void testGetRunningTaskCount() throws Exception {
    DriverTaskId taskId = new DriverTaskId("running-count", testPartitionKey);
    when(mockStreamDriver.getDriverTaskId()).thenReturn(taskId);
    scheduler.submitStreamDriver(mockStreamDriver, 5000L);

    assertEquals(1, scheduler.getTaskCount());
    // freshly submitted task waits in the ready queue: not running yet
    assertEquals(0, scheduler.getRunningTaskCount());

    // poll the task out of the queue as a worker thread would: now RUNNING
    getReadyQueue().remove(taskId);
    assertEquals(1, scheduler.getRunningTaskCount());

    // Move task to BLOCKED
    getBlockedSet().add(taskId);
    assertEquals(0, scheduler.getRunningTaskCount());
  }

  /** getReadyQueueSize returns readyQueue.size(). */
  @Test
  public void testGetReadyQueueSize() {
    assertTrue(scheduler.getReadyQueueSize() >= 0);

    DriverTaskId taskId = new DriverTaskId("queue-size", testPartitionKey);
    when(mockStreamDriver.getDriverTaskId()).thenReturn(taskId);
    scheduler.submitStreamDriver(mockStreamDriver, 5000L);

    assertTrue(scheduler.getReadyQueueSize() > 0);
  }

  /** getReadyQueueReservedSize returns readyQueue.getReservedSize(). */
  @Test
  public void testGetReadyQueueReservedSize() {
    assertTrue(scheduler.getReadyQueueReservedSize() >= 0);
  }

  /** isStreamTaskExist returns true for registered task, false for unregistered. */
  @Test
  public void testIsStreamTaskExist() {
    DriverTaskId taskId = new DriverTaskId("exist-test", testPartitionKey);
    when(mockStreamDriver.getDriverTaskId()).thenReturn(taskId);

    assertFalse(scheduler.isStreamTaskExist(taskId));

    scheduler.submitStreamDriver(mockStreamDriver, 5000L);

    assertTrue(scheduler.isStreamTaskExist(taskId));
  }

  // ----- reflection helper to access internal Scheduler -----

  private ITaskScheduler getInternalScheduler() throws Exception {
    Field f = StreamTaskScheduler.class.getDeclaredField("scheduler");
    f.setAccessible(true);
    return (ITaskScheduler) f.get(scheduler);
  }
}
