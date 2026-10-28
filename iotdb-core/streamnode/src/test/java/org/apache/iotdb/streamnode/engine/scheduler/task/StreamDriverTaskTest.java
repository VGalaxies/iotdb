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
import org.apache.iotdb.commons.stream.PartitionKey;
import org.apache.iotdb.streamnode.engine.scheduler.DriverTaskHandle;
import org.apache.iotdb.streamnode.engine.scheduler.queue.Priority;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class StreamDriverTaskTest {

  @Mock private IStreamDriver mockDriver;
  @Mock private DriverTaskHandle mockHandle;
  @Mock private DriverTaskHandle mockHandleAlt;

  private DriverTaskId idA;
  private DriverTaskId idB;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    idA = new DriverTaskId("stream-a", new ListPartitionKey(Collections.emptyList()));
    idB = new DriverTaskId("stream-b", new ListPartitionKey(Collections.emptyList()));
    when(mockDriver.getDriverTaskId()).thenReturn(idA);
  }

  @Test
  public void testNoArgConstructorCreatesStubDriverTaskId() {
    final StreamDriverTask stub = new StreamDriverTask();
    assertNotNull(stub.getDriverTaskId());
    assertEquals("stub-stream", stub.getDriverTaskId().getId());
  }

  @Test
  public void testStatusGetAndSet() {
    final StreamDriverTask task = new StreamDriverTask(mockDriver, 0, mockHandle);
    assertEquals(DriverTaskStatus.READY, task.getStatus());
    task.setStatus(DriverTaskStatus.RUNNING);
    assertEquals(DriverTaskStatus.RUNNING, task.getStatus());
    task.setStatus(DriverTaskStatus.BLOCKED);
    assertEquals(DriverTaskStatus.BLOCKED, task.getStatus());
    task.setStatus(DriverTaskStatus.ABORTED);
    assertEquals(DriverTaskStatus.ABORTED, task.getStatus());
  }

  @Test
  public void testIsEndState() {
    final StreamDriverTask task = new StreamDriverTask(mockDriver, 0, mockHandle);
    assertFalse(task.isEndState());
    task.setStatus(DriverTaskStatus.RUNNING);
    assertFalse(task.isEndState());
    task.setStatus(DriverTaskStatus.BLOCKED);
    assertFalse(task.isEndState());
    task.setStatus(DriverTaskStatus.ABORTED);
    assertTrue(task.isEndState());
  }

  @Test
  public void testSetQuantaScheduledNanosAccumulatesTotal() {
    final StreamDriverTask task = new StreamDriverTask(mockDriver, 0, mockHandle);
    assertEquals(0L, task.getTotalScheduledNanos());
    assertEquals(0L, task.getQuantaScheduledNanos());

    task.setQuantaScheduledNanos(100L);
    assertEquals(100L, task.getQuantaScheduledNanos());
    assertEquals(100L, task.getTotalScheduledNanos());

    task.setQuantaScheduledNanos(50L);
    assertEquals(50L, task.getQuantaScheduledNanos());
    assertEquals(150L, task.getTotalScheduledNanos());
  }

  @Test
  public void testUpdateSchedulePriorityDelegatesToHandle() {
    final Priority newP = new Priority(12345L);
    when(mockHandle.updateScheduledTimeInNanos(ArgumentMatchers.anyLong())).thenReturn(newP);
    final StreamDriverTask task = new StreamDriverTask(mockDriver, 0, mockHandle);
    task.setQuantaScheduledNanos(99L);
    task.updateSchedulePriority();
    assertEquals(12345L, task.getPriority().getLastScheduledTime());
  }

  @Test
  public void testMarkAndReleaseReservedInReadyQueue() {
    final StreamDriverTask task = new StreamDriverTask(mockDriver, 0, mockHandle);
    // First release returns false because we never marked it reserved.
    assertFalse(task.releaseReservedInReadyQueue());
    task.markReservedInReadyQueue();
    assertTrue(task.releaseReservedInReadyQueue());
    // Second release returns false because already released.
    assertFalse(task.releaseReservedInReadyQueue());
  }

  @Test
  public void testAbortCauseOptional() {
    final StreamDriverTask task = new StreamDriverTask(mockDriver, 0, mockHandle);
    assertFalse(task.getAbortCause().isPresent());
    final Throwable cause = new RuntimeException("boom");
    task.setAbortCause(cause);
    final Optional<Throwable> opt = task.getAbortCause();
    assertTrue(opt.isPresent());
    assertEquals(cause, opt.get());
  }

  @Test
  public void testAddReadyQueuedTimeAccumulates() {
    final StreamDriverTask task = new StreamDriverTask(mockDriver, 0, mockHandle);
    assertEquals(0L, task.getReadyQueuedTime());
    task.addReadyQueuedTime(100L);
    assertEquals(100L, task.getReadyQueuedTime());
    task.addReadyQueuedTime(200L);
    assertEquals(300L, task.getReadyQueuedTime());
  }

  @Test
  public void testLockIsReentrant() {
    // StreamDriverTask.lock() is backed by ReentrantLock; acquiring twice from the same
    // thread must not deadlock.
    final StreamDriverTask task = new StreamDriverTask(mockDriver, 0, mockHandle);
    task.lock();
    task.lock();
    task.unlock();
    task.unlock();
    // No deadlock = pass. Re-acquire after release should also work.
    task.lock();
    task.unlock();
  }

  @Test
  public void testGetDriverDelegatesToUnderlying() {
    final StreamDriverTask task = new StreamDriverTask(mockDriver, 0, mockHandle);
    assertEquals(mockDriver, task.getDriver());
  }

  @Test
  public void testGetDriverTaskIdDelegatesToDriver() {
    final StreamDriverTask task = new StreamDriverTask(mockDriver, 0, mockHandle);
    assertEquals(idA, task.getDriverTaskId());
  }

  @Test
  public void testSetIdPropagatesToUnderlyingDriver() {
    final StreamDriverTask task = new StreamDriverTask(mockDriver, 0, mockHandle);
    task.setId(idB);
    // setId delegates to streamDriver.setDriverTaskId; the mock doesn't store state, so we
    // verify the delegation rather than the post-state.
    verify(mockDriver).setDriverTaskId(idB);
  }

  @Test
  public void testWeightDefaultsToOneAndIsMutable() {
    final StreamDriverTask task = new StreamDriverTask(mockDriver, 0, mockHandle);
    assertEquals(1.0, task.getWeight(), 0.0);
    task.setWeight(2.5);
    assertEquals(2.5, task.getWeight(), 0.0);
  }

  @Test
  public void testLastEnterReadyQueueTime() {
    final StreamDriverTask task = new StreamDriverTask(mockDriver, 0, mockHandle);
    task.setLastEnterReadyQueueTime(42L);
    assertEquals(42L, task.getLastEnterReadyQueueTime());
  }

  @Test
  public void testSchedulePriorityComparatorSameIdReturnsZero() {
    final StreamDriverTask t1 = new StreamDriverTask(mockDriver, 0, mockHandle);
    final StreamDriverTask t2 = new StreamDriverTask(mockDriver, 0, mockHandleAlt);
    // Both have the same driverTaskId (idA, since mockDriver.getDriverTaskId() returns idA).
    final StreamDriverTask.SchedulePriorityComparator cmp =
        new StreamDriverTask.SchedulePriorityComparator();
    assertEquals(0, cmp.compare(t1, t2));
  }

  @Test
  public void testSchedulePriorityComparatorDifferentPriority() {
    // Two tasks with different driverTaskIds and different priorities.
    final IStreamDriver driverB = org.mockito.Mockito.mock(IStreamDriver.class);
    when(driverB.getDriverTaskId()).thenReturn(idB);

    final Priority p1 = new Priority(1000L);
    final Priority p2 = new Priority(2000L);
    when(mockHandle.updateScheduledTimeInNanos(org.mockito.ArgumentMatchers.anyLong()))
        .thenReturn(p1);
    when(mockHandleAlt.updateScheduledTimeInNanos(org.mockito.ArgumentMatchers.anyLong()))
        .thenReturn(p2);

    final StreamDriverTask t1 = new StreamDriverTask(mockDriver, 0, mockHandle);
    final StreamDriverTask t2 = new StreamDriverTask(driverB, 0, mockHandleAlt);
    t1.setQuantaScheduledNanos(1L);
    t1.updateSchedulePriority();
    t2.setQuantaScheduledNanos(2L);
    t2.updateSchedulePriority();

    final StreamDriverTask.SchedulePriorityComparator cmp =
        new StreamDriverTask.SchedulePriorityComparator();
    // t1 has smaller priority (1000 < 2000) -> comes first -> compare(t1, t2) < 0.
    assertTrue(cmp.compare(t1, t2) < 0);
    assertTrue(cmp.compare(t2, t1) > 0);
  }

  @Test
  public void testSchedulePriorityComparatorEqualPriorityFallsBackToIdCompareTo() {
    final IStreamDriver driverB = org.mockito.Mockito.mock(IStreamDriver.class);
    when(driverB.getDriverTaskId()).thenReturn(idB);

    final Priority sameP = new Priority(1000L);
    when(mockHandle.updateScheduledTimeInNanos(org.mockito.ArgumentMatchers.anyLong()))
        .thenReturn(sameP);
    when(mockHandleAlt.updateScheduledTimeInNanos(org.mockito.ArgumentMatchers.anyLong()))
        .thenReturn(sameP);

    final StreamDriverTask t1 = new StreamDriverTask(mockDriver, 0, mockHandle);
    final StreamDriverTask t2 = new StreamDriverTask(driverB, 0, mockHandleAlt);
    t1.setQuantaScheduledNanos(1L);
    t1.updateSchedulePriority();
    t2.setQuantaScheduledNanos(2L);
    t2.updateSchedulePriority();

    final StreamDriverTask.SchedulePriorityComparator cmp =
        new StreamDriverTask.SchedulePriorityComparator();
    // Equal priorities -> fallback to idA.compareTo(idB).
    final int expected = idA.compareTo(idB);
    assertEquals(expected, cmp.compare(t1, t2));
    assertEquals(-expected, cmp.compare(t2, t1));
    // Sanity: idA = "stream-a", idB = "stream-b" -> idA < idB -> expected < 0.
    assertTrue(expected < 0);
  }

  @Test
  public void testGetPriorityIsNotNullInitially() {
    final StreamDriverTask task = new StreamDriverTask(mockDriver, 0, mockHandle);
    assertNotNull(task.getPriority());
    // Each construction yields a fresh Priority object (System.nanoTime() based).
    final StreamDriverTask task2 = new StreamDriverTask(mockDriver, 0, mockHandle);
    assertNotSame(task.getPriority(), task2.getPriority());
  }

  @Test
  public void testSetAbortCauseOverwritesPrevious() {
    final StreamDriverTask task = new StreamDriverTask(mockDriver, 0, mockHandle);
    final Throwable first = new RuntimeException("first");
    final Throwable second = new RuntimeException("second");
    task.setAbortCause(first);
    task.setAbortCause(second);
    assertEquals(second, task.getAbortCause().get());
  }

  @Test
  public void testDriverTaskIdWithMultiSegmentPartitionKey() {
    final PartitionKey multi = new ListPartitionKey(Arrays.asList("seg1", "seg2", "seg3"));
    final DriverTaskId multiId = new DriverTaskId("multi-stream", multi);
    when(mockDriver.getDriverTaskId()).thenReturn(multiId);
    final StreamDriverTask task = new StreamDriverTask(mockDriver, 0, mockHandle);
    assertEquals(multiId, task.getDriverTaskId());
    // DriverTaskId has no getPartitionKey() getter; verify the key was embedded via fullId.
    assertTrue(multiId.getFullId().contains("seg1"));
    assertTrue(multiId.getFullId().contains("seg3"));
  }

  // ==================== uncovered getter methods ====================

  @Test
  public void testGetQuantaScheduledNanos() {
    final StreamDriverTask task = new StreamDriverTask(mockDriver, 0, mockHandle);
    task.setQuantaScheduledNanos(500L);
    assertEquals(500L, task.getQuantaScheduledNanos());
  }

  @Test
  public void testGetTotalScheduledNanosAccumulates() {
    final StreamDriverTask task = new StreamDriverTask(mockDriver, 0, mockHandle);
    task.setQuantaScheduledNanos(100L);
    task.setQuantaScheduledNanos(200L);
    task.setQuantaScheduledNanos(300L);
    assertEquals(600L, task.getTotalScheduledNanos());
  }

  @Test
  public void testGetReadyQueuedTime() {
    final StreamDriverTask task = new StreamDriverTask(mockDriver, 0, mockHandle);
    assertEquals(0L, task.getReadyQueuedTime());
    task.addReadyQueuedTime(500L);
    task.addReadyQueuedTime(300L);
    assertEquals(800L, task.getReadyQueuedTime());
  }

  @Test
  public void testUnlockReleasesLock() {
    final StreamDriverTask task = new StreamDriverTask(mockDriver, 0, mockHandle);
    task.lock();
    task.unlock();
    // Should be able to lock again after unlock
    task.lock();
    task.unlock();
  }

  @Test
  public void testGetWeightAndSetWeight() {
    final StreamDriverTask task = new StreamDriverTask(mockDriver, 0, mockHandle);
    assertEquals(1.0, task.getWeight(), 0.001);
    task.setWeight(2.5);
    assertEquals(2.5, task.getWeight(), 0.001);
  }

  @Test
  public void testGetPriorityInitiallyNotNull() {
    final StreamDriverTask task = new StreamDriverTask(mockDriver, 0, mockHandle);
    final Priority p1 = task.getPriority();
    assertNotNull(p1);
  }

  @Test
  public void testIsEndStateForAllStatuses() {
    final StreamDriverTask task = new StreamDriverTask(mockDriver, 0, mockHandle);
    task.setStatus(DriverTaskStatus.READY);
    assertFalse(task.isEndState());
    task.setStatus(DriverTaskStatus.RUNNING);
    assertFalse(task.isEndState());
    task.setStatus(DriverTaskStatus.BLOCKED);
    assertFalse(task.isEndState());
    task.setStatus(DriverTaskStatus.ABORTED);
    assertTrue(task.isEndState());
    task.setStatus(DriverTaskStatus.STOPPED);
    assertTrue(task.isEndState());
  }

  @Test
  public void testGetAbortCauseEmptyWhenNotSet() {
    final StreamDriverTask task = new StreamDriverTask(mockDriver, 0, mockHandle);
    assertFalse(task.getAbortCause().isPresent());
  }
}
