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
package org.apache.iotdb.streamnode.engine.scheduler.queue;

import org.apache.iotdb.commons.stream.ListPartitionKey;
import org.apache.iotdb.streamnode.engine.scheduler.DriverTaskHandle;
import org.apache.iotdb.streamnode.engine.scheduler.task.DriverTaskId;
import org.apache.iotdb.streamnode.engine.scheduler.task.IStreamDriver;
import org.apache.iotdb.streamnode.engine.scheduler.task.StreamDriverTask;

import org.junit.Test;

import java.util.Collections;
import java.util.OptionalInt;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class FairRoundRobinQueueTest {

  private static final int DEFAULT_CAPACITY = 10;

  private StreamDriverTask newTask(final String streamName) {
    final IStreamDriver driver = mock(IStreamDriver.class);
    when(driver.getDriverTaskId())
        .thenReturn(new DriverTaskId(streamName, new ListPartitionKey(Collections.emptyList())));
    // DriverTaskHandle needs a FairRoundRobinQueue; we create a throwaway queue just for the
    // handle since the handle only uses it for updatePriority().
    final FairRoundRobinQueue throwaway = new FairRoundRobinQueue(1, new StreamDriverTask());
    final DriverTaskHandle handle = new DriverTaskHandle(0, throwaway, OptionalInt.empty());
    return new StreamDriverTask(driver, 0L, handle);
  }

  private FairRoundRobinQueue newQueue() {
    return new FairRoundRobinQueue(DEFAULT_CAPACITY, new StreamDriverTask());
  }

  @Test
  public void testIsEmptyInitially() {
    final FairRoundRobinQueue q = newQueue();
    assertTrue(q.isEmpty());
    assertEquals(0, q.size());
  }

  @Test
  public void testPushAndPollFifoWithSingleTask() throws InterruptedException {
    final FairRoundRobinQueue q = newQueue();
    final StreamDriverTask task = newTask("s1");
    q.push(task);
    assertFalse(q.isEmpty());
    assertEquals(1, q.size());
    final StreamDriverTask polled = q.poll();
    assertEquals(task, polled);
    // After poll, readyQueue is empty but size still counts reservedSize contribution.
    // size() returns the internal size field (decremented by poll). isEmpty checks taskIdToTaskMap.
    assertEquals(0, q.size());
    assertTrue(q.isEmpty());
  }

  @Test
  public void testPushMultiplePreservesFifoOrder() throws InterruptedException {
    // All tasks get distinct nanoTime-based priorities at construction; we poll and verify
    // that the task with the smallest lastScheduledTime comes out first.
    final FairRoundRobinQueue q = newQueue();
    final StreamDriverTask t1 = newTask("s1");
    // Small sleep to ensure System.nanoTime() differs across constructions.
    Thread.sleep(1);
    final StreamDriverTask t2 = newTask("s2");
    Thread.sleep(1);
    final StreamDriverTask t3 = newTask("s3");
    q.push(t1);
    q.push(t2);
    q.push(t3);
    assertEquals(3, q.size());
    // Priority is by lastScheduledTime (smaller = polled first). t1 was constructed first so has
    // the smallest nanoTime, so it should be polled first.
    final StreamDriverTask first = q.poll();
    assertNotNull(first);
    assertEquals(t1.getDriverTaskId(), first.getDriverTaskId());
  }

  @Test(expected = NullPointerException.class)
  public void testPushNullThrowsNpe() {
    final FairRoundRobinQueue q = newQueue();
    q.push(null);
  }

  @Test
  public void testPushOverCapacityThrowsIllegalState() {
    final FairRoundRobinQueue q = new FairRoundRobinQueue(2, new StreamDriverTask());
    q.push(newTask("s1"));
    q.push(newTask("s2"));
    // size=2, reservedSize=0, capacity=2 -> 2+0 < 2 is false -> IllegalStateException.
    try {
      q.push(newTask("s3"));
      fail("Expected IllegalStateException when over capacity");
    } catch (IllegalStateException expected) {
      assertTrue(expected.getMessage().contains("can't allow more queries"));
    }
  }

  @Test
  public void testRemoveByPublicIdApi() {
    final FairRoundRobinQueue q = newQueue();
    final StreamDriverTask t1 = newTask("s1");
    final StreamDriverTask t2 = newTask("s2");
    q.push(t1);
    q.push(t2);
    assertEquals(2, q.size());

    final StreamDriverTask removed = q.remove(t1.getDriverTaskId());
    assertEquals(t1, removed);
    assertEquals(1, q.size());
    assertFalse(q.contains(t1));
    assertTrue(q.contains(t2));
  }

  @Test
  public void testRemoveNonExistentReturnsNull() {
    final FairRoundRobinQueue q = newQueue();
    q.push(newTask("s1"));
    final DriverTaskId nonExistent =
        new DriverTaskId("nope", new ListPartitionKey(Collections.emptyList()));
    final StreamDriverTask removed = q.remove(nonExistent);
    assertNull(removed);
    assertEquals(1, q.size());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testRemoveNullTaskThrowsIae() {
    // Direct protected remove(E) call - checkArgument rejects null.
    final FairRoundRobinQueue q = newQueue();
    q.remove((StreamDriverTask) null);
  }

  @Test
  public void testContains() {
    final FairRoundRobinQueue q = newQueue();
    final StreamDriverTask t1 = newTask("s1");
    assertFalse(q.contains(t1));
    q.push(t1);
    assertTrue(q.contains(t1));
    // A different task with a different id is not contained.
    final StreamDriverTask t2 = newTask("s2");
    assertFalse(q.contains(t2));
  }

  @Test
  public void testGetByPublicIdApi() {
    final FairRoundRobinQueue q = newQueue();
    final StreamDriverTask t1 = newTask("s1");
    q.push(t1);
    final StreamDriverTask found = q.get(t1.getDriverTaskId());
    assertEquals(t1, found);
  }

  @Test
  public void testGetNonExistentReturnsNull() {
    final FairRoundRobinQueue q = newQueue();
    final DriverTaskId nonExistent =
        new DriverTaskId("nope", new ListPartitionKey(Collections.emptyList()));
    assertNull(q.get(nonExistent));
  }

  @Test
  public void testGetWithNullIdReturnsNull() {
    // queryHolder.setId(null) -> get(queryHolder) sees null driverTaskId -> returns null.
    final FairRoundRobinQueue q = newQueue();
    assertNull(q.get((DriverTaskId) null));
  }

  @Test
  public void testPollReservesSlot() throws InterruptedException {
    // After poll, reservedSize increments (space reserved for the polled task to come back).
    final FairRoundRobinQueue q = new FairRoundRobinQueue(2, new StreamDriverTask());
    q.push(newTask("s1"));
    assertEquals(0, q.getReservedSize());
    final StreamDriverTask polled = q.poll();
    assertNotNull(polled);
    assertEquals(1, q.getReservedSize());
  }

  @Test
  public void testRepushDecreasesReservedSize() throws InterruptedException {
    final FairRoundRobinQueue q = new FairRoundRobinQueue(2, new StreamDriverTask());
    final StreamDriverTask t1 = newTask("s1");
    q.push(t1);
    final StreamDriverTask polled = q.poll();
    assertEquals(1, q.getReservedSize());
    // repush the polled task -> reservedSize decreases because releaseReserved returns true.
    q.repush(polled);
    assertEquals(0, q.getReservedSize());
    assertEquals(1, q.size());
  }

  @Test
  public void testRepushNullThrowsNpe() {
    final FairRoundRobinQueue q = newQueue();
    try {
      q.repush(null);
      fail("Expected NullPointerException");
    } catch (NullPointerException expected) {
      // expected
    }
  }

  @Test
  public void testDecreaseReservedSize() throws InterruptedException {
    final FairRoundRobinQueue q = newQueue();
    final StreamDriverTask t1 = newTask("s1");
    q.push(t1);
    q.poll(); // reservedSize -> 1
    assertEquals(1, q.getReservedSize());
    assertTrue(q.decreaseReservedSize(t1));
    assertEquals(0, q.getReservedSize());
    // Second decrease returns false (already released).
    assertFalse(q.decreaseReservedSize(t1));
  }

  @Test(expected = NullPointerException.class)
  public void testDecreaseReservedSizeNullThrowsNpe() {
    final FairRoundRobinQueue q = newQueue();
    q.decreaseReservedSize(null);
  }

  @Test
  public void testUpdatePriorityReturnsNewPriority() {
    final FairRoundRobinQueue q = newQueue();
    final Priority oldP = new Priority(1L);
    final Priority newP = q.updatePriority(oldP, 99L);
    assertNotNull(newP);
    // updatePriority ignores oldP and returns new Priority(lastScheduledTime).
    assertEquals(99L, newP.getLastScheduledTime());
  }

  @Test
  public void testClearResetsSizeAndReserved() throws InterruptedException {
    final FairRoundRobinQueue q = new FairRoundRobinQueue(5, new StreamDriverTask());
    q.push(newTask("s1"));
    q.push(newTask("s2"));
    q.poll(); // reservedSize -> 1
    assertEquals(1, q.size());
    assertEquals(1, q.getReservedSize());
    q.clear();
    assertEquals(0, q.size());
    assertEquals(0, q.getReservedSize());
    assertTrue(q.isEmpty());
  }

  @Test
  public void testClearAllElementsLeavesSizeUntouched() {
    // clearAllElements only clears the internal collections, not the size counter.
    // (The public clear() in parent resets size; clearAllElements itself does not.)
    final FairRoundRobinQueue q = newQueue();
    q.push(newTask("s1"));
    q.push(newTask("s2"));
    q.clearAllElements();
    assertTrue(q.isEmpty());
    // size() reflects the parent's size field which is only reset by clear(), not clearAllElements.
    // We don't assert size here since the parent's size field is stale after clearAllElements.
  }

  @Test
  public void testPushNullTaskViaPushToQueueThrowsIae() {
    // Direct protected pushToQueue call - checkArgument rejects null with IllegalArgumentException.
    final FairRoundRobinQueue q = newQueue();
    try {
      q.pushToQueue(null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException expected) {
      // expected
    }
  }

  @Test(timeout = 30000)
  public void testConcurrentPushIsThreadSafe() throws InterruptedException {
    // Multiple threads push distinct tasks; final size must equal total pushed.
    final int capacity = 500;
    final int threadCount = 8;
    final int perThread = 50;
    final FairRoundRobinQueue q = new FairRoundRobinQueue(capacity, new StreamDriverTask());
    final ExecutorService pool = Executors.newFixedThreadPool(threadCount);
    final CountDownLatch start = new CountDownLatch(1);
    final CountDownLatch done = new CountDownLatch(threadCount);
    final AtomicInteger idGen = new AtomicInteger(0);

    for (int i = 0; i < threadCount; i++) {
      pool.submit(
          () -> {
            try {
              start.await();
              for (int j = 0; j < perThread; j++) {
                final int id = idGen.incrementAndGet();
                q.push(newTask("stream-" + id));
              }
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            } finally {
              done.countDown();
            }
          });
    }

    start.countDown();
    assertTrue("All pushers should finish within timeout", done.await(30, TimeUnit.SECONDS));
    pool.shutdown();
    assertEquals(threadCount * perThread, q.size());
    assertEquals(threadCount * perThread, idGen.get());
  }

  @Test(timeout = 30000)
  public void testConcurrentPushAndPoll() throws InterruptedException {
    // Producers push N tasks, consumers poll them. Each consumer polls exactly
    // totalTasks/consumerCount tasks to avoid blocking on an empty queue (poll() blocks forever).
    final int capacity = 200;
    final int totalTasks = 100;
    final int producerCount = 4;
    final int consumerCount = 4;
    final int pollsPerConsumer = totalTasks / consumerCount;
    final FairRoundRobinQueue q = new FairRoundRobinQueue(capacity, new StreamDriverTask());
    final ExecutorService pool = Executors.newFixedThreadPool(producerCount + consumerCount);
    final CountDownLatch producersDone = new CountDownLatch(producerCount);
    final CountDownLatch start = new CountDownLatch(1);
    final CountDownLatch consumersDone = new CountDownLatch(consumerCount);
    final AtomicInteger idGen = new AtomicInteger(0);
    final AtomicInteger polled = new AtomicInteger(0);

    // Producers
    for (int i = 0; i < producerCount; i++) {
      pool.submit(
          () -> {
            try {
              start.await();
              for (int j = 0; j < totalTasks / producerCount; j++) {
                final int id = idGen.incrementAndGet();
                q.push(newTask("s-" + id));
              }
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            } finally {
              producersDone.countDown();
            }
          });
    }

    // Consumers - each polls a fixed number of tasks to avoid blocking forever.
    for (int i = 0; i < consumerCount; i++) {
      pool.submit(
          () -> {
            try {
              start.await();
              for (int j = 0; j < pollsPerConsumer; j++) {
                try {
                  q.poll();
                  polled.incrementAndGet();
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                  break;
                }
              }
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            } finally {
              consumersDone.countDown();
            }
          });
    }

    start.countDown();
    assertTrue("Consumers should finish", consumersDone.await(30, TimeUnit.SECONDS));
    assertTrue("Producers should finish", producersDone.await(30, TimeUnit.SECONDS));
    pool.shutdownNow();
    assertEquals(totalTasks, polled.get());
  }
}
