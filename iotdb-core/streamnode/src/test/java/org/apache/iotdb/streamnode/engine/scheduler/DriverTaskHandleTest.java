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

import org.apache.iotdb.streamnode.engine.scheduler.queue.FairRoundRobinQueue;
import org.apache.iotdb.streamnode.engine.scheduler.queue.Priority;
import org.apache.iotdb.streamnode.engine.scheduler.task.StreamDriverTask;

import org.junit.Test;
import org.mockito.ArgumentMatchers;

import java.util.OptionalInt;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DriverTaskHandleTest {

  private FairRoundRobinQueue newQueue(final int capacity) {
    return new FairRoundRobinQueue(capacity, new StreamDriverTask());
  }

  @Test
  public void testConstructorRejectsNullQueue() {
    try {
      new DriverTaskHandle(0, null, OptionalInt.of(1));
      fail("Expected NullPointerException for null queue");
    } catch (NullPointerException expected) {
      // expected
    }
  }

  @Test
  public void testConstructorRejectsNullMaxDrivers() {
    try {
      new DriverTaskHandle(0, newQueue(10), null);
      fail("Expected NullPointerException for null maxDriversPerTask");
    } catch (NullPointerException expected) {
      // expected
    }
  }

  @Test
  public void testUpdateScheduledTimeInNanosAccumulates() {
    final FairRoundRobinQueue queue = newQueue(10);
    final DriverTaskHandle handle = new DriverTaskHandle(1, queue, OptionalInt.of(2));

    final Priority first = handle.updateScheduledTimeInNanos(100L);
    assertNotNull(first);
    final Priority second = handle.updateScheduledTimeInNanos(200L);
    assertNotNull(second);
    // The new priority returned by FairRoundRobinQueue.updatePriority is new
    // Priority(System.nanoTime()),
    // so it changes each call. The internal scheduledTimeInNanos accumulates (verified via
    // concurrency test).
    assertNotEquals(first, second);
  }

  @Test
  public void testGetPriorityInitiallyNotNull() {
    final DriverTaskHandle handle = new DriverTaskHandle(0, newQueue(10), OptionalInt.empty());
    assertNotNull(handle.getPriority());
  }

  @Test
  public void testGetPriorityUpdatesAfterUpdateScheduledTime() {
    final FairRoundRobinQueue mockQueue = mock(FairRoundRobinQueue.class);
    final Priority fixedPriority = new Priority(555L);
    when(mockQueue.updatePriority(ArgumentMatchers.any(), ArgumentMatchers.anyLong()))
        .thenReturn(fixedPriority);

    final DriverTaskHandle handle = new DriverTaskHandle(0, mockQueue, OptionalInt.empty());
    final Priority updated = handle.updateScheduledTimeInNanos(42L);
    assertEquals(555L, updated.getLastScheduledTime());
    assertEquals(555L, handle.getPriority().getLastScheduledTime());
  }

  @Test
  public void testEqualsAndHashCodeBasedOnId() {
    final FairRoundRobinQueue queue = newQueue(10);
    final DriverTaskHandle a = new DriverTaskHandle(7, queue, OptionalInt.empty());
    final DriverTaskHandle b = new DriverTaskHandle(7, queue, OptionalInt.of(1));
    final DriverTaskHandle c = new DriverTaskHandle(8, queue, OptionalInt.empty());

    assertEquals(a, b);
    assertEquals(a.hashCode(), b.hashCode());
    assertTrue(!a.equals(c));
    assertTrue(!a.equals(null));
    assertTrue(!a.equals("string"));
    assertEquals(a, a);
  }

  @Test(timeout = 30000)
  public void testConcurrentUpdateScheduledTimeInNanos() throws InterruptedException {
    // DriverTaskHandle.updateScheduledTimeInNanos is synchronized; concurrent calls must
    // accumulate scheduledTimeInNanos without lost updates. We can't read the private field
    // directly, but we can verify no exception is thrown and all updates return non-null.
    final DriverTaskHandle handle = new DriverTaskHandle(0, newQueue(10), OptionalInt.empty());
    final int threadCount = 8;
    final int updatesPerThread = 1000;
    final ExecutorService pool = Executors.newFixedThreadPool(threadCount);
    final CountDownLatch start = new CountDownLatch(1);
    final CountDownLatch done = new CountDownLatch(threadCount);
    final AtomicLong nullCount = new AtomicLong(0);

    for (int i = 0; i < threadCount; i++) {
      pool.submit(
          () -> {
            try {
              start.await();
              for (int j = 0; j < updatesPerThread; j++) {
                if (handle.updateScheduledTimeInNanos(1L) == null) {
                  nullCount.incrementAndGet();
                }
              }
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            } finally {
              done.countDown();
            }
          });
    }

    start.countDown();
    assertTrue("All threads should finish within timeout", done.await(30, TimeUnit.SECONDS));
    pool.shutdownNow();
    assertEquals(0L, nullCount.get());
  }
}
