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

import org.apache.iotdb.streamnode.engine.task.StreamSubTask.DataSlice;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Verifies the event admission semantics of the window engine's two (unbounded) event queues:
 * offers always succeed (the producer is never blocked by the queue; backpressure is handled by the
 * memory pools instead), admission order is FIFO, the compute and recompute queues stay
 * independent, and an offered event wakes a driver blocked on an empty queue.
 */
public class WindowEngineOfferOrderTest {

  private static final int CAPACITY = 1000;

  private static final class StubWindowEngine extends WindowEngine {
    private StubWindowEngine() {
      super(null, null, Collections.emptyList(), null);
    }

    @Override
    public ListenableFuture<?> push(List<DataSlice> dataSlices) {
      return Futures.immediateVoidFuture();
    }

    @Override
    protected void processDataSlices(List<DataSlice> dataSlices) {}

    @Override
    public void start() {}

    @Override
    public void stop() {}

    @Override
    public ListenableFuture<?> forceClose() {
      return Futures.immediateVoidFuture();
    }

    @Override
    public void drop() {
      // nothing to release in the stub
    }
  }

  private static IEventInfo event(long windowId) {
    return new WindowEventInfo.Builder().windowId(windowId).build();
  }

  @Test
  public void testOffersAlwaysSucceedAndAdmissionIsFifo() {
    StubWindowEngine engine = new StubWindowEngine();
    for (int i = 0; i < CAPACITY; i++) {
      Assert.assertTrue(engine.computeEventQueue.offer(event(i)));
    }

    // The queue is unbounded: even with 1000 events pending, further offers succeed (the producer
    // is backpressured by the memory pools, never by the queue).
    Assert.assertTrue(engine.tryOfferComputeEvent(event(CAPACITY)));
    Assert.assertTrue(engine.tryOfferComputeEvent(event(CAPACITY + 1L)));

    List<Long> drained = new ArrayList<>();
    IEventInfo polled;
    while ((polled = engine.pollComputeEvent()) != null) {
      drained.add(polled.getWindowId());
    }
    Assert.assertEquals(CAPACITY + 2, drained.size());
    // FIFO: the first admitted event is polled first, the last offers polled last.
    Assert.assertEquals(Long.valueOf(0L), drained.get(0));
    Assert.assertEquals(Long.valueOf(CAPACITY), drained.get(drained.size() - 2));
    Assert.assertEquals(Long.valueOf(CAPACITY + 1L), drained.get(drained.size() - 1));
  }

  @Test
  public void testComputeAndRecomputeQueuesAreIndependent() {
    StubWindowEngine engine = new StubWindowEngine();
    Assert.assertTrue(engine.tryOfferComputeEvent(event(1L)));
    Assert.assertTrue(engine.tryOfferRecomputeEvent(event(2L)));
    Assert.assertTrue(engine.tryOfferComputeEvent(event(3L)));

    // Each queue is drained by its own poll; no cross-contamination.
    Assert.assertEquals(1L, engine.pollComputeEvent().getWindowId());
    Assert.assertEquals(3L, engine.pollComputeEvent().getWindowId());
    Assert.assertNull(engine.pollComputeEvent());
    Assert.assertEquals(2L, engine.pollRecomputeEvent().getWindowId());
    Assert.assertNull(engine.pollRecomputeEvent());
  }

  @Test
  public void testDriverBlockedFutureCompletedByOffer() {
    StubWindowEngine engine = new StubWindowEngine();
    ListenableFuture<?> blocked = engine.getBlockedFuture();
    Assert.assertFalse(blocked.isDone());

    // Offering an event completes the blocked future, waking the waiting driver.
    Assert.assertTrue(engine.tryOfferComputeEvent(event(1L)));
    Assert.assertTrue(blocked.isDone());
    // With an event pending, a fresh blocked future is already done.
    Assert.assertTrue(engine.getBlockedFuture().isDone());

    // The recompute queue has its own blocked future, completed independently.
    ListenableFuture<?> recomputeBlocked = engine.getRecomputeBlockedFuture();
    Assert.assertFalse(recomputeBlocked.isDone());
    Assert.assertTrue(engine.tryOfferRecomputeEvent(event(2L)));
    Assert.assertTrue(recomputeBlocked.isDone());
  }

  @Test
  public void testOfferBeforeFutureInstallDoesNotLoseWakeup() throws Exception {
    // Reproduces the lost wakeup: the producer's offer (whose completeDriverBlockedFuture is a
    // no-op while no future is installed) lands between the driver's queue-emptiness check and
    // the future's CAS install. The returned future must still complete, or the driver sleeps
    // until the next offer or tick - and the period engine never ticks, so a month-long period
    // would sleep for a month.
    StubWindowEngine engine = new StubWindowEngine();
    // The queue wrapper's first isEmpty() call IS the driver's check: while it "sees" an empty
    // queue, the producer offers into the real queue.
    AtomicBoolean driverChecked = new AtomicBoolean(false);
    LinkedBlockingQueue<IEventInfo> racingQueue =
        new LinkedBlockingQueue<IEventInfo>() {
          @Override
          public boolean isEmpty() {
            boolean empty = super.isEmpty();
            if (driverChecked.compareAndSet(false, true) && empty) {
              engine.tryOfferComputeEvent(event(42L));
            }
            return empty;
          }
        };
    swapQueueField(engine, "computeEventQueue", racingQueue);

    ListenableFuture<?> blocked = engine.getBlockedFuture();
    // The racing offer really happened, and it was the last one - nothing further will wake the
    // driver.
    Assert.assertNotNull(engine.pollComputeEvent());
    Assert.assertTrue(
        "the future installed after the offer's completion must not stay incomplete forever",
        blocked.isDone());
  }

  @Test
  public void testRecomputeOfferBeforeFutureInstallDoesNotLoseWakeup() throws Exception {
    // Same interleaving as the compute variant, on the recompute queue and its own future.
    StubWindowEngine engine = new StubWindowEngine();
    AtomicBoolean driverChecked = new AtomicBoolean(false);
    LinkedBlockingQueue<IEventInfo> racingQueue =
        new LinkedBlockingQueue<IEventInfo>() {
          @Override
          public boolean isEmpty() {
            boolean empty = super.isEmpty();
            if (driverChecked.compareAndSet(false, true) && empty) {
              engine.tryOfferRecomputeEvent(event(42L));
            }
            return empty;
          }
        };
    swapQueueField(engine, "recomputeEventQueue", racingQueue);

    ListenableFuture<?> blocked = engine.getRecomputeBlockedFuture();
    Assert.assertNotNull(engine.pollRecomputeEvent());
    Assert.assertTrue(
        "the recompute future installed after the offer's completion must not stay incomplete",
        blocked.isDone());
  }

  @Test
  public void testDoubleCheckHitLeavesNoCompletedFutureInstalled() throws Exception {
    // The lost-wakeup guard's double-check completes the freshly-installed future itself. It must
    // also REMOVE it from the field: a completed future left installed makes the next
    // getBlockedFuture() return it; the driver's prepareEvent then polls (null), registers no
    // future, and every later completion's getAndSet on the already-done stale future is a no-op -
    // the driver sleeps forever. This is the second lost wakeup the first fix introduced.
    StubWindowEngine engine = new StubWindowEngine();
    AtomicBoolean driverChecked = new AtomicBoolean(false);
    LinkedBlockingQueue<IEventInfo> racingQueue =
        new LinkedBlockingQueue<IEventInfo>() {
          @Override
          public boolean isEmpty() {
            boolean empty = super.isEmpty();
            if (driverChecked.compareAndSet(false, true) && empty) {
              engine.tryOfferComputeEvent(event(42L));
            }
            return empty;
          }
        };
    swapQueueField(engine, "computeEventQueue", racingQueue);

    // The double-check hit completes the returned future itself.
    ListenableFuture<?> first = engine.getBlockedFuture();
    Assert.assertTrue(first.isDone());

    // The driver consumes the racing event and goes idle on an empty queue.
    Assert.assertNotNull(engine.pollComputeEvent());
    Assert.assertNull(engine.pollComputeEvent());

    // The next blocked-future request must install a FRESH (incomplete) future, not the stale
    // completed one.
    ListenableFuture<?> second = engine.getBlockedFuture();
    Assert.assertFalse(
        "a completed future must never stay installed in the field", second.isDone());

    // A later offer wakes the driver through the fresh future.
    Assert.assertTrue(engine.tryOfferComputeEvent(event(43L)));
    Assert.assertTrue(second.isDone());
  }

  @Test
  public void testRecomputeDoubleCheckHitLeavesNoCompletedFutureInstalled() throws Exception {
    // Same invariant on the recompute side.
    StubWindowEngine engine = new StubWindowEngine();
    AtomicBoolean driverChecked = new AtomicBoolean(false);
    LinkedBlockingQueue<IEventInfo> racingQueue =
        new LinkedBlockingQueue<IEventInfo>() {
          @Override
          public boolean isEmpty() {
            boolean empty = super.isEmpty();
            if (driverChecked.compareAndSet(false, true) && empty) {
              engine.tryOfferRecomputeEvent(event(42L));
            }
            return empty;
          }
        };
    swapQueueField(engine, "recomputeEventQueue", racingQueue);

    ListenableFuture<?> first = engine.getRecomputeBlockedFuture();
    Assert.assertTrue(first.isDone());
    Assert.assertNotNull(engine.pollRecomputeEvent());
    Assert.assertNull(engine.pollRecomputeEvent());

    ListenableFuture<?> second = engine.getRecomputeBlockedFuture();
    Assert.assertFalse(
        "a completed future must never stay installed in the field", second.isDone());
    Assert.assertTrue(engine.tryOfferRecomputeEvent(event(43L)));
    Assert.assertTrue(second.isDone());
  }

  @Test
  public void testQueuedComputeEventCountPerWindow() {
    // The per-window queued-event count is what a destroy marker consults before it may dispatch:
    // it must drop exactly when the window's last queued compute event is polled (not before, not
    // later), and events without a window id must stay untracked.
    StubWindowEngine engine = new StubWindowEngine();
    Assert.assertFalse(engine.hasQueuedComputeEventsForWindow(1L));

    // Two events of window 1, one of window 2 queued.
    Assert.assertTrue(engine.tryOfferComputeEvent(event(1L)));
    Assert.assertTrue(engine.tryOfferComputeEvent(event(1L)));
    Assert.assertTrue(engine.tryOfferComputeEvent(event(2L)));
    Assert.assertTrue(engine.hasQueuedComputeEventsForWindow(1L));
    Assert.assertTrue(engine.hasQueuedComputeEventsForWindow(2L));

    // Polling decrements per event; the window stays gated until its last queued event is polled.
    Assert.assertEquals(1L, engine.pollComputeEvent().getWindowId());
    Assert.assertTrue(engine.hasQueuedComputeEventsForWindow(1L));
    Assert.assertEquals(1L, engine.pollComputeEvent().getWindowId());
    Assert.assertFalse(engine.hasQueuedComputeEventsForWindow(1L));
    Assert.assertTrue(engine.hasQueuedComputeEventsForWindow(2L));
    Assert.assertEquals(2L, engine.pollComputeEvent().getWindowId());
    Assert.assertFalse(engine.hasQueuedComputeEventsForWindow(2L));

    // Unassigned window ids (unit-test events without a sub-task context) are never tracked, and
    // polling them is a plain queue poll.
    Assert.assertTrue(engine.tryOfferComputeEvent(event(-1L)));
    Assert.assertFalse(engine.hasQueuedComputeEventsForWindow(-1L));
    Assert.assertEquals(-1L, engine.pollComputeEvent().getWindowId());
    Assert.assertFalse(engine.hasQueuedComputeEventsForWindow(1L));

    // drop() (clearAllPending) forgets the counts together with the drained queues.
    Assert.assertTrue(engine.tryOfferComputeEvent(event(3L)));
    engine.clearAllPending();
    Assert.assertFalse(engine.hasQueuedComputeEventsForWindow(3L));
    Assert.assertNull(engine.pollComputeEvent());
  }

  private static void swapQueueField(
      WindowEngine engine, String fieldName, LinkedBlockingQueue<IEventInfo> queue)
      throws Exception {
    Field field = WindowEngine.class.getDeclaredField(fieldName);
    field.setAccessible(true);
    field.set(engine, queue);
  }
}
