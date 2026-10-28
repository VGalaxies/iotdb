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

package org.apache.iotdb.streamnode.engine.sink;

import com.google.common.util.concurrent.ListenableFuture;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

public class SinkQueueTest {

  private static SinkEntry newEntry(long windowId, long memoryBytes) {
    return new SinkEntry(Optional.empty(), windowId, false, 0, memoryBytes);
  }

  private static SinkMemoryController newMemoryController(long pipelineLimitBytes) {
    return new SinkMemoryController(pipelineLimitBytes, GlobalSinkMemoryController.getInstance());
  }

  /**
   * Entries are either admitted (immediate future) or parked (backpressure future); {@code close()}
   * fails the parked entries so their producers are released.
   */
  @Test
  public void testEnqueueAdmitsOrParksAndCloseFailsParked() throws Exception {
    SinkMemoryController memoryController = newMemoryController(100L);
    SinkQueue queue = new SinkQueue(memoryController);

    SinkEntry admitted = newEntry(1L, 10L);
    ListenableFuture<Void> admittedFuture = queue.enqueue(admitted);
    Assert.assertTrue(admittedFuture.isDone());
    Assert.assertEquals(1, queue.size());

    // 10 + 95 exceeds the pipeline quota but each entry alone fits -> parked.
    SinkEntry parked = newEntry(1L, 95L);
    ListenableFuture<Void> parkedFuture = queue.enqueue(parked);
    Assert.assertFalse(parkedFuture.isDone());
    Assert.assertEquals(1, queue.size());
    Assert.assertTrue(queue.hasPendingPushes());

    queue.close();
    Assert.assertFalse(queue.hasPendingPushes());
    Assert.assertTrue(parkedFuture.isDone());
    try {
      parkedFuture.get();
      Assert.fail("expected the parked future to fail");
    } catch (ExecutionException expected) {
      Assert.assertTrue(expected.getCause() instanceof IllegalStateException);
    }

    // Balance the global pool used by the admitted entry.
    Assert.assertTrue(memoryController.release(10L));
  }

  /** A failed enqueue (closed queue) must surface the failure on the returned future. */
  @Test
  public void testEnqueueAfterCloseReturnsFailedFuture() throws Exception {
    SinkQueue queue = new SinkQueue(newMemoryController(100L));

    queue.close();

    ListenableFuture<Void> future = queue.enqueue(newEntry(1L, 10L));
    Assert.assertTrue(future.isDone());
    try {
      future.get();
      Assert.fail("expected the enqueue future to fail");
    } catch (ExecutionException expected) {
      Assert.assertTrue(expected.getCause() instanceof IllegalStateException);
    }
  }

  /** Releasing memory must re-admit the parked entries in FIFO order. */
  @Test
  public void testTryAdmitParkedEntriesAdmitsParkedEntries() {
    SinkMemoryController memoryController = newMemoryController(100L);
    SinkQueue queue = new SinkQueue(memoryController);

    SinkEntry admitted = newEntry(1L, 10L);
    Assert.assertTrue(queue.enqueue(admitted).isDone());

    SinkEntry parked = newEntry(2L, 95L);
    ListenableFuture<Void> parkedFuture = queue.enqueue(parked);
    Assert.assertFalse(parkedFuture.isDone());
    Assert.assertEquals(1, queue.size());

    // Freeing quota lets the parked entry be admitted on the next retry.
    Assert.assertTrue(memoryController.release(10L));
    Assert.assertTrue(queue.hasPendingPushes());
    Assert.assertTrue(queue.tryAdmitParkedEntries());
    Assert.assertFalse(queue.hasPendingPushes());
    Assert.assertTrue(parkedFuture.isDone());
    Assert.assertEquals(2, queue.size());

    // Balance the global pool used by both admitted entries.
    Assert.assertTrue(memoryController.release(95L));
  }

  /**
   * A smaller follow-up entry must not jump into the drainable queue while a larger entry is
   * already parked on this shard. Both stay parked in FIFO order until quota is freed.
   */
  @Test
  public void testEnqueueDoesNotCutInLineAheadOfParkedEntries() {
    SinkMemoryController memoryController = newMemoryController(100L);
    SinkQueue queue = new SinkQueue(memoryController);

    Assert.assertTrue(queue.enqueue(newEntry(1L, 60L)).isDone());

    SinkEntry parkedLarge = newEntry(1L, 50L);
    ListenableFuture<Void> largeFuture = queue.enqueue(parkedLarge);
    Assert.assertFalse(largeFuture.isDone());
    Assert.assertEquals(1, queue.size());

    // 60 + 10 would fit the quota, so without FIFO this would admit ahead of the parked 50.
    SinkEntry parkedSmall = newEntry(1L, 10L);
    ListenableFuture<Void> smallFuture = queue.enqueue(parkedSmall);
    Assert.assertFalse(smallFuture.isDone());
    Assert.assertEquals(1, queue.size());
    Assert.assertTrue(queue.hasPendingPushes());

    Assert.assertTrue(memoryController.release(60L));
    Assert.assertTrue(queue.tryAdmitParkedEntries());
    Assert.assertTrue(largeFuture.isDone());
    Assert.assertTrue(smallFuture.isDone());
    Assert.assertFalse(queue.hasPendingPushes());
    Assert.assertEquals(3, queue.size());

    List<SinkEntry> drained = new ArrayList<>();
    Assert.assertEquals(3, queue.drainTo(drained, 3));
    Assert.assertEquals(60L, drained.get(0).getMemorySizeInBytes());
    Assert.assertEquals(50L, drained.get(1).getMemorySizeInBytes());
    Assert.assertEquals(10L, drained.get(2).getMemorySizeInBytes());

    Assert.assertTrue(memoryController.release(60L));
  }

  /** {@code tryAdmitParkedEntries} reports whether anything was admitted. */
  @Test
  public void testTryAdmitParkedEntriesReturnsFalseWhenNothingParked() {
    SinkQueue queue = new SinkQueue(newMemoryController(100L));

    Assert.assertFalse(queue.tryAdmitParkedEntries());
  }

  @Test
  public void testDrainToAndPollRemoveEntriesInOrder() {
    SinkMemoryController memoryController = newMemoryController(10_000L);
    SinkQueue queue = new SinkQueue(memoryController);

    for (int i = 0; i < 3; i++) {
      Assert.assertTrue(queue.enqueue(newEntry(i, 10L)).isDone());
    }

    Assert.assertEquals(3, queue.size());
    Assert.assertTrue(queue.hasData());

    List<SinkEntry> drained = new ArrayList<>();
    Assert.assertEquals(2, queue.drainTo(drained, 2));
    Assert.assertEquals(2, drained.size());
    Assert.assertEquals(0L, drained.get(0).getWindowId());
    Assert.assertEquals(1L, drained.get(1).getWindowId());
    Assert.assertEquals(1, queue.size());

    SinkEntry last = queue.poll();
    Assert.assertNotNull(last);
    Assert.assertEquals(2L, last.getWindowId());
    Assert.assertNull(queue.poll());
    Assert.assertFalse(queue.hasData());
    Assert.assertEquals(0, queue.size());

    Assert.assertTrue(memoryController.release(30L));
  }

  /**
   * Parked entries are pending work but not drainable: {@link SinkQueue#hasData()} stays false
   * until {@link SinkQueue#tryAdmitParkedEntries()} admits them into the blocking queue.
   */
  @Test
  public void testHasDataIsFalseWhenOnlyParkedEntriesExist() {
    SinkMemoryController memoryController = newMemoryController(100L);
    SinkQueue queue = new SinkQueue(memoryController);

    // Fill to the quota so the next entry parks temporarily.
    Assert.assertTrue(queue.enqueue(newEntry(1L, 100L)).isDone());
    Assert.assertFalse(queue.enqueue(newEntry(2L, 1L)).isDone());

    Assert.assertEquals(1, queue.size());
    Assert.assertTrue(queue.hasPendingPushes());
    // Queue still has the admitted entry; drain it so only parked work remains.
    Assert.assertNotNull(queue.poll());
    Assert.assertFalse(queue.hasData());
    Assert.assertTrue(queue.hasPendingPushes());

    queue.close();
    Assert.assertFalse(queue.hasPendingPushes());
    Assert.assertFalse(queue.hasData());
    Assert.assertTrue(memoryController.release(100L));
  }

  /**
   * An entry larger than the pipeline quota can never be admitted; enqueue must fail immediately
   * instead of parking it as temporary backpressure.
   */
  @Test
  public void testOversizedEntryFailsImmediately() throws Exception {
    SinkMemoryController memoryController = newMemoryController(100L);
    SinkQueue queue = new SinkQueue(memoryController);

    ListenableFuture<Void> future = queue.enqueue(newEntry(1L, 101L));
    Assert.assertTrue(future.isDone());
    Assert.assertFalse(queue.hasPendingPushes());
    Assert.assertFalse(queue.hasData());
    try {
      future.get();
      Assert.fail("expected the oversized enqueue to fail");
    } catch (ExecutionException expected) {
      Assert.assertTrue(expected.getCause() instanceof IllegalArgumentException);
    }
  }

  @Test
  public void testCloseIsIdempotent() {
    SinkMemoryController memoryController = newMemoryController(100L);
    SinkQueue queue = new SinkQueue(memoryController);

    Assert.assertTrue(queue.enqueue(newEntry(1L, 100L)).isDone());
    Assert.assertFalse(queue.enqueue(newEntry(2L, 1L)).isDone());

    queue.close();
    Assert.assertFalse(queue.hasPendingPushes());
    Assert.assertTrue(memoryController.release(100L));
  }
}
