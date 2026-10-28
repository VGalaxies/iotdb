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

package org.apache.iotdb.streamnode.utils;

import org.apache.iotdb.commons.memory.IMemoryBlock;
import org.apache.iotdb.commons.memory.MemoryBlockType;
import org.apache.iotdb.commons.memory.MemoryManager;
import org.apache.iotdb.streamnode.engine.memory.StreamNodeMemoryPool;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.tsfile.read.common.block.TsBlock;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;

public class MemoryControlStreamTsBlockQueueTest {

  private static final String TASK_NAME = "task";
  private static final String PARTITION_KEY = "partition";
  private static final String PLAN_NODE_ID = "node";

  @Test
  public void testAddAndRemoveFreesMemory() {
    final long blockSize = 1024L;
    StreamNodeMemoryPool pool = createPool(blockSize);
    MemoryControlStreamTsBlockQueue queue = newQueue(pool, newDirectExecutorService());
    queue.setMaxBytesCanReserve(Long.MAX_VALUE);

    TsBlock block = createMockTsBlock(blockSize);
    ListenableFuture<?> addFuture = queue.add(block);

    Assert.assertTrue(addFuture.isDone());
    Assert.assertEquals(blockSize, pool.getUsedMemoryInBytes());
    Assert.assertFalse(queue.isEmpty());

    TsBlock removed = queue.remove();
    Assert.assertSame(block, removed);
    Assert.assertEquals(0L, pool.getUsedMemoryInBytes());
    Assert.assertTrue(queue.isEmpty());
  }

  @Test
  public void testReserveFailureBlocksUntilMemoryFreed() {
    final long blockSize = 1024L;
    StreamNodeMemoryPool pool = createPool(blockSize);
    MemoryControlStreamTsBlockQueue queue = newQueue(pool, newDirectExecutorService());
    queue.setMaxBytesCanReserve(Long.MAX_VALUE);

    TsBlock first = createMockTsBlock(blockSize);
    TsBlock second = createMockTsBlock(blockSize);

    ListenableFuture<?> firstFuture = queue.add(first);
    Assert.assertTrue(firstFuture.isDone());

    // Pool can only hold one block, so the second add must block on memory.
    ListenableFuture<?> secondFuture = queue.add(second);
    Assert.assertFalse(secondFuture.isDone());

    // Freeing the first block wakes up the pending reservation and adds the second block.
    Assert.assertSame(first, queue.remove());
    Assert.assertTrue(secondFuture.isDone());
    Assert.assertSame(second, queue.remove());
    Assert.assertEquals(0L, pool.getUsedMemoryInBytes());
  }

  @Test
  public void testAddRejectsAnotherPendingMemoryReservation() {
    final long blockSize = 1024L;
    SettableFuture<Void> pendingMemoryFuture = SettableFuture.create();
    StreamNodeMemoryPool mockPool = Mockito.mock(StreamNodeMemoryPool.class);
    Mockito.when(
            mockPool.reserve(
                Mockito.anyString(),
                Mockito.anyString(),
                Mockito.anyString(),
                Mockito.anyLong(),
                Mockito.anyLong()))
        .thenReturn(
            new StreamNodeMemoryPool.MemoryReservationResult(
                pendingMemoryFuture, false, blockSize));

    MemoryControlStreamTsBlockQueue queue = newQueue(mockPool, newDirectExecutorService());
    queue.setMaxBytesCanReserve(Long.MAX_VALUE);

    ListenableFuture<?> firstFuture = queue.add(createMockTsBlock(blockSize));
    Assert.assertFalse(firstFuture.isDone());

    try {
      queue.add(createMockTsBlock(blockSize));
      Assert.fail("Expected IllegalStateException");
    } catch (IllegalStateException e) {
      Assert.assertEquals("MemoryControlStreamTsBlockQueue is full", e.getMessage());
    }
    Mockito.verify(mockPool, Mockito.times(1))
        .reserve(
            Mockito.anyString(),
            Mockito.anyString(),
            Mockito.anyString(),
            Mockito.anyLong(),
            Mockito.anyLong());
  }

  @Test
  public void testSetNoMoreTsBlocksMarksFinished() {
    StreamNodeMemoryPool pool = createPool(1024L);
    MemoryControlStreamTsBlockQueue queue = newQueue(pool, newDirectExecutorService());
    queue.setMaxBytesCanReserve(Long.MAX_VALUE);

    Assert.assertFalse(queue.isFinished());
    queue.setNoMoreTsBlocks();
    Assert.assertTrue(queue.isFinished());
  }

  @Test
  public void testCloseReleasesBufferedMemory() {
    final long blockSize = 1024L;
    StreamNodeMemoryPool pool = createPool(2 * blockSize);
    MemoryControlStreamTsBlockQueue queue = newQueue(pool, newDirectExecutorService());
    queue.setMaxBytesCanReserve(Long.MAX_VALUE);

    queue.add(createMockTsBlock(blockSize));
    queue.add(createMockTsBlock(blockSize));
    Assert.assertEquals(2 * blockSize, pool.getUsedMemoryInBytes());

    queue.close();
    Assert.assertTrue(queue.isEmpty());
    Assert.assertEquals(0L, pool.getUsedMemoryInBytes());
  }

  @Test
  public void testAbortPropagatesExceptionToRemove() {
    StreamNodeMemoryPool pool = createPool(1024L);
    MemoryControlStreamTsBlockQueue queue = newQueue(pool, newDirectExecutorService());
    queue.setMaxBytesCanReserve(Long.MAX_VALUE);

    queue.add(createMockTsBlock(1024L));
    RuntimeException cause = new RuntimeException("boom");
    queue.abort(cause);

    try {
      queue.remove();
      Assert.fail("Expected IllegalStateException");
    } catch (IllegalStateException e) {
      Assert.assertSame(cause, e.getCause());
    }
    Assert.assertEquals(0L, pool.getUsedMemoryInBytes());
  }

  @Test
  public void testAbortPropagatesExceptionToStateChecks() {
    StreamNodeMemoryPool pool = createPool(1024L);
    MemoryControlStreamTsBlockQueue queue = newQueue(pool, newDirectExecutorService());
    queue.setMaxBytesCanReserve(Long.MAX_VALUE);

    RuntimeException cause = new RuntimeException("boom");
    queue.abort(cause);

    try {
      queue.isFinished();
      Assert.fail("Expected IllegalStateException");
    } catch (IllegalStateException e) {
      Assert.assertSame(cause, e.getCause());
    }

    try {
      queue.isBlocked();
      Assert.fail("Expected IllegalStateException");
    } catch (IllegalStateException e) {
      Assert.assertSame(cause, e.getCause());
    }

    try {
      queue.add(createMockTsBlock(1024L));
      Assert.fail("Expected IllegalStateException");
    } catch (IllegalStateException e) {
      Assert.assertSame(cause, e.getCause());
    }
  }

  /**
   * Test that when add() goes into the async listener path (memory blocked) and the queue is
   * aborted before the listener fires, the listener does NOT add the TsBlock to the closed queue.
   * This mirrors DataNode's {@code
   * SharedTsBlockQueueTest#testAsyncListenerAfterAbortDoesNotAddTsBlock}.
   */
  @Test
  public void testAsyncListenerAfterAbortDoesNotAddTsBlock() {
    final long mockTsBlockSize = 1024L;

    // Manually control when the blocked-on-memory future completes.
    SettableFuture<Void> manualFuture = SettableFuture.create();

    StreamNodeMemoryPool mockPool = Mockito.mock(StreamNodeMemoryPool.class);
    Mockito.when(
            mockPool.reserve(
                Mockito.anyString(),
                Mockito.anyString(),
                Mockito.anyString(),
                Mockito.anyLong(),
                Mockito.anyLong()))
        .thenReturn(
            new StreamNodeMemoryPool.MemoryReservationResult(manualFuture, false, mockTsBlockSize));
    Mockito.when(mockPool.tryCancel(Mockito.any())).thenReturn(0L);

    MemoryControlStreamTsBlockQueue queue =
        new MemoryControlStreamTsBlockQueue(
            mockPool, TASK_NAME, PARTITION_KEY, PLAN_NODE_ID, newDirectExecutorService());
    queue.setMaxBytesCanReserve(Long.MAX_VALUE);

    TsBlock mockTsBlock = createMockTsBlock(mockTsBlockSize);

    // add() goes into the async path — the TsBlock is not yet added.
    ListenableFuture<?> addFuture = queue.add(mockTsBlock);
    Assert.assertFalse(addFuture.isDone());
    Assert.assertTrue(queue.isEmpty());

    // Abort before the listener fires.
    queue.abort(new RuntimeException("test abort"));

    // Completing the memory future triggers the listener; it must not add to the closed queue.
    manualFuture.set(null);

    Assert.assertTrue(queue.isEmpty());
    Assert.assertTrue(addFuture.isDone());
  }

  @Test(timeout = 60000L)
  public void concurrencyTest() throws Exception {
    final long blockSize = 1024L;
    final int capacityBlocks = 5;
    final int totalBlocks = 200;

    StreamNodeMemoryPool pool = createPool((long) capacityBlocks * blockSize);
    MemoryControlStreamTsBlockQueue queue =
        new MemoryControlStreamTsBlockQueue(
            pool, TASK_NAME, PARTITION_KEY, PLAN_NODE_ID, Executors.newSingleThreadExecutor());
    queue.setMaxBytesCanReserve(Long.MAX_VALUE);

    AtomicInteger consumedCount = new AtomicInteger(0);
    ExecutorService executor = Executors.newFixedThreadPool(2);

    Future<?> producer =
        executor.submit(
            () -> {
              try {
                for (int i = 0; i < totalBlocks; i++) {
                  ListenableFuture<?> addFuture = queue.add(createMockTsBlock(blockSize));
                  if (!addFuture.isDone()) {
                    addFuture.get();
                  }
                }
                queue.setNoMoreTsBlocks();
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            });

    Future<?> consumer =
        executor.submit(
            () -> {
              try {
                while (consumedCount.get() < totalBlocks) {
                  ListenableFuture<?> blocked = queue.isBlocked();
                  if (!blocked.isDone()) {
                    blocked.get();
                  }
                  if (queue.remove() != null) {
                    consumedCount.incrementAndGet();
                  }
                }
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            });

    producer.get();
    consumer.get();
    executor.shutdownNow();

    Assert.assertEquals(totalBlocks, consumedCount.get());
    Assert.assertEquals(0L, pool.getUsedMemoryInBytes());
  }

  private static StreamNodeMemoryPool createPool(long totalBytes) {
    MemoryManager memoryManager = new MemoryManager(totalBytes);
    IMemoryBlock block = memoryManager.exactAllocate("test", totalBytes, MemoryBlockType.DYNAMIC);
    return new StreamNodeMemoryPool(block);
  }

  private static MemoryControlStreamTsBlockQueue newQueue(
      StreamNodeMemoryPool pool, ExecutorService executorService) {
    return new MemoryControlStreamTsBlockQueue(
        pool, TASK_NAME, PARTITION_KEY, PLAN_NODE_ID, executorService);
  }

  private static TsBlock createMockTsBlock(long sizeInBytes) {
    TsBlock tsBlock = Mockito.mock(TsBlock.class);
    Mockito.when(tsBlock.getRetainedSizeInBytes()).thenReturn(sizeInBytes);
    return tsBlock;
  }
}
