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

package org.apache.iotdb.streamnode.engine.memory;

import org.apache.iotdb.commons.memory.IMemoryBlock;
import org.apache.iotdb.commons.memory.MemoryBlockType;
import org.apache.iotdb.commons.memory.MemoryManager;
import org.apache.iotdb.streamnode.engine.memory.StreamNodeMemoryPool.MemoryReservationResult;
import org.apache.iotdb.streamnode.exception.MemoryLeakException;

import com.google.common.util.concurrent.ListenableFuture;
import org.junit.Assert;
import org.junit.Test;

public class StreamNodeMemoryPoolTest {

  private static final String TASK_NAME = "task";
  private static final String PARTITION_KEY = "partition";
  private static final String PLAN_NODE_ID = "node";

  @Test
  public void testReserveSuccess() {
    StreamNodeMemoryPool pool = createPool(1024L);
    pool.registerPlanNode(TASK_NAME, PARTITION_KEY, PLAN_NODE_ID);

    MemoryReservationResult result =
        pool.reserve(TASK_NAME, PARTITION_KEY, PLAN_NODE_ID, 512L, Long.MAX_VALUE);

    Assert.assertTrue(result.isReserveSuccess());
    Assert.assertTrue(result.getFuture().isDone());
    Assert.assertEquals(512L, result.getReservedBytes());
    Assert.assertEquals(512L, pool.getUsedMemoryInBytes());
  }

  @Test
  public void testReserveBlocksUntilFree() {
    StreamNodeMemoryPool pool = createPool(1024L);
    pool.registerPlanNode(TASK_NAME, PARTITION_KEY, PLAN_NODE_ID);

    Assert.assertTrue(
        pool.reserve(TASK_NAME, PARTITION_KEY, PLAN_NODE_ID, 1024L, Long.MAX_VALUE)
            .isReserveSuccess());

    MemoryReservationResult blocked =
        pool.reserve(TASK_NAME, PARTITION_KEY, PLAN_NODE_ID, 512L, Long.MAX_VALUE);
    Assert.assertFalse(blocked.isReserveSuccess());
    Assert.assertFalse(blocked.getFuture().isDone());

    pool.free(TASK_NAME, PARTITION_KEY, PLAN_NODE_ID, 1024L);

    Assert.assertTrue(blocked.getFuture().isDone());
    Assert.assertEquals(512L, pool.getUsedMemoryInBytes());
  }

  @Test
  public void testFreeWakesWaitersInQueueOrder() {
    StreamNodeMemoryPool pool = createPool(1024L);
    pool.registerPlanNode(TASK_NAME, PARTITION_KEY, PLAN_NODE_ID);

    Assert.assertTrue(
        pool.reserve(TASK_NAME, PARTITION_KEY, PLAN_NODE_ID, 1024L, Long.MAX_VALUE)
            .isReserveSuccess());

    ListenableFuture<Void> first =
        pool.reserve(TASK_NAME, PARTITION_KEY, PLAN_NODE_ID, 512L, Long.MAX_VALUE).getFuture();
    ListenableFuture<Void> second =
        pool.reserve(TASK_NAME, PARTITION_KEY, PLAN_NODE_ID, 512L, Long.MAX_VALUE).getFuture();
    Assert.assertFalse(first.isDone());
    Assert.assertFalse(second.isDone());

    pool.free(TASK_NAME, PARTITION_KEY, PLAN_NODE_ID, 1024L);

    Assert.assertTrue(first.isDone());
    Assert.assertTrue(second.isDone());
    Assert.assertEquals(1024L, pool.getUsedMemoryInBytes());
  }

  /**
   * Documents the actual (non-FIFO) wake-up semantics: a smaller later waiter is satisfied even
   * though the larger head waiter cannot fit yet.
   */
  @Test
  public void testFreeSatisfiesSmallerWaiterBehindLargerHead() {
    StreamNodeMemoryPool pool = createPool(1024L);
    String largePlanNodeId = "large";
    String smallPlanNodeId = "small";
    pool.registerPlanNode(TASK_NAME, PARTITION_KEY, largePlanNodeId);
    pool.registerPlanNode(TASK_NAME, PARTITION_KEY, smallPlanNodeId);

    Assert.assertTrue(
        pool.reserve(TASK_NAME, PARTITION_KEY, largePlanNodeId, 1024L, Long.MAX_VALUE)
            .isReserveSuccess());

    ListenableFuture<Void> large =
        pool.reserve(TASK_NAME, PARTITION_KEY, largePlanNodeId, 768L, Long.MAX_VALUE).getFuture();
    ListenableFuture<Void> small =
        pool.reserve(TASK_NAME, PARTITION_KEY, smallPlanNodeId, 256L, Long.MAX_VALUE).getFuture();
    Assert.assertFalse(large.isDone());
    Assert.assertFalse(small.isDone());

    pool.free(TASK_NAME, PARTITION_KEY, largePlanNodeId, 256L);

    Assert.assertFalse(large.isDone());
    Assert.assertTrue(small.isDone());
    Assert.assertEquals(1024L, pool.getUsedMemoryInBytes());
  }

  @Test
  public void testTryCancelPendingReservation() {
    StreamNodeMemoryPool pool = createPool(1024L);
    pool.registerPlanNode(TASK_NAME, PARTITION_KEY, PLAN_NODE_ID);

    Assert.assertTrue(
        pool.reserve(TASK_NAME, PARTITION_KEY, PLAN_NODE_ID, 1024L, Long.MAX_VALUE)
            .isReserveSuccess());

    ListenableFuture<Void> pending =
        pool.reserve(TASK_NAME, PARTITION_KEY, PLAN_NODE_ID, 512L, Long.MAX_VALUE).getFuture();
    Assert.assertFalse(pending.isDone());

    Assert.assertEquals(512L, pool.tryCancel(pending));
    Assert.assertTrue(pending.isDone());
    Assert.assertTrue(pending.isCancelled());
  }

  @Test
  public void testDeRegisterSubTaskWithLeakThrows() {
    StreamNodeMemoryPool pool = createPool(1024L);
    pool.registerPlanNode(TASK_NAME, PARTITION_KEY, PLAN_NODE_ID);

    Assert.assertTrue(
        pool.reserve(TASK_NAME, PARTITION_KEY, PLAN_NODE_ID, 512L, Long.MAX_VALUE)
            .isReserveSuccess());

    try {
      pool.deRegisterSubTask(TASK_NAME, PARTITION_KEY, true);
      Assert.fail("Expected MemoryLeakException");
    } catch (MemoryLeakException expected) {
      // expected
    }
  }

  @Test
  public void testDeRegisterSubTaskWithoutLeak() {
    StreamNodeMemoryPool pool = createPool(1024L);
    pool.registerPlanNode(TASK_NAME, PARTITION_KEY, PLAN_NODE_ID);

    Assert.assertTrue(
        pool.reserve(TASK_NAME, PARTITION_KEY, PLAN_NODE_ID, 512L, Long.MAX_VALUE)
            .isReserveSuccess());
    pool.free(TASK_NAME, PARTITION_KEY, PLAN_NODE_ID, 512L);

    pool.deRegisterSubTask(TASK_NAME, PARTITION_KEY, true);
    Assert.assertEquals(0L, pool.getUsedMemoryInBytes());
  }

  private static StreamNodeMemoryPool createPool(long totalBytes) {
    MemoryManager memoryManager = new MemoryManager(totalBytes);
    IMemoryBlock block = memoryManager.exactAllocate("test", totalBytes, MemoryBlockType.DYNAMIC);
    return new StreamNodeMemoryPool(block);
  }
}
