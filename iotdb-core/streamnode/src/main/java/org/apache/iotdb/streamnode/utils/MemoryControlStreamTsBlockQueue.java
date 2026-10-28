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

import org.apache.iotdb.streamnode.conf.StreamNodeDescriptor;
import org.apache.iotdb.streamnode.engine.memory.StreamNodeMemoryPool;
import org.apache.iotdb.streamnode.engine.memory.StreamNodeMemoryPool.MemoryReservationResult;
import org.apache.iotdb.streamnode.i18n.StreamNodeMessages;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.utils.Pair;

import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.ExecutorService;

import static com.google.common.base.Preconditions.checkState;
import static org.apache.iotdb.calc.execution.operator.Operator.NOT_BLOCKED;

/**
 * A memory-controlled {@link StreamTsBlockQueue} that mirrors DataNode's {@code
 * SharedTsBlockQueue}: it reserves memory from a shared {@link StreamNodeMemoryPool} keyed by
 * taskName / partitionKey / planNodeId. Each buffered {@link TsBlock} is stored together with its
 * reserved bytes; {@link #remove()} frees exactly those bytes and wakes up pending reservations in
 * the pool.
 */
public class MemoryControlStreamTsBlockQueue implements StreamTsBlockQueue {

  private final StreamNodeMemoryPool pool;
  private final String taskName;
  private final String partitionKey;
  private final String planNodeId;
  private final ExecutorService executorService;

  private long maxBytesCanReserve =
      StreamNodeDescriptor.getInstance().getMemoryConfig().getExchangeMaxBytesCanReserve();

  /** TsBlock together with the bytes reserved for it. */
  private final Queue<Pair<TsBlock, Long>> queue = new LinkedList<>();

  private long bufferRetainedSizeInBytes = 0L;

  private SettableFuture<Void> blocked = SettableFuture.create();
  private ListenableFuture<Void> blockedOnMemory;
  private boolean finished;
  private boolean alreadyRegistered;
  private Throwable abortedCause;

  public MemoryControlStreamTsBlockQueue(
      StreamNodeMemoryPool pool,
      String taskName,
      String partitionKey,
      String planNodeId,
      ExecutorService executorService) {
    this.pool = pool;
    this.taskName = taskName;
    this.partitionKey = partitionKey;
    this.planNodeId = planNodeId;
    this.executorService = executorService;
  }

  public void setMaxBytesCanReserve(long maxBytesCanReserve) {
    this.maxBytesCanReserve = maxBytesCanReserve;
  }

  @Override
  public synchronized ListenableFuture<?> isBlocked() {
    checkAborted();
    if (!queue.isEmpty() || finished) {
      return NOT_BLOCKED;
    }
    return blocked;
  }

  @Override
  public synchronized boolean isEmpty() {
    return queue.isEmpty();
  }

  @Override
  public synchronized boolean isFinished() {
    checkAborted();
    return finished && queue.isEmpty();
  }

  /**
   * Adds a TsBlock to the queue. Except for the first invocation, this method must only be invoked
   * after the future returned by the previous invocation has completed.
   */
  @Override
  public synchronized ListenableFuture<?> add(TsBlock tsBlock) {
    checkAborted();
    if (finished) {
      return NOT_BLOCKED;
    }
    checkState(
        blockedOnMemory == null || blockedOnMemory.isDone(),
        StreamNodeMessages.EXCEPTION_MEMORYCONTROLSTREAMTSBLOCKQUEUE_IS_FULL_0EBB374E);
    if (!alreadyRegistered) {
      pool.registerPlanNode(taskName, partitionKey, planNodeId);
      alreadyRegistered = true;
    }
    MemoryReservationResult reserveResult =
        pool.reserve(
            taskName,
            partitionKey,
            planNodeId,
            tsBlock.getRetainedSizeInBytes(),
            maxBytesCanReserve);
    blockedOnMemory = reserveResult.getFuture();
    long reservedBytes = reserveResult.getReservedBytes();
    bufferRetainedSizeInBytes += reservedBytes;

    if (!reserveResult.isReserveSuccess()) {
      // The pool is exhausted. Wait for a future that completes once another queue frees memory,
      // then add the TsBlock asynchronously (to avoid cross-queue lock deadlocks).
      SettableFuture<Void> channelBlocked = SettableFuture.create();
      blockedOnMemory.addListener(
          () -> {
            synchronized (this) {
              if (finished || abortedCause != null) {
                channelBlocked.set(null);
                return;
              }
              queue.add(new Pair<>(tsBlock, reservedBytes));
              unblock();
              channelBlocked.set(null);
            }
          },
          executorService);
      return channelBlocked;
    } else {
      queue.add(new Pair<>(tsBlock, reservedBytes));
      unblock();
      return blockedOnMemory;
    }
  }

  @Override
  public synchronized TsBlock remove() {
    checkAborted();
    Pair<TsBlock, Long> tsBlockWithReservedBytes = queue.poll();
    if (tsBlockWithReservedBytes != null) {
      long reservedBytes = tsBlockWithReservedBytes.right;
      pool.free(taskName, partitionKey, planNodeId, reservedBytes);
      bufferRetainedSizeInBytes -= reservedBytes;
    }
    if (queue.isEmpty() && !finished && blocked.isDone()) {
      blocked = SettableFuture.create();
    }
    return tsBlockWithReservedBytes == null ? null : tsBlockWithReservedBytes.left;
  }

  @Override
  public synchronized void setNoMoreTsBlocks() {
    finished = true;
    unblock();
  }

  @Override
  public synchronized void close() {
    finished = true;
    if (blockedOnMemory != null) {
      bufferRetainedSizeInBytes -= pool.tryCancel(blockedOnMemory);
    }
    queue.clear();
    releaseBufferedMemory();
    unblock();
  }

  @Override
  public synchronized void abort(Throwable t) {
    abortedCause = t;
    finished = true;
    if (blockedOnMemory != null) {
      bufferRetainedSizeInBytes -= pool.tryCancel(blockedOnMemory);
    }
    queue.clear();
    releaseBufferedMemory();
    if (!blocked.isDone()) {
      blocked.setException(t);
    }
  }

  private void releaseBufferedMemory() {
    if (bufferRetainedSizeInBytes > 0L) {
      pool.free(taskName, partitionKey, planNodeId, bufferRetainedSizeInBytes);
      bufferRetainedSizeInBytes = 0L;
    }
  }

  private void unblock() {
    if (!blocked.isDone()) {
      blocked.set(null);
    }
  }

  private void checkAborted() {
    if (abortedCause != null) {
      throw new IllegalStateException(abortedCause);
    }
  }
}
