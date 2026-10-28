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
import org.apache.iotdb.streamnode.exception.MemoryLeakException;
import org.apache.iotdb.streamnode.i18n.StreamNodeMessages;

import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.ListenableFuture;

import javax.annotation.Nullable;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;

import static com.google.common.util.concurrent.Futures.immediateVoidFuture;

/**
 * A thread-safe memory pool for the stream engine's exchange (TsBlock buffering) memory. It mirrors
 * DataNode's {@code MemoryPool}: one shared {@link IMemoryBlock} bounds the total memory, while
 * reservations are accounted as {@code taskName -> partitionKey -> planNodeId -> bytes}. When the
 * pool is exhausted, {@link #reserve} returns an uncompleted future that is woken up by {@link
 * #free}.
 */
public class StreamNodeMemoryPool {

  /** A reservation that has not completed yet, waiting for free memory. */
  public static class MemoryReservationFuture extends AbstractFuture<Void> {

    private final String taskName;
    private final String partitionKey;
    private final String planNodeId;
    private final long bytesToReserve;
    private final long maxBytesCanReserve;

    private MemoryReservationFuture(
        String taskName,
        String partitionKey,
        String planNodeId,
        long bytesToReserve,
        long maxBytesCanReserve) {
      this.taskName = taskName;
      this.partitionKey = partitionKey;
      this.planNodeId = planNodeId;
      this.bytesToReserve = bytesToReserve;
      this.maxBytesCanReserve = maxBytesCanReserve;
    }

    public String getTaskName() {
      return taskName;
    }

    public String getPartitionKey() {
      return partitionKey;
    }

    public String getPlanNodeId() {
      return planNodeId;
    }

    public long getBytesToReserve() {
      return bytesToReserve;
    }

    public long getMaxBytesCanReserve() {
      return maxBytesCanReserve;
    }

    public static MemoryReservationFuture create(
        String taskName,
        String partitionKey,
        String planNodeId,
        long bytesToReserve,
        long maxBytesCanReserve) {
      return new MemoryReservationFuture(
          taskName, partitionKey, planNodeId, bytesToReserve, maxBytesCanReserve);
    }

    @Override
    public boolean set(@Nullable Void value) {
      return super.set(value);
    }
  }

  /** The result of a {@link #reserve} call. */
  public static class MemoryReservationResult {
    private final ListenableFuture<Void> future;
    private final boolean reserveSuccess;
    private final long reservedBytes;

    public MemoryReservationResult(
        ListenableFuture<Void> future, boolean reserveSuccess, long reservedBytes) {
      this.future = future;
      this.reserveSuccess = reserveSuccess;
      this.reservedBytes = reservedBytes;
    }

    public ListenableFuture<Void> getFuture() {
      return future;
    }

    public boolean isReserveSuccess() {
      return reserveSuccess;
    }

    public long getReservedBytes() {
      return reservedBytes;
    }
  }

  private final IMemoryBlock memoryBlock;

  /** taskName -> partitionKey -> planNodeId -> bytesReserved. */
  private final Map<String, Map<String, Map<String, Long>>> reservations =
      new ConcurrentHashMap<>();

  private final Queue<MemoryReservationFuture> waitingFutures = new ConcurrentLinkedQueue<>();

  public StreamNodeMemoryPool(IMemoryBlock memoryBlock) {
    this.memoryBlock = memoryBlock;
  }

  public long getTotalMemoryInBytes() {
    return memoryBlock.getTotalMemorySizeInBytes();
  }

  public long getRemainingMemoryInBytes() {
    return memoryBlock.getFreeMemoryInBytes();
  }

  public long getUsedMemoryInBytes() {
    return memoryBlock.getUsedMemoryInBytes();
  }

  public int getQueryMemoryReservationSize() {
    return reservations.size();
  }

  public int getMemoryReservationSize() {
    return waitingFutures.size();
  }

  /**
   * Register the plan node memory map before reserving, so that de-registering can detect leaks.
   */
  public void registerPlanNode(String taskName, String partitionKey, String planNodeId) {
    synchronized (reservations) {
      reservations
          .computeIfAbsent(taskName, k -> new ConcurrentHashMap<>())
          .computeIfAbsent(partitionKey, k -> new ConcurrentHashMap<>())
          .putIfAbsent(planNodeId, 0L);
    }
  }

  /**
   * Reserve memory. On success the returned future is already completed and {@code reservedBytes}
   * equals {@code bytesToReserve}; on failure (pool or per-plan-node limit exhausted) an
   * uncompleted future is returned and the caller should wait for it.
   *
   * @throws IllegalArgumentException if the request exceeds {@code maxBytesCanReserve}.
   */
  public MemoryReservationResult reserve(
      String taskName,
      String partitionKey,
      String planNodeId,
      long bytesToReserve,
      long maxBytesCanReserve) {
    if (bytesToReserve <= 0L) {
      throw new IllegalArgumentException(
          StreamNodeMessages.EXCEPTION_BYTESTORESERVE_SHOULD_BE_POSITIVE_F7A74BA0);
    }
    if (bytesToReserve > maxBytesCanReserve) {
      throw new IllegalArgumentException(
          String.format(
              StreamNodeMessages
                  .EXCEPTION_CANNOT_RESERVE_ARG_BYTES_FOR_PLAN_NODE_ARG_MAX_BYTES_CAN_RESERVE_IS_ARG_9885F340,
              bytesToReserve,
              planNodeId,
              maxBytesCanReserve));
    }
    if (tryReserve(taskName, partitionKey, planNodeId, bytesToReserve, maxBytesCanReserve)) {
      return new MemoryReservationResult(immediateVoidFuture(), true, bytesToReserve);
    }
    rollbackReserve(taskName, partitionKey, planNodeId, bytesToReserve);
    MemoryReservationFuture future =
        MemoryReservationFuture.create(
            taskName, partitionKey, planNodeId, bytesToReserve, maxBytesCanReserve);
    synchronized (future) {
      waitingFutures.add(future);
      // Retry once after enqueuing, to close the lost-wakeup window between a failed reserve and
      // the waiter publication. A concurrent free() that released memory but saw an empty queue is
      // picked up by this retry.
      if (tryReserve(taskName, partitionKey, planNodeId, bytesToReserve, maxBytesCanReserve)) {
        future.set(null);
        waitingFutures.remove(future);
        return new MemoryReservationResult(future, true, bytesToReserve);
      }
      // The retry also failed, so roll back the reservation it just attempted. Otherwise the block
      // and the per-plan-node accounting stay inflated until free() retries, which would then
      // double-count the bytes.
      rollbackReserve(taskName, partitionKey, planNodeId, bytesToReserve);
    }
    return new MemoryReservationResult(future, false, bytesToReserve);
  }

  /**
   * Cancel a pending reservation. Returns the bytes that were being reserved if the future was not
   * completed yet, otherwise 0.
   */
  public synchronized long tryCancel(ListenableFuture<Void> future) {
    synchronized (future) {
      if (future.isDone()) {
        return 0L;
      }
      future.cancel(true);
    }
    return ((MemoryReservationFuture) future).getBytesToReserve();
  }

  /**
   * Release reserved memory. Releases the block and then sweeps the waiting queue, satisfying any
   * waiter that can now fit. This is not strict FIFO: if the head waiter cannot be satisfied, later
   * waiters are still tried, mirroring DataNode's {@code MemoryPool}.
   */
  public void free(String taskName, String partitionKey, String planNodeId, long bytes) {
    if (bytes <= 0L) {
      throw new IllegalArgumentException(
          StreamNodeMessages.EXCEPTION_BYTES_TO_FREE_SHOULD_BE_POSITIVE_B330BE00);
    }
    try {
      reservations
          .get(taskName)
          .get(partitionKey)
          .computeIfPresent(
              planNodeId,
              (k, reservedMemory) -> {
                if (reservedMemory < bytes) {
                  throw new IllegalArgumentException(
                      StreamNodeMessages
                          .EXCEPTION_FREE_MORE_MEMORY_THAN_HAS_BEEN_RESERVED_52C0122E);
                }
                return reservedMemory - bytes;
              });
    } catch (NullPointerException e) {
      throw new IllegalArgumentException(
          StreamNodeMessages
              .EXCEPTION_RELATED_MEMORY_RESERVATION_CANNOT_BE_NULL_WHEN_FREEING_68A8477C,
          e);
    }
    memoryBlock.release(bytes);

    if (waitingFutures.isEmpty()) {
      return;
    }
    Iterator<MemoryReservationFuture> iterator = waitingFutures.iterator();
    while (iterator.hasNext()) {
      MemoryReservationFuture future = iterator.next();
      synchronized (future) {
        if (future.isCancelled() || future.isDone()) {
          iterator.remove();
          continue;
        }
        if (tryReserve(
            future.getTaskName(),
            future.getPartitionKey(),
            future.getPlanNodeId(),
            future.getBytesToReserve(),
            future.getMaxBytesCanReserve())) {
          future.set(null);
          iterator.remove();
        } else {
          rollbackReserve(
              future.getTaskName(),
              future.getPartitionKey(),
              future.getPlanNodeId(),
              future.getBytesToReserve());
        }
      }
    }
  }

  /**
   * De-register all reservations of a sub task when its lifecycle ends. If there are remaining
   * non-zero reservations and {@code forceDeregister} is false, this is a no-op; otherwise the
   * entry is removed and a {@link MemoryLeakException} is thrown.
   */
  public void deRegisterSubTask(String taskName, String partitionKey, boolean forceDeregister) {
    Map<String, Map<String, Long>> taskReservations = reservations.get(taskName);
    if (taskReservations == null) {
      return;
    }
    Map<String, Long> subTaskReservations = taskReservations.get(partitionKey);
    boolean hasPotentialMemoryLeak = false;
    if (subTaskReservations != null) {
      hasPotentialMemoryLeak = subTaskReservations.values().stream().anyMatch(v -> v != 0L);
    }
    if (!forceDeregister && hasPotentialMemoryLeak) {
      return;
    }
    synchronized (reservations) {
      taskReservations.remove(partitionKey);
      if (taskReservations.isEmpty()) {
        reservations.remove(taskName);
      }
    }
    if (hasPotentialMemoryLeak) {
      List<Map.Entry<String, Long>> invalidEntryList =
          subTaskReservations.entrySet().stream()
              .filter(entry -> entry.getValue() != 0L)
              .collect(Collectors.toList());
      throw new MemoryLeakException(
          String.format(
              StreamNodeMessages
                  .EXCEPTION_PLAN_NODE_RELATED_MEMORY_IS_NOT_ZERO_WHEN_TRYING_TO_DEREGISTER_SUB_TASK_ARG_ARG_ARG_F4DED57C,
              taskName,
              partitionKey,
              invalidEntryList));
    }
  }

  private boolean tryReserve(
      String taskName,
      String partitionKey,
      String planNodeId,
      long bytesToReserve,
      long maxBytesCanReserve) {
    long tryUsedBytes = memoryBlock.forceAllocateWithoutLimitation(bytesToReserve);
    long remainingBytes =
        maxBytesCanReserve
            - reservations
                .get(taskName)
                .get(partitionKey)
                .merge(planNodeId, bytesToReserve, Long::sum);
    return tryUsedBytes <= memoryBlock.getTotalMemorySizeInBytes() && remainingBytes >= 0;
  }

  private void rollbackReserve(
      String taskName, String partitionKey, String planNodeId, long bytesToReserve) {
    reservations.get(taskName).get(partitionKey).merge(planNodeId, -bytesToReserve, Long::sum);
    memoryBlock.release(bytesToReserve);
  }
}
