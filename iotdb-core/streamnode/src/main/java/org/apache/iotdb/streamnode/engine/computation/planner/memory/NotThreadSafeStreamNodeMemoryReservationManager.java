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

package org.apache.iotdb.streamnode.engine.computation.planner.memory;

import org.apache.iotdb.calc.plan.planner.memory.MemoryReservationManager;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.streamnode.engine.computation.planner.StreamExecutionPlanner;
import org.apache.iotdb.streamnode.i18n.StreamNodeMessages;

import org.apache.tsfile.utils.Pair;

import javax.annotation.concurrent.NotThreadSafe;

import static com.google.common.base.Preconditions.checkState;

/**
 * Tracks the compute (operator) memory reserved by a single stream sub task against the shared
 * compute memory block held by {@link StreamExecutionPlanner}. The accounting logic mirrors
 * DataNode's {@code NotThreadSafeMemoryReservationManager}, except that there is no
 * highest-priority fallback: when the compute block is exhausted, a {@code
 * MemoryNotEnoughException} is thrown directly.
 */
@NotThreadSafe
public class NotThreadSafeStreamNodeMemoryReservationManager implements MemoryReservationManager {
  // To avoid reserving memory too frequently, we choose to do it in batches. This is the lower
  // bound for each batch.
  private static final long MEMORY_BATCH_THRESHOLD = 1024L * 1024L;

  private final StreamExecutionPlanner STREAM_EXECUTION_PLANNER =
      StreamExecutionPlanner.getInstance();

  private final String contextHolder;

  private long reservedBytesInTotal = 0;

  private long bytesToBeReserved = 0;

  private long bytesToBeReleased = 0;

  public NotThreadSafeStreamNodeMemoryReservationManager(final String contextHolder) {
    this.contextHolder = contextHolder;
  }

  @Override
  public void setHighestPriority(boolean isHighestPriority) {
    // StreamNode has no highest-priority scenario, so this is a no-op.
  }

  @TestOnly
  public long getReservedBytesInTotalForTest() {
    return reservedBytesInTotal;
  }

  @Override
  public void reserveMemoryCumulatively(final long size) {
    bytesToBeReserved += size;
    if (bytesToBeReserved >= MEMORY_BATCH_THRESHOLD) {
      try {
        reserveMemoryImmediately();
      } catch (RuntimeException | Error failure) {
        // reserveMemoryImmediately can fail only while asking the compute block for memory, before
        // it updates this manager's counters. Keep the caller-visible reservation operation atomic.
        bytesToBeReserved -= size;
        throw failure;
      }
    }
  }

  @Override
  public void reserveMemoryImmediately() {
    if (bytesToBeReserved != 0) {
      reserveFromComputeBlock(bytesToBeReserved);
      reservedBytesInTotal += bytesToBeReserved;
      bytesToBeReserved = 0;
    }
  }

  @Override
  public void reserveMemoryImmediately(final long size) {
    if (size != 0) {
      reserveFromComputeBlock(size);
      reservedBytesInTotal += size;
    }
  }

  private void reserveFromComputeBlock(final long size) {
    STREAM_EXECUTION_PLANNER.reserveFromFreeMemoryForCompute(
        size, reservedBytesInTotal, contextHolder);
  }

  @Override
  public void releaseMemoryCumulatively(final long size) {
    if (size <= 0) {
      return;
    }
    bytesToBeReleased += size;
    if (bytesToBeReleased >= MEMORY_BATCH_THRESHOLD) {
      releaseBytesImmediately(bytesToBeReleased);
      bytesToBeReleased = 0;
    }
  }

  @Override
  public void releaseMemoryImmediately(final long size) {
    if (size > 0) {
      releaseBytesImmediately(size);
    }
  }

  private void releaseBytesImmediately(final long size) {
    long poolBytes = deductReleaseAccounting(size);
    if (poolBytes > 0) {
      STREAM_EXECUTION_PLANNER.releaseToFreeMemoryForCompute(poolBytes);
    }
  }

  /** Deduct release size from pending reserve, then the compute block reservation in order. */
  private long deductReleaseAccounting(final long size) {
    if (size <= bytesToBeReserved) {
      bytesToBeReserved -= size;
      return 0L;
    }

    long remaining = size - bytesToBeReserved;
    checkState(
        remaining <= reservedBytesInTotal,
        StreamNodeMessages.EXCEPTION_RELEASED_BYTES_HAS_BEEN_LARGER_THAN_RESERVED_69DE5487);
    bytesToBeReserved = 0;
    reservedBytesInTotal -= remaining;
    return remaining;
  }

  @Override
  public void releaseAllReservedMemory() {
    if (reservedBytesInTotal != 0) {
      STREAM_EXECUTION_PLANNER.releaseToFreeMemoryForCompute(reservedBytesInTotal);
      reservedBytesInTotal = 0;
    }
    bytesToBeReserved = 0;
    bytesToBeReleased = 0;
  }

  @Override
  public Pair<Long, Long> releaseMemoryVirtually(final long size) {
    long poolBytes = deductReleaseAccounting(size);
    return new Pair<>(size - poolBytes, poolBytes);
  }

  @Override
  public void reserveMemoryVirtually(
      final long bytesToBeReserved, final long bytesAlreadyReserved) {
    reservedBytesInTotal += bytesAlreadyReserved;
    reserveMemoryCumulatively(bytesToBeReserved);
  }
}
