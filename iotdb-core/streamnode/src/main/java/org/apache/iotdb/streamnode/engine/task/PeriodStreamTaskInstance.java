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

package org.apache.iotdb.streamnode.engine.task;

import org.apache.iotdb.commons.concurrent.threadpool.ScheduledExecutorUtil;
import org.apache.iotdb.commons.stream.ListPartitionKey;
import org.apache.iotdb.commons.stream.PartitionKey;
import org.apache.iotdb.commons.stream.StreamTask;
import org.apache.iotdb.streamnode.engine.scheduler.IStreamTaskScheduler;

import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory.newScheduledThreadPoolWithDaemon;

public class PeriodStreamTaskInstance extends AbstractStreamTaskInstance {

  private static final long MIN_RECREATE_INTERVAL_MS = 1000L;
  private static final long MAX_RECREATE_INTERVAL_MS = 30000L;
  private static final long FAILURE_RESET_INTERVAL_MS = 30000L;
  // Only start timer when failures reach max backoff delay
  private static final int FAILURE_THRESHOLD_FOR_TIMER = 5;

  private static final ScheduledExecutorService SUB_TASK_RECREATE_POOL =
      newScheduledThreadPoolWithDaemon(1, "PeriodStreamTaskInstance-Recreate");

  private final AtomicLong lastRecreateTime = new AtomicLong(0L);
  private final AtomicInteger consecutiveFailures = new AtomicInteger(0);
  private final AtomicLong lastFailureTime = new AtomicLong(0L);
  private volatile ScheduledFuture<?> failureResetTimer;
  private volatile ScheduledFuture<?> pendingRecreate;

  public PeriodStreamTaskInstance(
      StreamTask taskDefinition,
      IStreamTaskScheduler scheduler,
      ExecutorService subTaskNotificationExecutor,
      boolean ownsExecutionResources) {
    super(taskDefinition, scheduler, subTaskNotificationExecutor, ownsExecutionResources);
  }

  @Override
  protected void startInternal() {
    // Period window only have one partition and not trigger by subscription
    if (subTaskExecutions.isEmpty()) {
      getOrCreateSubTask(new ListPartitionKey(Collections.emptyList()));
    } else {
      subTaskExecutions.values().forEach(StreamSubTaskExecution::start);
    }
  }

  @Override
  protected void stopInternal() {
    subTaskExecutions.values().forEach(StreamSubTaskExecution::stop);
  }

  @Override
  protected void dropInternal() {
    stopFailureResetTimer();
    cancelPendingRecreate();
  }

  @Override
  protected void onSubTaskStateChanged(
      PartitionKey partitionKey, StreamSubTaskExecution execution, StreamSubTaskState newState) {
    if (!newState.isDone()) {
      return;
    }
    subTaskExecutions.remove(partitionKey);
    if (newState.isFailed()) {
      LOGGER.warn(
          "Sub-task of task {} and partition {} failed",
          taskDefinition.getTaskName(),
          partitionKey,
          execution.getContext().getFailureCause().orElse(null));
      scheduleRecreateWithRateLimit();
      return;
    }
    // Long-running tasks rarely reach terminal state, so success handling is done by timer
    LOGGER.info(
        "Sub-task of task {} and partition {} entered terminal state {}",
        taskDefinition.getTaskName(),
        partitionKey,
        newState);
  }

  private void scheduleRecreateWithRateLimit() {
    if (!running.get()) {
      return;
    }
    long currentTime = System.currentTimeMillis();
    lastFailureTime.set(currentTime);

    int failures = consecutiveFailures.incrementAndGet();
    long backoffDelay =
        Math.min(
            MIN_RECREATE_INTERVAL_MS * (1 << Math.min(failures - 1, 5)), MAX_RECREATE_INTERVAL_MS);

    // Start timer on-demand when failures reach threshold (max backoff delay)
    if (failures >= FAILURE_THRESHOLD_FOR_TIMER && failureResetTimer == null) {
      startFailureResetTimer();
    }

    long lastTime = lastRecreateTime.get();
    long waitTime = Math.max(0L, lastTime + backoffDelay - currentTime);

    if (waitTime > 0) {
      LOGGER.warn(
          "Task {} will recreate sub-task after {} ms due to rate limiting (failures: {})",
          taskDefinition.getTaskName(),
          waitTime,
          failures);
      cancelPendingRecreate();
      pendingRecreate =
          SUB_TASK_RECREATE_POOL.schedule(
              this::recreatePeriodSubTask, waitTime, TimeUnit.MILLISECONDS);
    } else {
      recreatePeriodSubTask();
    }
  }

  private void recreatePeriodSubTask() {
    if (running.get()) {
      lastRecreateTime.set(System.currentTimeMillis());
      getOrCreateSubTask(new ListPartitionKey(Collections.emptyList()));
    }
  }

  private void startFailureResetTimer() {
    // Double-checked locking for thread safety
    if (failureResetTimer != null) {
      return;
    }

    synchronized (this) {
      if (failureResetTimer != null) {
        return;
      }

      failureResetTimer =
          ScheduledExecutorUtil.safelyScheduleWithFixedDelay(
              SUB_TASK_RECREATE_POOL,
              () -> {
                if (!running.get()) {
                  return;
                }

                long lastFail = lastFailureTime.get();
                if (lastFail <= 0) {
                  return;
                }

                // Check if any sub-task has successfully processed data after last failure
                // by checking StreamDriver's getLastSuccessfulProcessTime()
                boolean hasSuccessfulProcessing =
                    subTaskExecutions.values().stream()
                        .anyMatch(
                            execution -> {
                              if (execution != null && execution.getDriver() != null) {
                                long lastSuccess =
                                    execution.getDriver().getLastSuccessfulProcessTime();
                                return lastSuccess > lastFail;
                              }
                              return false;
                            });

                // Reset failure count only when successful processing is detected
                if (hasSuccessfulProcessing) {
                  int previousFailures = consecutiveFailures.getAndSet(0);
                  if (previousFailures > 0) {
                    LOGGER.info(
                        "Task {} failure count reset from {} to 0 (successful processing detected)",
                        taskDefinition.getTaskName(),
                        previousFailures);
                  }
                  lastFailureTime.set(0);
                  stopFailureResetTimer();
                }
              },
              FAILURE_RESET_INTERVAL_MS,
              FAILURE_RESET_INTERVAL_MS,
              TimeUnit.MILLISECONDS);
    }
  }

  private void stopFailureResetTimer() {
    if (failureResetTimer != null) {
      failureResetTimer.cancel(true);
      failureResetTimer = null;
    }
  }

  private void cancelPendingRecreate() {
    if (pendingRecreate != null) {
      pendingRecreate.cancel(true);
      pendingRecreate = null;
    }
  }
}
