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

package org.apache.iotdb.streamnode.engine.task;

import org.apache.iotdb.commons.stream.PartitionKey;
import org.apache.iotdb.streamnode.engine.memory.StreamNodeDataExchangeManager;
import org.apache.iotdb.streamnode.engine.scheduler.task.StreamDriverContext;
import org.apache.iotdb.streamnode.engine.sink.CommitTracker;
import org.apache.iotdb.streamnode.exception.MemoryLeakException;
import org.apache.iotdb.streamnode.i18n.StreamNodeMessages;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

public class StreamSubTaskContext {

  private static final Logger LOGGER = LoggerFactory.getLogger(StreamSubTaskContext.class);

  private static final long LONG_WAIT_DURATION_IN_NANOS = 5_000_000_000L;

  private final String taskName;
  private final PartitionKey partitionKey;
  private final StreamSubTaskStateMachine stateMachine;
  private final StreamDriverContext driverContext;
  private final CountDownLatch driverClosed = new CountDownLatch(1);
  private @Nullable CommitTracker commitTracker;

  /** Task-scoped window id allocator; shared across sub-tasks, null until wired. */
  private @Nullable AtomicLong windowIdAllocator;

  public StreamSubTaskContext(
      String taskName, PartitionKey partitionKey, StreamSubTaskStateMachine stateMachine) {
    this.taskName = Objects.requireNonNull(taskName, "taskName should not be null");
    this.partitionKey = Objects.requireNonNull(partitionKey, "partitionKey should not be null");
    this.stateMachine = Objects.requireNonNull(stateMachine, "stateMachine should not be null");
    this.driverContext = new StreamDriverContext(this);
  }

  public @Nullable CommitTracker getCommitTracker() {
    return commitTracker;
  }

  public void setCommitTracker(@Nullable CommitTracker commitTracker) {
    this.commitTracker = commitTracker;
  }

  public void setWindowIdAllocator(@Nullable AtomicLong windowIdAllocator) {
    this.windowIdAllocator = windowIdAllocator;
  }

  /** Returns the next task-scoped window id. */
  public long allocateWindowId() {
    if (windowIdAllocator == null) {
      throw new IllegalStateException("windowIdAllocator is not set");
    }
    return windowIdAllocator.getAndIncrement();
  }

  public String getTaskName() {
    return taskName;
  }

  public PartitionKey getPartitionKey() {
    return partitionKey;
  }

  public StreamSubTaskStateMachine getStateMachine() {
    return stateMachine;
  }

  public StreamSubTaskState getState() {
    return stateMachine.getState();
  }

  public boolean isDone() {
    return stateMachine.isDone();
  }

  public Optional<Throwable> getFailureCause() {
    return stateMachine.getFailureCause();
  }

  public LinkedBlockingQueue<Throwable> getFailureCauses() {
    return stateMachine.getFailureCauses();
  }

  public void failed(Throwable cause) {
    stateMachine.failed(cause);
  }

  public StreamDriverContext getDriverContext() {
    return driverContext;
  }

  public void signalDriverClosed() {
    driverClosed.countDown();
  }

  @SuppressWarnings("squid:S2142")
  public void releaseResourceWhenDriverClosed() {
    long startTime = System.nanoTime();
    while (true) {
      try {
        driverClosed.await();
        break;
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        LOGGER.warn(
            "Interrupted when await on allDriversClosed, SubTask is {}-{}",
            this.getTaskName(),
            this.getPartitionKey());
      }
    }
    long duration = System.nanoTime() - startTime;
    if (duration >= LONG_WAIT_DURATION_IN_NANOS) {
      LOGGER.warn("Wait {}ms for Driver closed", duration / 1_000_000);
    }
    releaseResource();
  }

  private void releaseResource() {
    try {
      StreamNodeDataExchangeManager.getInstance()
          .deRegisterSubTask(taskName, partitionKey.toString());
    } catch (MemoryLeakException e) {
      LOGGER.error(
          StreamNodeMessages.LOG_MEMORY_LEAK_DETECTED_WHEN_DE_REGISTERING_SUB_TASK_ARG_ARG_1C6BCE9E,
          taskName,
          partitionKey,
          e);
    }
    // TODO: Release resources used by driver.
  }
}
