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

import org.apache.iotdb.streamnode.engine.scheduler.IStreamTaskScheduler;
import org.apache.iotdb.streamnode.engine.scheduler.task.IStreamDriver;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.CountDownLatch;

public class StreamSubTaskExecution {

  private static final Logger LOGGER = LoggerFactory.getLogger(StreamSubTaskExecution.class);

  private final StreamSubTask subTask;
  private final IStreamDriver driver;
  private final CountDownLatch cleanupFinished = new CountDownLatch(1);

  public static StreamSubTaskExecution create(
      IStreamTaskScheduler scheduler, StreamSubTask subTask, IStreamDriver driver, long timeoutMs) {
    StreamSubTaskExecution execution = new StreamSubTaskExecution(subTask, driver);
    execution.initialize();
    scheduler.submitStreamDriver(driver, timeoutMs);
    execution.start();
    return execution;
  }

  private StreamSubTaskExecution(StreamSubTask subTask, IStreamDriver driver) {
    this.subTask = Objects.requireNonNull(subTask, "subTask should not be null");
    this.driver = Objects.requireNonNull(driver, "driver should not be null");
  }

  public StreamSubTaskState getState() {
    return subTask.getStateMachine().getState();
  }

  public StreamSubTaskContext getContext() {
    return subTask.getContext();
  }

  public StreamSubTask getSubTask() {
    return subTask;
  }

  public IStreamDriver getDriver() {
    return driver;
  }

  public void start() {
    subTask.getStateMachine().start();
  }

  public void markStopping() {
    subTask.getStateMachine().markStopping();
  }

  public void stop() {
    subTask.getStateMachine().stop();
  }

  public void drop() {
    subTask.getStateMachine().drop();
  }

  public void failed(Throwable cause) {
    subTask.getStateMachine().failed(cause);
  }

  @SuppressWarnings("squid:S2142")
  public void awaitCleanupFinished() {
    boolean wasInterrupted = false;
    while (true) {
      try {
        cleanupFinished.await();
        break;
      } catch (InterruptedException e) {
        wasInterrupted = true;
      }
    }
    if (wasInterrupted) {
      Thread.currentThread().interrupt();
    }
  }

  private void initialize() {
    subTask.getStateMachine().addStateChangeListener(this::onStateChanged);
  }

  private void onStateChanged(StreamSubTaskState newState) {
    if (!newState.isDone()) {
      return;
    }

    try {
      if (newState.isFailed()) {
        LOGGER.warn(
            "Stream sub task {} enters failed state, close its driver",
            subTask.getStateMachine(),
            subTask.getContext().getFailureCause().orElse(null));
      }
      driver.close();
    } catch (Throwable t) {
      LOGGER.error(
          "Errors occurred while attempting to close stream sub task {}.",
          subTask.getStateMachine(),
          t);
    }

    try {
      subTask.getContext().releaseResourceWhenDriverClosed();
    } catch (Throwable t) {
      LOGGER.error(
          "Errors occurred while attempting to release shared resources for stream sub task {}.",
          subTask.getStateMachine(),
          t);
    } finally {
      cleanupFinished.countDown();
    }
  }
}
