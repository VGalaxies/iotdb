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

package org.apache.iotdb.streamnode.engine.scheduler;

import org.apache.iotdb.calc.execution.schedule.queue.IndexedBlockingQueue;
import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.streamnode.conf.StreamNodeDescriptor;
import org.apache.iotdb.streamnode.engine.scheduler.task.IStreamDriver;
import org.apache.iotdb.streamnode.engine.scheduler.task.StreamDriverTask;
import org.apache.iotdb.streamnode.exception.DriverTaskAbortedException;
import org.apache.iotdb.streamnode.utils.SetThreadName;

import com.google.common.base.Ticker;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.Duration;

import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

/** The worker thread of {@link StreamDriverTask}. */
public class DriverTaskThread extends AbstractDriverThread {

  private static final double DRIVER_TASK_EXECUTION_TIME_SLICE_IN_MS =
      StreamNodeDescriptor.getInstance().getConfig().getDriverTaskExecutorTimeSliceInMS();

  // We manage thread pool size directly, so create an unlimited pool
  private static final Executor listeningExecutor =
      IoTDBThreadPoolFactory.newCachedThreadPool(
          ThreadName.DRIVER_TASK_SCHEDULER_NOTIFICATION.getName());

  private final Ticker ticker;

  public DriverTaskThread(
      String workerId,
      ThreadGroup tg,
      IndexedBlockingQueue<StreamDriverTask> queue,
      ITaskScheduler scheduler,
      ThreadProducer producer) {
    super(workerId, tg, queue, scheduler, producer);
    this.ticker = Ticker.systemTicker();
  }

  @Override
  public void execute(StreamDriverTask task) throws InterruptedException {
    long startNanos = ticker.read();
    // Try to switch it to RUNNING
    if (!scheduler.readyToRunning(task)) {
      return;
    }
    IStreamDriver driver = task.getDriver();
    Duration timeSlice = getExecutionTimeSliceForDriverTask(task);
    ListenableFuture<?> future = driver.processFor(timeSlice);
    // If the future is cancelled, the task is in an error and should be thrown.
    if (future.isCancelled()) {
      task.setAbortCause(
          new DriverTaskAbortedException(
              task.getDriverTaskId().getFullId(),
              DriverTaskAbortedException.BY_ALREADY_BEING_CANCELLED));
      scheduler.toAborted(task);
      return;
    }
    long quantaScheduledNanos = ticker.read() - startNanos;
    task.setQuantaScheduledNanos(quantaScheduledNanos);
    if (future.isDone()) {
      try {
        future.get();
        scheduler.runningToReady(task);
      } catch (Exception e) {
        Throwable cause = e.getCause();
        String causeMsg = cause != null ? cause.getMessage() : e.getMessage();
        task.setAbortCause(
            new DriverTaskAbortedException(task.getDriverTaskId().getFullId(), causeMsg));
        scheduler.toAborted(task);
      }
    } else {
      scheduler.runningToBlocked(task);
      future.addListener(
          () -> {
            try (SetThreadName driverTaskName =
                new SetThreadName(task.getDriver().getDriverTaskId().getFullId())) {
              scheduler.blockedToReady(task);
            }
          },
          listeningExecutor);
    }
  }

  private Duration getExecutionTimeSliceForDriverTask(StreamDriverTask driverTask) {
    return new Duration(
        driverTask.getWeight() * DRIVER_TASK_EXECUTION_TIME_SLICE_IN_MS, TimeUnit.MILLISECONDS);
  }
}
