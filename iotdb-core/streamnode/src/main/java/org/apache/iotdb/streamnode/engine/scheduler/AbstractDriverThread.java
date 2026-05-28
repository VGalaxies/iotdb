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
import org.apache.iotdb.commons.exception.IoTDBRuntimeException;
import org.apache.iotdb.commons.utils.ErrorHandlingCommonUtils;
import org.apache.iotdb.streamnode.engine.scheduler.task.StreamDriverTask;
import org.apache.iotdb.streamnode.exception.DriverTaskAbortedException;
import org.apache.iotdb.streamnode.utils.SetThreadName;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;

/** An abstract executor for {@link StreamDriverTask}. */
public abstract class AbstractDriverThread extends Thread implements Closeable {

  private static final Logger logger = LoggerFactory.getLogger(AbstractDriverThread.class);
  private final IndexedBlockingQueue<StreamDriverTask> queue;
  private final ThreadProducer producer;
  protected final ITaskScheduler scheduler;
  private volatile boolean closed;

  protected AbstractDriverThread(
      String workerId,
      ThreadGroup tg,
      IndexedBlockingQueue<StreamDriverTask> queue,
      ITaskScheduler scheduler,
      ThreadProducer producer) {
    super(tg, workerId);
    this.queue = queue;
    this.scheduler = scheduler;
    this.closed = false;
    this.producer = producer;
  }

  @Override
  public void run() {
    StreamDriverTask next;
    try {
      while (!closed && !Thread.currentThread().isInterrupted()) {
        try {
          next = queue.poll();
        } catch (InterruptedException e) {
          logger.warn("Executor {} failed to poll driver task from queue", this.getName());
          Thread.currentThread().interrupt();
          break;
        }

        if (next == null) {
          logger.error("StreamDriverTask should never be null");
          continue;
        }

        try (SetThreadName StreamDriverTaskName =
            new SetThreadName(next.getDriverTaskId().getFullId())) {
          execute(next);
        } catch (Exception e) {
          // Try-with-resource syntax will call close once after try block is done, so we need to
          // reset the thread name here
          try (SetThreadName StreamDriverTaskName =
              new SetThreadName(next.getDriver().getDriverTaskId().getFullId())) {
            Throwable rootCause = ErrorHandlingCommonUtils.getRootCause(e);
            if (rootCause instanceof IoTDBRuntimeException) {
              next.setAbortCause(rootCause);
            } else {
              logger.warn("[ExecuteFailed]", rootCause);
              next.setAbortCause(
                  new DriverTaskAbortedException(
                      next.getDriverTaskId().getFullId(),
                      DriverTaskAbortedException.BY_INTERNAL_ERROR_SCHEDULED));
            }
            scheduler.toAborted(next);
          }
        } finally {
          // Clear the interrupted flag on the current thread, driver cancellation may have
          // triggered an interrupt
          if (Thread.interrupted() && closed) {
            // Reset interrupted flag if closed before interrupt
            Thread.currentThread().interrupt();
          }
        }
      }
    } finally {
      // Unless we have been closed, we need to replace this thread
      if (!closed) {
        logger.warn(
            "Executor {} exits because it's interrupted. We will produce another thread to replace.",
            this.getName());
        producer.produce(getName(), getThreadGroup(), queue, producer);
      } else {
        logger.info("Executor {} exits because it is closed.", this.getName());
      }
    }
  }

  /**
   * Processing a task.
   *
   * @throws InterruptedException if the task processing is interrupted
   */
  protected abstract void execute(StreamDriverTask task) throws InterruptedException;

  @Override
  public void close() throws IOException {
    closed = true;
  }
}
