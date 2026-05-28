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
package org.apache.iotdb.streamnode.engine.scheduler.task;

import org.apache.iotdb.calc.execution.schedule.queue.ID;
import org.apache.iotdb.calc.execution.schedule.queue.IDIndexedAccessible;
import org.apache.iotdb.streamnode.engine.scheduler.DriverTaskHandle;
import org.apache.iotdb.streamnode.engine.scheduler.queue.Priority;

import java.util.Comparator;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class StreamDriverTask implements IDIndexedAccessible {

  private final IStreamDriver streamDriver;

  private final long timeoutMs;

  private AtomicReference<Priority> priority;
  private Throwable abortCause;

  private DriverTaskStatus status;

  private Lock lock = new ReentrantLock();

  private long lastEnterReadyQueueTime;

  // Total scheduled time
  private long totalScheduledNanos;

  // Last scheduled time
  private long quantaScheduledNanos;

  private final DriverTaskHandle driverTaskHandle;

  // Total ready queued time
  private long readyQueuedTime;

  private double weight = 1.0;

  public DriverTaskStatus getStatus() {
    return status;
  }

  public void setStatus(DriverTaskStatus status) {
    this.status = status;
  }

  public StreamDriverTask(
      IStreamDriver streamDriver, long timeoutMs, DriverTaskHandle driverTaskHandle) {
    this.streamDriver = streamDriver;
    this.timeoutMs = timeoutMs;
    this.driverTaskHandle = driverTaskHandle;
    this.status = DriverTaskStatus.READY;
    priority = new AtomicReference<>(new Priority(System.nanoTime()));
  }

  public double getWeight() {
    return weight;
  }

  public void setWeight(double weight) {
    this.weight = weight;
  }

  public void lock() {
    lock.lock();
  }

  public Priority getPriority() {
    return priority.get();
  }

  public IStreamDriver getDriver() {
    return streamDriver;
  }

  public void setQuantaScheduledNanos(long quantaScheduledNanos) {
    this.quantaScheduledNanos = quantaScheduledNanos;
    totalScheduledNanos += quantaScheduledNanos;
  }

  public long getQuantaScheduledNanos() {
    return quantaScheduledNanos;
  }

  public long getTotalScheduledNanos() {
    return totalScheduledNanos;
  }

  @Override
  public DriverTaskId getDriverTaskId() {
    return getDriver().getDriverTaskId();
  }

  public void setAbortCause(Throwable abortCause) {
    this.abortCause = abortCause;
  }

  @Override
  public void setId(ID id) {
    this.streamDriver.setDriverTaskId(id);
  }

  public void unlock() {
    lock.unlock();
  }

  public long getLastEnterReadyQueueTime() {
    return lastEnterReadyQueueTime;
  }

  public void setLastEnterReadyQueueTime(long lastEnterReadyQueueTime) {
    this.lastEnterReadyQueueTime = lastEnterReadyQueueTime;
  }

  public void addReadyQueuedTime(long time) {
    readyQueuedTime += time;
  }

  public long getReadyQueuedTime() {
    return readyQueuedTime;
  }

  public void updateSchedulePriority() {
    Priority newPriority = driverTaskHandle.updateScheduledTimeInNanos(quantaScheduledNanos);
    this.priority.set(newPriority);
  }

  public boolean isEndState() {
    return status == DriverTaskStatus.ABORTED;
  }

  public Optional<Throwable> getAbortCause() {
    return Optional.ofNullable(abortCause);
  }

  public static class SchedulePriorityComparator implements Comparator<StreamDriverTask> {

    @Override
    public int compare(StreamDriverTask o1, StreamDriverTask o2) {
      if (o1.getDriverTaskId().equals(o2.getDriverTaskId())) {
        return 0;
      }
      int result =
          Long.compare(
              o1.priority.get().getLastScheduledTime(), o2.priority.get().getLastScheduledTime());
      if (result != 0) {
        return result;
      }
      return o1.getDriverTaskId().compareTo(o2.getDriverTaskId());
    }
  }
}
