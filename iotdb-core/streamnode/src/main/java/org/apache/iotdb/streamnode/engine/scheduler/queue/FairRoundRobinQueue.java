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

package org.apache.iotdb.streamnode.engine.scheduler.queue;

import org.apache.iotdb.calc.execution.schedule.queue.IndexedBlockingReserveQueue;
import org.apache.iotdb.streamnode.engine.scheduler.task.DriverTaskId;
import org.apache.iotdb.streamnode.engine.scheduler.task.StreamDriverTask;

import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.ConcurrentHashMap;

import static com.google.common.base.Preconditions.checkArgument;

public class FairRoundRobinQueue extends IndexedBlockingReserveQueue<StreamDriverTask> {

  private final PriorityQueue<StreamDriverTask> readyQueue;
  private final Map<DriverTaskId, StreamDriverTask> taskIdToTaskMap;

  public FairRoundRobinQueue(int maxCapacity, StreamDriverTask queryHolder) {
    super(maxCapacity, queryHolder);
    this.readyQueue = new PriorityQueue<>(new StreamDriverTask.SchedulePriorityComparator());
    this.taskIdToTaskMap = new ConcurrentHashMap<>();
  }

  @Override
  protected void pushToQueue(StreamDriverTask task) {
    checkArgument(task != null, "task to be pushed is null");
    taskIdToTaskMap.put(task.getDriverTaskId(), task);
    readyQueue.offer(task);
  }

  @Override
  protected StreamDriverTask pollFirst() {
    StreamDriverTask task = readyQueue.poll();
    if (task != null) {
      taskIdToTaskMap.remove(task.getDriverTaskId());
    }
    return task;
  }

  @Override
  protected StreamDriverTask remove(StreamDriverTask task) {
    checkArgument(task != null, "task is null");
    StreamDriverTask removed = taskIdToTaskMap.remove(task.getDriverTaskId());
    if (removed != null) {
      readyQueue.remove(removed);
    }
    return removed;
  }

  @Override
  public boolean isEmpty() {
    return taskIdToTaskMap.isEmpty();
  }

  @Override
  protected boolean contains(StreamDriverTask task) {
    return taskIdToTaskMap.containsKey(task.getDriverTaskId());
  }

  @Override
  protected StreamDriverTask get(StreamDriverTask task) {
    if (task.getDriverTaskId() == null) {
      return null;
    }
    return taskIdToTaskMap.get(task.getDriverTaskId());
  }

  @Override
  protected void clearAllElements() {
    readyQueue.clear();
    taskIdToTaskMap.clear();
  }

  /**
   * Called by the scheduler (via {@link
   * org.apache.iotdb.streamnode.engine.scheduler.DriverTaskHandle}) after each execution round.
   * Returns {@code Priority(lastScheduledTime)} — the task's last finish time, which {@link
   * StreamDriverTask.SchedulePriorityComparator} uses for round-robin ordering: smaller values →
   * finished earlier → waited longer → polled first.
   */
  public Priority updatePriority(Priority oldPriority, long lastScheduledTime) {
    return new Priority(lastScheduledTime);
  }
}
