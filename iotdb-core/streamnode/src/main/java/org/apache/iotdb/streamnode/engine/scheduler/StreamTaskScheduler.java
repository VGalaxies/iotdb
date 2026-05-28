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
import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.commons.stream.PartitionKey;
import org.apache.iotdb.commons.stream.TumbleWindow;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.streamnode.conf.StreamNodeConfig;
import org.apache.iotdb.streamnode.conf.StreamNodeDescriptor;
import org.apache.iotdb.streamnode.engine.scheduler.queue.FairRoundRobinQueue;
import org.apache.iotdb.streamnode.engine.scheduler.task.DriverTaskId;
import org.apache.iotdb.streamnode.engine.scheduler.task.DriverTaskStatus;
import org.apache.iotdb.streamnode.engine.scheduler.task.IStreamDriver;
import org.apache.iotdb.streamnode.engine.scheduler.task.StreamDriver;
import org.apache.iotdb.streamnode.engine.scheduler.task.StreamDriverTask;
import org.apache.iotdb.streamnode.engine.task.StreamSubTask;
import org.apache.iotdb.streamnode.exception.DriverTaskAbortedException;
import org.apache.iotdb.streamnode.utils.SetThreadName;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class StreamTaskScheduler implements IStreamTaskScheduler {
  private static final Logger logger = LoggerFactory.getLogger(StreamTaskScheduler.class);
  private final FairRoundRobinQueue readyQueue;
  private final StreamNodeConfig config = StreamNodeDescriptor.getInstance().getConfig();
  private final int WORKER_THREAD_NUM = config.getExecutorThreadNum();
  private final List<AbstractDriverThread> threads;
  private final ThreadGroup workerGroups;
  private final ITaskScheduler scheduler;
  private final Set<DriverTaskId> idleSet = ConcurrentHashMap.newKeySet();
  private final Map<DriverTaskId, StreamDriverTask> registeredTaskMap = new ConcurrentHashMap<>();

  public StreamTaskScheduler() {
    StreamSubTask dummySubTask =
        new StreamSubTask(
            new PartitionKey() {
              @Override
              public int partitionHash() {
                return 0;
              }
            },
            new TumbleWindow("test", 1000, 0),
            null,
            null,
            null,
            "__DUMMY__");
    int TASK_MAX_CAPACITY = WORKER_THREAD_NUM * config.getExecutedTaskCountPerThread();
    this.readyQueue =
        new FairRoundRobinQueue(
            TASK_MAX_CAPACITY, new StreamDriverTask(new StreamDriver(dummySubTask), 0, null));
    this.threads = new ArrayList<>();
    workerGroups = new ThreadGroup("ScheduleThreads");
    scheduler = new Scheduler();
  }

  @Override
  public void submitStreamDriver(IStreamDriver driver, long timeoutMs) {
    DriverTaskHandle driverTaskHandle = new DriverTaskHandle(0, readyQueue, OptionalInt.of(1));
    StreamDriverTask task = new StreamDriverTask(driver, timeoutMs, driverTaskHandle);
    readyQueue.push(task);
    registeredTaskMap.put(task.getDriverTaskId(), task);
  }

  @Override
  public void cancelStreamTask(DriverTaskId id) {
    StreamDriverTask removedTask = registeredTaskMap.get(id);
    if (removedTask != null) {
      removedTask.setAbortCause(
          new DriverTaskAbortedException(
              removedTask.getDriverTaskId().getFullId(),
              DriverTaskAbortedException.BY_SCHEDULER_ABORT_CALLED));
      scheduler.toAborted(removedTask);
    }
  }

  @TestOnly
  public boolean isStreamTaskExist(DriverTaskId id) {
    StreamDriverTask streamDriverTask = registeredTaskMap.get(id);
    return streamDriverTask != null;
  }

  @Override
  public void cancelStreamTask(String streamName) {
    List<DriverTaskId> tasksToRemove = new ArrayList<>();
    registeredTaskMap
        .keySet()
        .forEach(
            id -> {
              if (id.getId().equals(streamName)) {
                tasksToRemove.add(id);
              }
            });

    for (DriverTaskId id : tasksToRemove) {
      cancelStreamTask(id);
    }
  }

  @Override
  public void start() {
    for (int i = 0; i < WORKER_THREAD_NUM; i++) {
      int index = i;
      String threadName = ThreadName.STREAM_EXECUTOR_WORKER.getName() + "-" + i;
      ThreadProducer producer =
          new ThreadProducer() {
            @Override
            public void produce(
                String threadName,
                ThreadGroup workerGroups,
                IndexedBlockingQueue<StreamDriverTask> queue,
                ThreadProducer producer) {
              DriverTaskThread newThread =
                  new DriverTaskThread(threadName, workerGroups, readyQueue, scheduler, this);
              threads.set(index, newThread);
              newThread.start();
            }
          };
      AbstractDriverThread t =
          new DriverTaskThread(threadName, workerGroups, readyQueue, scheduler, producer);
      threads.add(t);
      t.start();
    }
  }

  @Override
  public void stop() {
    for (AbstractDriverThread thread : threads) {
      try {
        thread.close();
        thread.interrupt();
      } catch (Exception e) {
        logger.warn("Failed to stop thread: {}", thread.getName(), e);
      }
    }
    threads.clear();
  }

  /** the default scheduler implementation. */
  private class Scheduler implements ITaskScheduler {
    @Override
    public boolean readyToRunning(StreamDriverTask task) {
      task.lock();
      try {
        if (task.getStatus() != DriverTaskStatus.READY) {
          return false;
        }

        task.setStatus(DriverTaskStatus.RUNNING);
        long readyQueuedTime = System.nanoTime() - task.getLastEnterReadyQueueTime();
        task.addReadyQueuedTime(readyQueuedTime);
      } finally {
        task.unlock();
      }
      return true;
    }

    @Override
    public void runningToReady(StreamDriverTask task) {
      task.lock();
      try {
        if (task.getStatus() != DriverTaskStatus.RUNNING) {
          return;
        }
        task.updateSchedulePriority();
        task.setStatus(DriverTaskStatus.READY);
        task.setLastEnterReadyQueueTime(System.nanoTime());
        readyQueue.repush(task);
      } finally {
        task.unlock();
      }
    }

    @Override
    public void runningToBlocked(StreamDriverTask task) {
      task.lock();
      try {
        if (task.getStatus() != DriverTaskStatus.RUNNING) {
          return;
        }
        task.updateSchedulePriority();
        task.setStatus(DriverTaskStatus.BLOCKED);
        idleSet.add(task.getDriverTaskId());
      } finally {
        task.unlock();
      }
    }

    @Override
    public void blockedToReady(StreamDriverTask task) {
      task.lock();
      try {
        if (task.getStatus() != DriverTaskStatus.BLOCKED) {
          return;
        }
        task.setStatus(DriverTaskStatus.READY);
        readyQueue.repush(task);
        idleSet.remove(task.getDriverTaskId());
      } finally {
        task.unlock();
      }
    }

    @Override
    public void toAborted(StreamDriverTask task) {
      try (SetThreadName driverTaskName =
          new SetThreadName(task.getDriver().getDriverTaskId().getFullId())) {
        task.lock();
        try {
          // If a task is already in an end state, it indicates that the task is finalized in other
          // threads.
          if (task.isEndState()) {
            return;
          }
          logger.info(
              "The task {} is aborted. All other tasks in the same query will be cancelled",
              task.getDriverTaskId());
        } finally {
          task.unlock();
        }
        clearDriverTask(task);
      }
    }
  }

  private void clearDriverTask(StreamDriverTask task) {
    try (SetThreadName driverTaskName =
        new SetThreadName(task.getDriver().getDriverTaskId().getFullId())) {
      try {
        task.lock();
        DriverTaskStatus status = task.getStatus();
        switch (status) {
          // If it has been aborted, return directly
          case ABORTED:
            return;
          case READY:
            task.setStatus(DriverTaskStatus.ABORTED);
            readyQueue.remove(task.getDriverTaskId());
            break;
          case RUNNING:
          case BLOCKED:
            task.setStatus(DriverTaskStatus.ABORTED);
            readyQueue.decreaseReservedSize();
            break;
          default:
            task.setStatus(DriverTaskStatus.ABORTED);
            break;
        }
      } finally {
        task.unlock();
      }

      try {
        task.lock();
        if (task.getStatus() == DriverTaskStatus.ABORTED) {
          try {
            readyQueue.remove(task.getDriverTaskId());
            idleSet.remove(task.getDriverTaskId());
            registeredTaskMap.remove(task.getDriverTaskId());
            task.getDriver().close();
          } catch (Exception e) {
            logger.error("Clear DriverTask failed", e);
          }
        }
      } finally {
        task.unlock();
      }
    }
  }
}
