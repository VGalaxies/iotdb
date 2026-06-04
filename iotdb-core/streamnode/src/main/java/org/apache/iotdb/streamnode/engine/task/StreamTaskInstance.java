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

import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.concurrent.threadpool.ScheduledExecutorUtil;
import org.apache.iotdb.commons.stream.PartitionKey;
import org.apache.iotdb.commons.stream.StreamSource;
import org.apache.iotdb.commons.stream.StreamTask;
import org.apache.iotdb.streamnode.conf.StreamNodeConfig;
import org.apache.iotdb.streamnode.conf.StreamNodeDescriptor;
import org.apache.iotdb.streamnode.engine.computation.planner.StreamExecutionPlanner;
import org.apache.iotdb.streamnode.engine.dispatcher.TabletDispatcher;
import org.apache.iotdb.streamnode.engine.scheduler.IStreamTaskScheduler;
import org.apache.iotdb.streamnode.engine.scheduler.StreamTaskScheduler;
import org.apache.iotdb.streamnode.engine.scheduler.task.StreamDriver;
import org.apache.iotdb.streamnode.engine.sink.WriteBackEngine;
import org.apache.iotdb.streamnode.engine.source.StreamSourceInstance;

import org.apache.tsfile.write.record.Tablet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;

import static org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory.newScheduledThreadPoolWithDaemon;

public class StreamTaskInstance {

  private static final Logger LOGGER = LoggerFactory.getLogger(StreamTaskInstance.class);
  private static final ScheduledExecutorService TASK_COMMITTER_POOL =
      newScheduledThreadPoolWithDaemon(1, "StreamTaskInstance-Committer");
  private static final long COMMIT_INTERVAL_IN_MS = 1000L;

  private final StreamTask taskDefinition;
  private final Map<PartitionKey, StreamSubTaskExecution> subTaskExecutions =
      new ConcurrentHashMap<>();
  private final WriteBackEngine writeBackEngine;
  private final ExecutorService subTaskNotificationExecutor;
  private final IStreamTaskScheduler scheduler;
  private final boolean ownsExecutionResources;
  private final AtomicBoolean running = new AtomicBoolean(false);
  private final AtomicLong lastCommittedSourceIndex = new AtomicLong(-1L);
  private final StreamNodeConfig nodeConfig = StreamNodeDescriptor.getInstance().getConfig();

  private StreamSourceInstance sourceInstance;
  private TabletDispatcher dispatcher;
  private ScheduledFuture<?> taskCommitter;

  public StreamTaskInstance(StreamTask taskDefinition) {
    this(
        taskDefinition,
        new StreamTaskScheduler(),
        IoTDBThreadPoolFactory.newFixedThreadPool(4, "Stream-SubTask-Notification"),
        true);
  }

  public StreamTaskInstance(
      StreamTask taskDefinition,
      IStreamTaskScheduler scheduler,
      ExecutorService subTaskNotificationExecutor) {
    this(taskDefinition, scheduler, subTaskNotificationExecutor, false);
  }

  private StreamTaskInstance(
      StreamTask taskDefinition,
      IStreamTaskScheduler scheduler,
      ExecutorService subTaskNotificationExecutor,
      boolean ownsExecutionResources) {
    this.taskDefinition = taskDefinition;
    this.scheduler = scheduler;
    this.subTaskNotificationExecutor = subTaskNotificationExecutor;
    this.ownsExecutionResources = ownsExecutionResources;
    this.writeBackEngine = new WriteBackEngine(taskDefinition);
  }

  public void start() {
    if (!running.compareAndSet(false, true)) {
      LOGGER.warn("Task {} is already running", taskDefinition.getTaskName());
      return;
    }

    try {
      if (ownsExecutionResources) {
        scheduler.start();
      }

      if (taskDefinition.getTarget() != null) {
        writeBackEngine.start();
      }

      if (taskDefinition.getSource() != null) {
        dispatcher = createDispatcher(taskDefinition.getSource());
        sourceInstance =
            createSourceInstance(
                taskDefinition.getSource(),
                taskDefinition.getTaskName(),
                dispatcher::dispatch,
                nodeConfig);
        startTaskCommitter();
        sourceInstance.start();
      }

      LOGGER.info("Task instance started: {}", taskDefinition.getTaskName());
    } catch (Exception e) {
      LOGGER.error("Failed to start task instance: {}", taskDefinition.getTaskName(), e);
      stopTaskCommitter();
      if (ownsExecutionResources) {
        scheduler.stop();
      }
      running.set(false);
    }
  }

  public void stop() {
    if (!running.compareAndSet(true, false)) {
      return;
    }

    try {
      stopTaskCommitter();
      if (sourceInstance != null) {
        sourceInstance.stop();
      }

      subTaskExecutions.values().forEach(StreamSubTaskExecution::stop);
      subTaskExecutions.values().forEach(StreamSubTaskExecution::awaitCleanupFinished);
      subTaskExecutions.clear();

      writeBackEngine.stop();
      if (ownsExecutionResources) {
        scheduler.stop();
        subTaskNotificationExecutor.shutdown();
      }

      LOGGER.info("Task instance stopped: {}", taskDefinition.getTaskName());
    } catch (Exception e) {
      LOGGER.error("Error stopping task instance: {}", taskDefinition.getTaskName(), e);
    }
  }

  TabletDispatcher createDispatcher(StreamSource source) {
    return TabletDispatcher.create(source, this::getOrCreateSubTask);
  }

  StreamSourceInstance createSourceInstance(
      StreamSource source,
      String taskName,
      BiConsumer<Tablet, Long> consumer,
      StreamNodeConfig config) {
    return StreamSourceInstance.create(source, taskName, consumer, config);
  }

  public StreamSubTask getOrCreateSubTask(PartitionKey key) {
    return subTaskExecutions
        .computeIfAbsent(
            key,
            partitionKey -> {
              LOGGER.debug("Creating sub-task for partition: {}", partitionKey);
              StreamSubTaskStateMachine stateMachine =
                  new StreamSubTaskStateMachine(
                      taskDefinition.getTaskName() + "-" + partitionKey,
                      subTaskNotificationExecutor);
              StreamSubTaskContext subTaskContext =
                  new StreamSubTaskContext(
                      taskDefinition.getTaskName(), partitionKey, stateMachine);
              StreamSubTask subTask =
                  StreamExecutionPlanner.getInstance().plan(subTaskContext, this);
              StreamDriver driver = new StreamDriver(subTask, subTask.getDriverContext());
              subTask.setConsumer(driver::push);
              StreamSubTaskExecution execution =
                  StreamSubTaskExecution.create(scheduler, subTask, driver, 0);
              stateMachine.addStateChangeListener(
                  newState -> onSubTaskStateChanged(partitionKey, execution, newState));
              return execution;
            })
        .getSubTask();
  }

  private void onSubTaskStateChanged(
      PartitionKey partitionKey, StreamSubTaskExecution execution, StreamSubTaskState newState) {
    if (!newState.isDone()) {
      return;
    }

    // TODO: After StreamSubTaskExecution finishes cleanup, remove the old execution with
    // remove(partitionKey, execution) and decide whether a failed sub-task should be isolated
    // to its partition or cascade to the whole stream task.
    if (newState.isFailed()) {
      LOGGER.warn(
          "Sub-task of task {} and partition {} failed",
          taskDefinition.getTaskName(),
          partitionKey,
          execution.getContext().getFailureCause().orElse(null));
      return;
    }

    LOGGER.info(
        "Sub-task of task {} and partition {} entered terminal state {}",
        taskDefinition.getTaskName(),
        partitionKey,
        newState);
  }

  public StreamTask getTaskDefinition() {
    return taskDefinition;
  }

  public WriteBackEngine getWriteBackEngine() {
    return writeBackEngine;
  }

  public boolean isRunning() {
    return running.get();
  }

  public Map<PartitionKey, StreamSubTask> getSubTasksSnapshot() {
    Map<PartitionKey, StreamSubTask> snapshot = new HashMap<>();
    subTaskExecutions.forEach(
        (partitionKey, execution) -> snapshot.put(partitionKey, execution.getSubTask()));
    return Collections.unmodifiableMap(snapshot);
  }

  public long getLastCommittedSourceIndex() {
    return lastCommittedSourceIndex.get();
  }

  public long getMinimumSubTaskCommitId() {
    return subTaskExecutions.values().stream()
        .map(StreamSubTaskExecution::getSubTask)
        .mapToLong(StreamSubTask::getCommitId)
        .min()
        .orElse(-1L);
  }

  public synchronized long commitProcessedProgress() throws Exception {
    if (sourceInstance == null || subTaskExecutions.isEmpty()) {
      return lastCommittedSourceIndex.get();
    }

    long minimumCommitId = getMinimumSubTaskCommitId();
    if (minimumCommitId > lastCommittedSourceIndex.get()) {
      sourceInstance.commit(minimumCommitId);
      lastCommittedSourceIndex.set(minimumCommitId);
      LOGGER.debug(
          "Committed source progress {} for task {}",
          minimumCommitId,
          taskDefinition.getTaskName());
    }
    return lastCommittedSourceIndex.get();
  }

  private void startTaskCommitter() {
    stopTaskCommitter();
    taskCommitter =
        ScheduledExecutorUtil.safelyScheduleWithFixedDelay(
            TASK_COMMITTER_POOL,
            this::commitProcessedProgressSafely,
            COMMIT_INTERVAL_IN_MS,
            COMMIT_INTERVAL_IN_MS,
            TimeUnit.MILLISECONDS);
  }

  private void stopTaskCommitter() {
    if (taskCommitter != null) {
      taskCommitter.cancel(true);
      taskCommitter = null;
    }
  }

  private void commitProcessedProgressSafely() {
    if (!running.get()) {
      return;
    }
    try {
      commitProcessedProgress();
    } catch (Exception e) {
      LOGGER.warn("Failed to commit source progress for task {}", taskDefinition.getTaskName(), e);
    }
  }
}
