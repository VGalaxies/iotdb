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

import org.apache.iotdb.commons.audit.UserEntity;
import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.queryengine.common.SessionInfo;
import org.apache.iotdb.commons.queryengine.common.SqlDialect;
import org.apache.iotdb.commons.service.metric.MetricService;
import org.apache.iotdb.commons.stream.PartitionKey;
import org.apache.iotdb.commons.stream.StreamTask;
import org.apache.iotdb.commons.stream.StreamWindowType;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.streamnode.conf.StreamNodeConfig;
import org.apache.iotdb.streamnode.conf.StreamNodeDescriptor;
import org.apache.iotdb.streamnode.engine.computation.planner.StreamExecutionPlanner;
import org.apache.iotdb.streamnode.engine.scheduler.IStreamTaskScheduler;
import org.apache.iotdb.streamnode.engine.scheduler.StreamTaskScheduler;
import org.apache.iotdb.streamnode.engine.scheduler.task.StreamDriver;
import org.apache.iotdb.streamnode.engine.sink.CommitTracker;
import org.apache.iotdb.streamnode.engine.sink.SinkPipeline;
import org.apache.iotdb.streamnode.engine.sink.WriteBackEngine;
import org.apache.iotdb.streamnode.engine.window.WindowEngineTickScheduler;
import org.apache.iotdb.streamnode.i18n.StreamNodeMessages;
import org.apache.iotdb.streamnode.metrics.SinkPipelineMetrics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.time.ZoneId;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public abstract class AbstractStreamTaskInstance {

  protected static final Logger LOGGER = LoggerFactory.getLogger(AbstractStreamTaskInstance.class);

  protected final StreamTask taskDefinition;
  protected final Map<PartitionKey, StreamSubTaskExecution> subTaskExecutions =
      new ConcurrentHashMap<>();
  protected final WriteBackEngine writeBackEngine;
  protected final ExecutorService subTaskNotificationExecutor;
  protected final IStreamTaskScheduler scheduler;
  protected final WindowEngineTickScheduler windowTickScheduler;

  protected final boolean ownsExecutionResources;
  protected final AtomicBoolean running = new AtomicBoolean(false);
  protected final StreamNodeConfig nodeConfig = StreamNodeDescriptor.getInstance().getConfig();
  private boolean writeBackEngineStarted = false;
  private @Nullable SinkPipelineMetrics sinkPipelineMetrics;

  private static final SessionInfo DEFAULT_SESSION_INFO =
      new SessionInfo(
          0L, new UserEntity(-1, "", ""), ZoneId.systemDefault(), null, SqlDialect.TABLE);

  protected @Nullable CommitTracker commitTracker;

  /** Task-scoped window id allocator shared by all sub-tasks. */
  private final AtomicLong nextWindowId = new AtomicLong(0);

  protected AbstractStreamTaskInstance(
      StreamTask taskDefinition,
      IStreamTaskScheduler scheduler,
      ExecutorService subTaskNotificationExecutor,
      boolean ownsExecutionResources) {
    this.taskDefinition = taskDefinition;
    this.scheduler = scheduler;
    this.subTaskNotificationExecutor = subTaskNotificationExecutor;
    this.ownsExecutionResources = ownsExecutionResources;
    this.writeBackEngine = new WriteBackEngine(taskDefinition);
    this.windowTickScheduler =
        new WindowEngineTickScheduler("WindowEngineTick-" + taskDefinition.getTaskName());
  }

  public static AbstractStreamTaskInstance create(StreamTask taskDefinition) {
    return create(
        taskDefinition,
        new StreamTaskScheduler(),
        IoTDBThreadPoolFactory.newFixedThreadPool(4, "Stream-SubTask-Notification"),
        true);
  }

  public static AbstractStreamTaskInstance create(
      StreamTask taskDefinition,
      IStreamTaskScheduler scheduler,
      ExecutorService subTaskNotificationExecutor) {
    return create(taskDefinition, scheduler, subTaskNotificationExecutor, false);
  }

  private static AbstractStreamTaskInstance create(
      StreamTask taskDefinition,
      IStreamTaskScheduler scheduler,
      ExecutorService subTaskNotificationExecutor,
      boolean ownsExecutionResources) {
    if (taskDefinition.getWindow().getType().equals(StreamWindowType.PERIOD)) {
      return new PeriodStreamTaskInstance(
          taskDefinition, scheduler, subTaskNotificationExecutor, ownsExecutionResources);
    } else {
      return new EventStreamTaskInstance(
          taskDefinition, scheduler, subTaskNotificationExecutor, ownsExecutionResources);
    }
  }

  public void start() throws Exception {
    if (!running.compareAndSet(false, true)) {
      LOGGER.warn("Task {} is already running", taskDefinition.getTaskName());
      return;
    }

    if (ownsExecutionResources) {
      scheduler.start();
    }

    if (taskDefinition.getTarget() != null && !writeBackEngineStarted) {
      writeBackEngine.start();
      writeBackEngineStarted = true;
      registerSinkPipelineMetrics();
    }

    windowTickScheduler.start();

    startInternal();

    LOGGER.info("Task instance started: {}", taskDefinition.getTaskName());
  }

  protected abstract void startInternal() throws Exception;

  public void stop() {
    if (!running.compareAndSet(true, false)) {
      return;
    }
    try {
      stopInternal();
    } catch (Exception e) {
      LOGGER.warn("Error stopping task instance: {}", taskDefinition.getTaskName(), e);
    }
  }

  public void drop() {
    try {
      running.set(false);
      dropInternal();

      subTaskExecutions.values().forEach(StreamSubTaskExecution::drop);
      subTaskExecutions.values().forEach(StreamSubTaskExecution::awaitCleanupFinished);
      subTaskExecutions.clear();

      windowTickScheduler.stop();

      deregisterSinkPipelineMetrics();
      writeBackEngine.stop();
      writeBackEngineStarted = false;
      if (ownsExecutionResources) {
        scheduler.stop();
        subTaskNotificationExecutor.shutdown();
      }

      LOGGER.info("Task instance stopped: {}", taskDefinition.getTaskName());
    } catch (Exception e) {
      LOGGER.warn("Error stopping task instance: {}", taskDefinition.getTaskName(), e);
    }
  }

  private void registerSinkPipelineMetrics() {
    SinkPipeline pipeline = writeBackEngine.getPipeline();
    if (pipeline == null || sinkPipelineMetrics != null) {
      return;
    }
    sinkPipelineMetrics = new SinkPipelineMetrics(pipeline, taskDefinition.getTaskName());
    MetricService.getInstance().addMetricSet(sinkPipelineMetrics);
  }

  private void deregisterSinkPipelineMetrics() {
    if (sinkPipelineMetrics == null) {
      return;
    }
    MetricService.getInstance().removeMetricSet(sinkPipelineMetrics);
    sinkPipelineMetrics = null;
  }

  protected abstract void stopInternal() throws Exception;

  protected abstract void dropInternal() throws Exception;

  protected StreamSubTask getOrCreateSubTask(PartitionKey key) {
    return subTaskExecutions.computeIfAbsent(key, this::createSubTaskExecution).getSubTask();
  }

  private StreamSubTaskExecution createSubTaskExecution(PartitionKey partitionKey) {
    LOGGER.debug("Creating sub-task for partition: {}", partitionKey);
    StreamSubTaskStateMachine stateMachine =
        new StreamSubTaskStateMachine(
            taskDefinition.getTaskName() + "-" + partitionKey, subTaskNotificationExecutor);
    StreamSubTaskContext subTaskContext =
        new StreamSubTaskContext(taskDefinition.getTaskName(), partitionKey, stateMachine);
    subTaskContext.setCommitTracker(commitTracker);
    subTaskContext.setWindowIdAllocator(nextWindowId);
    StreamSubTask subTask;
    try {
      subTask = StreamExecutionPlanner.getInstance().plan(subTaskContext, this);
    } catch (Throwable t) {
      // Mark the state machine failed so that the pre-allocation listener registered inside plan()
      // releases the reserved memory, mirroring DataNode's FragmentInstanceManager catch block.
      stateMachine.failed(t);
      LOGGER.warn(
          StreamNodeMessages.LOG_FAILED_TO_PLAN_SUB_TASK_FOR_TASK_ARG_AND_PARTITION_ARG_5DDA0008,
          taskDefinition.getTaskName(),
          partitionKey,
          t);
      throw new RuntimeException(t);
    }
    StreamDriver driver = new StreamDriver(subTask, subTaskContext.getDriverContext());
    subTask.setConsumer(driver::push);
    StreamSubTaskExecution execution = StreamSubTaskExecution.create(scheduler, subTask, driver, 0);
    // create() may have already transitioned the state machine to FAILED (scheduler
    // overload). StateMachine.addStateChangeListener fires the current state to new
    // listeners by contract, so onSubTaskStateChanged will still be invoked.
    stateMachine.addStateChangeListener(
        newState -> onSubTaskStateChanged(partitionKey, execution, newState));
    return execution;
  }

  protected abstract void onSubTaskStateChanged(
      PartitionKey partitionKey, StreamSubTaskExecution execution, StreamSubTaskState newState);

  public SessionInfo getSessionInfo() {
    return DEFAULT_SESSION_INFO;
  }

  public StreamTask getTaskDefinition() {
    return taskDefinition;
  }

  public WriteBackEngine getWriteBackEngine() {
    return writeBackEngine;
  }

  public WindowEngineTickScheduler getWindowTickScheduler() {
    return windowTickScheduler;
  }

  @TestOnly
  public boolean isRunning() {
    return running.get();
  }

  @TestOnly
  public Map<PartitionKey, StreamSubTask> getSubTasksSnapshot() {
    Map<PartitionKey, StreamSubTask> snapshot = new HashMap<>();
    subTaskExecutions.forEach(
        (partitionKey, execution) -> snapshot.put(partitionKey, execution.getSubTask()));
    return Collections.unmodifiableMap(snapshot);
  }

  /** Returns the current number of active sub-tasks tracked by this instance. */
  public int getSubTaskCount() {
    return subTaskExecutions.size();
  }
}
