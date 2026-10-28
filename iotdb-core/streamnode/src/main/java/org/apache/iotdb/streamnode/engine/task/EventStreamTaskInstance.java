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
import org.apache.iotdb.commons.service.metric.MetricService;
import org.apache.iotdb.commons.stream.PartitionKey;
import org.apache.iotdb.commons.stream.StreamSource;
import org.apache.iotdb.commons.stream.StreamTask;
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.streamnode.conf.StreamNodeConfig;
import org.apache.iotdb.streamnode.engine.dispatcher.ColumnPartitionedTabletDispatcher;
import org.apache.iotdb.streamnode.engine.dispatcher.TabletDispatcher;
import org.apache.iotdb.streamnode.engine.scheduler.IStreamTaskScheduler;
import org.apache.iotdb.streamnode.engine.sink.CommitTracker;
import org.apache.iotdb.streamnode.engine.source.StreamSourceInstance;
import org.apache.iotdb.streamnode.engine.task.StreamSubTask.DataSlice;
import org.apache.iotdb.streamnode.metrics.StreamDispatcherMetrics;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory.newScheduledThreadPoolWithDaemon;

public class EventStreamTaskInstance extends AbstractStreamTaskInstance {

  private static final ScheduledExecutorService TASK_COMMITTER_POOL =
      newScheduledThreadPoolWithDaemon(1, "StreamTaskInstance-Committer");
  private static final long COMMIT_INTERVAL_IN_MS = 1000L;

  private final AtomicLong lastCommittedSourceIndex = new AtomicLong(-1L);

  private StreamSourceInstance sourceInstance;
  private TabletDispatcher dispatcher;
  private IMetricSet dispatcherMetrics;
  private ScheduledFuture<?> taskCommitter;

  public EventStreamTaskInstance(
      StreamTask taskDefinition,
      IStreamTaskScheduler scheduler,
      ExecutorService subTaskNotificationExecutor,
      boolean ownsExecutionResources) {
    super(taskDefinition, scheduler, subTaskNotificationExecutor, ownsExecutionResources);
  }

  @Override
  protected void startInternal() throws Exception {
    if (sourceInstance == null) {
      if (taskDefinition.getSource() != null) {
        sourceInstance =
            createSourceInstance(
                taskDefinition.getSource(), taskDefinition.getTaskName(), nodeConfig);
        this.commitTracker = new CommitTracker(sourceInstance::commit);
        writeBackEngine.getPipeline().setCommitTracker(commitTracker);
        dispatcher = createDispatcher(taskDefinition.getSource(), commitTracker);
        try {
          if (dispatcher instanceof ColumnPartitionedTabletDispatcher columnDispatcher) {
            dispatcherMetrics =
                new StreamDispatcherMetrics(columnDispatcher, taskDefinition.getTaskName());
            MetricService.getInstance().addMetricSet(dispatcherMetrics);
          }
          sourceInstance.start(dispatcher::dispatch);
        } catch (final Exception e) {
          deregisterDispatcherMetrics();
          dispatcher = null;
          sourceInstance = null;
          throw e;
        }
      }
    }
    // to restart the stopped subscription.
    else {
      sourceInstance.update(taskDefinition.getSource());
      sourceInstance.start(null);
    }
  }

  @Override
  protected void stopInternal() throws Exception {
    if (sourceInstance != null) {
      sourceInstance.stop();
    }
  }

  @Override
  protected void dropInternal() throws Exception {
    stopTaskCommitter();
    try {
      if (sourceInstance != null) {
        sourceInstance.drop();
      }
    } finally {
      sourceInstance = null;
      deregisterDispatcherMetrics();
      dispatcher = null;
    }
  }

  private void deregisterDispatcherMetrics() {
    if (dispatcherMetrics == null) {
      return;
    }
    MetricService.getInstance().removeMetricSet(dispatcherMetrics);
    dispatcherMetrics = null;
  }

  @Override
  protected void onSubTaskStateChanged(
      PartitionKey partitionKey, StreamSubTaskExecution execution, StreamSubTaskState newState) {
    if (!newState.isDone()) {
      return;
    }
    subTaskExecutions.remove(partitionKey);
    if (newState.isFailed()) {
      // NOTE: scheduler-overload-induced FAILED is not retried here. The synchronous recreate
      // path (TabletDispatcher.process -> getOrCreateSubTask) bypasses this callback entirely,
      // so a backoff scheduled here would not throttle the source-driven recreate storm.
      // Throttling overload requires an admission gate inside getOrCreateSubTask plus a
      // TabletDispatcher contract change (allow null / placeholder return) — left as future work.
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

  protected TabletDispatcher createDispatcher(StreamSource source, CommitTracker commitTracker) {
    return TabletDispatcher.create(source, this::consumeData, commitTracker);
  }

  private Future<?> consumeData(PartitionKey partitionKey, List<DataSlice> slices) {
    final StreamSubTask subTask = getOrCreateSubTask(partitionKey);
    return subTask.offer(slices);
  }

  protected StreamSourceInstance createSourceInstance(
      StreamSource source, String taskName, StreamNodeConfig config) {
    return StreamSourceInstance.create(source, taskDefinition.getProperties(), taskName, config);
  }

  public long getLastCommittedSourceIndex() {
    return lastCommittedSourceIndex.get();
  }

  public long getMinimumSubTaskCommitId() {
    return subTaskExecutions.values().stream()
        .filter(Objects::nonNull)
        .map(StreamSubTaskExecution::getSubTask)
        .filter(Objects::nonNull)
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
