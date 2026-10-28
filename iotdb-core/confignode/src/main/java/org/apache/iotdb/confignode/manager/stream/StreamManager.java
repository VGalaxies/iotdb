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

package org.apache.iotdb.confignode.manager.stream;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.common.rpc.thrift.TStreamNodeConfiguration;
import org.apache.iotdb.common.rpc.thrift.TStreamNodeLocation;
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.commons.concurrent.threadpool.ScheduledExecutorUtil;
import org.apache.iotdb.commons.pipe.config.constant.SystemConstant;
import org.apache.iotdb.commons.stream.IoTDBSubscriptionSource;
import org.apache.iotdb.commons.stream.StreamTask;
import org.apache.iotdb.commons.stream.StreamTaskExpectedStatus;
import org.apache.iotdb.commons.stream.StreamTaskStatus;
import org.apache.iotdb.commons.subscription.meta.topic.TopicMeta;
import org.apache.iotdb.commons.utils.StatusUtils;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.confignode.client.sync.CnToSnSyncRequestType;
import org.apache.iotdb.confignode.client.sync.SyncStreamNodeClientPool;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.consensus.request.write.stream.StartStreamPlan;
import org.apache.iotdb.confignode.consensus.request.write.stream.StopStreamPlan;
import org.apache.iotdb.confignode.consensus.request.write.stream.UpdateStreamEpochPlan;
import org.apache.iotdb.confignode.i18n.ManagerMessages;
import org.apache.iotdb.confignode.i18n.ProcedureMessages;
import org.apache.iotdb.confignode.manager.IManager;
import org.apache.iotdb.confignode.manager.load.cache.AbstractHeartbeatSample;
import org.apache.iotdb.confignode.manager.load.cache.IFailureDetector;
import org.apache.iotdb.confignode.manager.load.cache.detector.FixedDetector;
import org.apache.iotdb.confignode.manager.load.cache.detector.PhiAccrualDetector;
import org.apache.iotdb.confignode.manager.load.cache.node.NodeStatistics;
import org.apache.iotdb.confignode.manager.load.subscriber.IClusterStatusSubscriber;
import org.apache.iotdb.confignode.manager.load.subscriber.NodeStatisticsChangeEvent;
import org.apache.iotdb.confignode.persistence.stream.StreamInfo;
import org.apache.iotdb.confignode.rpc.thrift.TAlterTopicReq;
import org.apache.iotdb.confignode.rpc.thrift.TCreateTopicReq;
import org.apache.iotdb.consensus.exception.ConsensusException;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.rpc.subscription.config.TopicConstant;
import org.apache.iotdb.streamnode.rpc.thrift.TDropTaskOnStreamNodeReq;
import org.apache.iotdb.streamnode.rpc.thrift.TStartTaskOnStreamNodeReq;
import org.apache.iotdb.streamnode.rpc.thrift.TStopTaskOnStreamNodeReq;
import org.apache.iotdb.streamnode.rpc.thrift.TStreamNodeHeartbeatReq;
import org.apache.iotdb.streamnode.rpc.thrift.TStreamNodeHeartbeatResp;
import org.apache.iotdb.streamnode.rpc.thrift.TTaskHeartbeat;

import org.apache.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;

@SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
public class StreamManager implements IClusterStatusSubscriber {

  private static final long cnStartTime = System.currentTimeMillis();
  private static final Logger LOGGER = LoggerFactory.getLogger(StreamManager.class);

  // Maximum number of heartbeat samples retained per task
  private static final int MAX_HEARTBEAT_HISTORY = 200;
  private static final int FALLBACK_RPC_RETRY_NUM = 1;
  private static final int STREAM_TASK_RPC_THREAD_NUM = 4;

  private final IManager configManager;
  private final StreamInfo streamInfo;
  private final IFailureDetector failureDetector;
  private final Consumer<String> runningOnMetricRegistrar;
  // taskName -> ordered deque of heartbeat samples (nano timestamps)
  private final Map<String, Deque<AbstractHeartbeatSample>> heartbeatHistory =
      new ConcurrentHashMap<>();
  private final Set<String> streamsBeingDropped = ConcurrentHashMap.newKeySet();

  /**
   * In-flight guard for {@link #migrateRuntimeTasks}: task names currently being migrated off a
   * StreamNode. Blocks concurrent {@link #stopStreamTask} / {@link #markStreamBeingDropped} /
   * {@link #dropStreamTask} / task starts on those tasks for the duration of one migrate call
   * (cleared in {@code finally}, or per-task just before Phase-2 start).
   */
  private final Set<String> streamsBeingMigrated = ConcurrentHashMap.newKeySet();

  private final AtomicInteger nextAssignIndex = new AtomicInteger(0);
  private final ScheduledExecutorService streamMonitorExecutor;
  private final ExecutorService streamTaskRpcExecutor;
  private ScheduledFuture<?> streamMonitorFuture;

  public StreamManager(IManager configManager, StreamInfo streamInfo) {
    this(configManager, streamInfo, runningOn -> {});
  }

  public StreamManager(
      IManager configManager, StreamInfo streamInfo, Consumer<String> runningOnMetricRegistrar) {
    this.configManager = configManager;
    this.streamInfo = streamInfo;
    this.runningOnMetricRegistrar = runningOnMetricRegistrar;
    final long heartbeatIntervalNs =
        ConfigNodeDescriptor.getInstance().getConf().getStreamHeartbeatLostThresholdMS()
            * 1_000_000L;
    final FixedDetector fixedFallback = new FixedDetector(heartbeatIntervalNs * 2);
    this.failureDetector =
        new PhiAccrualDetector(
            /* threshold= */ 10,
            /* acceptableHeartbeatPauseNs= */ heartbeatIntervalNs,
            /* minHeartbeatStdNs= */ (long) (heartbeatIntervalNs * 0.1),
            /* minimalSampleCount= */ IFailureDetector.PHI_COLD_START_THRESHOLD,
            fixedFallback);
    this.streamMonitorExecutor =
        IoTDBThreadPoolFactory.newScheduledThreadPool(1, ThreadName.STREAM_MONITOR.getName());
    this.streamTaskRpcExecutor =
        IoTDBThreadPoolFactory.newFixedThreadPool(
            STREAM_TASK_RPC_THREAD_NUM, ThreadName.STREAM_TASK_RPC.getName());
  }

  public synchronized void startStreamMonitor() {
    if (streamMonitorFuture != null && !streamMonitorFuture.isCancelled()) {
      return;
    }
    streamMonitorFuture =
        ScheduledExecutorUtil.safelyScheduleAtFixedRate(
            streamMonitorExecutor, this::monitorStreams, 0, 5, TimeUnit.SECONDS);
  }

  public synchronized void stopStreamMonitor() {
    if (streamMonitorFuture != null) {
      streamMonitorFuture.cancel(false);
      streamMonitorFuture = null;
    }
  }

  public void triggerReconcile() {
    if (!streamMonitorExecutor.isShutdown()) {
      streamMonitorExecutor.execute(this::monitorStreams);
    }
  }

  @Override
  public void onNodeStatisticsChanged(final NodeStatisticsChangeEvent event) {
    final Set<Integer> registeredStreamNodeIds =
        getRegisteredStreamNodes().stream()
            .map(configuration -> configuration.getLocation().getStreamNodeId())
            .collect(Collectors.toSet());
    final boolean streamNodeBecameRunning =
        event.getDifferentNodeStatisticsMap().entrySet().stream()
            .filter(entry -> registeredStreamNodeIds.contains(entry.getKey()))
            .map(Map.Entry::getValue)
            .anyMatch(StreamManager::becameRunning);
    if (streamNodeBecameRunning) {
      triggerReconcile();
    }
  }

  private static boolean becameRunning(
      final Pair<NodeStatistics, NodeStatistics> statisticsChange) {
    final NodeStatistics previousStatistics = statisticsChange.getLeft();
    final NodeStatistics currentStatistics = statisticsChange.getRight();
    return currentStatistics != null
        && currentStatistics.getStatus() == NodeStatus.Running
        && (previousStatistics == null || previousStatistics.getStatus() != NodeStatus.Running);
  }

  public StreamInfo getStreamInfo() {
    return streamInfo;
  }

  public TSStatus markStreamBeingDropped(final String streamName) {
    final StreamTask task = streamInfo.getTask(streamName);
    if (task == null) {
      return new TSStatus(TSStatusCode.STREAM_NOT_EXIST.getStatusCode())
          .setMessage(String.format(ManagerMessages.STREAM_NOT_FOUND, streamName));
    }
    // Do not hold the StreamInfo write lock while entering the task monitor. START holds the task
    // monitor while issuing topic consensus writes; a concurrent CREATE STREAM plan can otherwise
    // block on the write lock and wedge START's own consensus write behind it.
    synchronized (task) {
      if (streamsBeingMigrated.contains(streamName)) {
        return new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode())
            .setMessage(
                String.format(ManagerMessages.STREAM_IS_BEING_MIGRATED_8E91E470, streamName));
      }
      // The task may have been dropped between the map lookup and this critical section.
      if (streamInfo.getTask(streamName) != task) {
        return new TSStatus(TSStatusCode.STREAM_NOT_EXIST.getStatusCode())
            .setMessage(String.format(ManagerMessages.STREAM_NOT_FOUND, streamName));
      }
      streamsBeingDropped.add(streamName);
      return StatusUtils.OK;
    }
  }

  public void unmarkStreamBeingDropped(final String streamName) {
    streamsBeingDropped.remove(streamName);
  }

  public TSStatus dropStreamTask(String streamName) {
    final StreamTask task = streamInfo.getTask(streamName);
    if (task == null) {
      return new TSStatus(TSStatusCode.STREAM_NOT_EXIST.getStatusCode())
          .setMessage(String.format(ManagerMessages.STREAM_NOT_FOUND, streamName));
    }
    // No write lock here: dropTaskInternal must not run under it.
    return dropTaskInternal(task);
  }

  /**
   * Drop the runtime of RUNNING StreamTasks on the given StreamNode and, in a cluster environment,
   * migrate them to another available StreamNode.
   *
   * <p>{@code stoppedTasksToRestart} is owned by RemoveStreamNodeProcedure and persists across
   * migrate retries: Phase 1 adds each successfully stopped task; on stop failure the set is kept
   * (tasks stay STOPPED) so the caller can retry / restart them after {@code RETRY_THRESHOLD}. Once
   * Phase 1 completes for all tasks the set is cleared — Phase 2 failures are best-effort and must
   * not trigger restart-on-original-node. Tasks that were STOPPED before remove started are never
   * added.
   *
   * <p>Takes a task snapshot under the write lock, then processes each task under the task monitor
   * without holding the StreamInfo locks (see {@link #stopStreamTask} / {@link #startStreamTask}).
   * {@link #streamsBeingMigrated} blocks concurrent stop/drop/start on those tasks. Phase 2 skips
   * restart if the persisted expected status is not {@link StreamTaskExpectedStatus#RUNNING} after
   * the per-task migrate guard is cleared (manual stop or start rollback won the race).
   *
   * @param streamNode the StreamNode whose StreamTasks need to be migrated
   * @param stoppedTasksToRestart in/out set of task names to restart on the original node if Phase
   *     1 migrate retries are exhausted
   * @return SUCCESS_STATUS if all StreamTasks were stopped successfully, the failing status
   *     otherwise
   */
  public TSStatus migrateRuntimeTasks(
      TStreamNodeLocation streamNode, Set<String> stoppedTasksToRestart) {
    final String removedEndpoint = toEndpointString(streamNode.getInternalEndPoint());
    final Set<StreamTask> tasksToStop;
    final Set<StreamTask> tasksToMigrate;

    streamInfo.writeLock();
    try {
      // In single-node environment, checkStreamTaskStatus has already checked there is no
      // StreamTask on the node to be removed at submit time. Double check here in case a task was
      // assigned after the procedure started.
      if (!isClusterEnvironment()) {
        final boolean hasTaskOnNode =
            streamInfo.getAllTasks().stream()
                .map(StreamTask::getRunningOn)
                .anyMatch(removedEndpoint::equals);
        if (hasTaskOnNode) {
          return new TSStatus(TSStatusCode.STREAM_TASK_EXIST_ON_STREAMNODE.getStatusCode())
              .setMessage(
                  String.format(
                      ProcedureMessages
                          .MESSAGE_FAILED_TO_REMOVE_STREAM_NODE_BECAUSE_STREAM_TASK_EXISTS_ON_IT_8582D7D6,
                      streamNode));
        }
        return StatusUtils.OK;
      }

      // Running tasks on the stream node to remove (stable order for retries)
      tasksToStop =
          streamInfo.getAllTasks().stream()
              .filter(
                  task ->
                      removedEndpoint.equals(task.getRunningOn())
                          && task.getStatus() == StreamTaskStatus.RUNNING)
              .collect(Collectors.toSet());

      // tasksToMigrate = tasksToStop ∪ prior stoppedTasksToRestart
      tasksToMigrate = new LinkedHashSet<>(tasksToStop);
      streamInfo.getAllTasks().stream()
          .filter(task -> removedEndpoint.equals(task.getRunningOn()))
          .filter(task -> task.getStatus() == StreamTaskStatus.STOPPED)
          .filter(
              task ->
                  streamInfo.getExpectedStatus(task.getTaskName())
                      != StreamTaskExpectedStatus.RUNNING)
          .forEach(tasksToMigrate::add);
      for (String taskName : stoppedTasksToRestart) {
        final StreamTask task = streamInfo.getTask(taskName);
        if (task != null && removedEndpoint.equals(task.getRunningOn())) {
          tasksToMigrate.add(task);
        }
      }

      // set streamsBeingMigrated
      for (StreamTask task : tasksToMigrate) {
        streamsBeingMigrated.add(task.getTaskName());
      }
    } finally {
      streamInfo.writeUnlock();
    }

    try {
      // Phase 1: stop RUNNING tasks
      for (StreamTask task : tasksToStop) {
        // No write lock here: stopTaskInternal must not run under it.
        final TSStatus epochStatus = advanceStreamEpoch(task, task.getEpoch());
        if (epochStatus.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
          return epochStatus;
        }
        final TSStatus stopStatus =
            stopTaskInternal(
                task, task.getEpoch(), ManagerMessages.MESSAGE_STREAMNODE_SHUTDOWN_C83EA302);
        if (stopStatus.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
          LOGGER.error(
              ManagerMessages.LOG_FAILED_TO_STOP_RUNTIME_OF_STREAMTASK_ON_STREAMNODE_B5E07753,
              task.getTaskName(),
              streamNode);
          return stopStatus;
        }
        stoppedTasksToRestart.add(task.getTaskName());
      }

      // Phase 1 succeeded for all tasks: no restart-on-original-node is needed anymore.
      stoppedTasksToRestart.clear();

      // Phase 2: drop and migrate tasks
      for (StreamTask task : tasksToMigrate) {
        synchronized (task) {
          if (!tasksToStop.contains(task)
              && task.getStatus() == StreamTaskStatus.STOPPED
              && streamInfo.getExpectedStatus(task.getTaskName())
                  != StreamTaskExpectedStatus.RUNNING) {
            task.setRunningOn("");
            heartbeatHistory.remove(task.getTaskName());
            streamsBeingMigrated.remove(task.getTaskName());
            continue;
          }
        }

        // A DROP_TASK failure is only logged and does not abort migration.
        // Stopping the StreamNode later will attempt cleanup again.
        final TSStatus dropStatus = dropTaskOnNodeInternal(task);
        if (dropStatus.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
          LOGGER.error(
              ManagerMessages
                  .LOG_FAILED_TO_DROP_RUNTIME_OF_STREAMTASK_ARG_ON_STREAMNODE_ARG_E5D5C253,
              task.getTaskName(),
              streamNode);
        }

        // Allow the task to start; other tasks remain blocked until
        // finally. If the user stops the task after the guard is cleared, skip restart so a manual
        // STOP is not overwritten by migrate. Check and start share the same synchronized(task)
        // (reentrant) so the STOPPED observation cannot race with startTaskInternal.
        streamsBeingMigrated.remove(task.getTaskName());

        // No read lock here: startTaskInternal must not run under it.
        final TSStatus startStatus;
        synchronized (task) {
          if (streamInfo.getExpectedStatus(task.getTaskName())
              == StreamTaskExpectedStatus.STOPPED) {
            task.setStatus(StreamTaskStatus.STOPPED);
            task.setRunningOn("");
            heartbeatHistory.remove(task.getTaskName());
            continue;
          }
          task.setStatus(StreamTaskStatus.UNKNOWN);
          heartbeatHistory.remove(task.getTaskName());
          // A START_TASK failure is only logged and does not abort migration. The task stays
          // UNKNOWN and the ConfigNode periodical heartbeat/monitor will retry
          // startTaskInternal later.
          startStatus = startTaskInternal(task, assignStreamToStreamNode());
        }
        if (startStatus.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
          LOGGER.warn(
              ManagerMessages
                  .LOG_FAILED_TO_MIGRATE_STREAMTASK_ARG_TO_ANOTHER_STREAMNODE_REASON_ARG_0307D370,
              task.getTaskName(),
              startStatus.getMessage());
        }
      }
      return StatusUtils.OK;
    } finally {
      for (StreamTask task : tasksToMigrate) {
        streamsBeingMigrated.remove(task.getTaskName());
      }
    }
  }

  /**
   * Best-effort restart of tasks in {@code stoppedTasksToRestart} on their current {@code
   * runningOn}. Used by RemoveStreamNodeProcedure after Phase 1 migrate retries are exhausted.
   * Names already cleared after a successful Phase 1 are not restarted.
   */
  public void restartStoppedTasks(Set<String> stoppedTasksToRestart) {
    if (stoppedTasksToRestart == null || stoppedTasksToRestart.isEmpty()) {
      return;
    }
    for (String taskName : stoppedTasksToRestart) {
      final StreamTask task = streamInfo.getTask(taskName);
      if (task == null
          || task.getStatus() != StreamTaskStatus.STOPPED
          || streamInfo.getExpectedStatus(taskName) != StreamTaskExpectedStatus.RUNNING) {
        continue;
      }
      // No read lock here: startTaskInternal must not run under it. A task dropped in the meantime
      // is rejected by the map re-check inside startTaskInternal.
      final String runningOn = task.getRunningOn();
      final TSStatus restartStatus = startTaskInternal(task, runningOn);
      if (restartStatus.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        LOGGER.warn(
            ManagerMessages.LOG_FAILED_TO_RESTART_STREAMTASK_ON_STREAMNODE_DURING_ROLLBACK_D4EADD44,
            taskName,
            runningOn);
      }
    }
  }

  private TSStatus dropTaskOnNodeInternal(StreamTask task) {
    synchronized (task) {
      final String streamNode = task.getRunningOn();
      if (streamNode == null || streamNode.isEmpty()) {
        return StatusUtils.OK;
      }
      final TEndPoint endPoint;
      try {
        endPoint = parseEndpoint(streamNode);
      } catch (RuntimeException e) {
        return new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode())
            .setMessage(
                String.format(
                    ManagerMessages.STREAM_MANAGER_INVALID_STREAMNODE_ENDPOINT,
                    task.getTaskName(),
                    streamNode));
      }
      return sendDropTask(endPoint, new TDropTaskOnStreamNodeReq(task.getTaskName()));
    }
  }

  public boolean isClusterEnvironment() {
    return configManager.getNodeManager().getRegisteredStreamNodes().size() > 1;
  }

  public boolean isSubscriptionSource(final String streamName) {
    streamInfo.readLock();
    try {
      final StreamTask task = streamInfo.getTask(streamName);
      return task != null && task.getSource() instanceof IoTDBSubscriptionSource;
    } finally {
      streamInfo.readUnlock();
    }
  }

  // Must NOT be called while holding the StreamInfo write lock: the StreamNode
  // RPCs inside would stall the apply of concurrent CREATE STREAM plans for the RPC duration.
  private TSStatus dropTaskInternal(StreamTask task) {
    final String streamName = task.getTaskName();
    synchronized (task) {
      // The task may have been dropped between the caller's map lookup and this critical section.
      if (streamInfo.getTask(streamName) != task) {
        return new TSStatus(TSStatusCode.STREAM_NOT_EXIST.getStatusCode())
            .setMessage(String.format(ManagerMessages.STREAM_NOT_FOUND, streamName));
      }
      final String streamNode = task.getRunningOn();
      if (streamNode != null && !streamNode.isEmpty()) {
        final TEndPoint endPoint;
        try {
          endPoint = parseEndpoint(streamNode);
        } catch (RuntimeException e) {
          return new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode())
              .setMessage(
                  String.format(
                      ManagerMessages.STREAM_MANAGER_INVALID_STREAMNODE_ENDPOINT,
                      streamName,
                      streamNode));
        }
        final NodeStatus streamNodeStatus = getStreamNodeStatus(endPoint);
        if (streamNodeStatus != NodeStatus.Running) {
          return new TSStatus(TSStatusCode.STREAM_NODE_STOP_ERROR.getStatusCode())
              .setMessage(
                  String.format(
                      ManagerMessages
                          .MESSAGE_CANNOT_DROP_STREAM_ARG_BECAUSE_ITS_STREAMNODE_ARG_IS_ARG_CB6283F1,
                      streamName,
                      streamNode,
                      streamNodeStatus.getStatus()));
        }

        if (task.getStatus() == StreamTaskStatus.RUNNING) {
          final TSStatus epochStatus = advanceStreamEpoch(task, task.getEpoch());
          if (epochStatus.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
            return epochStatus;
          }
          final int stopEpoch = task.getEpoch();
          final TSStatus stopStatus =
              sendStopTask(
                  endPoint, new TStopTaskOnStreamNodeReq(streamName, stopEpoch, cnStartTime));
          if (stopStatus.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
            return stopStatus;
          }
        }

        final TSStatus dropStatus =
            sendDropTask(endPoint, new TDropTaskOnStreamNodeReq(streamName));
        if (dropStatus.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
          return dropStatus;
        }
      }

      heartbeatHistory.remove(streamName);
      return StatusUtils.OK;
    }
  }

  private NodeStatus getStreamNodeStatus(final TEndPoint endPoint) {
    return configManager.getNodeManager().getRegisteredStreamNodes().stream()
        .filter(streamNode -> endPoint.equals(streamNode.getLocation().getInternalEndPoint()))
        .findFirst()
        .map(
            streamNode ->
                configManager
                    .getLoadManager()
                    .getNodeStatus(streamNode.getLocation().getStreamNodeId()))
        .orElse(NodeStatus.Unknown);
  }

  public TSStatus startStreamTask(final String streamName) {
    LOGGER.info(ManagerMessages.STREAM_MANAGER_STARTING_STREAM, streamName);
    // Fetch the task without keeping the StreamInfo read lock: startTaskInternal issues
    // consensus writes (topic create/alter) and StreamNode RPCs, and holding the read lock
    // across them deadlocks a concurrent CREATE STREAM - its plan applies on a thread that
    // blocks on the StreamInfo write lock while this read lock is still held, wedging every
    // later write behind that apply. Concurrent start/drop/migrate on the same task are
    // serialized by the task monitor inside startTaskInternal instead.
    final StreamTask task = streamInfo.getTask(streamName);
    if (task == null) {
      return new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode())
          .setMessage(String.format(ManagerMessages.STREAM_NOT_FOUND, streamName));
    }
    synchronized (task) {
      if (streamsBeingDropped.contains(streamName)) {
        return new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode())
            .setMessage(String.format(ManagerMessages.STREAM_IS_BEING_DROPPED, streamName));
      }
      if (streamsBeingMigrated.contains(streamName)) {
        return new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode())
            .setMessage(
                String.format(ManagerMessages.STREAM_IS_BEING_MIGRATED_8E91E470, streamName));
      }
      return startTaskInternal(task);
    }
  }

  // Must NOT be called while holding the StreamInfo read lock.
  private TSStatus startTaskInternal(StreamTask task) {
    synchronized (task) {
      if (task.getStatus() != StreamTaskStatus.RUNNING && discoverTaskRuntime(task.getTaskName())) {
        task.setStatus(StreamTaskStatus.RUNNING);
        return StatusUtils.OK;
      }
    }
    return startTaskInternal(task, assignStreamToStreamNode());
  }

  // Must NOT be called while holding the StreamInfo read lock.
  private TSStatus startTaskInternal(StreamTask task, String streamNode) {
    synchronized (task) {
      // The task may have been dropped between the caller's map lookup and this critical
      // section; the map check under the task monitor closes that race.
      if (streamInfo.getTask(task.getTaskName()) != task) {
        return new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode())
            .setMessage(String.format(ManagerMessages.STREAM_NOT_FOUND, task.getTaskName()));
      }
      if (task.getStatus() == StreamTaskStatus.RUNNING) {
        return StatusUtils.OK;
      }
      if (streamNode == null) {
        return new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode())
            .setMessage(
                String.format(
                    ManagerMessages.STREAM_MANAGER_NO_AVAILABLE_STREAMNODE, task.getTaskName()));
      }
      // Parse runningOn to TEndPoint
      TEndPoint endPoint = parseEndpoint(streamNode);
      task.setLeaderTerm(configManager.getConsensusManager().getLeaderTerm());
      task.setCnStartTime(cnStartTime);
      final TSStatus topicStatus = prepareSubscriptionTopic(task, streamNode);
      if (topicStatus.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return topicStatus;
      }
      // Create request
      TStartTaskOnStreamNodeReq req =
          new TStartTaskOnStreamNodeReq(task.toByteBuffer(), task.getEpoch(), cnStartTime);
      // Send request to StreamNode
      TSStatus status = sendStartTask(endPoint, req);
      if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return status;
      }
      // Update status
      task.setRunningOn(streamNode);
      runningOnMetricRegistrar.accept(streamNode);
      task.setStatus(StreamTaskStatus.RUNNING);
      task.setLastUpTime(System.currentTimeMillis());
      task.setLastHeartbeatTime(System.currentTimeMillis());
      // Seed the heartbeat history with the start timestamp so the detector has an anchor point
      heartbeatHistory
          .computeIfAbsent(task.getTaskName(), k -> new ArrayDeque<>())
          .add(new StreamHeartbeatSample(task.getEpoch(), task.getLeaderTerm()));
      return StatusUtils.OK;
    }
  }

  /**
   * Record a heartbeat for the given task. Should be called whenever a heartbeat reply is received
   * from the StreamNode.
   */
  public TSStatus recordHeartbeat(
      String taskName, int epoch, long cnStartTime, long leaderTerm, String runningOn) {
    final StreamTask task = streamInfo.getTask(taskName);
    if (task == null) {
      LOGGER.warn(ManagerMessages.STREAM_MANAGER_RECEIVED_UNKNOWN_TASK_HEARTBEAT, taskName);
      return new TSStatus(TSStatusCode.STREAM_NOT_EXIST.getStatusCode())
          .setMessage(String.format(ManagerMessages.STREAM_MANAGER_TASK_NOT_FOUND, taskName));
    }
    synchronized (task) {
      if (streamInfo.getTask(taskName) != task) {
        return new TSStatus(TSStatusCode.STREAM_NOT_EXIST.getStatusCode())
            .setMessage(String.format(ManagerMessages.STREAM_MANAGER_TASK_NOT_FOUND, taskName));
      }
      if (task.getLeaderTerm() != leaderTerm || task.getEpoch() > epoch) {
        return reportStaleHeartbeat(taskName, epoch, leaderTerm, task);
      }
      if (epoch > task.getEpoch()) {
        final TSStatus epochStatus = persistStreamEpoch(task, epoch);
        if (epochStatus.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
          return epochStatus;
        }
      }
      task.setLastHeartbeatTime(System.currentTimeMillis());
      task.setRunningOn(runningOn);
      runningOnMetricRegistrar.accept(runningOn);
      if (!configManager.getProcedureManager().getInProgressStreamNames().contains(taskName)) {
        if (streamInfo.getExpectedStatus(taskName) == StreamTaskExpectedStatus.RUNNING) {
          task.setStatus(StreamTaskStatus.RUNNING);
        } else if (streamInfo.getExpectedStatus(taskName) == StreamTaskExpectedStatus.STOPPED) {
          task.setStatus(StreamTaskStatus.UNKNOWN);
        }
      }
    }

    final Deque<AbstractHeartbeatSample> history =
        heartbeatHistory.computeIfAbsent(taskName, k -> new ArrayDeque<>());
    history.removeIf(
        s -> {
          final StreamHeartbeatSample sample = (StreamHeartbeatSample) s;
          return sample.getEpoch() != epoch || sample.getLeaderTerm() != leaderTerm;
        });
    history.addLast(new StreamHeartbeatSample(epoch, leaderTerm));
    while (history.size() > MAX_HEARTBEAT_HISTORY) {
      history.pollFirst();
    }
    return StatusUtils.OK;
  }

  private TSStatus reportStaleHeartbeat(
      String taskName, int epoch, long leaderTerm, StreamTask task) {
    final String message =
        String.format(
            ManagerMessages.STREAM_MANAGER_STALE_HEARTBEAT,
            taskName,
            task.getEpoch(),
            task.getLeaderTerm(),
            epoch,
            leaderTerm);
    LOGGER.warn(message);
    return new TSStatus(TSStatusCode.STREAM_STALE.getStatusCode()).setMessage(message);
  }

  private String assignStreamToStreamNode() {
    // Only Running StreamNodes are eligible for new task assignment
    final Set<Integer> removingStreamNodeIds =
        configManager.getProcedureManager().getRemovingStreamNodeIds();
    final List<TStreamNodeConfiguration> streamNodes =
        configManager.getNodeManager().filterStreamNodeThroughStatus(NodeStatus.Running).stream()
            .filter(
                streamNode ->
                    !removingStreamNodeIds.contains(streamNode.getLocation().getStreamNodeId()))
            .toList();
    if (streamNodes.isEmpty()) {
      return null;
    }
    final int index = Math.floorMod(nextAssignIndex.getAndIncrement(), streamNodes.size());
    return toEndpointString(streamNodes.get(index).getLocation().getInternalEndPoint());
  }

  public boolean hasAvailableStreamNode() {
    return configManager.getNodeManager().filterStreamNodeThroughStatus(NodeStatus.Running).stream()
        .anyMatch(
            streamNode ->
                !configManager
                    .getProcedureManager()
                    .getRemovingStreamNodeIds()
                    .contains(streamNode.getLocation().getStreamNodeId()));
  }

  /**
   * Persists the next task epoch, then creates and assigns the source topic after scheduling has
   * selected its StreamNode.
   */
  protected TSStatus prepareSubscriptionTopic(final StreamTask task, final String ownerId) {
    if (!(task.getSource() instanceof IoTDBSubscriptionSource source)) {
      return advanceStreamEpoch(task, task.getEpoch());
    }

    final String topicName = IoTDBSubscriptionSource.getTopicName(task.getTaskName());
    final Map<String, String> topicAttributes =
        toStringMap(source.getTopicProperties(task.getProperties()));
    TSStatus status =
        configManager.createTopic(
            new TCreateTopicReq()
                .setTopicName(topicName)
                .setTopicAttributes(topicAttributes)
                .setIfNotExistsCondition(true));
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return status;
    }

    final TopicMeta topicMeta =
        configManager
            .getSubscriptionManager()
            .getSubscriptionCoordinator()
            .getSubscriptionInfo()
            .deepCopyTopicMeta(topicName, true);
    if (topicMeta == null) {
      return new TSStatus(TSStatusCode.TOPIC_NOT_EXIST_ERROR.getStatusCode())
          .setMessage(
              String.format(
                  ManagerMessages.FAILED_TO_ALTER_TOPIC_THE_TOPIC_IS_NOT_EXISTED, topicName));
    }
    // Subscription metadata survives CN restart/failover and retains epochs even after the owner
    // is cleared. Keep the task epoch in sync because it supplies the consumer's owner epoch.
    final TSStatus epochStatus = advanceStreamEpoch(task, topicMeta.getMaxOwnerEpoch());
    if (epochStatus.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return epochStatus;
    }
    final int ownerEpoch = task.getEpoch();
    final Map<String, String> ownerAttributes = new HashMap<>();
    ownerAttributes.put(SystemConstant.SQL_DIALECT_KEY, SystemConstant.SQL_DIALECT_TABLE_VALUE);
    ownerAttributes.put(TopicConstant.OWNER_ID_KEY, ownerId);
    ownerAttributes.put(TopicConstant.OWNER_EPOCH_KEY, String.valueOf(ownerEpoch));
    status =
        configManager.alterTopic(
            new TAlterTopicReq()
                .setTopicName(topicName)
                .setTopicAttributes(ownerAttributes)
                .setSubscribedConsumerGroupIds(Collections.emptySet()));
    return status;
  }

  private static Map<String, String> toStringMap(final Properties properties) {
    final Map<String, String> attributes = new HashMap<>();
    for (final String name : properties.stringPropertyNames()) {
      attributes.put(name, properties.getProperty(name));
    }
    return attributes;
  }

  public TSStatus stopStreamTask(final String streamName, final int targetEpoch) {
    LOGGER.info(ManagerMessages.STREAM_MANAGER_STOPPING_STREAM, streamName);
    final StreamTask task = streamInfo.getTask(streamName);
    if (task == null) {
      return new TSStatus(TSStatusCode.STREAM_NOT_EXIST.getStatusCode())
          .setMessage(String.format(ManagerMessages.STREAM_NOT_FOUND, streamName));
    }
    // No StreamInfo write lock across the StreamNode RPC below. Concurrent stop/drop/migrate on
    // the same task are serialized by the task monitor.
    synchronized (task) {
      if (streamsBeingDropped.contains(streamName)) {
        return new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode())
            .setMessage(String.format(ManagerMessages.STREAM_IS_BEING_DROPPED, streamName));
      }
      if (streamsBeingMigrated.contains(streamName)) {
        return new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode())
            .setMessage(
                String.format(ManagerMessages.STREAM_IS_BEING_MIGRATED_8E91E470, streamName));
      }
      // The task may have been dropped between the map lookup and this critical section.
      if (streamInfo.getTask(streamName) != task) {
        return new TSStatus(TSStatusCode.STREAM_NOT_EXIST.getStatusCode())
            .setMessage(String.format(ManagerMessages.STREAM_NOT_FOUND, streamName));
      }
      final TSStatus epochStatus = persistStreamEpoch(task, targetEpoch);
      if (epochStatus.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return epochStatus;
      }
      return stopTaskInternal(
          task, task.getEpoch(), ManagerMessages.STREAM_MANAGER_MANUALLY_STOPPED);
    }
  }

  public TSStatus rollbackStartStreamTask(final StreamTask task, final int rollbackEpoch) {
    // Procedure compensation bypasses user-operation guards by design.
    final TSStatus epochStatus = persistStreamEpoch(task, rollbackEpoch);
    final TSStatus stopStatus =
        stopTaskInternal(task, task.getEpoch(), ManagerMessages.MESSAGE_START_ROLLBACK_B6601816);
    return stopStatus.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()
        ? epochStatus
        : stopStatus;
  }

  public TSStatus rollbackStopStreamTask(final StreamTask task) {
    // Procedure compensation bypasses user-operation guards by design.
    return startTaskInternal(task);
  }

  /**
   * Stop the given StreamTask: notify its StreamNode to stop the runtime, then update the task
   * status to {@link StreamTaskStatus#STOPPED}. A STOP that has already reached the runtime is
   * idempotent.
   *
   * <p>Must NOT be called while holding the {@link #streamInfo} write lock: the StreamNode RPC
   * inside would stall the apply of concurrent CREATE STREAM plans.
   *
   * @param task the StreamTask to stop
   * @param targetEpoch the fencing epoch carried by the stop request
   * @param reason the reason recorded for the stop transition
   * @return SUCCESS_STATUS if the task was stopped, the failing status otherwise
   */
  private TSStatus stopTaskInternal(
      final StreamTask task, final int targetEpoch, final String reason) {
    synchronized (task) {
      if (task.getStatus() == StreamTaskStatus.STOPPED) {
        return StatusUtils.OK;
      }
      task.setCnStartTime(cnStartTime);
      final String streamNode = task.getRunningOn();
      int effectiveEpoch = targetEpoch;
      TSStatus status;
      if (streamNode != null && !streamNode.isEmpty()) {
        // Parse runningOn to TEndPoint
        final TEndPoint endPoint = parseEndpoint(streamNode);
        // Create request
        final TStopTaskOnStreamNodeReq req =
            new TStopTaskOnStreamNodeReq(task.getTaskName(), targetEpoch, task.getCnStartTime());
        // Send request to StreamNode
        status = sendStopTask(endPoint, req);
        if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
          return completeStopTask(task, reason);
        }
      }

      boolean discovered = discoverTaskRuntime(task.getTaskName());
      if (discovered) {
        final TSStatus epochStatus =
            advanceStreamEpoch(task, Math.max(targetEpoch, task.getEpoch()));
        if (epochStatus.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
          return epochStatus;
        }
        effectiveEpoch = task.getEpoch();
      }
      status = broadcastStopTask(task, effectiveEpoch);
      if (status.getCode() == TSStatusCode.STREAM_STALE.getStatusCode()) {
        discovered = discoverTaskRuntime(task.getTaskName());
        if (discovered) {
          final TSStatus epochStatus =
              advanceStreamEpoch(task, Math.max(effectiveEpoch, task.getEpoch()));
          if (epochStatus.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
            return epochStatus;
          }
          effectiveEpoch = task.getEpoch();
        }
        status = broadcastStopTask(task, effectiveEpoch);
      }
      return status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()
          ? completeStopTask(task, reason)
          : status;
    }
  }

  private TSStatus broadcastStopTask(final StreamTask task, final int epoch) {
    final List<TStreamNodeConfiguration> streamNodes = getRunningStreamNodes();
    if (streamNodes.isEmpty()) {
      return StatusUtils.OK;
    }
    final List<CompletableFuture<TSStatus>> stopFutures = new ArrayList<>(streamNodes.size());
    for (final TStreamNodeConfiguration streamNode : streamNodes) {
      stopFutures.add(
          CompletableFuture.supplyAsync(
              () ->
                  sendStopTask(
                      streamNode.getLocation().getInternalEndPoint(),
                      new TStopTaskOnStreamNodeReq(
                          task.getTaskName(), epoch, task.getCnStartTime()),
                      FALLBACK_RPC_RETRY_NUM),
              streamTaskRpcExecutor));
    }

    TSStatus failure = null;
    for (int i = 0; i < streamNodes.size(); i++) {
      final TStreamNodeConfiguration streamNode = streamNodes.get(i);
      final TSStatus status = stopFutures.get(i).join();
      if (status.getCode() == TSStatusCode.STREAM_STALE.getStatusCode()) {
        failure = status;
      } else if (status.getCode() == TSStatusCode.INTERNAL_REQUEST_RETRY_ERROR.getStatusCode()) {
        LOGGER.warn(
            ManagerMessages.LOG_FAILED_TO_STOP_RUNTIME_OF_STREAMTASK_ON_STREAMNODE_B5E07753,
            task.getTaskName(),
            streamNode.getLocation().getInternalEndPoint());
      } else if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()
          && failure == null) {
        failure = status;
      }
    }
    return failure == null ? StatusUtils.OK : failure;
  }

  private TSStatus completeStopTask(final StreamTask task, final String reason) {
    task.setStatus(StreamTaskStatus.STOPPED);
    task.setLastDownTime(System.currentTimeMillis());
    task.setLastDownReason(reason);
    heartbeatHistory.remove(task.getTaskName());
    return StatusUtils.OK;
  }

  public TSStatus persistStartStream(final StreamTask task, final int epoch) {
    try {
      return configManager
          .getConsensusManager()
          .write(new StartStreamPlan(task.getTaskName(), epoch));
    } catch (final ConsensusException e) {
      return new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode())
          .setMessage(
              ManagerMessages.FAILED_IN_THE_READ_WRITE_API_EXECUTING_THE_CONSENSUS_LAYER
                  + e.getMessage());
    }
  }

  public TSStatus persistStopStream(final StreamTask task, final int epoch) {
    try {
      return configManager
          .getConsensusManager()
          .write(new StopStreamPlan(task.getTaskName(), epoch));
    } catch (final ConsensusException e) {
      return new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode())
          .setMessage(
              ManagerMessages.FAILED_IN_THE_READ_WRITE_API_EXECUTING_THE_CONSENSUS_LAYER
                  + e.getMessage());
    }
  }

  private TSStatus advanceStreamEpoch(final StreamTask task, final long observedEpoch) {
    final long baseEpoch = Math.max(task.getEpoch(), observedEpoch);
    if (baseEpoch >= Integer.MAX_VALUE) {
      return epochOverflowStatus(task);
    }
    return persistStreamEpoch(task, StreamTask.incrementEpochChecked((int) baseEpoch));
  }

  private TSStatus persistStreamEpoch(final StreamTask task, final int epoch) {
    try {
      return configManager
          .getConsensusManager()
          .write(new UpdateStreamEpochPlan(task.getTaskName(), epoch));
    } catch (final ConsensusException e) {
      return new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode())
          .setMessage(
              ManagerMessages.FAILED_IN_THE_READ_WRITE_API_EXECUTING_THE_CONSENSUS_LAYER
                  + e.getMessage());
    }
  }

  private static TSStatus epochOverflowStatus(final StreamTask task) {
    return new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode())
        .setMessage(
            String.format(
                ManagerMessages
                    .MESSAGE_CANNOT_ADVANCE_STREAM_ARG_TASK_EPOCH_HAS_REACHED_THE_MAXIMUM_SUPPORTED_VALUE_ED9FA887,
                task.getTaskName()));
  }

  public List<StreamTask> showStreams() {
    streamInfo.readLock();
    try {
      return streamInfo.getAllTasks();
    } finally {
      streamInfo.readUnlock();
    }
  }

  private void monitorStreams() {
    if (configManager.getConsensusManager() == null
        || !configManager.getConsensusManager().isLeaderReady()) {
      return;
    }
    collectStreamNodeHeartbeats();

    final Set<String> inProgressStreamNames =
        configManager.getProcedureManager().getInProgressStreamNames();
    List<StreamTask> toUpdate = new ArrayList<>();
    streamInfo.readLock();
    try {
      for (StreamTask task : streamInfo.getAllTasks()) {
        if (inProgressStreamNames.contains(task.getTaskName())
            || streamInfo.getExpectedStatus(task.getTaskName()) != StreamTaskExpectedStatus.RUNNING
            || task.getStatus() != StreamTaskStatus.RUNNING) {
          continue;
        }
        Deque<AbstractHeartbeatSample> samples = heartbeatHistory.get(task.getTaskName());
        final List<AbstractHeartbeatSample> history =
            samples != null
                ? Collections.unmodifiableList(new ArrayList<>(samples))
                : Collections.emptyList();
        if (!failureDetector.isAvailable(task.getTaskName(), history)) {
          toUpdate.add(task);
        }
      }
    } finally {
      streamInfo.readUnlock();
    }

    if (!toUpdate.isEmpty()) {
      streamInfo.writeLock();
      try {
        for (StreamTask task : toUpdate) {
          if (!configManager
                  .getProcedureManager()
                  .getInProgressStreamNames()
                  .contains(task.getTaskName())
              && task.getStatus() == StreamTaskStatus.RUNNING) { // Double check
            task.setStatus(StreamTaskStatus.UNKNOWN);
            task.setLastDownTime(System.currentTimeMillis());
            task.setLastDownReason(ManagerMessages.STREAM_MANAGER_HEARTBEAT_LOST);
            LOGGER.info(ManagerMessages.STREAM_MANAGER_MARKED_TASK_UNKNOWN, task.getTaskName());
          }
        }
      } finally {
        streamInfo.writeUnlock();
      }
    }

    // Reconcile tasks whose runtime has not reached the persisted target state.
    List<StreamTask> tasksToStart = new ArrayList<>();
    List<StreamTask> tasksToStop = new ArrayList<>();
    streamInfo.readLock();
    try {
      for (StreamTask task : streamInfo.getAllTasks()) {
        if (inProgressStreamNames.contains(task.getTaskName())) {
          continue;
        }
        final StreamTaskExpectedStatus expectedStatus =
            streamInfo.getExpectedStatus(task.getTaskName());
        if (expectedStatus == StreamTaskExpectedStatus.RUNNING
            && task.getStatus() != StreamTaskStatus.RUNNING) {
          tasksToStart.add(task);
        } else if (expectedStatus == StreamTaskExpectedStatus.STOPPED
            && task.getStatus() != StreamTaskStatus.STOPPED) {
          tasksToStop.add(task);
        }
      }
    } finally {
      streamInfo.readUnlock();
    }

    for (StreamTask task : tasksToStart) {
      LOGGER.info(ManagerMessages.STREAM_MANAGER_RESTARTING_UNKNOWN_TASK, task.getTaskName());
      synchronized (task) {
        if (!configManager
                .getProcedureManager()
                .getInProgressStreamNames()
                .contains(task.getTaskName())
            && streamInfo.getExpectedStatus(task.getTaskName()) == StreamTaskExpectedStatus.RUNNING
            && task.getStatus() != StreamTaskStatus.RUNNING) {
          if (streamsBeingDropped.contains(task.getTaskName())
              || streamsBeingMigrated.contains(task.getTaskName())) {
            continue;
          }
          // The heartbeat collection at the beginning of this monitor pass has already discovered
          // existing runtimes. Avoid another synchronous heartbeat round trip for every task.
          TSStatus status = startTaskInternal(task, assignStreamToStreamNode());
          if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
            LOGGER.warn(
                ManagerMessages.STREAM_MANAGER_FAILED_TO_RESTART_TASK,
                task.getTaskName(),
                status.getMessage());
          } else {
            // Clear stale heartbeat history so the detector starts fresh after restart
            heartbeatHistory.remove(task.getTaskName());
          }
        }
      }
    }

    for (StreamTask task : tasksToStop) {
      synchronized (task) {
        if (!configManager
                .getProcedureManager()
                .getInProgressStreamNames()
                .contains(task.getTaskName())
            && streamInfo.getExpectedStatus(task.getTaskName()) == StreamTaskExpectedStatus.STOPPED
            && task.getStatus() != StreamTaskStatus.STOPPED) {
          if (streamsBeingDropped.contains(task.getTaskName())
              || streamsBeingMigrated.contains(task.getTaskName())) {
            continue;
          }
          final TSStatus status =
              stopTaskInternal(
                  task, task.getEpoch(), ManagerMessages.MESSAGE_STATE_RECONCILIATION_4D36E98B);
          if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
            LOGGER.warn(
                ManagerMessages.LOG_FAILED_TO_RECONCILE_STOPPED_STREAM_ARG_REASON_ARG_4324F28F,
                task.getTaskName(),
                status.getMessage());
          }
        }
      }
    }
  }

  @TestOnly
  void runStreamMonitorOnceForTest() {
    monitorStreams();
  }

  private void collectStreamNodeHeartbeats() {
    for (final TStreamNodeConfiguration configuration : getRegisteredStreamNodes()) {
      final TEndPoint endPoint = configuration.getLocation().getInternalEndPoint();
      final TStreamNodeHeartbeatResp resp =
          sendHeartbeat(
              endPoint, new TStreamNodeHeartbeatReq(System.nanoTime(), cnStartTime, false));
      if (resp == null || resp.getRunningTasks() == null) {
        continue;
      }
      for (final TTaskHeartbeat heartbeat : resp.getRunningTasks()) {
        recordHeartbeatFromStreamNode(heartbeat, endPoint);
      }
    }
  }

  private boolean discoverTaskRuntime(final String taskName) {
    boolean discovered = false;
    for (final TStreamNodeConfiguration configuration : getRunningStreamNodes()) {
      final TEndPoint endPoint = configuration.getLocation().getInternalEndPoint();
      final TStreamNodeHeartbeatResp resp =
          sendHeartbeat(
              endPoint,
              new TStreamNodeHeartbeatReq(System.nanoTime(), cnStartTime, false),
              FALLBACK_RPC_RETRY_NUM);
      if (resp == null || resp.getRunningTasks() == null) {
        continue;
      }
      for (final TTaskHeartbeat heartbeat : resp.getRunningTasks()) {
        if (taskName.equals(heartbeat.getTaskName())) {
          final TSStatus status = recordHeartbeatFromStreamNode(heartbeat, endPoint);
          discovered |= status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode();
        }
      }
    }
    return discovered;
  }

  private TSStatus recordHeartbeatFromStreamNode(
      final TTaskHeartbeat heartbeat, final TEndPoint endPoint) {
    final StreamTask task = streamInfo.getTask(heartbeat.getTaskName());
    if (task == null) {
      LOGGER.warn(
          ManagerMessages.STREAM_MANAGER_RECEIVED_UNKNOWN_TASK_HEARTBEAT, heartbeat.getTaskName());
      return new TSStatus(TSStatusCode.STREAM_NOT_EXIST.getStatusCode())
          .setMessage(
              String.format(
                  ManagerMessages.STREAM_MANAGER_TASK_NOT_FOUND, heartbeat.getTaskName()));
    }
    final TSStatus status =
        recordHeartbeat(
            heartbeat.getTaskName(),
            heartbeat.getEpoch(),
            cnStartTime,
            task.getLeaderTerm(),
            toEndpointString(endPoint));
    if (status.getCode() == TSStatusCode.STREAM_STALE.getStatusCode()) {
      stopStaleRuntime(heartbeat, task, endPoint);
    }
    return status;
  }

  private void stopStaleRuntime(
      final TTaskHeartbeat heartbeat, final StreamTask task, final TEndPoint endPoint) {
    synchronized (task) {
      if (streamInfo.getTask(heartbeat.getTaskName()) != task
          || heartbeat.getEpoch() >= task.getEpoch()) {
        return;
      }
      final TSStatus stopStatus =
          sendStopTask(
              endPoint,
              new TStopTaskOnStreamNodeReq(task.getTaskName(), heartbeat.getEpoch(), cnStartTime));
      if (stopStatus.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        LOGGER.warn(
            ManagerMessages.LOG_FAILED_TO_STOP_RUNTIME_OF_STREAMTASK_ON_STREAMNODE_B5E07753,
            task.getTaskName(),
            endPoint);
      }
    }
  }

  private List<TStreamNodeConfiguration> getRegisteredStreamNodes() {
    final List<TStreamNodeConfiguration> streamNodes =
        configManager.getNodeManager().getRegisteredStreamNodes();
    if (streamNodes == null || streamNodes.isEmpty()) {
      return Collections.emptyList();
    }
    return streamNodes.stream()
        .sorted(Comparator.comparingInt(node -> node.getLocation().getStreamNodeId()))
        .collect(Collectors.toList());
  }

  private List<TStreamNodeConfiguration> getRunningStreamNodes() {
    final List<TStreamNodeConfiguration> streamNodes =
        configManager.getNodeManager().filterStreamNodeThroughStatus(NodeStatus.Running);
    if (streamNodes == null || streamNodes.isEmpty()) {
      return Collections.emptyList();
    }
    return streamNodes.stream()
        .sorted(Comparator.comparingInt(node -> node.getLocation().getStreamNodeId()))
        .collect(Collectors.toList());
  }

  public void close() {
    stopStreamMonitor();
    streamMonitorExecutor.shutdownNow();
    streamTaskRpcExecutor.shutdownNow();
  }

  protected TSStatus sendStartTask(
      final TEndPoint endPoint, final TStartTaskOnStreamNodeReq request) {
    return (TSStatus)
        SyncStreamNodeClientPool.getInstance()
            .sendSyncRequestToStreamNodeWithRetry(
                endPoint, request, CnToSnSyncRequestType.START_TASK);
  }

  protected TSStatus sendStopTask(
      final TEndPoint endPoint, final TStopTaskOnStreamNodeReq request) {
    return (TSStatus)
        SyncStreamNodeClientPool.getInstance()
            .sendSyncRequestToStreamNodeWithRetry(
                endPoint, request, CnToSnSyncRequestType.STOP_TASK);
  }

  protected TSStatus sendStopTask(
      final TEndPoint endPoint, final TStopTaskOnStreamNodeReq request, final int retryNum) {
    return (TSStatus)
        SyncStreamNodeClientPool.getInstance()
            .sendSyncRequestToStreamNodeWithGivenRetry(
                endPoint, request, CnToSnSyncRequestType.STOP_TASK, retryNum);
  }

  protected TSStatus sendDropTask(
      final TEndPoint endPoint, final TDropTaskOnStreamNodeReq request) {
    return (TSStatus)
        SyncStreamNodeClientPool.getInstance()
            .sendSyncRequestToStreamNodeWithRetry(
                endPoint, request, CnToSnSyncRequestType.DROP_TASK);
  }

  protected TStreamNodeHeartbeatResp sendHeartbeat(
      final TEndPoint endPoint, final TStreamNodeHeartbeatReq request) {
    final Object result =
        SyncStreamNodeClientPool.getInstance()
            .sendSyncRequestToStreamNodeWithRetry(
                endPoint, request, CnToSnSyncRequestType.GET_HEARTBEAT);
    if (result instanceof TStreamNodeHeartbeatResp) {
      return (TStreamNodeHeartbeatResp) result;
    }
    if (result instanceof TSStatus) {
      LOGGER.warn(
          ManagerMessages.STREAM_MANAGER_FAILED_TO_FETCH_STREAMNODE_HEARTBEAT, endPoint, result);
      return null;
    }
    LOGGER.warn(
        ManagerMessages.STREAM_MANAGER_UNEXPECTED_STREAMNODE_HEARTBEAT_RESPONSE, endPoint, result);
    return null;
  }

  protected TStreamNodeHeartbeatResp sendHeartbeat(
      final TEndPoint endPoint, final TStreamNodeHeartbeatReq request, final int retryNum) {
    final Object result =
        SyncStreamNodeClientPool.getInstance()
            .sendSyncRequestToStreamNodeWithGivenRetry(
                endPoint, request, CnToSnSyncRequestType.GET_HEARTBEAT, retryNum);
    if (result instanceof TStreamNodeHeartbeatResp) {
      return (TStreamNodeHeartbeatResp) result;
    }
    if (result instanceof TSStatus) {
      LOGGER.warn(
          ManagerMessages.STREAM_MANAGER_FAILED_TO_FETCH_STREAMNODE_HEARTBEAT, endPoint, result);
      return null;
    }
    LOGGER.warn(
        ManagerMessages.STREAM_MANAGER_UNEXPECTED_STREAMNODE_HEARTBEAT_RESPONSE, endPoint, result);
    return null;
  }

  private String toEndpointString(final TEndPoint endPoint) {
    return endPoint.getIp() + ":" + endPoint.getPort();
  }

  private TEndPoint parseEndpoint(final String streamNode) {
    final String[] parts = streamNode.split(":");
    return new TEndPoint(parts[0], Integer.parseInt(parts[1]));
  }
}
