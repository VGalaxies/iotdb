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
import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.commons.concurrent.threadpool.ScheduledExecutorUtil;
import org.apache.iotdb.commons.stream.StreamTask;
import org.apache.iotdb.commons.stream.StreamTaskStatus;
import org.apache.iotdb.commons.utils.StatusUtils;
import org.apache.iotdb.confignode.client.sync.CnToSnSyncRequestType;
import org.apache.iotdb.confignode.client.sync.SyncStreamNodeClientPool;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.manager.IManager;
import org.apache.iotdb.confignode.manager.load.cache.AbstractHeartbeatSample;
import org.apache.iotdb.confignode.manager.load.cache.IFailureDetector;
import org.apache.iotdb.confignode.manager.load.cache.detector.FixedDetector;
import org.apache.iotdb.confignode.manager.load.cache.detector.PhiAccrualDetector;
import org.apache.iotdb.confignode.persistence.stream.StreamInfo;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.streamnode.rpc.thrift.TDropTaskOnStreamNodeReq;
import org.apache.iotdb.streamnode.rpc.thrift.TStartTaskOnStreamNodeReq;
import org.apache.iotdb.streamnode.rpc.thrift.TStopTaskOnStreamNodeReq;
import org.apache.iotdb.streamnode.rpc.thrift.TStreamNodeHeartbeatReq;
import org.apache.iotdb.streamnode.rpc.thrift.TStreamNodeHeartbeatResp;
import org.apache.iotdb.streamnode.rpc.thrift.TTaskHeartbeat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

@SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
public class StreamManager {

  private static final long cnStartTime = System.currentTimeMillis();
  private static final Logger LOGGER = LoggerFactory.getLogger(StreamManager.class);

  // Maximum number of heartbeat samples retained per task
  private static final int MAX_HEARTBEAT_HISTORY = 200;

  private final IManager configManager;
  private final StreamInfo streamInfo;
  private final IFailureDetector failureDetector;
  // taskName -> ordered deque of heartbeat samples (nano timestamps)
  private final Map<String, Deque<AbstractHeartbeatSample>> heartbeatHistory =
      new ConcurrentHashMap<>();
  private final AtomicInteger nextAssignIndex = new AtomicInteger(0);
  private final ScheduledExecutorService streamMonitorExecutor;

  public StreamManager(IManager configManager, StreamInfo streamInfo) {
    this.configManager = configManager;
    this.streamInfo = streamInfo;
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
    ScheduledExecutorUtil.safelyScheduleAtFixedRate(
        this.streamMonitorExecutor, this::monitorStreams, 0, 5, TimeUnit.SECONDS);
  }

  public StreamInfo getStreamInfo() {
    return streamInfo;
  }

  public TSStatus dropStream(String streamName) {
    streamInfo.writeLock();
    try {
      LOGGER.info("Dropping stream: {}", streamName);
      final TSStatus dropRuntimeStatus = dropRuntimeTaskInternal(streamName);
      if (dropRuntimeStatus.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return dropRuntimeStatus;
      }
      final TSStatus removeStatus = streamInfo.removeTask(streamName);
      if (removeStatus.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        heartbeatHistory.remove(streamName);
      }
      return removeStatus;
    } finally {
      streamInfo.writeUnlock();
    }
  }

  public TSStatus dropRuntimeTask(String streamName) {
    streamInfo.writeLock();
    try {
      return dropRuntimeTaskInternal(streamName);
    } finally {
      streamInfo.writeUnlock();
    }
  }

  private TSStatus dropRuntimeTaskInternal(String streamName) {
    final StreamTask task = streamInfo.getTask(streamName);
    if (task == null) {
      return new TSStatus(TSStatusCode.STREAM_NOT_EXIST.getStatusCode())
          .setMessage("Stream not found: " + streamName);
    }

    synchronized (task) {
      final String streamNode = task.getRunningOn();
      if (streamNode != null && !streamNode.isEmpty()) {
        final TEndPoint endPoint;
        try {
          endPoint = parseEndpoint(streamNode);
        } catch (RuntimeException e) {
          return new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode())
              .setMessage(
                  "Invalid StreamNode endpoint for stream " + streamName + ": " + streamNode);
        }

        if (task.getStatus() == StreamTaskStatus.RUNNING) {
          final int stopEpoch = task.getEpoch() + 1;
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

  public TSStatus startStream(String streamName) {
    streamInfo.readLock();
    try {
      LOGGER.info("Starting stream: {}", streamName);
      StreamTask task = streamInfo.getTask(streamName);
      if (task == null) {
        return new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode())
            .setMessage("Stream not found: " + streamName);
      }
      return startStreamInternal(task);
    } finally {
      streamInfo.readUnlock();
    }
  }

  // Caller must hold the read lock
  private TSStatus startStreamInternal(StreamTask task) {
    synchronized (task) {
      if (task.getStatus() == StreamTaskStatus.RUNNING) {
        return StatusUtils.OK;
      }

      String streamNode = assignStreamToStreamNode(task);
      if (streamNode == null) {
        return new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode())
            .setMessage("No available StreamNode to run stream task " + task.getTaskName());
      }
      // Parse runningOn to TEndPoint
      TEndPoint endPoint = parseEndpoint(streamNode);
      task.setEpoch(task.getEpoch() + 1);
      task.setLeaderTerm(configManager.getConsensusManager().getLeaderTerm());
      task.setCnStartTime(cnStartTime);
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
    streamInfo.readLock();
    try {
      final StreamTask task = streamInfo.getTask(taskName);
      if (task == null) {
        LOGGER.warn("Received heartbeat for unknown task: {}", taskName);
        return new TSStatus(TSStatusCode.STREAM_NOT_EXIST.getStatusCode())
            .setMessage("Stream task not found: " + taskName);
      }
      synchronized (task) {
        if (task.getLeaderTerm() != leaderTerm
            || cnStartTime < task.getCnStartTime()
            || task.getEpoch() > epoch) {
          return reportStaleHeartbeat(taskName, epoch, leaderTerm, task);
        }
        task.setLastHeartbeatTime(System.currentTimeMillis());
        task.setRunningOn(runningOn);
        task.setStatus(StreamTaskStatus.RUNNING);
      }
    } finally {
      streamInfo.readUnlock();
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
    LOGGER.warn(
        "Stale heartbeat for task {}: expected epoch={} leaderTerm={}, got epoch={} leaderTerm={}",
        taskName,
        task.getEpoch(),
        task.getLeaderTerm(),
        epoch,
        leaderTerm);
    return new TSStatus(TSStatusCode.STREAM_STALE.getStatusCode())
        .setMessage(
            String.format(
                "Stale heartbeat for task %s: expected epoch=%d leaderTerm=%d, got epoch=%d leaderTerm=%d",
                taskName, task.getEpoch(), task.getLeaderTerm(), epoch, leaderTerm));
  }

  private String assignStreamToStreamNode(StreamTask task) {
    final List<TStreamNodeConfiguration> streamNodes = getRegisteredStreamNodes();
    if (streamNodes.isEmpty()) {
      return task.getRunningOn() == null || task.getRunningOn().isEmpty()
          ? null
          : task.getRunningOn();
    }
    final int index = Math.floorMod(nextAssignIndex.getAndIncrement(), streamNodes.size());
    return toEndpointString(streamNodes.get(index).getLocation().getInternalEndPoint());
  }

  public TSStatus stopStream(String streamName) {
    streamInfo.writeLock();
    try {
      LOGGER.info("Stopping stream: {}", streamName);
      StreamTask task = streamInfo.getTask(streamName);
      if (task == null) {
        return new TSStatus(TSStatusCode.STREAM_NOT_EXIST.getStatusCode())
            .setMessage("Stream not found: " + streamName);
      }

      synchronized (task) {
        if (task.getStatus() == StreamTaskStatus.STOPPED) {
          return StatusUtils.OK;
        }
        task.setEpoch(task.getEpoch() + 1);
        task.setCnStartTime(cnStartTime);
        String streamNode = task.getRunningOn();
        if (streamNode != null && !streamNode.isEmpty()) {
          // Parse runningOn to TEndPoint
          TEndPoint endPoint = parseEndpoint(streamNode);
          // Create request
          TStopTaskOnStreamNodeReq req =
              new TStopTaskOnStreamNodeReq(streamName, task.getEpoch(), task.getCnStartTime());
          // Send request to StreamNode
          TSStatus status = sendStopTask(endPoint, req);
          if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
            return status;
          }
        }
        // Update status
        task.setStatus(StreamTaskStatus.STOPPED);
        task.setLastDownTime(System.currentTimeMillis());
        task.setLastDownReason("Manually Stopped");
        heartbeatHistory.remove(streamName);
      }
      return StatusUtils.OK;
    } finally {
      streamInfo.writeUnlock();
    }
  }

  public List<StreamTask> showStreams() {
    streamInfo.readLock();
    try {
      return streamInfo.getAllTasks();
    } finally {
      streamInfo.readUnlock();
    }
  }

  public List<StreamTask> showStreams(String username) {
    streamInfo.readLock();
    try {
      return streamInfo.getAllTasks().stream()
          .filter(s -> username == null || Objects.equals(s.getCreator(), username))
          .collect(Collectors.toList());
    } finally {
      streamInfo.readUnlock();
    }
  }

  private void monitorStreams() {
    collectStreamNodeHeartbeats();

    List<StreamTask> toUpdate = new ArrayList<>();
    streamInfo.readLock();
    try {
      for (StreamTask task : streamInfo.getAllTasks()) {
        if (task.getStatus() != StreamTaskStatus.RUNNING) {
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
          if (task.getStatus() == StreamTaskStatus.RUNNING) { // Double check
            task.setStatus(StreamTaskStatus.UNKNOWN);
            task.setLastDownTime(System.currentTimeMillis());
            task.setLastDownReason("Heartbeat Lost");
            LOGGER.info("Marked task {} as UNKNOWN due to heartbeat loss", task.getTaskName());
          }
        }
      } finally {
        streamInfo.writeUnlock();
      }
    }

    // Restart all UNKNOWN tasks
    List<StreamTask> unknownTasks = new ArrayList<>();
    streamInfo.readLock();
    try {
      for (StreamTask task : streamInfo.getAllTasks()) {
        if (task.getStatus() == StreamTaskStatus.UNKNOWN) {
          unknownTasks.add(task);
        }
      }
    } finally {
      streamInfo.readUnlock();
    }

    for (StreamTask task : unknownTasks) {
      LOGGER.info("Restarting UNKNOWN task: {}", task.getTaskName());
      synchronized (task) {
        if (task.getStatus() == StreamTaskStatus.UNKNOWN) { // Double check
          TSStatus status = startStreamInternal(task);
          if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
            LOGGER.warn("Failed to restart task {}: {}", task.getTaskName(), status.getMessage());
          } else {
            // Clear stale heartbeat history so the detector starts fresh after restart
            heartbeatHistory.remove(task.getTaskName());
          }
        }
      }
    }
  }

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
      final String runningOn = toEndpointString(endPoint);
      for (final TTaskHeartbeat heartbeat : resp.getRunningTasks()) {
        recordHeartbeatFromStreamNode(heartbeat, runningOn);
      }
    }
  }

  private void recordHeartbeatFromStreamNode(
      final TTaskHeartbeat heartbeat, final String runningOn) {
    final long leaderTerm;
    streamInfo.readLock();
    try {
      final StreamTask task = streamInfo.getTask(heartbeat.getTaskName());
      if (task == null) {
        LOGGER.warn("Received heartbeat for unknown task: {}", heartbeat.getTaskName());
        return;
      }
      leaderTerm = task.getLeaderTerm();
    } finally {
      streamInfo.readUnlock();
    }
    recordHeartbeat(
        heartbeat.getTaskName(), heartbeat.getEpoch(), cnStartTime, leaderTerm, runningOn);
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

  public void close() {
    streamMonitorExecutor.shutdownNow();
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
      LOGGER.warn("Failed to fetch StreamNode heartbeat from {}: {}", endPoint, result);
      return null;
    }
    LOGGER.warn("Unexpected StreamNode heartbeat response from {}: {}", endPoint, result);
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
