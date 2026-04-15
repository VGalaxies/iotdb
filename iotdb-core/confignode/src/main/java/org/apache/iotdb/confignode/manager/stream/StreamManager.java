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
import org.apache.iotdb.streamnode.rpc.thrift.TStartTaskOnStreamNodeReq;
import org.apache.iotdb.streamnode.rpc.thrift.TStopTaskOnStreamNodeReq;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
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

  public TSStatus dropStream(String database, String streamName) {
    streamInfo.writeLock();
    try {
      LOGGER.info("Dropping stream: {}.{}", database, streamName);
      String taskName = database + "." + streamName;
      return streamInfo.removeTask(taskName);
    } finally {
      streamInfo.writeUnlock();
    }
  }

  public TSStatus startStream(String database, String streamName) {
    streamInfo.readLock();
    try {
      LOGGER.info("Starting stream: {}.{}", database, streamName);
      String taskName = database + "." + streamName;
      StreamTask task = streamInfo.getTask(taskName);
      if (task == null) {
        return new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode())
            .setMessage("Stream not found: " + taskName);
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
      // Parse runningOn to TEndPoint
      String[] parts = streamNode.split(":");
      TEndPoint endPoint = new TEndPoint(parts[0], Integer.parseInt(parts[1]));
      task.setEpoch(task.getEpoch() + 1);
      task.setLeaderTerm(configManager.getConsensusManager().getLeaderTerm());
      // Create request
      TStartTaskOnStreamNodeReq req =
          new TStartTaskOnStreamNodeReq(task.toByteBuffer(), task.getEpoch(), cnStartTime);
      // Send request to StreamNode
      TSStatus status =
          (TSStatus)
              SyncStreamNodeClientPool.getInstance()
                  .sendSyncRequestToStreamNodeWithRetry(
                      endPoint, req, CnToSnSyncRequestType.START_TASK);
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
  public TSStatus recordHeartbeat(String taskName, int epoch, long cnStartTime, long leaderTerm, String runningOn) {
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
            || StreamManager.cnStartTime > cnStartTime
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

  private TSStatus reportStaleHeartbeat(String taskName, int epoch, long leaderTerm,
      StreamTask task) {
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
    // TODO: let StreamNodeManager assign a StreamNode based on load and other factors, for now just
    // return a placeholder
    return "placeholder:12345";
  }

  public TSStatus stopStream(String database, String streamName) {
    streamInfo.writeLock();
    try {
      LOGGER.info("Stopping stream: {}.{}", database, streamName);
      String taskName = database + "." + streamName;
      StreamTask task = streamInfo.getTask(taskName);
      if (task == null) {
        return new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode())
            .setMessage("Stream not found: " + taskName);
      }

      synchronized (task) {
        String streamNode = task.getRunningOn();
        // Parse runningOn to TEndPoint
        String[] parts = streamNode.split(":");
        TEndPoint endPoint = new TEndPoint(parts[0], Integer.parseInt(parts[1]));
        // Create request
        TStopTaskOnStreamNodeReq req =
            new TStopTaskOnStreamNodeReq(
                streamName, task.getEpoch(), configManager.getConsensusManager().getLeaderTerm());
        // Send request to StreamNode
        TSStatus status =
            (TSStatus)
                SyncStreamNodeClientPool.getInstance()
                    .sendSyncRequestToStreamNodeWithRetry(
                        endPoint, req, CnToSnSyncRequestType.STOP_TASK);
        if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
          return status;
        }
        // Update status
        task.setStatus(StreamTaskStatus.STOPPED);
        task.setLastDownTime(System.currentTimeMillis());
        task.setLastDownReason("Manually stopped");
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

  public List<StreamTask> showStreams(String database) {
    streamInfo.readLock();
    try {
      return streamInfo.getAllTasks().stream()
          .filter(s -> s.getDatabase().equals(database))
          .collect(Collectors.toList());
    } finally {
      streamInfo.readUnlock();
    }
  }

  private void monitorStreams() {
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
}
