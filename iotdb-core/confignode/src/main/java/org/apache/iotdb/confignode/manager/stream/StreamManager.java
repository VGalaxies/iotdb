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

import java.util.stream.Collectors;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.stream.StreamTask;
import org.apache.iotdb.commons.stream.StreamTaskStatus;
import org.apache.iotdb.commons.utils.StatusUtils;
import org.apache.iotdb.confignode.client.sync.CnToSnSyncRequestType;
import org.apache.iotdb.confignode.client.sync.SyncStreamNodeClientPool;
import org.apache.iotdb.confignode.manager.IManager;
import org.apache.iotdb.confignode.persistence.stream.StreamInfo;
import org.apache.iotdb.streamnode.rpc.thrift.TStartTaskOnStreamNodeReq;
import org.apache.iotdb.streamnode.rpc.thrift.TStopTaskOnStreamNodeReq;

import org.apache.iotdb.rpc.TSStatusCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class StreamManager {

  private static final long CN_STRAT_TIME = System.currentTimeMillis();
  private static final Logger LOGGER = LoggerFactory.getLogger(StreamManager.class);

  private final IManager configManager;
  private final StreamInfo streamInfo;
  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

  public StreamManager(IManager configManager, StreamInfo streamInfo) {
    this.configManager = configManager;
    this.streamInfo = streamInfo;
  }

  public TSStatus createStream(StreamTask task) {
    lock.writeLock().lock();
    try {
      LOGGER.info("Creating stream task: {}", task.getTaskName());
      return streamInfo.addTask(task);
    } finally {
      lock.writeLock().unlock();
    }
  }

  public TSStatus dropStream(String database, String streamName) {
    lock.writeLock().lock();
    try {
      LOGGER.info("Dropping stream: {}.{}", database, streamName);
      String taskName = database + "." + streamName;
      return streamInfo.removeTask(taskName);
    } finally {
      lock.writeLock().unlock();
    }
  }

  public TSStatus startStream(String database, String streamName) {
    lock.writeLock().lock();
    try {
      LOGGER.info("Starting stream: {}.{}", database, streamName);
      String taskName = database + "." + streamName;
      StreamTask task = streamInfo.getTask(taskName);
      if (task == null) {
        return new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode())
            .setMessage("Stream not found: " + taskName);
      }
      String streamNode = assignStreamToStreamNode(task);
      // Parse runningOn to TEndPoint
      String[] parts = streamNode.split(":");
      TEndPoint endPoint = new TEndPoint(parts[0], Integer.parseInt(parts[1]));
      task.setEpoch(task.getEpoch() + 1);
      // Create request
      TStartTaskOnStreamNodeReq req = new TStartTaskOnStreamNodeReq(task.toByteBuffer(), task.getEpoch(), CN_STRAT_TIME);
      // Send request to StreamNode
      TSStatus status = (TSStatus) SyncStreamNodeClientPool.getInstance()
          .sendSyncRequestToStreamNodeWithRetry(endPoint, req, CnToSnSyncRequestType.START_TASK);
      if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return status;
      }
      // Update status
      task.setRunningOn(streamNode);
      task.setStatus(StreamTaskStatus.RUNNING);
      task.setLastUpTime(System.currentTimeMillis());
      return StatusUtils.OK;
    } finally {
      lock.writeLock().unlock();
    }
  }

  private String assignStreamToStreamNode(StreamTask task) {
    // TODO: let StreamNodeManager assign a StreamNode based on load and other factors, for now just return a placeholder
    return "placeholder:12345";
  }

  public TSStatus stopStream(String database, String streamName) {
    lock.writeLock().lock();
    try {
      LOGGER.info("Stopping stream: {}.{}", database, streamName);
      String taskName = database + "." + streamName;
      StreamTask task = streamInfo.getTask(taskName);
      if (task == null) {
        return new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode())
            .setMessage("Stream not found: " + taskName);
      }
      String streamNode = task.getRunningOn();
      // Parse runningOn to TEndPoint
      String[] parts = streamNode.split(":");
      TEndPoint endPoint = new TEndPoint(parts[0], Integer.parseInt(parts[1]));
      // Create request
      TStopTaskOnStreamNodeReq req = new TStopTaskOnStreamNodeReq(streamName, task.getEpoch(), CN_STRAT_TIME);
      // Send request to StreamNode
      TSStatus status = (TSStatus) SyncStreamNodeClientPool.getInstance()
          .sendSyncRequestToStreamNodeWithRetry(endPoint, req, CnToSnSyncRequestType.STOP_TASK);
      if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return status;
      }
      // Update status
      task.setStatus(StreamTaskStatus.STOPPED);
      task.setLastDownTime(System.currentTimeMillis());
      task.setLastDownReason("Manually stopped");
      return StatusUtils.OK;
    } finally {
      lock.writeLock().unlock();
    }
  }

  public List<StreamTask> showStreams() {
    lock.readLock().lock();
    try {
      return streamInfo.getAllTasks();
    } finally {
      lock.readLock().unlock();
    }
  }

  public List<StreamTask> showStreams(String database) {
    lock.readLock().lock();
    try {
      return streamInfo.getAllTasks().stream().filter(s -> s.getDatabase().equals(database)).collect(
          Collectors.toList());
    } finally {
      lock.readLock().unlock();
    }
  }
}
