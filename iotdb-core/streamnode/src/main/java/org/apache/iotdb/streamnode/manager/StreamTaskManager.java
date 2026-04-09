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

package org.apache.iotdb.streamnode.manager;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.stream.StreamTask;
import org.apache.iotdb.commons.stream.StreamTaskStatus;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** Manages the lifecycle of stream tasks on this StreamNode. */
public class StreamTaskManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(StreamTaskManager.class);

  /** taskName -> StreamTask */
  private final Map<String, StreamTask> taskMap = new ConcurrentHashMap<>();

  public StreamTaskManager() {}

  public TSStatus createTask(StreamTask task, int epoch) {
    String taskName = task.getTaskName();
    if (taskMap.containsKey(taskName)) {
      LOGGER.warn("Task {} already exists, skipping creation", taskName);
      return new TSStatus(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode())
          .setMessage("Task " + taskName + " already exists");
    }
    task.setEpoch(epoch);
    task.setStatus(StreamTaskStatus.CREATED);
    taskMap.put(taskName, task);
    LOGGER.info("Task {} created with epoch {}", taskName, epoch);
    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }

  public TSStatus startTask(String taskName, int epoch) {
    StreamTask task = taskMap.get(taskName);
    if (task == null) {
      LOGGER.warn("Task {} not found", taskName);
      return new TSStatus(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode())
          .setMessage("Task " + taskName + " not found");
    }
    if (epoch < task.getEpoch()) {
      LOGGER.warn(
          "Stale epoch {} for task {}, current epoch is {}", epoch, taskName, task.getEpoch());
      return new TSStatus(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode())
          .setMessage("Stale epoch " + epoch + ", current is " + task.getEpoch());
    }
    task.setEpoch(epoch);
    task.setStatus(StreamTaskStatus.RUNNING);
    // TODO: start the actual execution engine for this task
    LOGGER.info("Task {} started with epoch {}", taskName, epoch);
    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }

  public TSStatus stopTask(String taskName) {
    StreamTask task = taskMap.get(taskName);
    if (task == null) {
      LOGGER.warn("Task {} not found", taskName);
      return new TSStatus(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode())
          .setMessage("Task " + taskName + " not found");
    }
    task.setStatus(StreamTaskStatus.STOPPED);
    // TODO: stop the actual execution engine for this task
    LOGGER.info("Task {} stopped", taskName);
    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }

  public TSStatus dropTask(String taskName) {
    StreamTask task = taskMap.remove(taskName);
    if (task == null) {
      LOGGER.warn("Task {} not found", taskName);
      return new TSStatus(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode())
          .setMessage("Task " + taskName + " not found");
    }
    task.setStatus(StreamTaskStatus.DROPPED);
    // TODO: clean up all resources for this task
    LOGGER.info("Task {} dropped", taskName);
    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }

  public void dropAllTasks() {
    LOGGER.info("Dropping all {} tasks", taskMap.size());
    for (String taskName : taskMap.keySet()) {
      dropTask(taskName);
    }
  }

  public StreamTask getTask(String taskName) {
    return taskMap.get(taskName);
  }

  public int getTaskCount() {
    return taskMap.size();
  }
}
