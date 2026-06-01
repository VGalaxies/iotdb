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
import org.apache.iotdb.commons.exception.StartupException;
import org.apache.iotdb.commons.service.IService;
import org.apache.iotdb.commons.service.ServiceType;
import org.apache.iotdb.commons.stream.StreamTask;
import org.apache.iotdb.commons.stream.StreamTaskStatus;
import org.apache.iotdb.commons.utils.StatusUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.streamnode.engine.scheduler.IStreamTaskScheduler;
import org.apache.iotdb.streamnode.engine.scheduler.StreamTaskScheduler;
import org.apache.iotdb.streamnode.engine.scheduler.task.IStreamDriver;
import org.apache.iotdb.streamnode.engine.task.StreamTaskInstance;
import org.apache.iotdb.streamnode.rpc.thrift.TTaskHeartbeat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/** Manages the lifecycle of stream tasks on this StreamNode. */
public class StreamTaskManager implements IService {

  private static final Logger LOGGER = LoggerFactory.getLogger(StreamTaskManager.class);

  private final Map<String, StreamTask> taskMap = new ConcurrentHashMap<>();
  private final Map<String, StreamTaskInstance> instances = new ConcurrentHashMap<>();
  private ExecutorService executorService;
  private IStreamTaskScheduler scheduler;

  public StreamTaskManager() {
    this.executorService = Executors.newFixedThreadPool(1);
    this.scheduler = new StreamTaskScheduler();
  }

  private static class StreamTaskManagerHolder {
    private static final StreamTaskManager INSTANCE = new StreamTaskManager();
  }

  public static StreamTaskManager getInstance() {
    return StreamTaskManagerHolder.INSTANCE;
  }

  @Override
  public void start() throws StartupException {
    scheduler.start();
  }

  @Override
  public void stop() {
    for (StreamTaskInstance instance : instances.values()) {
      instance.stop();
    }
    instances.clear();
    taskMap.clear();
    scheduler.stop();
    executorService.shutdown();
    LOGGER.info("StreamTaskManager stop complete");
  }

  @Override
  public ServiceType getID() {
    return ServiceType.STREAM_TASK_MANAGER;
  }

  public TSStatus createTask(StreamTask task, int epoch) {
    final String taskName = task.getTaskName();
    if (taskMap.containsKey(taskName)) {
      LOGGER.warn("Task {} already exists, skipping creation", taskName);
      return new TSStatus(TSStatusCode.STREAM_ALREADY_EXISTS.getStatusCode())
          .setMessage("Task " + taskName + " already exists");
    }

    task.setEpoch(epoch);
    task.setStatus(StreamTaskStatus.CREATED);
    taskMap.put(taskName, task);
    LOGGER.info("Task {} created with epoch {}", taskName, epoch);
    return StatusUtils.OK;
  }

  public TSStatus startTask(StreamTask task, int epoch, long cnStartTime) {
    final String taskName = task.getTaskName();
    final StreamTask existing = taskMap.get(taskName);
    if (existing != null && isStaleRequest(existing, epoch, cnStartTime)) {
      LOGGER.warn(
          "Ignoring stale start request for task {}, epoch={}, cnStartTime={}",
          taskName,
          epoch,
          cnStartTime);
      return new TSStatus(TSStatusCode.STREAM_STALE.getStatusCode())
          .setMessage("Stale start request for task " + taskName);
    }

    stopInstance(taskName);
    task.setEpoch(epoch);
    task.setCnStartTime(cnStartTime);
    task.setStatus(StreamTaskStatus.RUNNING);
    taskMap.put(taskName, task);
    startInstance(task);
    LOGGER.info("Task {} started with epoch {} and cnStartTime {}", taskName, epoch, cnStartTime);
    return StatusUtils.OK;
  }

  public TSStatus startTask(String taskName, int epoch) {
    StreamTask task = taskMap.get(taskName);
    if (task == null) {
      LOGGER.warn("Task {} not found", taskName);
      return new TSStatus(TSStatusCode.STREAM_NOT_EXIST.getStatusCode())
          .setMessage("Task " + taskName + " not found");
    }
    if (epoch < task.getEpoch()) {
      LOGGER.warn(
          "Stale epoch {} for task {}, current epoch is {}", epoch, taskName, task.getEpoch());
      return new TSStatus(TSStatusCode.STREAM_STALE.getStatusCode())
          .setMessage("Stale epoch " + epoch + ", current is " + task.getEpoch());
    }
    task.setEpoch(epoch);
    task.setStatus(StreamTaskStatus.RUNNING);
    return startTask(task, epoch, task.getCnStartTime());
  }

  public TSStatus stopTask(String taskName) {
    return stopTask(taskName, Integer.MAX_VALUE, Long.MAX_VALUE);
  }

  public TSStatus stopTask(String taskName, int epoch, long cnStartTime) {
    StreamTask task = taskMap.get(taskName);
    if (task == null) {
      LOGGER.info(
          "Task {} not found locally while stopping, treating as already stopped", taskName);
      return StatusUtils.OK;
    }
    if (isStaleRequest(task, epoch, cnStartTime)) {
      LOGGER.warn(
          "Ignoring stale stop request for task {}, epoch={}, cnStartTime={}",
          taskName,
          epoch,
          cnStartTime);
      return StatusUtils.OK;
    }

    task.setEpoch(epoch);
    task.setCnStartTime(cnStartTime);
    task.setStatus(StreamTaskStatus.STOPPED);
    stopInstance(taskName);
    LOGGER.info("Task {} stopped", taskName);
    return StatusUtils.OK;
  }

  public TSStatus dropTask(String taskName) {
    StreamTask task = taskMap.remove(taskName);
    if (task == null) {
      LOGGER.info(
          "Task {} not found locally while dropping, treating as already dropped", taskName);
      return StatusUtils.OK;
    }

    stopInstance(taskName);
    task.setStatus(StreamTaskStatus.DROPPED);
    LOGGER.info("Task {} dropped", taskName);
    return StatusUtils.OK;
  }

  public void dropAllTasks() {
    LOGGER.info("Dropping all {} tasks", taskMap.size());
    for (String taskName : new ArrayList<>(taskMap.keySet())) {
      dropTask(taskName);
    }
  }

  public StreamTask getTask(String taskName) {
    return taskMap.get(taskName);
  }

  public List<TTaskHeartbeat> getRunningTaskHeartbeats() {
    final List<TTaskHeartbeat> heartbeats = new ArrayList<>();
    taskMap.values().stream()
        .filter(task -> task.getStatus() == StreamTaskStatus.RUNNING)
        .forEach(task -> heartbeats.add(new TTaskHeartbeat(task.getTaskName(), task.getEpoch())));
    return heartbeats;
  }

  public int getTaskCount() {
    return taskMap.size();
  }

  public void start(StreamTask task) {
    task.setStatus(StreamTaskStatus.RUNNING);
    taskMap.put(task.getTaskName(), task);
    startInstance(task);
  }

  public void create(StreamTask task) {
    start(task);
  }

  public void stop(String taskName) {
    StreamTask task = taskMap.get(taskName);
    if (task != null) {
      task.setStatus(StreamTaskStatus.STOPPED);
    }
    stopInstance(taskName);
  }

  public void drop(String taskName) {
    dropTask(taskName);
  }

  public void dropAll() {
    dropAllTasks();
  }

  private StreamTaskInstance startInstance(StreamTask task) {
    final String taskName = task.getTaskName();
    final StreamTaskInstance existing = instances.get(taskName);
    if (existing != null) {
      if (!existing.isRunning()) {
        executorService.submit(existing::start);
      }
      return existing;
    }

    final StreamTaskInstance instance =
        new StreamTaskInstance(
            task,
            (tsBlock, commitId, partitionKey) -> {
              StreamTaskInstance streamTaskInstance = instances.get(taskName);
              boolean isNewDriver = !streamTaskInstance.isStreamDriverExists(partitionKey);
              IStreamDriver streamDriver = streamTaskInstance.getOrCreateStreamDriver(partitionKey);
              if (isNewDriver) {
                scheduler.submitStreamDriver(streamDriver, 0);
              }
              return streamDriver.push(tsBlock, commitId);
            });
    instances.put(taskName, instance);
    executorService.submit(instance::start);
    LOGGER.info("Submitted task for execution: {}", taskName);
    return instance;
  }

  private boolean isStaleRequest(StreamTask task, int epoch, long cnStartTime) {
    return cnStartTime < task.getCnStartTime() || epoch < task.getEpoch();
  }

  private void stopInstance(String taskName) {
    final StreamTaskInstance instance = instances.remove(taskName);
    if (instance != null) {
      instance.stop();
    }
    scheduler.cancelStreamTask(taskName);
  }

  public int getTaskNum() {
    return scheduler.getTaskCount();
  }

  public int getRunningTaskNum() {
    return scheduler.getRunningTaskCount();
  }
}
