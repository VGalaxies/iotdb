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
import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.exception.StartupException;
import org.apache.iotdb.commons.service.IService;
import org.apache.iotdb.commons.service.ServiceType;
import org.apache.iotdb.commons.stream.StreamTask;
import org.apache.iotdb.commons.stream.StreamTaskStatus;
import org.apache.iotdb.commons.utils.StatusUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.streamnode.common.constant.ThreadName;
import org.apache.iotdb.streamnode.engine.scheduler.IStreamTaskScheduler;
import org.apache.iotdb.streamnode.engine.scheduler.StreamTaskScheduler;
import org.apache.iotdb.streamnode.engine.sink.GlobalSinkMemoryController;
import org.apache.iotdb.streamnode.engine.sink.GlobalSinkWriterPool;
import org.apache.iotdb.streamnode.engine.task.AbstractStreamTaskInstance;
import org.apache.iotdb.streamnode.i18n.StreamNodeMessages;
import org.apache.iotdb.streamnode.rpc.thrift.TTaskHeartbeat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

/** Manages the lifecycle of stream tasks on this StreamNode. */
public class StreamTaskManager implements IService {

  private static final Logger LOGGER = LoggerFactory.getLogger(StreamTaskManager.class);

  private final Map<String, StreamTask> taskMap = new ConcurrentHashMap<>();
  private final Map<String, AbstractStreamTaskInstance> instances = new ConcurrentHashMap<>();
  private final ExecutorService subTaskNotificationExecutor;
  private final IStreamTaskScheduler scheduler;
  private final GlobalSinkWriterPool globalSinkWriterPool;
  private final GlobalSinkMemoryController globalSinkMemoryController;

  public StreamTaskManager() {
    this.subTaskNotificationExecutor =
        IoTDBThreadPoolFactory.newFixedThreadPool(4, ThreadName.STREAM_SUBTASK_NOTIFICATION);
    this.scheduler = new StreamTaskScheduler();
    this.globalSinkWriterPool = GlobalSinkWriterPool.getInstance();
    this.globalSinkMemoryController = GlobalSinkMemoryController.getInstance();
  }

  public IStreamTaskScheduler getScheduler() {
    return this.scheduler;
  }

  /** Returns the number of stream tasks whose status is {@link StreamTaskStatus#RUNNING}. */
  public int getRunningStreamTaskCount() {
    return (int)
        taskMap.values().stream()
            .filter(task -> task.getStatus() == StreamTaskStatus.RUNNING)
            .count();
  }

  /** Returns the total number of sub-tasks across all stream task instances. */
  public int getSubTaskTotal() {
    return instances.values().stream().mapToInt(AbstractStreamTaskInstance::getSubTaskCount).sum();
  }

  private static class StreamTaskManagerHolder {
    private static final StreamTaskManager INSTANCE = new StreamTaskManager();
  }

  public static StreamTaskManager getInstance() {
    return StreamTaskManagerHolder.INSTANCE;
  }

  @Override
  public void start() throws StartupException {
    globalSinkWriterPool.start();
    scheduler.start();
  }

  @Override
  public void stop() {
    dropAllTasks();
    instances.clear();
    taskMap.clear();
    scheduler.stop();
    subTaskNotificationExecutor.shutdown();
    // Writer pool and write-back memory are peer global resources owned by this manager.
    globalSinkWriterPool.stopAll();
    globalSinkMemoryController.close();
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
    if (existing != null && isStaleEpoch(existing, epoch)) {
      LOGGER.warn(
          "Ignoring stale start request for task {}, epoch={}, cnStartTime={}",
          taskName,
          epoch,
          cnStartTime);
      return new TSStatus(TSStatusCode.STREAM_STALE.getStatusCode())
          .setMessage("Stale start request for task " + taskName);
    }
    task.setEpoch(epoch);
    task.setCnStartTime(cnStartTime);
    taskMap.put(taskName, task);
    try {
      startInstance(task);
    } catch (Exception e) {
      LOGGER.warn("Failed to start task {}", taskName, e);
      // start() left the instance half-initialized; clean up accordingly.
      // If the task existed before (restarting a stopped task), stop to preserve state.
      // If it's a brand new task, fully clean up with drop.
      if (existing == null) {
        dropInstance(taskName);
      } else {
        stopInstance(taskName);
      }
      taskMap.remove(taskName);
      return new TSStatus(TSStatusCode.STREAM_START_ERROR.getStatusCode())
          .setMessage("Failed to start task " + taskName + ": " + e.getMessage());
    }
    task.setStatus(StreamTaskStatus.RUNNING);
    LOGGER.info("Task {} started with epoch {} and cnStartTime {}", taskName, epoch, cnStartTime);
    return StatusUtils.OK;
  }

  public TSStatus stopTask(String taskName, int epoch, long cnStartTime) {
    StreamTask task = taskMap.get(taskName);
    if (task == null) {
      LOGGER.info(
          "Task {} not found locally while stopping, treating as already stopped", taskName);
      return StatusUtils.OK;
    }
    if (isStaleEpoch(task, epoch)) {
      if (task.getStatus() != StreamTaskStatus.RUNNING) {
        return StatusUtils.OK;
      }
      LOGGER.warn(
          "Ignoring stale stop request for task {}, epoch={}, cnStartTime={}",
          taskName,
          epoch,
          cnStartTime);
      return new TSStatus(TSStatusCode.STREAM_STALE.getStatusCode())
          .setMessage(
              String.format(
                  StreamNodeMessages.MESSAGE_STALE_STOP_REQUEST_FOR_TASK_ARG_4309B991, taskName));
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

    dropInstance(taskName);
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

  public void dropAll() {
    dropAllTasks();
  }

  private AbstractStreamTaskInstance startInstance(StreamTask task) throws Exception {
    final String taskName = task.getTaskName();
    final AbstractStreamTaskInstance instance =
        instances.computeIfAbsent(
            taskName,
            name ->
                AbstractStreamTaskInstance.create(task, scheduler, subTaskNotificationExecutor));
    // A stopped task is restarted with a fresh StreamTask object from the ConfigNode, while the
    // local instance is intentionally retained to preserve its execution resources. Keep the
    // source and scheduling metadata on the retained definition in sync before rebuilding
    // consumers with the new owner epoch.
    if (instance.getTaskDefinition() != task) {
      instance.getTaskDefinition().setEpoch(task.getEpoch());
      instance.getTaskDefinition().setSource(task.getSource());
      instance.getTaskDefinition().setCnStartTime(task.getCnStartTime());
    }
    instance.start();
    LOGGER.info("Started task: {}", taskName);
    return instance;
  }

  private boolean isStaleEpoch(StreamTask task, int epoch) {
    return epoch < task.getEpoch();
  }

  private void stopInstance(String taskName) {
    final AbstractStreamTaskInstance instance = instances.get(taskName);
    if (instance != null) {
      try {
        instance.stop();
      } catch (Exception e) {
        LOGGER.warn("Error stopping instance for task {}", taskName, e);
      }
    }
  }

  private void dropInstance(String taskName) {
    final AbstractStreamTaskInstance instance = instances.remove(taskName);
    scheduler.cancelStreamTask(taskName);
    if (instance != null) {
      instance.drop();
    }
  }

  public int getTaskNum() {
    return scheduler.getTaskCount();
  }

  public int getRunningTaskNum() {
    return scheduler.getRunningTaskCount();
  }
}
