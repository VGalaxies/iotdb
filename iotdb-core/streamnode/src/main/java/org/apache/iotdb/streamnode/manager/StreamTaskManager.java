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

import org.apache.iotdb.commons.exception.StartupException;
import org.apache.iotdb.commons.service.IService;
import org.apache.iotdb.commons.service.ServiceType;
import org.apache.iotdb.commons.stream.StreamTask;
import org.apache.iotdb.streamnode.engine.scheduler.IStreamTaskScheduler;
import org.apache.iotdb.streamnode.engine.scheduler.StreamTaskScheduler;
import org.apache.iotdb.streamnode.engine.scheduler.task.IStreamDriver;
import org.apache.iotdb.streamnode.engine.task.StreamTaskInstance;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/** Manages the lifecycle of stream tasks on this StreamNode. */
public class StreamTaskManager implements IService {

  private static final Logger LOGGER = LoggerFactory.getLogger(StreamTaskManager.class);

  private final Map<String, StreamTaskInstance> instances = new ConcurrentHashMap<>();
  private final ExecutorService executorService;
  private final IStreamTaskScheduler scheduler;

  public StreamTaskManager() {
    this.executorService = Executors.newFixedThreadPool(1);
    this.scheduler = new StreamTaskScheduler();
  }

  @Override
  public void start() throws StartupException {
    this.scheduler.start();
  }

  @Override
  public void stop() {
    for (StreamTaskInstance instance : instances.values()) {
      instance.stop();
    }
    instances.clear();
    this.scheduler.stop();
    executorService.shutdown();
    LOGGER.info("StreamTaskRunner stop complete");
  }

  @Override
  public ServiceType getID() {
    return ServiceType.STREAM_TASK_MANAGER;
  }

  private static class StreamTaskManagerHolder {
    private static final StreamTaskManager INSTANCE = new StreamTaskManager();
  }

  public static StreamTaskManager getInstance() {
    return StreamTaskManager.StreamTaskManagerHolder.INSTANCE;
  }

  public void start(StreamTask task) {
    String taskName = task.getTaskName();
    StreamTaskInstance existInstance = instances.get(taskName);
    if (existInstance != null) {
      if (!existInstance.isRunning()) {
        executorService.submit(existInstance::start);
      }
      return;
    }
    StreamTaskInstance instance =
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
  }

  public void drop(String taskName) {
    this.stop(taskName);
  }

  public void stop(String taskName) {
    StreamTaskInstance instance = instances.remove(taskName);
    if (instance != null) {
      instance.stop();
    }
    scheduler.cancelStreamTask(taskName);
  }

  public void dropAll() {
    for (StreamTaskInstance instance : instances.values()) {
      instance.stop();
    }
    instances.clear();
  }

  public void create(StreamTask task) {
    this.start(task);
  }

  public int getTaskNum() {
    return scheduler.getTaskCount();
  }

  public int getRunningTaskNum() {
    return scheduler.getRunningTaskCount();
  }
}
