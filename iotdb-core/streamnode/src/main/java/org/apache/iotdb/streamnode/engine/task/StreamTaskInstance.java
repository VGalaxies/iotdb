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

import org.apache.iotdb.commons.stream.PartitionKey;
import org.apache.iotdb.commons.stream.StreamTask;
import org.apache.iotdb.streamnode.conf.StreamNodeConfig;
import org.apache.iotdb.streamnode.conf.StreamNodeDescriptor;
import org.apache.iotdb.streamnode.engine.computation.ComputationEngine;
import org.apache.iotdb.streamnode.engine.dispatcher.TabletDispatcher;
import org.apache.iotdb.streamnode.engine.sink.WriteBackEngine;
import org.apache.iotdb.streamnode.engine.source.StreamSourceInstance;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

public class StreamTaskInstance {

  private static final Logger LOGGER = LoggerFactory.getLogger(StreamTaskInstance.class);

  private final StreamTask taskDefinition;
  private StreamSourceInstance sourceInstance;
  private TabletDispatcher dispatcher;
  private final Map<PartitionKey, StreamSubTask> subTasks = new ConcurrentHashMap<>();
  private final ComputationEngine computationEngine = new ComputationEngine();
  private final WriteBackEngine writeBackEngine = new WriteBackEngine();
  private final AtomicBoolean running = new AtomicBoolean(false);
  // TODO: may try to reduce singleton pattern
  private StreamNodeConfig nodeConfig = StreamNodeDescriptor.getInstance().getConfig();

  public StreamTaskInstance(StreamTask taskDefinition) {
    this.taskDefinition = taskDefinition;
  }

  public void start() {
    if (!running.compareAndSet(false, true)) {
      LOGGER.warn("Task {} is already running", taskDefinition.getTaskName());
      return;
    }

    try {
      // Initialize write-back
      if (taskDefinition.getTarget() != null) {
        writeBackEngine.start(taskDefinition.getTarget());
      }

      // Initialize dispatcher
      if (taskDefinition.getSource() != null) {
        dispatcher = TabletDispatcher.create(taskDefinition.getSource(), partitionKey -> subTasks.computeIfAbsent(partitionKey, pk -> new StreamSubTask(pk, taskDefinition.getWindow())));
      }

      // Initialize source
      if (taskDefinition.getSource() != null) {
        sourceInstance = StreamSourceInstance.create(taskDefinition.getSource(), taskDefinition.getTaskName(), dispatcher::dispatch, nodeConfig);
        sourceInstance.start();
      }

      LOGGER.info("Task instance started: {}", taskDefinition.getTaskName());

      // TODO: Start process loop in a separate thread
    } catch (Exception e) {
      LOGGER.error("Failed to start task instance: {}", taskDefinition.getTaskName(), e);
      running.set(false);
    }
  }

  public void stop() {
    if (!running.compareAndSet(true, false)) {
      return;
    }

    try {
      // Stop source
      if (sourceInstance != null) {
        sourceInstance.stop();
      }
      // Stop write-back
      writeBackEngine.stop();

      // Clear sub-tasks
      subTasks.clear();

      LOGGER.info("Task instance stopped: {}", taskDefinition.getTaskName());
    } catch (Exception e) {
      LOGGER.error("Error stopping task instance: {}", taskDefinition.getTaskName(), e);
    }
  }

  public StreamSubTask getOrCreateSubTask(PartitionKey key) {
    return subTasks.computeIfAbsent(
        key,
        k -> {
          LOGGER.debug("Creating sub-task for partition: {}", k);
          return new StreamSubTask(k, taskDefinition.getWindow());
        });
  }

  public StreamTask getTaskDefinition() {
    return taskDefinition;
  }

  public boolean isRunning() {
    return running.get();
  }
}
