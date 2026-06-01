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
import org.apache.iotdb.commons.stream.StreamSource;
import org.apache.iotdb.commons.stream.StreamTask;
import org.apache.iotdb.streamnode.conf.StreamNodeConfig;
import org.apache.iotdb.streamnode.conf.StreamNodeDescriptor;
import org.apache.iotdb.streamnode.engine.computation.ComputationEngine;
import org.apache.iotdb.streamnode.engine.dispatcher.TabletDispatcher;
import org.apache.iotdb.streamnode.engine.scheduler.task.IStreamDriver;
import org.apache.iotdb.streamnode.engine.scheduler.task.StreamDriverFactory;
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
  private final WriteBackEngine writeBackEngine;
  private final AtomicBoolean running = new AtomicBoolean(false);
  private final StreamDataConsumer subTaskConsumer;
  private final Map<PartitionKey, IStreamDriver> streamDriverMap = new ConcurrentHashMap<>();
  private final StreamNodeConfig nodeConfig = StreamNodeDescriptor.getInstance().getConfig();

  public StreamTaskInstance(StreamTask taskDefinition, StreamDataConsumer subTaskConsumer) {
    this.taskDefinition = taskDefinition;
    this.subTaskConsumer = subTaskConsumer;
    this.writeBackEngine = new WriteBackEngine(taskDefinition);
  }

  public boolean isStreamDriverExists(PartitionKey partitionKey) {
    return streamDriverMap.containsKey(partitionKey);
  }

  public IStreamDriver getOrCreateStreamDriver(PartitionKey partitionKey) {
    return streamDriverMap.computeIfAbsent(
        partitionKey,
        key ->
            StreamDriverFactory.createDriver(
                partitionKey, taskDefinition.getTaskName(), subTasks.get(partitionKey)));
  }

  public void start() {
    if (!running.compareAndSet(false, true)) {
      LOGGER.warn("Task {} is already running", taskDefinition.getTaskName());
      return;
    }

    try {
      // Initialize write-back
      if (taskDefinition.getTarget() != null) {
        writeBackEngine.start();
      }

      // Initialize dispatcher
      if (taskDefinition.getSource() != null) {
        dispatcher = createDispatcher(taskDefinition.getSource());
      }

      // Initialize source
      if (taskDefinition.getSource() != null) {
        sourceInstance =
            createSourceInstance(
                taskDefinition.getSource(),
                taskDefinition.getTaskName(),
                dispatcher::dispatch,
                nodeConfig);
        sourceInstance.start();
      }

      LOGGER.info("Task instance started: {}", taskDefinition.getTaskName());
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

  TabletDispatcher createDispatcher(StreamSource source) {
    return TabletDispatcher.create(source, this::getOrCreateSubTask);
  }

  StreamSourceInstance createSourceInstance(
      StreamSource source,
      String taskName,
      java.util.function.BiConsumer<org.apache.tsfile.write.record.Tablet, Long> consumer,
      StreamNodeConfig config) {
    return StreamSourceInstance.create(source, taskName, consumer, config);
  }

  public StreamSubTask getOrCreateSubTask(PartitionKey key) {
    return subTasks.computeIfAbsent(
        key,
        partitionKey -> {
          LOGGER.debug("Creating sub-task for partition: {}", partitionKey);
          return new StreamSubTask(
              partitionKey,
              taskDefinition.getWindow(),
              subTaskConsumer,
              computationEngine,
              writeBackEngine,
              taskDefinition.getTaskName());
        });
  }

  public StreamTask getTaskDefinition() {
    return taskDefinition;
  }

  public boolean isRunning() {
    return running.get();
  }
}
