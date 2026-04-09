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

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.stream.StreamTask;
import org.apache.iotdb.commons.stream.StreamTaskStatus;
import org.apache.iotdb.confignode.manager.IManager;
import org.apache.iotdb.confignode.persistence.stream.StreamInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class StreamManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(StreamManager.class);

  private final IManager configManager;
  private final StreamInfo streamInfo;

  public StreamManager(IManager configManager, StreamInfo streamInfo) {
    this.configManager = configManager;
    this.streamInfo = streamInfo;
  }

  public TSStatus createStream(StreamTask task) {
    LOGGER.info("Creating stream task: {}", task.getTaskName());
    return streamInfo.addTask(task);
  }

  public TSStatus dropStream(String database, String streamName) {
    LOGGER.info("Dropping stream: {}.{}", database, streamName);
    String taskName = database + "." + streamName;
    return streamInfo.removeTask(taskName);
  }

  public TSStatus startStream(String database, String streamName) {
    LOGGER.info("Starting stream: {}.{}", database, streamName);
    String taskName = database + "." + streamName;
    return streamInfo.updateTaskStatus(taskName, StreamTaskStatus.RUNNING);
  }

  public TSStatus stopStream(String database, String streamName) {
    LOGGER.info("Stopping stream: {}.{}", database, streamName);
    String taskName = database + "." + streamName;
    return streamInfo.updateTaskStatus(taskName, StreamTaskStatus.STOPPED);
  }

  public List<StreamTask> showStreams() {
    return streamInfo.getAllTasks();
  }

  public List<StreamTask> showStreams(String database) {
    // TODO: filter by database
    return streamInfo.getAllTasks();
  }
}
