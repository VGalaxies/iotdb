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

package org.apache.iotdb.confignode.persistence.stream;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.snapshot.SnapshotProcessor;
import org.apache.iotdb.commons.stream.StreamTask;
import org.apache.iotdb.commons.stream.StreamTaskStatus;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class StreamInfo implements SnapshotProcessor {

  private static final Logger LOGGER = LoggerFactory.getLogger(StreamInfo.class);

  private final Map<String, StreamTask> streamTaskMap = new ConcurrentHashMap<>();
  private final AtomicLong nextStreamId = new AtomicLong(0);

  public StreamInfo() {
    // Load existing tasks from streams directory
    File streamsDir = new File(ConfigNodeDescriptor.getInstance().getConf().getStreamsDir());
    if (streamsDir.exists() && streamsDir.isDirectory()) {
      try {
        processLoadSnapshot(streamsDir);
        LOGGER.info("Loaded existing StreamTasks from {}", streamsDir.getAbsolutePath());
      } catch (IOException e) {
        LOGGER.error("Failed to load existing StreamTasks from {}", streamsDir.getAbsolutePath(), e);
      }
    }
  }

  public TSStatus addTask(StreamTask task) {
    if (streamTaskMap.containsKey(task.getTaskName())) {
      return new TSStatus(TSStatusCode.STREAM_ALREADY_EXISTS.getStatusCode())
          .setMessage("Stream already exists: " + task.getTaskName());
    }
    task.setId(nextStreamId.getAndIncrement());
    task.setStatus(StreamTaskStatus.CREATED);
    // Serialize the task to streams directory
    File streamsDir = new File(ConfigNodeDescriptor.getInstance().getConf().getStreamsDir());
    if (!streamsDir.exists()) {
      streamsDir.mkdirs();
    }
    File taskFile = new File(streamsDir, task.getId() + ".stm");
    try (FileOutputStream fos = new FileOutputStream(taskFile);
         BufferedOutputStream bos = new BufferedOutputStream(fos)) {
      task.serialize(bos);
      bos.flush();
      fos.getFD().sync();
      LOGGER.info("Serialized StreamTask {} to {}", task.getTaskName(), taskFile.getAbsolutePath());
    } catch (IOException e) {
      LOGGER.error("Failed to serialize StreamTask {}", task.getTaskName(), e);
      return new TSStatus(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode())
          .setMessage("Failed to serialize StreamTask: " + task.getTaskName());
    }
    streamTaskMap.put(task.getTaskName(), task);
    LOGGER.info("Added stream task: {} with id {}", task.getTaskName(), task.getId());
    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }

  public TSStatus removeTask(String taskName) {
    StreamTask removed = streamTaskMap.remove(taskName);
    if (removed == null) {
      // STREAM_NOT_EXIST does not exist in TSStatusCode, use EXECUTE_STATEMENT_ERROR
      return new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode())
          .setMessage("Stream not found: " + taskName);
    }
    // Delete the task file from disk
    File streamsDir = new File(ConfigNodeDescriptor.getInstance().getConf().getStreamsDir());
    File taskFile = new File(streamsDir, removed.getId() + ".stm");
    if (taskFile.exists()) {
      if (!taskFile.delete()) {
        LOGGER.warn("Failed to delete task file: {}", taskFile.getAbsolutePath());
      }
    }
    LOGGER.info("Removed stream task: {}", taskName);
    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }

  public TSStatus updateTaskStatus(String taskName, StreamTaskStatus status) {
    StreamTask task = streamTaskMap.get(taskName);
    if (task == null) {
      // STREAM_NOT_EXIST does not exist in TSStatusCode, use EXECUTE_STATEMENT_ERROR
      return new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode())
          .setMessage("Stream not found: " + taskName);
    }
    task.setStatus(status);
    LOGGER.info("Updated stream task {} status to {}", taskName, status);
    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }

  public StreamTask getTask(String taskName) {
    return streamTaskMap.get(taskName);
  }

  public List<StreamTask> getAllTasks() {
    return new ArrayList<>(streamTaskMap.values());
  }

  @Override
  public boolean processTakeSnapshot(File snapshotDir) throws IOException {
    LOGGER.info("Taking snapshot for StreamInfo to {}", snapshotDir.getAbsolutePath());
    // Serialize each task
    for (StreamTask task : streamTaskMap.values()) {
      File taskFile = new File(snapshotDir, task.getId() + ".stm");
      try (FileOutputStream fos = new FileOutputStream(taskFile);
           BufferedOutputStream bos = new BufferedOutputStream(fos)) {
        task.serialize(bos);
        bos.flush();
        fos.getFD().sync();
      }
    }
    LOGGER.info("Snapshot taken for StreamInfo with {} tasks", streamTaskMap.size());
    return true;
  }

  @Override
  public void processLoadSnapshot(File snapshotDir) throws IOException {
    LOGGER.info("Loading snapshot for StreamInfo from {}", snapshotDir.getAbsolutePath());
    // Clear existing map
    streamTaskMap.clear();
    long maxId = -1;
    // Read all .stm files
    File[] files = snapshotDir.listFiles((dir, name) -> name.endsWith(".stm"));
    if (files != null) {
      for (File file : files) {
        try (FileInputStream fis = new FileInputStream(file);
             BufferedInputStream bis = new BufferedInputStream(fis)) {
          StreamTask task = StreamTask.deserialize(bis);
          streamTaskMap.put(task.getTaskName(), task);
          if (task.getId() > maxId) {
            maxId = task.getId();
          }
        } catch (IOException e) {
          LOGGER.error("Failed to deserialize StreamTask from {}", file.getAbsolutePath(), e);
        }
      }
    }
    // Set nextStreamId
    nextStreamId.set(maxId + 1);
    LOGGER.info("Loaded snapshot for StreamInfo with {} tasks, nextStreamId set to {}", streamTaskMap.size(), nextStreamId.get());
  }
}