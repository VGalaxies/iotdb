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

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class StreamInfo implements SnapshotProcessor {

  private static final Logger LOGGER = LoggerFactory.getLogger(StreamInfo.class);
  private static final String SNAPSHOT_FILE_NAME = "stream_info.bin";

  private final Map<String, StreamTask> streamTaskMap = new ConcurrentHashMap<>();
  private final AtomicLong nextStreamId = new AtomicLong(0);
  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

  public TSStatus addTask(StreamTask task) {
    lock.writeLock().lock();
    try {
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
    } finally {
      lock.writeLock().unlock();
    }
  }

  public TSStatus removeTask(String taskName) {
    lock.writeLock().lock();
    try {
      StreamTask removed = streamTaskMap.remove(taskName);
      if (removed == null) {
        // STREAM_NOT_EXIST does not exist in TSStatusCode, use EXECUTE_STATEMENT_ERROR
        return new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode())
            .setMessage("Stream not found: " + taskName);
      }
      LOGGER.info("Removed stream task: {}", taskName);
      return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    } finally {
      lock.writeLock().unlock();
    }
  }

  public TSStatus updateTaskStatus(String taskName, StreamTaskStatus status) {
    lock.writeLock().lock();
    try {
      StreamTask task = streamTaskMap.get(taskName);
      if (task == null) {
        // STREAM_NOT_EXIST does not exist in TSStatusCode, use EXECUTE_STATEMENT_ERROR
        return new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode())
            .setMessage("Stream not found: " + taskName);
      }
      task.setStatus(status);
      LOGGER.info("Updated stream task {} status to {}", taskName, status);
      return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    } finally {
      lock.writeLock().unlock();
    }
  }

  public StreamTask getTask(String taskName) {
    lock.readLock().lock();
    try {
      return streamTaskMap.get(taskName);
    } finally {
      lock.readLock().unlock();
    }
  }

  public List<StreamTask> getAllTasks() {
    lock.readLock().lock();
    try {
      return new ArrayList<>(streamTaskMap.values());
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public boolean processTakeSnapshot(File snapshotDir) throws IOException {
    // TODO: implement snapshot serialization
    LOGGER.info("Taking snapshot for StreamInfo");
    return true;
  }

  @Override
  public void processLoadSnapshot(File snapshotDir) throws IOException {
    // TODO: implement snapshot deserialization
    LOGGER.info("Loading snapshot for StreamInfo");
  }
}