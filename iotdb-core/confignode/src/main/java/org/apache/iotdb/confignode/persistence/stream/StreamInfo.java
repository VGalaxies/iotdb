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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.snapshot.SnapshotProcessor;
import org.apache.iotdb.commons.stream.StreamTask;
import org.apache.iotdb.commons.stream.StreamTaskStatus;
import org.apache.iotdb.confignode.consensus.request.write.stream.CreateStreamPlan;
import org.apache.iotdb.confignode.consensus.request.write.stream.DropStreamPlan;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
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
  private static final String SNAPSHOT_FILENAME = "stream_info.bin";

  private final Map<String, StreamTask> streamTaskMap = new ConcurrentHashMap<>();
  private final AtomicLong nextStreamId = new AtomicLong(0);
  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

  // ==================== Lock interface ====================

  public void readLock() {
    lock.readLock().lock();
  }

  public void readUnlock() {
    lock.readLock().unlock();
  }

  public void writeLock() {
    lock.writeLock().lock();
  }

  public void writeUnlock() {
    lock.writeLock().unlock();
  }

  public StreamInfo() {
  }

  public TSStatus addTask(StreamTask task) {
    lock.writeLock().lock();
    try {
      if (streamTaskMap.containsKey(task.getTaskName())) {
        return new TSStatus(TSStatusCode.STREAM_ALREADY_EXISTS.getStatusCode())
            .setMessage("Stream already exists: " + task.getTaskName());
      }
      task.setId(nextStreamId.getAndIncrement());
      task.setStatus(StreamTaskStatus.UNKNOWN);
      streamTaskMap.put(task.getTaskName(), task);
      LOGGER.info("Added stream task: {} with id {}", task.getTaskName(), task.getId());
      return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    } finally {
      lock.writeLock().unlock();
    }
  }

  public TSStatus removeTask(String taskName) {
    StreamTask removed = streamTaskMap.remove(taskName);
    if (removed == null) {
      // STREAM_NOT_EXIST does not exist in TSStatusCode, use EXECUTE_STATEMENT_ERROR
      return new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode())
          .setMessage("Stream not found: " + taskName);
    }
    LOGGER.info("Removed stream task: {}", taskName);
    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }

  public StreamTask getTask(String taskName) {
    return streamTaskMap.get(taskName);
  }

  public List<StreamTask> getAllTasks() {
    return new ArrayList<>(streamTaskMap.values());
  }

  /** Apply a {@link CreateStreamPlan} from the consensus layer. */
  public TSStatus applyCreateStream(final CreateStreamPlan plan) {
    return addTask(plan.getStreamTask());
  }

  /** Apply a {@link DropStreamPlan} from the consensus layer. */
  public TSStatus applyDropStream(final DropStreamPlan plan) {
    return removeTask(plan.getStreamName());
  }

  @Override
  public boolean processTakeSnapshot(File snapshotDir) throws IOException {
    final File snapshotFile = new File(snapshotDir, SNAPSHOT_FILENAME);
    if (snapshotFile.exists() && snapshotFile.isFile()) {
      LOGGER.error(
          "Failed to take snapshot, because snapshot file [{}] already exists.",
          snapshotFile.getAbsolutePath());
      return false;
    }

    readLock();
    try (final FileOutputStream fos = new FileOutputStream(snapshotFile);
        final DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(fos))) {
      dos.writeInt(streamTaskMap.size());
      for (final StreamTask task : streamTaskMap.values()) {
        task.serialize(dos);
      }
      fos.getFD().sync();
      LOGGER.info(
          "Snapshot taken for StreamInfo: {} tasks written to {}",
          streamTaskMap.size(),
          snapshotFile.getAbsolutePath());
      return true;
    } finally {
      readUnlock();
    }
  }

  @Override
  public void processLoadSnapshot(File snapshotDir) throws IOException {
    final File snapshotFile = new File(snapshotDir, SNAPSHOT_FILENAME);
    if (!snapshotFile.exists() || !snapshotFile.isFile()) {
      LOGGER.error(
          "Failed to load snapshot, snapshot file [{}] does not exist.",
          snapshotFile.getAbsolutePath());
      return;
    }

    writeLock();
    try (final FileInputStream fis = new FileInputStream(snapshotFile);
        final DataInputStream dis = new DataInputStream(new BufferedInputStream(fis))) {
      streamTaskMap.clear();
      final int count = dis.readInt();
      for (int i = 0; i < count; i++) {
        final StreamTask task = StreamTask.deserialize(dis);
        streamTaskMap.put(task.getTaskName(), task);
      }
      LOGGER.info(
          "Snapshot loaded for StreamInfo: {} tasks restored from {}",
          streamTaskMap.size(),
          snapshotFile.getAbsolutePath());
      nextStreamId.set(streamTaskMap.values().stream().mapToLong(StreamTask::getId).max().orElse(0));
    } finally {
      writeUnlock();
    }
  }
}
