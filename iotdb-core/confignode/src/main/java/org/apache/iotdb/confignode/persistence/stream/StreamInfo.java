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
import org.apache.iotdb.commons.stream.StreamTaskExpectedStatus;
import org.apache.iotdb.commons.stream.StreamTaskStatus;
import org.apache.iotdb.confignode.consensus.request.write.stream.CreateStreamPlan;
import org.apache.iotdb.confignode.consensus.request.write.stream.DropStreamPlan;
import org.apache.iotdb.confignode.consensus.request.write.stream.StartStreamPlan;
import org.apache.iotdb.confignode.consensus.request.write.stream.StopStreamPlan;
import org.apache.iotdb.confignode.consensus.request.write.stream.UpdateStreamEpochPlan;
import org.apache.iotdb.confignode.i18n.ManagerMessages;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class StreamInfo implements SnapshotProcessor {

  private static final Logger LOGGER = LoggerFactory.getLogger(StreamInfo.class);
  private static final String SNAPSHOT_FILENAME = "stream_info.bin";
  private static final int SNAPSHOT_MAGIC = (int) 0xA17E5A71L;
  private static final int SNAPSHOT_VERSION = 1;

  private final Map<String, StreamTask> streamTaskMap = new ConcurrentHashMap<>();
  private final Map<String, StreamTaskExpectedStatus> expectedStatusMap = new ConcurrentHashMap<>();
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

  public StreamInfo() {}

  public TSStatus addTask(StreamTask task) {
    lock.writeLock().lock();
    try {
      if (streamTaskMap.containsKey(task.getTaskName())) {
        return new TSStatus(TSStatusCode.STREAM_ALREADY_EXISTS.getStatusCode())
            .setMessage("Stream already exists: " + task.getTaskName());
      }
      task.setId(nextStreamId.getAndIncrement());
      if (task.getCreationTime() <= 0) {
        task.setCreationTime(System.currentTimeMillis());
      }
      task.setStatus(StreamTaskStatus.CREATED);
      if (task.getRunningOn() == null) {
        task.setRunningOn("");
      }
      if (task.getLastDownReason() == null) {
        task.setLastDownReason("");
      }
      streamTaskMap.put(task.getTaskName(), task);
      expectedStatusMap.put(task.getTaskName(), StreamTaskExpectedStatus.CREATED);
      LOGGER.info("Added stream task: {} with id {}", task.getTaskName(), task.getId());
      return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    } finally {
      lock.writeLock().unlock();
    }
  }

  public TSStatus removeTask(String taskName) {
    lock.writeLock().lock();
    try {
      final StreamTask removed = streamTaskMap.get(taskName);
      if (removed == null) {
        return new TSStatus(TSStatusCode.STREAM_NOT_EXIST.getStatusCode())
            .setMessage("Stream not found: " + taskName);
      }
      if (removed.getSource() != null) {
        try {
          removed.getSource().onRemoved(removed);
        } catch (IOException e) {
          LOGGER.warn("Failed to release source resource for stream task {}", taskName, e);
          return new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode())
              .setMessage("Failed to release source resource for stream task " + taskName);
        }
      }
      streamTaskMap.remove(taskName);
      expectedStatusMap.remove(taskName);
      LOGGER.info("Removed stream task: {}", taskName);
      return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    } finally {
      lock.writeLock().unlock();
    }
  }

  public StreamTask getTask(String taskName) {
    return streamTaskMap.get(taskName);
  }

  public List<StreamTask> getAllTasks() {
    return new ArrayList<>(streamTaskMap.values());
  }

  public StreamTaskExpectedStatus getExpectedStatus(final String taskName) {
    return expectedStatusMap.get(taskName);
  }

  public long countTasksByStatus(final StreamTaskStatus status) {
    return streamTaskMap.values().stream().filter(task -> task.getStatus() == status).count();
  }

  public long countTasksByRunningOn(final String runningOn) {
    return streamTaskMap.values().stream()
        .filter(task -> Objects.equals(task.getRunningOn(), runningOn))
        .count();
  }

  /** Apply a {@link CreateStreamPlan} from the consensus layer. */
  public TSStatus applyCreateStream(final CreateStreamPlan plan) {
    return addTask(plan.getStreamTask());
  }

  /** Apply a {@link DropStreamPlan} from the consensus layer. */
  public TSStatus applyDropStream(final DropStreamPlan plan) {
    return removeTask(plan.getStreamName());
  }

  /** Apply a {@link StartStreamPlan} from the consensus layer. */
  public TSStatus applyStartStream(final StartStreamPlan plan) {
    return updateExpectedStatus(
        plan.getStreamName(), StreamTaskExpectedStatus.RUNNING, plan.getEpoch());
  }

  /** Apply a {@link StopStreamPlan} from the consensus layer. */
  public TSStatus applyStopStream(final StopStreamPlan plan) {
    return updateExpectedStatus(
        plan.getStreamName(), StreamTaskExpectedStatus.STOPPED, plan.getEpoch());
  }

  /** Apply an {@link UpdateStreamEpochPlan} without changing the expected runtime status. */
  public TSStatus applyUpdateStreamEpoch(final UpdateStreamEpochPlan plan) {
    lock.writeLock().lock();
    try {
      final StreamTask task = streamTaskMap.get(plan.getStreamName());
      if (task == null) {
        return new TSStatus(TSStatusCode.STREAM_NOT_EXIST.getStatusCode())
            .setMessage(String.format(ManagerMessages.STREAM_NOT_FOUND, plan.getStreamName()));
      }
      task.advanceEpochToAtLeast(plan.getEpoch());
      return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    } finally {
      lock.writeLock().unlock();
    }
  }

  private TSStatus updateExpectedStatus(
      final String streamName, final StreamTaskExpectedStatus expectedStatus, final int epoch) {
    lock.writeLock().lock();
    try {
      final StreamTask task = streamTaskMap.get(streamName);
      if (task == null) {
        return new TSStatus(TSStatusCode.STREAM_NOT_EXIST.getStatusCode())
            .setMessage(String.format(ManagerMessages.STREAM_NOT_FOUND, streamName));
      }
      expectedStatusMap.put(streamName, expectedStatus);
      task.advanceEpochToAtLeast(epoch);
      switch (expectedStatus) {
        case RUNNING:
          if (task.getStatus() != StreamTaskStatus.RUNNING) {
            task.setStatus(StreamTaskStatus.UNKNOWN);
          }
          break;
        case STOPPED:
          if (task.getStatus() != StreamTaskStatus.STOPPED) {
            task.setStatus(StreamTaskStatus.UNKNOWN);
          }
          break;
        default:
          throw new IllegalStateException();
      }
      return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    } finally {
      lock.writeLock().unlock();
    }
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
      dos.writeInt(SNAPSHOT_MAGIC);
      dos.writeInt(SNAPSHOT_VERSION);
      dos.writeInt(streamTaskMap.size());
      for (final StreamTask task : streamTaskMap.values()) {
        final ByteBuffer taskBuffer = task.toByteBuffer();
        final byte[] taskBytes = new byte[taskBuffer.remaining()];
        taskBuffer.get(taskBytes);
        dos.writeInt(taskBytes.length);
        dos.write(taskBytes);
        dos.writeShort(expectedStatusMap.get(task.getTaskName()).ordinal());
      }
      dos.flush();
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
      LOGGER.info(
          ManagerMessages
              .LOG_STREAMINFO_SNAPSHOT_FILE_ARG_DOES_NOT_EXIST_WHICH_IS_EXPECTED_WHEN_UPGRADING_FROM_A_VERSION_WITHOUT_STREAMNODE_218B1A42,
          snapshotFile.getAbsolutePath());
      return;
    }

    writeLock();
    try (final FileInputStream fis = new FileInputStream(snapshotFile);
        final DataInputStream dis = new DataInputStream(new BufferedInputStream(fis))) {
      final Map<String, StreamTask> recoveredTasks = new ConcurrentHashMap<>();
      final Map<String, StreamTaskExpectedStatus> recoveredExpectedStatuses =
          new ConcurrentHashMap<>();
      final int snapshotMagic = dis.readInt();
      if (snapshotMagic != SNAPSHOT_MAGIC) {
        throw new IOException(
            String.format(
                ManagerMessages.EXCEPTION_INVALID_STREAMINFO_SNAPSHOT_MAGIC_ARG_6431D13A,
                snapshotMagic));
      }
      final int snapshotVersion = dis.readInt();
      if (snapshotVersion != SNAPSHOT_VERSION) {
        throw new IOException(
            String.format(
                ManagerMessages.EXCEPTION_UNSUPPORTED_STREAMINFO_SNAPSHOT_VERSION_ARG_F766EDEE,
                snapshotVersion));
      }
      final int count = dis.readInt();
      for (int i = 0; i < count; i++) {
        final int taskSize = dis.readInt();
        final byte[] taskBytes = new byte[taskSize];
        dis.readFully(taskBytes);
        final StreamTask task = StreamTask.deserialize(ByteBuffer.wrap(taskBytes));
        final short expectedStatusOrdinal = dis.readShort();
        if (expectedStatusOrdinal < 0
            || expectedStatusOrdinal >= StreamTaskExpectedStatus.values().length) {
          throw new IOException(
              String.format(
                  ManagerMessages
                      .EXCEPTION_INVALID_EXPECTED_STATUS_ORDINAL_IN_STREAMINFO_SNAPSHOT_ARG_43EC6A81,
                  expectedStatusOrdinal));
        }
        final StreamTaskExpectedStatus expectedStatus =
            StreamTaskExpectedStatus.values()[expectedStatusOrdinal];
        normalizeRuntimeStatusAfterRecovery(task, expectedStatus);
        task.setRunningOn("");
        task.setLastHeartbeatTime(0);
        task.setLastDownTime(0);
        task.setLastDownReason("");
        recoveredTasks.put(task.getTaskName(), task);
        recoveredExpectedStatuses.put(task.getTaskName(), expectedStatus);
      }
      streamTaskMap.clear();
      streamTaskMap.putAll(recoveredTasks);
      expectedStatusMap.clear();
      expectedStatusMap.putAll(recoveredExpectedStatuses);
      LOGGER.info(
          "Snapshot loaded for StreamInfo: {} tasks restored from {}",
          streamTaskMap.size(),
          snapshotFile.getAbsolutePath());
      nextStreamId.set(
          streamTaskMap.values().stream().mapToLong(StreamTask::getId).max().orElse(-1L) + 1);
    } finally {
      writeUnlock();
    }
  }

  private static void normalizeRuntimeStatusAfterRecovery(
      final StreamTask task, final StreamTaskExpectedStatus expectedStatus) {
    switch (expectedStatus) {
      case CREATED:
        task.setStatus(StreamTaskStatus.CREATED);
        break;
      case RUNNING:
      case STOPPED:
        task.setStatus(StreamTaskStatus.UNKNOWN);
        break;
      default:
        throw new IllegalStateException();
    }
  }
}
