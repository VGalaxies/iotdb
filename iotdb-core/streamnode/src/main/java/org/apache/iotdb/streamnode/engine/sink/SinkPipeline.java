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

package org.apache.iotdb.streamnode.engine.sink;

import org.apache.iotdb.commons.stream.IoTDBTarget;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.streamnode.engine.task.StreamSubTaskContext;
import org.apache.iotdb.streamnode.metrics.SinkPipelineMetrics;

import com.google.common.util.concurrent.ListenableFuture;

import javax.annotation.Nullable;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

/**
 * One pipeline per stream task. Entries are sharded inside the pipeline by {@code
 * floorMod(windowId, shardCount)} so that every entry of a window is handled by exactly one writer
 * thread, while different windows of the same pipeline may be written in parallel.
 *
 * <p>Batching is per {@code (pipeline, shard)}: each shard owns its own {@link SinkQueue} and
 * {@link BatchAccumulator}, so a flush always targets a single database/table.
 */
public class SinkPipeline {
  private final String pipelineId;
  private final IoTDBTarget iotdbTarget;
  private final SinkMemoryController memoryController;
  private final SinkQueue[] queues;
  private final BatchAccumulator[] accumulators;
  private final int shardCount;

  /**
   * Guards a shard's {@link BatchAccumulator} and the flush-vs-discard decision between the owning
   * writer thread and {@link #doStop()}. The writer holds it for the whole drain+flush, so stop can
   * never discard an entry that is being flushed, and a flush can never run after stop discarded
   * it. Reentrant because {@code processShard} holds it while calling {@code doFlush}.
   */
  private final ReentrantLock[] shardLocks;

  private @Nullable CommitTracker commitTracker;
  private @Nullable SinkPipelineMetrics metrics;
  private final AtomicReference<State> state = new AtomicReference<>(State.RUNNING);
  private final AtomicInteger activeTaskCount = new AtomicInteger(0);
  private final AtomicInteger subTaskId = new AtomicInteger(0);
  private final GlobalSinkWriterPool writerPool;

  private enum State {
    RUNNING,
    STOPPING,
    STOPPED
  }

  public SinkPipeline(
      String pipelineId,
      IoTDBTarget iotdbTarget,
      SinkPipelineConfig config,
      GlobalSinkWriterPool writerPool) {
    this(
        pipelineId,
        iotdbTarget,
        config,
        writerPool,
        new SinkMemoryController(
            config.getPipelineMemoryLimitBytes(), GlobalSinkMemoryController.getInstance()));
  }

  @TestOnly
  SinkPipeline(
      String pipelineId,
      IoTDBTarget iotdbTarget,
      SinkPipelineConfig config,
      GlobalSinkWriterPool writerPool,
      SinkMemoryController memoryController) {
    this.pipelineId = pipelineId;
    this.iotdbTarget = iotdbTarget;
    this.memoryController = memoryController;
    this.shardCount = writerPool.getShardCount();
    this.queues = new SinkQueue[shardCount];
    this.accumulators = new BatchAccumulator[shardCount];
    this.shardLocks = new ReentrantLock[shardCount];
    for (int shard = 0; shard < shardCount; shard++) {
      this.queues[shard] = new SinkQueue(memoryController);
      this.queues[shard].setBackpressureListener(this::onBackpressure);
      this.shardLocks[shard] = new ReentrantLock();
      this.accumulators[shard] =
          new BatchAccumulator(
              iotdbTarget.getTableName(),
              iotdbTarget.getColumnNames(),
              iotdbTarget.getColumnDataTypes(),
              iotdbTarget.getColumnCategories(),
              iotdbTarget.getColumnIndex2TsBlockColumnIndexList(),
              config);
    }
    this.writerPool = writerPool;
    writerPool.register(this);
  }

  private void onBackpressure() {
    SinkPipelineMetrics sinkMetrics = metrics;
    if (sinkMetrics != null) {
      sinkMetrics.recordBackpressure();
    }
  }

  public String getPipelineId() {
    return pipelineId;
  }

  public int getShardCount() {
    return shardCount;
  }

  /** Maps a window to its owning writer thread; {@code windowId < 0} maps to a valid shard too. */
  public int shardOf(long windowId) {
    return (int) Math.floorMod(windowId, (long) shardCount);
  }

  public SinkQueue getQueue(int shard) {
    return queues[shard];
  }

  public BatchAccumulator getAccumulator(int shard) {
    return accumulators[shard];
  }

  /** Lock that serializes writes and stop-cleanup for one shard; used by the writer pool. */
  public ReentrantLock getShardLock(int shard) {
    return shardLocks[shard];
  }

  public SinkMemoryController getMemoryController() {
    return memoryController;
  }

  public CommitTracker getCommitTracker() {
    return commitTracker;
  }

  public void setCommitTracker(CommitTracker commitTracker) {
    this.commitTracker = commitTracker;
  }

  public void setMetrics(@Nullable SinkPipelineMetrics metrics) {
    this.metrics = metrics;
  }

  @Nullable
  public SinkPipelineMetrics getMetrics() {
    return metrics;
  }

  public long getPipelineUsedMemoryBytes() {
    return memoryController.getUsedBytes();
  }

  /**
   * Whether this pipeline is currently blocking pushes with backpressure: entries are parked in a
   * shard queue because the pipeline quota or the shared write-back pool rejected their
   * reservation. Parked entries are the ground truth of "blocked now" — they drain automatically
   * once memory is admitted, so this signal needs no separate clearing logic.
   */
  public boolean isBackpressureActive() {
    for (final SinkQueue queue : queues) {
      if (queue.hasPendingPushes()) {
        return true;
      }
    }
    return false;
  }

  public long getCommittedId() {
    CommitTracker tracker = commitTracker;
    return tracker == null ? 0L : tracker.getCommittedId();
  }

  public IoTDBTarget getIotdbTarget() {
    return iotdbTarget;
  }

  public boolean isRunning() {
    return state.get() == State.RUNNING;
  }

  public StreamSinkTask createSinkTask(StreamSubTaskContext subTaskContext) {
    activeTaskCount.incrementAndGet();
    return new StreamSinkTask(this, subTaskId.getAndIncrement(), subTaskContext);
  }

  ListenableFuture<Void> push(SinkEntry entry) {
    final int shard = shardOf(entry.getWindowId());
    ListenableFuture<Void> future = queues[shard].enqueue(entry);
    SinkPipelineMetrics sinkMetrics = metrics;
    if (sinkMetrics != null) {
      sinkMetrics.recordPush();
    }
    writerPool.notifyShard(this, shard);
    return future;
  }

  void onTaskStopped() {
    if (activeTaskCount.decrementAndGet() == 0 && state.get() == State.STOPPING) {
      doStop();
    }
  }

  public void stop() {
    if (!state.compareAndSet(State.RUNNING, State.STOPPING)) {
      return;
    }
    if (activeTaskCount.get() == 0) {
      doStop();
    }
  }

  private void doStop() {
    // Claim the transition so a racing stop()/onTaskStopped() cannot clean up twice.
    if (!state.compareAndSet(State.STOPPING, State.STOPPED)) {
      return;
    }
    writerPool.unregister(this);
    // For each shard, take the writer's shard lock so a flush already in progress finishes first;
    // everything still buffered afterwards is released and dropped.
    long releasedBytes = 0L;
    for (int shard = 0; shard < shardCount; shard++) {
      ReentrantLock lock = shardLocks[shard];
      lock.lock();
      try {
        releasedBytes += discardShard(shard);
      } finally {
        lock.unlock();
      }
    }
    // The tracker is task-scoped and this task is stopping: clear every window from commit gating
    // in one shot instead of settling each discarded entry. Nothing can advance the commit
    // afterwards, and clearing never advances it, so no unwritten data can be committed.
    CommitTracker tracker = commitTracker;
    if (tracker != null) {
      tracker.clearWindowStates();
    }
    // Outside shard locks: returned global memory must wake other pipelines that parked only
    // because this pipeline held the shared pool (same protocol as flush success/failure).
    if (releasedBytes > 0) {
      writerPool.notifyMemoryReleased();
    }
  }

  /**
   * Releases the memory of one shard's buffered entries. Must be called while holding its lock.
   *
   * @return bytes successfully returned to the memory controllers
   */
  private long discardShard(int shard) {
    // Rejects parked entries (never reserved memory) and fails their futures.
    queues[shard].close();
    long releasedBytes = 0L;
    SinkEntry entry;
    while ((entry = queues[shard].poll()) != null) {
      long bytes = entry.getMemorySizeInBytes();
      if (memoryController.release(bytes)) {
        releasedBytes += bytes;
      }
    }
    long accumulatorMemory = 0;
    for (SinkEntry discarded : accumulators[shard].discardAndReset()) {
      accumulatorMemory += discarded.getMemorySizeInBytes();
    }
    if (accumulatorMemory > 0 && memoryController.release(accumulatorMemory)) {
      releasedBytes += accumulatorMemory;
    }
    return releasedBytes;
  }
}
