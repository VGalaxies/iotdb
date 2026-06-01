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

import com.google.common.util.concurrent.ListenableFuture;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class SinkPipeline {
  private final String pipelineId;
  private final IoTDBTarget iotdbTarget;
  private final SinkQueue queue;
  private final SinkMemoryController memoryController;
  private final BatchAccumulator accumulator;
  private final CommitTracker commitTracker;
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
      GlobalSinkMemoryController globalMemCtrl,
      GlobalSinkWriterPool writerPool,
      SourceCommitCallback commitCallback) {
    this.pipelineId = pipelineId;
    this.iotdbTarget = iotdbTarget;
    this.memoryController =
        new SinkMemoryController(config.getPipelineMemoryLimitBytes(), globalMemCtrl);
    this.queue = new SinkQueue(memoryController);
    this.accumulator =
        new BatchAccumulator(
            iotdbTarget.getTableName(),
            iotdbTarget.getColumnNames(),
            iotdbTarget.getColumnDataTypes(),
            iotdbTarget.getColumnCategories(),
            config);
    this.commitTracker = new CommitTracker(commitCallback);
    this.writerPool = writerPool;
    writerPool.register(this);
  }

  public String getPipelineId() {
    return pipelineId;
  }

  public SinkQueue getQueue() {
    return queue;
  }

  public SinkMemoryController getMemoryController() {
    return memoryController;
  }

  public BatchAccumulator getAccumulator() {
    return accumulator;
  }

  public CommitTracker getCommitTracker() {
    return commitTracker;
  }

  public IoTDBTarget getIotdbTarget() {
    return iotdbTarget;
  }

  public boolean isRunning() {
    return state.get() == State.RUNNING;
  }

  /**
   * Check if this pipeline has work that needs immediate processing. Returns true when: Queue has
   * data to drain, OR Accumulator has data that needs to be flushed by time (linger timeout)
   *
   * @return true if there is pending work that needs immediate processing
   */
  public boolean needsImmediateProcessing() {
    return queue.hasData() || accumulator.shouldFlushByTime();
  }

  public int drainTo(List<SinkEntry> target, int maxElements) {
    return queue.drainTo(target, maxElements);
  }

  public StreamSinkTask createSinkTask() {
    activeTaskCount.incrementAndGet();
    return new StreamSinkTask(this, subTaskId.getAndIncrement());
  }

  ListenableFuture<Void> push(SinkEntry entry) {
    ListenableFuture<Void> future = queue.enqueue(entry);
    writerPool.notifyDataAvailable(this);
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
    queue.close();
    writerPool.unregister(this);
    cleanup();
    state.set(State.STOPPED);
  }

  private void cleanup() {
    SinkEntry entry;
    while ((entry = queue.poll()) != null) {
      memoryController.release(entry.getMemorySizeInBytes());
    }
    long accumulatorMemory = accumulator.discardAndReset();
    if (accumulatorMemory > 0) {
      memoryController.release(accumulatorMemory);
    }
  }
}
