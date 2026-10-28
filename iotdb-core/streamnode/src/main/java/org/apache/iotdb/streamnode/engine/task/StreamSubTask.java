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
import org.apache.iotdb.commons.stream.StreamWindow;
import org.apache.iotdb.streamnode.engine.computation.IStreamComputeTask;
import org.apache.iotdb.streamnode.engine.sink.IStreamSinkTask;
import org.apache.iotdb.streamnode.engine.window.WindowEngine;

import org.apache.tsfile.write.record.Tablet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

public class StreamSubTask {

  private static final Logger LOGGER = LoggerFactory.getLogger(StreamSubTask.class);
  private final String streamName;
  private final PartitionKey partitionKey;
  private final AtomicLong lastCommitId = new AtomicLong(-1);
  private final StreamSubTaskContext context;

  private final WindowEngine windowEngine;
  private final IStreamComputeTask computeTask;
  private final IStreamComputeTask recomputeTask;
  private final IStreamSinkTask sinkTask;

  private StreamDataConsumer consumer;

  public StreamSubTask(
      PartitionKey partitionKey,
      WindowEngine windowEngine,
      IStreamComputeTask computeTask,
      IStreamComputeTask recomputeTask,
      IStreamSinkTask sinkTask,
      String streamName,
      StreamSubTaskContext context) {
    this.partitionKey = partitionKey;
    this.windowEngine = windowEngine;
    this.computeTask = computeTask;
    this.recomputeTask = recomputeTask;
    this.sinkTask = sinkTask;
    this.streamName = streamName;
    this.context = context;
    this.consumer = dataSlices -> CompletableFuture.completedFuture(null);
  }

  public void setConsumer(StreamDataConsumer consumer) {
    this.consumer =
        consumer == null ? dataSlices -> CompletableFuture.completedFuture(null) : consumer;
  }

  public StreamSubTask(PartitionKey partitionKey, StreamWindow window) {
    this(partitionKey, WindowEngine.create(window, null, null, null), null, null, null, "", null);
  }

  public String getStreamName() {
    return streamName;
  }

  public Future<?> offer(List<DataSlice> dataSlices) {
    if (dataSlices == null || dataSlices.isEmpty()) {
      return CompletableFuture.completedFuture(null);
    }
    long commitId = -1;
    for (DataSlice slice : dataSlices) {
      commitId = Math.max(commitId, slice.getTabletId());
    }
    final long maxCommitId = commitId;
    lastCommitId.updateAndGet(lastCommittedId -> Math.max(lastCommittedId, maxCommitId));
    return consumer.accept(dataSlices);
  }

  public long getCommitId() {
    return lastCommitId.get();
  }

  public PartitionKey getPartitionKey() {
    return partitionKey;
  }

  public WindowEngine getWindowEngine() {
    return windowEngine;
  }

  public IStreamComputeTask getComputeTask() {
    return computeTask;
  }

  public IStreamComputeTask getRecomputeTask() {
    return recomputeTask;
  }

  public IStreamSinkTask getSink() {
    return sinkTask;
  }

  public StreamSubTaskStateMachine getStateMachine() {
    return context.getStateMachine();
  }

  public StreamSubTaskContext getContext() {
    return context;
  }

  public static class DataSlice {

    private final PartitionKey partitionKey;
    private final Tablet tablet;
    private final int startRow;
    private final int endRow;
    private final long tabletId;

    public DataSlice(
        final PartitionKey partitionKey,
        final Tablet tablet,
        final int startRow,
        final int endRow,
        final long tabletId) {
      this.partitionKey = partitionKey;
      this.tablet = tablet;
      this.startRow = startRow;
      this.endRow = endRow;
      this.tabletId = tabletId;
    }

    public PartitionKey getPartitionKey() {
      return partitionKey;
    }

    public Tablet getTablet() {
      return tablet;
    }

    public int getStartRow() {
      return startRow;
    }

    public int getEndRow() {
      return endRow;
    }

    public long getTabletId() {
      return tabletId;
    }
  }
}
