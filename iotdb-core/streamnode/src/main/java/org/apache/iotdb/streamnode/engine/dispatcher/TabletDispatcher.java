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

package org.apache.iotdb.streamnode.engine.dispatcher;

import org.apache.iotdb.commons.stream.IoTDBSubscriptionSource;
import org.apache.iotdb.commons.stream.PartitionKey;
import org.apache.iotdb.commons.stream.StreamSource;
import org.apache.iotdb.streamnode.engine.sink.CommitTracker;
import org.apache.iotdb.streamnode.engine.task.StreamSubTask.DataSlice;

import org.apache.tsfile.write.record.Tablet;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.BiFunction;

public abstract class TabletDispatcher {

  private final BiFunction<PartitionKey, List<DataSlice>, Future<?>> dataApplier;
  protected final CommitTracker commitTracker;

  protected TabletDispatcher(
      BiFunction<PartitionKey, List<DataSlice>, Future<?>> dataApplier,
      CommitTracker commitTracker) {
    this.dataApplier = dataApplier;
    this.commitTracker = commitTracker;
  }

  /**
   * Dispatch a data batch to sub-tasks based on partition keys. In iteration 1, the data batch is
   * represented as an Object (will be Tablet in future).
   */
  public void dispatch(Tablet tablet, long tabletId) {
    process(tablet, tabletId);
  }

  public void process(Tablet tablet, long tabletId) {
    final List<DataSlice> split = split(tablet, tabletId);
    final Map<PartitionKey, List<DataSlice>> slicesByPartition = new HashMap<>();
    split.forEach(
        slice ->
            slicesByPartition
                .computeIfAbsent(slice.getPartitionKey(), k -> new ArrayList<>())
                .add(slice));

    // Always register, including empty tablets (group count 0), so CommitTracker can advance
    // minTabletId consecutively without treating an unregistered earlier id as a skippable hole.
    if (commitTracker != null) {
      commitTracker.registerCommitId(tabletId, slicesByPartition.size());
    }

    final List<Future<?>> futures = new ArrayList<>(slicesByPartition.size());
    for (final Entry<PartitionKey, List<DataSlice>> entry : slicesByPartition.entrySet()) {
      futures.add(dataApplier.apply(entry.getKey(), entry.getValue()));
    }

    // Synchronously wait for all dispatched sub-tasks to complete.
    // A failure in any future propagates as a RuntimeException to the caller.
    for (final Future<?> future : futures) {
      try {
        future.get();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException("Interrupted while waiting for data apply to complete", e);
      } catch (ExecutionException e) {
        throw new RuntimeException("Data apply failed", e.getCause());
      }
    }
  }

  public abstract List<DataSlice> split(Tablet tablet, long tabletId);

  public static TabletDispatcher create(
      StreamSource source,
      BiFunction<PartitionKey, List<DataSlice>, Future<?>> dataApplier,
      CommitTracker commitTracker) {
    if (source instanceof IoTDBSubscriptionSource subSource) {
      return new ColumnPartitionedTabletDispatcher(
          subSource.getPartitionColumns(), dataApplier, commitTracker);
    }
    throw new UnsupportedOperationException("Unsupported source type: " + source.getType());
  }
}
