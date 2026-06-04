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
import org.apache.iotdb.streamnode.engine.task.StreamSubTask;
import org.apache.iotdb.streamnode.engine.task.StreamSubTask.DataSlice;

import org.apache.tsfile.write.record.Tablet;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.BiConsumer;
import java.util.function.Function;

public abstract class TabletDispatcher {

  protected Function<PartitionKey, StreamSubTask> subTaskMapper;
  private BiConsumer<List<DataSlice>, StreamSubTask> consumer = this::offerToSubTask;

  /**
   * Dispatch a data batch to sub-tasks based on partition keys. In iteration 1, the data batch is
   * represented as an Object (will be Tablet in future).
   */
  public void dispatch(Tablet tablet, long tabletId) {
    process(tablet, tabletId);
  }

  public void process(Tablet tablet, long tabletId) {
    final List<DataSlice> split = split(tablet, tabletId);
    final Map<StreamSubTask, List<DataSlice>> slicesByPartition = new HashMap<>();
    split.forEach(
        slice ->
            slicesByPartition
                .computeIfAbsent(
                    subTaskMapper.apply(slice.getPartitionKey()), k -> new ArrayList<>())
                .add(slice));

    for (final Entry<StreamSubTask, List<DataSlice>> streamSubTaskListEntry :
        slicesByPartition.entrySet()) {
      consumer.accept(streamSubTaskListEntry.getValue(), streamSubTaskListEntry.getKey());
    }
  }

  public abstract List<DataSlice> split(Tablet tablet, long tabletId);

  public void setConsumer(final BiConsumer<List<DataSlice>, StreamSubTask> consumer) {
    if (consumer != null) {
      this.consumer = consumer;
    }
  }

  public static TabletDispatcher create(
      StreamSource source, Function<PartitionKey, StreamSubTask> subTaskMapper) {
    if (source instanceof IoTDBSubscriptionSource) {
      IoTDBSubscriptionSource subSource = (IoTDBSubscriptionSource) source;
      return new ColumnPartitionedTabletDispatcher(subSource.getPartitionColumns(), subTaskMapper);
    }
    throw new UnsupportedOperationException("Unsupported source type: " + source.getType());
  }

  public static TabletDispatcher from(
      StreamSource source, Function<PartitionKey, StreamSubTask> subTaskMapper) {
    return create(source, subTaskMapper);
  }

  private void offerToSubTask(final List<DataSlice> slices, final StreamSubTask subTask) {
    final Future<?> future = subTask.offer(slices);
    try {
      future.get();
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Dispatch data slice to sub-task interrupted", e);
    } catch (final ExecutionException e) {
      throw new IllegalStateException("Failed to dispatch data slice to sub-task", e);
    }
  }
}
