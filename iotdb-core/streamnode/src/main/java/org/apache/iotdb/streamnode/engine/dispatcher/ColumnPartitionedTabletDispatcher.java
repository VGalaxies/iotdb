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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Function;
import org.apache.iotdb.commons.stream.PartitionKey;
import org.apache.iotdb.commons.stream.TabletPositionPartitionKey;
import org.apache.iotdb.streamnode.engine.task.StreamSubTask;
import org.apache.iotdb.streamnode.engine.task.StreamSubTask.DataSlice;

import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class ColumnPartitionedTabletDispatcher extends TabletDispatcher {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(ColumnPartitionedTabletDispatcher.class);

  private final List<String> partitionColumns;
  private final Map<String, Integer> columnIndexMap;

  public ColumnPartitionedTabletDispatcher(List<String> partitionColumns,
      Function<PartitionKey, StreamSubTask> subTaskMapper) {
    this.partitionColumns = partitionColumns == null ? Collections.emptyList() : partitionColumns;
    this.subTaskMapper = subTaskMapper;
    this.columnIndexMap = new HashMap<>();
    for (int i = 0; i < this.partitionColumns.size(); i++) {
      columnIndexMap.put(this.partitionColumns.get(i), i);
    }
  }

  @Override
  public void dispatch(Tablet tablet, long tabletId) {
    LOGGER.debug("Dispatching data with id {} to {} sub-tasks", tablet, tabletId);
    List<DataSlice> split = split(tablet, tabletId);
    Map<StreamSubTask, List<DataSlice>> slicesByPartition = new HashMap<>();
    split.forEach(slice -> {
      slicesByPartition.computeIfAbsent(subTaskMapper.apply(slice.getPartitionKey()), k -> new ArrayList<>()).add(slice);
    });

    List<Future<Void>> futures = new ArrayList<>();
    for (Entry<StreamSubTask, List<DataSlice>> streamSubTaskListEntry : slicesByPartition.entrySet()) {
      StreamSubTask subTask = streamSubTaskListEntry.getKey();
      List<DataSlice> slices = streamSubTaskListEntry.getValue();
      for (DataSlice dataSlice : slices) {
        futures.add(subTask.offer(dataSlice));
      }
    }
    for (Future<Void> future : futures) {
      try {
        future.get();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        LOGGER.error("Dispatch data slice to sub-task interrupted");
      } catch (ExecutionException e) {
        LOGGER.error("Failed to dispatch data slice to sub-task", e);
      }
    }
  }

  /**
   * Split a Tablet into {@link DataSlice}s based on the partition columns.
   *
   * <p>Scans the tablet row by row and records a boundary whenever the combined value of all
   * partition columns changes. Each resulting {@link DataSlice} carries the
   * {@link TabletPositionPartitionKey} built from the first row of that group.
   *
   * @param tablet   the source tablet (rows must already be sorted by partition columns)
   * @param tabletId the identifier of the tablet, forwarded to each slice
   * @return one {@link DataSlice} per distinct partition-key group
   */
  private List<DataSlice> split(final Tablet tablet, final long tabletId) {
    final List<DataSlice> slices = new ArrayList<>();
    final int rowSize = tablet.getRowSize();

    if (rowSize == 0) {
      return slices;
    }

    // Resolve column indices once
    final List<IMeasurementSchema> schemas = tablet.getSchemas();
    final int[] colIndicesInTablet = new int[partitionColumns.size()];
    for (int p = 0; p < partitionColumns.size(); p++) {
      final String colName = partitionColumns.get(p);
      colIndicesInTablet[p] = -1;
      for (int s = 0; s < schemas.size(); s++) {
        if (colName.equals(schemas.get(s).getMeasurementName())) {
          colIndicesInTablet[p] = s;
          break;
        }
      }
    }

    int groupStart = 0;
    for (int row = 1; row < rowSize; row++) {
      boolean boundary = false;
      for (final int colIdx : colIndicesInTablet) {
        if (colIdx < 0) {
          continue;
        }
        if (!Objects.equals(tablet.getValue(row - 1, colIdx), tablet.getValue(row, colIdx))) {
          boundary = true;
          break;
        }
      }
      if (boundary) {
        slices.add(new DataSlice(
            new TabletPositionPartitionKey(tablet, groupStart, colIndicesInTablet),
            tablet, groupStart, row, tabletId));
        groupStart = row;
      }
    }
    // Last (or only) group
    slices.add(new DataSlice(
        new TabletPositionPartitionKey(tablet, groupStart, colIndicesInTablet),
        tablet, groupStart, rowSize, tabletId));
    return slices;
  }

  public List<String> getPartitionColumns() {
    return partitionColumns;
  }
}
