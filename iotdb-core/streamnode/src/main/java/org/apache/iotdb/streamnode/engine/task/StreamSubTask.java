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
import org.apache.iotdb.streamnode.engine.computation.ComputationEngine;
import org.apache.iotdb.streamnode.engine.sink.WriteBackEngine;
import org.apache.iotdb.streamnode.engine.window.WindowEngine;
import org.apache.iotdb.streamnode.engine.window.WindowEvent;

import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.common.block.column.LongColumn;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

public class StreamSubTask {

  private static final Logger LOGGER = LoggerFactory.getLogger(StreamSubTask.class);
  private final String streamName;
  private final PartitionKey partitionKey;
  private final WindowEngine windowEngine;
  private final AtomicLong lastCommitId = new AtomicLong(-1);
  private final WriteBackEngine writeBackEngine;
  private final ComputationEngine computationEngine;
  private final StreamDataConsumer consumer;

  public StreamSubTask(
      PartitionKey partitionKey,
      StreamWindow window,
      StreamDataConsumer consumer,
      ComputationEngine computationEngine,
      WriteBackEngine writeBackEngine,
      String streamName) {
    this.partitionKey = partitionKey;
    this.windowEngine = WindowEngine.create(window);
    this.consumer =
        consumer == null
            ? (tsBlock, commitId, partition) -> CompletableFuture.completedFuture(null)
            : consumer;
    this.computationEngine =
        computationEngine == null ? new ComputationEngine() : computationEngine;
    this.writeBackEngine = writeBackEngine;
    this.streamName = streamName;
  }

  public StreamSubTask(PartitionKey partitionKey, StreamWindow window) {
    this(
        partitionKey,
        window,
        (tsBlock, commitId, partition) -> CompletableFuture.completedFuture(null),
        new ComputationEngine(),
        null,
        "");
  }

  public String getStreamName() {
    return streamName;
  }

  public List<WindowEvent> offer(Object data, int startRow, int endRow, long dataId) {
    List<WindowEvent> events = windowEngine.process(data, startRow, endRow);
    lastCommitId.updateAndGet(lastCommittedId -> Math.max(lastCommittedId, dataId));
    return events;
  }

  public Future<?> offer(List<DataSlice> dataSlices) {
    if (dataSlices == null || dataSlices.isEmpty()) {
      return CompletableFuture.completedFuture(null);
    }
    TsBlock tsBlock = toTsBlock(dataSlices);
    long commitId = -1;
    for (DataSlice slice : dataSlices) {
      commitId = Math.max(commitId, slice.getTabletId());
    }
    final long maxCommitId = commitId;
    lastCommitId.updateAndGet(lastCommittedId -> Math.max(lastCommittedId, maxCommitId));
    PartitionKey partitionKey = dataSlices.get(0).getPartitionKey();
    return consumer.accept(tsBlock, commitId, partitionKey);
  }

  private TsBlock toTsBlock(List<DataSlice> dataSlices) {
    DataSlice first = dataSlices.get(0);
    Tablet tablet = first.getTablet();
    List<IMeasurementSchema> schemas = tablet.getSchemas();
    List<TSDataType> dataTypes = new ArrayList<>(schemas.size());
    for (IMeasurementSchema schema : schemas) {
      dataTypes.add(schema.getType());
    }

    int totalRows = 0;
    for (DataSlice slice : dataSlices) {
      totalRows += (slice.getEndRow() - slice.getStartRow());
    }

    TsBlockBuilder builder = new TsBlockBuilder(dataTypes);
    long[] resultTimestamps = new long[totalRows];
    int timestampIdx = 0;

    for (DataSlice slice : dataSlices) {
      Tablet sliceTablet = slice.getTablet();
      Object[] values = sliceTablet.getValues();
      BitMap[] bitMaps = sliceTablet.getBitMaps();
      long[] timestamps = sliceTablet.getTimestamps();
      for (int row = slice.getStartRow(); row < slice.getEndRow(); row++) {
        resultTimestamps[timestampIdx++] = timestamps[row];
        for (int col = 0; col < schemas.size(); col++) {
          ColumnBuilder columnBuilder = builder.getColumnBuilder(col);
          if (bitMaps != null
              && col < bitMaps.length
              && bitMaps[col] != null
              && bitMaps[col].isMarked(row)) {
            columnBuilder.appendNull();
          } else {
            writeColumnValue(columnBuilder, dataTypes.get(col), values[col], row);
          }
        }
        builder.declarePosition();
      }
    }

    return builder.build(new LongColumn(totalRows, Optional.empty(), resultTimestamps));
  }

  private void writeColumnValue(
      ColumnBuilder columnBuilder, TSDataType type, Object columnValues, int row) {
    switch (type) {
      case BOOLEAN:
        columnBuilder.writeBoolean(((boolean[]) columnValues)[row]);
        break;
      case INT32:
      case DATE:
        columnBuilder.writeInt(((int[]) columnValues)[row]);
        break;
      case INT64:
      case TIMESTAMP:
        columnBuilder.writeLong(((long[]) columnValues)[row]);
        break;
      case FLOAT:
        columnBuilder.writeFloat(((float[]) columnValues)[row]);
        break;
      case DOUBLE:
        columnBuilder.writeDouble(((double[]) columnValues)[row]);
        break;
      case TEXT:
      case STRING:
      case BLOB:
        columnBuilder.writeBinary((Binary) ((Object[]) columnValues)[row]);
        break;
      default:
        columnBuilder.appendNull();
    }
  }

  public long getCommitId() {
    return lastCommitId.get();
  }

  public PartitionKey getPartitionKey() {
    return partitionKey;
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
