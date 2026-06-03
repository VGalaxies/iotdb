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
import org.apache.iotdb.streamnode.engine.computation.IStreamComputeTask;
import org.apache.iotdb.streamnode.engine.scheduler.task.StreamDriverContext;
import org.apache.iotdb.streamnode.engine.sink.IStreamSinkTask;
import org.apache.iotdb.streamnode.engine.window.WindowEngine;

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
  private final AtomicLong lastCommitId = new AtomicLong(-1);
  private final StreamSubTaskContext context;
  private final StreamDriverContext driverContext;

  private final WindowEngine windowEngine;
  private final IStreamComputeTask computeTask;
  private final IStreamSinkTask sinkTask;

  private StreamDataConsumer consumer;

  public StreamSubTask(
      PartitionKey partitionKey,
      WindowEngine windowEngine,
      IStreamComputeTask computeTask,
      IStreamSinkTask sinkTask,
      String streamName,
      StreamSubTaskContext context,
      StreamDriverContext driverContext) {
    this.partitionKey = partitionKey;
    this.windowEngine = windowEngine;
    this.computeTask = computeTask;
    this.sinkTask = sinkTask;
    this.streamName = streamName;
    this.context = context;
    this.driverContext = driverContext;
  }

  public void setConsumer(StreamDataConsumer consumer) {
    this.consumer = consumer;
  }

  public String getStreamName() {
    return streamName;
  }

  public Future<?> offer(List<DataSlice> dataSlices) {
    if (dataSlices.isEmpty()) {
      return CompletableFuture.completedFuture(null);
    }
    TsBlock tsBlock = toTsBlock(dataSlices);
    long commitId = dataSlices.get(0).getTabletId();
    return consumer.accept(tsBlock, commitId);
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
    Object[] values = tablet.getValues();
    BitMap[] bitMaps = tablet.getBitMaps();
    long[] timestamps = tablet.getTimestamps();

    long[] resultTimestamps = new long[totalRows];
    int timestampIdx = 0;

    for (DataSlice slice : dataSlices) {
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

  public WindowEngine getWindowEngine() {
    return windowEngine;
  }

  public IStreamComputeTask getComputeTask() {
    return computeTask;
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

  public StreamDriverContext getDriverContext() {
    return driverContext;
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
