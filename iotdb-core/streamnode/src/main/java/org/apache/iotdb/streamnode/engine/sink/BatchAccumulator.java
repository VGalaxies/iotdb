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

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.write.record.Tablet;

import java.util.ArrayList;
import java.util.List;

public class BatchAccumulator {
  private final int maxBatchRows;
  private final long maxBatchMemoryBytes;
  private final long maxBatchLingerMs;
  private final String tableName;
  private final List<String> columnNames;
  private List<TSDataType> columnDataTypes;
  private List<ColumnCategory> columnCategories;
  private final List<SinkEntry> bufferedEntries = new ArrayList<>();
  private int currentRowCount = 0;
  private long currentMemoryBytes = 0;
  private long firstEntryTimeNanos = -1;

  public BatchAccumulator(
      String tableName,
      List<String> columnNames,
      List<TSDataType> columnDataTypes,
      List<ColumnCategory> columnCategories,
      SinkPipelineConfig config) {
    this.tableName = tableName;
    this.columnNames = columnNames;
    this.columnDataTypes = columnDataTypes;
    this.columnCategories = columnCategories;
    this.maxBatchRows = config.getMaxBatchRows();
    this.maxBatchMemoryBytes = config.getMaxBatchMemoryBytes();
    this.maxBatchLingerMs = config.getMaxBatchLingerMs();
  }

  private void resolveSchemasFromTsBlock(TsBlock tsBlock) {
    if (columnDataTypes != null) return;
    int columnCount = tsBlock.getValueColumnCount();
    columnDataTypes = new ArrayList<>(columnCount);
    columnCategories = new ArrayList<>(columnCount);
    for (int i = 0; i < columnCount; i++) {
      TSDataType dataType = tsBlock.getColumn(i).getDataType();
      columnDataTypes.add(dataType);
      columnCategories.add(ColumnCategory.FIELD);
    }
  }

  public void add(SinkEntry entry) {
    if (!entry.getTsBlock().isPresent()) {
      return;
    }
    TsBlock tsBlock = entry.getTsBlock().get();
    resolveSchemasFromTsBlock(tsBlock);
    if (bufferedEntries.isEmpty()) {
      firstEntryTimeNanos = entry.getEnqueueTimeNanos();
    }
    bufferedEntries.add(entry);
    currentRowCount += tsBlock.getPositionCount();
    currentMemoryBytes += entry.getMemorySizeInBytes();
  }

  public boolean shouldFlushBySize() {
    if (bufferedEntries.isEmpty()) return false;
    return currentRowCount >= maxBatchRows || currentMemoryBytes >= maxBatchMemoryBytes;
  }

  public boolean shouldFlushByTime() {
    if (bufferedEntries.isEmpty() || firstEntryTimeNanos < 0) {
      return false;
    }
    return (System.nanoTime() - firstEntryTimeNanos) / 1_000_000 >= maxBatchLingerMs;
  }

  public FlushBatch buildTabletAndReset() {
    Tablet tablet =
        new Tablet(tableName, columnNames, columnDataTypes, columnCategories, currentRowCount);
    tablet.initBitMaps();

    int rowIndex = 0;
    for (SinkEntry entry : bufferedEntries) {
      if (!entry.getTsBlock().isPresent()) {
        continue;
      }
      TsBlock tsBlock = entry.getTsBlock().get();
      int positionCount = tsBlock.getPositionCount();
      Column timeColumn = tsBlock.getTimeColumn();
      for (int pos = 0; pos < positionCount; pos++) {
        tablet.addTimestamp(rowIndex, timeColumn.getLong(pos));
        for (int colIdx = 0; colIdx < columnNames.size(); colIdx++) {
          Column valueColumn = tsBlock.getColumn(colIdx);
          if (valueColumn.isNull(pos)) {
            tablet.addValue(columnNames.get(colIdx), rowIndex, null);
          } else {
            addColumnValueToTablet(
                tablet,
                columnNames.get(colIdx),
                rowIndex,
                columnDataTypes.get(colIdx),
                valueColumn,
                pos);
          }
        }
        rowIndex++;
      }
    }

    long memoryToRelease = currentMemoryBytes;
    List<SinkEntry> entries = new ArrayList<>(bufferedEntries);
    bufferedEntries.clear();
    currentRowCount = 0;
    currentMemoryBytes = 0;
    firstEntryTimeNanos = -1;

    return new FlushBatch(tablet, entries, memoryToRelease);
  }

  private void addColumnValueToTablet(
      Tablet tablet,
      String measurementName,
      int rowIndex,
      TSDataType dataType,
      Column column,
      int position) {
    switch (dataType) {
      case BOOLEAN:
        tablet.addValue(measurementName, rowIndex, column.getBoolean(position));
        break;
      case INT32:
        tablet.addValue(measurementName, rowIndex, column.getInt(position));
        break;
      case INT64:
      case TIMESTAMP:
      case DATE:
        tablet.addValue(measurementName, rowIndex, column.getLong(position));
        break;
      case FLOAT:
        tablet.addValue(measurementName, rowIndex, column.getFloat(position));
        break;
      case DOUBLE:
        tablet.addValue(measurementName, rowIndex, column.getDouble(position));
        break;
      case TEXT:
      case STRING:
      case BLOB:
        tablet.addValue(measurementName, rowIndex, column.getBinary(position));
        break;
      case OBJECT:
        tablet.addValue(measurementName, rowIndex, column.getObject(position));
        break;
      default:
        tablet.addValue(measurementName, rowIndex, null);
    }
  }

  public boolean isEmpty() {
    return bufferedEntries.isEmpty();
  }

  public long discardAndReset() {
    long memoryToRelease = currentMemoryBytes;
    bufferedEntries.clear();
    currentRowCount = 0;
    currentMemoryBytes = 0;
    firstEntryTimeNanos = -1;
    return memoryToRelease;
  }
}
