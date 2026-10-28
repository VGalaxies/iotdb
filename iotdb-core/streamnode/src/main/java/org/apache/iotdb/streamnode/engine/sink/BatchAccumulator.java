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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;

public class BatchAccumulator {
  private final int maxBatchRows;
  private final long maxBatchMemoryBytes;
  private final long maxBatchLingerMs;
  private final String tableName;
  private final List<String> tabletColumnNames;
  private final List<TSDataType> tabletColumnDataTypes;
  private final List<ColumnCategory> tabletColumnCategories;
  private final int[] tabletColumnIndexesInTsBlock;
  private final int timeTsBlockColumnIndex;
  private final List<SinkEntry> bufferedEntries = new ArrayList<>();
  private int currentRowCount = 0;
  private long currentMemoryBytes = 0;
  private long firstEntryTimeNanos = -1;

  public BatchAccumulator(
      @Nonnull String tableName,
      @Nonnull List<String> columnNames,
      @Nonnull List<TSDataType> columnDataTypes,
      @Nonnull List<ColumnCategory> columnCategories,
      @Nullable List<Integer> columnIndex2TsBlockColumnIndexList,
      SinkPipelineConfig config) {
    this.tableName = tableName;
    int columnCount = columnNames.size();
    if (columnDataTypes.size() != columnCount
        || columnCategories.size() != columnCount
        || (columnIndex2TsBlockColumnIndexList != null
            && columnIndex2TsBlockColumnIndexList.size() != columnCount)) {
      throw new IllegalArgumentException("Mismatched column metadata size");
    }
    this.tabletColumnNames = new ArrayList<>(columnCount - 1);
    this.tabletColumnDataTypes = new ArrayList<>(columnCount - 1);
    this.tabletColumnCategories = new ArrayList<>(columnCount - 1);
    this.tabletColumnIndexesInTsBlock = new int[columnCount - 1];
    int timeColumnIndexInTsBlock = -1;
    int tabletColumnIndex = 0;
    for (int i = 0; i < columnCount; i++) {
      if (columnCategories.get(i) == ColumnCategory.TIME) {
        if (timeColumnIndexInTsBlock >= 0) {
          throw new IllegalArgumentException("Multiple TIME columns are not allowed");
        }
        timeColumnIndexInTsBlock = getTsBlockColumnIndex(columnIndex2TsBlockColumnIndexList, i);
        continue;
      }
      if (tabletColumnIndex >= tabletColumnIndexesInTsBlock.length) {
        throw new IllegalArgumentException("TIME column is required");
      }
      tabletColumnNames.add(columnNames.get(i));
      tabletColumnDataTypes.add(columnDataTypes.get(i));
      tabletColumnCategories.add(columnCategories.get(i));
      tabletColumnIndexesInTsBlock[tabletColumnIndex] =
          getTsBlockColumnIndex(columnIndex2TsBlockColumnIndexList, i);
      tabletColumnIndex++;
    }
    if (timeColumnIndexInTsBlock < 0) {
      throw new IllegalArgumentException("TIME column is required");
    }
    this.timeTsBlockColumnIndex = timeColumnIndexInTsBlock;
    this.maxBatchRows = config.getMaxBatchRows();
    this.maxBatchMemoryBytes = config.getMaxBatchMemoryBytes();
    this.maxBatchLingerMs = config.getMaxBatchLingerMs();
  }

  public void add(SinkEntry entry) {
    if (bufferedEntries.isEmpty()) {
      firstEntryTimeNanos = entry.getEnqueueTimeNanos();
    }
    bufferedEntries.add(entry);
    if (entry.getTsBlock().isPresent()) {
      TsBlock tsBlock = entry.getTsBlock().get();
      currentRowCount += tsBlock.getPositionCount();
      currentMemoryBytes += entry.getMemorySizeInBytes();
    }
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
        new Tablet(
            tableName,
            tabletColumnNames,
            tabletColumnDataTypes,
            tabletColumnCategories,
            currentRowCount);
    tablet.initBitMaps();

    int rowIndex = 0;
    for (SinkEntry entry : bufferedEntries) {
      if (entry.getTsBlock().isEmpty()) {
        continue;
      }
      TsBlock tsBlock = entry.getTsBlock().get();
      int positionCount = tsBlock.getPositionCount();
      for (int pos = 0; pos < positionCount; pos++) {
        Column timeColumn = tsBlock.getColumn(timeTsBlockColumnIndex);
        tablet.addTimestamp(rowIndex, timeColumn.getLong(pos));
        for (int colIdx = 0; colIdx < tabletColumnNames.size(); colIdx++) {
          Column valueColumn = tsBlock.getColumn(tabletColumnIndexesInTsBlock[colIdx]);
          if (!valueColumn.isNull(pos)) {
            addColumnValueToTablet(
                tablet, colIdx, rowIndex, tabletColumnDataTypes.get(colIdx), valueColumn, pos);
          }
        }
        rowIndex++;
      }
    }

    long memoryToRelease = currentMemoryBytes;
    long batchLingerNanos = firstEntryTimeNanos < 0 ? -1 : System.nanoTime() - firstEntryTimeNanos;
    List<SinkEntry> entries = new ArrayList<>(bufferedEntries);
    bufferedEntries.clear();
    currentRowCount = 0;
    currentMemoryBytes = 0;
    firstEntryTimeNanos = -1;

    return new FlushBatch(tablet, entries, memoryToRelease, batchLingerNanos);
  }

  private int getTsBlockColumnIndex(
      @Nullable List<Integer> columnIndex2TsBlockColumnIndexList, int columnIndex) {
    return columnIndex2TsBlockColumnIndexList == null
        ? columnIndex
        : columnIndex2TsBlockColumnIndexList.get(columnIndex);
  }

  private void addColumnValueToTablet(
      Tablet tablet,
      int columnIndex,
      int rowIndex,
      TSDataType dataType,
      Column column,
      int position) {
    switch (dataType) {
      case BOOLEAN:
        tablet.addValue(rowIndex, columnIndex, column.getBoolean(position));
        break;
      case INT32:
      case DATE:
        tablet.addValue(rowIndex, columnIndex, column.getInt(position));
        break;
      case INT64:
      case TIMESTAMP:
        tablet.addValue(rowIndex, columnIndex, column.getLong(position));
        break;
      case FLOAT:
        tablet.addValue(rowIndex, columnIndex, column.getFloat(position));
        break;
      case DOUBLE:
        tablet.addValue(rowIndex, columnIndex, column.getDouble(position));
        break;
      case TEXT:
      case STRING:
      case BLOB:
      case OBJECT:
        tablet.addValue(rowIndex, columnIndex, column.getBinary(position).getValues());
        break;
    }
  }

  public boolean isEmpty() {
    return bufferedEntries.isEmpty();
  }

  /**
   * Discards every buffered entry and returns them, so the caller can release their memory and
   * settle their commit accounting. These entries will never be flushed.
   */
  public List<SinkEntry> discardAndReset() {
    List<SinkEntry> discarded = new ArrayList<>(bufferedEntries);
    bufferedEntries.clear();
    currentRowCount = 0;
    currentMemoryBytes = 0;
    firstEntryTimeNanos = -1;
    return discarded;
  }
}
