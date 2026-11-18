/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.streamnode.engine.window;

import org.apache.iotdb.streamnode.engine.task.StreamSubTask.DataSlice;

import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;

import javax.annotation.Nullable;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Bounded pending suffix buffer ({@code beta}) for regime-aware {@link VariationWindowEngine}.
 *
 * <p>Stores candidate excursion rows as compact 1-row {@link Tablet} copies so large upstream poll
 * tablets are never retained on the heap during candidate verification, while enforcing strict row
 * ({@code B_max}) and span ({@code T_max}) budgets.
 */
final class PendingSuffixBuffer {

  static final class BufferedRow {
    final DataSlice slice;
    final long timestamp;
    final @Nullable Object monitoredValue;
    final long arrivalWallMs;

    BufferedRow(
        DataSlice slice, long timestamp, @Nullable Object monitoredValue, long arrivalWallMs) {
      this.slice = slice;
      this.timestamp = timestamp;
      this.monitoredValue = monitoredValue;
      this.arrivalWallMs = arrivalWallMs;
    }
  }

  private final int maxRows;
  private final long maxSpanMs;
  private final List<BufferedRow> rows = new ArrayList<>();

  PendingSuffixBuffer(int maxRows, long maxSpanMs) {
    this.maxRows = Math.max(1, maxRows);
    this.maxSpanMs = Math.max(1L, maxSpanMs);
  }

  void append(
      DataSlice srcSlice,
      int rowIndex,
      long timestamp,
      @Nullable Object monitoredValue,
      long nowWallMs) {
    Tablet singleRowTablet = copyRowAsSingleRowTablet(srcSlice.getTablet(), rowIndex);
    DataSlice copiedSlice =
        new DataSlice(srcSlice.getPartitionKey(), singleRowTablet, 0, 1, srcSlice.getTabletId());
    rows.add(new BufferedRow(copiedSlice, timestamp, monitoredValue, nowWallMs));
  }

  boolean isEmpty() {
    return rows.isEmpty();
  }

  int size() {
    return rows.size();
  }

  long oldestArrivalMs() {
    return rows.isEmpty() ? Long.MAX_VALUE : rows.get(0).arrivalWallMs;
  }

  long oldestEventTime() {
    return rows.isEmpty() ? Long.MAX_VALUE : rows.get(0).timestamp;
  }

  long currentSpanMs(long currentEventTime, long nowWallMs) {
    if (rows.isEmpty()) {
      return 0L;
    }
    long eventSpan =
        currentEventTime != Long.MIN_VALUE
            ? Math.max(0L, currentEventTime - rows.get(0).timestamp)
            : 0L;
    long wallSpan = Math.max(0L, nowWallMs - rows.get(0).arrivalWallMs);
    return Math.max(eventSpan, wallSpan);
  }

  boolean isBudgetExceeded(long currentEventTime, long nowWallMs) {
    if (rows.isEmpty()) {
      return false;
    }
    if (rows.size() >= maxRows) {
      return true;
    }
    return currentSpanMs(currentEventTime, nowWallMs) >= maxSpanMs;
  }

  List<BufferedRow> drainAll() {
    if (rows.isEmpty()) {
      return Collections.emptyList();
    }
    List<BufferedRow> copy = new ArrayList<>(rows);
    rows.clear();
    return copy;
  }

  List<BufferedRow> sliceView() {
    return Collections.unmodifiableList(rows);
  }

  void clear() {
    rows.clear();
  }

  static Tablet copyRowAsSingleRowTablet(Tablet src, int row) {
    List<IMeasurementSchema> schemas = src.getSchemas();
    List<ColumnCategory> columnTypes = src.getColumnTypes();
    Tablet copy;
    if (columnTypes != null && columnTypes.size() == schemas.size() && !columnTypes.isEmpty()) {
      List<String> names = new ArrayList<>(schemas.size());
      List<TSDataType> types = new ArrayList<>(schemas.size());
      for (IMeasurementSchema schema : schemas) {
        names.add(schema.getMeasurementName());
        types.add(schema.getType());
      }
      copy = new Tablet(src.getDeviceId(), names, types, columnTypes, 1);
    } else {
      copy = new Tablet(src.getDeviceId(), schemas, 1);
    }
    copy.initBitMaps();
    copy.addTimestamp(0, src.getTimestamps()[row]);
    Object[] values = src.getValues();
    for (int col = 0; col < schemas.size(); col++) {
      if (values == null || values[col] == null || src.isNull(row, col)) {
        continue;
      }
      TSDataType type = schemas.get(col).getType();
      Object colArr = values[col];
      switch (type) {
        case BOOLEAN:
          copy.addValue(0, col, ((boolean[]) colArr)[row]);
          break;
        case INT32:
          copy.addValue(0, col, ((int[]) colArr)[row]);
          break;
        case DATE:
          LocalDate localDate = ((LocalDate[]) colArr)[row];
          if (localDate != null) {
            copy.addValue(0, col, localDate);
          }
          break;
        case INT64:
        case TIMESTAMP:
          copy.addValue(0, col, ((long[]) colArr)[row]);
          break;
        case FLOAT:
          copy.addValue(0, col, ((float[]) colArr)[row]);
          break;
        case DOUBLE:
          copy.addValue(0, col, ((double[]) colArr)[row]);
          break;
        case TEXT:
        case STRING:
        case BLOB:
        case OBJECT:
          Object obj = ((Object[]) colArr)[row];
          if (obj instanceof Binary) {
            copy.addValue(0, col, ((Binary) obj).getValues());
          } else if (obj instanceof byte[]) {
            copy.addValue(0, col, (byte[]) obj);
          } else if (obj instanceof String) {
            copy.addValue(0, col, (String) obj);
          }
          break;
        default:
          break;
      }
    }
    copy.setRowSize(1);
    return copy;
  }
}
