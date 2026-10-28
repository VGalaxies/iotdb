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

package org.apache.iotdb.streamnode.engine.window;

import org.apache.iotdb.streamnode.engine.task.StreamSubTask.DataSlice;
import org.apache.iotdb.streamnode.engine.window.datastructure.WindowRowList;
import org.apache.iotdb.streamnode.i18n.StreamNodeMessages;

import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.common.constant.TsFileConstant;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.utils.DateUtils;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Schema mapping and row conversion between pushed tablet slices and the windows' columnar row
 * caches. The column mapping is resolved <b>per tablet</b>: subscription tablets follow each
 * INSERT's column set and order (e.g. {@code (time, s2, s1)} after {@code (time, s1, s2)}), so the
 * first tablet's layout must never be assumed for later ones. {@link #upsertInto(WindowRowStore,
 * DataSlice, int)} writes a tablet row directly into a {@link WindowRowStore} (no intermediate
 * per-row object) - a scan column the current tablet does not carry upserts as null - and the
 * materialization side ({@link #newBlockBuilder()} plus {@link #appendRow(TsBlockBuilder,
 * WindowRowList.Cursor)}) reads stored rows back through a time-ordered cursor and assembles them
 * into TsBlocks row by row (the time column carries the placeholder value; the true event time is
 * the store's time column).
 */
final class WindowRowConverter {

  private static final long PLACEHOLDER_TIME = 0L;
  private static final int TIMESTAMP_COLUMN_INDEX = -1;

  /** Marks a scan column absent from the current tablet; its row value upserts as null. */
  private static final int MISSING_COLUMN_INDEX = -2;

  private final List<String> eventScanInputColumns;
  private final int maxWindowBlockRows;

  /**
   * Output column types, fixed for the converter's lifetime (every window row store is created with
   * them, and a store's column types cannot change after creation). Stream creation resolves them
   * against the table schema ({@code eventInputColumnTypes}): subscription tablets carry each
   * INSERT's column set, and alternating partial-column INSERTs never produce a tablet with the
   * full scan column set, so no single tablet is a reliable type source. {@code null} when the
   * stream did not carry the types: they are then established from the first tablet (which must
   * carry every scan column) in {@link #ensureColumnMapping}.
   */
  private List<TSDataType> outputTypes;

  /**
   * Single-entry per-tablet mapping cache keyed by tablet identity: a batch iterates many rows of
   * one tablet (and consecutive slices usually share one), so the common case resolves with a
   * single reference comparison; a genuinely new tablet layout only costs an O(schema) rebuild.
   */
  private Tablet cachedTablet;

  private int[] cachedTabletColumns;

  WindowRowConverter(
      List<String> eventScanInputColumns,
      List<TSDataType> eventScanInputColumnTypes,
      int maxWindowBlockRows) {
    this.eventScanInputColumns = eventScanInputColumns;
    this.maxWindowBlockRows = maxWindowBlockRows;
    if (eventScanInputColumnTypes != null) {
      if (eventScanInputColumnTypes.size() != eventScanInputColumns.size()) {
        throw new IllegalArgumentException(
            String.format(
                StreamNodeMessages.EXCEPTION_EVENT_SCAN_COLUMN_TYPE_COUNT_MISMATCH_B770DE42,
                eventScanInputColumnTypes.size(),
                eventScanInputColumns.size()));
      }
      this.outputTypes = List.copyOf(eventScanInputColumnTypes);
    }
  }

  /**
   * Extracts one tablet row and upserts it directly into the given store: the column values are
   * read from the tablet here (the tablet is never touched again afterwards) and unboxed into the
   * store's typed columns immediately. The row's device identity is derived per row from the
   * tablet's TAG-category columns ({@code Tablet.getDeviceID(row)}, a {@code StringArrayDeviceID}
   * of table name + tag values), NOT frozen at stream creation: a TAG column added to the source
   * table changes the derived identity from the first tablet that carries it - and TAG columns are
   * exempt from the subscription side's column-filter pruning, so the new column is always
   * delivered (see {@link WindowRowStore}'s class javadoc).
   *
   * @return the store's upsert result: whether a <b>main-view</b> row was inserted (the tumble
   *     engine's pending-row bookkeeping counts main-view inserts only)
   */
  boolean upsertInto(WindowRowStore store, DataSlice dataSlice, int row) {
    ensureColumnMapping(dataSlice);
    Tablet tablet = dataSlice.getTablet();
    Object[] values = new Object[cachedTabletColumns.length];
    for (int col = 0; col < values.length; col++) {
      int tabletColumnIndex = cachedTabletColumns[col];
      if (tabletColumnIndex == TIMESTAMP_COLUMN_INDEX) {
        values[col] = tablet.getTimestamps()[row];
      } else if (tabletColumnIndex == MISSING_COLUMN_INDEX) {
        // The current tablet does not carry this scan column (its INSERT omitted it): the row's
        // value is null; a previously upserted non-null value at the same timestamp is kept.
        values[col] = null;
      } else {
        values[col] = readValue(tablet, row, tabletColumnIndex, outputTypes.get(col));
      }
    }
    return store.upsert(tablet.getDeviceID(row), tablet.getTimestamps()[row], values);
  }

  /**
   * Output column types: the stream-creation-resolved types when the stream carried them, otherwise
   * the (lazily established) first tablet's types. The engine passes these to {@code
   * WindowRowStore} when creating a window's columnar row store.
   */
  List<TSDataType> outputTypes() {
    return outputTypes;
  }

  /** Maximum number of rows one materialized block assembles into a TsBlock. */
  int maxWindowBlockRows() {
    return maxWindowBlockRows;
  }

  /**
   * Starts one output block. The caller ({@code WindowEventRowsIterator}) appends the rows of its
   * device-major cursor walk one by one and builds the block when its row cap is reached.
   */
  TsBlockBuilder newBlockBuilder() {
    return new TsBlockBuilder(outputTypes);
  }

  /**
   * Appends one stored row - the cursor's current node - to the block: the placeholder time, then
   * per output column a null check followed by the typed read straight off the cursor's slot (the
   * cursor resolves the node's chunk/offset once per node, so every value read is a direct array
   * access).
   */
  void appendRow(TsBlockBuilder builder, WindowRowList.Cursor row) {
    builder.getTimeColumnBuilder().writeLong(PLACEHOLDER_TIME);
    for (int col = 0; col < outputTypes.size(); col++) {
      writeValue(builder.getColumnBuilder(col), outputTypes.get(col), row, col);
    }
    builder.declarePosition();
  }

  /**
   * Resolves the column mapping for the slice's tablet, reusing the cached mapping while rows of
   * the same tablet keep arriving. Without stream-creation-resolved types, the first tablet
   * additionally establishes {@link #outputTypes()} (callers that create a {@link WindowRowStore},
   * which needs those types, must invoke this with a slice of the batch first).
   */
  void ensureColumnMapping(DataSlice dataSlice) {
    Tablet tablet = dataSlice.getTablet();
    if (tablet == cachedTablet) {
      return;
    }
    List<IMeasurementSchema> schemas = tablet.getSchemas();
    Map<String, Integer> columnIndexByName = new HashMap<>();
    for (int i = 0; i < schemas.size(); i++) {
      columnIndexByName.put(schemas.get(i).getMeasurementName(), i);
    }
    int[] tabletColumns = new int[eventScanInputColumns.size()];
    for (int i = 0; i < eventScanInputColumns.size(); i++) {
      String column = eventScanInputColumns.get(i);
      if (TsFileConstant.TIME_COLUMN_ID.equals(column)) {
        tabletColumns[i] = TIMESTAMP_COLUMN_INDEX;
      } else {
        Integer tabletColumnIndex = columnIndexByName.get(column);
        tabletColumns[i] = tabletColumnIndex == null ? MISSING_COLUMN_INDEX : tabletColumnIndex;
      }
    }
    if (outputTypes == null) {
      outputTypes = buildOutputTypes(schemas, tabletColumns);
    }
    cachedTablet = tablet;
    cachedTabletColumns = tabletColumns;
  }

  /**
   * Output column types from the first tablet's schema. A scan column missing even there has an
   * unknown type that could never be resolved later (the row stores' column types are fixed at
   * creation), so it fails fast instead of mis-mapping silently.
   */
  private List<TSDataType> buildOutputTypes(List<IMeasurementSchema> schemas, int[] tabletColumns) {
    List<TSDataType> types = new ArrayList<>(eventScanInputColumns.size());
    for (int i = 0; i < tabletColumns.length; i++) {
      if (tabletColumns[i] == TIMESTAMP_COLUMN_INDEX) {
        types.add(TSDataType.TIMESTAMP);
      } else if (tabletColumns[i] == MISSING_COLUMN_INDEX) {
        throw new IllegalStateException(
            String.format(
                StreamNodeMessages.EXCEPTION_EVENT_SCAN_COLUMN_MISSING_IN_FIRST_TABLET_12D68452,
                eventScanInputColumns.get(i)));
      } else {
        types.add(schemas.get(tabletColumns[i]).getType());
      }
    }
    return types;
  }

  private static Object readValue(Tablet tablet, int row, int tabletColumnIndex, TSDataType type) {
    if (tablet.isNull(row, tabletColumnIndex)) {
      return null;
    }
    Object columnValues = tablet.getValues()[tabletColumnIndex];
    switch (type) {
      case BOOLEAN:
        return ((boolean[]) columnValues)[row];
      case INT32:
        return ((int[]) columnValues)[row];
      case DATE:
        return DateUtils.parseDateExpressionToInt(((LocalDate[]) columnValues)[row]);
      case INT64:
      case TIMESTAMP:
        return ((long[]) columnValues)[row];
      case FLOAT:
        return ((float[]) columnValues)[row];
      case DOUBLE:
        return ((double[]) columnValues)[row];
      case TEXT:
      case STRING:
      case BLOB:
        return ((Object[]) columnValues)[row];
      default:
        return null;
    }
  }

  private static void writeValue(
      ColumnBuilder builder, TSDataType type, WindowRowList.Cursor row, int column) {
    if (row.isNull(column)) {
      builder.appendNull();
      return;
    }
    switch (type) {
      case BOOLEAN:
        builder.writeBoolean(row.getBoolean(column));
        break;
      case INT32:
      case DATE:
        builder.writeInt(row.getInt(column));
        break;
      case INT64:
      case TIMESTAMP:
        builder.writeLong(row.getLong(column));
        break;
      case FLOAT:
        builder.writeFloat(row.getFloat(column));
        break;
      case DOUBLE:
        builder.writeDouble(row.getDouble(column));
        break;
      case TEXT:
      case STRING:
      case BLOB:
        builder.writeBinary(row.getBinary(column));
        break;
      default:
        builder.appendNull();
    }
  }
}
