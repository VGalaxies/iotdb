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
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied.  See the License for the specific
 * language governing permissions and limitations under the License.
 */

package org.apache.iotdb.streamnode.engine.window;

import org.apache.iotdb.streamnode.engine.window.datastructure.WindowRowList;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.StringArrayDeviceID;
import org.apache.tsfile.utils.RamUsageEstimator;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.apache.tsfile.utils.RamUsageEstimator.NUM_BYTES_ARRAY_HEADER;
import static org.apache.tsfile.utils.RamUsageEstimator.NUM_BYTES_OBJECT_HEADER;
import static org.apache.tsfile.utils.RamUsageEstimator.NUM_BYTES_OBJECT_REF;

/**
 * Per-window row store keyed by <b>device identity</b>, mirroring the memtable's structure ({@code
 * Map<IDeviceID, chunk group>}): one columnar {@link WindowRowList} (arrival-order chunked typed
 * arrays plus a skiplist time index, modeled on AlignedTVList) per device. Row identity lives
 * <b>outside</b> the row structure, so rows of different devices never merge - the
 * un-grouped-stream same-timestamp-merge defect (rows of different devices sharing a timestamp
 * collapsing into one row) cannot recur: the device id is derived per row from the tablet's TAG
 * columns at push time, not frozen at stream creation.
 *
 * <p><b>End-to-end scope</b>: TAG columns are exempt from the subscription side's column-filter
 * pruning, so a TAG column added to the source table mid-stream is still delivered and every row's
 * identity keeps resolving per row at push time. Rows differing only in such a TAG column never
 * merge. Verified end to end by {@code IoTDBStreamTumbleRowIdentityIT
 * #testTumblePartitionedStreamSurvivesSourceTagColumnAddition}.
 *
 * <p>Reads are served through a <b>device-major cursor walk</b>: the devices in map insertion
 * order, each device's rows in time order (a {@link WindowRowList.Cursor} per device). Within one
 * device the rows are time-sorted, but the concatenation is not globally time-sorted; the
 * range-count queries ({@link #countBetween(long, long)}, {@link #countAbove(long)}) aggregate per
 * device. A <b>generation cursor</b> in {@code WindowEventRowsIterator} walks the same device order
 * device by device, so a block boundary inside an equal-timestamp run no longer loses the run's
 * tail (the old global-time-order cursor skipped strictly past the last emitted timestamp).
 *
 * <p><b>Frozen view</b>: at window close the owning engine calls {@link #freeze()}. From then on
 * every read serves exactly the rows present at close (the rows the close event counted), and
 * {@link #upsert} diverts into a separate late map: a lazily materializing close event must never
 * pick up rows that entered after close - its row count was fixed then. The late rows surface only
 * in the destroy-time full recompute, via {@link #mergedCopy()}.
 *
 * <p>All state is guarded by the owning engine's lock; the store is not thread-safe on its own.
 */
final class WindowRowStore {

  private final TSDataType[] columnTypes;

  /**
   * The store's main (frozen) view: device identity to that device's time-sorted rows, in insertion
   * order (LinkedHashMap) - the device-major read order.
   */
  private final LinkedHashMap<IDeviceID, WindowRowList> rows = new LinkedHashMap<>();

  /** Post-freeze late rows (see the class javadoc); lazily created on the first diverted upsert. */
  private LinkedHashMap<IDeviceID, WindowRowList> lateRows;

  /** Rows in the main view (the frozen read view's size); maintained on insert for O(1) reads. */
  private int totalRows;

  /**
   * Running maximum main-view timestamp (main-view inserts only; late rows are invisible to the
   * frozen read view). Maintained on insert for an O(1) {@link #maxTimestamp()}.
   */
  private long maxTimestamp = Long.MIN_VALUE;

  /**
   * Running {@link #ramSizeEstimate()} over both views, maintained per upsert as the written list's
   * O(1) estimate delta plus each new device's fixed overhead (its list's skeleton, the map entry
   * and the retained key, charged at device creation), and at directory rebuilds - the owning
   * engine reconciles the data-pool charge on every processed batch, which the many-devices shape
   * must not turn into a per-batch O(devices) walk.
   */
  private long ramBytes;

  /** Set at window close; see {@link #freeze()}. */
  private boolean frozen;

  // ===== device directory (rebuilt lazily after a new device is added) =====

  /**
   * The devices' row lists in map insertion order. Devices are only ever appended (a {@code
   * LinkedHashMap} with no removal), so the array is rebuilt exactly when its length no longer
   * matches the map's size; row inserts inside an existing device never touch it.
   */
  private WindowRowList[] deviceLists = new WindowRowList[0];

  /**
   * Bytes currently charged to the global window <b>data</b> memory pool for this store's rows.
   * Reconciled against {@link #ramSizeEstimate()} by the owning engine after each processed batch.
   */
  private long chargedDataBytes;

  /**
   * Owners still referencing this store: the owning engine (initial count 1) plus every live event
   * iterator that pinned it (an iterator keeps reading the store after the engine discards the
   * window). When the count reaches zero the data-pool charge is returned - until then a pinned
   * store keeps its charge, matching the memory it really occupies ("light events, heavy
   * references"). All accesses happen under the owning engine's lock.
   */
  private int ownerCount = 1;

  WindowRowStore(List<TSDataType> columnTypes) {
    this.columnTypes = columnTypes.toArray(new TSDataType[0]);
  }

  boolean isEmpty() {
    return totalRows == 0;
  }

  int size() {
    return totalRows;
  }

  /**
   * Estimated heap bytes of this store, maintained incrementally for O(1) reads, for the
   * memory-control watermark (see the tech design doc's memory backpressure section): every
   * device's fixed overhead (its list's skeleton, the map entry, the retained key, the directory
   * slot) plus its chunked arrays, headers, null bitmaps, binary payloads and skiplist index bytes.
   * The fixed part is charged per device because the many-devices-few-rows shape is exactly the
   * shape where it dominates.
   */
  long ramSizeEstimate() {
    return ramBytes;
  }

  // ===== device directory (caller must hold the owning engine's lock) =====

  /** Number of devices in the main view - the device-major read order's length. */
  int deviceCount() {
    ensureDirectory();
    return deviceLists.length;
  }

  /** The device {@code index}'s row list (device-major read order). */
  WindowRowList deviceList(int index) {
    ensureDirectory();
    return deviceLists[index];
  }

  /**
   * Rebuilds the directory after a new device was added (devices are only ever appended). The
   * rebuilt array is real retained heap sized exactly by the device count, so its bytes enter the
   * running estimate here; between a device's insertion and the first directory read they are the
   * only un-billed part of the store (one reference per device, less than one percent of the
   * device's fixed overhead).
   */
  private void ensureDirectory() {
    if (deviceLists.length == rows.size()) {
      return;
    }
    ramBytes += directoryBytes(rows.size()) - directoryBytes(deviceLists.length);
    deviceLists = new WindowRowList[rows.size()];
    int i = 0;
    for (WindowRowList list : rows.values()) {
      deviceLists[i++] = list;
    }
  }

  /** Heap bytes of the device directory array holding {@code devices} references. */
  private static long directoryBytes(int devices) {
    return RamUsageEstimator.alignObjectSize(
        NUM_BYTES_ARRAY_HEADER + (long) devices * NUM_BYTES_OBJECT_REF);
  }

  // ===== frozen view (close fixes the content the close event counted) =====

  /**
   * Freezes the store's main view at window close (caller holds the engine lock). After the freeze
   * every read serves exactly the rows present at close, while {@link #upsert} diverts to a
   * separate late map. Idempotent.
   */
  void freeze() {
    frozen = true;
  }

  /**
   * Whether any late row arrived after the freeze. The late map is lazily created on the first
   * diverted upsert and never emptied (its lists only grow), so a non-null map is exactly "has late
   * rows".
   */
  boolean hasLateRows() {
    return lateRows != null;
  }

  /**
   * A fresh, unfrozen store holding this store's full content: the frozen main rows plus the late
   * rows merged back with the usual same-device same-timestamp merge semantics (non-null wins).
   * Used by the destroy-time full recompute, which must see every row including post-close late
   * ones. The copy stands apart from this store on purpose: the close event's iterator keeps
   * reading this (frozen) store, so merging in place would leak late rows into a possibly
   * still-unconsumed close event. The copy happens once per dirty window at destroy - the rare
   * late-data path. The replay is per device in time order, so the copy's main-view upserts hit
   * only the same-timestamp merge or a fresh row (the replayed rows are already unique per
   * timestamp within a device); the late rows genuinely re-insert at their time positions, each
   * O(log n).
   */
  WindowRowStore mergedCopy() {
    WindowRowStore copy = new WindowRowStore(toList());
    replayInto(copy, rows);
    if (lateRows != null) {
      replayInto(copy, lateRows);
    }
    return copy;
  }

  private void replayInto(WindowRowStore copy, LinkedHashMap<IDeviceID, WindowRowList> map) {
    for (Map.Entry<IDeviceID, WindowRowList> entry : map.entrySet()) {
      WindowRowList.Cursor cursor = entry.getValue().newCursor();
      if (cursor.seekFirst()) {
        do {
          copy.upsert(entry.getKey(), cursor.timestamp(), readRow(cursor));
        } while (cursor.advance());
      }
    }
  }

  private List<TSDataType> toList() {
    List<TSDataType> types = new ArrayList<>(columnTypes.length);
    for (TSDataType type : columnTypes) {
      types.add(type);
    }
    return types;
  }

  /** One row at the cursor as the transient boxed array the upsert boundary takes. */
  private Object[] readRow(WindowRowList.Cursor cursor) {
    Object[] values = new Object[columnTypes.length];
    for (int col = 0; col < columnTypes.length; col++) {
      if (cursor.isNull(col)) {
        continue;
      }
      switch (columnTypes[col]) {
        case BOOLEAN:
          values[col] = cursor.getBoolean(col);
          break;
        case INT32:
        case DATE:
          values[col] = cursor.getInt(col);
          break;
        case INT64:
        case TIMESTAMP:
          values[col] = cursor.getLong(col);
          break;
        case FLOAT:
          values[col] = cursor.getFloat(col);
          break;
        case DOUBLE:
          values[col] = cursor.getDouble(col);
          break;
        case TEXT:
        case STRING:
        case BLOB:
          values[col] = cursor.getBinary(col);
          break;
        default:
          // No typed representation (the converter extracted only nulls for such types).
          values[col] = null;
      }
    }
    return values;
  }

  // ===== data-pool memory accounting (caller must hold the owning engine's lock) =====

  /**
   * Reconciles the store's data-pool charge with its current {@link #ramSizeEstimate()}: reserves
   * the growth and returns any shrink immediately. Called by the owning engine after each processed
   * batch.
   *
   * @return false when the growth could not be reserved (pool exhausted): the charge is left
   *     unchanged and the caller memory-pauses so the next release retries the full delta
   */
  boolean reconcileDataMemory() {
    long desired = ramSizeEstimate();
    if (desired > chargedDataBytes) {
      long delta = desired - chargedDataBytes;
      if (GlobalWindowMemoryController.getInstance().tryReserveDataMemory(delta)) {
        chargedDataBytes = desired;
        return true;
      }
      return false;
    }
    if (desired < chargedDataBytes) {
      GlobalWindowMemoryController.getInstance().releaseDataMemory(chargedDataBytes - desired);
      chargedDataBytes = desired;
    }
    return true;
  }

  /** Registers one more owner (an event iterator pinning this store). */
  void retain() {
    ownerCount++;
  }

  /**
   * Drops one owner. When the last owner (engine or event iterator) releases the store, its whole
   * data-pool charge is returned; a store still pinned by iterators keeps its charge so the pool
   * tracks the memory those iterators can still materialize.
   */
  void releaseOwner() {
    if (ownerCount == 0) {
      return;
    }
    if (--ownerCount == 0 && chargedDataBytes > 0) {
      GlobalWindowMemoryController.getInstance().releaseDataMemory(chargedDataBytes);
      chargedDataBytes = 0;
    }
  }

  // ===== timestamp range queries (aggregated over the devices, main view only) =====

  /** The maximum timestamp over all devices, or {@link Long#MIN_VALUE} if empty. */
  long maxTimestamp() {
    return maxTimestamp;
  }

  /**
   * The maximum timestamp strictly below {@code bound} over all devices, or {@link Long#MIN_VALUE}
   * if none.
   */
  long maxTimestampBelow(long bound) {
    long max = Long.MIN_VALUE;
    for (WindowRowList list : rows.values()) {
      max = Math.max(max, list.maxTimestampBelow(bound));
    }
    return max;
  }

  /**
   * Number of rows with timestamp strictly between {@code fromExclusive} and {@code toExclusive},
   * summed over the devices.
   */
  int countBetween(long fromExclusive, long toExclusive) {
    int count = 0;
    for (WindowRowList list : rows.values()) {
      count += list.countBetween(fromExclusive, toExclusive);
    }
    return count;
  }

  /** Number of rows with timestamp strictly above {@code bound}, summed over the devices. */
  int countAbove(long bound) {
    int count = 0;
    for (WindowRowList list : rows.values()) {
      count += list.countAbove(bound);
    }
    return count;
  }

  // ===== write path =====

  /**
   * Inserts or updates the row of device {@code device} at {@code timestamp} with merge mode: an
   * existing row of the <b>same device</b> keeps its prior non-null columns, while a new timestamp
   * inserts the row as-is; rows of other devices are untouched. {@code values} holds one
   * (transient, boxed) value per column - unboxed into the typed columns immediately. Per-row
   * tablet ids are not stored (nothing reads them); commit gating registers every contributing
   * tablet on the window when its rows enter. After {@link #freeze()} the row diverts to the late
   * map, invisible to the frozen read view.
   *
   * @return whether a <b>main-view</b> row was inserted ({@code false} = same-timestamp merge, or a
   *     late row diverted after the freeze)
   */
  boolean upsert(IDeviceID device, long timestamp, Object[] values) {
    if (frozen) {
      if (lateRows == null) {
        lateRows = new LinkedHashMap<>();
      }
      trackDelta(getOrCreate(lateRows, device), timestamp, values);
      return false;
    }
    WindowRowList list = getOrCreate(rows, device);
    long before = list.ramSizeEstimate();
    boolean inserted = list.upsert(timestamp, values);
    ramBytes += list.ramSizeEstimate() - before;
    if (inserted) {
      totalRows++;
      maxTimestamp = Math.max(maxTimestamp, timestamp);
    }
    return inserted;
  }

  private void trackDelta(WindowRowList list, long timestamp, Object[] values) {
    long before = list.ramSizeEstimate();
    list.upsert(timestamp, values);
    ramBytes += list.ramSizeEstimate() - before;
  }

  private WindowRowList getOrCreate(LinkedHashMap<IDeviceID, WindowRowList> map, IDeviceID device) {
    WindowRowList list = map.get(device);
    if (list == null) {
      // A fresh list's first upsert always inserts a row, so the caller's row-count bookkeeping
      // covers the new device as well; the device directory notices the new device by its size.
      list = new WindowRowList(columnTypes);
      map.put(device, list);
      // The per-device fixed overhead beyond the list's own (already self-billed) estimate: the
      // map entry and the retained IDeviceID key. With many devices per window this dominates the
      // per-row bytes, so it must enter the running estimate at device creation, not stay
      // invisible to the memory controller.
      ramBytes += list.ramSizeEstimate() + deviceEntryBytes(device);
    }
    return list;
  }

  // ===== per-device fixed overhead (map entry + retained key) =====

  /** Shallow bytes of one {@link LinkedHashMap} entry: header, int hash, and five object refs. */
  private static final long LINKED_HASH_MAP_ENTRY_BYTES =
      RamUsageEstimator.alignObjectSize(
          NUM_BYTES_OBJECT_HEADER + Integer.BYTES + 5L * NUM_BYTES_OBJECT_REF);

  /**
   * Heap bytes of one device's overhead outside its list: the map entry plus the retained {@link
   * IDeviceID} key. The key is exact for {@link StringArrayDeviceID} - the identity {@code
   * Tablet.getDeviceID} always builds (table name + TAG values, the many-devices shape) - covering
   * the segments array and each segment string; other implementations get a flat conservative
   * value.
   */
  private static long deviceEntryBytes(IDeviceID device) {
    long keyBytes;
    if (device instanceof StringArrayDeviceID) {
      keyBytes =
          RamUsageEstimator.alignObjectSize(
                  NUM_BYTES_OBJECT_HEADER + 2L * Integer.BYTES + NUM_BYTES_OBJECT_REF)
              + RamUsageEstimator.sizeOf(((StringArrayDeviceID) device).getSegments());
    } else {
      keyBytes = 2L * RamUsageEstimator.ARRAY_LIST_INSTANCE_SIZE;
    }
    return LINKED_HASH_MAP_ENTRY_BYTES + keyBytes;
  }
}
