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

package org.apache.iotdb.streamnode.engine.window.datastructure;

import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.streamnode.i18n.StreamNodeMessages;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.tsfile.utils.RamUsageEstimator.NUM_BYTES_ARRAY_HEADER;
import static org.apache.tsfile.utils.RamUsageEstimator.NUM_BYTES_OBJECT_REF;

/**
 * Columnar storage for <b>one device's</b> rows within a window, modeled on the AlignedTVList
 * design ({@code org.apache.iotdb.db.utils.datastructure.AlignedTVList}). The device identity
 * itself lives <b>outside</b> this structure: {@code WindowRowStore} keeps a {@code Map<IDeviceID,
 * WindowRowList>}, exactly like the memtable's {@code Map<IDeviceID, chunk group>}. Rows of
 * different devices therefore never share a list, and the un-grouped-stream same-timestamp-merge
 * defect (rows of different devices merging into one row) cannot recur. TAG columns (including one
 * added to the source table mid-stream - TAG columns are exempt from the subscription side's
 * column-filter pruning) all reach the per-row identity, so rows differing in any TAG column never
 * merge; see {@link WindowRowStore}'s class javadoc for the end-to-end scope.
 *
 * <p>AlignedTVList's essential design points are preserved:
 *
 * <ul>
 *   <li><b>Doubling chunks</b>: one shared {@code long[]} time column plus one value array per
 *       column, chunked with a progressive schedule - the first chunk holds {@link
 *       #FIRST_CHUNK_ROWS} rows and chunks double up to the full {@link #ARRAY_SIZE} rows - so a
 *       1-row window is billed ~16 rows per column, not a full-size chunk (the many-devices x
 *       few-rows shape multiplies the per-device fixed cost by the device count).
 *   <li><b>Lazy value materialization</b>: {@link #values} holds a {@code null} entry for a chunk
 *       whose rows are all null in that column - the array is only allocated on the first non-null
 *       write ({@code AlignedTVList.getOrCreateValueArray}), so an all-null column of a small
 *       window costs nothing.
 *   <li><b>Lazy null bitmaps</b>: {@link #bitMaps} holds {@code null} for a chunk with no nulls
 *       among its live rows; a {@code null} value array also implies all-null, so a bitmap exists
 *       only when a materialized array needs per-slot null marks.
 *   <li><b>Typed arrays</b>: values are never stored as {@code Object[]} - each materialized chunk
 *       is a real typed array ({@code boolean[]}, {@code int[]}, ...); boxed values exist only at
 *       the transient push-time boundary ({@code WindowRowConverter#upsertInto}'s value array).
 * </ul>
 *
 * <p>Differences from AlignedTVList, driven by the window cache's access pattern:
 *
 * <ul>
 *   <li><b>Arrival-order append + lazy skiplist time index</b>: rows are appended to the physical
 *       chunks in <b>arrival order</b> and never moved. While every arrival is in order the
 *       physical order <i>is</i> the time order: an upsert appends (O(1), no descent) and queries
 *       are binary searches over the sorted time column. The first out-of-order arrival builds the
 *       {@link SlotSkipList} (keyed by the within-device-unique timestamp, each node pointing at
 *       the row's physical slot), making the out-of-order {@link #upsert} and every seek O(log n) -
 *       the old design instead kept the arrays sorted and shifted the tail on an out-of-order
 *       insert, worst O(n) per row. A same-timestamp upsert finds the row and merges the non-null
 *       columns into its slot in place (no new row, no new node).
 *   <li><b>Cursor reads</b>: reads walk the time-ordered view through {@link Cursor}s (a cursor
 *       resolves the node's slot to its chunk/offset once, then reads the typed arrays directly);
 *       range counts ({@link #countBetween}, {@link #countAbove}) and seeks ({@link
 *       #maxTimestampBelow}) are O(log n) through the index's span augmentation.
 *   <li><b>No query/flush machinery</b>: no sort indices, cloning or WAL - the list is accessed
 *       only under the owning engine's lock and only supports the window engine's range-emission
 *       reads.
 * </ul>
 */
public final class WindowRowList {

  /** Column types, kept for serialization (one dataType byte per column). */
  private final TSDataType[] types;

  /**
   * Chunked time column in <b>arrival order</b> (the physical write order); the time-ordered view
   * is the {@link #index}, whose nodes point at these slots. Timestamps are unique within the
   * device.
   */
  private final List<long[]> timestamps = new ArrayList<>();

  /**
   * Per column, per chunk, the typed value array - or {@code null} while every live row in that
   * chunk is null in that column (AlignedTVList's lazy materialization). A non-null array's
   * unmarked slots are always valid values; the slots below the chunk's live-row count are either
   * valid or null-marked in the chunk's bitmap.
   */
  private final List<List<Object>> values = new ArrayList<>();

  /**
   * Per column, per chunk, the null bitmap - or {@code null} while the chunk's live rows hold no
   * nulls in that column. A non-null entry implies the chunk's value array is materialized.
   */
  private final List<List<BitMap>> bitMaps = new ArrayList<>();

  private int rowCount;

  /**
   * The time-ordered index over {@link #timestamps}' slots (see {@link SlotSkipList}); {@code null}
   * until the first out-of-order arrival, because until then the physical order is the time order
   * and every query is a binary search over the sorted time column. Its bytes are part of {@link
   * #ramSizeEstimate()}.
   */
  private SlotSkipList index;

  /**
   * The maximum timestamp ever appended (rows are never removed, so this is the current maximum; a
   * merge or a below-the-max insert never changes it). Makes {@link #getMaxTimestamp()} O(1) in
   * both index modes. Starts at {@link Long#MIN_VALUE}, NOT zero: a device's rows can all carry
   * negative timestamps, and a zero default would make the first {@code ts == 0} row look like a
   * duplicate of the maximum and silently merge into the negative row before it.
   */
  private long lastTimestamp = Long.MIN_VALUE;

  /**
   * Running sum of {@code getLength() + NUM_BYTES_ARRAY_HEADER} over the stored payloads of all
   * binary-typed columns. A replace subtracts the old value and adds the new one; rows are never
   * moved, so no other path can double-bill or un-bill a payload.
   */
  private long binaryPayloadBytes;

  /**
   * Running sum of the retained heap bytes: the fixed skeleton (charged once at construction, see
   * {@link #fixedSkeletonBytes}) plus every allocated array - each time chunk (at {@link
   * #ensureCapacity}) and every materialized value array / null bitmap (at their allocation
   * points), each billed at its chunk's scheduled size. Every allocation is the last chunk of its
   * column and rows are never removed, so the running sum always equals the full scan - and makes
   * {@link #ramSizeEstimate()} O(1) on the engine's per-row push path.
   */
  private long ramBytes;

  /**
   * @param types one entry per value column, aligned with the converter's output columns (a single
   *     {@code TIMESTAMP} entry in the time-column-only mode)
   */
  public WindowRowList(TSDataType[] types) {
    this.types = types.clone();
    for (int i = 0; i < types.length; i++) {
      values.add(new ArrayList<>());
      bitMaps.add(new ArrayList<>());
    }
    // The skeleton (this object, the types array, the chunk directories) is real retained heap
    // from construction on, and in the many-devices-few-rows shape it dominates the estimate -
    // charge it once here so {@link #ramSizeEstimate()} covers it.
    ramBytes = fixedSkeletonBytes(types.length);
  }

  // ===== fixed skeleton (the per-device overhead beyond the chunks) =====

  /**
   * One chunk-directory ArrayList's billed bytes: the list object plus a default-capacity backing
   * array ({@code Object[10]}), which a directory that ever holds an entry really allocates.
   * Billing the whole default array even below ten chunks is a bounded overestimate (~80 B per
   * directory) that keeps the skeleton O(1) and on the safe side of the memory controller (an
   * overestimate triggers backpressure early; an underestimate lets real heap drain unseen).
   */
  static final long CHUNK_DIRECTORY_BYTES =
      RamUsageEstimator.ARRAY_LIST_INSTANCE_SIZE
          + RamUsageEstimator.alignObjectSize(NUM_BYTES_ARRAY_HEADER + 10L * NUM_BYTES_OBJECT_REF);

  /**
   * The list's fixed skeleton, independent of the row count: the list object, the {@link #types}
   * array, the two outer column directories ({@link #values} and {@link #bitMaps}), and the {@code
   * 1 + 2 per column} inner chunk directories ({@link #timestamps}, plus each column's entry in
   * {@link #values} and {@link #bitMaps}). Every device's list pays this from construction, so a
   * window holding many devices with few rows each must count it per device.
   */
  @TestOnly
  static long fixedSkeletonBytes(int columnCount) {
    return RamUsageEstimator.shallowSizeOfInstance(WindowRowList.class)
        + RamUsageEstimator.alignObjectSize(
            NUM_BYTES_ARRAY_HEADER + (long) columnCount * NUM_BYTES_OBJECT_REF)
        + (3 + 2L * columnCount) * CHUNK_DIRECTORY_BYTES;
  }

  // ===== chunk schedule (progressive: 16, 32, ..., 512, 1024, 1024, ...) =====

  /** Rows per full-size chunk, mirroring TVList's chunked-array design. */
  static final int ARRAY_SIZE = 1024;

  /**
   * Rows held by the first (smallest) chunk. Small windows are the norm under multi-partition x
   * small-window x ALLOWED_LATENESS caching, and a full-size first chunk would bill the whole chunk
   * to a 1-row window; chunks double from here up to {@link #ARRAY_SIZE}.
   */
  static final int FIRST_CHUNK_ROWS = 16;

  /** Number of chunks whose size doubles from {@link #FIRST_CHUNK_ROWS}: 16, 32, ..., 512. */
  static final int VARIABLE_CHUNK_COUNT = 6;

  /** Rows held by the doubling chunks before the full-size ones begin: 16+32+...+512 = 1008. */
  static final int VARIABLE_ROWS_TOTAL = FIRST_CHUNK_ROWS * ((1 << VARIABLE_CHUNK_COUNT) - 1);

  /** Rows held by chunk {@code chunkIndex}: 16, 32, ..., 512, 1024, 1024, ... */
  static int chunkRows(int chunkIndex) {
    return chunkIndex < VARIABLE_CHUNK_COUNT ? FIRST_CHUNK_ROWS << chunkIndex : ARRAY_SIZE;
  }

  /**
   * Row index at which chunk {@code chunkIndex} starts; also the total rows held by the chunks
   * before it, so it doubles as the schedule's prefix sum ({@code chunkStart(0) == 0}).
   */
  static int chunkStart(int chunkIndex) {
    if (chunkIndex <= VARIABLE_CHUNK_COUNT) {
      return (FIRST_CHUNK_ROWS << chunkIndex) - FIRST_CHUNK_ROWS;
    }
    return VARIABLE_ROWS_TOTAL + (chunkIndex - VARIABLE_CHUNK_COUNT) * ARRAY_SIZE;
  }

  /**
   * Index of the chunk holding row {@code index}, in O(1): below the full-size region it is {@code
   * floor(log2(index + 16)) - 4} (the doubling chunks' boundaries), above it a plain division.
   */
  static int chunkIndexOf(int index) {
    if (index < 0) {
      throw new ArrayIndexOutOfBoundsException(index);
    }
    if (index >= VARIABLE_ROWS_TOTAL) {
      return VARIABLE_CHUNK_COUNT + (index - VARIABLE_ROWS_TOTAL) / ARRAY_SIZE;
    }
    return (31 - Integer.numberOfLeadingZeros(index + FIRST_CHUNK_ROWS)) - 4;
  }

  /** The row's offset within its chunk; the inverse of {@link #chunkStart} at its chunk. */
  static int offsetInChunk(int index) {
    return index - chunkStart(chunkIndexOf(index));
  }

  // ===== basic queries =====

  public int size() {
    return rowCount;
  }

  public boolean isEmpty() {
    return rowCount == 0;
  }

  /** The maximum (last in time order) timestamp currently stored, or {@link Long#MIN_VALUE}. */
  public long getMaxTimestamp() {
    return rowCount == 0 ? Long.MIN_VALUE : lastTimestamp;
  }

  // ===== range queries =====
  // While the index is un-built (no out-of-order arrival yet) the physical order is the time
  // order, so the same predicates run as binary searches over the time column; once the index
  // exists they run as span-augmented skiplist descents (O(log n), not a walk of the range).

  /**
   * Number of rows with timestamp strictly between the two bounds. The tumble engine's push-path
   * count depends on this staying a two-search, not a range walk.
   */
  public int countBetween(long fromExclusive, long toExclusive) {
    if (toExclusive <= fromExclusive || rowCount == 0) {
      return 0;
    }
    if (index == null) {
      return Math.max(0, firstIndexAtOrAfter(toExclusive) - firstIndexAfter(fromExclusive));
    }
    return Math.max(0, index.indexAtOrAfter(toExclusive) - index.indexAfter(fromExclusive));
  }

  /** Number of rows with timestamp strictly above {@code bound}. */
  public int countAbove(long bound) {
    if (index == null) {
      return rowCount - firstIndexAfter(bound);
    }
    return rowCount - index.indexAfter(bound);
  }

  /**
   * The maximum timestamp strictly below {@code bound}, or {@link Long#MIN_VALUE} if none. (The
   * last row below the bound is the predecessor of the first row at/after it.)
   */
  public long maxTimestampBelow(long bound) {
    if (index == null) {
      int atOrAfter = firstIndexAtOrAfter(bound);
      return atOrAfter > 0 ? getTimestamp(atOrAfter - 1) : Long.MIN_VALUE;
    }
    return index.lastKeyBelow(bound);
  }

  // ===== un-disordered mode: the physical order is the time order (index == null) =====

  /** The timestamp of the row at physical {@code slot}. */
  long getTimestamp(int slot) {
    return timestamps.get(chunkIndexOf(slot))[offsetInChunk(slot)];
  }

  /** First row index with timestamp &gt; {@code bound}; {@code rowCount} when none. */
  private int firstIndexAfter(long bound) {
    int low = 0;
    int high = rowCount - 1;
    int result = rowCount;
    while (low <= high) {
      int mid = (low + high) >>> 1;
      if (getTimestamp(mid) > bound) {
        result = mid;
        high = mid - 1;
      } else {
        low = mid + 1;
      }
    }
    return result;
  }

  /** First row index with timestamp &gt;= {@code bound}; {@code rowCount} when none. */
  private int firstIndexAtOrAfter(long bound) {
    int low = 0;
    int high = rowCount - 1;
    int result = rowCount;
    while (low <= high) {
      int mid = (low + high) >>> 1;
      if (getTimestamp(mid) >= bound) {
        result = mid;
        high = mid - 1;
      } else {
        low = mid + 1;
      }
    }
    return result;
  }

  // ===== cursor reads (the time-ordered view) =====

  /**
   * A live position in this list's time-ordered view. A cursor caches its row's physical slot
   * (resolved to chunk/offset), so every value read is one array access - no per-value binary
   * search. Rows are never moved and keys never change, so a cursor stays valid across later
   * upserts: a row inserted below it is skipped by the next {@link Cursor#advance()}, a merge at
   * its key mutates an already-read row in place, and an insert above it becomes its successor.
   * This also holds across the one-way lazy-index transition: a cursor parked before the index is
   * built re-resolves its row by its (unique, never re-keyed) timestamp on the next advance.
   */
  public Cursor newCursor() {
    return new Cursor();
  }

  /**
   * Reads one row's values through a cursor. All methods except the seeks require a valid cursor
   * ({@link #isValid()}); production callers always check.
   */
  public final class Cursor {

    /**
     * The parked row's skiplist node; {@code null} while the index is un-built or a parked row has
     * not been re-resolved since the index was built (see {@link #resolve()}).
     */
    private SlotSkipList.Node node;

    /** Whether the cursor sits on a row; sticky, so an exhausted cursor never revives. */
    private boolean valid;

    /** The parked row's physical slot; value reads resolve it to (chunk, offset). */
    private int slot = -1;

    /** The parked row's timestamp - the re-resolution identity across the index transition. */
    private long ts;

    private int chunk;
    private int offset;

    private Cursor() {}

    /** Positions at the first row in time order; {@code false} when the list is empty. */
    public boolean seekFirst() {
      if (index == null) {
        slot = 0;
        valid = rowCount > 0;
      } else {
        node = index.first();
        valid = node != null;
      }
      if (valid) {
        refresh();
      }
      return valid;
    }

    /** Positions at the first row with timestamp &gt;= {@code ts}; {@code false} when none. */
    public boolean seekAtOrAfter(long ts) {
      if (index == null) {
        slot = firstIndexAtOrAfter(ts);
        valid = slot < rowCount;
      } else {
        node = index.firstAtOrAfter(ts);
        valid = node != null;
      }
      if (valid) {
        refresh();
      }
      return valid;
    }

    /** Positions at the first row with timestamp &gt; {@code ts}; {@code false} when none. */
    public boolean seekAfter(long ts) {
      if (index == null) {
        slot = firstIndexAfter(ts);
        valid = slot < rowCount;
      } else {
        node = index.firstAfter(ts);
        valid = node != null;
      }
      if (valid) {
        refresh();
      }
      return valid;
    }

    /**
     * Moves to the successor in time order; {@code false} when the cursor is now exhausted. On an
     * invalid cursor this is a no-op returning {@code false}.
     */
    public boolean advance() {
      if (!valid) {
        return false;
      }
      if (index != null) {
        resolve();
        node = node.next[0];
        valid = node != null;
      } else {
        slot++;
        valid = slot < rowCount;
      }
      if (valid) {
        refresh();
      }
      return valid;
    }

    public boolean isValid() {
      return valid;
    }

    public long timestamp() {
      requireValid();
      return ts;
    }

    public boolean isNull(int column) {
      requireValid();
      return isNullAt(column, chunk, offset);
    }

    public boolean getBoolean(int column) {
      return ((boolean[]) valueArray(column))[offset];
    }

    public int getInt(int column) {
      return ((int[]) valueArray(column))[offset];
    }

    public long getLong(int column) {
      return ((long[]) valueArray(column))[offset];
    }

    public float getFloat(int column) {
      return ((float[]) valueArray(column))[offset];
    }

    public double getDouble(int column) {
      return ((double[]) valueArray(column))[offset];
    }

    public Binary getBinary(int column) {
      return ((Binary[]) valueArray(column))[offset];
    }

    /**
     * Resolves the cached state for the row the cursor now sits on: in index mode from the node,
     * otherwise from the slot (the physical order is the time order there). Value reads use only
     * (chunk, offset), which both modes fill identically.
     */
    private void refresh() {
      if (node != null) {
        slot = node.slot;
        ts = node.key;
      } else {
        ts = getTimestamp(slot);
      }
      chunk = chunkIndexOf(slot);
      offset = offsetInChunk(slot);
    }

    /**
     * Re-resolves a cursor that parked before the index was built: the parked row still exists at
     * its slot and its key is unique and never re-keyed, so {@code find(ts)} lands exactly on it
     * and the spliced-in rows around it take their correct time-order places from here on.
     */
    private void resolve() {
      if (node == null) {
        node = index.find(ts);
      }
    }

    private void requireValid() {
      if (!valid) {
        throw readWithoutCheckException(-1);
      }
    }

    /**
     * The materialized chunk array of {@code column} at the cursor's position; the null-check gate
     * ({@link #isNull(int)}) must run first (an unmaterialized chunk means the slot is null).
     */
    private Object valueArray(int column) {
      requireValid();
      Object array = values.get(column).get(chunk);
      if (array == null) {
        throw readWithoutCheckException(slot);
      }
      return array;
    }

    private IllegalStateException readWithoutCheckException(int slot) {
      return new IllegalStateException(
          String.format(
              StreamNodeMessages
                  .EXCEPTION_COLUMN_VALUE_READ_WITHOUT_NULL_CHECK_AT_INDEX_ARG_B5EDBDD7,
              slot));
    }
  }

  // ===== write path =====

  /**
   * Inserts or updates the row at {@code timestamp}: an existing row (same device, same timestamp)
   * keeps its prior values in columns where the new row is null (non-null new values win); a new
   * timestamp is appended to the physical chunks in arrival order and indexed at its time position.
   * The common strictly-ascending arrival is an O(1) append with no descent; the first out-of-order
   * arrival pays a one-time index build.
   *
   * @param values one boxed value per column, aligned with the constructor's types ({@code null}
   *     marks a null column); unboxed into the typed columns immediately
   * @return whether a new row was inserted ({@code false} = same-timestamp merge)
   */
  public boolean upsert(long timestamp, Object[] values) {
    if (index == null) {
      if (rowCount == 0 || timestamp > lastTimestamp) {
        appendRow(timestamp, values);
        return true;
      }
      if (timestamp == lastTimestamp) {
        // While un-disordered the physical order is the time order, so the only row the timestamp
        // can match is the last one; an earlier position would be an insert below the maximum.
        mergeRow(rowCount - 1, values);
        return false;
      }
      // First out-of-order arrival: pay the one-time index build, then fall through to the index
      // path for this row.
      buildIndex();
    } else if (timestamp > lastTimestamp) {
      // Append fast path: the new key is the maximum, so the find descent is redundant (the index
      // insert still runs, landing at the tail).
      appendRow(timestamp, values);
      return true;
    }
    SlotSkipList.Node node = index.find(timestamp);
    if (node != null) {
      mergeRow(node.slot, values);
      return false;
    }
    appendRow(timestamp, values);
    return true;
  }

  private void appendRow(long timestamp, Object[] values) {
    ensureCapacity(rowCount + 1);
    writeRow(rowCount, timestamp, values);
    if (index != null) {
      index.insert(timestamp, rowCount);
    }
    lastTimestamp = Math.max(lastTimestamp, timestamp);
    rowCount++;
  }

  /**
   * Builds the time-ordered index over the stored rows. The rows are appended in ascending key
   * order (the physical order is still the time order at this point - the disorder that triggered
   * the build is handled by the caller right after), so this is a sequence of tail inserts through
   * the regular tested insert path. One-time O(n log n) per list per window; from here on the index
   * is never torn down (the transition is one-way).
   */
  private void buildIndex() {
    SlotSkipList built = new SlotSkipList();
    for (int slot = 0; slot < rowCount; slot++) {
      built.insert(getTimestamp(slot), slot);
    }
    index = built;
  }

  private void mergeRow(int slot, Object[] values) {
    // A non-null new value wins; a null new value keeps the stored one. Per-row tablet ids are not
    // stored: commit gating registers every contributing tablet on the window when its rows enter
    // (once per slice), so a merge cannot lose a tablet's commit.
    for (int col = 0; col < types.length; col++) {
      if (values[col] != null) {
        setValue(col, slot, values[col], false);
      }
    }
  }

  private void writeRow(int slot, long timestamp, Object[] values) {
    setTimestamp(slot, timestamp);
    for (int col = 0; col < types.length; col++) {
      if (values[col] == null) {
        markNull(col, slot);
      } else {
        setValue(col, slot, values[col], true);
      }
    }
  }

  /**
   * Writes one non-null boxed value into the column's slot, materializing the chunk's typed array
   * on first use (AlignedTVList's {@code getOrCreateValueArray}). Materialization marks all of the
   * chunk's live rows null and then unmarks the written slot: a null array meant every prior row
   * was null, so the invariant "every live slot is either null-marked or holds a valid value" is
   * restored before the new value lands. In arrival order the new row is always the last live row
   * of its chunk, so the live count is exact.
   *
   * @param rowIsNew whether the slot is a row being appended (not yet counted in {@code rowCount});
   *     a merge into an existing row passes {@code false}
   */
  private void setValue(int column, int slot, Object value, boolean rowIsNew) {
    int chunk = chunkIndexOf(slot);
    int offset = offsetInChunk(slot);
    Object array = getOrCreateValueArray(column, chunk, offset, liveRowsInChunk(chunk, rowIsNew));
    if (types[column].isBinary()) {
      Binary[] binaries = (Binary[]) array;
      Binary old = binaries[offset];
      // A replace swaps the payloads (old subtracted, new added); a write into a fresh or
      // null-marked slot only adds (the old ref is null or already un-billed).
      boolean wasLive = old != null && !isNullAt(column, chunk, offset);
      binaries[offset] = (Binary) value;
      binaryPayloadBytes += binaryHeapBytes((Binary) value);
      if (wasLive) {
        binaryPayloadBytes -= binaryHeapBytes(old);
      }
    } else {
      addTo(types[column], array, offset, value);
    }
    BitMap map = bitMaps.get(column).get(chunk);
    if (map != null) {
      map.unmark(offset);
    }
  }

  /**
   * Marks the slot null. A chunk whose array is still {@code null} needs no bitmap (all-null by
   * definition); otherwise the bitmap is materialized lazily.
   */
  private void markNull(int column, int slot) {
    int chunk = chunkIndexOf(slot);
    Object array = values.get(column).get(chunk);
    if (array == null) {
      return;
    }
    BitMap map = bitMaps.get(column).get(chunk);
    if (map == null) {
      map = new BitMap(chunkRows(chunk));
      bitMaps.get(column).set(chunk, map);
      ramBytes += bitmapHeapSize(chunkRows(chunk));
    }
    int offset = offsetInChunk(slot);
    if (types[column].isBinary()) {
      // Drop the stale reference so the payload cannot be billed or read while null-marked. The
      // write paths only null-mark vacant or already-null slots, so no live value is dropped.
      ((Binary[]) array)[offset] = null;
    }
    map.mark(offset);
  }

  /**
   * Live rows in the chunk (the new row counted only when it is not yet in {@code rowCount}),
   * capped at the chunk size.
   */
  private int liveRowsInChunk(int chunk, boolean rowIsNew) {
    int start = chunkStart(chunk);
    int rows = chunkRows(chunk);
    int live = Math.min(rowCount, start + rows) - start;
    return rowIsNew ? Math.min(live + 1, rows) : live;
  }

  /**
   * The chunk's typed array, materialized on first use: every live row is marked null (a null array
   * meant they all were), then the written slot is unmarked.
   */
  private Object getOrCreateValueArray(int column, int chunk, int offset, int liveRows) {
    List<Object> chunks = values.get(column);
    Object array = chunks.get(chunk);
    if (array == null) {
      array = newArray(types[column], chunkRows(chunk));
      chunks.set(chunk, array);
      BitMap map = new BitMap(chunkRows(chunk));
      bitMaps.get(column).set(chunk, map);
      map.markRange(0, liveRows);
      map.unmark(offset);
      ramBytes += arrayHeapSize(types[column], chunkRows(chunk)) + bitmapHeapSize(chunkRows(chunk));
    }
    return array;
  }

  private void ensureCapacity(int capacity) {
    // The prefix-sum comparison (not size * chunkRows: the chunks are variable-sized) drives the
    // progressive allocation: a 1-row window allocates a 16-row chunk, not a full-size one.
    while (chunkStart(timestamps.size()) < capacity) {
      int rows = chunkRows(timestamps.size());
      timestamps.add(new long[rows]);
      ramBytes += rows * (long) Long.BYTES + NUM_BYTES_ARRAY_HEADER + NUM_BYTES_OBJECT_REF;
      for (int column = 0; column < types.length; column++) {
        values.get(column).add(null);
        bitMaps.get(column).add(null);
      }
    }
  }

  private void setTimestamp(int slot, long timestamp) {
    timestamps.get(chunkIndexOf(slot))[offsetInChunk(slot)] = timestamp;
  }

  // ===== memory estimation (mirrors AlignedTVList's materialized-array accounting) =====

  /**
   * Estimated heap bytes of this list, O(1) (maintained incrementally): the fixed skeleton ({@link
   * #fixedSkeletonBytes} - the list object, the types array and the chunk directories, a real cost
   * per device) plus the time chunks and, per column, only the <b>materialized</b> value chunks and
   * bitmaps (a null entry costs nothing beyond its container slot) - each billed at its chunk's
   * <b>scheduled</b> size, so an all-null column of a small window is not billed and a small window
   * pays the progressive schedule's small first chunks - plus the binary payloads and the {@link
   * SlotSkipList} index bytes (its exact node/head/scratch sizes; zero while the index is
   * un-built). Every allocation is the last chunk of its column and rows are never removed, so the
   * running sum always equals the full scan.
   */
  public long ramSizeEstimate() {
    return ramBytes + binaryPayloadBytes + (index == null ? 0 : index.ramSizeEstimate());
  }

  /**
   * The skiplist index's share of {@link #ramSizeEstimate()}; deterministic under random levels,
   * and zero while the list has seen no out-of-order arrival (the index is un-built).
   */
  long indexBytesEstimate() {
    return index == null ? 0 : index.ramSizeEstimate();
  }

  // ===== serialization =====

  /**
   * Serialized size in bytes of {@link #serialize(DataOutputStream)}: header (column count, one
   * dataType byte per column, row count) + per row time + each column's values with null flags
   * (binary payloads carry a length prefix plus their bytes).
   */
  public int serializedSize() {
    int size = Integer.BYTES * 2 + types.length + rowCount * Long.BYTES;
    for (int column = 0; column < types.length; column++) {
      Cursor cursor = newCursor();
      if (cursor.seekFirst()) {
        do {
          if (cursor.isNull(column)) {
            size += Byte.BYTES;
          } else {
            size += Byte.BYTES + valueSize(types[column]);
            if (types[column].isBinary()) {
              size += cursor.getBinary(column).getLength();
            }
          }
        } while (cursor.advance());
      }
    }
    return size;
  }

  /**
   * Serializes the rows in <b>time order</b> (a cursor walk; the physical arrival order is not on
   * the wire): header, time per row, then each column - null flag per row, then the non-null
   * values. The wire format is unchanged from the sorted-array design.
   */
  public void serialize(DataOutputStream stream) throws IOException {
    stream.writeInt(types.length);
    for (TSDataType type : types) {
      ReadWriteIOUtils.write(type, stream);
    }
    stream.writeInt(rowCount);
    Cursor cursor = newCursor();
    if (cursor.seekFirst()) {
      do {
        stream.writeLong(cursor.timestamp());
      } while (cursor.advance());
    }
    for (int column = 0; column < types.length; column++) {
      cursor = newCursor();
      if (cursor.seekFirst()) {
        do {
          if (cursor.isNull(column)) {
            stream.writeBoolean(false);
          } else {
            stream.writeBoolean(true);
            writeValue(stream, types[column], column, cursor);
          }
        } while (cursor.advance());
      }
    }
  }

  /** Deserializes a list written by {@link #serialize(DataOutputStream)}. */
  public static WindowRowList deserialize(DataInputStream stream) throws IOException {
    int columnCount = stream.readInt();
    TSDataType[] types = new TSDataType[columnCount];
    for (int i = 0; i < columnCount; i++) {
      types[i] = ReadWriteIOUtils.readDataType(stream);
    }
    WindowRowList list = new WindowRowList(types);
    int rowCount = stream.readInt();
    long[] times = new long[rowCount];
    for (int i = 0; i < rowCount; i++) {
      times[i] = stream.readLong();
    }
    // Column-major, matching serialize's layout (all of column 0's rows, then column 1's, ...).
    Object[][] rowValues = new Object[rowCount][];
    for (int i = 0; i < rowCount; i++) {
      rowValues[i] = new Object[columnCount];
    }
    for (int column = 0; column < columnCount; column++) {
      for (int i = 0; i < rowCount; i++) {
        if (stream.readBoolean()) {
          rowValues[i][column] = readValue(types[column], stream);
        }
      }
    }
    for (int i = 0; i < rowCount; i++) {
      // The rows were serialized in sorted order; upsert re-establishes the lazy arrays and null
      // marks through the regular write path. The replay is ascending, so the copy never leaves
      // the un-disordered mode: no index, exactly like a list that only ever saw in-order rows.
      list.upsert(times[i], rowValues[i]);
    }
    return list;
  }

  // ===== internals: null / array access =====

  private boolean isNullAt(int column, int chunk, int offset) {
    Object array = values.get(column).get(chunk);
    if (array == null) {
      return true;
    }
    BitMap map = bitMaps.get(column).get(chunk);
    return map != null && map.isMarked(offset);
  }

  // ===== static typed-array helpers (one switch per operation, TVList-style) =====

  private static Object newArray(TSDataType type, int size) {
    switch (type) {
      case BOOLEAN:
        return new boolean[size];
      case INT32:
      case DATE:
        return new int[size];
      case INT64:
      case TIMESTAMP:
        return new long[size];
      case FLOAT:
        return new float[size];
      case DOUBLE:
        return new double[size];
      case TEXT:
      case STRING:
      case BLOB:
        return new Binary[size];
      default:
        throw new UnsupportedOperationException(String.valueOf(type));
    }
  }

  private static void addTo(TSDataType type, Object array, int offset, Object value) {
    switch (type) {
      case BOOLEAN:
        ((boolean[]) array)[offset] = (Boolean) value;
        break;
      case INT32:
      case DATE:
        ((int[]) array)[offset] = (Integer) value;
        break;
      case INT64:
      case TIMESTAMP:
        ((long[]) array)[offset] = (Long) value;
        break;
      case FLOAT:
        ((float[]) array)[offset] = (Float) value;
        break;
      case DOUBLE:
        ((double[]) array)[offset] = (Double) value;
        break;
      case TEXT:
      case STRING:
      case BLOB:
        ((Binary[]) array)[offset] = (Binary) value;
        break;
      default:
        throw new UnsupportedOperationException(String.valueOf(type));
    }
  }

  /**
   * Heap bytes of one chunk's null bitmap: the {@link BitMap} object, its mark array (with the
   * array header), and the object reference in the list.
   */
  private static long bitmapHeapSize(int chunkRows) {
    return RamUsageEstimator.shallowSizeOfInstance(BitMap.class)
        + NUM_BYTES_ARRAY_HEADER
        + chunkRows / 8L
        + 1
        + NUM_BYTES_OBJECT_REF;
  }

  /**
   * Heap bytes of one typed chunk of the given scheduled size (payload bytes for binaries are
   * accounted separately, in {@link #binaryPayloadBytes}).
   */
  private static long arrayHeapSize(TSDataType type, int chunkRows) {
    switch (type) {
      case BOOLEAN:
        return chunkRows + (long) NUM_BYTES_ARRAY_HEADER;
      case INT32:
      case DATE:
      case FLOAT:
        return chunkRows * 4L + NUM_BYTES_ARRAY_HEADER;
      case INT64:
      case TIMESTAMP:
      case DOUBLE:
        return chunkRows * 8L + NUM_BYTES_ARRAY_HEADER;
      case TEXT:
      case STRING:
      case BLOB:
        return chunkRows * (long) NUM_BYTES_OBJECT_REF + NUM_BYTES_ARRAY_HEADER;
      default:
        return 0;
    }
  }

  private static long binaryHeapBytes(Binary binary) {
    return binary.getLength() + NUM_BYTES_ARRAY_HEADER;
  }

  // ===== static typed serialization helpers =====

  /** Size of one non-null value's fixed part (binary payloads add their byte length). */
  private static int valueSize(TSDataType type) {
    switch (type) {
      case BOOLEAN:
        return Byte.BYTES;
      case INT32:
      case DATE:
      case FLOAT:
        return Integer.BYTES;
      case INT64:
      case TIMESTAMP:
      case DOUBLE:
        return Long.BYTES;
      case TEXT:
      case STRING:
      case BLOB:
        return Integer.BYTES; // length prefix; the payload follows
      default:
        return 0;
    }
  }

  /** Writes the (non-null) value at (column, cursor) through the cursor's typed getters. */
  private void writeValue(DataOutputStream stream, TSDataType type, int column, Cursor cursor)
      throws IOException {
    switch (type) {
      case BOOLEAN:
        stream.writeBoolean(cursor.getBoolean(column));
        break;
      case INT32:
      case DATE:
        stream.writeInt(cursor.getInt(column));
        break;
      case INT64:
      case TIMESTAMP:
        stream.writeLong(cursor.getLong(column));
        break;
      case FLOAT:
        stream.writeFloat(cursor.getFloat(column));
        break;
      case DOUBLE:
        stream.writeDouble(cursor.getDouble(column));
        break;
      case TEXT:
      case STRING:
      case BLOB:
        Binary binary = cursor.getBinary(column);
        stream.writeInt(binary.getLength());
        stream.write(binary.getValues());
        break;
      default:
        break;
    }
  }

  private static Object readValue(TSDataType type, DataInputStream stream) throws IOException {
    switch (type) {
      case BOOLEAN:
        return stream.readBoolean();
      case INT32:
      case DATE:
        return stream.readInt();
      case INT64:
      case TIMESTAMP:
        return stream.readLong();
      case FLOAT:
        return stream.readFloat();
      case DOUBLE:
        return stream.readDouble();
      case TEXT:
      case STRING:
      case BLOB:
        int length = stream.readInt();
        byte[] bytes = new byte[length];
        stream.readFully(bytes);
        return new Binary(bytes);
      default:
        return null;
    }
  }
}
