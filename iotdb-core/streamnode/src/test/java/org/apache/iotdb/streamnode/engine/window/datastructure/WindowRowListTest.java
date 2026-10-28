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

package org.apache.iotdb.streamnode.engine.window.datastructure;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

import static org.apache.tsfile.utils.RamUsageEstimator.NUM_BYTES_ARRAY_HEADER;
import static org.apache.tsfile.utils.RamUsageEstimator.NUM_BYTES_OBJECT_REF;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class WindowRowListTest {

  private static final TSDataType[] TYPES = {
    TSDataType.BOOLEAN, TSDataType.INT64, TSDataType.DOUBLE, TSDataType.STRING
  };

  /**
   * The doubling schedule's first chunk: in-order rows 0..15 share the 16-row first chunk, so the
   * small-window memory story (a 1-row window is billed ~16 rows per column, not a full-size chunk)
   * is exercised densely.
   */
  private static final int FIRST_CHUNK = WindowRowList.FIRST_CHUNK_ROWS;

  private static Binary binary(String value) {
    return new Binary(value.getBytes(StandardCharsets.UTF_8));
  }

  private static WindowRowList newList() {
    return new WindowRowList(TYPES);
  }

  /**
   * The value store's share of the estimate: the skiplist index's node levels are random, so exact
   * assertions must compare the value bytes (deterministic) rather than the whole estimate.
   */
  private static long valueBytes(WindowRowList list) {
    return list.ramSizeEstimate() - list.indexBytesEstimate();
  }

  /**
   * The exact heap bytes a 1-column INT64 list's chunk {@code chunk} opening adds: the time chunk
   * (payload + header + its list slot), the materialized INT64 array (payload + header) and its
   * null bitmap (the BitMap object, its mark array's payload + header, and the billing formula's
   * object reference).
   */
  private static long int64ChunkBytes(int chunk) {
    long rows = WindowRowList.chunkRows(chunk);
    return rows * Long.BYTES
        + NUM_BYTES_ARRAY_HEADER
        + NUM_BYTES_OBJECT_REF
        + rows * Long.BYTES
        + NUM_BYTES_ARRAY_HEADER
        + bitmapBytes(rows);
  }

  /**
   * One chunk bitmap's billing: the BitMap object, mark array payload + header, and its slot ref.
   */
  private static long bitmapBytes(long rows) {
    return RamUsageEstimator.shallowSizeOfInstance(BitMap.class)
        + NUM_BYTES_ARRAY_HEADER
        + rows / Byte.SIZE
        + 1
        + NUM_BYTES_OBJECT_REF;
  }

  /**
   * AlignedTVList's lazy materialization: a column that is null in every row of a chunk never
   * allocates its value array, so an all-null column is not billed.
   */
  @Test
  public void testAllNullColumnIsNotBilled() {
    WindowRowList list = new WindowRowList(TYPES);
    for (long t = 0; t < 2 * FIRST_CHUNK; t++) {
      list.upsert(t, new Object[] {null, t, null, null});
    }
    // Only the time column and the one INT64 column are materialized: the variable part of the
    // 4-column list (skeleton subtracted) matches the 1-column list's (skeleton subtracted too -
    // the skeletons differ by their column count).
    WindowRowList timeOnly = new WindowRowList(new TSDataType[] {TSDataType.TIMESTAMP});
    for (long t = 0; t < 2 * FIRST_CHUNK; t++) {
      timeOnly.upsert(t, new Object[] {t});
    }
    assertEquals(
        valueBytes(timeOnly) - WindowRowList.fixedSkeletonBytes(1),
        valueBytes(list) - WindowRowList.fixedSkeletonBytes(TYPES.length));
    // The null columns still read as null.
    WindowRowList.Cursor cursor = list.newCursor();
    assertTrue(cursor.seekFirst());
    assertTrue(cursor.isNull(0));
    assertTrue(cursor.seekAtOrAfter(5L));
    assertTrue(cursor.isNull(2));
    assertTrue(cursor.isNull(3));
    assertFalse(cursor.isNull(1));
  }

  @Test
  public void testNoPerRowTabletIdOverhead() {
    // The tablet-id column was read by nothing but serialize() (unused in production) - a per-row
    // 8B + one chunk per window of pure overhead. Amortized cost of a time
    // + one INT64 column must stay near the true 16B/row (chunk slack + headers included), with
    // the skiplist index billed separately and kept a compact per-row cost.
    WindowRowList list = new WindowRowList(new TSDataType[] {TSDataType.INT64});
    for (long t = 0; t < 1008; t++) {
      list.upsert(t, new Object[] {t});
    }
    long valuePerRow = valueBytes(list) / list.size();
    assertTrue(
        "amortized per-row value cost must not carry a hidden tablet-id column, perRow="
            + valuePerRow,
        valuePerRow < 20);
    // The 1008 rows are exactly the doubling chunks (16+32+...+512) and all arrived in order:
    // the lazy index never builds, so the index is billed nothing.
    assertEquals(0, list.indexBytesEstimate());
  }

  /**
   * Memory guard: a full-size first chunk would bill the whole chunk to a 1-row window; with the
   * doubling schedule a fully materialized 1-row 4-column list is under 2 KB - a ~1.5 KB fixed
   * skeleton (the many-devices-few-rows shape's per-device cost, charged once at construction) plus
   * one 16-row chunk per column - not a 1024-row chunk per column.
   */
  @Test
  public void testSmallWindowBillingMatchesSchedule() {
    WindowRowList list = new WindowRowList(TYPES);
    list.upsert(1L, new Object[] {true, 1L, 1.5, binary("x")});
    long bytes = valueBytes(list);
    // Exact: the fixed skeleton plus one 16-row chunk per column (time + 3 materialized value
    // arrays + their bitmaps) plus the 1-byte binary payload. Assert the formula, then a sane
    // magnitude bound.
    long expected =
        WindowRowList.fixedSkeletonBytes(TYPES.length)
            + (FIRST_CHUNK * Long.BYTES + NUM_BYTES_ARRAY_HEADER + NUM_BYTES_OBJECT_REF)
            + (FIRST_CHUNK + NUM_BYTES_ARRAY_HEADER)
            + (FIRST_CHUNK * Long.BYTES + NUM_BYTES_ARRAY_HEADER)
            + (FIRST_CHUNK * Double.BYTES + NUM_BYTES_ARRAY_HEADER)
            + (FIRST_CHUNK * (long) NUM_BYTES_OBJECT_REF + NUM_BYTES_ARRAY_HEADER)
            + 4 * bitmapBytes(FIRST_CHUNK)
            + binary("x").getLength()
            + NUM_BYTES_ARRAY_HEADER;
    assertEquals(expected, bytes);
    assertTrue(
        "a 1-row 4-column list must stay in the two-kilobyte range, bytes=" + bytes, bytes < 2500);
  }

  /**
   * The doubling chunk schedule: the value-store estimate steps up exactly at the schedule's row
   * boundaries (16, 16+32, ...), each step being the opened chunk's scheduled size.
   */
  @Test
  public void testDoublingChunkSchedule() {
    WindowRowList list = new WindowRowList(new TSDataType[] {TSDataType.INT64});
    assertEquals(WindowRowList.fixedSkeletonBytes(1), valueBytes(list));
    list.upsert(0L, new Object[] {0L});
    long firstChunkValueBytes = valueBytes(list);
    assertEquals(int64ChunkBytes(0) + WindowRowList.fixedSkeletonBytes(1), firstChunkValueBytes);
    // The rest of the first chunk's rows are free.
    for (long t = 1; t < FIRST_CHUNK; t++) {
      list.upsert(t, new Object[] {t});
    }
    assertEquals(firstChunkValueBytes, valueBytes(list));
    // Every chunk's worth of rows opens exactly one chunk of its scheduled size.
    long previous = valueBytes(list);
    int nextRow = FIRST_CHUNK;
    for (int chunk = 1; chunk <= WindowRowList.VARIABLE_CHUNK_COUNT + 1; chunk++) {
      int rowsInChunk = WindowRowList.chunkRows(chunk);
      for (int k = 0; k < rowsInChunk; k++) {
        list.upsert(nextRow++, new Object[] {(long) nextRow});
      }
      assertEquals(previous + int64ChunkBytes(chunk), valueBytes(list));
      previous = valueBytes(list);
      assertEquals(nextRow, WindowRowList.chunkStart(chunk + 1));
    }
    // The schedule's constants agree with the walk above: 6 doubling chunks cover 1008 rows, then
    // the full-size chunks begin.
    assertEquals(1008, WindowRowList.VARIABLE_ROWS_TOTAL);
    assertEquals(
        WindowRowList.VARIABLE_ROWS_TOTAL,
        WindowRowList.chunkStart(WindowRowList.VARIABLE_CHUNK_COUNT));
    assertEquals(
        WindowRowList.ARRAY_SIZE, WindowRowList.chunkRows(WindowRowList.VARIABLE_CHUNK_COUNT));
  }

  /**
   * The skiplist index is lazy: 1000 in-order rows keep it un-built (zero index bytes, all queries
   * served by binary searches over the time column), and the first out-of-order arrival builds it -
   * its bytes then grow monotonically with every further row and stay within sane per-row bounds (a
   * level-1 node is the floor; the expected cost with p=1/4 is ~80B/row).
   */
  @Test
  public void testLazyIndexBytes() {
    WindowRowList list = new WindowRowList(new TSDataType[] {TSDataType.INT64});
    for (long t = 0; t < 1000; t++) {
      list.upsert(t, new Object[] {t});
      assertEquals("in-order rows must not build the index", 0, list.indexBytesEstimate());
    }
    assertEquals(1000, list.countAbove(-1L));
    assertEquals(999L, list.maxTimestampBelow(1000L));
    // The first out-of-order arrival builds the index over all stored rows.
    list.upsert(-1L, new Object[] {-1L});
    long built = list.indexBytesEstimate();
    assertTrue("the index must be built and billed", built >= 1000L * 16);
    assertTrue(built <= 1000L * 200);
    long previous = built;
    for (long t = -2; t > -50; t--) {
      list.upsert(t, new Object[] {t});
      assertTrue("index bytes must not shrink on insert", list.indexBytesEstimate() >= previous);
      previous = list.indexBytesEstimate();
    }
    assertTrue(list.ramSizeEstimate() >= list.indexBytesEstimate());
  }

  /**
   * TEXT/STRING/BLOB payloads must be billed to the data pool: the tablet is released once its
   * batch is processed and the window cache becomes the byte arrays' sole owner. BLOB / long-text
   * caching would then drain real heap without the memory controller noticing.
   */
  @Test
  public void testBinaryPayloadCountedInMemoryEstimate() {
    WindowRowList list = new WindowRowList(new TSDataType[] {TSDataType.STRING});
    byte[] payload = new byte[4096];
    list.upsert(1L, new Object[] {new Binary(payload)});
    long estimate = list.ramSizeEstimate();
    assertTrue("payload bytes must be billed to the pool, estimate=" + estimate, estimate >= 4096);
  }

  /**
   * The payload accounting must stay exact through every write path: append, merge-replace (the old
   * payload is un-billed), and a late insert below the existing rows (rows are never moved, so no
   * stale-reference hazard exists anymore - only the new payload is added).
   */
  @Test
  public void testBinaryPayloadAccountingTracksAllWritePaths() {
    WindowRowList list = new WindowRowList(new TSDataType[] {TSDataType.STRING});
    list.upsert(10L, new Object[] {new Binary(new byte[100])});
    long base = valueBytes(list);
    // Merge-replace at the same timestamp: payload swap 100 -> 300, no chunk growth, no new node.
    list.upsert(10L, new Object[] {new Binary(new byte[300])});
    assertEquals(base + 200, valueBytes(list));
    // Append: a second row shares the first chunk, so only the payload (length + array header)
    // grows.
    list.upsert(11L, new Object[] {new Binary(new byte[50])});
    assertEquals(base + 250 + NUM_BYTES_ARRAY_HEADER, valueBytes(list));
    // A late insert below both rows: the payload (length + array header) is the only change.
    // Both new payloads (the 50-byte append and this 7-byte late insert) carry their own array
    // header, hence two headers on top of the 200 + 50 + 7 payload bytes.
    list.upsert(9L, new Object[] {new Binary(new byte[7])});
    assertEquals(base + 257 + 2 * NUM_BYTES_ARRAY_HEADER, valueBytes(list));
    assertEquals(3, list.size());
    // The index was built by the late insert and grew with the two inserted rows (a merge adds no
    // node); an empty un-disordered list is billed nothing.
    WindowRowList fresh = new WindowRowList(new TSDataType[] {TSDataType.STRING});
    assertEquals(0, fresh.indexBytesEstimate());
    assertTrue(list.indexBytesEstimate() > 16);
  }

  /**
   * The incremental estimate stays exact through every mutation shape: append (chunk opening), late
   * insert (index build), same-timestamp merge with a binary replace, null writes, and growth
   * across a schedule boundary.
   */
  @Test
  public void testIncrementalEstimateExactThroughMutations() {
    WindowRowList list = new WindowRowList(new TSDataType[] {TSDataType.INT64});
    // Append into an under-filled first chunk: a later physical row can still land in it.
    for (long t = 0; t < 10; t++) {
      list.upsert(t, new Object[] {t});
    }
    assertEquals(int64ChunkBytes(0) + WindowRowList.fixedSkeletonBytes(1), valueBytes(list));
    // A late insert below the minimum builds the index and lands in the same under-filled chunk:
    // the arrays do not grow, only the index does.
    long beforeLate = valueBytes(list);
    list.upsert(-1L, new Object[] {-1L});
    assertEquals(beforeLate, valueBytes(list));
    assertTrue(list.indexBytesEstimate() > 0);
    // A same-timestamp merge changes nothing (no new row, no new chunk, no payload).
    list.upsert(0L, new Object[] {42L});
    assertEquals(beforeLate, valueBytes(list));
    // Filling the first chunk exactly to its boundary changes nothing (it is already allocated).
    for (long t = 10; t < 15; t++) {
      list.upsert(t, new Object[] {t});
    }
    assertEquals(beforeLate, valueBytes(list));
    // The first row past the boundary opens the 32-row second chunk.
    list.upsert(FIRST_CHUNK, new Object[] {(long) FIRST_CHUNK});
    assertEquals(beforeLate + int64ChunkBytes(1), valueBytes(list));
    // A null-valued merge into a materialized row writes nothing (a merge keeps stored values).
    list.upsert(1L, new Object[] {null});
    assertEquals(beforeLate + int64ChunkBytes(1), valueBytes(list));
  }

  /**
   * The sorted-array design moved the tail rows on every out-of-order insert, so an out-of-order
   * ratio of r made ingestion O(r * n^2) row-copies. The skiplist index must land each insert at
   * O(log n) - a reverse-order stream (the worst case for row moves) must finish comfortably within
   * the bound. Correctness of the time-ordered read-back is asserted too.
   */
  @Test(timeout = 30_000)
  public void testReverseOrderInsertPerfGuard() {
    // Production-sized chunks, as in the real windows.
    WindowRowList list = new WindowRowList(TYPES);
    int rows = 200_000;
    for (long t = rows - 1; t >= 0; t--) {
      list.upsert(t, new Object[] {t % 2 == 0, t, t + 0.5, binary("v" + t)});
    }
    assertEquals(rows, list.size());
    WindowRowList.Cursor cursor = list.newCursor();
    assertTrue(cursor.seekFirst());
    long expected = 0;
    while (cursor.isValid()) {
      assertEquals(expected, cursor.timestamp());
      assertEquals(expected, cursor.getLong(1));
      assertEquals(expected % 2 == 0, cursor.getBoolean(0));
      cursor.advance();
      expected++;
    }
    assertEquals(rows, expected);
    assertEquals((long) rows - 1, list.getMaxTimestamp());
  }

  /**
   * Shuffled out-of-order arrival plus late rows at chunk-boundary-straddling positions: the full
   * time-ordered content (timestamps, null flags, values) is compared against a sorted-map oracle,
   * and the count/seek queries are checked against the oracle over a grid of bounds.
   */
  @Test
  public void testRandomOrderInsertsMatchTreeMapOracle() {
    WindowRowList list = newList();
    TreeMap<Long, Object[]> oracle = new TreeMap<>();
    List<Long> order = new ArrayList<>();
    for (long t = 0; t < 2000; t++) {
      order.add(2 * t);
    }
    Collections.shuffle(order, new Random(42));
    for (long ts : order) {
      Object[] row = {
        ts % 5 == 0 ? null : ts % 2 == 0, ts, ts + 0.5, ts % 7 == 0 ? null : binary("v" + ts)
      };
      list.upsert(ts, row);
      oracle.put(ts, row);
    }
    // Late rows land at row positions straddling each schedule boundary (plus the very front, the
    // middle of the last doubling chunk, and both sides of the 1008 full-size boundary).
    int[] latePositions = {
      0, 7, 15, 16, 17, 31, 32, 47, 48, 63, 64, 111, 112, 511, 512, 1000, 1007, 1008, 1998
    };
    for (int k = 0; k < latePositions.length; k++) {
      long ts = 2 * latePositions[k] + 1;
      Object[] row = {
        k % 3 == 0 ? null : k % 2 == 0, (long) k, k + 0.25, k % 2 == 0 ? null : binary("late" + k)
      };
      list.upsert(ts, row);
      oracle.put(ts, row);
    }

    assertMatchesOracle(list, oracle);
    long[] bounds = {Long.MIN_VALUE, -1L, 0L, 1L, 7L, 100L, 999L, 3999L, 4000L, Long.MAX_VALUE};
    for (long bound : bounds) {
      assertEquals(oracle.tailMap(bound, false).size(), list.countAbove(bound));
      assertEquals(
          oracle.headMap(bound, false).isEmpty()
              ? Long.MIN_VALUE
              : oracle.headMap(bound, false).lastKey(),
          list.maxTimestampBelow(bound));
    }
    for (long from : bounds) {
      for (long to : bounds) {
        assertEquals(
            to <= from ? 0 : oracle.subMap(from, false, to, false).size(),
            list.countBetween(from, to));
      }
    }
  }

  /**
   * A cursor parked on a row stays on that row across later upserts: a row inserted below it is
   * skipped by the next advance, a merge at its key is read in place (not re-delivered), and an
   * insert above it becomes its successor. This is the iterator's block-cap resume semantics. The
   * late insert below the maximum also builds the lazy index mid-walk: the cursor (parked before
   * the index existed) must re-resolve its row by its timestamp and continue correctly.
   */
  @Test
  public void testCursorStableAcrossLateInserts() {
    WindowRowList list = newList();
    list.upsert(10L, new Object[] {true, 10L, 0.5, null});
    list.upsert(20L, new Object[] {false, 20L, 1.5, binary("b")});
    list.upsert(30L, new Object[] {true, 30L, 2.5, null});
    WindowRowList.Cursor cursor = list.newCursor();
    assertTrue(cursor.seekAtOrAfter(20L));
    assertEquals(20L, cursor.timestamp());
    // The walk is still in the un-disordered mode.
    assertEquals(0, list.indexBytesEstimate());
    // Late rows around the parked cursor: below (15, builds the index), above (25), and a merge at
    // its key.
    list.upsert(15L, new Object[] {null, 15L, null, null});
    assertTrue(list.indexBytesEstimate() > 0);
    list.upsert(25L, new Object[] {null, 25L, null, null});
    list.upsert(20L, new Object[] {null, 99L, null, null});
    // The parked row is untouched: same key, and the merge's new value reads in place.
    assertEquals(20L, cursor.timestamp());
    assertEquals(99L, cursor.getLong(1));
    // The successor is the row above (25): the late row below the cursor is skipped, and the
    // merged row is the cursor's own row, not re-delivered.
    assertTrue(cursor.advance());
    assertEquals(25L, cursor.timestamp());
    assertTrue(cursor.advance());
    assertEquals(30L, cursor.timestamp());
    assertFalse(cursor.advance());
    // An exhausted cursor stays exhausted even as rows keep arriving (the sticky valid flag).
    list.upsert(35L, new Object[] {null, 35L, null, null});
    assertFalse(cursor.advance());
    assertFalse(cursor.isValid());
    // A cursor parked at the tail picks up a later-arriving row as its successor.
    WindowRowList.Cursor tail = list.newCursor();
    assertTrue(tail.seekAtOrAfter(30L));
    assertTrue(tail.advance());
    assertEquals(35L, tail.timestamp());
  }

  /**
   * A null written over a materialized chunk (append a null-only column, then a non-null value into
   * a later row of the same chunk) must re-establish the "every live slot is null-marked or valid"
   * invariant: materialization marks the chunk's live rows null before the new value lands.
   */
  @Test
  public void testMaterializationMarksPriorRowsNull() {
    WindowRowList list = newList();
    // Column 0 (BOOLEAN) stays null for the whole first chunk.
    for (long t = 0; t < FIRST_CHUNK; t++) {
      list.upsert(t, new Object[] {null, t, t + 0.5, binary("v" + t)});
    }
    // A non-null value materializes the chunk's array mid-chunk.
    list.upsert(4L, new Object[] {true, 4L, 4.5, binary("c")});
    assertTrue(isNullAt(list, 0, 0));
    assertTrue(isNullAt(list, 0, 3));
    assertFalse(isNullAt(list, 0, 4));
    assertTrue(isNullAt(list, 0, 5));
    assertTrue(isNullAt(list, 0, FIRST_CHUNK - 1));
    // Late merge into another slot keeps the invariant.
    list.upsert(2L, new Object[] {false, 2L, 2.5, null});
    assertFalse(isNullAt(list, 0, 2));
    assertTrue(isNullAt(list, 0, 1));
    assertTrue(isNullAt(list, 0, 3));
  }

  @Test
  public void testUpsertAppendMergeInsert() {
    WindowRowList list = newList();
    // In-order appends.
    assertTrue(list.upsert(1L, new Object[] {true, 10L, 1.5, binary("a")}));
    assertTrue(list.upsert(3L, new Object[] {false, 30L, null, null}));
    assertTrue(list.upsert(4L, new Object[] {null, 40L, 4.5, binary("d")}));
    assertEquals(3, list.size());
    assertEquals(4L, list.getMaxTimestamp());

    // Same timestamp merges: non-null wins, null keeps old. No row is inserted.
    assertFalse(list.upsert(3L, new Object[] {true, null, 3.5, binary("c")}));
    assertEquals(3, list.size());
    WindowRowList.Cursor row3 = list.newCursor();
    assertTrue(row3.seekAtOrAfter(3L));
    assertTrue(row3.getBoolean(0));
    assertEquals(30L, row3.getLong(1));
    assertEquals(3.5, row3.getDouble(2), 0);
    assertEquals("c", row3.getBinary(3).getStringValue(StandardCharsets.UTF_8));

    // Late data inserts at its time position: the time-ordered view shows it between its
    // neighbors, and the neighbors keep their values.
    assertTrue(list.upsert(2L, new Object[] {null, 20L, 2.5, null}));
    assertEquals(4, list.size());
    WindowRowList.Cursor walk = list.newCursor();
    assertTrue(walk.seekFirst());
    assertEquals(1L, walk.timestamp());
    assertTrue(walk.advance());
    assertEquals(2L, walk.timestamp());
    assertEquals(20L, walk.getLong(1));
    assertTrue(walk.isNull(0));
    assertTrue(walk.isNull(3));
    assertTrue(walk.advance());
    assertEquals(3L, walk.timestamp());
    assertEquals("c", walk.getBinary(3).getStringValue(StandardCharsets.UTF_8));
    assertFalse(walk.isNull(3));
  }

  /**
   * {@code lastTimestamp} must start at {@link Long#MIN_VALUE}, not 0: a device whose first row
   * carries a negative timestamp leaves a zero default stuck at 0, and the next row at exactly
   * {@code ts == 0} then matches the (phantom) maximum and silently merges into the negative row,
   * losing the 0 row. Real duplicates still merge.
   */
  @Test
  public void testNegativeTimestampsThenZeroRow() {
    WindowRowList list = new WindowRowList(new TSDataType[] {TSDataType.INT64});
    // Exact repro: a device's first row at -100, the next at exactly 0 - two rows, no merge.
    assertTrue(list.upsert(-100L, new Object[] {1L}));
    assertTrue(list.upsert(0L, new Object[] {2L}));
    assertEquals(2, list.size());
    assertEquals(0L, list.getMaxTimestamp());

    // All-negative rows keep the no-index fast path and report the true (negative) maximum.
    WindowRowList negative = new WindowRowList(new TSDataType[] {TSDataType.INT64});
    assertTrue(negative.upsert(-300L, new Object[] {30L}));
    assertTrue(negative.upsert(-200L, new Object[] {20L}));
    assertTrue(negative.upsert(-100L, new Object[] {10L}));
    assertEquals(3, negative.size());
    assertEquals(-100L, negative.getMaxTimestamp());
    assertEquals(
        "negative in-order rows must not build the index", 0, negative.indexBytesEstimate());
    assertEquals(2, negative.countBetween(-250L, -50L));
    assertEquals(-200L, negative.maxTimestampBelow(-150L));

    // A genuine duplicate at ts 0 still merges (the sentinel must not break merge semantics).
    WindowRowList zeroDup = new WindowRowList(new TSDataType[] {TSDataType.INT64});
    assertTrue(zeroDup.upsert(0L, new Object[] {1L}));
    assertFalse(zeroDup.upsert(0L, new Object[] {2L}));
    assertEquals(1, zeroDup.size());
    WindowRowList.Cursor row = zeroDup.newCursor();
    assertTrue(row.seekAtOrAfter(0L));
    assertEquals(2L, row.getLong(0));
  }

  @Test
  public void testRangeQueries() {
    WindowRowList list = newList();
    for (long t = 0; t < 10; t++) {
      list.upsert(t, new Object[] {null, t, null, null});
    }
    assertEquals(10, list.size());
    // Seeks (served by binary searches here; the same queries run as skiplist descents once a
    // late row builds the index, see testRandomOrderInsertsMatchTreeMapOracle).
    WindowRowList.Cursor cursor = list.newCursor();
    assertTrue(cursor.seekAfter(-1L));
    assertEquals(0L, cursor.timestamp());
    assertTrue(cursor.seekAfter(4L));
    assertEquals(5L, cursor.timestamp());
    assertTrue(cursor.seekAtOrAfter(5L));
    assertEquals(5L, cursor.timestamp());
    assertFalse(cursor.seekAfter(9L));
    // Strictly between 4 and 9: rows 5, 6, 7, 8.
    assertEquals(4, list.countBetween(4L, 9L));
    assertEquals(0, list.countBetween(9L, 4L));
    assertEquals(0, list.countBetween(9L, 100L));
    assertEquals(5, list.countAbove(4L));
    assertEquals(0, list.countAbove(9L));
    assertEquals(10, list.countAbove(Long.MIN_VALUE));
    assertEquals(4L, list.maxTimestampBelow(5L));
    assertEquals(Long.MIN_VALUE, list.maxTimestampBelow(0L));
    assertEquals(9L, list.maxTimestampBelow(Long.MAX_VALUE));
  }

  @Test
  public void testRamSizeEstimate() {
    WindowRowList list = newList();
    // Empty: only the fixed skeleton is retained - no index (lazy), no chunks.
    assertEquals(WindowRowList.fixedSkeletonBytes(TYPES.length), list.ramSizeEstimate());
    assertEquals(0, list.indexBytesEstimate());
    assertEquals(WindowRowList.fixedSkeletonBytes(TYPES.length), valueBytes(list));
    list.upsert(1L, new Object[] {true, 1L, 1.0, binary("x")});
    assertTrue(list.ramSizeEstimate() > 0);
    assertTrue(list.ramSizeEstimate() > list.indexBytesEstimate());
    // Rows share the first chunk in every column.
    for (long t = 2; t <= FIRST_CHUNK; t++) {
      list.upsert(t, new Object[] {true, t, 1.0, binary("x")});
    }
    assertEquals(FIRST_CHUNK, list.size());
    long firstChunkValueBytes = valueBytes(list);
    // The row past the boundary steps every column to the second chunk.
    list.upsert(FIRST_CHUNK + 1, new Object[] {true, 17L, 1.0, binary("x")});
    assertTrue(valueBytes(list) > firstChunkValueBytes);
  }

  @Test
  public void testSerializeDeserializeRoundTrip() throws IOException {
    WindowRowList list = newList();
    list.upsert(1L, new Object[] {true, 10L, 1.5, binary("a")});
    list.upsert(3L, new Object[] {false, 30L, null, null});
    list.upsert(2L, new Object[] {null, 20L, 2.5, binary("c")});
    list.upsert(4L, new Object[] {true, null, 4.5, null});

    ByteArrayOutputStream byteArrayStream = new ByteArrayOutputStream();
    DataOutputStream dataStream = new DataOutputStream(byteArrayStream);
    list.serialize(dataStream);
    assertEquals(list.serializedSize(), byteArrayStream.size());

    WindowRowList deserialized =
        WindowRowList.deserialize(
            new DataInputStream(new ByteArrayInputStream(byteArrayStream.toByteArray())));
    assertEquals(list.size(), deserialized.size());
    WindowRowList.Cursor expected = list.newCursor();
    WindowRowList.Cursor actual = deserialized.newCursor();
    assertTrue(expected.seekFirst());
    assertTrue(actual.seekFirst());
    while (expected.isValid()) {
      assertTrue(actual.isValid());
      assertEquals(expected.timestamp(), actual.timestamp());
      for (int col = 0; col < TYPES.length; col++) {
        assertEquals(expected.isNull(col), actual.isNull(col));
      }
      assertEquals(expected.getBoolean(0), actual.getBoolean(0));
      assertEquals(expected.getLong(1), actual.getLong(1));
      assertEquals(expected.getDouble(2), actual.getDouble(2), 0);
      if (!expected.isNull(3)) {
        assertEquals(
            expected.getBinary(3).getStringValue(StandardCharsets.UTF_8),
            actual.getBinary(3).getStringValue(StandardCharsets.UTF_8));
      }
      expected.advance();
      actual.advance();
    }
    assertFalse(actual.isValid());
    // The deserialized list carries the same value-store accounting (the index's random node
    // levels are excluded; the ascending replay never builds the copy's index).
    assertEquals(valueBytes(list), valueBytes(deserialized));
    assertEquals(0, deserialized.indexBytesEstimate());
    // The deserialized list is fully functional.
    assertEquals(list.countBetween(1L, 4L), deserialized.countBetween(1L, 4L));
    deserialized.upsert(5L, new Object[] {true, 50L, 5.5, binary("e")});
    assertEquals(5, deserialized.size());
    assertEquals(5L, deserialized.getMaxTimestamp());
  }

  @Test
  public void testSerializeDeserializeEmpty() throws IOException {
    WindowRowList list = newList();
    ByteArrayOutputStream byteArrayStream = new ByteArrayOutputStream();
    list.serialize(new DataOutputStream(byteArrayStream));
    assertEquals(list.serializedSize(), byteArrayStream.size());
    WindowRowList deserialized =
        WindowRowList.deserialize(
            new DataInputStream(new ByteArrayInputStream(byteArrayStream.toByteArray())));
    assertTrue(deserialized.isEmpty());
  }

  @Test
  public void testSerializeDeserializeAcrossChunkBoundary() throws IOException {
    WindowRowList list = newList();
    for (long t = 0; t < 2 * FIRST_CHUNK + 3; t++) {
      list.upsert(t, new Object[] {t % 2 == 0, t, t + 0.5, binary("v" + t)});
    }
    ByteArrayOutputStream byteArrayStream = new ByteArrayOutputStream();
    list.serialize(new DataOutputStream(byteArrayStream));
    WindowRowList deserialized =
        WindowRowList.deserialize(
            new DataInputStream(new ByteArrayInputStream(byteArrayStream.toByteArray())));
    assertEquals(list.size(), deserialized.size());
    WindowRowList.Cursor expected = list.newCursor();
    WindowRowList.Cursor actual = deserialized.newCursor();
    assertTrue(expected.seekFirst());
    assertTrue(actual.seekFirst());
    while (expected.isValid()) {
      assertEquals(expected.getLong(1), actual.getLong(1));
      assertEquals(expected.isNull(0), actual.isNull(0));
      assertEquals(
          expected.getBinary(3).getStringValue(StandardCharsets.UTF_8),
          actual.getBinary(3).getStringValue(StandardCharsets.UTF_8));
      expected.advance();
      actual.advance();
    }
    assertFalse(actual.isValid());
  }

  // ===== helpers =====

  /** Walks the list's time-ordered view and compares every row against the oracle. */
  private static void assertMatchesOracle(WindowRowList list, TreeMap<Long, Object[]> oracle) {
    assertEquals(oracle.size(), list.size());
    WindowRowList.Cursor cursor = list.newCursor();
    assertTrue(cursor.seekFirst());
    for (Map.Entry<Long, Object[]> entry : oracle.entrySet()) {
      assertTrue(cursor.isValid());
      assertEquals((long) entry.getKey(), cursor.timestamp());
      Object[] row = entry.getValue();
      assertEquals(row[0] == null, cursor.isNull(0));
      assertEquals(row[1] == null, cursor.isNull(1));
      assertEquals(row[2] == null, cursor.isNull(2));
      assertEquals(row[3] == null, cursor.isNull(3));
      if (row[0] != null) {
        assertEquals(row[0], cursor.getBoolean(0));
      }
      if (row[1] != null) {
        assertEquals(row[1], cursor.getLong(1));
      }
      if (row[2] != null) {
        assertEquals((double) row[2], cursor.getDouble(2), 0);
      }
      if (row[3] != null) {
        assertEquals(
            ((Binary) row[3]).getStringValue(StandardCharsets.UTF_8),
            cursor.getBinary(3).getStringValue(StandardCharsets.UTF_8));
      }
      cursor.advance();
    }
    assertFalse(cursor.isValid());
  }

  private static boolean isNullAt(WindowRowList list, int column, long ts) {
    WindowRowList.Cursor cursor = list.newCursor();
    assertTrue(cursor.seekAtOrAfter(ts));
    assertEquals(ts, cursor.timestamp());
    return cursor.isNull(column);
  }
}
