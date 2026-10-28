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

import org.apache.tsfile.utils.RamUsageEstimator;

import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.tsfile.utils.RamUsageEstimator.NUM_BYTES_ARRAY_HEADER;
import static org.apache.tsfile.utils.RamUsageEstimator.NUM_BYTES_OBJECT_REF;

/**
 * Time-ordered index over one device's {@link WindowRowList} rows: a span-augmented skiplist
 * (Redis-ZSKIPLIST style) keyed by the (within a device unique) event timestamp, each node pointing
 * at the row's physical slot in the arrival-order columnar storage.
 *
 * <p>The physical rows are appended in <b>arrival order</b> and never moved; this index is what
 * makes the out-of-order upsert cost O(log n) - the old sorted-array insert shifted every row above
 * the insertion point, worst O(n) per row and O(n^2) per window, which grows with the window's row
 * count. The index itself is <b>lazy</b>: {@link WindowRowList} keeps it {@code null} while every
 * arrival is in order (the physical order is then the time order, so an upsert is an O(1) append
 * and queries are binary searches) and builds it on the first out-of-order arrival; from then on an
 * above-the-maximum arrival takes the append fast path in the caller, and every other insert is one
 * descent plus the splice.
 *
 * <p><b>Span augmentation</b>: every forward link carries {@code span = index(target) -
 * index(source)}, the number of key-order positions the link advances (1 for adjacent). Spans make
 * rank queries ({@link #indexAtOrAfter}/{@link #indexAfter}) O(log n) without walking the range -
 * required by the tumble engine's emission-time {@code countBetween} over an emitted increment. The
 * span of a {@code null} link is write-only garbage: no traversal ever follows a null link, and a
 * splice that makes a link non-null overwrites its span without reading the old value.
 *
 * <p>Keys are never removed or changed (rows are never deleted from a window; a same-timestamp
 * upsert merges into the existing slot), so there is no delete path and nodes stay valid for the
 * list's lifetime - a cursor parked on a node keeps pointing at that row. Single-threaded under the
 * owning engine's lock; not thread-safe on its own.
 */
final class SlotSkipList {

  /** Upper bound on a node's level; p=1/4 at 16 levels covers 4^16 nodes. */
  private static final int MAX_LEVEL = 16;

  /** Probability that a node's level grows by one more. */
  private static final double LEVEL_PROBABILITY = 0.25;

  /**
   * One indexed row. {@code key}/{@code slot} and the link arrays are visible to the enclosing
   * package for {@link WindowRowList}'s cursor reads; nothing outside the package touches nodes.
   */
  static final class Node {

    /** The row's event timestamp; unique within the device. */
    final long key;

    /**
     * The row's physical slot in the arrival-order storage: {@code chunk = chunkIndexOf(slot)},
     * {@code offset = offsetInChunk(slot)} ({@code WindowRowList}'s progressive chunk schedule).
     */
    final int slot;

    /**
     * Forward links, length = the node's level: links exist at levels {@code 0 .. next.length - 1}.
     * A node reachable via a level-i link always has level &gt; i (it was spliced into that chain
     * at its own insertion), so {@code x.next[i]} is always in bounds during a descent. Not final:
     * the head sentinel re-grows its arrays on level growth (copy-on-grow); an inserted node never
     * reassigns them after construction.
     */
    Node[] next;

    /**
     * Per level, the span of the link in {@link #next} (see the class javadoc). Not final for the
     * same copy-on-grow reason as {@link #next}.
     */
    int[] span;

    Node(long key, int slot, int level) {
      this.key = key;
      this.slot = slot;
      this.next = new Node[level];
      this.span = new int[level];
    }
  }

  /**
   * Head sentinel; {@code key}/{@code slot} unused. Its link arrays hold one entry per level in use
   * and grow (copy-on-grow) only when a node's level exceeds the current {@link #topLevel}, so a
   * small list pays for two short arrays, not two max-size ones.
   */
  private final Node head = new Node(Long.MIN_VALUE, -1, 1);

  /** Levels in use: {@code head.next.length}; grows lazily with the tallest inserted node. */
  private int topLevel = 1;

  private int size;

  /**
   * Running heap bytes of all nodes (the exact sum of {@link #nodeHeapBytes(int)} at their levels);
   * the head and the descent scratch are computed from their current array lengths instead, since
   * those change only on level growth.
   */
  private long nodeBytes;

  /**
   * Descent scratch for {@link #insert}: the predecessor per level and its 0-based key-order index.
   * Persistent to keep the hot insert path allocation-free; single-threaded use only. Billed in
   * {@link #ramSizeEstimate()} as real heap this list retains.
   */
  private final Node[] update = new Node[MAX_LEVEL];

  private final int[] updateRank = new int[MAX_LEVEL];

  // ===== descent =====

  /**
   * Descends from the head for {@code key}, advancing while the next node's key {@code < key} (or
   * {@code <= key} when {@code allowEqual}), and leaves the terminal state in {@link #lastTerminal}
   * / {@link #lastRank}: the last node satisfying the predicate and its index ({@code -1} for the
   * head). Returns the first node <b>not</b> satisfying it, or {@code null} at the tail.
   */
  private Node descend(long key, boolean allowEqual) {
    Node x = head;
    int index = -1;
    for (int i = topLevel - 1; i >= 0; i--) {
      Node next = x.next[i];
      while (next != null && (next.key < key || (allowEqual && next.key == key))) {
        index += x.span[i];
        x = next;
        next = x.next[i];
      }
    }
    lastTerminal = x;
    lastRank = index;
    return x.next[0];
  }

  /** Terminal node of the last {@link #descend} (the head when nothing satisfied the predicate). */
  private Node lastTerminal;

  /** Key-order index of {@link #lastTerminal}; {@code -1} when it is the head. */
  private int lastRank;

  // ===== queries (all O(log n)) =====

  int size() {
    return size;
  }

  boolean isEmpty() {
    return size == 0;
  }

  /** The node with exactly {@code key}, or {@code null}. */
  Node find(long key) {
    Node node = descend(key, false);
    return node != null && node.key == key ? node : null;
  }

  /**
   * 0-based key-order index of the first node with key &gt;= {@code bound}; {@code size()} when all
   * keys are below it. With spans this is one descent, not a range walk.
   */
  int indexAtOrAfter(long bound) {
    descend(bound, false);
    return lastRank + 1;
  }

  /** 0-based key-order index of the first node with key &gt; {@code bound}. */
  int indexAfter(long bound) {
    descend(bound, true);
    return lastRank + 1;
  }

  /** The largest key strictly below {@code bound}, or {@link Long#MIN_VALUE} when none. */
  long lastKeyBelow(long bound) {
    descend(bound, false);
    return lastTerminal == head ? Long.MIN_VALUE : lastTerminal.key;
  }

  /** The first node in key order, or {@code null} when empty. */
  Node first() {
    return head.next[0];
  }

  /** The first node with key &gt;= {@code bound}, or {@code null} when none. */
  Node firstAtOrAfter(long bound) {
    return descend(bound, false);
  }

  /** The first node with key &gt; {@code bound}, or {@code null} when none. */
  Node firstAfter(long bound) {
    return descend(bound, true);
  }

  // ===== write =====

  /**
   * Inserts the key (the caller guarantees it is absent - the merge case never reaches here) at
   * key-order index {@code m}, splicing it into every level of its chain and adjusting the spans of
   * the links that now skip it. O(log n) expected.
   */
  void insert(long key, int slot) {
    int level = randomLevel();
    if (level > topLevel) {
      // Grow before descending so the loop below records the (still empty) new levels' update path.
      head.next = Arrays.copyOf(head.next, level);
      head.span = Arrays.copyOf(head.span, level);
      topLevel = level;
    }
    Node node = new Node(key, slot, level);
    // Descend with the strict predicate "< key", recording the predecessor chain and its indices.
    Node x = head;
    int index = -1;
    for (int i = topLevel - 1; i >= 0; i--) {
      Node next = x.next[i];
      while (next != null && next.key < key) {
        index += x.span[i];
        x = next;
        next = x.next[i];
      }
      update[i] = x;
      updateRank[i] = index;
    }
    int m = index + 1;
    for (int i = 0; i < level; i++) {
      if (update[i].next[i] == null) {
        // The new node ends this level's chain: no target, span unused (null links are never read).
        node.next[i] = null;
        node.span[i] = 0;
      } else {
        node.next[i] = update[i].next[i];
        // X->T span: T's key-order index shifts past X (its pre-insert index was
        // updateRank[i] + span[i], +1 for the insertion), X sits at m, so the link X->T now
        // spans updateRank[i] + span[i] + 1 - m positions. Omitting the +1 (the trap this
        // formula fell into) silently undercounts every rank query crossing the link.
        node.span[i] = update[i].span[i] - (m - updateRank[i]) + 1;
      }
      update[i].next[i] = node;
      update[i].span[i] = m - updateRank[i];
    }
    // Links above the node's level skip one more node; spans of null links stay write-only.
    for (int i = level; i < topLevel; i++) {
      update[i].span[i] += 1;
    }
    size++;
    nodeBytes += nodeHeapBytes(level);
  }

  private static int randomLevel() {
    int level = 1;
    while (level < MAX_LEVEL && ThreadLocalRandom.current().nextDouble() < LEVEL_PROBABILITY) {
      level++;
    }
    return level;
  }

  // ===== memory estimation (exact, maintained incrementally) =====

  /**
   * Exact heap bytes of one node with {@code arrayLength}-long link arrays: the node object (header
   * + key + slot + the two array refs) plus both arrays, each 8-aligned. Called with the node's
   * level for real nodes and with {@link #topLevel} for the head (same shape).
   */
  private static long nodeHeapBytes(int arrayLength) {
    return RamUsageEstimator.alignObjectSize(
            RamUsageEstimator.NUM_BYTES_OBJECT_HEADER
                + Long.BYTES
                + Integer.BYTES
                + 2L * NUM_BYTES_OBJECT_REF)
        + RamUsageEstimator.alignObjectSize(
            NUM_BYTES_ARRAY_HEADER + arrayLength * (long) NUM_BYTES_OBJECT_REF)
        + RamUsageEstimator.alignObjectSize(
            NUM_BYTES_ARRAY_HEADER + arrayLength * (long) Integer.BYTES);
  }

  /**
   * Exact heap bytes this list retains: all nodes (running sum), the head node with its current
   * arrays, and the fixed {@code MAX_LEVEL}-long descent scratch arrays. The dominant term is the
   * per-node cost; with p=1/4 a node's expected link count is 4/3, i.e. roughly 80-90 bytes per
   * indexed row on a compressed-oops JVM (the alignment floor of a level-1 node dominates: the node
   * object plus one-element next/span arrays).
   */
  long ramSizeEstimate() {
    return nodeBytes
        + nodeHeapBytes(topLevel)
        + RamUsageEstimator.alignObjectSize(
            NUM_BYTES_ARRAY_HEADER + (long) MAX_LEVEL * NUM_BYTES_OBJECT_REF)
        + RamUsageEstimator.alignObjectSize(
            NUM_BYTES_ARRAY_HEADER + (long) MAX_LEVEL * Integer.BYTES);
  }
}
