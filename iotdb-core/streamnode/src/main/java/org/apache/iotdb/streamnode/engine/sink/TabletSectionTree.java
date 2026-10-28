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

package org.apache.iotdb.streamnode.engine.sink;

/**
 * Dynamic segment tree over tabletIds {@code [1, UNIVERSE_RIGHT]}.
 *
 * <p>Each leaf stores the remaining partition-group count for one tabletId. Each node stores {@code
 * prefixDone}: the highest id such that every tablet in {@code [L, prefixDone]} is complete (count
 * 0). Missing children count as incomplete, so an unregistered earlier id blocks the prefix.
 *
 * <p>Not thread-safe; callers must serialize access (e.g. {@link CommitTracker}'s lock). When a
 * section is fully complete, children are pruned to reclaim space.
 */
final class TabletSectionTree {

  /** Inclusive right end of the id universe ({@link Long#MAX_VALUE}). */
  static final long UNIVERSE_RIGHT = Long.MAX_VALUE;

  private Node root;

  /**
   * Highest tabletId such that tablets {@code 1..prefixDone} are all complete; {@code 0} if none.
   */
  long prefixDone() {
    return root == null ? 0L : root.prefixDone;
  }

  /**
   * Registers {@code id} with remaining {@code count} (0 means already complete / empty tablet).
   *
   * @throws IllegalStateException if {@code id} was already registered or lies in a fully completed
   *     pruned section
   */
  void register(long id, int count) {
    checkId(id);
    if (count < 0) {
      throw new IllegalArgumentException("count must be >= 0: " + count);
    }
    root = register(root, 1L, UNIVERSE_RIGHT, id, count);
  }

  /**
   * Decrements the remaining group count for {@code id}.
   *
   * @throws IllegalStateException if {@code id} is not an active registered tablet
   */
  void decrement(long id) {
    checkId(id);
    root = decrement(root, 1L, UNIVERSE_RIGHT, id);
  }

  private static void checkId(long id) {
    if (id < 1L || id > UNIVERSE_RIGHT) {
      throw new IllegalArgumentException("tabletId out of range: " + id);
    }
  }

  private static Node register(Node node, long L, long R, long id, int count) {
    if (node != null && isFullyDoneCompressed(node, R)) {
      throw new IllegalStateException("commitId already registered: " + id);
    }
    if (L == R) {
      if (node != null) {
        throw new IllegalStateException("commitId already registered: " + id);
      }
      Node leaf = new Node();
      leaf.remaining = count;
      leaf.prefixDone = count == 0 ? id : id - 1;
      return leaf;
    }
    if (node == null) {
      node = new Node();
      node.prefixDone = L - 1;
    }
    long mid = L + (R - L) / 2;
    if (id <= mid) {
      node.left = register(node.left, L, mid, id, count);
    } else {
      node.right = register(node.right, mid + 1, R, id, count);
    }
    pullUp(node, L, R);
    return node;
  }

  private static Node decrement(Node node, long L, long R, long id) {
    if (node == null || isFullyDoneCompressed(node, R)) {
      throw new IllegalStateException("commitId not registered: " + id);
    }
    if (L == R) {
      if (node.remaining <= 0) {
        throw new IllegalStateException("group count underflow for commitId " + id);
      }
      node.remaining--;
      node.prefixDone = node.remaining == 0 ? id : id - 1;
      return node;
    }
    long mid = L + (R - L) / 2;
    if (id <= mid) {
      node.left = decrement(node.left, L, mid, id);
    } else {
      node.right = decrement(node.right, mid + 1, R, id);
    }
    pullUp(node, L, R);
    return node;
  }

  /** Fully completed section whose children were pruned. */
  private static boolean isFullyDoneCompressed(Node node, long R) {
    return node.prefixDone == R && node.left == null && node.right == null;
  }

  private static void pullUp(Node node, long L, long R) {
    long mid = L + (R - L) / 2;
    long leftPrefix = node.left == null ? L - 1 : node.left.prefixDone;
    if (leftPrefix < mid) {
      node.prefixDone = leftPrefix;
    } else {
      long rightPrefix = node.right == null ? mid : node.right.prefixDone;
      node.prefixDone = rightPrefix;
    }
    if (node.prefixDone == R) {
      node.left = null;
      node.right = null;
    }
  }

  private static class Node {
    /** Remaining partition groups; meaningful on leaves only. */
    private int remaining;

    /** Highest id such that {@code [L, prefixDone]} in this section are all complete. */
    private long prefixDone;

    private Node left;
    private Node right;
  }
}
