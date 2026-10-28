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

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.TreeSet;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Tracks when a source tabletId (commitId) can be safely acknowledged.
 *
 * <p>Source commit is gated by two watermarks and may advance when either side catches up:
 *
 * <ul>
 *   <li>{@code minTabletId}: highest tabletId such that tablets {@code 1..minTabletId} have all
 *       reached remaining partition-group count 0. Maintained by {@link TabletSectionTree} so an
 *       unregistered earlier id cannot be skipped when a later tablet finishes first. Raised by
 *       {@link #registerCommitId} (empty tablets) and {@link #onTabletGroupDone}.
 *   <li>per-window allow: while open, {@code window.minTabletId - 1}; after destroy flush, {@code
 *       window.maxTabletId}. Open {@code minTabletId} is lowered when a later smaller tablet
 *       arrives in the same window. Raised by {@link #onWindowDestroy} / {@link #onWindowFailed}.
 * </ul>
 *
 * <p>{@code advanceTo = min(minTabletId, min(window allow))}. Empty tablets must be {@link
 * #registerCommitId registered} with group count 0 so the consecutive watermark can drain them.
 * Raising either watermark retries source commit, so a late tablet-prefix catch-up after windows
 * already flushed is not stuck waiting for another window event.
 */
public class CommitTracker {

  private final SourceCommitCallback callback;

  /**
   * Dynamic section tree over tabletIds. {@link TabletSectionTree#prefixDone()} is the consecutive
   * completion watermark ({@code minTabletId}).
   */
  private final TabletSectionTree tabletSectionTree = new TabletSectionTree();

  /** windowId -> open/write-back/allow state. */
  private final Map<Long, WindowState> windowStates = new HashMap<>();

  /** Same instances as in {@link #windowStates}, ordered by effective allow commit id. */
  private final TreeSet<WindowState> windowStateSet = new TreeSet<>();

  private volatile long committedId = 0;
  private final ReentrantLock lock = new ReentrantLock();

  public CommitTracker(SourceCommitCallback callback) {
    this.callback = Objects.requireNonNull(callback, "callback");
  }

  public long getCommittedId() {
    return committedId;
  }

  /**
   * Registers {@code commitId} with the actual partition-group count. {@code groupCount <= 0} marks
   * an empty tablet complete (stored as 0) and may advance the consecutive completion watermark and
   * source commit. Positive counts wait for {@link #onTabletGroupDone}.
   */
  public void registerCommitId(long commitId, int groupCount) {
    Long advanceTo = null;
    lock.lock();
    try {
      tabletSectionTree.register(commitId, Math.max(groupCount, 0));
      // Empty tablets (count 0) may raise prefixDone and unblock a stalled source commit.
      advanceTo = advanceCommittedId();
    } finally {
      lock.unlock();
    }
    fireCommitted(advanceTo);
  }

  /**
   * Marks one partition group finished scanning {@code commitId}. Decrements the remaining count;
   * when a leading run of tablets reaches count 0, {@link TabletSectionTree} advances the
   * consecutive watermark and source commit may proceed if window allow already permits it.
   */
  public void onTabletGroupDone(long commitId) {
    Long advanceTo = null;
    lock.lock();
    try {
      tabletSectionTree.decrement(commitId);
      advanceTo = advanceCommittedId();
    } finally {
      lock.unlock();
    }
    fireCommitted(advanceTo);
  }

  /**
   * Records that {@code windowId} received rows from {@code tabletId}.
   *
   * <p>First call creates {@link WindowState} with {@code minTabletId = maxTabletId = tabletId}.
   * Later calls on the same open window lower {@code minTabletId} and raise {@code maxTabletId}, so
   * out-of-order tablet arrival cannot leave an open allow ({@code minTabletId - 1}) that claims an
   * unflushed smaller tablet is safe to commit. Does not advance source commit here.
   */
  public void onWindowOpen(long windowId, long tabletId) {
    lock.lock();
    try {
      WindowState state = windowStates.get(windowId);
      if (state == null) {
        state = new WindowState(windowId, tabletId);
        windowStates.put(windowId, state);
        windowStateSet.add(state);
        return;
      }
      if (state.destroyed) {
        throw new IllegalStateException("window already destroyed: " + windowId);
      }
      final WindowState openState = state;
      openState.maxTabletId = Math.max(openState.maxTabletId, tabletId);
      if (tabletId < openState.minTabletId) {
        refreshWindowStateSet(openState, () -> openState.minTabletId = tabletId);
      }
    } finally {
      lock.unlock();
    }
  }

  /**
   * The window's destroy entry was flushed successfully: marks the window destroyed (allow becomes
   * the {@code maxTabletId} already tracked from {@link #onWindowOpen}) and tries to advance the
   * source commit. The destroy entry is the last entry of a window, so all of its rows have already
   * been flushed when this is called.
   */
  public void onWindowDestroy(long windowId) {
    if (windowId < 0) {
      return;
    }
    Long advanceTo = null;
    lock.lock();
    try {
      WindowState state = windowStates.get(windowId);
      if (state == null) {
        return;
      }
      refreshWindowStateSet(state, () -> state.destroyed = true);
      // Advance and prune in a loop: removing a blocking destroyed window can expose a higher
      // allow from a later window that already flushed out of order. Fire the callback once with
      // the final watermark after unlocking because it may block on IO.
      advanceTo = advanceCommittedId();
    } finally {
      lock.unlock();
    }
    fireCommitted(advanceTo);
  }

  /**
   * Marks the write-back of {@code windowId} as failed so it stops gating source commit.
   *
   * <p>Best-effort: the failed window's data is dropped. The window is removed from the ordered
   * allow set so later windows can still advance commit instead of being blocked forever by a
   * window that will never flush. A later destroy of this window is ignored by {@link
   * #onWindowDestroy}.
   */
  public void onWindowFailed(long windowId) {
    if (windowId < 0) {
      return;
    }
    Long advanceTo = null;
    lock.lock();
    try {
      WindowState state = windowStates.remove(windowId);
      if (state == null) {
        return;
      }
      windowStateSet.remove(state);
      // Removing a failed gate may expose already-destroyed later windows; cascade like destroy.
      advanceTo = advanceCommittedId();
    } finally {
      lock.unlock();
    }
    fireCommitted(advanceTo);
  }

  /**
   * Drops every window from commit gating without firing the commit callback.
   *
   * <p>Called when the owning task stops: the source subscription is gone, so no further commit can
   * need advancing, and the buffered/discarded entries of this task's windows must stop gating.
   * Relies on this tracker being task-scoped (one per {@code EventStreamTaskInstance}).
   */
  public void clearWindowStates() {
    lock.lock();
    try {
      windowStates.clear();
      windowStateSet.clear();
    } finally {
      lock.unlock();
    }
  }

  /**
   * Mutates fields that affect {@link WindowState#getAllowCommitId()} ordering. Removes {@code
   * state} from {@link #windowStateSet} before the mutation, then re-inserts it so ordering stays
   * valid.
   */
  private void refreshWindowStateSet(WindowState state, Runnable mutation) {
    windowStateSet.remove(state);
    mutation.run();
    windowStateSet.add(state);
  }

  /**
   * Advances {@link #committedId} while {@link #tryGetAdvanceTo()} reports a higher watermark,
   * pruning destroyed windows that are already covered. Must be called with {@link #lock} held.
   *
   * @return final watermark to report to {@link #callback}, or {@code null} if nothing advanced
   */
  private Long advanceCommittedId() {
    Long advanceTo = null;
    while (true) {
      Long next = tryGetAdvanceTo();
      if (next == null) {
        break;
      }
      committedId = next;
      advanceTo = next;
      removeRedundantWindows();
    }
    return advanceTo;
  }

  private void fireCommitted(Long advanceTo) {
    if (advanceTo != null) {
      callback.onCommitted(advanceTo);
    }
  }

  /**
   * Computes {@code advanceTo = min(tabletSectionTree.prefixDone(), min(window allow))}, where
   * open-window allow is {@code window.minTabletId - 1} and destroyed-window allow is {@code
   * window.maxTabletId}.
   *
   * @return candidate watermark if greater than {@link #committedId}, otherwise {@code null}
   */
  private Long tryGetAdvanceTo() {
    if (windowStateSet.isEmpty()) {
      return null;
    }
    long minAllowCommitId = windowStateSet.first().getAllowCommitId();
    long advanceTo = Math.min(tabletSectionTree.prefixDone(), minAllowCommitId);
    if (advanceTo <= committedId) {
      return null;
    }
    return advanceTo;
  }

  /**
   * Removes destroyed windows whose allow watermark is already covered by {@link #committedId}.
   * Still-open windows are kept even if their open allow is low, since their later destroy is what
   * raises the watermark for the rows they contain.
   */
  private void removeRedundantWindows() {
    while (!windowStateSet.isEmpty()) {
      WindowState state = windowStateSet.first();
      if (state.getAllowCommitId() > committedId || !state.destroyed) {
        break;
      }
      windowStateSet.pollFirst();
      windowStates.remove(state.windowId);
    }
  }

  /**
   * Per-window tablet range and write-back progress. Ordered by effective allow commit id then
   * {@code windowId} for {@link #windowStateSet}.
   */
  private static final class WindowState implements Comparable<WindowState> {
    private final long windowId;

    /** Lowest tabletId observed in this window. */
    private long minTabletId;

    /** Highest tabletId observed in this window. */
    private long maxTabletId;

    /** True once the window's destroy entry has been flushed. */
    private boolean destroyed = false;

    private WindowState(long windowId, long tabletId) {
      this.windowId = windowId;
      this.minTabletId = tabletId;
      this.maxTabletId = tabletId;
    }

    /**
     * Open windows gate commit at {@code minTabletId - 1}; destroyed windows raise the gate to
     * {@code maxTabletId}.
     */
    private long getAllowCommitId() {
      return destroyed ? maxTabletId : minTabletId - 1;
    }

    @Override
    public int compareTo(WindowState other) {
      int cmp = Long.compare(this.getAllowCommitId(), other.getAllowCommitId());
      if (cmp != 0) {
        return cmp;
      }
      return Long.compare(this.windowId, other.windowId);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof WindowState)) {
        return false;
      }
      WindowState other = (WindowState) o;
      return windowId == other.windowId
          && minTabletId == other.minTabletId
          && maxTabletId == other.maxTabletId
          && destroyed == other.destroyed;
    }

    @Override
    public int hashCode() {
      return Objects.hash(windowId, minTabletId, maxTabletId, destroyed);
    }
  }
}
