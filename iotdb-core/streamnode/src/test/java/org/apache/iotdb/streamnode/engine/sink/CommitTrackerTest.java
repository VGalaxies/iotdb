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

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class CommitTrackerTest {

  private final List<Long> committed = new ArrayList<>();

  private CommitTracker newTracker() {
    return new CommitTracker(committed::add);
  }

  /**
   * When the leading tablets complete out of order, a single {@code onTabletGroupDone} call must
   * advance {@code minTabletId} past every consecutive completed tablet, not just the first one, so
   * the window destroy can commit all of them.
   */
  @Test
  public void testMinTabletIdAdvancesThroughConsecutiveCompletedTablets() {
    CommitTracker tracker = newTracker();

    tracker.registerCommitId(1L, 1);
    tracker.registerCommitId(2L, 1);

    // Window spans tablets 1 and 2; both must be recorded via onWindowOpen so destroy allow = max.
    tracker.onWindowOpen(0L, 1L);
    tracker.onWindowOpen(0L, 2L);

    // Tablet 2 finishes before 1: the watermark cannot move past the still-incomplete 1.
    tracker.onTabletGroupDone(2L);
    Assert.assertTrue(committed.isEmpty());

    // Tablet 1 finishes: 1 and 2 are both complete and must be consumed in one step.
    tracker.onTabletGroupDone(1L);

    // The window destroy triggers the commit; it must advance past both completed tablets.
    tracker.onWindowDestroy(0L);
    Assert.assertEquals(Collections.singletonList(2L), committed);
  }

  /**
   * Empty tablets are registered with count 0 so the consecutive watermark can drain them without
   * waiting for {@code onTabletGroupDone}. A later group-done on an empty id is underflow.
   */
  @Test
  public void testEmptyTabletRegistersAsZeroAndDrains() {
    CommitTracker tracker = newTracker();

    tracker.registerCommitId(1L, 0);
    tracker.registerCommitId(2L, -1); // treated as 0
    tracker.registerCommitId(3L, 1);
    tracker.onTabletGroupDone(3L);

    tracker.onWindowOpen(0L, 3L);
    tracker.onWindowDestroy(0L);
    Assert.assertEquals(Collections.singletonList(3L), committed);

    try {
      tracker.onTabletGroupDone(1L);
      Assert.fail("expected IllegalStateException for an already-drained empty tablet");
    } catch (IllegalStateException expected) {
      // expected: id 1 was removed after draining, so it looks unregistered
    }
  }

  /**
   * A later tablet finishing first must not advance {@code minTabletId} past an earlier id that is
   * not yet registered. Regression for the allocate-vs-register hole race.
   */
  @Test
  public void testUnregisteredEarlierTabletBlocksLaterCommit() {
    CommitTracker tracker = newTracker();

    // Tablet 2 registers and completes while tablet 1 has not been registered yet.
    tracker.registerCommitId(2L, 1);
    tracker.onTabletGroupDone(2L);
    tracker.onWindowOpen(0L, 2L);
    tracker.onWindowDestroy(0L);
    Assert.assertTrue(committed.isEmpty());

    // Tablet 1 catching up raises prefixDone; source commit must retry here (no further window
    // event is required once window allow already permits 2).
    tracker.registerCommitId(1L, 1);
    tracker.onTabletGroupDone(1L);
    Assert.assertEquals(Collections.singletonList(2L), committed);
  }

  /**
   * An empty earlier tablet registered after a later window already flushed must itself trigger
   * source commit: empty tablets never open a window, so waiting for another destroy would stall.
   */
  @Test
  public void testEmptyTabletCatchUpAfterWindowDestroyCommits() {
    CommitTracker tracker = newTracker();

    tracker.registerCommitId(2L, 1);
    tracker.onTabletGroupDone(2L);
    tracker.onWindowOpen(0L, 2L);
    tracker.onWindowDestroy(0L);
    Assert.assertTrue(committed.isEmpty());

    tracker.registerCommitId(1L, 0);
    Assert.assertEquals(Collections.singletonList(2L), committed);
  }

  @Test
  public void testRegisterCommitIdTwiceThrows() {
    CommitTracker tracker = newTracker();
    tracker.registerCommitId(1L, 1);

    try {
      tracker.registerCommitId(1L, 1);
      Assert.fail("expected IllegalStateException for a duplicate registration");
    } catch (IllegalStateException expected) {
      // expected
    }
  }

  @Test
  public void testOnTabletGroupDoneForUnregisteredTabletThrows() {
    CommitTracker tracker = newTracker();

    try {
      tracker.onTabletGroupDone(42L);
      Assert.fail("expected IllegalStateException for an unregistered tablet");
    } catch (IllegalStateException expected) {
      // expected
    }
  }

  /**
   * A late smaller tablet in an already-open window must lower the open allow watermark so another
   * window's destroy cannot claim that smaller tablet is safe to commit before this window flushes.
   */
  @Test
  public void testLateSmallerTabletLowersOpenAllow() {
    CommitTracker tracker = newTracker();
    tracker.registerCommitId(1L, 1);
    tracker.registerCommitId(2L, 1);
    tracker.registerCommitId(3L, 1);
    tracker.onTabletGroupDone(1L);
    tracker.onTabletGroupDone(2L);
    tracker.onTabletGroupDone(3L);

    // Window A opens on tablet 2 first (out of order), open allow = minTabletId - 1 = 1.
    tracker.onWindowOpen(0L, 2L);
    // Window B covers tablet 3 and flushes; must stop at A's open allow = 1.
    tracker.onWindowOpen(1L, 3L);
    tracker.onWindowDestroy(1L);
    Assert.assertEquals(Collections.singletonList(1L), committed);

    // Tablet 1 arrives in A: minTabletId drops to 1, open allow = 0. No further commit until A
    // destroys.
    tracker.onWindowOpen(0L, 1L);
    Assert.assertEquals(Collections.singletonList(1L), committed);

    // Destroy raises allow to the window max (2), not the last-seen tablet (1).
    tracker.onWindowDestroy(0L);
    Assert.assertEquals(Arrays.asList(1L, 3L), committed);
  }

  @Test
  public void testOnWindowOpenAfterDestroyThrows() {
    CommitTracker tracker = newTracker();
    // Do not complete tablet groups so destroy cannot advance/prune; the destroyed state remains.
    tracker.onWindowOpen(0L, 1L);
    tracker.onWindowDestroy(0L);
    Assert.assertTrue(committed.isEmpty());

    try {
      tracker.onWindowOpen(0L, 2L);
      Assert.fail("expected IllegalStateException for a destroyed window");
    } catch (IllegalStateException expected) {
      // expected
    }
  }

  /**
   * Destroy raises allow to the max tablet observed via {@code onWindowOpen}, independent of event
   * order (tablets arrived high-then-low).
   */
  @Test
  public void testWindowDestroyUsesMaxTabletIdFromOpen() {
    CommitTracker tracker = newTracker();
    tracker.registerCommitId(1L, 1);
    tracker.registerCommitId(2L, 1);
    tracker.onTabletGroupDone(1L);
    tracker.onTabletGroupDone(2L);

    tracker.onWindowOpen(0L, 2L);
    tracker.onWindowOpen(0L, 1L); // lowers minTabletId to 1; max stays 2

    tracker.onWindowDestroy(0L);
    Assert.assertEquals(Collections.singletonList(2L), committed);
  }

  @Test
  public void testEventsForUnknownOrNegativeWindowsAreIgnored() {
    CommitTracker tracker = newTracker();

    tracker.onWindowDestroy(-1L);
    tracker.onWindowDestroy(7L);
    tracker.onWindowFailed(-1L);
    tracker.onWindowFailed(7L);

    Assert.assertTrue(committed.isEmpty());
  }

  @Test
  public void testCommitAdvancesOnWindowDestroy() {
    CommitTracker tracker = newTracker();
    tracker.registerCommitId(1L, 1);
    tracker.onTabletGroupDone(1L);
    tracker.onWindowOpen(0L, 1L);

    tracker.onWindowDestroy(0L);
    Assert.assertEquals(Collections.singletonList(1L), committed);
  }

  /** A window may span several source tablets; its destroy commits all of them. */
  @Test
  public void testWindowDestroyCommitsEveryTabletOfTheWindow() {
    CommitTracker tracker = newTracker();
    tracker.registerCommitId(1L, 1);
    tracker.registerCommitId(2L, 1);
    tracker.registerCommitId(3L, 1);
    tracker.onTabletGroupDone(1L);
    tracker.onTabletGroupDone(2L);
    tracker.onTabletGroupDone(3L);
    // Window spans 1..3; each tablet must be recorded so destroy allow rises to max=3.
    tracker.onWindowOpen(0L, 1L);
    tracker.onWindowOpen(0L, 2L);
    tracker.onWindowOpen(0L, 3L);

    tracker.onWindowDestroy(0L);
    Assert.assertEquals(Collections.singletonList(3L), committed);
  }

  @Test
  public void testWindowsAdvanceCommitInOrder() {
    CommitTracker tracker = newTracker();
    tracker.registerCommitId(1L, 1);
    tracker.registerCommitId(2L, 1);
    tracker.registerCommitId(3L, 1);
    tracker.onTabletGroupDone(1L);
    tracker.onTabletGroupDone(2L);
    tracker.onTabletGroupDone(3L);

    tracker.onWindowOpen(0L, 1L);
    tracker.onWindowOpen(1L, 2L);
    tracker.onWindowOpen(2L, 3L);

    tracker.onWindowDestroy(0L);
    Assert.assertEquals(Collections.singletonList(1L), committed);
    tracker.onWindowDestroy(1L);
    Assert.assertEquals(Arrays.asList(1L, 2L), committed);
    tracker.onWindowDestroy(2L);
    Assert.assertEquals(Arrays.asList(1L, 2L, 3L), committed);
  }

  @Test
  public void testOutOfOrderWindowDoesNotCommitPastGap() {
    CommitTracker tracker = newTracker();
    tracker.registerCommitId(1L, 1);
    tracker.registerCommitId(2L, 1);
    tracker.onTabletGroupDone(1L);
    tracker.onTabletGroupDone(2L);

    tracker.onWindowOpen(0L, 1L);
    tracker.onWindowOpen(1L, 2L);

    tracker.onWindowDestroy(1L);
    // Window 0 has not cleared its allow watermark yet, so commit must not jump to 2.
    Assert.assertTrue(committed.isEmpty());

    // Destroying the blocker must also drain the already-destroyed W2 in the same call.
    tracker.onWindowDestroy(0L);
    Assert.assertEquals(Collections.singletonList(2L), committed);
  }

  /**
   * When a later window flushes before an earlier one, destroying the earlier window must cascade:
   * advance past the blocker, prune it, then advance again through the already-destroyed tail.
   * Without the cascade, commit would stick at the first step and never see another EOF for the
   * later window.
   */
  @Test
  public void testDestroyCascadesThroughAlreadyDestroyedLaterWindows() {
    CommitTracker tracker = newTracker();
    tracker.registerCommitId(1L, 1);
    tracker.registerCommitId(2L, 1);
    tracker.onTabletGroupDone(1L);
    tracker.onTabletGroupDone(2L);

    tracker.onWindowOpen(0L, 1L); // W1 allow open = 0
    tracker.onWindowOpen(1L, 2L); // W2 allow open = 1

    tracker.onWindowDestroy(1L); // W2 allow = 2, gated by W1
    Assert.assertTrue(committed.isEmpty());

    tracker.onWindowDestroy(0L); // advance 1, prune W1, then advance 2 and prune W2
    Assert.assertEquals(Collections.singletonList(2L), committed);
  }

  /**
   * An open window must not be pruned just because the committed watermark already covers its open
   * allow: its later destroy is what commits the rows it contains. Regression guard for the {@code
   * removeRedundantWindows} pruning.
   */
  @Test
  public void testOpenWindowIsNotPrunedAfterEarlierWindowCommits() {
    CommitTracker tracker = newTracker();
    tracker.registerCommitId(1L, 1);
    tracker.registerCommitId(2L, 1);
    tracker.onTabletGroupDone(1L);
    tracker.onTabletGroupDone(2L);

    tracker.onWindowOpen(0L, 1L);
    tracker.onWindowOpen(1L, 2L);

    // Committing window 0 raises the watermark to 1, which equals window 1's open allow. Window 1
    // is still open, so it must survive and its later destroy must commit tablet 2.
    tracker.onWindowDestroy(0L);
    Assert.assertEquals(Collections.singletonList(1L), committed);

    tracker.onWindowDestroy(1L);
    Assert.assertEquals(Arrays.asList(1L, 2L), committed);
  }

  @Test
  public void testOnWindowFailedRemovesGateSoLaterWindowCanAdvance() {
    CommitTracker tracker = newTracker();
    tracker.registerCommitId(1L, 1);
    tracker.registerCommitId(2L, 1);
    tracker.onTabletGroupDone(1L);
    tracker.onTabletGroupDone(2L);

    tracker.onWindowOpen(0L, 1L);
    tracker.onWindowOpen(1L, 2L);

    // Window 1's destroy is gated by the still-open window 0.
    tracker.onWindowDestroy(1L);
    Assert.assertTrue(committed.isEmpty());

    // Failing window 0 removes the gate and must cascade through already-destroyed window 1.
    tracker.onWindowFailed(0L);
    Assert.assertEquals(Collections.singletonList(2L), committed);
  }

  @Test
  public void testDuplicateDestroyAfterWindowIsPrunedIsIgnored() {
    CommitTracker tracker = newTracker();
    tracker.registerCommitId(1L, 1);
    tracker.onTabletGroupDone(1L);
    tracker.onWindowOpen(0L, 1L);

    tracker.onWindowDestroy(0L);
    Assert.assertEquals(Collections.singletonList(1L), committed);

    // The window was pruned after committing, so a late duplicate destroy is a no-op.
    tracker.onWindowDestroy(0L);
    Assert.assertEquals(Collections.singletonList(1L), committed);
  }
}
