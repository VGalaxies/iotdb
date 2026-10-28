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

import org.junit.Assert;
import org.junit.Test;

public class TabletSectionTreeTest {

  @Test
  public void testPrefixDoneStartsAtZero() {
    TabletSectionTree tree = new TabletSectionTree();
    Assert.assertEquals(0L, tree.prefixDone());
  }

  @Test
  public void testLaterCompleteDoesNotSkipUnregisteredEarlier() {
    TabletSectionTree tree = new TabletSectionTree();
    tree.register(2L, 1);
    tree.decrement(2L);
    Assert.assertEquals(0L, tree.prefixDone());

    tree.register(1L, 1);
    Assert.assertEquals(0L, tree.prefixDone());
    tree.decrement(1L);
    Assert.assertEquals(2L, tree.prefixDone());
  }

  @Test
  public void testEmptyTabletDrainsPrefix() {
    TabletSectionTree tree = new TabletSectionTree();
    tree.register(1L, 0);
    Assert.assertEquals(1L, tree.prefixDone());
    tree.register(2L, 0);
    Assert.assertEquals(2L, tree.prefixDone());
  }

  @Test
  public void testOutOfOrderCompletionAdvancesInOnePullUp() {
    TabletSectionTree tree = new TabletSectionTree();
    tree.register(1L, 1);
    tree.register(2L, 1);
    tree.register(3L, 1);

    tree.decrement(3L);
    tree.decrement(2L);
    Assert.assertEquals(0L, tree.prefixDone());

    tree.decrement(1L);
    Assert.assertEquals(3L, tree.prefixDone());
  }

  @Test
  public void testDuplicateRegisterThrows() {
    TabletSectionTree tree = new TabletSectionTree();
    tree.register(1L, 1);
    try {
      tree.register(1L, 1);
      Assert.fail("expected IllegalStateException");
    } catch (IllegalStateException expected) {
      // expected
    }
  }

  @Test
  public void testDecrementUnregisteredThrows() {
    TabletSectionTree tree = new TabletSectionTree();
    try {
      tree.decrement(1L);
      Assert.fail("expected IllegalStateException");
    } catch (IllegalStateException expected) {
      // expected
    }
  }

  @Test
  public void testRegisterAfterPrefixPruneThrows() {
    TabletSectionTree tree = new TabletSectionTree();
    tree.register(1L, 0);
    tree.register(2L, 0);
    Assert.assertEquals(2L, tree.prefixDone());
    try {
      tree.register(1L, 1);
      Assert.fail("expected IllegalStateException for pruned completed id");
    } catch (IllegalStateException expected) {
      // expected
    }
  }
}
