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
package org.apache.iotdb.streamnode.engine.scheduler.task;

import org.apache.iotdb.commons.stream.ListPartitionKey;
import org.apache.iotdb.commons.stream.PartitionKey;

import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class DriverTaskIdTest {

  private static PartitionKey key(final Object... values) {
    return new ListPartitionKey(Arrays.asList(values));
  }

  @Test
  public void testCompareToDifferentIdsCaseInsensitive() {
    final DriverTaskId a = new DriverTaskId("abc", key());
    final DriverTaskId b = new DriverTaskId("ABD", key());
    // "abc" vs "abd" (case-insensitive) -> "abc" < "abd"
    assertTrue(a.compareTo(b) < 0);
    assertTrue(b.compareTo(a) > 0);
  }

  @Test
  public void testCompareToSameIdBothPartitionKeysEqual() {
    final DriverTaskId a = new DriverTaskId("stream", key("seg1"));
    final DriverTaskId b = new DriverTaskId("stream", key("seg1"));
    assertEquals(0, a.compareTo(b));
  }

  @Test
  public void testCompareToSameIdDifferentPartitionKeys() {
    final DriverTaskId a = new DriverTaskId("stream", key("seg1"));
    final DriverTaskId b = new DriverTaskId("stream", key("seg2"));
    // partitionKey not equal -> return 1 (asymmetric, documented quirk)
    assertEquals(1, a.compareTo(b));
    assertEquals(1, b.compareTo(a));
  }

  @Test
  public void testCompareToSameIdEmptyVsNonEmptyPartitionKey() {
    final DriverTaskId empty =
        new DriverTaskId("stream", new ListPartitionKey(Collections.emptyList()));
    final DriverTaskId nonEmpty = new DriverTaskId("stream", key("seg"));
    // ListPartitionKey([]).equals(ListPartitionKey(["seg"])) -> false
    assertEquals(1, empty.compareTo(nonEmpty));
  }

  @Test
  public void testEqualsSameIdAndPartitionKey() {
    final DriverTaskId a = new DriverTaskId("stream", key("seg1"));
    final DriverTaskId b = new DriverTaskId("stream", key("seg1"));
    assertTrue(a.equals(b));
    assertEquals(a.hashCode(), b.hashCode());
  }

  @Test
  public void testEqualsDifferentId() {
    final DriverTaskId a = new DriverTaskId("stream1", key());
    final DriverTaskId b = new DriverTaskId("stream2", key());
    assertFalse(a.equals(b));
    assertNotEquals(a.hashCode(), b.hashCode());
  }

  @Test
  public void testEqualsDifferentPartitionKey() {
    final DriverTaskId a = new DriverTaskId("stream", key("seg1"));
    final DriverTaskId b = new DriverTaskId("stream", key("seg2"));
    assertFalse(a.equals(b));
  }

  @Test
  public void testEqualsNullAndOtherType() {
    final DriverTaskId a = new DriverTaskId("stream", key());
    assertFalse(a.equals(null));
    assertFalse(a.equals("not a DriverTaskId"));
  }

  @Test
  public void testGetFullIdWithNonNullStreamName() {
    final DriverTaskId id = new DriverTaskId("myStream", key("seg"));
    // fullId format: "<id>.<partitionKey.toString()>"
    assertEquals("myStream.ListPartitionKey{values=[seg]}", id.getFullId());
    assertEquals(id.getFullId(), id.toString());
  }

  @Test
  public void testGetFullIdWithNullStreamName() {
    // When id is null, fullId uses EMPTY_FULL_ID constant.
    final DriverTaskId id = new DriverTaskId(null, key("seg"));
    assertTrue(id.getFullId().startsWith("EmptyFullId."));
  }

  @Test
  public void testGetIdReturnsStreamName() {
    final DriverTaskId id = new DriverTaskId("myStream", key());
    assertEquals("myStream", id.getId());
  }

  @Test
  public void testGetIdReturnsNullWhenStreamNameIsNull() {
    final DriverTaskId id = new DriverTaskId(null, key());
    assertNull(id.getId());
  }

  // ==================== equals/hashCode/toString tests ====================

  /** equals with null returns false. */
  @Test
  public void testEqualsNullReturnsFalse() {
    final DriverTaskId id = new DriverTaskId("stream", key());
    assertFalse(id.equals(null));
  }

  /** equals with same object returns true. */
  @Test
  public void testEqualsSameObject() {
    final DriverTaskId id = new DriverTaskId("stream", key());
    assertTrue(id.equals(id));
  }

  /** equals with non-DriverTaskId object returns false. */
  @Test
  public void testEqualsNonDriverTaskIdReturnsFalse() {
    final DriverTaskId id = new DriverTaskId("stream", key());
    assertFalse(id.equals("not-a-task-id"));
  }

  /** hashCode is consistent with equals. */
  @Test
  public void testHashCodeConsistentWithEquals() {
    final DriverTaskId id1 = new DriverTaskId("stream", key());
    final DriverTaskId id2 = new DriverTaskId("stream", key());
    assertTrue(id1.equals(id2));
    assertEquals(id1.hashCode(), id2.hashCode());
  }

  /** toString returns fullId. */
  @Test
  public void testToStringReturnsFullId() {
    final DriverTaskId id = new DriverTaskId("stream", key());
    assertEquals(id.getFullId(), id.toString());
  }
}
