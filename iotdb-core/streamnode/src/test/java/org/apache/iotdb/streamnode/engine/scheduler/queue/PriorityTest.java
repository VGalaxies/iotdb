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
package org.apache.iotdb.streamnode.engine.scheduler.queue;

import org.junit.Test;

import java.lang.reflect.Modifier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

public class PriorityTest {

  @Test
  public void testGetLastScheduledTimeReturnsConstructorValue() {
    final Priority priority = new Priority(12345L);
    assertEquals(12345L, priority.getLastScheduledTime());
  }

  @Test
  public void testGetLastScheduledTimeZero() {
    final Priority priority = new Priority(0L);
    assertEquals(0L, priority.getLastScheduledTime());
  }

  @Test
  public void testGetLastScheduledTimeNegative() {
    final Priority priority = new Priority(-1L);
    assertEquals(-1L, priority.getLastScheduledTime());
  }

  @Test
  public void testToStringContainsLastScheduledTime() {
    final Priority priority = new Priority(99L);
    final String str = priority.toString();
    assertTrue("toString should contain field name", str.contains("lastScheduledTime"));
    assertTrue("toString should contain value", str.contains("99"));
  }

  @Test
  public void testPriorityClassIsFinal() {
    // Priority is documented as @Immutable; class should be final to prevent mutable subclasses.
    assertTrue(
        "Priority class should be final for immutability",
        Modifier.isFinal(Priority.class.getModifiers()));
  }

  @Test
  public void testEqualsIsIdentityBased() {
    // Priority does not override equals; two equal-valued instances are not equals().
    final Priority p1 = new Priority(1L);
    final Priority p2 = new Priority(1L);
    assertNotSame(p1, p2);
    assertFalse(p1.equals(p2));
    assertTrue(p1.equals(p1));
  }

  @Test
  public void testHashCodeConsistentWithIdentity() {
    final Priority priority = new Priority(7L);
    assertEquals(System.identityHashCode(priority), priority.hashCode());
  }
}
