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

package org.apache.iotdb.streamnode.engine.computation.planner;

import org.apache.iotdb.calc.exception.MemoryNotEnoughException;
import org.apache.iotdb.streamnode.engine.computation.planner.memory.NotThreadSafeStreamNodeMemoryReservationManager;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

public class StreamExecutionPlannerComputeMemoryTest {

  private static final long MEMORY_BATCH_THRESHOLD = 1024L * 1024L;
  private static final StreamExecutionPlanner PLANNER = StreamExecutionPlanner.getInstance();

  private NotThreadSafeStreamNodeMemoryReservationManager manager;
  private long bytesHeldByTest;

  @After
  public void tearDown() {
    if (manager != null && manager.getReservedBytesInTotalForTest() > 0) {
      manager.releaseAllReservedMemory();
    }
    if (bytesHeldByTest > 0) {
      PLANNER.releaseToFreeMemoryForCompute(bytesHeldByTest);
      bytesHeldByTest = 0L;
    }
  }

  @Test
  public void testReserveMemoryFailsWhenInsufficient() {
    long free = PLANNER.getFreeMemoryForCompute();
    try {
      PLANNER.reserveFromFreeMemoryForCompute(free + 1024L, 0L, "test");
      Assert.fail("Expected insufficient compute memory");
    } catch (MemoryNotEnoughException expected) {
      // expected
    }
  }

  @Test
  public void testReserveMemorySucceedsWhenAvailable() {
    long request = getAvailableRequest();
    if (request <= 0) {
      return;
    }
    long freeBefore = PLANNER.getFreeMemoryForCompute();

    PLANNER.reserveFromFreeMemoryForCompute(request, 0L, "test");
    Assert.assertEquals(freeBefore - request, PLANNER.getFreeMemoryForCompute());
    bytesHeldByTest = request;
  }

  @Test
  public void testMemoryReservationManagerReserveAndRelease() {
    long request = getAvailableRequest();
    if (request <= 0) {
      return;
    }
    manager = new NotThreadSafeStreamNodeMemoryReservationManager("test");
    long freeBefore = PLANNER.getFreeMemoryForCompute();

    manager.reserveMemoryImmediately(request);
    Assert.assertEquals(request, manager.getReservedBytesInTotalForTest());
    Assert.assertEquals(freeBefore - request, PLANNER.getFreeMemoryForCompute());

    manager.releaseAllReservedMemory();
    Assert.assertEquals(0L, manager.getReservedBytesInTotalForTest());
    Assert.assertEquals(freeBefore, PLANNER.getFreeMemoryForCompute());
  }

  @Test
  public void testImmediateReservationRollback() {
    long request = getAvailableRequest();
    if (request <= 0) {
      return;
    }
    manager = new NotThreadSafeStreamNodeMemoryReservationManager("test");
    long freeBefore = PLANNER.getFreeMemoryForCompute();

    manager.reserveMemoryCumulatively(request);
    manager.releaseMemoryImmediately(request);
    manager.reserveMemoryImmediately();

    Assert.assertEquals(0L, manager.getReservedBytesInTotalForTest());
    Assert.assertEquals(freeBefore, PLANNER.getFreeMemoryForCompute());

    manager.reserveMemoryImmediately(request);
    manager.releaseMemoryImmediately(request);

    Assert.assertEquals(0L, manager.getReservedBytesInTotalForTest());
    Assert.assertEquals(freeBefore, PLANNER.getFreeMemoryForCompute());
  }

  @Test
  public void testFailedCumulativeReservationDoesNotRemainPending() {
    long freeBefore = PLANNER.getFreeMemoryForCompute();
    manager = new NotThreadSafeStreamNodeMemoryReservationManager("test");

    try {
      manager.reserveMemoryCumulatively(freeBefore + MEMORY_BATCH_THRESHOLD);
      Assert.fail("Expected insufficient compute memory");
    } catch (MemoryNotEnoughException expected) {
      // expected
    }

    manager.reserveMemoryImmediately();
    Assert.assertEquals(0L, manager.getReservedBytesInTotalForTest());
    Assert.assertEquals(freeBefore, PLANNER.getFreeMemoryForCompute());
  }

  @Test
  public void testOverReleaseDoesNotCorruptAccounting() {
    long request = getAvailableRequest();
    if (request <= 0) {
      return;
    }
    manager = new NotThreadSafeStreamNodeMemoryReservationManager("test");
    long freeBefore = PLANNER.getFreeMemoryForCompute();

    manager.reserveMemoryImmediately(request);
    try {
      manager.releaseMemoryImmediately(request + 1L);
      Assert.fail("Expected IllegalStateException");
    } catch (IllegalStateException expected) {
      // expected
    }

    Assert.assertEquals(request, manager.getReservedBytesInTotalForTest());
    Assert.assertEquals(freeBefore - request, PLANNER.getFreeMemoryForCompute());

    manager.releaseAllReservedMemory();
    Assert.assertEquals(0L, manager.getReservedBytesInTotalForTest());
    Assert.assertEquals(freeBefore, PLANNER.getFreeMemoryForCompute());
  }

  private long getAvailableRequest() {
    return Math.min(1024L, PLANNER.getFreeMemoryForCompute());
  }
}
