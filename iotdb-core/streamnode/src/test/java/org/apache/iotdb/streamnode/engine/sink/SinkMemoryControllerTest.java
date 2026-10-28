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

public class SinkMemoryControllerTest {

  /** Reservations are admitted up to the pipeline quota; anything beyond it is refused. */
  @Test
  public void testReserveUpToQuotaIsAdmittedAndBeyondRefused() {
    SinkMemoryController controller =
        new SinkMemoryController(1000L, GlobalSinkMemoryController.getInstance());

    Assert.assertTrue(controller.tryReserve(600L));
    // 600 + 401 would exceed the quota; 600 + 400 fills it exactly.
    Assert.assertFalse(controller.tryReserve(401L));
    Assert.assertTrue(controller.tryReserve(400L));
    Assert.assertEquals(1000L, controller.getUsedBytes());
    Assert.assertTrue(controller.release(1000L));
    Assert.assertEquals(0L, controller.getUsedBytes());
  }

  /**
   * A single reservation larger than the quota can never be admitted, even on an empty pipeline, so
   * it must fail hard instead of parking as temporary backpressure.
   */
  @Test
  public void testOversizedReservationIsRefusedEvenOnEmptyPipeline() {
    SinkMemoryController controller =
        new SinkMemoryController(100L, GlobalSinkMemoryController.getInstance());

    Assert.assertTrue(controller.exceedsAdmissionLimit(101L));
    Assert.assertFalse(controller.exceedsAdmissionLimit(100L));
    Assert.assertFalse(controller.tryReserve(101L));
    // The quota-sized reservation still succeeds afterwards.
    Assert.assertTrue(controller.tryReserve(100L));
    Assert.assertTrue(controller.release(100L));
  }

  /** Releasing memory that was never reserved is rejected by the global controller. */
  @Test
  public void testReleaseBelowReservedAmountIsRejected() {
    SinkMemoryController controller =
        new SinkMemoryController(1000L, GlobalSinkMemoryController.getInstance());

    Assert.assertTrue(controller.tryReserve(100L));
    // The global controller still holds other reservations, but releasing far more than reserved
    // must be refused and leave the pipeline accounting untouched.
    Assert.assertFalse(controller.release(Long.MAX_VALUE));
    Assert.assertEquals(100L, controller.getUsedBytes());
    Assert.assertTrue(controller.release(100L));
  }
}
