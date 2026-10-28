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

import java.util.Optional;

public class SinkEntryTest {

  @Test
  public void testConstructorRejectsNullBlock() {
    try {
      new SinkEntry(null, 0L, false, 0, 0L);
      Assert.fail("expected NullPointerException for a null block");
    } catch (NullPointerException expected) {
      // expected
    }
  }

  @Test
  public void testAccessorsExposeConstructorArguments() {
    SinkEntry entry = new SinkEntry(Optional.empty(), 5L, true, 3, 128L);

    Assert.assertFalse(entry.getTsBlock().isPresent());
    Assert.assertEquals(5L, entry.getWindowId());
    Assert.assertTrue(entry.isEof());
    Assert.assertEquals(3, entry.getSubTaskId());
    Assert.assertEquals(128L, entry.getMemorySizeInBytes());
    Assert.assertTrue(entry.getEnqueueTimeNanos() > 0L);
  }

  @Test
  public void testBackpressureFutureIsCreatedLazilyAndStable() {
    SinkEntry entry = new SinkEntry(Optional.empty(), 1L, false, 0, 0L);

    Assert.assertSame(entry.getOrCreateBackpressureFuture(), entry.getOrCreateBackpressureFuture());
    Assert.assertFalse(entry.getOrCreateBackpressureFuture().isDone());
  }
}
