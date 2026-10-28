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
package org.apache.iotdb.streamnode.engine.task;

import org.apache.iotdb.commons.stream.ListPartitionKey;
import org.apache.iotdb.commons.stream.PartitionKey;

import org.junit.Test;

import java.util.Collections;
import java.util.concurrent.LinkedBlockingQueue;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class StreamSubTaskContextTest {

  private StreamSubTaskContext newContext() {
    PartitionKey partitionKey = new ListPartitionKey(Collections.emptyList());
    StreamSubTaskStateMachine stateMachine =
        new StreamSubTaskStateMachine("test-task", Runnable::run);
    return new StreamSubTaskContext("test-task", partitionKey, stateMachine);
  }

  @Test
  public void testGetTaskName() {
    StreamSubTaskContext context = newContext();
    assertEquals("test-task", context.getTaskName());
  }

  @Test
  public void testGetPartitionKey() {
    StreamSubTaskContext context = newContext();
    assertNotNull(context.getPartitionKey());
  }

  @Test
  public void testGetStateMachine() {
    StreamSubTaskContext context = newContext();
    assertNotNull(context.getStateMachine());
  }

  @Test
  public void testGetStateDelegatesToStateMachine() {
    StreamSubTaskContext context = newContext();
    assertEquals(StreamSubTaskState.CREATED, context.getState());
    context.getStateMachine().start();
    assertEquals(StreamSubTaskState.RUNNING, context.getState());
  }

  @Test
  public void testIsDoneDelegatesToStateMachine() {
    StreamSubTaskContext context = newContext();
    assertFalse(context.isDone());
    context.getStateMachine().start();
    assertFalse(context.isDone());
    context.failed(new RuntimeException("boom"));
    assertTrue(context.isDone());
  }

  @Test
  public void testFailedDelegatesToStateMachine() {
    StreamSubTaskContext context = newContext();
    RuntimeException cause = new RuntimeException("test-failure");
    context.failed(cause);
    assertTrue(context.getFailureCause().isPresent());
    assertEquals(cause, context.getFailureCause().get());
    assertEquals(StreamSubTaskState.FAILED, context.getState());
  }

  @Test
  public void testGetFailureCauseEmptyWhenNotFailed() {
    StreamSubTaskContext context = newContext();
    assertFalse(context.getFailureCause().isPresent());
  }

  @Test
  public void testGetFailureCauses() {
    StreamSubTaskContext context = newContext();
    LinkedBlockingQueue<Throwable> causes = context.getFailureCauses();
    assertNotNull(causes);
    assertTrue(causes.isEmpty());
    context.failed(new RuntimeException("cause1"));
    assertEquals(1, causes.size());
  }

  @Test
  public void testSignalDriverClosed() {
    StreamSubTaskContext context = newContext();
    context.signalDriverClosed();
    // releaseResourceWhenDriverClosed should return immediately after signal
    long start = System.nanoTime();
    context.releaseResourceWhenDriverClosed();
    long duration = System.nanoTime() - start;
    assertTrue(
        "releaseResourceWhenDriverClosed should return quickly after signal",
        duration < 1_000_000_000L);
  }

  @Test
  public void testReleaseResourceWhenDriverClosedInterrupted() throws Exception {
    StreamSubTaskContext context = newContext();
    // Signal before calling releaseResourceWhenDriverClosed so it returns immediately
    context.signalDriverClosed();
    // Should return without blocking
    context.releaseResourceWhenDriverClosed();
    // No exception means success
    org.junit.Assert.assertNotNull(context);
  }

  /** failed() records multiple causes in failureCauses queue. */
  @Test
  public void testFailedRecordsMultipleCauses() {
    StreamSubTaskContext context = newContext();
    context.failed(new RuntimeException("cause1"));
    context.failed(new RuntimeException("cause2"));
    java.util.concurrent.LinkedBlockingQueue<Throwable> causes = context.getFailureCauses();
    assertEquals(2, causes.size());
  }

  @Test(expected = NullPointerException.class)
  public void testConstructorRejectsNullTaskName() {
    new StreamSubTaskContext(
        null,
        new ListPartitionKey(Collections.emptyList()),
        new StreamSubTaskStateMachine("id", Runnable::run));
  }

  @Test(expected = NullPointerException.class)
  public void testConstructorRejectsNullPartitionKey() {
    new StreamSubTaskContext("task", null, new StreamSubTaskStateMachine("id", Runnable::run));
  }

  @Test(expected = NullPointerException.class)
  public void testConstructorRejectsNullStateMachine() {
    new StreamSubTaskContext("task", new ListPartitionKey(Collections.emptyList()), null);
  }
}
