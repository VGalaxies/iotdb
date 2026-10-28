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

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class StreamSubTaskStateMachineTest {

  private StreamSubTaskStateMachine newMachine() {
    return new StreamSubTaskStateMachine("test-sub-task", Runnable::run);
  }

  @Test
  public void testInitialStateIsCreated() {
    StreamSubTaskStateMachine machine = newMachine();
    assertEquals(StreamSubTaskState.CREATED, machine.getState());
    assertFalse(machine.isDone());
  }

  @Test
  public void testStartFromCreated() {
    StreamSubTaskStateMachine machine = newMachine();
    assertTrue(machine.start());
    assertEquals(StreamSubTaskState.RUNNING, machine.getState());
  }

  @Test
  public void testStopFromRunning() {
    StreamSubTaskStateMachine machine = newMachine();
    machine.start();
    assertTrue(machine.stop());
    assertEquals(StreamSubTaskState.STOPPED, machine.getState());
  }

  @Test
  public void testStopFromCreated() {
    StreamSubTaskStateMachine machine = newMachine();
    assertTrue(machine.stop());
    assertEquals(StreamSubTaskState.STOPPED, machine.getState());
  }

  /** Critical: STOPPED is non-terminal, start() can transition STOPPED -> RUNNING. */
  @Test
  public void testRestartFromStopped() {
    StreamSubTaskStateMachine machine = newMachine();
    machine.start();
    machine.stop();
    assertEquals(StreamSubTaskState.STOPPED, machine.getState());

    assertTrue(machine.start());
    assertEquals(StreamSubTaskState.RUNNING, machine.getState());
  }

  @Test
  public void testMultipleStopStartCycles() {
    StreamSubTaskStateMachine machine = newMachine();
    for (int i = 0; i < 3; i++) {
      assertTrue(machine.start());
      assertEquals(StreamSubTaskState.RUNNING, machine.getState());
      assertTrue(machine.stop());
      assertEquals(StreamSubTaskState.STOPPED, machine.getState());
    }
  }

  @Test
  public void testMarkStoppingFromRunning() {
    StreamSubTaskStateMachine machine = newMachine();
    machine.start();
    assertTrue(machine.markStopping());
    assertEquals(StreamSubTaskState.STOPPING, machine.getState());
  }

  @Test
  public void testStopFromStopping() {
    StreamSubTaskStateMachine machine = newMachine();
    machine.start();
    machine.markStopping();
    assertTrue(machine.stop());
    assertEquals(StreamSubTaskState.STOPPED, machine.getState());
  }

  @Test
  public void testStartFromRunningReturnsFalse() {
    StreamSubTaskStateMachine machine = newMachine();
    machine.start();
    assertFalse(machine.start());
    assertEquals(StreamSubTaskState.RUNNING, machine.getState());
  }

  @Test
  public void testFailedIsTerminal() {
    StreamSubTaskStateMachine machine = newMachine();
    machine.start();
    assertTrue(machine.failed(new RuntimeException("boom")));
    assertEquals(StreamSubTaskState.FAILED, machine.getState());
    assertTrue(machine.isDone());
  }

  @Test
  public void testCannotStartFromFailed() {
    StreamSubTaskStateMachine machine = newMachine();
    machine.failed(new RuntimeException("boom"));
    assertFalse(machine.start());
    assertEquals(StreamSubTaskState.FAILED, machine.getState());
  }

  @Test
  public void testCannotStopFromFailed() {
    StreamSubTaskStateMachine machine = newMachine();
    machine.failed(new RuntimeException("boom"));
    assertFalse(machine.stop());
    assertEquals(StreamSubTaskState.FAILED, machine.getState());
  }

  @Test
  public void testDropIsTerminal() {
    StreamSubTaskStateMachine machine = newMachine();
    machine.start();
    assertTrue(machine.drop());
    assertEquals(StreamSubTaskState.DROPPED, machine.getState());
    assertTrue(machine.isDone());
  }

  @Test
  public void testCannotStartFromDropped() {
    StreamSubTaskStateMachine machine = newMachine();
    machine.drop();
    assertFalse(machine.start());
    assertEquals(StreamSubTaskState.DROPPED, machine.getState());
  }

  @Test
  public void testFailedRecordsCause() {
    StreamSubTaskStateMachine machine = newMachine();
    RuntimeException cause = new RuntimeException("test-cause");
    machine.failed(cause);
    assertTrue(machine.getFailureCause().isPresent());
    assertEquals(cause, machine.getFailureCause().get());
  }

  @Test
  public void testStopDoesNotMakeDone() {
    StreamSubTaskStateMachine machine = newMachine();
    machine.start();
    machine.stop();
    assertFalse(machine.isDone());
  }
}
