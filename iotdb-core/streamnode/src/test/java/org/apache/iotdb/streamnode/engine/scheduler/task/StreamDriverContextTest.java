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
import org.apache.iotdb.streamnode.engine.task.StreamSubTaskContext;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class StreamDriverContextTest {

  @Mock private StreamSubTaskContext mockSubTaskContext;

  private StreamDriverContext driverContext;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    when(mockSubTaskContext.getTaskName()).thenReturn("test-task");
    final PartitionKey partitionKey = new ListPartitionKey(Collections.singletonList("seg1"));
    when(mockSubTaskContext.getPartitionKey()).thenReturn(partitionKey);
    when(mockSubTaskContext.isDone()).thenReturn(false);
    driverContext = new StreamDriverContext(mockSubTaskContext);
  }

  @Test
  public void testConstructorBuildsDriverTaskIdFromSubTaskContext() {
    final DriverTaskId id = driverContext.getDriverTaskId();
    assertEquals("test-task", id.getId());
    // DriverTaskId has no getPartitionKey() getter; verify the key was embedded via fullId.
    assertTrue(id.getFullId().contains("seg1"));
  }

  @Test(expected = NullPointerException.class)
  public void testConstructorRejectsNullSubTaskContext() {
    new StreamDriverContext(null);
  }

  @Test
  public void testFailedDelegatesToSubTaskContextOnFirstCall() {
    final Throwable cause = new RuntimeException("boom");
    driverContext.failed(cause);
    verify(mockSubTaskContext, times(1)).failed(cause);
  }

  @Test
  public void testFailedOnlyDelegatesOnceDueToCas() {
    final Throwable cause = new RuntimeException("boom");
    driverContext.failed(cause);
    driverContext.failed(cause);
    driverContext.failed(cause);
    // CAS: only the first call should propagate to subTaskContext
    verify(mockSubTaskContext, times(1)).failed(cause);
  }

  @Test
  public void testFinishedSetsIsDone() {
    assertFalse(driverContext.isDone());
    driverContext.finished();
    assertTrue(driverContext.isDone());
  }

  @Test
  public void testIsDoneReturnsTrueWhenSubTaskContextDone() {
    assertFalse(driverContext.isDone());
    when(mockSubTaskContext.isDone()).thenReturn(true);
    assertTrue(driverContext.isDone());
  }

  @Test
  public void testMarkTaskClosingAndIsTaskClosing() {
    assertFalse(driverContext.isTaskClosing());
    driverContext.markTaskClosing();
    assertTrue(driverContext.isTaskClosing());
    // Idempotent: calling again doesn't flip back
    driverContext.markTaskClosing();
    assertTrue(driverContext.isTaskClosing());
  }

  @Test
  public void testSetDriverTaskId() {
    final DriverTaskId newId =
        new DriverTaskId("another-task", new ListPartitionKey(Arrays.asList("x", "y")));
    driverContext.setDriverTaskId(newId);
    assertEquals(newId, driverContext.getDriverTaskId());
  }

  @Test
  public void testGetPipelineIdAlwaysZero() {
    assertEquals(0, driverContext.getPipelineId());
  }

  @Test
  public void testGetSubTaskContextReturnsConstructorContext() {
    assertEquals(mockSubTaskContext, driverContext.getSubTaskContext());
  }

  @Test
  public void testFailedAfterFinishedDoesNotDelegateAgain() {
    driverContext.finished();
    assertTrue(driverContext.isDone());
    final Throwable cause = new RuntimeException("late");
    driverContext.failed(cause);
    // finished() set the flag, so failed()'s CAS should fail and not delegate
    verify(mockSubTaskContext, never()).failed(cause);
  }

  @Test
  public void testFailedWithNullCauseDoesNotThrow() {
    // The implementation doesn't validate null; it just passes it through.
    driverContext.failed(null);
    verify(mockSubTaskContext, times(1)).failed(null);
  }

  @Test
  public void testIsDoneWithSubTaskContextException() {
    // Verify isDone reads from subTaskContext each call (not cached).
    when(mockSubTaskContext.isDone()).thenReturn(false);
    assertFalse(driverContext.isDone());
    when(mockSubTaskContext.isDone()).thenReturn(true);
    assertTrue(driverContext.isDone());
  }

  @Test(expected = NullPointerException.class)
  public void testConstructorWithNullPartitionKeyFromSubTaskContext() {
    // DriverTaskId's constructor calls partitionKey.toString() unconditionally, so a null
    // partitionKey from subTaskContext propagates NPE out of StreamDriverContext's constructor.
    when(mockSubTaskContext.getPartitionKey()).thenReturn(null);
    new StreamDriverContext(mockSubTaskContext);
  }
}
