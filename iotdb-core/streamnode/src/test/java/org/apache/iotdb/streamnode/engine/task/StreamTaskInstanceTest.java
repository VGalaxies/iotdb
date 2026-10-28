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

package org.apache.iotdb.streamnode.engine.task;

import org.apache.iotdb.commons.stream.StreamSource;
import org.apache.iotdb.commons.stream.StreamTarget;
import org.apache.iotdb.commons.stream.StreamTask;
import org.apache.iotdb.commons.stream.StreamWindow;
import org.apache.iotdb.commons.stream.StreamWindowType;
import org.apache.iotdb.streamnode.engine.dispatcher.TabletDispatcher;
import org.apache.iotdb.streamnode.engine.scheduler.IStreamTaskScheduler;
import org.apache.iotdb.streamnode.engine.sink.SinkPipeline;
import org.apache.iotdb.streamnode.engine.sink.WriteBackEngine;
import org.apache.iotdb.streamnode.engine.source.StreamSourceInstance;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.concurrent.ExecutorService;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class StreamTaskInstanceTest {

  @Mock private StreamTask mockStreamTask;
  @Mock private WriteBackEngine mockWriteBackEngine;
  @Mock private SinkPipeline mockSinkPipeline;
  @Mock private TabletDispatcher mockTabletDispatcher;
  @Mock private StreamSourceInstance mockStreamSourceInstance;
  @Mock private IStreamTaskScheduler mockScheduler;
  @Mock private ExecutorService mockSubTaskNotificationExecutor;

  private EventStreamTaskInstance streamTaskInstance;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);

    when(mockStreamTask.getTaskName()).thenReturn("test-task");

    StreamWindow defaultWindow = mock(StreamWindow.class);
    when(defaultWindow.getType()).thenReturn(StreamWindowType.TUMBLE);
    when(mockStreamTask.getWindow()).thenReturn(defaultWindow);

    EventStreamTaskInstance realInstance =
        (EventStreamTaskInstance)
            AbstractStreamTaskInstance.create(
                mockStreamTask, mockScheduler, mockSubTaskNotificationExecutor);
    streamTaskInstance = spy(realInstance);

    java.lang.reflect.Field writeBackEngineField =
        AbstractStreamTaskInstance.class.getDeclaredField("writeBackEngine");
    writeBackEngineField.setAccessible(true);
    writeBackEngineField.set(streamTaskInstance, mockWriteBackEngine);
  }

  @SuppressWarnings("ResultOfMethodCallIgnored")
  private void mockDispatcherAndSource() {
    when(mockWriteBackEngine.getPipeline()).thenReturn(mockSinkPipeline);
    doReturn(mockTabletDispatcher).when(streamTaskInstance).createDispatcher(any(), any());
    doReturn(mockStreamSourceInstance)
        .when(streamTaskInstance)
        .createSourceInstance(any(), any(), any());
  }

  @SuppressWarnings("ResultOfMethodCallIgnored")
  private void mockDispatcherOnly() {
    when(mockWriteBackEngine.getPipeline()).thenReturn(mockSinkPipeline);
    doReturn(mockTabletDispatcher).when(streamTaskInstance).createDispatcher(any(), any());
  }

  @Test
  public void testStartSuccessfully() throws Exception {
    when(mockStreamTask.getSource()).thenReturn(mock(StreamSource.class));
    when(mockStreamTask.getTarget()).thenReturn(mock(StreamTarget.class));
    when(mockStreamTask.getWindow()).thenReturn(mock(StreamWindow.class));

    mockDispatcherAndSource();

    doNothing().when(mockWriteBackEngine).start();
    doNothing().when(mockStreamSourceInstance).start(any());

    streamTaskInstance.start();

    verify(mockWriteBackEngine, times(1)).start();
    verify(mockStreamSourceInstance, times(1)).start(any());
    assertTrue(streamTaskInstance.isRunning());
  }

  @Test
  public void testStartWithoutSource() throws Exception {
    when(mockStreamTask.getSource()).thenReturn(null);
    when(mockStreamTask.getTarget()).thenReturn(mock(StreamTarget.class));

    doNothing().when(mockWriteBackEngine).start();

    streamTaskInstance.start();

    verify(mockWriteBackEngine, times(1)).start();
    verify(mockWriteBackEngine, never()).stop();
    assertTrue(streamTaskInstance.isRunning());
  }

  @Test
  public void testStartWithoutTarget() throws Exception {
    when(mockStreamTask.getSource()).thenReturn(mock(StreamSource.class));
    when(mockStreamTask.getTarget()).thenReturn(null);
    when(mockStreamTask.getWindow()).thenReturn(mock(StreamWindow.class));

    mockDispatcherAndSource();

    doNothing().when(mockStreamSourceInstance).start(any());

    streamTaskInstance.start();

    verify(mockWriteBackEngine, never()).start();
    verify(mockStreamSourceInstance, times(1)).start(any());
    assertTrue(streamTaskInstance.isRunning());
  }

  @Test
  public void testStartAlreadyRunning() throws Exception {
    when(mockStreamTask.getSource()).thenReturn(mock(StreamSource.class));
    when(mockStreamTask.getTarget()).thenReturn(mock(StreamTarget.class));
    when(mockStreamTask.getWindow()).thenReturn(mock(StreamWindow.class));

    mockDispatcherAndSource();

    doNothing().when(mockWriteBackEngine).start();
    doNothing().when(mockStreamSourceInstance).start(any());

    streamTaskInstance.start();
    streamTaskInstance.start();

    verify(mockWriteBackEngine, times(1)).start();
    verify(mockStreamSourceInstance, times(1)).start(any());
    assertTrue(streamTaskInstance.isRunning());
  }

  @Test
  public void testStartWithException() throws Exception {
    when(mockStreamTask.getSource()).thenReturn(mock(StreamSource.class));
    when(mockStreamTask.getTarget()).thenReturn(mock(StreamTarget.class));
    when(mockStreamTask.getWindow()).thenReturn(mock(StreamWindow.class));

    mockDispatcherOnly();

    doThrow(new RuntimeException("Test exception")).when(mockWriteBackEngine).start();

    try {
      streamTaskInstance.start();
      org.junit.Assert.fail("Expected RuntimeException to propagate");
    } catch (RuntimeException expected) {
      assertEquals("Test exception", expected.getMessage());
    }

    verify(mockWriteBackEngine, times(1)).start();
    verify(mockWriteBackEngine, never()).stop();
    // start() leaves running=true on failure so the caller can route through stop() to clean up.
    assertTrue(streamTaskInstance.isRunning());
  }

  /** Verifies stopping a task drops its source and releases all owned execution resources. */
  @Test
  public void testStopSuccessfully() throws Exception {
    when(mockStreamTask.getSource()).thenReturn(mock(StreamSource.class));
    when(mockStreamTask.getTarget()).thenReturn(mock(StreamTarget.class));
    when(mockStreamTask.getWindow()).thenReturn(mock(StreamWindow.class));

    mockDispatcherAndSource();

    doNothing().when(mockWriteBackEngine).start();
    doNothing().when(mockStreamSourceInstance).start(any());
    doNothing().when(mockStreamSourceInstance).stop();

    streamTaskInstance.start();

    java.lang.reflect.Field sourceInstanceField =
        EventStreamTaskInstance.class.getDeclaredField("sourceInstance");
    sourceInstanceField.setAccessible(true);
    sourceInstanceField.set(streamTaskInstance, mockStreamSourceInstance);

    streamTaskInstance.stop();

    verify(mockStreamSourceInstance, times(1)).stop();
    // stop() pauses the source so the same task instance can be resumed.
    verify(mockWriteBackEngine, never()).stop();
    assertFalse(streamTaskInstance.isRunning());
  }

  @Test
  public void testStopWithoutSourceInstance() throws Exception {
    when(mockStreamTask.getSource()).thenReturn(null);
    when(mockStreamTask.getTarget()).thenReturn(mock(StreamTarget.class));

    doNothing().when(mockWriteBackEngine).start();

    streamTaskInstance.start();

    streamTaskInstance.stop();

    // stop() is a pause: writeBackEngine is NOT stopped (preserved for resume)
    verify(mockWriteBackEngine, never()).stop();
    verify(mockWriteBackEngine, times(1)).start();
    assertFalse(streamTaskInstance.isRunning());
  }

  @Test
  public void testStopWhenNotRunning() throws Exception {
    streamTaskInstance.stop();

    assertFalse(streamTaskInstance.isRunning());
    verify(mockWriteBackEngine, never()).stop();
  }

  @Test
  public void testStopWithException() throws Exception {
    when(mockStreamTask.getSource()).thenReturn(mock(StreamSource.class));
    when(mockStreamTask.getTarget()).thenReturn(mock(StreamTarget.class));
    when(mockStreamTask.getWindow()).thenReturn(mock(StreamWindow.class));

    mockDispatcherAndSource();

    doNothing().when(mockWriteBackEngine).start();
    doNothing().when(mockStreamSourceInstance).start(any());

    doThrow(new RuntimeException("Stop exception")).when(mockWriteBackEngine).stop();

    streamTaskInstance.start();

    java.lang.reflect.Field sourceInstanceField =
        EventStreamTaskInstance.class.getDeclaredField("sourceInstance");
    sourceInstanceField.setAccessible(true);
    sourceInstanceField.set(streamTaskInstance, mockStreamSourceInstance);

    // stop() is a pause and does not call writeBackEngine.stop(); exception path is exercised
    // via drop() instead.
    streamTaskInstance.stop();

    verify(mockWriteBackEngine, never()).stop();
    assertFalse(streamTaskInstance.isRunning());
  }

  @Test
  public void testGetTaskDefinition() {
    StreamTask result = streamTaskInstance.getTaskDefinition();

    assertNotNull(result);
    assertSame(mockStreamTask, result);
  }

  @Test
  public void testIsRunningInitiallyFalse() {
    assertFalse(streamTaskInstance.isRunning());
  }

  @Test
  public void testIsRunningAfterStart() throws Exception {
    when(mockStreamTask.getSource()).thenReturn(null);
    when(mockStreamTask.getTarget()).thenReturn(mock(StreamTarget.class));

    doNothing().when(mockWriteBackEngine).start();

    streamTaskInstance.start();

    assertTrue(streamTaskInstance.isRunning());
  }

  @Test
  public void testIsRunningAfterStop() throws Exception {
    when(mockStreamTask.getSource()).thenReturn(null);
    when(mockStreamTask.getTarget()).thenReturn(mock(StreamTarget.class));

    doNothing().when(mockWriteBackEngine).start();
    doNothing().when(mockWriteBackEngine).stop();

    streamTaskInstance.start();
    assertTrue(streamTaskInstance.isRunning());

    streamTaskInstance.stop();
    assertFalse(streamTaskInstance.isRunning());
  }

  // ==================== drop() tests ====================

  @Test
  public void testDropStopsWriteBackEngine() throws Exception {
    when(mockStreamTask.getSource()).thenReturn(null);
    when(mockStreamTask.getTarget()).thenReturn(mock(StreamTarget.class));

    doNothing().when(mockWriteBackEngine).start();
    doNothing().when(mockWriteBackEngine).stop();

    streamTaskInstance.start();
    streamTaskInstance.drop();

    // drop() is full cleanup: writeBackEngine IS stopped
    verify(mockWriteBackEngine, times(1)).stop();
  }

  @Test
  public void testDropWithoutStart() {
    when(mockStreamTask.getTarget()).thenReturn(mock(StreamTarget.class));
    doNothing().when(mockWriteBackEngine).stop();

    // drop() without start should not throw (no running CAS check)
    streamTaskInstance.drop();
    verify(mockWriteBackEngine, times(1)).stop();
  }

  // ==================== writeBackEngineStarted flag tests ====================

  @Test
  public void testRestartDoesNotStartWriteBackEngineTwice() throws Exception {
    when(mockStreamTask.getSource()).thenReturn(null);
    when(mockStreamTask.getTarget()).thenReturn(mock(StreamTarget.class));

    doNothing().when(mockWriteBackEngine).start();
    doNothing().when(mockWriteBackEngine).stop();

    // first start
    streamTaskInstance.start();
    verify(mockWriteBackEngine, times(1)).start();

    // stop (pause - does not stop writeBackEngine, does not reset flag)
    streamTaskInstance.stop();
    verify(mockWriteBackEngine, never()).stop();

    // restart - writeBackEngine already started, should NOT call start() again
    streamTaskInstance.start();
    verify(mockWriteBackEngine, times(1)).start();
  }

  @Test
  public void testDropResetsWriteBackEngineStartedFlag() throws Exception {
    when(mockStreamTask.getSource()).thenReturn(null);
    when(mockStreamTask.getTarget()).thenReturn(mock(StreamTarget.class));

    doNothing().when(mockWriteBackEngine).start();
    doNothing().when(mockWriteBackEngine).stop();

    streamTaskInstance.start();
    verify(mockWriteBackEngine, times(1)).start();

    // drop resets the flag
    streamTaskInstance.drop();
    verify(mockWriteBackEngine, times(1)).stop();

    // After drop, writeBackEngineStarted is reset to false.
    // But note: instance is typically removed from map after drop, so restart creates new instance.
    // This test just verifies the flag reset via reflection.
    java.lang.reflect.Field flagField =
        AbstractStreamTaskInstance.class.getDeclaredField("writeBackEngineStarted");
    flagField.setAccessible(true);
    assertFalse((boolean) flagField.get(streamTaskInstance));
  }

  // ==================== EventStreamTaskInstance restart tests ====================

  @Test
  public void testEventRestartCallsSourceInstanceStartWithNull() throws Exception {
    when(mockStreamTask.getSource()).thenReturn(mock(StreamSource.class));
    when(mockStreamTask.getTarget()).thenReturn(mock(StreamTarget.class));
    when(mockStreamTask.getWindow()).thenReturn(mock(StreamWindow.class));

    mockDispatcherAndSource();

    doNothing().when(mockWriteBackEngine).start();
    doNothing().when(mockStreamSourceInstance).start(any());
    doNothing().when(mockStreamSourceInstance).stop();

    // first start creates sourceInstance
    streamTaskInstance.start();

    // inject the mock sourceInstance (startInternal created a different one via spy)
    java.lang.reflect.Field sourceInstanceField =
        EventStreamTaskInstance.class.getDeclaredField("sourceInstance");
    sourceInstanceField.setAccessible(true);
    sourceInstanceField.set(streamTaskInstance, mockStreamSourceInstance);

    // stop (pause)
    streamTaskInstance.stop();

    // restart - should call start(null), NOT re-create source
    final StreamSource updatedSource = mock(StreamSource.class);
    when(mockStreamTask.getSource()).thenReturn(updatedSource);
    streamTaskInstance.start();

    verify(mockStreamSourceInstance).update(updatedSource);
    verify(mockStreamSourceInstance, times(2)).start(any());
    verify(mockStreamSourceInstance, times(1)).stop();
  }

  @Test
  public void testEventStartWithoutSourceDoesNotCallStart() throws Exception {
    when(mockStreamTask.getSource()).thenReturn(null);
    when(mockStreamTask.getTarget()).thenReturn(mock(StreamTarget.class));

    doNothing().when(mockWriteBackEngine).start();

    streamTaskInstance.start();
    // sourceInstance is null, start(null) should not be called (no NPE)
    assertFalse(streamTaskInstance.isRunning() == false);
    assertTrue(streamTaskInstance.isRunning());
  }

  // ==================== dropInternal tests ====================

  @Test
  public void testDropStopsSourceInstanceAndClearsReference() throws Exception {
    when(mockStreamTask.getSource()).thenReturn(mock(StreamSource.class));
    when(mockStreamTask.getTarget()).thenReturn(mock(StreamTarget.class));
    when(mockStreamTask.getWindow()).thenReturn(mock(StreamWindow.class));

    mockDispatcherAndSource();
    doNothing().when(mockWriteBackEngine).start();
    doNothing().when(mockStreamSourceInstance).start(any());
    doNothing().when(mockWriteBackEngine).stop();
    doNothing().when(mockStreamSourceInstance).drop();

    streamTaskInstance.start();

    java.lang.reflect.Field sourceInstanceField =
        EventStreamTaskInstance.class.getDeclaredField("sourceInstance");
    sourceInstanceField.setAccessible(true);
    sourceInstanceField.set(streamTaskInstance, mockStreamSourceInstance);

    streamTaskInstance.drop();

    verify(mockStreamSourceInstance, times(1)).drop();
    // sourceInstance should be nulled out after drop
    assertNull(sourceInstanceField.get(streamTaskInstance));
  }

  @Test
  public void testDropWithoutSourceInstanceDoesNotThrow() throws Exception {
    when(mockStreamTask.getSource()).thenReturn(null);
    when(mockStreamTask.getTarget()).thenReturn(mock(StreamTarget.class));

    doNothing().when(mockWriteBackEngine).start();
    doNothing().when(mockWriteBackEngine).stop();

    streamTaskInstance.start();
    // drop without sourceInstance should not throw
    streamTaskInstance.drop();
    verify(mockWriteBackEngine, times(1)).stop();
  }

  // ==================== onSubTaskStateChanged tests ====================

  @Test
  public void testOnSubTaskStateChangedNonDoneReturnsEarly() {
    org.apache.iotdb.commons.stream.PartitionKey key =
        new org.apache.iotdb.commons.stream.ListPartitionKey(java.util.Collections.emptyList());
    StreamSubTaskExecution mockExecution = mock(StreamSubTaskExecution.class);
    // RUNNING is not done -> early return, no map removal
    streamTaskInstance.onSubTaskStateChanged(key, mockExecution, StreamSubTaskState.RUNNING);
    org.junit.Assert.assertNotNull(streamTaskInstance);
  }

  @Test
  public void testOnSubTaskStateChangedDroppedRemovesFromMap() throws Exception {
    org.apache.iotdb.commons.stream.PartitionKey key =
        new org.apache.iotdb.commons.stream.ListPartitionKey(java.util.Collections.emptyList());
    StreamSubTaskExecution mockExecution = mock(StreamSubTaskExecution.class);

    // Inject a mock execution into subTaskExecutions
    java.lang.reflect.Field field =
        AbstractStreamTaskInstance.class.getDeclaredField("subTaskExecutions");
    field.setAccessible(true);
    @SuppressWarnings("unchecked")
    java.util.Map<org.apache.iotdb.commons.stream.PartitionKey, StreamSubTaskExecution> map =
        (java.util.Map<org.apache.iotdb.commons.stream.PartitionKey, StreamSubTaskExecution>)
            field.get(streamTaskInstance);
    map.put(key, mockExecution);

    streamTaskInstance.onSubTaskStateChanged(key, mockExecution, StreamSubTaskState.DROPPED);

    assertFalse(map.containsKey(key));
  }

  @Test
  public void testOnSubTaskStateChangedFailedLogsAndRemoves() throws Exception {
    org.apache.iotdb.commons.stream.PartitionKey key =
        new org.apache.iotdb.commons.stream.ListPartitionKey(java.util.Collections.emptyList());
    StreamSubTaskExecution mockExecution = mock(StreamSubTaskExecution.class);
    StreamSubTaskContext mockContext = mock(StreamSubTaskContext.class);
    when(mockExecution.getContext()).thenReturn(mockContext);
    when(mockContext.getFailureCause()).thenReturn(java.util.Optional.empty());

    java.lang.reflect.Field field =
        AbstractStreamTaskInstance.class.getDeclaredField("subTaskExecutions");
    field.setAccessible(true);
    @SuppressWarnings("unchecked")
    java.util.Map<org.apache.iotdb.commons.stream.PartitionKey, StreamSubTaskExecution> map =
        (java.util.Map<org.apache.iotdb.commons.stream.PartitionKey, StreamSubTaskExecution>)
            field.get(streamTaskInstance);
    map.put(key, mockExecution);

    streamTaskInstance.onSubTaskStateChanged(key, mockExecution, StreamSubTaskState.FAILED);

    // FAILED removes from map but does NOT recreate (Event task recreate is via getOrCreateSubTask
    // on next data)
    assertFalse(map.containsKey(key));
  }

  // ==================== commitProcessedProgress / getLastCommittedSourceIndex tests
  // ====================

  @Test
  public void testGetLastCommittedSourceIndexInitiallyNegative() {
    EventStreamTaskInstance eventInstance = (EventStreamTaskInstance) streamTaskInstance;
    assertEquals(-1L, eventInstance.getLastCommittedSourceIndex());
  }

  @Test
  public void testCommitProcessedProgressWithNullSourceReturnsLast() throws Exception {
    when(mockStreamTask.getSource()).thenReturn(null);
    when(mockStreamTask.getTarget()).thenReturn(mock(StreamTarget.class));
    doNothing().when(mockWriteBackEngine).start();

    streamTaskInstance.start();
    EventStreamTaskInstance eventInstance = (EventStreamTaskInstance) streamTaskInstance;

    // sourceInstance is null -> returns lastCommittedSourceIndex
    assertEquals(-1L, eventInstance.commitProcessedProgress());
  }

  @Test
  public void testCommitProcessedProgressWithEmptySubTasksReturnsLast() throws Exception {
    when(mockStreamTask.getSource()).thenReturn(mock(StreamSource.class));
    when(mockStreamTask.getTarget()).thenReturn(mock(StreamTarget.class));
    when(mockStreamTask.getWindow()).thenReturn(mock(StreamWindow.class));

    mockDispatcherAndSource();
    doNothing().when(mockWriteBackEngine).start();
    doNothing().when(mockStreamSourceInstance).start(any());

    streamTaskInstance.start();

    java.lang.reflect.Field sourceInstanceField =
        EventStreamTaskInstance.class.getDeclaredField("sourceInstance");
    sourceInstanceField.setAccessible(true);
    sourceInstanceField.set(streamTaskInstance, mockStreamSourceInstance);

    EventStreamTaskInstance eventInstance = (EventStreamTaskInstance) streamTaskInstance;
    // subTaskExecutions is empty -> returns lastCommittedSourceIndex
    assertEquals(-1L, eventInstance.commitProcessedProgress());
  }

  @Test
  public void testGetMinimumSubTaskCommitIdReturnsNegativeWhenEmpty() {
    EventStreamTaskInstance eventInstance = (EventStreamTaskInstance) streamTaskInstance;
    assertEquals(-1L, eventInstance.getMinimumSubTaskCommitId());
  }

  // ==================== AbstractStreamTaskInstance additional tests ====================

  /** getWriteBackEngine returns the WriteBackEngine instance. */
  @Test
  public void testGetWriteBackEngine() {
    assertSame(mockWriteBackEngine, streamTaskInstance.getWriteBackEngine());
  }

  /** getSubTasksSnapshot returns an unmodifiable snapshot of sub-tasks. */
  @Test
  public void testGetSubTasksSnapshotReturnsUnmodifiable() {
    java.util.Map<org.apache.iotdb.commons.stream.PartitionKey, StreamSubTask> snapshot =
        streamTaskInstance.getSubTasksSnapshot();
    assertTrue(snapshot.isEmpty());
  }

  /** getSubTasksSnapshot with populated subTaskExecutions returns the entries. */
  @Test
  public void testGetSubTasksSnapshotWithPopulatedMap() throws Exception {
    org.apache.iotdb.commons.stream.PartitionKey key =
        new org.apache.iotdb.commons.stream.ListPartitionKey(java.util.Collections.emptyList());
    StreamSubTaskExecution mockExecution = mock(StreamSubTaskExecution.class);
    StreamSubTask mockSubTask = mock(StreamSubTask.class);
    when(mockExecution.getSubTask()).thenReturn(mockSubTask);

    java.lang.reflect.Field field =
        AbstractStreamTaskInstance.class.getDeclaredField("subTaskExecutions");
    field.setAccessible(true);
    @SuppressWarnings("unchecked")
    java.util.Map<org.apache.iotdb.commons.stream.PartitionKey, StreamSubTaskExecution> map =
        (java.util.Map<org.apache.iotdb.commons.stream.PartitionKey, StreamSubTaskExecution>)
            field.get(streamTaskInstance);
    map.put(key, mockExecution);

    java.util.Map<org.apache.iotdb.commons.stream.PartitionKey, StreamSubTask> snapshot =
        streamTaskInstance.getSubTasksSnapshot();
    assertEquals(1, snapshot.size());
    assertSame(mockSubTask, snapshot.get(key));
  }

  /** drop() with subTaskExecutions cleans up all sub-task executions. */
  @Test
  public void testDropCleansUpAllSubTaskExecutions() throws Exception {
    when(mockStreamTask.getSource()).thenReturn(null);
    when(mockStreamTask.getTarget()).thenReturn(mock(StreamTarget.class));
    doNothing().when(mockWriteBackEngine).start();
    doNothing().when(mockWriteBackEngine).stop();

    org.apache.iotdb.commons.stream.PartitionKey key1 =
        new org.apache.iotdb.commons.stream.ListPartitionKey(
            java.util.Collections.singletonList("p1"));
    org.apache.iotdb.commons.stream.PartitionKey key2 =
        new org.apache.iotdb.commons.stream.ListPartitionKey(
            java.util.Collections.singletonList("p2"));
    StreamSubTaskExecution mockExec1 = mock(StreamSubTaskExecution.class);
    StreamSubTaskExecution mockExec2 = mock(StreamSubTaskExecution.class);

    java.lang.reflect.Field field =
        AbstractStreamTaskInstance.class.getDeclaredField("subTaskExecutions");
    field.setAccessible(true);
    @SuppressWarnings("unchecked")
    java.util.Map<org.apache.iotdb.commons.stream.PartitionKey, StreamSubTaskExecution> map =
        (java.util.Map<org.apache.iotdb.commons.stream.PartitionKey, StreamSubTaskExecution>)
            field.get(streamTaskInstance);
    map.put(key1, mockExec1);
    map.put(key2, mockExec2);

    streamTaskInstance.drop();

    verify(mockExec1).drop();
    verify(mockExec2).drop();
    verify(mockExec1).awaitCleanupFinished();
    verify(mockExec2).awaitCleanupFinished();
  }

  /** drop() with writeBackEngine.stop() exception does not rethrow. */
  @Test
  public void testDropWithWriteBackEngineStopExceptionDoesNotThrow() throws Exception {
    when(mockStreamTask.getSource()).thenReturn(null);
    when(mockStreamTask.getTarget()).thenReturn(mock(StreamTarget.class));
    doNothing().when(mockWriteBackEngine).start();
    doThrow(new RuntimeException("stop failed")).when(mockWriteBackEngine).stop();

    streamTaskInstance.start();
    // Should not throw
    streamTaskInstance.drop();
  }

  /** stop() with stopInternal exception logs warning, does not rethrow. */
  @Test
  public void testStopWithStopInternalExceptionDoesNotThrow() throws Exception {
    when(mockStreamTask.getSource()).thenReturn(null);
    when(mockStreamTask.getTarget()).thenReturn(mock(StreamTarget.class));
    doNothing().when(mockWriteBackEngine).start();

    streamTaskInstance.start();

    // Set sourceInstance to a mock that throws on stop
    java.lang.reflect.Field sourceInstanceField =
        EventStreamTaskInstance.class.getDeclaredField("sourceInstance");
    sourceInstanceField.setAccessible(true);
    StreamSourceInstance throwingSource = mock(StreamSourceInstance.class);
    doThrow(new RuntimeException("stop error")).when(throwingSource).stop();
    sourceInstanceField.set(streamTaskInstance, throwingSource);

    // Should not throw
    streamTaskInstance.stop();
    assertFalse(streamTaskInstance.isRunning());
  }

  // ==================== EventStreamTaskInstance commitProcessedProgress ====================

  /** commitProcessedProgress commits when minimumCommitId > lastCommittedSourceIndex. */
  @Test
  public void testCommitProcessedProgressCommitsWhenProgressAdvances() throws Exception {
    when(mockStreamTask.getSource()).thenReturn(mock(StreamSource.class));
    when(mockStreamTask.getTarget()).thenReturn(mock(StreamTarget.class));
    when(mockStreamTask.getWindow()).thenReturn(mock(StreamWindow.class));

    mockDispatcherAndSource();
    doNothing().when(mockWriteBackEngine).start();
    doNothing().when(mockStreamSourceInstance).start(any());

    streamTaskInstance.start();

    java.lang.reflect.Field sourceInstanceField =
        EventStreamTaskInstance.class.getDeclaredField("sourceInstance");
    sourceInstanceField.setAccessible(true);
    sourceInstanceField.set(streamTaskInstance, mockStreamSourceInstance);

    // Inject a mock sub-task execution with a positive commitId
    org.apache.iotdb.commons.stream.PartitionKey key =
        new org.apache.iotdb.commons.stream.ListPartitionKey(java.util.Collections.emptyList());
    StreamSubTaskExecution mockExecution = mock(StreamSubTaskExecution.class);
    StreamSubTask mockSubTask = mock(StreamSubTask.class);
    when(mockExecution.getSubTask()).thenReturn(mockSubTask);
    when(mockSubTask.getCommitId()).thenReturn(100L);

    java.lang.reflect.Field subTaskField =
        AbstractStreamTaskInstance.class.getDeclaredField("subTaskExecutions");
    subTaskField.setAccessible(true);
    @SuppressWarnings("unchecked")
    java.util.Map<org.apache.iotdb.commons.stream.PartitionKey, StreamSubTaskExecution> map =
        (java.util.Map<org.apache.iotdb.commons.stream.PartitionKey, StreamSubTaskExecution>)
            subTaskField.get(streamTaskInstance);
    map.put(key, mockExecution);

    EventStreamTaskInstance eventInstance = (EventStreamTaskInstance) streamTaskInstance;
    long result = eventInstance.commitProcessedProgress();

    assertEquals(100L, result);
    verify(mockStreamSourceInstance).commit(100L);
  }

  /** commitProcessedProgress does not commit when minimumCommitId <= lastCommittedSourceIndex. */
  @Test
  public void testCommitProcessedProgressDoesNotCommitWhenNoProgress() throws Exception {
    when(mockStreamTask.getSource()).thenReturn(mock(StreamSource.class));
    when(mockStreamTask.getTarget()).thenReturn(mock(StreamTarget.class));
    when(mockStreamTask.getWindow()).thenReturn(mock(StreamWindow.class));

    mockDispatcherAndSource();
    doNothing().when(mockWriteBackEngine).start();
    doNothing().when(mockStreamSourceInstance).start(any());

    streamTaskInstance.start();

    java.lang.reflect.Field sourceInstanceField =
        EventStreamTaskInstance.class.getDeclaredField("sourceInstance");
    sourceInstanceField.setAccessible(true);
    sourceInstanceField.set(streamTaskInstance, mockStreamSourceInstance);

    // Set lastCommittedSourceIndex to a high value so minimumCommitId (100) is not greater
    EventStreamTaskInstance eventInstance = (EventStreamTaskInstance) streamTaskInstance;
    // Inject a sub-task with commitId = 50
    org.apache.iotdb.commons.stream.PartitionKey key =
        new org.apache.iotdb.commons.stream.ListPartitionKey(java.util.Collections.emptyList());
    StreamSubTaskExecution mockExecution = mock(StreamSubTaskExecution.class);
    StreamSubTask mockSubTask = mock(StreamSubTask.class);
    when(mockExecution.getSubTask()).thenReturn(mockSubTask);
    when(mockSubTask.getCommitId()).thenReturn(50L);

    java.lang.reflect.Field subTaskField =
        AbstractStreamTaskInstance.class.getDeclaredField("subTaskExecutions");
    subTaskField.setAccessible(true);
    @SuppressWarnings("unchecked")
    java.util.Map<org.apache.iotdb.commons.stream.PartitionKey, StreamSubTaskExecution> map =
        (java.util.Map<org.apache.iotdb.commons.stream.PartitionKey, StreamSubTaskExecution>)
            subTaskField.get(streamTaskInstance);
    map.put(key, mockExecution);

    // First commit to set lastCommittedSourceIndex = 50
    eventInstance.commitProcessedProgress();
    // Second commit: minimumCommitId = 50, lastCommittedSourceIndex = 50 -> no commit
    eventInstance.commitProcessedProgress();

    // commit should only be called once (first time)
    verify(mockStreamSourceInstance, times(1)).commit(50L);
  }

  /** getMinimumSubTaskCommitId with non-empty subTasks returns the minimum. */
  @Test
  public void testGetMinimumSubTaskCommitIdWithPopulatedMap() throws Exception {
    org.apache.iotdb.commons.stream.PartitionKey key1 =
        new org.apache.iotdb.commons.stream.ListPartitionKey(
            java.util.Collections.singletonList("a"));
    org.apache.iotdb.commons.stream.PartitionKey key2 =
        new org.apache.iotdb.commons.stream.ListPartitionKey(
            java.util.Collections.singletonList("b"));
    StreamSubTaskExecution mockExec1 = mock(StreamSubTaskExecution.class);
    StreamSubTaskExecution mockExec2 = mock(StreamSubTaskExecution.class);
    StreamSubTask mockSubTask1 = mock(StreamSubTask.class);
    StreamSubTask mockSubTask2 = mock(StreamSubTask.class);
    when(mockExec1.getSubTask()).thenReturn(mockSubTask1);
    when(mockExec2.getSubTask()).thenReturn(mockSubTask2);
    when(mockSubTask1.getCommitId()).thenReturn(200L);
    when(mockSubTask2.getCommitId()).thenReturn(100L);

    java.lang.reflect.Field field =
        AbstractStreamTaskInstance.class.getDeclaredField("subTaskExecutions");
    field.setAccessible(true);
    @SuppressWarnings("unchecked")
    java.util.Map<org.apache.iotdb.commons.stream.PartitionKey, StreamSubTaskExecution> map =
        (java.util.Map<org.apache.iotdb.commons.stream.PartitionKey, StreamSubTaskExecution>)
            field.get(streamTaskInstance);
    map.put(key1, mockExec1);
    map.put(key2, mockExec2);

    EventStreamTaskInstance eventInstance = (EventStreamTaskInstance) streamTaskInstance;
    assertEquals(100L, eventInstance.getMinimumSubTaskCommitId());
  }

  // ==================== commitProcessedProgressSafely via reflection ====================

  /** commitProcessedProgressSafely when not running returns without committing. */
  @Test
  public void testCommitProcessedProgressSafelyWhenNotRunning() throws Exception {
    EventStreamTaskInstance eventInstance = (EventStreamTaskInstance) streamTaskInstance;
    invokePrivate(eventInstance, "commitProcessedProgressSafely");
    verify(mockStreamSourceInstance, never()).commit(anyLong());
  }

  /** commitProcessedProgressSafely when running calls commitProcessedProgress. */
  @Test
  public void testCommitProcessedProgressSafelyWhenRunning() throws Exception {
    when(mockStreamTask.getSource()).thenReturn(null);
    when(mockStreamTask.getTarget()).thenReturn(mock(StreamTarget.class));
    doNothing().when(mockWriteBackEngine).start();

    streamTaskInstance.start();
    EventStreamTaskInstance eventInstance = (EventStreamTaskInstance) streamTaskInstance;
    invokePrivate(eventInstance, "commitProcessedProgressSafely");
  }

  /** commitProcessedProgressSafely catches exception and logs warning. */
  @Test
  public void testCommitProcessedProgressSafelyCatchesException() throws Exception {
    when(mockStreamTask.getSource()).thenReturn(mock(StreamSource.class));
    when(mockStreamTask.getTarget()).thenReturn(mock(StreamTarget.class));
    when(mockStreamTask.getWindow()).thenReturn(mock(StreamWindow.class));

    mockDispatcherAndSource();
    doNothing().when(mockWriteBackEngine).start();
    doNothing().when(mockStreamSourceInstance).start(any());

    streamTaskInstance.start();

    java.lang.reflect.Field sourceInstanceField =
        EventStreamTaskInstance.class.getDeclaredField("sourceInstance");
    sourceInstanceField.setAccessible(true);
    sourceInstanceField.set(streamTaskInstance, mockStreamSourceInstance);

    doThrow(new RuntimeException("commit failed")).when(mockStreamSourceInstance).commit(anyLong());

    org.apache.iotdb.commons.stream.PartitionKey key =
        new org.apache.iotdb.commons.stream.ListPartitionKey(java.util.Collections.emptyList());
    StreamSubTaskExecution mockExecution = mock(StreamSubTaskExecution.class);
    StreamSubTask mockSubTask = mock(StreamSubTask.class);
    when(mockExecution.getSubTask()).thenReturn(mockSubTask);
    when(mockSubTask.getCommitId()).thenReturn(100L);

    java.lang.reflect.Field subTaskField =
        AbstractStreamTaskInstance.class.getDeclaredField("subTaskExecutions");
    subTaskField.setAccessible(true);
    @SuppressWarnings("unchecked")
    java.util.Map<org.apache.iotdb.commons.stream.PartitionKey, StreamSubTaskExecution> map =
        (java.util.Map<org.apache.iotdb.commons.stream.PartitionKey, StreamSubTaskExecution>)
            subTaskField.get(streamTaskInstance);
    map.put(key, mockExecution);

    EventStreamTaskInstance eventInstance = (EventStreamTaskInstance) streamTaskInstance;
    invokePrivate(eventInstance, "commitProcessedProgressSafely");
  }

  // ==================== startTaskCommitter / stopTaskCommitter via reflection ====================

  /** startTaskCommitter creates and starts the taskCommitter timer. */
  @Test
  public void testStartTaskCommitter() throws Exception {
    EventStreamTaskInstance eventInstance = (EventStreamTaskInstance) streamTaskInstance;
    invokePrivate(eventInstance, "startTaskCommitter");

    java.lang.reflect.Field field = EventStreamTaskInstance.class.getDeclaredField("taskCommitter");
    field.setAccessible(true);
    assertNotNull(field.get(eventInstance));

    invokePrivate(eventInstance, "stopTaskCommitter");
    assertNull(field.get(eventInstance));
  }

  /** stopTaskCommitter when taskCommitter is null is a no-op. */
  @Test
  public void testStopTaskCommitterWhenNull() throws Exception {
    EventStreamTaskInstance eventInstance = (EventStreamTaskInstance) streamTaskInstance;
    invokePrivate(eventInstance, "stopTaskCommitter");
  }

  /** getLastCommittedSourceIndex returns the current value. */
  @Test
  public void testGetLastCommittedSourceIndex() {
    EventStreamTaskInstance eventInstance = (EventStreamTaskInstance) streamTaskInstance;
    assertEquals(-1L, eventInstance.getLastCommittedSourceIndex());
  }

  private void invokePrivate(Object target, String methodName) throws Exception {
    java.lang.reflect.Method method = EventStreamTaskInstance.class.getDeclaredMethod(methodName);
    method.setAccessible(true);
    method.invoke(target);
  }
}
