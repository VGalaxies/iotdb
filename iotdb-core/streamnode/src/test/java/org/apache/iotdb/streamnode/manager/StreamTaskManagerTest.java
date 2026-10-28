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

package org.apache.iotdb.streamnode.manager;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.stream.IoTDBSubscriptionSource;
import org.apache.iotdb.commons.stream.StreamTask;
import org.apache.iotdb.commons.stream.StreamTaskStatus;
import org.apache.iotdb.commons.stream.StreamWindow;
import org.apache.iotdb.commons.stream.StreamWindowType;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.streamnode.engine.scheduler.IStreamTaskScheduler;
import org.apache.iotdb.streamnode.engine.task.AbstractStreamTaskInstance;
import org.apache.iotdb.streamnode.rpc.thrift.TTaskHeartbeat;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class StreamTaskManagerTest {

  @Mock private StreamTask mockStreamTask;
  @Mock private AbstractStreamTaskInstance mockAbstractStreamTaskInstance;
  @Mock private IStreamTaskScheduler mockScheduler;

  private StreamTaskManager streamTaskManager;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);

    when(mockStreamTask.getTaskName()).thenReturn("test-task-1");

    StreamWindow window = mock(StreamWindow.class);
    when(window.getType()).thenReturn(StreamWindowType.TUMBLE);
    when(mockStreamTask.getWindow()).thenReturn(window);

    streamTaskManager = new StreamTaskManager();

    java.lang.reflect.Field schedulerField = StreamTaskManager.class.getDeclaredField("scheduler");
    schedulerField.setAccessible(true);
    schedulerField.set(streamTaskManager, mockScheduler);
  }

  // ==================== Instance lifecycle (via RPC entry points) ====================

  @Test
  public void testStartTask() throws Exception {
    final java.lang.reflect.Field instancesField =
        StreamTaskManager.class.getDeclaredField("instances");
    instancesField.setAccessible(true);
    final java.util.Map<String, AbstractStreamTaskInstance> instances =
        (java.util.Map<String, AbstractStreamTaskInstance>) instancesField.get(streamTaskManager);

    streamTaskManager.createTask(mockStreamTask, 1);
    streamTaskManager.startTask(mockStreamTask, 2, 100L);

    assertTrue(instances.containsKey("test-task-1"));
  }

  @Test
  public void testRestartUpdatesRetainedSourceDefinition() throws Exception {
    final Field instancesField = StreamTaskManager.class.getDeclaredField("instances");
    instancesField.setAccessible(true);
    @SuppressWarnings("unchecked")
    final Map<String, AbstractStreamTaskInstance> instances =
        (Map<String, AbstractStreamTaskInstance>) instancesField.get(streamTaskManager);
    final StreamTask retainedTask = new StreamTask();
    when(mockAbstractStreamTaskInstance.getTaskDefinition()).thenReturn(retainedTask);
    instances.put("test-task-1", mockAbstractStreamTaskInstance);

    final StreamTask updatedTask = new StreamTask();
    updatedTask.setTaskName("test-task-1");
    final IoTDBSubscriptionSource updatedSource =
        new IoTDBSubscriptionSource("db", "table1", null, null, "127.0.0.1", 6667, "root", "root");
    updatedTask.setSource(updatedSource);

    final TSStatus status = streamTaskManager.startTask(updatedTask, 8, 100L);

    assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
    assertSame(updatedSource, retainedTask.getSource());
    assertEquals(8L, updatedSource.getOwnerEpoch());
    assertEquals(8, retainedTask.getEpoch());
    verify(mockAbstractStreamTaskInstance).start();
  }

  @Test
  public void testStartTaskTwice() throws Exception {
    final java.lang.reflect.Field instancesField =
        StreamTaskManager.class.getDeclaredField("instances");
    instancesField.setAccessible(true);
    final java.util.Map<String, AbstractStreamTaskInstance> instances =
        (java.util.Map<String, AbstractStreamTaskInstance>) instancesField.get(streamTaskManager);

    // First start
    streamTaskManager.createTask(mockStreamTask, 1);
    streamTaskManager.startTask(mockStreamTask, 2, 100L);
    assertTrue(instances.containsKey("test-task-1"));

    // Replace with a mock that reports running=true
    when(mockAbstractStreamTaskInstance.isRunning()).thenReturn(true);
    instances.put("test-task-1", mockAbstractStreamTaskInstance);

    // Second start: finds existing running instance, skips re-start
    streamTaskManager.startTask(mockStreamTask, 3, 200L);
  }

  @Test
  public void testStopExistingTask() throws Exception {
    final java.lang.reflect.Field instancesField =
        StreamTaskManager.class.getDeclaredField("instances");
    instancesField.setAccessible(true);
    final java.util.Map<String, AbstractStreamTaskInstance> instances =
        (java.util.Map<String, AbstractStreamTaskInstance>) instancesField.get(streamTaskManager);

    // Create task in taskMap so stopTask finds it, and put mock instance in instances map.
    streamTaskManager.createTask(mockStreamTask, 1);
    when(mockStreamTask.getEpoch()).thenReturn(1);
    when(mockStreamTask.getCnStartTime()).thenReturn(100L);
    instances.put("test-task-1", mockAbstractStreamTaskInstance);

    doNothing().when(mockAbstractStreamTaskInstance).stop();

    streamTaskManager.stopTask("test-task-1", 2, 200L);

    verify(mockAbstractStreamTaskInstance, times(1)).stop();
    // stop() is a pause: instance is preserved in map for potential restart
    assertTrue(instances.containsKey("test-task-1"));
  }

  @Test
  public void testStopNonExistingTask() throws Exception {
    final java.lang.reflect.Field instancesField =
        StreamTaskManager.class.getDeclaredField("instances");
    instancesField.setAccessible(true);
    final java.util.Map<String, AbstractStreamTaskInstance> instances =
        (java.util.Map<String, AbstractStreamTaskInstance>) instancesField.get(streamTaskManager);

    // stopTask returns OK for non-existent tasks (idempotent).
    streamTaskManager.stopTask("non-existing-task", 1, 100L);

    assertTrue(instances.isEmpty());
    verify(mockAbstractStreamTaskInstance, never()).stop();
  }

  @Test
  public void testShutdown() throws Exception {
    java.lang.reflect.Field instancesField = StreamTaskManager.class.getDeclaredField("instances");
    instancesField.setAccessible(true);
    java.util.Map<String, AbstractStreamTaskInstance> instances =
        (java.util.Map<String, AbstractStreamTaskInstance>) instancesField.get(streamTaskManager);

    // stop() delegates to dropAllTasks(), which iterates taskMap: create tasks there too
    StreamTask task1 = mock(StreamTask.class);
    when(task1.getTaskName()).thenReturn("task-1");
    StreamTask task2 = mock(StreamTask.class);
    when(task2.getTaskName()).thenReturn("task-2");
    streamTaskManager.createTask(task1, 1);
    streamTaskManager.createTask(task2, 1);

    AbstractStreamTaskInstance instance1 = mock(AbstractStreamTaskInstance.class);
    AbstractStreamTaskInstance instance2 = mock(AbstractStreamTaskInstance.class);
    instances.put("task-1", instance1);
    instances.put("task-2", instance2);

    doNothing().when(instance1).drop();
    doNothing().when(instance2).drop();

    streamTaskManager.stop();

    // shutdown = full cleanup via dropAllTasks: drop() + scheduler cancel per task
    verify(instance1, times(1)).drop();
    verify(instance2, times(1)).drop();
    verify(mockScheduler, times(1)).cancelStreamTask("task-1");
    verify(mockScheduler, times(1)).cancelStreamTask("task-2");
    assertTrue(instances.isEmpty());
  }

  @Test
  public void testShutdownWithEmptyInstances() throws Exception {
    java.lang.reflect.Field instancesField = StreamTaskManager.class.getDeclaredField("instances");
    instancesField.setAccessible(true);
    java.util.Map<String, AbstractStreamTaskInstance> instances =
        (java.util.Map<String, AbstractStreamTaskInstance>) instancesField.get(streamTaskManager);

    streamTaskManager.stop();

    assertTrue(instances.isEmpty());
  }

  @Test
  public void testGetInstance() {
    StreamTaskManager instance1 = StreamTaskManager.getInstance();
    StreamTaskManager instance2 = StreamTaskManager.getInstance();

    assertSame("StreamTaskManager should be a singleton", instance1, instance2);
  }

  // ==================== RPC entry points ====================

  @Test
  public void testCreateTaskRpc() {
    final TSStatus status = streamTaskManager.createTask(mockStreamTask, 5);
    assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
    assertEquals(mockStreamTask, streamTaskManager.getTask("test-task-1"));
    verify(mockStreamTask).setEpoch(5);
    verify(mockStreamTask).setStatus(StreamTaskStatus.CREATED);
  }

  @Test
  public void testCreateTaskDuplicate() {
    streamTaskManager.createTask(mockStreamTask, 1);
    final TSStatus status = streamTaskManager.createTask(mockStreamTask, 2);
    assertEquals(TSStatusCode.STREAM_ALREADY_EXISTS.getStatusCode(), status.getCode());
  }

  @Test
  public void testStartTaskRpcSuccess() throws Exception {
    final java.lang.reflect.Field instancesField =
        StreamTaskManager.class.getDeclaredField("instances");
    instancesField.setAccessible(true);
    final java.util.Map<String, AbstractStreamTaskInstance> instances =
        (java.util.Map<String, AbstractStreamTaskInstance>) instancesField.get(streamTaskManager);

    // Create then start via RPC
    streamTaskManager.createTask(mockStreamTask, 1);
    final TSStatus status = streamTaskManager.startTask(mockStreamTask, 2, 100L);
    assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
    verify(mockStreamTask).setStatus(StreamTaskStatus.RUNNING);
    assertTrue(instances.containsKey("test-task-1"));
  }

  @Test
  public void testStartTaskRpcStaleEpoch() {
    streamTaskManager.createTask(mockStreamTask, 5);
    when(mockStreamTask.getEpoch()).thenReturn(5);
    when(mockStreamTask.getCnStartTime()).thenReturn(100L);
    // Request with lower epoch is stale
    final TSStatus status = streamTaskManager.startTask(mockStreamTask, 3, 200L);
    assertEquals(TSStatusCode.STREAM_STALE.getStatusCode(), status.getCode());
  }

  @Test
  public void testStartTaskRpcAcceptsLowerCnStartTimeWithHigherEpoch() {
    streamTaskManager.createTask(mockStreamTask, 5);
    when(mockStreamTask.getEpoch()).thenReturn(5);
    when(mockStreamTask.getCnStartTime()).thenReturn(100L);
    final TSStatus status = streamTaskManager.startTask(mockStreamTask, 10, 50L);
    assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
    verify(mockStreamTask).setEpoch(10);
    verify(mockStreamTask).setCnStartTime(50L);
  }

  @Test
  public void testStopTaskRpcExisting() {
    streamTaskManager.createTask(mockStreamTask, 1);
    when(mockStreamTask.getEpoch()).thenReturn(1);
    when(mockStreamTask.getCnStartTime()).thenReturn(100L);
    final TSStatus status = streamTaskManager.stopTask("test-task-1", 2, 50L);
    assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
    verify(mockStreamTask).setStatus(StreamTaskStatus.STOPPED);
    verify(mockStreamTask).setEpoch(2);
    verify(mockStreamTask).setCnStartTime(50L);
  }

  @Test
  public void testStopTaskRpcNotFound() {
    final TSStatus status = streamTaskManager.stopTask("non-existent", 1, 100L);
    assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
  }

  @Test
  public void testStopTaskRpcStale() {
    streamTaskManager.createTask(mockStreamTask, 5);
    when(mockStreamTask.getEpoch()).thenReturn(5);
    when(mockStreamTask.getCnStartTime()).thenReturn(100L);
    when(mockStreamTask.getStatus()).thenReturn(StreamTaskStatus.RUNNING);
    // A stale stop did not affect the runtime and must be visible to the caller.
    final TSStatus status = streamTaskManager.stopTask("test-task-1", 3, 50L);
    assertEquals(TSStatusCode.STREAM_STALE.getStatusCode(), status.getCode());
  }

  @Test
  public void testStopTaskRpcStaleForStoppedTaskIsIdempotent() {
    streamTaskManager.createTask(mockStreamTask, 5);
    when(mockStreamTask.getEpoch()).thenReturn(5);
    when(mockStreamTask.getCnStartTime()).thenReturn(100L);
    when(mockStreamTask.getStatus()).thenReturn(StreamTaskStatus.STOPPED);

    final TSStatus status = streamTaskManager.stopTask("test-task-1", 3, 50L);

    assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
    verify(mockStreamTask, never()).setEpoch(3);
    verify(mockStreamTask, never()).setCnStartTime(50L);
  }

  @Test
  public void testStopTaskShortNameDelegatesToFull() {
    streamTaskManager.createTask(mockStreamTask, 1);
    final TSStatus status =
        streamTaskManager.stopTask("test-task-1", Integer.MAX_VALUE, Long.MAX_VALUE);
    assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
    verify(mockStreamTask).setStatus(StreamTaskStatus.STOPPED);
  }

  @Test
  public void testDropTaskExisting() {
    streamTaskManager.createTask(mockStreamTask, 1);
    final TSStatus status = streamTaskManager.dropTask("test-task-1");
    assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
    verify(mockStreamTask).setStatus(StreamTaskStatus.DROPPED);
    assertFalse(streamTaskManager.getTask("test-task-1") != null);
  }

  @Test
  public void testDropTaskNotFound() {
    final TSStatus status = streamTaskManager.dropTask("non-existent");
    assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
  }

  @Test
  public void testDropAllTasks() {
    // Create two tasks
    final StreamTask task2 = mock(StreamTask.class);
    when(task2.getTaskName()).thenReturn("task-2");
    final StreamWindow window2 = mock(StreamWindow.class);
    when(window2.getType()).thenReturn(StreamWindowType.TUMBLE);
    when(task2.getWindow()).thenReturn(window2);

    streamTaskManager.createTask(mockStreamTask, 1);
    streamTaskManager.createTask(task2, 1);
    streamTaskManager.dropAllTasks();
    // Both tasks should be removed from taskMap
    assertEquals(0, streamTaskManager.getTaskCount());
  }

  @Test
  public void testGetRunningTaskHeartbeats() {
    streamTaskManager.createTask(mockStreamTask, 1);
    when(mockStreamTask.getStatus()).thenReturn(StreamTaskStatus.RUNNING);
    when(mockStreamTask.getEpoch()).thenReturn(5);

    final List<TTaskHeartbeat> heartbeats = streamTaskManager.getRunningTaskHeartbeats();
    assertEquals(1, heartbeats.size());
    assertEquals("test-task-1", heartbeats.get(0).getTaskName());
    assertEquals(5, heartbeats.get(0).getEpoch());
  }

  @Test
  public void testGetRunningTaskHeartbeatsFiltersNonRunning() {
    streamTaskManager.createTask(mockStreamTask, 1);
    // Task is CREATED, not RUNNING -> filtered out.
    when(mockStreamTask.getStatus()).thenReturn(StreamTaskStatus.CREATED);

    final List<TTaskHeartbeat> heartbeats = streamTaskManager.getRunningTaskHeartbeats();
    assertTrue(heartbeats.isEmpty());
  }

  @Test
  public void testGetTaskCount() {
    assertEquals(0, streamTaskManager.getTaskCount());
    streamTaskManager.createTask(mockStreamTask, 1);
    assertEquals(1, streamTaskManager.getTaskCount());
  }

  @Test
  public void testGetTaskNumDelegatesToScheduler() {
    // Scheduler is mocked; verify getTaskNum returns the scheduler's count.
    when(mockScheduler.getTaskCount()).thenReturn(7);
    assertEquals(7, streamTaskManager.getTaskNum());
  }

  @Test
  public void testGetRunningTaskNumDelegatesToScheduler() {
    when(mockScheduler.getRunningTaskCount()).thenReturn(3);
    assertEquals(3, streamTaskManager.getRunningTaskNum());
  }

  @Test
  public void testGetTaskExisting() {
    streamTaskManager.createTask(mockStreamTask, 1);
    assertNotNull(streamTaskManager.getTask("test-task-1"));
    assertEquals(mockStreamTask, streamTaskManager.getTask("test-task-1"));
  }

  @Test
  public void testGetTaskNonExisting() {
    assertEquals(null, streamTaskManager.getTask("non-existent"));
  }

  @Test
  public void testServiceType() {
    assertEquals(
        org.apache.iotdb.commons.service.ServiceType.STREAM_TASK_MANAGER,
        streamTaskManager.getID());
  }

  @Test
  public void testStartScheduler() throws Exception {
    streamTaskManager.start();
    verify(mockScheduler).start();
  }

  @Test
  public void testStopScheduler() {
    streamTaskManager.stop();
    verify(mockScheduler).stop();
  }

  @Test
  public void testDropAllShortNameDelegates() {
    streamTaskManager.createTask(mockStreamTask, 1);
    streamTaskManager.dropAll();
    assertEquals(0, streamTaskManager.getTaskCount());
  }

  // ==================== stop vs drop distinction tests ====================

  @Test
  public void testStopTaskKeepsInstanceInMap() throws Exception {
    final java.lang.reflect.Field instancesField =
        StreamTaskManager.class.getDeclaredField("instances");
    instancesField.setAccessible(true);
    final java.util.Map<String, AbstractStreamTaskInstance> instances =
        (java.util.Map<String, AbstractStreamTaskInstance>) instancesField.get(streamTaskManager);

    streamTaskManager.createTask(mockStreamTask, 1);
    when(mockStreamTask.getEpoch()).thenReturn(1);
    when(mockStreamTask.getCnStartTime()).thenReturn(100L);
    instances.put("test-task-1", mockAbstractStreamTaskInstance);
    doNothing().when(mockAbstractStreamTaskInstance).stop();

    streamTaskManager.stopTask("test-task-1", 2, 200L);

    // stop = pause: instance is preserved in map for restart
    assertTrue(instances.containsKey("test-task-1"));
    verify(mockAbstractStreamTaskInstance, times(1)).stop();
    verify(mockAbstractStreamTaskInstance, never()).drop();
  }

  @Test
  public void testDropTaskRemovesInstanceFromMap() throws Exception {
    final java.lang.reflect.Field instancesField =
        StreamTaskManager.class.getDeclaredField("instances");
    instancesField.setAccessible(true);
    final java.util.Map<String, AbstractStreamTaskInstance> instances =
        (java.util.Map<String, AbstractStreamTaskInstance>) instancesField.get(streamTaskManager);

    streamTaskManager.createTask(mockStreamTask, 1);
    instances.put("test-task-1", mockAbstractStreamTaskInstance);
    doNothing().when(mockAbstractStreamTaskInstance).drop();

    streamTaskManager.dropTask("test-task-1");

    // drop = full cleanup: instance removed from map
    assertFalse(instances.containsKey("test-task-1"));
    verify(mockAbstractStreamTaskInstance, times(1)).drop();
    verify(mockScheduler, times(1)).cancelStreamTask("test-task-1");
  }

  @Test
  public void testStartTaskFailureWithExistingTaskCallsStopInstance() throws Exception {
    final java.lang.reflect.Field instancesField =
        StreamTaskManager.class.getDeclaredField("instances");
    instancesField.setAccessible(true);
    final java.util.Map<String, AbstractStreamTaskInstance> instances =
        (java.util.Map<String, AbstractStreamTaskInstance>) instancesField.get(streamTaskManager);

    // Task already exists in taskMap (was previously stopped, now restarting)
    streamTaskManager.createTask(mockStreamTask, 1);
    instances.put("test-task-1", mockAbstractStreamTaskInstance);

    // start() throws -> existing != null -> should call stop() (preserve), not drop()
    doThrow(new RuntimeException("start failed")).when(mockAbstractStreamTaskInstance).start();
    doNothing().when(mockAbstractStreamTaskInstance).stop();

    final TSStatus status = streamTaskManager.startTask(mockStreamTask, 2, 200L);

    assertEquals(TSStatusCode.STREAM_START_ERROR.getStatusCode(), status.getCode());
    // existing task: stop() called (preserves state), drop() NOT called
    verify(mockAbstractStreamTaskInstance, times(1)).stop();
    verify(mockAbstractStreamTaskInstance, never()).drop();
  }

  @Test
  public void testStartTaskFailureWithNewTaskCallsDropInstance() throws Exception {
    final java.lang.reflect.Field instancesField =
        StreamTaskManager.class.getDeclaredField("instances");
    instancesField.setAccessible(true);
    final java.util.Map<String, AbstractStreamTaskInstance> instances =
        (java.util.Map<String, AbstractStreamTaskInstance>) instancesField.get(streamTaskManager);

    // Task NOT in taskMap before startTask (brand new task)
    when(mockStreamTask.getEpoch()).thenReturn(1);
    when(mockStreamTask.getCnStartTime()).thenReturn(100L);
    instances.put("test-task-1", mockAbstractStreamTaskInstance);

    // start() throws -> existing == null -> should call drop() (full cleanup)
    doThrow(new RuntimeException("start failed")).when(mockAbstractStreamTaskInstance).start();
    doNothing().when(mockAbstractStreamTaskInstance).drop();

    final TSStatus status = streamTaskManager.startTask(mockStreamTask, 2, 200L);

    assertEquals(TSStatusCode.STREAM_START_ERROR.getStatusCode(), status.getCode());
    // new task: drop() called (full cleanup), stop() NOT called
    verify(mockAbstractStreamTaskInstance, never()).stop();
    verify(mockAbstractStreamTaskInstance, times(1)).drop();
    verify(mockScheduler, times(1)).cancelStreamTask("test-task-1");
  }

  @Test
  public void testStopInstanceDoesNotCancelSchedulerTask() throws Exception {
    final java.lang.reflect.Field instancesField =
        StreamTaskManager.class.getDeclaredField("instances");
    instancesField.setAccessible(true);
    final java.util.Map<String, AbstractStreamTaskInstance> instances =
        (java.util.Map<String, AbstractStreamTaskInstance>) instancesField.get(streamTaskManager);

    streamTaskManager.createTask(mockStreamTask, 1);
    when(mockStreamTask.getEpoch()).thenReturn(1);
    when(mockStreamTask.getCnStartTime()).thenReturn(100L);
    instances.put("test-task-1", mockAbstractStreamTaskInstance);
    doNothing().when(mockAbstractStreamTaskInstance).stop();

    streamTaskManager.stopTask("test-task-1", 2, 200L);

    // stop = pause: scheduler tasks NOT cancelled (driver remains in scheduler)
    verify(mockScheduler, never()).cancelStreamTask("test-task-1");
  }
}
