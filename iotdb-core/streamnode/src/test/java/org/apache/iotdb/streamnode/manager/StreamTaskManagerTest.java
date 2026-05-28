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

import org.apache.iotdb.commons.stream.StreamTask;
import org.apache.iotdb.streamnode.engine.scheduler.IStreamTaskScheduler;
import org.apache.iotdb.streamnode.engine.task.StreamTaskInstance;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.concurrent.ExecutorService;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class StreamTaskManagerTest {

  @Mock private StreamTask mockStreamTask;
  @Mock private StreamTaskInstance mockStreamTaskInstance;
  @Mock private IStreamTaskScheduler mockScheduler;
  @Mock private ExecutorService mockExecutorService;

  private StreamTaskManager streamTaskManager;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);

    when(mockStreamTask.getTaskName()).thenReturn("test-task-1");

    streamTaskManager = new StreamTaskManager();

    java.lang.reflect.Field schedulerField = StreamTaskManager.class.getDeclaredField("scheduler");
    schedulerField.setAccessible(true);
    schedulerField.set(streamTaskManager, mockScheduler);

    java.lang.reflect.Field executorField =
        StreamTaskManager.class.getDeclaredField("executorService");
    executorField.setAccessible(true);
    executorField.set(streamTaskManager, mockExecutorService);
  }

  @Test
  public void testStartTask() throws Exception {
    java.lang.reflect.Field instancesField = StreamTaskManager.class.getDeclaredField("instances");
    instancesField.setAccessible(true);
    java.util.Map<String, StreamTaskInstance> instances =
        (java.util.Map<String, StreamTaskInstance>) instancesField.get(streamTaskManager);

    streamTaskManager.start(mockStreamTask);

    verify(mockExecutorService, times(1)).submit(any(Runnable.class));
    assertTrue(instances.containsKey("test-task-1"));
  }

  @Test
  public void testStartTaskTwice() throws Exception {
    java.lang.reflect.Field instancesField = StreamTaskManager.class.getDeclaredField("instances");
    instancesField.setAccessible(true);
    java.util.Map<String, StreamTaskInstance> instances =
        (java.util.Map<String, StreamTaskInstance>) instancesField.get(streamTaskManager);

    // First start: creates and submits new instance
    streamTaskManager.start(mockStreamTask);
    verify(mockExecutorService, times(1)).submit(any(Runnable.class));
    assertTrue(instances.containsKey("test-task-1"));

    // Replace with a mock that reports running=true
    when(mockStreamTaskInstance.isRunning()).thenReturn(true);
    instances.put("test-task-1", mockStreamTaskInstance);

    // Second start: finds existing running instance, skips submit
    streamTaskManager.start(mockStreamTask);

    // Submit still only called once (from the first start)
    verify(mockExecutorService, times(1)).submit(any(Runnable.class));
  }

  @Test
  public void testStopExistingTask() throws Exception {
    java.lang.reflect.Field instancesField = StreamTaskManager.class.getDeclaredField("instances");
    instancesField.setAccessible(true);
    java.util.Map<String, StreamTaskInstance> instances =
        (java.util.Map<String, StreamTaskInstance>) instancesField.get(streamTaskManager);
    instances.put("test-task-1", mockStreamTaskInstance);

    doNothing().when(mockStreamTaskInstance).stop();

    streamTaskManager.stop("test-task-1");

    verify(mockStreamTaskInstance, times(1)).stop();
    assertFalse(instances.containsKey("test-task-1"));
  }

  @Test
  public void testStopNonExistingTask() throws Exception {
    java.lang.reflect.Field instancesField = StreamTaskManager.class.getDeclaredField("instances");
    instancesField.setAccessible(true);
    java.util.Map<String, StreamTaskInstance> instances =
        (java.util.Map<String, StreamTaskInstance>) instancesField.get(streamTaskManager);

    streamTaskManager.stop("non-existing-task");

    assertTrue(instances.isEmpty());
    verify(mockStreamTaskInstance, never()).stop();
  }

  @Test
  public void testShutdown() throws Exception {
    java.lang.reflect.Field instancesField = StreamTaskManager.class.getDeclaredField("instances");
    instancesField.setAccessible(true);
    java.util.Map<String, StreamTaskInstance> instances =
        (java.util.Map<String, StreamTaskInstance>) instancesField.get(streamTaskManager);

    StreamTaskInstance instance1 = mock(StreamTaskInstance.class);
    StreamTaskInstance instance2 = mock(StreamTaskInstance.class);
    instances.put("task-1", instance1);
    instances.put("task-2", instance2);

    doNothing().when(instance1).stop();
    doNothing().when(instance2).stop();

    streamTaskManager.stop();

    verify(instance1, times(1)).stop();
    verify(instance2, times(1)).stop();
    verify(mockExecutorService, times(1)).shutdown();
    assertTrue(instances.isEmpty());
  }

  @Test
  public void testShutdownWithEmptyInstances() throws Exception {
    java.lang.reflect.Field instancesField = StreamTaskManager.class.getDeclaredField("instances");
    instancesField.setAccessible(true);
    java.util.Map<String, StreamTaskInstance> instances =
        (java.util.Map<String, StreamTaskInstance>) instancesField.get(streamTaskManager);

    streamTaskManager.stop();

    verify(mockExecutorService, times(1)).shutdown();
    assertTrue(instances.isEmpty());
  }

  @Test
  public void testGetInstance() {
    StreamTaskManager instance1 = StreamTaskManager.getInstance();
    StreamTaskManager instance2 = StreamTaskManager.getInstance();

    assertSame("StreamTaskManager should be a singleton", instance1, instance2);
  }
}
