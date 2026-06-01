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
import org.apache.iotdb.streamnode.engine.dispatcher.TabletDispatcher;
import org.apache.iotdb.streamnode.engine.sink.IStreamSinkTask;
import org.apache.iotdb.streamnode.engine.sink.WriteBackEngine;
import org.apache.iotdb.streamnode.engine.source.StreamSourceInstance;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
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
  @Mock private TabletDispatcher mockTabletDispatcher;
  @Mock private StreamSourceInstance mockStreamSourceInstance;

  private StreamTaskInstance streamTaskInstance;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);

    when(mockStreamTask.getTaskName()).thenReturn("test-task");

    StreamDataConsumer mockConsumer =
        (tsBlock, commitId, partitionKey) -> {
          return CompletableFuture.completedFuture(null);
        };

    StreamTaskInstance realInstance = new StreamTaskInstance(mockStreamTask, mockConsumer);
    streamTaskInstance = spy(realInstance);

    java.lang.reflect.Field writeBackEngineField =
        StreamTaskInstance.class.getDeclaredField("writeBackEngine");
    writeBackEngineField.setAccessible(true);
    writeBackEngineField.set(streamTaskInstance, mockWriteBackEngine);
  }

  @SuppressWarnings("ResultOfMethodCallIgnored")
  private void mockDispatcherAndSource() {
    doReturn(mockTabletDispatcher).when(streamTaskInstance).createDispatcher(any());
    doReturn(mockStreamSourceInstance)
        .when(streamTaskInstance)
        .createSourceInstance(any(), any(), any(), any());
  }

  @SuppressWarnings("ResultOfMethodCallIgnored")
  private void mockDispatcherOnly() {
    doReturn(mockTabletDispatcher).when(streamTaskInstance).createDispatcher(any());
  }

  @Test
  public void testStartSuccessfully() throws Exception {
    when(mockStreamTask.getSource()).thenReturn(mock(StreamSource.class));
    when(mockStreamTask.getTarget()).thenReturn(mock(StreamTarget.class));
    when(mockStreamTask.getWindow()).thenReturn(mock(StreamWindow.class));

    mockDispatcherAndSource();

    doReturn(mock(IStreamSinkTask.class)).when(mockWriteBackEngine).start();
    doNothing().when(mockStreamSourceInstance).start();

    streamTaskInstance.start();

    verify(mockWriteBackEngine, times(1)).start();
    verify(mockStreamSourceInstance, times(1)).start();
    assertTrue(streamTaskInstance.isRunning());
  }

  @Test
  public void testStartWithoutSource() throws Exception {
    when(mockStreamTask.getSource()).thenReturn(null);
    when(mockStreamTask.getTarget()).thenReturn(mock(StreamTarget.class));

    doReturn(mock(IStreamSinkTask.class)).when(mockWriteBackEngine).start();

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

    doNothing().when(mockStreamSourceInstance).start();

    streamTaskInstance.start();

    verify(mockWriteBackEngine, never()).start();
    verify(mockStreamSourceInstance, times(1)).start();
    assertTrue(streamTaskInstance.isRunning());
  }

  @Test
  public void testStartAlreadyRunning() throws Exception {
    when(mockStreamTask.getSource()).thenReturn(mock(StreamSource.class));
    when(mockStreamTask.getTarget()).thenReturn(mock(StreamTarget.class));
    when(mockStreamTask.getWindow()).thenReturn(mock(StreamWindow.class));

    mockDispatcherAndSource();

    doReturn(mock(IStreamSinkTask.class)).when(mockWriteBackEngine).start();
    doNothing().when(mockStreamSourceInstance).start();

    streamTaskInstance.start();
    streamTaskInstance.start();

    verify(mockWriteBackEngine, times(1)).start();
    verify(mockStreamSourceInstance, times(1)).start();
    assertTrue(streamTaskInstance.isRunning());
  }

  @Test
  public void testStartWithException() throws Exception {
    when(mockStreamTask.getSource()).thenReturn(mock(StreamSource.class));
    when(mockStreamTask.getTarget()).thenReturn(mock(StreamTarget.class));
    when(mockStreamTask.getWindow()).thenReturn(mock(StreamWindow.class));

    mockDispatcherOnly();

    doThrow(new RuntimeException("Test exception")).when(mockWriteBackEngine).start();

    streamTaskInstance.start();

    verify(mockWriteBackEngine, times(1)).start();
    verify(mockWriteBackEngine, never()).stop();
    assertFalse(streamTaskInstance.isRunning());
  }

  @Test
  public void testStopSuccessfully() throws Exception {
    when(mockStreamTask.getSource()).thenReturn(mock(StreamSource.class));
    when(mockStreamTask.getTarget()).thenReturn(mock(StreamTarget.class));
    when(mockStreamTask.getWindow()).thenReturn(mock(StreamWindow.class));

    mockDispatcherAndSource();

    doReturn(mock(IStreamSinkTask.class)).when(mockWriteBackEngine).start();
    doNothing().when(mockStreamSourceInstance).start();
    doNothing().when(mockStreamSourceInstance).stop();
    doNothing().when(mockWriteBackEngine).stop();

    streamTaskInstance.start();

    java.lang.reflect.Field sourceInstanceField =
        StreamTaskInstance.class.getDeclaredField("sourceInstance");
    sourceInstanceField.setAccessible(true);
    sourceInstanceField.set(streamTaskInstance, mockStreamSourceInstance);

    streamTaskInstance.stop();

    verify(mockStreamSourceInstance, times(1)).stop();
    verify(mockWriteBackEngine, times(1)).stop();
    assertFalse(streamTaskInstance.isRunning());
  }

  @Test
  public void testStopWithoutSourceInstance() throws Exception {
    when(mockStreamTask.getSource()).thenReturn(null);
    when(mockStreamTask.getTarget()).thenReturn(mock(StreamTarget.class));

    doReturn(mock(IStreamSinkTask.class)).when(mockWriteBackEngine).start();

    streamTaskInstance.start();

    streamTaskInstance.stop();

    verify(mockWriteBackEngine, times(1)).stop();
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

    doReturn(mock(IStreamSinkTask.class)).when(mockWriteBackEngine).start();
    doNothing().when(mockStreamSourceInstance).start();

    doThrow(new RuntimeException("Stop exception")).when(mockWriteBackEngine).stop();

    streamTaskInstance.start();

    java.lang.reflect.Field sourceInstanceField =
        StreamTaskInstance.class.getDeclaredField("sourceInstance");
    sourceInstanceField.setAccessible(true);
    sourceInstanceField.set(streamTaskInstance, mockStreamSourceInstance);

    streamTaskInstance.stop();

    verify(mockWriteBackEngine, times(1)).stop();
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

    doReturn(mock(IStreamSinkTask.class)).when(mockWriteBackEngine).start();

    streamTaskInstance.start();

    assertTrue(streamTaskInstance.isRunning());
  }

  @Test
  public void testIsRunningAfterStop() throws Exception {
    when(mockStreamTask.getSource()).thenReturn(null);
    when(mockStreamTask.getTarget()).thenReturn(mock(StreamTarget.class));

    doReturn(mock(IStreamSinkTask.class)).when(mockWriteBackEngine).start();
    doNothing().when(mockWriteBackEngine).stop();

    streamTaskInstance.start();
    assertTrue(streamTaskInstance.isRunning());

    streamTaskInstance.stop();
    assertFalse(streamTaskInstance.isRunning());
  }
}
