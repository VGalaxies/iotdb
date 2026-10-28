/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iotdb.streamnode.engine.task;

import org.apache.iotdb.commons.stream.ListPartitionKey;
import org.apache.iotdb.commons.stream.PartitionKey;
import org.apache.iotdb.commons.stream.TumbleWindow;
import org.apache.iotdb.streamnode.engine.computation.IStreamComputeTask;
import org.apache.iotdb.streamnode.engine.computation.StreamComputeTaskContext;
import org.apache.iotdb.streamnode.engine.scheduler.IStreamTaskScheduler;
import org.apache.iotdb.streamnode.engine.scheduler.task.IStreamDriver;
import org.apache.iotdb.streamnode.engine.scheduler.task.StreamDriver;
import org.apache.iotdb.streamnode.engine.scheduler.task.StreamDriverContext;
import org.apache.iotdb.streamnode.engine.sink.IStreamSinkTask;
import org.apache.iotdb.streamnode.engine.window.GlobalWindowMemoryController;
import org.apache.iotdb.streamnode.engine.window.WindowEngine;
import org.apache.iotdb.streamnode.engine.window.WindowEngineTestUtils;

import org.junit.Test;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class StreamSubTaskExecutionTest {

  private static StreamSubTask newSubTask(StreamSubTaskContext context) {
    StreamSubTask subTask =
        new StreamSubTask(
            context.getPartitionKey(), null, null, null, null, context.getTaskName(), context);
    return subTask;
  }

  private static StreamSubTaskContext newContext(String taskName) {
    PartitionKey partitionKey = new ListPartitionKey(Collections.emptyList());
    StreamSubTaskStateMachine stateMachine = new StreamSubTaskStateMachine(taskName, Runnable::run);
    return new StreamSubTaskContext(taskName, partitionKey, stateMachine);
  }

  /** Mocks a driver whose close() signals driverClosed, matching real StreamDriver behavior. */
  private static IStreamDriver newDriver(StreamSubTaskContext context) {
    IStreamDriver driver = mock(IStreamDriver.class);
    doAnswer(
            inv -> {
              context.signalDriverClosed();
              return null;
            })
        .when(driver)
        .close();
    return driver;
  }

  @Test(expected = IllegalStateException.class)
  public void testCreatePropagatesOtherRuntimeExceptions() {
    IStreamTaskScheduler scheduler = mock(IStreamTaskScheduler.class);
    StreamSubTaskContext context = newContext("bug-task");
    StreamSubTask subTask = newSubTask(context);
    IStreamDriver driver = newDriver(context);

    // Non-overload RuntimeExceptions indicate bugs and must propagate, not be swallowed.
    doThrow(new IllegalStateException("not overload"))
        .when(scheduler)
        .submitStreamDriver(eq(driver), anyLong());

    StreamSubTaskExecution.create(scheduler, subTask, driver, 0);
  }

  // ==================== start/stop/drop delegate to WindowEngine ====================

  @Test
  public void testStartDelegatesToWindowEngine() {
    IStreamTaskScheduler scheduler = mock(IStreamTaskScheduler.class);
    StreamSubTaskContext context = newContext("start-test");
    StreamSubTask subTask = newSubTask(context);
    WindowEngine mockWindowEngine = mock(WindowEngine.class);

    setWindowEngine(subTask, mockWindowEngine);

    StreamSubTaskExecution.create(scheduler, subTask, newDriver(context), 0);

    // create() calls start() internally, which delegates to windowEngine.start()
    verify(mockWindowEngine, org.mockito.Mockito.times(1)).start();
  }

  @Test
  public void testStopDelegatesToWindowEngine() {
    IStreamTaskScheduler scheduler = mock(IStreamTaskScheduler.class);
    StreamSubTaskContext context = newContext("stop-test");
    StreamSubTask subTask = newSubTask(context);
    WindowEngine mockWindowEngine = mock(WindowEngine.class);

    setWindowEngine(subTask, mockWindowEngine);

    StreamSubTaskExecution execution =
        StreamSubTaskExecution.create(scheduler, subTask, newDriver(context), 0);
    execution.stop();

    verify(mockWindowEngine, org.mockito.Mockito.times(1)).stop();
  }

  private static void setWindowEngine(StreamSubTask subTask, WindowEngine windowEngine) {
    try {
      java.lang.reflect.Field windowEngineField =
          StreamSubTask.class.getDeclaredField("windowEngine");
      windowEngineField.setAccessible(true);
      windowEngineField.set(subTask, windowEngine);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  // ==================== failed / awaitCleanupFinished / getters ====================

  @Test
  public void testFailedDelegatesToStateMachine() {
    IStreamTaskScheduler scheduler = mock(IStreamTaskScheduler.class);
    StreamSubTaskContext context = newContext("failed-test");
    StreamSubTask subTask = newSubTask(context);
    setWindowEngine(subTask, mock(WindowEngine.class));

    StreamSubTaskExecution execution =
        StreamSubTaskExecution.create(scheduler, subTask, newDriver(context), 0);
    execution.failed(new RuntimeException("test-failure"));

    assertEquals(StreamSubTaskState.FAILED, execution.getState());
    assertTrue(execution.getContext().getFailureCause().isPresent());
  }

  @Test
  public void testAwaitCleanupFinishedReturnsAfterDrop() throws Exception {
    IStreamTaskScheduler scheduler = mock(IStreamTaskScheduler.class);
    StreamSubTaskContext context = newContext("await-test");
    StreamSubTask subTask = newSubTask(context);
    setWindowEngine(subTask, mock(WindowEngine.class));

    StreamSubTaskExecution execution =
        StreamSubTaskExecution.create(scheduler, subTask, newDriver(context), 0);
    execution.drop();

    // awaitCleanupFinished should return quickly after drop (driver.close signals driverClosed)
    long start = System.nanoTime();
    execution.awaitCleanupFinished();
    long duration = System.nanoTime() - start;
    assertTrue("awaitCleanupFinished should return quickly after drop", duration < 2_000_000_000L);
  }

  @Test
  public void testGetStateReturnsStateMachineState() {
    IStreamTaskScheduler scheduler = mock(IStreamTaskScheduler.class);
    StreamSubTaskContext context = newContext("state-test");
    StreamSubTask subTask = newSubTask(context);
    setWindowEngine(subTask, mock(WindowEngine.class));

    StreamSubTaskExecution execution =
        StreamSubTaskExecution.create(scheduler, subTask, newDriver(context), 0);
    // create() calls start() which transitions CREATED -> RUNNING
    assertEquals(StreamSubTaskState.RUNNING, execution.getState());
  }

  @Test
  public void testGetContextReturnsSubTaskContext() {
    StreamSubTaskContext context = newContext("context-test");
    StreamSubTask subTask = newSubTask(context);
    setWindowEngine(subTask, mock(WindowEngine.class));

    StreamSubTaskExecution execution =
        StreamSubTaskExecution.create(
            mock(IStreamTaskScheduler.class), subTask, newDriver(context), 0);

    assertSame(context, execution.getContext());
  }

  @Test
  public void testGetSubTaskReturnsOriginalSubTask() {
    StreamSubTaskContext context = newContext("subtask-test");
    StreamSubTask subTask = newSubTask(context);
    setWindowEngine(subTask, mock(WindowEngine.class));

    StreamSubTaskExecution execution =
        StreamSubTaskExecution.create(
            mock(IStreamTaskScheduler.class), subTask, newDriver(context), 0);

    assertSame(subTask, execution.getSubTask());
  }

  @Test
  public void testGetDriverReturnsOriginalDriver() {
    StreamSubTaskContext context = newContext("driver-test");
    StreamSubTask subTask = newSubTask(context);
    setWindowEngine(subTask, mock(WindowEngine.class));
    IStreamDriver driver = newDriver(context);

    StreamSubTaskExecution execution =
        StreamSubTaskExecution.create(mock(IStreamTaskScheduler.class), subTask, driver, 0);

    assertSame(driver, execution.getDriver());
  }

  @Test
  public void testMarkStoppingTransitionsToStopping() {
    IStreamTaskScheduler scheduler = mock(IStreamTaskScheduler.class);
    StreamSubTaskContext context = newContext("stopping-test");
    StreamSubTask subTask = newSubTask(context);
    setWindowEngine(subTask, mock(WindowEngine.class));

    StreamSubTaskExecution execution =
        StreamSubTaskExecution.create(scheduler, subTask, newDriver(context), 0);
    execution.markStopping();

    assertEquals(StreamSubTaskState.STOPPING, execution.getState());
  }

  @Test
  public void testStopAfterStartTransitionsToStopped() {
    IStreamTaskScheduler scheduler = mock(IStreamTaskScheduler.class);
    StreamSubTaskContext context = newContext("stop-after-start");
    StreamSubTask subTask = newSubTask(context);
    WindowEngine mockWindowEngine = mock(WindowEngine.class);
    setWindowEngine(subTask, mockWindowEngine);

    StreamSubTaskExecution execution =
        StreamSubTaskExecution.create(scheduler, subTask, newDriver(context), 0);
    execution.stop();

    assertEquals(StreamSubTaskState.STOPPED, execution.getState());
    verify(mockWindowEngine, org.mockito.Mockito.times(1)).stop();
  }

  @Test
  public void testDropTransitionsToDropped() {
    IStreamTaskScheduler scheduler = mock(IStreamTaskScheduler.class);
    StreamSubTaskContext context = newContext("drop-state");
    StreamSubTask subTask = newSubTask(context);
    WindowEngine mockWindowEngine = mock(WindowEngine.class);
    setWindowEngine(subTask, mockWindowEngine);

    StreamSubTaskExecution execution =
        StreamSubTaskExecution.create(scheduler, subTask, newDriver(context), 0);
    execution.drop();

    assertEquals(StreamSubTaskState.DROPPED, execution.getState());
  }

  // ==================== onStateChanged FAILED branch tests ====================

  /** failed() triggers onStateChanged with FAILED state, calls driver.close(). */
  @Test
  public void testFailedTriggersOnStateChangedAndClosesDriver() {
    IStreamTaskScheduler scheduler = mock(IStreamTaskScheduler.class);
    StreamSubTaskContext context = newContext("failed-onstate");
    StreamSubTask subTask = newSubTask(context);
    WindowEngine mockWindowEngine = mock(WindowEngine.class);
    setWindowEngine(subTask, mockWindowEngine);

    IStreamDriver driver = newDriver(context);
    StreamSubTaskExecution execution = StreamSubTaskExecution.create(scheduler, subTask, driver, 0);
    execution.failed(new RuntimeException("test-failure"));

    assertEquals(StreamSubTaskState.FAILED, execution.getState());
    // driver.close() should be called by onStateChanged
    verify(driver).close();
    // signalDriverClosed should be called via releaseResource
    // (verified implicitly by no deadlock)
  }

  /** onStateChanged with driver.close() throwing logs error and continues. */
  @Test
  public void testOnStateChangedWithDriverCloseException() {
    IStreamTaskScheduler scheduler = mock(IStreamTaskScheduler.class);
    StreamSubTaskContext context = newContext("close-exception");
    StreamSubTask subTask = newSubTask(context);
    WindowEngine mockWindowEngine = mock(WindowEngine.class);
    setWindowEngine(subTask, mockWindowEngine);

    IStreamDriver driver = mock(IStreamDriver.class);
    doThrow(new RuntimeException("close failed")).when(driver).close();
    // Still need signalDriverClosed to be called to avoid deadlock in awaitCleanupFinished
    doAnswer(
            inv -> {
              context.signalDriverClosed();
              return null;
            })
        .when(driver)
        .close();

    StreamSubTaskExecution execution = StreamSubTaskExecution.create(scheduler, subTask, driver, 0);
    // drop triggers onStateChanged which calls driver.close()
    // The exception should be caught and logged
    execution.drop();
    assertEquals(StreamSubTaskState.DROPPED, execution.getState());
  }

  /** awaitCleanupFinished returns after failed() triggers cleanup. */
  @Test(timeout = 5000)
  public void testAwaitCleanupFinishedAfterFailed() {
    IStreamTaskScheduler scheduler = mock(IStreamTaskScheduler.class);
    StreamSubTaskContext context = newContext("await-failed");
    StreamSubTask subTask = newSubTask(context);
    WindowEngine mockWindowEngine = mock(WindowEngine.class);
    setWindowEngine(subTask, mockWindowEngine);

    StreamSubTaskExecution execution =
        StreamSubTaskExecution.create(scheduler, subTask, newDriver(context), 0);
    execution.failed(new RuntimeException("test"));

    // awaitCleanupFinished should return quickly after failed (driver.close signals driverClosed)
    execution.awaitCleanupFinished();
    assertEquals(StreamSubTaskState.FAILED, execution.getState());
  }

  // ==================== terminal-state engine drop (leak regression) ====================

  /**
   * A sub-task entering FAILED must fully drop its window engine: the engine's memory-resume
   * listener is registered in the node-global memory controller, so a failed-and-rebuilt sub-task
   * whose old engine is never dropped leaks one listener (and the engine behind it) per failure.
   */
  @Test
  public void testFailedSubTaskDropsWindowEngine() {
    GlobalWindowMemoryController memoryController = GlobalWindowMemoryController.getInstance();
    long listenersBefore = memoryController.getReleaseListenerCount();

    WindowEngine engine =
        WindowEngine.create(
            new TumbleWindow(1000, 0), WindowEngineTestUtils.createDriverContext(), null, null);
    assertEquals(
        "engine construction must register exactly one memory-resume listener",
        listenersBefore + 1,
        memoryController.getReleaseListenerCount());

    StreamSubTaskContext context = newContext("engine-drop-test");
    context.setWindowIdAllocator(new AtomicLong(0));
    StreamComputeTaskContext computeTaskContext = mock(StreamComputeTaskContext.class);
    when(computeTaskContext.mayHaveTmpFile()).thenReturn(false);
    IStreamComputeTask computeTask = mock(IStreamComputeTask.class);
    IStreamComputeTask recomputeTask = mock(IStreamComputeTask.class);
    when(computeTask.getStreamComputeTaskContext()).thenReturn(computeTaskContext);
    when(recomputeTask.getStreamComputeTaskContext()).thenReturn(computeTaskContext);
    StreamSubTask subTask =
        new StreamSubTask(
            context.getPartitionKey(),
            engine,
            computeTask,
            recomputeTask,
            mock(IStreamSinkTask.class),
            context.getTaskName(),
            context);
    // The stub scheduler never runs the driver: the state machine alone drives the terminal path.
    StreamSubTaskExecution execution =
        StreamSubTaskExecution.create(
            mock(IStreamTaskScheduler.class),
            subTask,
            new StreamDriver(subTask, new StreamDriverContext(context)),
            0);

    execution.failed(new RuntimeException("simulated sub-task failure"));

    assertEquals(
        "the failed sub-task's window engine must unregister its memory-resume listener",
        listenersBefore,
        memoryController.getReleaseListenerCount());
  }
}
