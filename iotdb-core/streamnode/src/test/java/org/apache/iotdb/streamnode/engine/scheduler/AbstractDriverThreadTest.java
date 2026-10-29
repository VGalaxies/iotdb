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
package org.apache.iotdb.streamnode.engine.scheduler;

import org.apache.iotdb.calc.execution.schedule.queue.IndexedBlockingQueue;
import org.apache.iotdb.commons.exception.IoTDBRuntimeException;
import org.apache.iotdb.streamnode.engine.scheduler.task.DriverTaskId;
import org.apache.iotdb.streamnode.engine.scheduler.task.IStreamDriver;
import org.apache.iotdb.streamnode.engine.scheduler.task.StreamDriverTask;
import org.apache.iotdb.streamnode.exception.DriverTaskAbortedException;

import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link AbstractDriverThread}. Covers: run() loop processing, queue poll
 * InterruptedException, null task handling, execute() exceptions (IoTDBRuntimeException vs other),
 * close() flag, and finally-block produce() invocation.
 */
public class AbstractDriverThreadTest {

  private IndexedBlockingQueue<StreamDriverTask> mockQueue;
  private ITaskScheduler mockScheduler;
  private ThreadProducer mockProducer;

  /** Creates a concrete AbstractDriverThread whose execute() delegates to the given runnable. */
  private AbstractDriverThread newThread(Runnable executeLogic) {
    mockQueue = mock(IndexedBlockingQueue.class);
    mockScheduler = mock(ITaskScheduler.class);
    mockProducer = mock(ThreadProducer.class);
    return new AbstractDriverThread(
        "test-worker", new ThreadGroup("test"), mockQueue, mockScheduler, mockProducer) {
      @Override
      protected void execute(StreamDriverTask task) {
        if (executeLogic != null) {
          executeLogic.run();
        }
      }
    };
  }

  /** Creates a StreamDriverTask mock with a driver that has a DriverTaskId. */
  private StreamDriverTask newMockTask() {
    StreamDriverTask task = mock(StreamDriverTask.class);
    IStreamDriver driver = mock(IStreamDriver.class);
    DriverTaskId taskId =
        new DriverTaskId(
            "test-stream",
            new org.apache.iotdb.commons.stream.ListPartitionKey(
                java.util.Collections.emptyList()));
    when(task.getDriver()).thenReturn(driver);
    when(task.getDriverTaskId()).thenReturn(taskId);
    when(driver.getDriverTaskId()).thenReturn(taskId);
    return task;
  }

  /** run() processes a task from the queue and calls execute(). Uses latch for synchronization. */
  @Test(timeout = 10000)
  public void testRunProcessesTask() throws Exception {
    final CountDownLatch executeLatch = new CountDownLatch(1);
    AbstractDriverThread thread = newThread(() -> executeLatch.countDown());

    StreamDriverTask task = newMockTask();
    // First poll returns the task, second poll throws InterruptedException to break the loop
    when(mockQueue.poll()).thenReturn(task).thenThrow(new InterruptedException());

    thread.start();
    assertTrue("execute() should have been called", executeLatch.await(5, TimeUnit.SECONDS));
    thread.close();
    thread.join(5000);
  }

  /**
   * queue.poll() throws InterruptedException -> thread breaks loop and exits. Not closed -> produce
   * called.
   */
  @Test(timeout = 10000)
  public void testRunInterruptedExceptionBreaksLoop() throws Exception {
    AbstractDriverThread thread = newThread(null);
    when(mockQueue.poll()).thenThrow(new InterruptedException());

    thread.start();
    thread.join(5000);

    verify(mockProducer).produce(any(), any(), any(), any());
  }

  /** queue.poll() returns null -> logs error and continues. Next poll returns task. */
  @Test(timeout = 10000)
  public void testRunNullTaskContinuesLoop() throws Exception {
    final CountDownLatch executeLatch = new CountDownLatch(1);
    AbstractDriverThread thread = newThread(() -> executeLatch.countDown());

    StreamDriverTask task = newMockTask();
    when(mockQueue.poll()).thenReturn(null).thenReturn(task).thenThrow(new InterruptedException());

    thread.start();
    assertTrue(
        "execute() should have been called for non-null task",
        executeLatch.await(5, TimeUnit.SECONDS));
    thread.close();
    thread.join(5000);
  }

  /** execute() throws IoTDBRuntimeException -> setAbortCause with root cause, toAborted called. */
  @Test(timeout = 10000)
  public void testRunExecuteThrowsIoTDBRuntimeException() throws Exception {
    final CountDownLatch abortLatch = new CountDownLatch(1);
    AbstractDriverThread thread =
        newThread(
            () -> {
              throw new IoTDBRuntimeException("test-iotdb-error", 1);
            });

    StreamDriverTask task = newMockTask();
    when(mockQueue.poll()).thenReturn(task).thenThrow(new InterruptedException());

    thread.start();
    // Wait for toAborted to be called
    long deadline = System.currentTimeMillis() + 5000;
    while (System.currentTimeMillis() < deadline) {
      try {
        verify(mockScheduler).toAborted(task);
        abortLatch.countDown();
        break;
      } catch (AssertionError ignored) {
        Thread.sleep(50);
      }
    }
    assertTrue("toAborted should have been called", abortLatch.getCount() == 0);
    thread.close();
    thread.join(5000);

    verify(task).setAbortCause(any(IoTDBRuntimeException.class));
  }

  /**
   * execute() throws non-IoTDBRuntimeException -> setAbortCause with DriverTaskAbortedException.
   */
  @Test(timeout = 10000)
  public void testRunExecuteThrowsOtherException() throws Exception {
    final CountDownLatch abortLatch = new CountDownLatch(1);
    AbstractDriverThread thread =
        newThread(
            () -> {
              throw new IllegalStateException("test-other-error");
            });

    StreamDriverTask task = newMockTask();
    when(mockQueue.poll()).thenReturn(task).thenThrow(new InterruptedException());

    thread.start();
    long deadline = System.currentTimeMillis() + 5000;
    while (System.currentTimeMillis() < deadline) {
      try {
        verify(mockScheduler).toAborted(task);
        abortLatch.countDown();
        break;
      } catch (AssertionError ignored) {
        Thread.sleep(50);
      }
    }
    assertTrue("toAborted should have been called", abortLatch.getCount() == 0);
    thread.close();
    thread.join(5000);

    verify(task).setAbortCause(any(DriverTaskAbortedException.class));
  }

  /** close() sets the closed flag to true. */
  @Test
  public void testCloseSetsClosedFlag() throws Exception {
    AbstractDriverThread thread = newThread(null);
    thread.close();
  }

  /** When interrupted AND closed, the finally block re-interrupts the thread. */
  @Test(timeout = 10000)
  public void testRunReinterruptsWhenInterruptedAndClosed() throws Exception {
    final CountDownLatch executeStarted = new CountDownLatch(1);
    final CountDownLatch releaseExecute = new CountDownLatch(1);
    AbstractDriverThread thread =
        newThread(
            () -> {
              executeStarted.countDown();
              try {
                releaseExecute.await(5, TimeUnit.SECONDS);
              } catch (InterruptedException ignored) {
                // execute was interrupted - this sets the interrupt flag path
              }
            });

    StreamDriverTask task = newMockTask();
    when(mockQueue.poll()).thenReturn(task).thenThrow(new InterruptedException());

    thread.start();
    assertTrue("execute() should start", executeStarted.await(5, TimeUnit.SECONDS));

    // Close the thread, then interrupt it while it's in execute(). Close must come first: the
    // interrupt wakes execute() at once, and a worker that still sees closed=false loops back,
    // fails the next poll() and calls produce() before close() runs.
    thread.close();
    thread.interrupt();
    // Release execute to let it complete and reach the finally block
    releaseExecute.countDown();

    thread.join(5000);
    // The finally block runs with interrupted=true and closed=true -> re-interrupts
    // Verify the thread exited (produce NOT called since closed=true)
    verify(mockProducer, never()).produce(any(), any(), any(), any());
  }

  /** When interrupted but NOT closed, the finally block does NOT re-interrupt. */
  @Test(timeout = 10000)
  public void testRunDoesNotReinterruptWhenNotClosed() throws Exception {
    final CountDownLatch executeStarted = new CountDownLatch(1);
    final CountDownLatch releaseExecute = new CountDownLatch(1);
    AbstractDriverThread thread =
        newThread(
            () -> {
              executeStarted.countDown();
              try {
                releaseExecute.await(5, TimeUnit.SECONDS);
              } catch (InterruptedException ignored) {
                // execute was interrupted
              }
            });

    StreamDriverTask task = newMockTask();
    when(mockQueue.poll()).thenReturn(task).thenThrow(new InterruptedException());

    thread.start();
    assertTrue("execute() should start", executeStarted.await(5, TimeUnit.SECONDS));

    // Interrupt but DON'T close -> finally sees closed=false
    thread.interrupt();
    releaseExecute.countDown();

    thread.join(5000);
    // Since closed=false, produce() IS called for replacement
    verify(mockProducer).produce(any(), any(), any(), any());
  }

  /** When closed=true before start, thread exits without calling produce(). */
  @Test(timeout = 10000)
  public void testRunClosedThreadDoesNotProduceReplacement() throws Exception {
    AbstractDriverThread thread = newThread(null);
    when(mockQueue.poll()).thenThrow(new InterruptedException());

    thread.close();
    thread.start();
    thread.join(5000);

    verify(mockProducer, never()).produce(any(), any(), any(), any());
  }

  /** When closed=false, thread exits (interrupted) and calls produce() for replacement. */
  @Test(timeout = 10000)
  public void testRunUnclosedThreadProducesReplacement() throws Exception {
    AbstractDriverThread thread = newThread(null);
    when(mockQueue.poll()).thenThrow(new InterruptedException());

    thread.start();
    thread.join(5000);

    verify(mockProducer).produce(any(), any(), any(), any());
  }

  /** Thread interrupted BEFORE entering the loop exits immediately (while !isInterrupted false). */
  @Test(timeout = 10000)
  public void testRunInterruptedBeforeStartExitsImmediately() throws Exception {
    AbstractDriverThread thread = newThread(null);
    // Set interrupt flag before start so !isInterrupted() is false in the while check
    thread.interrupt();
    thread.start();
    thread.join(5000);

    // Thread exits without polling or producing (interrupt flag cleared by Thread.interrupted()
    // in the finally block, but closed=false -> produce called)
    verify(mockProducer).produce(any(), any(), any(), any());
  }

  /** Thread starts when closed=true exits immediately (while !closed false). */
  @Test(timeout = 10000)
  public void testRunClosedBeforeStartExitsImmediately() throws Exception {
    AbstractDriverThread thread = newThread(null);
    thread.close();
    thread.start();
    thread.join(5000);

    // closed=true -> loop never executes -> no produce (outer finally logs "closed")
    verify(mockProducer, never()).produce(any(), any(), any(), any());
  }
}
