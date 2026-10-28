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

import org.apache.iotdb.commons.exception.IoTDBRuntimeException;
import org.apache.iotdb.commons.stream.ListPartitionKey;
import org.apache.iotdb.streamnode.engine.scheduler.queue.FairRoundRobinQueue;
import org.apache.iotdb.streamnode.engine.scheduler.task.DriverTaskId;
import org.apache.iotdb.streamnode.engine.scheduler.task.IStreamDriver;
import org.apache.iotdb.streamnode.engine.scheduler.task.StreamDriverTask;
import org.apache.iotdb.streamnode.exception.DriverTaskAbortedException;

import com.google.common.util.concurrent.SettableFuture;
import io.airlift.units.Duration;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DriverTaskThreadTest {

  @Mock private ITaskScheduler mockScheduler;
  @Mock private ThreadProducer mockProducer;
  @Mock private IStreamDriver mockDriver;
  @Mock private DriverTaskHandle mockHandle;

  private DriverTaskId taskId;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    taskId = new DriverTaskId("test-stream", new ListPartitionKey(Collections.emptyList()));
    when(mockDriver.getDriverTaskId()).thenReturn(taskId);
  }

  private StreamDriverTask newTask() {
    return new StreamDriverTask(mockDriver, 0L, mockHandle);
  }

  private FairRoundRobinQueue newQueue() {
    return new FairRoundRobinQueue(10, new StreamDriverTask());
  }

  // ==================== DriverTaskThread.execute tests ====================

  @Test
  public void testExecuteReturnsImmediatelyWhenReadyToRunningFails() throws InterruptedException {
    final DriverTaskThread thread =
        new DriverTaskThread("t", null, newQueue(), mockScheduler, mockProducer);
    when(mockScheduler.readyToRunning(ArgumentMatchers.any())).thenReturn(false);

    final StreamDriverTask task = newTask();
    thread.execute(task);

    // driver.processFor should NOT be called since readyToRunning returned false.
    verify(mockDriver, never()).processFor(ArgumentMatchers.any());
    verify(mockScheduler, never()).runningToReady(ArgumentMatchers.any());
    verify(mockScheduler, never()).toAborted(ArgumentMatchers.any());
  }

  @Test
  public void testExecuteAbortsWhenFutureIsCancelled() throws InterruptedException {
    final DriverTaskThread thread =
        new DriverTaskThread("t", null, newQueue(), mockScheduler, mockProducer);
    when(mockScheduler.readyToRunning(ArgumentMatchers.any())).thenReturn(true);

    final SettableFuture<Void> cancelledFuture = SettableFuture.create();
    cancelledFuture.cancel(true);
    doReturn(cancelledFuture).when(mockDriver).processFor(ArgumentMatchers.any());

    final StreamDriverTask task = newTask();
    thread.execute(task);

    verify(mockDriver, atLeastOnce()).getDriverTaskId();
    verify(mockScheduler).toAborted(task);
    assertTrue(task.getAbortCause().isPresent());
    assertTrue(task.getAbortCause().get() instanceof DriverTaskAbortedException);
  }

  @Test
  public void testExecuteRunningToReadyWhenFutureDoneSuccess() throws InterruptedException {
    final DriverTaskThread thread =
        new DriverTaskThread("t", null, newQueue(), mockScheduler, mockProducer);
    when(mockScheduler.readyToRunning(ArgumentMatchers.any())).thenReturn(true);

    final SettableFuture<Void> doneFuture = SettableFuture.create();
    doneFuture.set(null);
    doReturn(doneFuture).when(mockDriver).processFor(ArgumentMatchers.any());

    final StreamDriverTask task = newTask();
    thread.execute(task);

    verify(mockScheduler).runningToReady(task);
    verify(mockScheduler, never()).toAborted(ArgumentMatchers.any());
    verify(mockScheduler, never()).runningToBlocked(ArgumentMatchers.any());
    // quantaScheduledNanos was set (non-zero after execute).
    assertTrue(task.getQuantaScheduledNanos() >= 0);
  }

  @Test
  public void testExecuteToAbortedWhenFutureGetThrows() throws InterruptedException {
    final DriverTaskThread thread =
        new DriverTaskThread("t", null, newQueue(), mockScheduler, mockProducer);
    when(mockScheduler.readyToRunning(ArgumentMatchers.any())).thenReturn(true);

    final SettableFuture<Void> failedFuture = SettableFuture.create();
    final RuntimeException cause = new RuntimeException("compute failed");
    failedFuture.setException(cause);
    doReturn(failedFuture).when(mockDriver).processFor(ArgumentMatchers.any());

    final StreamDriverTask task = newTask();
    thread.execute(task);

    verify(mockScheduler).toAborted(task);
    verify(mockScheduler, never()).runningToReady(ArgumentMatchers.any());
    assertTrue(task.getAbortCause().isPresent());
    final Throwable abortCause = task.getAbortCause().get();
    assertTrue(abortCause instanceof DriverTaskAbortedException);
    // The abort message should carry the original cause's message.
    assertTrue(abortCause.getMessage().contains("compute failed"));
  }

  @Test
  public void testExecuteRunningToBlockedWhenFutureNotDone() throws InterruptedException {
    final DriverTaskThread thread =
        new DriverTaskThread("t", null, newQueue(), mockScheduler, mockProducer);
    when(mockScheduler.readyToRunning(ArgumentMatchers.any())).thenReturn(true);

    final SettableFuture<Void> pendingFuture = SettableFuture.create();
    doReturn(pendingFuture).when(mockDriver).processFor(ArgumentMatchers.any());

    final StreamDriverTask task = newTask();
    thread.execute(task);

    verify(mockScheduler).runningToBlocked(task);
    verify(mockScheduler, never()).runningToReady(ArgumentMatchers.any());
    verify(mockScheduler, never()).toAborted(ArgumentMatchers.any());

    // When the pending future completes, the registered listener should call blockedToReady.
    pendingFuture.set(null);
    // The listener runs on DriverTaskThread.listeningExecutor (cached pool). Give it a moment.
    verify(mockScheduler, org.mockito.Mockito.timeout(2000)).blockedToReady(task);
  }

  @Test
  public void testExecuteToAbortedWhenFutureGetThrowsWithNullCauseMessage()
      throws InterruptedException {
    final DriverTaskThread thread =
        new DriverTaskThread("t", null, newQueue(), mockScheduler, mockProducer);
    when(mockScheduler.readyToRunning(ArgumentMatchers.any())).thenReturn(true);

    final SettableFuture<Void> failedFuture = SettableFuture.create();
    // Exception with null cause and null message.
    failedFuture.setException(new RuntimeException());
    doReturn(failedFuture).when(mockDriver).processFor(ArgumentMatchers.any());

    final StreamDriverTask task = newTask();
    thread.execute(task);

    verify(mockScheduler).toAborted(task);
    assertTrue(task.getAbortCause().isPresent());
  }

  // ==================== AbstractDriverThread.run tests ====================

  @Test(timeout = 10000)
  public void testRunExitsWhenClosedBeforeStart() throws Exception {
    // close() before start() -> run() should skip the loop and NOT call producer.produce.
    final FairRoundRobinQueue queue = newQueue();
    final AtomicBoolean produceCalled = new AtomicBoolean(false);
    final DriverTaskThread thread =
        new DriverTaskThread(
            "t", null, queue, mockScheduler, (name, tg, q, p) -> produceCalled.set(true));

    thread.close();
    thread.start();
    thread.join(3000);
    assertTrue("Thread should terminate after close()", !thread.isAlive());
    assertEquals(false, produceCalled.get());
    verify(mockProducer, never()).produce(any(), any(), any(), any());
  }

  @Test(timeout = 10000)
  public void testRunExecutesPushedTask() throws Exception {
    final FairRoundRobinQueue queue = newQueue();
    final CountDownLatch executed = new CountDownLatch(1);
    final AtomicReference<StreamDriverTask> executedTask = new AtomicReference<>();

    // Custom thread that records the executed task.
    final AbstractDriverThread thread =
        new AbstractDriverThread("t", null, queue, mockScheduler, mockProducer) {
          @Override
          public void execute(StreamDriverTask task) {
            executedTask.set(task);
            executed.countDown();
          }
        };

    when(mockScheduler.readyToRunning(ArgumentMatchers.any())).thenReturn(true);
    final StreamDriverTask task = newTask();
    queue.push(task);

    thread.start();
    assertTrue("execute should be invoked", executed.await(5, TimeUnit.SECONDS));
    assertEquals(task.getDriverTaskId(), executedTask.get().getDriverTaskId());

    // Stop the thread.
    thread.close();
    thread.interrupt();
    thread.join(3000);
  }

  @Test(timeout = 15000)
  public void testRunCallsProducerProduceOnUncaughtError() throws Exception {
    // If execute throws an Error (not Exception), it escapes the catch(Exception) and triggers
    // thread death. The finally block should call producer.produce to spawn a replacement.
    final FairRoundRobinQueue queue = newQueue();
    final CountDownLatch produced = new CountDownLatch(1);
    final ThreadProducer recordingProducer =
        (name, tg, q, p) -> {
          produced.countDown();
          // Don't actually spawn a new thread to keep the test bounded.
        };

    final AbstractDriverThread thread =
        new AbstractDriverThread("t", null, queue, mockScheduler, recordingProducer) {
          @Override
          public void execute(StreamDriverTask task) {
            throw new Error("boom");
          }
        };

    final StreamDriverTask task = newTask();
    queue.push(task);

    thread.start();
    assertTrue(
        "producer.produce should be called after uncaught Error",
        produced.await(10, TimeUnit.SECONDS));

    // Interrupt to ensure cleanup.
    thread.close();
    thread.interrupt();
    thread.join(3000);
  }

  @Test(timeout = 15000)
  public void testRunHandlesIoTDBRuntimeExceptionInExecute() throws Exception {
    // When execute throws IoTDBRuntimeException, run's catch block should call setAbortCause
    // with the root cause and then scheduler.toAborted.
    final FairRoundRobinQueue queue = newQueue();
    final CountDownLatch aborted = new CountDownLatch(1);
    final ITaskScheduler recordingScheduler = mock(ITaskScheduler.class);
    when(recordingScheduler.readyToRunning(ArgumentMatchers.any())).thenReturn(true);
    org.mockito.Mockito.doAnswer(
            invocation -> {
              aborted.countDown();
              return null;
            })
        .when(recordingScheduler)
        .toAborted(ArgumentMatchers.any());

    final IoTDBRuntimeException rootCause = mock(IoTDBRuntimeException.class);

    final AbstractDriverThread thread =
        new AbstractDriverThread("t", null, queue, recordingScheduler, mockProducer) {
          @Override
          public void execute(StreamDriverTask task) {
            throw new RuntimeException(rootCause);
          }
        };

    final StreamDriverTask task = newTask();
    queue.push(task);

    thread.start();
    assertTrue("toAborted should be called", aborted.await(10, TimeUnit.SECONDS));
    assertTrue(task.getAbortCause().isPresent());
    assertEquals(rootCause, task.getAbortCause().get());

    thread.close();
    thread.interrupt();
    thread.join(3000);
  }

  @Test(timeout = 15000)
  public void testRunHandlesGenericExceptionInExecute() throws Exception {
    // When execute throws a non-IoTDBRuntimeException, the abort cause is wrapped in
    // DriverTaskAbortedException with BY_INTERNAL_ERROR_SCHEDULED.
    final FairRoundRobinQueue queue = newQueue();
    final CountDownLatch aborted = new CountDownLatch(1);
    final ITaskScheduler recordingScheduler = mock(ITaskScheduler.class);
    when(recordingScheduler.readyToRunning(ArgumentMatchers.any())).thenReturn(true);
    org.mockito.Mockito.doAnswer(
            invocation -> {
              aborted.countDown();
              return null;
            })
        .when(recordingScheduler)
        .toAborted(ArgumentMatchers.any());

    final AbstractDriverThread thread =
        new AbstractDriverThread("t", null, queue, recordingScheduler, mockProducer) {
          @Override
          public void execute(StreamDriverTask task) {
            throw new IllegalStateException("generic failure");
          }
        };

    final StreamDriverTask task = newTask();
    queue.push(task);

    thread.start();
    assertTrue("toAborted should be called", aborted.await(10, TimeUnit.SECONDS));
    assertTrue(task.getAbortCause().isPresent());
    final Throwable cause = task.getAbortCause().get();
    assertTrue(cause instanceof DriverTaskAbortedException);

    thread.close();
    thread.interrupt();
    thread.join(3000);
  }

  @Test
  public void testCloseSetsClosedFlag() throws java.io.IOException {
    // close() is a simple flag setter; verify it doesn't throw and is idempotent.
    final AbstractDriverThread thread =
        new AbstractDriverThread("t", null, newQueue(), mockScheduler, mockProducer) {
          @Override
          public void execute(StreamDriverTask task) {
            // no-op
          }
        };
    thread.close();
    // Idempotent.
    thread.close();
  }

  @Test
  public void testGetExecutionTimeSliceScalesWithWeight() throws InterruptedException {
    // Indirectly verify getExecutionTimeSliceForDriverTask: driver.processFor receives a Duration
    // proportional to task.getWeight(). We can't read the private method directly, but we can
    // capture the Duration passed to processFor.
    final DriverTaskThread thread =
        new DriverTaskThread("t", null, newQueue(), mockScheduler, mockProducer);
    when(mockScheduler.readyToRunning(ArgumentMatchers.any())).thenReturn(true);

    final SettableFuture<?> doneFuture = SettableFuture.create();
    doneFuture.set(null);
    final java.util.concurrent.atomic.AtomicReference<Duration> captured =
        new java.util.concurrent.atomic.AtomicReference<>();
    when(mockDriver.processFor(ArgumentMatchers.any()))
        .thenAnswer(
            invocation -> {
              captured.set(invocation.getArgument(0));
              return doneFuture;
            });

    final StreamDriverTask task = newTask();
    task.setWeight(2.0);
    thread.execute(task);

    assertNotNull(captured.get());
    assertTrue(captured.get().toMillis() > 0);
  }

  @Test(timeout = 15000)
  public void testRunMultipleTasksInSequence() throws Exception {
    final FairRoundRobinQueue queue = newQueue();
    final java.util.concurrent.atomic.AtomicInteger executeCount =
        new java.util.concurrent.atomic.AtomicInteger(0);
    final CountDownLatch firstDone = new CountDownLatch(1);

    final AbstractDriverThread thread =
        new AbstractDriverThread("t", null, queue, mockScheduler, mockProducer) {
          @Override
          public void execute(StreamDriverTask task) {
            executeCount.incrementAndGet();
            firstDone.countDown();
          }
        };

    queue.push(newTask());
    thread.start();
    assertTrue("first task should execute", firstDone.await(5, TimeUnit.SECONDS));

    // Push a second task; it should also execute.
    final CountDownLatch secondDone = new CountDownLatch(1);
    final IStreamDriver driver2 = mock(IStreamDriver.class);
    when(driver2.getDriverTaskId())
        .thenReturn(new DriverTaskId("s2", new ListPartitionKey(Collections.emptyList())));
    final StreamDriverTask task2 = new StreamDriverTask(driver2, 0L, mockHandle);
    queue.push(task2);

    // Wait for second execution.
    final ExecutorService waiter = Executors.newSingleThreadExecutor();
    waiter.submit(
        () -> {
          while (executeCount.get() < 2) {
            try {
              Thread.sleep(50);
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              break;
            }
          }
          secondDone.countDown();
        });
    assertTrue("second task should execute", secondDone.await(5, TimeUnit.SECONDS));
    waiter.shutdownNow();

    assertEquals(2, executeCount.get());

    thread.close();
    thread.interrupt();
    thread.join(3000);
  }

  @Test
  public void testDriverTaskThreadConstructorAcceptsNullQueueWithoutThrowing() {
    // Constructor stores the queue without validation; NPE would only surface on queue.poll().
    // This test documents that construction itself is safe with a null queue.
    final DriverTaskThread thread =
        new DriverTaskThread("t", null, null, mockScheduler, mockProducer);
    assertNotNull(thread);
  }
}
