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

import org.apache.iotdb.commons.stream.PartitionKey;
import org.apache.iotdb.streamnode.engine.scheduler.task.DriverTaskId;
import org.apache.iotdb.streamnode.engine.scheduler.task.IStreamDriver;

import com.google.common.util.concurrent.SettableFuture;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class StreamTaskSchedulerIntegrationTest {

  private StreamTaskScheduler scheduler;
  private PartitionKey testPartitionKey;
  private ExecutorService executorService;

  @Mock private IStreamDriver mockStreamDriver;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    testPartitionKey =
        new PartitionKey() {
          @Override
          public int partitionHash() {
            return 0;
          }

          @Override
          public int segmentNum() {
            return 1;
          }

          @Override
          public Object segmentValue(int segmentIndex) {
            return 0;
          }
        };
    scheduler = new StreamTaskScheduler();
    executorService = Executors.newFixedThreadPool(10);
  }

  @After
  public void tearDown() {
    try {
      scheduler.stop();
      executorService.shutdown();
      if (!executorService.awaitTermination(5, TimeUnit.SECONDS)) {
        executorService.shutdownNow();
      }
    } catch (Exception e) {
      // Ignore cleanup exceptions
    }
  }

  @Test
  public void testCompleteTaskLifecycle() throws Exception {
    DriverTaskId taskId = new DriverTaskId("complete-lifecycle-task", testPartitionKey);
    long timeoutMs = 10000L;

    when(mockStreamDriver.getDriverTaskId()).thenReturn(taskId);
    when(mockStreamDriver.isFinished()).thenReturn(true);
    when(mockStreamDriver.processFor(any()))
        .thenReturn(com.google.common.util.concurrent.Futures.immediateFuture(null));

    scheduler.start();

    scheduler.submitStreamDriver(mockStreamDriver, timeoutMs);

    Thread.sleep(1000);

    verify(mockStreamDriver, atLeast(1)).getDriverTaskId();
  }

  @Test
  public void testConcurrentTaskSubmissionAndCancellation() throws Exception {
    int numTasks = 10;
    CountDownLatch submissionLatch = new CountDownLatch(numTasks);
    CountDownLatch cancellationLatch = new CountDownLatch(numTasks);
    List<DriverTaskId> taskIds = new ArrayList<>();
    List<IStreamDriver> drivers = new ArrayList<>();

    scheduler.start();

    for (int i = 0; i < numTasks; i++) {
      DriverTaskId taskId = new DriverTaskId("concurrent-task-" + i, testPartitionKey);
      IStreamDriver driver = mock(IStreamDriver.class);
      when(driver.getDriverTaskId()).thenReturn(taskId);
      when(driver.isFinished()).thenReturn(true);
      mockDoProcessOn(driver, 1);

      taskIds.add(taskId);
      drivers.add(driver);

      final int index = i;
      final IStreamDriver finalDriver = driver;

      executorService.submit(
          () -> {
            try {
              scheduler.submitStreamDriver(finalDriver, 5000L);
              submissionLatch.countDown();
            } catch (Exception e) {
              fail("Exception occurred while submitting task: " + e.getMessage());
            }
          });

      if (i % 2 == 0) {
        executorService.submit(
            () -> {
              try {
                Thread.sleep(100);
                scheduler.cancelStreamTask(taskIds.get(index));
                cancellationLatch.countDown();
              } catch (Exception e) {
                fail("Exception occurred while cancelling task: " + e.getMessage());
              }
            });
      } else {
        cancellationLatch.countDown();
      }
    }

    assertTrue("Submission timeout", submissionLatch.await(5, TimeUnit.SECONDS));
    assertTrue("Cancellation timeout", cancellationLatch.await(5, TimeUnit.SECONDS));

    Thread.sleep(1000);

    for (IStreamDriver driver : drivers) {
      verify(driver, atLeast(1)).getDriverTaskId();
    }
  }

  private static void mockDoProcessOn(IStreamDriver driver, int completedCount) {
    AtomicInteger executionCount = new AtomicInteger(0);
    when(driver.processFor(any()))
        .thenAnswer(
            invocation -> {
              int count = executionCount.incrementAndGet();
              if (count <= completedCount) {
                return com.google.common.util.concurrent.Futures.immediateFuture(null);
              } else {
                SettableFuture<Void> pendingFuture = SettableFuture.create();
                return pendingFuture;
              }
            });
  }

  @Ignore
  @Test
  public void testTaskPriorityScheduling() throws Exception {
    scheduler.start();

    List<IStreamDriver> drivers = new ArrayList<>();

    for (int i = 0; i < 5; i++) {
      DriverTaskId taskId = new DriverTaskId("priority-task-" + i, testPartitionKey);
      IStreamDriver driver = mock(IStreamDriver.class);
      when(driver.getDriverTaskId()).thenReturn(taskId);
      when(driver.isFinished()).thenReturn(true);
      when(driver.processFor(any()))
          .thenReturn(com.google.common.util.concurrent.Futures.immediateFuture(null));

      drivers.add(driver);

      scheduler.submitStreamDriver(driver, 3000L);
    }

    Thread.sleep(2000);

    for (IStreamDriver driver : drivers) {
      verify(driver, atLeast(1)).getDriverTaskId();
    }
  }

  @Test
  public void testSchedulerRestart() throws Exception {
    DriverTaskId taskId = new DriverTaskId("restart-task", testPartitionKey);

    when(mockStreamDriver.getDriverTaskId()).thenReturn(taskId);
    when(mockStreamDriver.isFinished()).thenReturn(true);
    mockDoProcessOn(mockStreamDriver, 1);
    scheduler.start();
    scheduler.submitStreamDriver(mockStreamDriver, 5000L);
    Thread.sleep(1000);

    scheduler.stop();
    Thread.sleep(500);

    scheduler = new StreamTaskScheduler();
    scheduler.start();

    DriverTaskId newTaskId = new DriverTaskId("new-restart-task", testPartitionKey);
    IStreamDriver newDriver = mock(IStreamDriver.class);
    when(newDriver.getDriverTaskId()).thenReturn(newTaskId);
    when(newDriver.isFinished()).thenReturn(true);
    mockDoProcessOn(newDriver, 1);

    scheduler.submitStreamDriver(newDriver, 5000L);

    Thread.sleep(1000);

    verify(newDriver, atLeast(1)).getDriverTaskId();
  }

  @Test
  public void testTaskTimeoutHandling() throws Exception {
    scheduler.start();

    DriverTaskId taskId = new DriverTaskId("timeout-task", testPartitionKey);

    when(mockStreamDriver.getDriverTaskId()).thenReturn(taskId);
    when(mockStreamDriver.isFinished()).thenReturn(true);
    when(mockStreamDriver.processFor(any()))
        .thenReturn(com.google.common.util.concurrent.Futures.immediateFuture(null));

    scheduler.submitStreamDriver(mockStreamDriver, 100L);

    Thread.sleep(500);

    verify(mockStreamDriver, atLeast(1)).getDriverTaskId();
  }

  @Ignore
  @Test
  public void testHighLoadTaskProcessing() throws Exception {
    int numTasks = 10;
    CountDownLatch latch = new CountDownLatch(numTasks);
    List<IStreamDriver> drivers = Collections.synchronizedList(new ArrayList<>());
    scheduler.start();
    for (int i = 0; i < numTasks; i++) {
      final int index = i;
      executorService.submit(
          () -> {
            try {
              DriverTaskId taskId = new DriverTaskId("highload-task-" + index, testPartitionKey);
              IStreamDriver driver = mock(IStreamDriver.class);
              when(driver.getDriverTaskId()).thenReturn(taskId);
              when(driver.isFinished()).thenReturn(true);
              mockDoProcessOn(driver, 1);
              drivers.add(driver);

              scheduler.submitStreamDriver(driver, 10000L);
              latch.countDown();
            } catch (Exception e) {
              fail("Failed to submit task in high load test: " + e.getMessage());
            }
          });
    }

    assertTrue("High load task submission timeout", latch.await(10, TimeUnit.SECONDS));

    Thread.sleep(2000);

    assertEquals("All drivers should be tracked", numTasks, drivers.size());
    for (IStreamDriver driver : drivers) {
      verify(driver, atLeast(1)).getDriverTaskId();
    }
  }

  @Test
  public void testMixedOperationTypes() throws Exception {
    scheduler.start();

    List<DriverTaskId> taskIds = new ArrayList<>();
    List<IStreamDriver> drivers = new ArrayList<>();

    for (int i = 0; i < 10; i++) {
      DriverTaskId taskId = new DriverTaskId("mixed-task-" + i, testPartitionKey);
      IStreamDriver driver = mock(IStreamDriver.class);
      when(driver.getDriverTaskId()).thenReturn(taskId);
      when(driver.isFinished()).thenReturn(true);
      mockDoProcessOn(driver, 1);
      taskIds.add(taskId);
      drivers.add(driver);
    }

    for (int i = 0; i < 5; i++) {
      scheduler.submitStreamDriver(drivers.get(i), 5000L);
    }

    Thread.sleep(500);

    scheduler.cancelStreamTask(taskIds.get(1));
    scheduler.cancelStreamTask(taskIds.get(3));

    Thread.sleep(500);

    for (int i = 5; i < 10; i++) {
      scheduler.submitStreamDriver(drivers.get(i), 5000L);
    }

    Thread.sleep(2000);

    for (IStreamDriver driver : drivers) {
      verify(driver, atLeast(1)).getDriverTaskId();
    }
  }

  @Test
  public void testResourceCleanup() throws Exception {
    scheduler.start();

    for (int i = 0; i < 5; i++) {
      DriverTaskId taskId = new DriverTaskId("cleanup-task-" + i, testPartitionKey);
      IStreamDriver driver = mock(IStreamDriver.class);
      when(driver.getDriverTaskId()).thenReturn(taskId);
      when(driver.isFinished()).thenReturn(true);
      when(driver.processFor(any()))
          .thenReturn(com.google.common.util.concurrent.Futures.immediateFuture(null));

      scheduler.submitStreamDriver(driver, 3000L);
    }

    Thread.sleep(1000);

    scheduler.stop();

    scheduler = new StreamTaskScheduler();

    DriverTaskId newTaskId = new DriverTaskId("new-cleanup-task", testPartitionKey);
    IStreamDriver newDriver = mock(IStreamDriver.class);
    when(newDriver.getDriverTaskId()).thenReturn(newTaskId);
    when(newDriver.isFinished()).thenReturn(true);
    when(newDriver.processFor(any()))
        .thenReturn(com.google.common.util.concurrent.Futures.immediateFuture(null));

    scheduler.start();
    scheduler.submitStreamDriver(newDriver, 3000L);

    Thread.sleep(1000);

    verify(newDriver, atLeast(1)).getDriverTaskId();
  }

  @Test
  public void testErrorRecovery() throws Exception {
    scheduler.start();

    DriverTaskId normalTaskId = new DriverTaskId("normal-task", testPartitionKey);
    IStreamDriver normalDriver = mock(IStreamDriver.class);
    when(normalDriver.getDriverTaskId()).thenReturn(normalTaskId);
    when(normalDriver.isFinished()).thenReturn(true);
    mockDoProcessOn(normalDriver, 1);
    scheduler.submitStreamDriver(normalDriver, 3000L);

    Thread.sleep(1000);

    DriverTaskId nonExistentTaskId = new DriverTaskId("non-existent", testPartitionKey);
    try {
      scheduler.cancelStreamTask(nonExistentTaskId);
    } catch (Exception e) {
      fail("Cancelling non-existent task should not cause scheduler to crash: " + e.getMessage());
    }

    DriverTaskId recoveryTaskId = new DriverTaskId("recovery-task", testPartitionKey);
    IStreamDriver recoveryDriver = mock(IStreamDriver.class);
    when(recoveryDriver.getDriverTaskId()).thenReturn(recoveryTaskId);
    when(recoveryDriver.isFinished()).thenReturn(true);
    mockDoProcessOn(recoveryDriver, 1);

    scheduler.submitStreamDriver(recoveryDriver, 3000L);

    Thread.sleep(1000);

    verify(normalDriver, atLeast(1)).getDriverTaskId();
    verify(recoveryDriver, atLeast(1)).getDriverTaskId();
  }

  @Test
  public void testLongRunningTaskScheduling() throws Exception {
    scheduler.start();

    AtomicLong lastExecutionTime = new AtomicLong(0);

    DriverTaskId taskId = new DriverTaskId("long-running-task", testPartitionKey);
    IStreamDriver driver = mock(IStreamDriver.class);
    when(driver.getDriverTaskId()).thenReturn(taskId);
    when(driver.isFinished()).thenReturn(false);
    mockDoProcessOn(driver, 2);

    scheduler.submitStreamDriver(driver, 30000L);

    Thread.sleep(1000);

    verify(driver, atLeast(2)).processFor(any());
  }

  @Test
  public void testCancelLongRunningTask() throws Exception {
    scheduler.start();

    DriverTaskId taskId = new DriverTaskId("cancel-long-running-task", testPartitionKey);
    IStreamDriver driver = mock(IStreamDriver.class);
    when(driver.getDriverTaskId()).thenReturn(taskId);
    when(driver.isFinished()).thenReturn(false);
    mockDoProcessOn(driver, 2);

    scheduler.submitStreamDriver(driver, 30000L);

    Thread.sleep(500);

    scheduler.cancelStreamTask(taskId);

    Thread.sleep(500);

    assertFalse("Task should not exist after cancellation", scheduler.isStreamTaskExist(taskId));
  }
}
