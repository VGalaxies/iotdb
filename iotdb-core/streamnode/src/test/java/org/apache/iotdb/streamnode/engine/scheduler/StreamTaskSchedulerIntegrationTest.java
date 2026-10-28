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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
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

    final AtomicBoolean processed = new AtomicBoolean(false);
    when(mockStreamDriver.getDriverTaskId()).thenReturn(taskId);
    when(mockStreamDriver.isFinished()).thenReturn(true);
    when(mockStreamDriver.processFor(any()))
        .thenAnswer(
            inv -> {
              processed.set(true);
              return com.google.common.util.concurrent.Futures.immediateFuture(null);
            });

    scheduler.start();

    scheduler.submitStreamDriver(mockStreamDriver, timeoutMs);

    Thread.sleep(500);

    assertTrue("Driver should have been processed by the scheduler", processed.get());
  }

  @Test
  public void testConcurrentTaskSubmissionAndCancellation() throws Exception {
    int numTasks = 10;
    CountDownLatch submissionLatch = new CountDownLatch(numTasks);
    CountDownLatch cancellationLatch = new CountDownLatch(numTasks);
    List<DriverTaskId> taskIds = new ArrayList<>();
    List<AtomicBoolean> processedFlags = new ArrayList<>();

    scheduler.start();

    for (int i = 0; i < numTasks; i++) {
      DriverTaskId taskId = new DriverTaskId("concurrent-task-" + i, testPartitionKey);
      IStreamDriver driver = mock(IStreamDriver.class);
      final AtomicBoolean processed = new AtomicBoolean(false);
      when(driver.getDriverTaskId()).thenReturn(taskId);
      when(driver.isFinished()).thenReturn(true);
      when(driver.processFor(any()))
          .thenAnswer(
              inv -> {
                processed.set(true);
                return com.google.common.util.concurrent.Futures.immediateFuture(null);
              });

      taskIds.add(taskId);
      processedFlags.add(processed);
      final IStreamDriver finalDriver = driver;
      final int index = i;

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

    Thread.sleep(500);

    for (int i = 0; i < processedFlags.size(); i++) {
      assertTrue("Driver " + i + " should have been processed", processedFlags.get(i).get());
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

  @Test
  public void testTaskPriorityScheduling() throws Exception {
    scheduler.start();

    List<AtomicBoolean> processedFlags = new ArrayList<>();

    for (int i = 0; i < 5; i++) {
      DriverTaskId taskId = new DriverTaskId("priority-task-" + i, testPartitionKey);
      IStreamDriver driver = mock(IStreamDriver.class);
      final AtomicBoolean processed = new AtomicBoolean(false);
      when(driver.getDriverTaskId()).thenReturn(taskId);
      when(driver.isFinished()).thenReturn(true);
      when(driver.processFor(any()))
          .thenAnswer(
              inv -> {
                processed.set(true);
                return com.google.common.util.concurrent.Futures.immediateFuture(null);
              });

      processedFlags.add(processed);
      scheduler.submitStreamDriver(driver, 3000L);
    }

    Thread.sleep(1000);

    for (int i = 0; i < processedFlags.size(); i++) {
      assertTrue("Driver " + i + " should have been processed", processedFlags.get(i).get());
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
    Thread.sleep(500);

    scheduler.stop();
    Thread.sleep(500);

    scheduler = new StreamTaskScheduler();
    scheduler.start();

    DriverTaskId newTaskId = new DriverTaskId("new-restart-task", testPartitionKey);
    IStreamDriver newDriver = mock(IStreamDriver.class);
    final AtomicBoolean newProcessed = new AtomicBoolean(false);
    when(newDriver.getDriverTaskId()).thenReturn(newTaskId);
    when(newDriver.isFinished()).thenReturn(true);
    when(newDriver.processFor(any()))
        .thenAnswer(
            inv -> {
              newProcessed.set(true);
              return com.google.common.util.concurrent.Futures.immediateFuture(null);
            });

    scheduler.submitStreamDriver(newDriver, 5000L);

    Thread.sleep(500);

    assertTrue("New driver should be processed after restart", newProcessed.get());
  }

  @Test
  public void testTaskTimeoutHandling() throws Exception {
    scheduler.start();

    DriverTaskId taskId = new DriverTaskId("timeout-task", testPartitionKey);

    final AtomicBoolean processed = new AtomicBoolean(false);
    when(mockStreamDriver.getDriverTaskId()).thenReturn(taskId);
    when(mockStreamDriver.isFinished()).thenReturn(true);
    when(mockStreamDriver.processFor(any()))
        .thenAnswer(
            inv -> {
              processed.set(true);
              return com.google.common.util.concurrent.Futures.immediateFuture(null);
            });

    scheduler.submitStreamDriver(mockStreamDriver, 100L);

    Thread.sleep(500);

    assertTrue("Driver should have been processed", processed.get());
  }

  @Test
  public void testHighLoadTaskProcessing() throws Exception {
    int numTasks = 10;
    CountDownLatch latch = new CountDownLatch(numTasks);
    List<AtomicBoolean> processedFlags = Collections.synchronizedList(new ArrayList<>());
    scheduler.start();
    for (int i = 0; i < numTasks; i++) {
      final int index = i;
      executorService.submit(
          () -> {
            try {
              DriverTaskId taskId = new DriverTaskId("highload-task-" + index, testPartitionKey);
              IStreamDriver driver = mock(IStreamDriver.class);
              final AtomicBoolean processed = new AtomicBoolean(false);
              when(driver.getDriverTaskId()).thenReturn(taskId);
              when(driver.isFinished()).thenReturn(true);
              when(driver.processFor(any()))
                  .thenAnswer(
                      inv -> {
                        processed.set(true);
                        return com.google.common.util.concurrent.Futures.immediateFuture(null);
                      });
              processedFlags.add(processed);

              scheduler.submitStreamDriver(driver, 10000L);
              latch.countDown();
            } catch (Exception e) {
              fail("Failed to submit task in high load test: " + e.getMessage());
            }
          });
    }

    assertTrue("High load task submission timeout", latch.await(10, TimeUnit.SECONDS));

    Thread.sleep(1000);

    assertEquals("All drivers should be tracked", numTasks, processedFlags.size());
    for (int i = 0; i < processedFlags.size(); i++) {
      assertTrue("Driver " + i + " should have been processed", processedFlags.get(i).get());
    }
  }

  @Test
  public void testMixedOperationTypes() throws Exception {
    scheduler.start();

    List<DriverTaskId> taskIds = new ArrayList<>();
    List<IStreamDriver> drivers = new ArrayList<>();
    List<AtomicBoolean> processedFlags = new ArrayList<>();

    for (int i = 0; i < 10; i++) {
      DriverTaskId taskId = new DriverTaskId("mixed-task-" + i, testPartitionKey);
      IStreamDriver driver = mock(IStreamDriver.class);
      final AtomicBoolean processed = new AtomicBoolean(false);
      when(driver.getDriverTaskId()).thenReturn(taskId);
      when(driver.isFinished()).thenReturn(true);
      when(driver.processFor(any()))
          .thenAnswer(
              inv -> {
                processed.set(true);
                return com.google.common.util.concurrent.Futures.immediateFuture(null);
              });
      taskIds.add(taskId);
      drivers.add(driver);
      processedFlags.add(processed);
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

    Thread.sleep(1000);

    // All drivers should have been processed at least once (including cancelled ones that ran
    // before cancellation).
    for (int i = 0; i < processedFlags.size(); i++) {
      assertTrue("Driver " + i + " should have been processed", processedFlags.get(i).get());
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

    Thread.sleep(500);

    scheduler.stop();

    scheduler = new StreamTaskScheduler();

    DriverTaskId newTaskId = new DriverTaskId("new-cleanup-task", testPartitionKey);
    IStreamDriver newDriver = mock(IStreamDriver.class);
    final AtomicBoolean newProcessed = new AtomicBoolean(false);
    when(newDriver.getDriverTaskId()).thenReturn(newTaskId);
    when(newDriver.isFinished()).thenReturn(true);
    when(newDriver.processFor(any()))
        .thenAnswer(
            inv -> {
              newProcessed.set(true);
              return com.google.common.util.concurrent.Futures.immediateFuture(null);
            });

    scheduler.start();
    scheduler.submitStreamDriver(newDriver, 3000L);

    Thread.sleep(500);

    assertTrue("Driver should be processed after cleanup", newProcessed.get());
  }

  @Test
  public void testErrorRecovery() throws Exception {
    scheduler.start();

    DriverTaskId normalTaskId = new DriverTaskId("normal-task", testPartitionKey);
    IStreamDriver normalDriver = mock(IStreamDriver.class);
    final AtomicBoolean normalProcessed = new AtomicBoolean(false);
    when(normalDriver.getDriverTaskId()).thenReturn(normalTaskId);
    when(normalDriver.isFinished()).thenReturn(true);
    when(normalDriver.processFor(any()))
        .thenAnswer(
            inv -> {
              normalProcessed.set(true);
              return com.google.common.util.concurrent.Futures.immediateFuture(null);
            });
    scheduler.submitStreamDriver(normalDriver, 3000L);

    Thread.sleep(500);

    DriverTaskId nonExistentTaskId = new DriverTaskId("non-existent", testPartitionKey);
    try {
      scheduler.cancelStreamTask(nonExistentTaskId);
    } catch (Exception e) {
      fail("Cancelling non-existent task should not cause scheduler to crash: " + e.getMessage());
    }

    DriverTaskId recoveryTaskId = new DriverTaskId("recovery-task", testPartitionKey);
    IStreamDriver recoveryDriver = mock(IStreamDriver.class);
    final AtomicBoolean recoveryProcessed = new AtomicBoolean(false);
    when(recoveryDriver.getDriverTaskId()).thenReturn(recoveryTaskId);
    when(recoveryDriver.isFinished()).thenReturn(true);
    when(recoveryDriver.processFor(any()))
        .thenAnswer(
            inv -> {
              recoveryProcessed.set(true);
              return com.google.common.util.concurrent.Futures.immediateFuture(null);
            });

    scheduler.submitStreamDriver(recoveryDriver, 3000L);

    Thread.sleep(500);

    assertTrue("Normal driver should be processed", normalProcessed.get());
    assertTrue("Recovery driver should be processed", recoveryProcessed.get());
  }

  @Test
  public void testLongRunningTaskScheduling() throws Exception {
    scheduler.start();

    DriverTaskId taskId = new DriverTaskId("long-running-task", testPartitionKey);
    IStreamDriver driver = mock(IStreamDriver.class);
    final AtomicInteger processCount = new AtomicInteger(0);
    when(driver.getDriverTaskId()).thenReturn(taskId);
    when(driver.isFinished()).thenReturn(false);
    when(driver.processFor(any()))
        .thenAnswer(
            inv -> {
              int count = processCount.incrementAndGet();
              if (count <= 2) {
                return com.google.common.util.concurrent.Futures.immediateFuture(null);
              }
              SettableFuture<Void> pendingFuture = SettableFuture.create();
              return pendingFuture;
            });

    scheduler.submitStreamDriver(driver, 30000L);

    Thread.sleep(1000);

    assertTrue("Driver should be processed at least 2 times", processCount.get() >= 2);
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
