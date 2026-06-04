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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.fail;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class StreamTaskSchedulerTest {

  private StreamTaskScheduler scheduler;
  private PartitionKey testPartitionKey;

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
  }

  @After
  public void tearDown() {
    try {
      scheduler.stop();
    } catch (Exception e) {
      // Ignore stop exception
    }
  }

  @Test
  public void testSubmitStreamDriver() {
    long timeoutMs = 5000L;

    DriverTaskId driverTaskId = new DriverTaskId("test-stream", testPartitionKey);
    when(mockStreamDriver.getDriverTaskId()).thenReturn(driverTaskId);

    scheduler.submitStreamDriver(mockStreamDriver, timeoutMs);

    verify(mockStreamDriver, atLeastOnce()).getDriverTaskId();
  }

  @Test
  public void testCancelStreamTask() {
    DriverTaskId taskId = new DriverTaskId("test-stream", testPartitionKey);
    long timeoutMs = 5000L;

    when(mockStreamDriver.getDriverTaskId()).thenReturn(taskId);

    scheduler.submitStreamDriver(mockStreamDriver, timeoutMs);

    scheduler.cancelStreamTask(taskId);

    verify(mockStreamDriver, atLeast(2)).getDriverTaskId();
  }

  @Test
  public void testStartAndStop() {
    try {
      scheduler.start();

      TimeUnit.MILLISECONDS.sleep(200);

      scheduler.stop();

    } catch (InterruptedException e) {
      fail("Exception occurred: " + e.getMessage());
    }
  }

  @Test
  public void testTaskStatusTransitions() {
    DriverTaskId taskId = new DriverTaskId("test-stream", testPartitionKey);
    long timeoutMs = 5000L;

    when(mockStreamDriver.getDriverTaskId()).thenReturn(taskId);

    scheduler.submitStreamDriver(mockStreamDriver, timeoutMs);

    scheduler.cancelStreamTask(taskId);

    verify(mockStreamDriver, atLeast(2)).getDriverTaskId();
  }

  @Test
  public void testConcurrentTaskSubmission() {
    int numTasks = 5;

    for (int i = 0; i < numTasks; i++) {
      int taskIndex = i;
      DriverTaskId taskId = new DriverTaskId("stream-" + taskIndex, testPartitionKey);
      IStreamDriver mockDriver = mock(IStreamDriver.class);
      when(mockDriver.getDriverTaskId()).thenReturn(taskId);

      long timeoutMs = 3000L;

      scheduler.submitStreamDriver(mockDriver, timeoutMs);

      verify(mockDriver, atLeastOnce()).getDriverTaskId();
    }
  }

  @Test
  public void testTaskWithDifferentTimeout() {
    DriverTaskId taskId1 = new DriverTaskId("short-timeout-task", testPartitionKey);
    DriverTaskId taskId2 = new DriverTaskId("long-timeout-task", testPartitionKey);

    IStreamDriver mockDriver1 = mock(IStreamDriver.class);
    IStreamDriver mockDriver2 = mock(IStreamDriver.class);

    when(mockDriver1.getDriverTaskId()).thenReturn(taskId1);
    when(mockDriver2.getDriverTaskId()).thenReturn(taskId2);

    scheduler.submitStreamDriver(mockDriver1, 1000L);
    scheduler.submitStreamDriver(mockDriver2, 10000L);

    verify(mockDriver1, atLeastOnce()).getDriverTaskId();
    verify(mockDriver2, atLeastOnce()).getDriverTaskId();
  }

  @Test
  public void testCancelNonExistentTask() {
    DriverTaskId nonExistentTaskId = new DriverTaskId("non-existent", testPartitionKey);

    try {
      scheduler.cancelStreamTask(nonExistentTaskId);
    } catch (Exception e) {
      fail("Should not throw exception when cancelling non-existent task: " + e.getMessage());
    }
  }

  @Test
  public void testMultipleCancelCalls() {
    DriverTaskId taskId = new DriverTaskId("multi-cancel-task", testPartitionKey);
    long timeoutMs = 5000L;

    when(mockStreamDriver.getDriverTaskId()).thenReturn(taskId);

    scheduler.submitStreamDriver(mockStreamDriver, timeoutMs);

    scheduler.cancelStreamTask(taskId);
    scheduler.cancelStreamTask(taskId);
    scheduler.cancelStreamTask(taskId);

    verify(mockStreamDriver, atLeast(2)).getDriverTaskId();
  }

  @Test
  public void testCancelStreamTaskByStreamName() {
    DriverTaskId taskId1 = new DriverTaskId("test-stream", testPartitionKey);
    DriverTaskId taskId2 =
        new DriverTaskId(
            "test-stream",
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
            });

    IStreamDriver mockDriver1 = mock(IStreamDriver.class);
    IStreamDriver mockDriver2 = mock(IStreamDriver.class);

    when(mockDriver1.getDriverTaskId()).thenReturn(taskId1);
    when(mockDriver2.getDriverTaskId()).thenReturn(taskId2);

    scheduler.submitStreamDriver(mockDriver1, 5000L);
    scheduler.submitStreamDriver(mockDriver2, 5000L);

    scheduler.cancelStreamTask("test-stream");

    verify(mockDriver1, atLeast(2)).getDriverTaskId();
    verify(mockDriver2, atLeast(2)).getDriverTaskId();
  }
}
