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

package org.apache.iotdb.confignode.persistence.stream;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.stream.IoTDBSubscriptionSource;
import org.apache.iotdb.commons.stream.IoTDBTarget;
import org.apache.iotdb.commons.stream.PeriodWindow;
import org.apache.iotdb.commons.stream.StreamNodeTableTypeProvider;
import org.apache.iotdb.commons.stream.StreamProperties;
import org.apache.iotdb.commons.stream.StreamSource;
import org.apache.iotdb.commons.stream.StreamSourceType;
import org.apache.iotdb.commons.stream.StreamTask;
import org.apache.iotdb.commons.stream.StreamTaskStatus;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicReference;

public class StreamInfoTest {

  @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void testRemoveTaskCallsSourceOnRemoved() {
    final StreamInfo streamInfo = new StreamInfo();
    final AtomicReference<StreamTask> removedTask = new AtomicReference<>();
    final StreamTask task = buildTask("drop_me");
    task.setSource(new TrackingSource(removedTask));

    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), streamInfo.addTask(task).code);
    Assert.assertEquals(StreamTaskStatus.CREATED, task.getStatus());

    final TSStatus status = streamInfo.removeTask("drop_me");

    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.code);
    Assert.assertSame(task, removedTask.get());
    Assert.assertNull(streamInfo.getTask("drop_me"));
  }

  @Test
  public void testRemoveTaskKeepsTaskWhenSourceCleanupFails() {
    final StreamInfo streamInfo = new StreamInfo();
    final StreamTask task = buildTask("drop_failed");
    task.setSource(new FailingSource());

    Assert.assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(), streamInfo.addTask(task).getCode());

    final TSStatus status = streamInfo.removeTask("drop_failed");

    Assert.assertEquals(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode(), status.getCode());
    Assert.assertSame(task, streamInfo.getTask("drop_failed"));
  }

  @Test
  public void testSnapshotRecoveryNormalizesRuntimeStateAndNextStreamId() throws Exception {
    final StreamInfo streamInfo = new StreamInfo();
    final StreamTask first = buildTask("stream_0");
    final StreamTask second = buildTask("stream_1");
    Assert.assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(), streamInfo.addTask(first).code);
    Assert.assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(), streamInfo.addTask(second).code);
    first.setStatus(StreamTaskStatus.STOPPED);
    first.setRunningOn("127.0.0.1:10820");
    first.setLastDownReason("manual");

    Assert.assertTrue(streamInfo.processTakeSnapshot(temporaryFolder.getRoot()));

    final StreamInfo recovered = new StreamInfo();
    recovered.processLoadSnapshot(temporaryFolder.getRoot());

    final StreamTask recoveredFirst = recovered.getTask("stream_0");
    Assert.assertEquals(StreamTaskStatus.RUNNING, recoveredFirst.getStatus());
    Assert.assertEquals("", recoveredFirst.getRunningOn());
    Assert.assertEquals("", recoveredFirst.getLastDownReason());
    Assert.assertTrue(recoveredFirst.getLastUpTime() > 0);
    Assert.assertEquals(recoveredFirst.getLastUpTime(), recoveredFirst.getLastHeartbeatTime());

    final StreamTask third = buildTask("stream_2");
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), recovered.addTask(third).code);
    Assert.assertEquals(2L, third.getId());
  }

  private StreamTask buildTask(final String taskName) {
    final StreamTask task = new StreamTask();
    task.setTaskName(taskName);
    task.setCreator("creator");
    task.setCreationTime(1L);
    task.setSubQuery("select count(*) from test_table");
    task.setTypeProvider(new StreamNodeTableTypeProvider(Collections.emptyMap()));
    task.setSource(
        new IoTDBSubscriptionSource(
            "db",
            "table1",
            null,
            Collections.singletonList("status"),
            "localhost",
            6667,
            "root",
            "root"));
    task.setWindow(new PeriodWindow(1L, 0L));
    task.setTarget(new IoTDBTarget("db", "target", Collections.singletonList("value")));
    task.setProperties(
        new StreamProperties(-1L, -1L, false, null, -1L, StreamProperties.EventType.WINDOW_CLOSE));
    return task;
  }

  private static class TrackingSource extends StreamSource {

    private final AtomicReference<StreamTask> removedTask;

    private TrackingSource(final AtomicReference<StreamTask> removedTask) {
      this.removedTask = removedTask;
    }

    @Override
    public StreamSourceType getType() {
      return StreamSourceType.IOTDB_SUBSCRIPTION;
    }

    @Override
    public void serialize(final DataOutputStream outputStream) throws IOException {
      throw new UnsupportedOperationException("TrackingSource is not serialized");
    }

    @Override
    public void onRemoved(final StreamTask task) {
      removedTask.set(task);
    }
  }

  private static class FailingSource extends StreamSource {

    @Override
    public StreamSourceType getType() {
      return StreamSourceType.IOTDB_SUBSCRIPTION;
    }

    @Override
    public void serialize(final DataOutputStream outputStream) throws IOException {
      throw new UnsupportedOperationException("FailingSource is not serialized");
    }

    @Override
    public void onRemoved(final StreamTask task) throws IOException {
      throw new IOException("cleanup failed");
    }
  }
}
