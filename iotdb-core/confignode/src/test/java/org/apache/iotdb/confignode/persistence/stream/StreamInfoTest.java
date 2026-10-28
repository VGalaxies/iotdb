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
import org.apache.iotdb.commons.stream.StreamTaskExpectedStatus;
import org.apache.iotdb.commons.stream.StreamTaskStatus;
import org.apache.iotdb.confignode.consensus.request.write.stream.StartStreamPlan;
import org.apache.iotdb.confignode.consensus.request.write.stream.StopStreamPlan;
import org.apache.iotdb.confignode.consensus.request.write.stream.UpdateStreamEpochPlan;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicReference;

public class StreamInfoTest {

  @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  /** Verifies that every Stream status is counted independently and absent statuses return zero. */
  @Test
  public void testCountTasksByStatus() {
    final StreamInfo streamInfo = new StreamInfo();
    final StreamTask created = buildTask("created");
    final StreamTask running = buildTask("running");
    final StreamTask stopped = buildTask("stopped");
    final StreamTask unknown = buildTask("unknown");

    streamInfo.addTask(created);
    streamInfo.addTask(running);
    streamInfo.addTask(stopped);
    streamInfo.addTask(unknown);
    running.setStatus(StreamTaskStatus.RUNNING);
    stopped.setStatus(StreamTaskStatus.STOPPED);
    unknown.setStatus(StreamTaskStatus.UNKNOWN);

    Assert.assertEquals(1, streamInfo.countTasksByStatus(StreamTaskStatus.CREATED));
    Assert.assertEquals(1, streamInfo.countTasksByStatus(StreamTaskStatus.RUNNING));
    Assert.assertEquals(1, streamInfo.countTasksByStatus(StreamTaskStatus.STOPPED));
    Assert.assertEquals(1, streamInfo.countTasksByStatus(StreamTaskStatus.UNKNOWN));
    Assert.assertEquals(0, streamInfo.countTasksByStatus(StreamTaskStatus.DROPPED));

    created.setStatus(StreamTaskStatus.RUNNING);
    Assert.assertEquals(0, streamInfo.countTasksByStatus(StreamTaskStatus.CREATED));
    Assert.assertEquals(2, streamInfo.countTasksByStatus(StreamTaskStatus.RUNNING));
  }

  /** Verifies that Streams are grouped by non-empty RunningOn values and move between groups. */
  @Test
  public void testCountTasksByRunningOn() {
    final StreamInfo streamInfo = new StreamInfo();
    final StreamTask first = buildTask("first");
    final StreamTask second = buildTask("second");
    final StreamTask unassigned = buildTask("unassigned");

    streamInfo.addTask(first);
    streamInfo.addTask(second);
    streamInfo.addTask(unassigned);
    first.setRunningOn("127.0.0.1:10820");
    second.setRunningOn("127.0.0.1:10820");

    Assert.assertEquals(2, streamInfo.countTasksByRunningOn("127.0.0.1:10820"));
    Assert.assertEquals(0, streamInfo.countTasksByRunningOn("127.0.0.2:10820"));
    Assert.assertEquals(1, streamInfo.countTasksByRunningOn(""));

    second.setRunningOn("127.0.0.2:10820");
    Assert.assertEquals(1, streamInfo.countTasksByRunningOn("127.0.0.1:10820"));
    Assert.assertEquals(1, streamInfo.countTasksByRunningOn("127.0.0.2:10820"));
  }

  @Test
  public void testRemoveTaskCallsSourceOnRemoved() {
    final StreamInfo streamInfo = new StreamInfo();
    final AtomicReference<StreamTask> removedTask = new AtomicReference<>();
    final StreamTask task = buildTask("drop_me");
    task.setSource(new TrackingSource(removedTask));

    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), streamInfo.addTask(task).code);
    Assert.assertEquals(StreamTaskStatus.CREATED, task.getStatus());
    Assert.assertEquals(
        StreamTaskExpectedStatus.CREATED, streamInfo.getExpectedStatus(task.getTaskName()));

    final TSStatus status = streamInfo.removeTask("drop_me");

    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.code);
    Assert.assertSame(task, removedTask.get());
    Assert.assertNull(streamInfo.getTask("drop_me"));
    Assert.assertNull(streamInfo.getExpectedStatus("drop_me"));
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
    Assert.assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(),
        streamInfo.applyStopStream(new StopStreamPlan("stream_0", 1)).getCode());
    Assert.assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(),
        streamInfo.applyStartStream(new StartStreamPlan("stream_1", 2)).getCode());
    first.setRunningOn("127.0.0.1:10820");
    first.setLastDownReason("manual");

    Assert.assertTrue(streamInfo.processTakeSnapshot(temporaryFolder.getRoot()));

    final StreamInfo recovered = new StreamInfo();
    recovered.processLoadSnapshot(temporaryFolder.getRoot());

    final StreamTask recoveredFirst = recovered.getTask("stream_0");
    Assert.assertEquals(StreamTaskStatus.UNKNOWN, recoveredFirst.getStatus());
    Assert.assertEquals(StreamTaskExpectedStatus.STOPPED, recovered.getExpectedStatus("stream_0"));
    Assert.assertEquals("", recoveredFirst.getRunningOn());
    Assert.assertEquals("", recoveredFirst.getLastDownReason());
    Assert.assertEquals(0L, recoveredFirst.getLastHeartbeatTime());
    Assert.assertEquals(1, recoveredFirst.getEpoch());

    final StreamTask recoveredSecond = recovered.getTask("stream_1");
    Assert.assertEquals(StreamTaskStatus.UNKNOWN, recoveredSecond.getStatus());
    Assert.assertEquals(StreamTaskExpectedStatus.RUNNING, recovered.getExpectedStatus("stream_1"));
    Assert.assertEquals(2, recoveredSecond.getEpoch());

    final StreamTask third = buildTask("stream_2");
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), recovered.addTask(third).code);
    Assert.assertEquals(2L, third.getId());
  }

  @Test
  public void testLoadSnapshotReportsInvalidMagic() throws Exception {
    final File snapshotFile = new File(temporaryFolder.getRoot(), "stream_info.bin");
    try (DataOutputStream output = new DataOutputStream(new FileOutputStream(snapshotFile))) {
      output.writeInt(1);
    }

    final IOException exception =
        Assert.assertThrows(
            IOException.class,
            () -> new StreamInfo().processLoadSnapshot(temporaryFolder.getRoot()));

    Assert.assertEquals("Invalid StreamInfo snapshot magic: 1", exception.getMessage());
  }

  @Test
  public void testLoadSnapshotReportsUnsupportedVersion() throws Exception {
    final File snapshotFile = new File(temporaryFolder.getRoot(), "stream_info.bin");
    try (DataOutputStream output = new DataOutputStream(new FileOutputStream(snapshotFile))) {
      output.writeInt((int) 0xA17E5A71L);
      output.writeInt(2);
    }

    final IOException exception =
        Assert.assertThrows(
            IOException.class,
            () -> new StreamInfo().processLoadSnapshot(temporaryFolder.getRoot()));

    Assert.assertEquals("Unsupported StreamInfo snapshot version: 2", exception.getMessage());
  }

  @Test
  public void testLoadSnapshotReportsInvalidExpectedStatusOrdinal() throws Exception {
    final ByteBuffer taskBuffer = buildTask("stream_0").toByteBuffer();
    final byte[] taskBytes = new byte[taskBuffer.remaining()];
    taskBuffer.get(taskBytes);
    final File snapshotFile = new File(temporaryFolder.getRoot(), "stream_info.bin");
    try (DataOutputStream output = new DataOutputStream(new FileOutputStream(snapshotFile))) {
      output.writeInt((int) 0xA17E5A71L);
      output.writeInt(1);
      output.writeInt(1);
      output.writeInt(taskBytes.length);
      output.write(taskBytes);
      output.writeShort(Short.MAX_VALUE);
    }

    final IOException exception =
        Assert.assertThrows(
            IOException.class,
            () -> new StreamInfo().processLoadSnapshot(temporaryFolder.getRoot()));

    Assert.assertEquals(
        "Invalid expected status ordinal in StreamInfo snapshot: 32767", exception.getMessage());
  }

  @Test
  public void testStopPlanDoesNotRollBackEpoch() {
    final StreamInfo streamInfo = new StreamInfo();
    final StreamTask task = buildTask("stream_0");
    streamInfo.addTask(task);
    streamInfo.applyStartStream(new StartStreamPlan("stream_0", 5));

    streamInfo.applyStopStream(new StopStreamPlan("stream_0", 4));

    Assert.assertEquals(StreamTaskExpectedStatus.STOPPED, streamInfo.getExpectedStatus("stream_0"));
    Assert.assertEquals(StreamTaskStatus.UNKNOWN, task.getStatus());
    Assert.assertEquals(5, task.getEpoch());
  }

  @Test
  public void testStartPlanDoesNotRollBackEpoch() {
    final StreamInfo streamInfo = new StreamInfo();
    final StreamTask task = buildTask("stream_0");
    task.setEpoch(5);
    streamInfo.addTask(task);

    streamInfo.applyStartStream(new StartStreamPlan("stream_0", 4));

    Assert.assertEquals(StreamTaskExpectedStatus.RUNNING, streamInfo.getExpectedStatus("stream_0"));
    Assert.assertEquals(StreamTaskStatus.UNKNOWN, task.getStatus());
    Assert.assertEquals(5, task.getEpoch());
  }

  @Test
  public void testUpdateEpochPlanKeepsExpectedStatusAndDoesNotRollBack() {
    final StreamInfo streamInfo = new StreamInfo();
    final StreamTask task = buildTask("stream_0");
    streamInfo.addTask(task);
    streamInfo.applyStartStream(new StartStreamPlan("stream_0", 5));

    Assert.assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(),
        streamInfo.applyUpdateStreamEpoch(new UpdateStreamEpochPlan("stream_0", 8)).getCode());
    Assert.assertEquals(8, task.getEpoch());
    Assert.assertEquals(StreamTaskExpectedStatus.RUNNING, streamInfo.getExpectedStatus("stream_0"));

    streamInfo.applyUpdateStreamEpoch(new UpdateStreamEpochPlan("stream_0", 6));

    Assert.assertEquals(8, task.getEpoch());
    Assert.assertEquals(StreamTaskExpectedStatus.RUNNING, streamInfo.getExpectedStatus("stream_0"));
  }

  @Test
  public void testUpdateExpectedStatusForMissingStreamHasMessage() {
    final TSStatus status =
        new StreamInfo().applyStartStream(new StartStreamPlan("missing_stream", 1));

    Assert.assertEquals(TSStatusCode.STREAM_NOT_EXIST.getStatusCode(), status.getCode());
    Assert.assertEquals("Stream not found: missing_stream", status.getMessage());
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
    task.setWindow(new PeriodWindow(0, 1L, 0L));
    task.setTarget(new IoTDBTarget("db", "target", Collections.singletonList("value")));
    task.setProperties(new StreamProperties(-1L, 0, -1L, 0, false));
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
