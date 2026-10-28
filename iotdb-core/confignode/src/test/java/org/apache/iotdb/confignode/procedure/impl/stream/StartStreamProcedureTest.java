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

package org.apache.iotdb.confignode.procedure.impl.stream;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.commons.stream.StreamTask;
import org.apache.iotdb.commons.stream.StreamTaskExpectedStatus;
import org.apache.iotdb.commons.stream.StreamTaskStatus;
import org.apache.iotdb.commons.utils.StatusUtils;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.manager.stream.StreamManager;
import org.apache.iotdb.confignode.persistence.stream.StreamInfo;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.state.stream.StartStreamState;
import org.apache.iotdb.confignode.procedure.store.ProcedureFactory;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.utils.PublicBAOS;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.DataOutputStream;
import java.nio.ByteBuffer;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class StartStreamProcedureTest {

  private ConfigNodeProcedureEnv env;
  private StreamManager streamManager;
  private StreamTask task;

  @Before
  public void setUp() {
    env = mock(ConfigNodeProcedureEnv.class);
    final ConfigManager configManager = mock(ConfigManager.class);
    streamManager = mock(StreamManager.class);
    final StreamInfo streamInfo = mock(StreamInfo.class);
    task = new StreamTask();
    task.setTaskName("stream_0");
    task.setEpoch(3);
    task.setStatus(StreamTaskStatus.CREATED);
    when(env.getConfigManager()).thenReturn(configManager);
    when(configManager.getStreamManager()).thenReturn(streamManager);
    when(streamManager.getStreamInfo()).thenReturn(streamInfo);
    when(streamInfo.getTask("stream_0")).thenReturn(task);
    when(streamInfo.getExpectedStatus("stream_0")).thenReturn(StreamTaskExpectedStatus.CREATED);
    when(streamManager.hasAvailableStreamNode()).thenReturn(true);
    when(streamManager.startStreamTask("stream_0")).thenReturn(StatusUtils.OK);
    when(streamManager.rollbackStartStreamTask(task, 4)).thenReturn(StatusUtils.OK);
    when(streamManager.persistStartStream(task, 3)).thenReturn(StatusUtils.OK);
  }

  @Test
  public void testSerializeAndDeserialize() throws Exception {
    Assert.assertEquals(0, StartStreamState.CHECK_EXISTENCE.ordinal());
    Assert.assertEquals(1, StartStreamState.START_RUNTIME.ordinal());
    Assert.assertEquals(2, StartStreamState.START_STREAM_METADATA.ordinal());
    final StartStreamProcedure procedure = new StartStreamProcedure("stream_0");

    final StartStreamProcedure recovered = (StartStreamProcedure) serializeAndRecover(procedure);

    Assert.assertEquals("stream_0", recovered.getStreamName());
  }

  @Test
  public void testPersistTargetAfterStartingRuntime() {
    final TestStartStreamProcedure procedure = new TestStartStreamProcedure("stream_0");

    procedure.executeState(env, StartStreamState.START_RUNTIME);

    verify(streamManager).startStreamTask("stream_0");
    verify(streamManager, never()).persistStartStream(task, 3);

    procedure.executeState(env, StartStreamState.START_STREAM_METADATA);

    verify(streamManager).persistStartStream(task, 3);
  }

  @Test
  public void testContinuesAcrossLeaderChanges() throws Exception {
    final TestStartStreamProcedure procedure = new TestStartStreamProcedure("stream_0");
    procedure.executeState(env, StartStreamState.CHECK_EXISTENCE);
    final StartStreamProcedure recoveredBeforeRuntime =
        (StartStreamProcedure) serializeAndRecover(procedure);

    recoveredBeforeRuntime.executeFromState(env, StartStreamState.START_RUNTIME);
    final StartStreamProcedure recoveredBeforeMetadata =
        (StartStreamProcedure) serializeAndRecover(recoveredBeforeRuntime);

    recoveredBeforeMetadata.executeFromState(env, StartStreamState.START_STREAM_METADATA);

    Assert.assertFalse(recoveredBeforeMetadata.isFailed());
    verify(streamManager).startStreamTask("stream_0");
    verify(streamManager).persistStartStream(task, 3);
  }

  @Test
  public void testPersistRuntimeEpochWhenTaskEpochFallsBehind() {
    final TestStartStreamProcedure procedure = new TestStartStreamProcedure("stream_0");
    procedure.executeState(env, StartStreamState.START_RUNTIME);
    task.setEpoch(2);

    procedure.executeState(env, StartStreamState.START_STREAM_METADATA);

    verify(streamManager).persistStartStream(task, 3);
  }

  @Test
  public void testRuntimeFailureDoesNotPersistTarget() {
    final TestStartStreamProcedure procedure = new TestStartStreamProcedure("stream_0");
    when(streamManager.startStreamTask("stream_0"))
        .thenReturn(new TSStatus(TSStatusCode.STREAM_START_ERROR.getStatusCode()));

    procedure.executeState(env, StartStreamState.START_RUNTIME);

    Assert.assertTrue(procedure.isFailed());
    verify(streamManager, never()).persistStartStream(task, 3);
  }

  @Test
  public void testMetadataFailureRollsBackRuntime() {
    final TestStartStreamProcedure procedure = new TestStartStreamProcedure("stream_0");
    when(streamManager.persistStartStream(task, 3))
        .thenReturn(new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode()));

    procedure.executeState(env, StartStreamState.CHECK_EXISTENCE);
    procedure.executeState(env, StartStreamState.START_RUNTIME);
    procedure.executeState(env, StartStreamState.START_STREAM_METADATA);

    Assert.assertTrue(procedure.isFailed());
    verify(streamManager).rollbackStartStreamTask(task, 4);
    Assert.assertEquals(
        TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode(),
        ((IoTDBException) procedure.getException().getCause()).getErrorCode());
  }

  @Test
  public void testNonExistentStreamFailureMessage() {
    final TestStartStreamProcedure procedure = new TestStartStreamProcedure("missing_stream");

    procedure.executeState(env, StartStreamState.CHECK_EXISTENCE);

    Assert.assertTrue(procedure.isFailed());
    Assert.assertEquals(
        "Stream not found: missing_stream", procedure.getException().getCause().getMessage());
  }

  @Test
  public void testStreamDisappearsAfterRuntimeStartFailureMessage() {
    final TestStartStreamProcedure procedure = new TestStartStreamProcedure("stream_0");
    when(streamManager.getStreamInfo().getTask("stream_0")).thenReturn(null);

    procedure.executeState(env, StartStreamState.START_RUNTIME);

    Assert.assertTrue(procedure.isFailed());
    Assert.assertEquals(
        "Stream not found: stream_0", procedure.getException().getCause().getMessage());
  }

  private static Object serializeAndRecover(final StartStreamProcedure procedure) throws Exception {
    try (PublicBAOS output = new PublicBAOS();
        DataOutputStream stream = new DataOutputStream(output)) {
      procedure.serialize(stream);
      return ProcedureFactory.getInstance()
          .create(ByteBuffer.wrap(output.getBuf(), 0, output.size()));
    }
  }

  private static class TestStartStreamProcedure extends StartStreamProcedure {

    private TestStartStreamProcedure(final String streamName) {
      super(streamName);
    }

    private void executeState(final ConfigNodeProcedureEnv env, final StartStreamState state) {
      super.executeFromState(env, state);
    }
  }
}
