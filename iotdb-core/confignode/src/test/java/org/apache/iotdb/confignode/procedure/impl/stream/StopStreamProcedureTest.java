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
import org.apache.iotdb.commons.stream.StreamTask;
import org.apache.iotdb.commons.stream.StreamTaskExpectedStatus;
import org.apache.iotdb.commons.stream.StreamTaskStatus;
import org.apache.iotdb.commons.utils.StatusUtils;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.manager.stream.StreamManager;
import org.apache.iotdb.confignode.persistence.stream.StreamInfo;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.state.stream.StopStreamState;
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

public class StopStreamProcedureTest {

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
    task.setStatus(StreamTaskStatus.RUNNING);
    when(env.getConfigManager()).thenReturn(configManager);
    when(configManager.getStreamManager()).thenReturn(streamManager);
    when(streamManager.getStreamInfo()).thenReturn(streamInfo);
    when(streamInfo.getTask("stream_0")).thenReturn(task);
    when(streamInfo.getExpectedStatus("stream_0")).thenReturn(StreamTaskExpectedStatus.RUNNING);
    when(streamManager.stopStreamTask("stream_0", 4)).thenReturn(StatusUtils.OK);
    when(streamManager.rollbackStopStreamTask(task)).thenReturn(StatusUtils.OK);
    when(streamManager.persistStopStream(task, 4)).thenReturn(StatusUtils.OK);
  }

  @Test
  public void testSerializeAndDeserialize() throws Exception {
    Assert.assertEquals(0, StopStreamState.CHECK_EXISTENCE.ordinal());
    Assert.assertEquals(1, StopStreamState.STOP_RUNTIME.ordinal());
    Assert.assertEquals(2, StopStreamState.STOP_STREAM_METADATA.ordinal());
    final StopStreamProcedure procedure = new StopStreamProcedure("stream_0");

    final StopStreamProcedure recovered = (StopStreamProcedure) serializeAndRecover(procedure);

    Assert.assertEquals("stream_0", recovered.getStreamName());
  }

  @Test
  public void testPersistTargetAfterStoppingRuntime() throws Exception {
    final TestStopStreamProcedure procedure = new TestStopStreamProcedure("stream_0");
    procedure.executeState(env, StopStreamState.CHECK_EXISTENCE);
    final StopStreamProcedure recovered = (StopStreamProcedure) serializeAndRecover(procedure);

    recovered.executeFromState(env, StopStreamState.STOP_RUNTIME);

    verify(streamManager).stopStreamTask("stream_0", 4);
    verify(streamManager, never()).persistStopStream(task, 4);

    final StopStreamProcedure recoveredAfterRuntime =
        (StopStreamProcedure) serializeAndRecover(recovered);
    recoveredAfterRuntime.executeFromState(env, StopStreamState.STOP_STREAM_METADATA);

    verify(streamManager).persistStopStream(task, 4);
    Assert.assertFalse(recoveredAfterRuntime.isFailed());
  }

  @Test
  public void testRuntimeFailureDoesNotPersistTarget() {
    final TestStopStreamProcedure procedure = new TestStopStreamProcedure("stream_0");
    when(streamManager.stopStreamTask("stream_0", 4))
        .thenReturn(new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode()));

    procedure.executeState(env, StopStreamState.CHECK_EXISTENCE);
    procedure.executeState(env, StopStreamState.STOP_RUNTIME);

    Assert.assertTrue(procedure.isFailed());
    verify(streamManager, never()).persistStopStream(task, 4);
  }

  @Test
  public void testPersistEpochAdvancedDuringRuntimeDiscovery() {
    final TestStopStreamProcedure procedure = new TestStopStreamProcedure("stream_0");
    when(streamManager.stopStreamTask("stream_0", 4))
        .thenAnswer(
            invocation -> {
              task.setEpoch(7);
              return StatusUtils.OK;
            });
    when(streamManager.persistStopStream(task, 7)).thenReturn(StatusUtils.OK);

    procedure.executeState(env, StopStreamState.CHECK_EXISTENCE);
    procedure.executeState(env, StopStreamState.STOP_RUNTIME);
    procedure.executeState(env, StopStreamState.STOP_STREAM_METADATA);

    verify(streamManager).persistStopStream(task, 7);
  }

  @Test
  public void testCreatedStreamTransitionsToStopped() {
    final TestStopStreamProcedure procedure = new TestStopStreamProcedure("stream_0");
    task.setStatus(StreamTaskStatus.CREATED);
    when(streamManager.getStreamInfo().getExpectedStatus("stream_0"))
        .thenReturn(StreamTaskExpectedStatus.CREATED);

    procedure.executeState(env, StopStreamState.CHECK_EXISTENCE);
    procedure.executeState(env, StopStreamState.STOP_RUNTIME);
    procedure.executeState(env, StopStreamState.STOP_STREAM_METADATA);

    Assert.assertFalse(procedure.isFailed());
    verify(streamManager).stopStreamTask("stream_0", 4);
    verify(streamManager).persistStopStream(task, 4);
  }

  @Test
  public void testEpochOverflowFailsBeforeStoppingRuntime() {
    final TestStopStreamProcedure procedure = new TestStopStreamProcedure("stream_0");
    task.setEpoch(Integer.MAX_VALUE);

    procedure.executeState(env, StopStreamState.CHECK_EXISTENCE);

    Assert.assertTrue(procedure.isFailed());
    verify(streamManager, never()).stopStreamTask("stream_0", Integer.MAX_VALUE);
  }

  @Test
  public void testMetadataFailureRestartsRuntime() {
    final TestStopStreamProcedure procedure = new TestStopStreamProcedure("stream_0");
    when(streamManager.persistStopStream(task, 4))
        .thenReturn(new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode()));

    procedure.executeState(env, StopStreamState.CHECK_EXISTENCE);
    procedure.executeState(env, StopStreamState.STOP_RUNTIME);
    procedure.executeState(env, StopStreamState.STOP_STREAM_METADATA);

    Assert.assertTrue(procedure.isFailed());
    verify(streamManager).rollbackStopStreamTask(task);
  }

  @Test
  public void testNonExistentStreamFailureMessage() {
    final TestStopStreamProcedure procedure = new TestStopStreamProcedure("missing_stream");

    procedure.executeState(env, StopStreamState.CHECK_EXISTENCE);

    Assert.assertTrue(procedure.isFailed());
    Assert.assertEquals(
        "Stream not found: missing_stream", procedure.getException().getCause().getMessage());
  }

  @Test
  public void testStreamDisappearsAfterRuntimeStopFailureMessage() {
    final TestStopStreamProcedure procedure = new TestStopStreamProcedure("stream_0");
    procedure.executeState(env, StopStreamState.CHECK_EXISTENCE);
    when(streamManager.getStreamInfo().getTask("stream_0")).thenReturn(null);

    procedure.executeState(env, StopStreamState.STOP_RUNTIME);

    Assert.assertTrue(procedure.isFailed());
    Assert.assertEquals(
        "Stream not found: stream_0", procedure.getException().getCause().getMessage());
  }

  private static Object serializeAndRecover(final StopStreamProcedure procedure) throws Exception {
    try (PublicBAOS output = new PublicBAOS();
        DataOutputStream stream = new DataOutputStream(output)) {
      procedure.serialize(stream);
      return ProcedureFactory.getInstance()
          .create(ByteBuffer.wrap(output.getBuf(), 0, output.size()));
    }
  }

  private static class TestStopStreamProcedure extends StopStreamProcedure {

    private TestStopStreamProcedure(final String streamName) {
      super(streamName);
    }

    private void executeState(final ConfigNodeProcedureEnv env, final StopStreamState state) {
      super.executeFromState(env, state);
    }
  }
}
