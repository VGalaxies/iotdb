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
package org.apache.iotdb.confignode.procedure.impl.node;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TStreamNodeLocation;
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.manager.stream.StreamManager;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.env.RemoveStreamNodeHandler;
import org.apache.iotdb.confignode.procedure.impl.StateMachineProcedure;
import org.apache.iotdb.confignode.procedure.state.RemoveStreamNodeState;
import org.apache.iotdb.confignode.procedure.store.ProcedureFactory;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.utils.PublicBAOS;
import org.junit.Assert;
import org.junit.Test;

import java.io.DataOutputStream;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class RemoveStreamNodeProcedureTest {

  private final TStreamNodeLocation location =
      new TStreamNodeLocation(3, new TEndPoint("127.0.0.1", 10820));

  @Test
  public void serDeTest() throws Exception {
    RemoveStreamNodeProcedure procedure0 = new RemoveStreamNodeProcedure(location);
    try (PublicBAOS byteArrayOutputStream = new PublicBAOS();
        DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      procedure0.serialize(outputStream);
      ByteBuffer buffer =
          ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
      Assert.assertEquals(procedure0, ProcedureFactory.getInstance().create(buffer));
    }
  }

  @Test
  public void testExecutePrepareMarksRemoving() throws Exception {
    final TestEnv env = TestEnv.create();
    final TestRemoveStreamNodeProcedure procedure = new TestRemoveStreamNodeProcedure(location);

    procedure.executeState(env.env, RemoveStreamNodeState.REMOVE_STREAM_NODE_PREPARE);

    verify(env.handler).changeStreamNodeStatus(location, NodeStatus.Removing);
  }

  @Test
  public void testExecuteMigrateAlwaysDelegatesToHandler() throws Exception {
    final TestEnv env = TestEnv.create();
    final TestRemoveStreamNodeProcedure procedure = new TestRemoveStreamNodeProcedure(location);

    procedure.executeState(env.env, RemoveStreamNodeState.STREAM_TASKS_RUNTIME_MIGRATE);

    verify(env.handler).migrateRuntimeStreamTasks(eq(location), any());
  }

  @Test
  public void testExecuteStopPersistsThenStops() throws Exception {
    final TestEnv env = TestEnv.create();
    final TestRemoveStreamNodeProcedure procedure = new TestRemoveStreamNodeProcedure(location);

    procedure.executeState(env.env, RemoveStreamNodeState.STOP_STREAM_NODE);

    verify(env.handler).removeStreamNodePersistence(location);
    verify(env.handler).stopStreamNode(location);
  }

  @Test
  public void testMigrateFailureDoesNotFailBeforeRetryThreshold() throws Exception {
    final TestEnv env = TestEnv.create();
    doThrow(
            new IoTDBException(
                "migrate fail", TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode()))
        .when(env.handler)
        .migrateRuntimeStreamTasks(eq(location), any());
    final TestRemoveStreamNodeProcedure procedure = new TestRemoveStreamNodeProcedure(location);

    procedure.executeState(env.env, RemoveStreamNodeState.STREAM_TASKS_RUNTIME_MIGRATE);

    Assert.assertFalse(procedure.isFailed());
    verify(env.handler, never()).restartStoppedStreamTasks(any());
  }

  @Test
  public void testMigrateFailureAfterRetriesRestartsStoppedTasks() throws Exception {
    final TestEnv env = TestEnv.create();
    doThrow(
            new IoTDBException(
                "migrate fail", TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode()))
        .when(env.handler)
        .migrateRuntimeStreamTasks(eq(location), any());
    final TestRemoveStreamNodeProcedure procedure = new TestRemoveStreamNodeProcedure(location);
    procedure.setCyclesForTest(5);

    procedure.executeState(env.env, RemoveStreamNodeState.STREAM_TASKS_RUNTIME_MIGRATE);

    Assert.assertTrue(procedure.isFailed());
    verify(env.handler).restartStoppedStreamTasks(any());
  }

  private static class TestEnv {
    private final ConfigNodeProcedureEnv env;
    private final RemoveStreamNodeHandler handler;
    private final StreamManager streamManager;

    private TestEnv(
        ConfigNodeProcedureEnv env, RemoveStreamNodeHandler handler, StreamManager streamManager) {
      this.env = env;
      this.handler = handler;
      this.streamManager = streamManager;
    }

    private static TestEnv create() {
      final ConfigNodeProcedureEnv env = mock(ConfigNodeProcedureEnv.class);
      final ConfigManager configManager = mock(ConfigManager.class);
      final StreamManager streamManager = mock(StreamManager.class);
      final RemoveStreamNodeHandler handler = mock(RemoveStreamNodeHandler.class);
      when(env.getRemoveStreamNodeHandler()).thenReturn(handler);
      when(env.getConfigManager()).thenReturn(configManager);
      when(configManager.getStreamManager()).thenReturn(streamManager);
      return new TestEnv(env, handler, streamManager);
    }
  }

  private static class TestRemoveStreamNodeProcedure extends RemoveStreamNodeProcedure {

    private TestRemoveStreamNodeProcedure(TStreamNodeLocation removedStreamNode) {
      super(removedStreamNode);
    }

    private Flow executeState(ConfigNodeProcedureEnv env, RemoveStreamNodeState state)
        throws InterruptedException {
      return super.executeFromState(env, state);
    }

    private void setCyclesForTest(int cycles) {
      try {
        final Field cyclesField = StateMachineProcedure.class.getDeclaredField("cycles");
        cyclesField.setAccessible(true);
        cyclesField.setInt(this, cycles);
      } catch (ReflectiveOperationException e) {
        throw new AssertionError(e);
      }
    }
  }
}
