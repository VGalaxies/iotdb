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

package org.apache.iotdb.confignode.procedure.env;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TNodeResource;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.common.rpc.thrift.TStreamNodeConfiguration;
import org.apache.iotdb.common.rpc.thrift.TStreamNodeLocation;
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.commons.stream.StreamTask;
import org.apache.iotdb.commons.utils.StatusUtils;
import org.apache.iotdb.confignode.consensus.request.write.streamnode.RemoveStreamNodePlan;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.manager.ProcedureManager;
import org.apache.iotdb.confignode.manager.consensus.ConsensusManager;
import org.apache.iotdb.confignode.manager.load.LoadManager;
import org.apache.iotdb.confignode.manager.node.NodeManager;
import org.apache.iotdb.confignode.manager.stream.StreamManager;
import org.apache.iotdb.consensus.exception.ConsensusException;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.LinkedHashSet;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class RemoveStreamNodeHandlerTest {

  private NodeManager nodeManager;
  private StreamManager streamManager;
  private ProcedureManager procedureManager;
  private LoadManager loadManager;
  private ConsensusManager consensusManager;
  private RemoveStreamNodeHandler handler;

  private final TStreamNodeLocation location1 =
      new TStreamNodeLocation(1, new TEndPoint("127.0.0.1", 10820));

  @Before
  public void setUp() {
    ConfigManager configManager = mock(ConfigManager.class);
    nodeManager = mock(NodeManager.class);
    streamManager = mock(StreamManager.class);
    procedureManager = mock(ProcedureManager.class);
    loadManager = mock(LoadManager.class);
    consensusManager = mock(ConsensusManager.class);
    when(configManager.getNodeManager()).thenReturn(nodeManager);
    when(configManager.getStreamManager()).thenReturn(streamManager);
    when(configManager.getProcedureManager()).thenReturn(procedureManager);
    when(configManager.getLoadManager()).thenReturn(loadManager);
    when(configManager.getConsensusManager()).thenReturn(consensusManager);
    handler = new RemoveStreamNodeHandler(configManager);
  }

  @Test
  public void testCheckStreamNodeExistFail() {
    when(nodeManager.getRegisteredStreamNodes()).thenReturn(Collections.emptyList());
    TSStatus status = handler.checkStreamNodeExist(new RemoveStreamNodePlan(location1));
    Assert.assertEquals(TSStatusCode.STREAM_NODE_NOT_EXIST.getStatusCode(), status.getCode());
  }

  @Test
  public void testCheckStreamNodeExistSuccess() {
    when(nodeManager.getRegisteredStreamNodes())
        .thenReturn(Collections.singletonList(configuration(location1)));
    TSStatus status = handler.checkStreamNodeExist(new RemoveStreamNodePlan(location1));
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
  }

  @Test
  public void testCheckStreamTaskStatusRejectedInSingleNode() {
    when(streamManager.isClusterEnvironment()).thenReturn(false);
    final StreamTask task = new StreamTask();
    task.setRunningOn("127.0.0.1:10820");
    when(streamManager.showStreams()).thenReturn(Collections.singletonList(task));

    TSStatus status = handler.checkStreamTaskStatus(new RemoveStreamNodePlan(location1));
    Assert.assertEquals(
        TSStatusCode.STREAM_TASK_EXIST_ON_STREAMNODE.getStatusCode(), status.getCode());
  }

  @Test
  public void testCheckStreamTaskStatusSkippedInCluster() {
    when(streamManager.isClusterEnvironment()).thenReturn(true);
    final StreamTask task = new StreamTask();
    task.setRunningOn("127.0.0.1:10820");
    when(streamManager.showStreams()).thenReturn(Collections.singletonList(task));

    TSStatus status = handler.checkStreamTaskStatus(new RemoveStreamNodePlan(location1));
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
    verify(streamManager, never()).showStreams();
  }

  @Test
  public void testCheckAllowRemoveDelegatesToProcedureManager() {
    when(procedureManager.checkRemoveStreamNode(location1)).thenReturn(StatusUtils.OK);
    TSStatus status = handler.checkAllowRemoveStreamNodes(new RemoveStreamNodePlan(location1));
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
    verify(procedureManager).checkRemoveStreamNode(location1);
  }

  @Test
  public void testChangeStreamNodeStatusForceUpdatesCache() {
    handler.changeStreamNodeStatus(location1, NodeStatus.Removing);
    verify(loadManager).trySetNodeStatus(eq(1), eq(NodeStatus.Removing), isNull(), eq(true));
  }

  @Test
  public void testMigrateRuntimeStreamTasksThrowsOnFailure() {
    when(streamManager.migrateRuntimeTasks(eq(location1), any()))
        .thenReturn(
            new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode())
                .setMessage("migrate fail"));
    try {
      handler.migrateRuntimeStreamTasks(location1, new LinkedHashSet<>());
      Assert.fail("expected IoTDBException");
    } catch (IoTDBException e) {
      Assert.assertEquals(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode(), e.getErrorCode());
    }
  }

  @Test
  public void testRemoveStreamNodePersistenceThrowsOnConsensusFail() throws Exception {
    when(consensusManager.write(any())).thenThrow(new ConsensusException("consensus fail"));
    try {
      handler.removeStreamNodePersistence(location1);
      Assert.fail("expected IoTDBException");
    } catch (IoTDBException e) {
      Assert.assertEquals(TSStatusCode.REMOVE_STREAM_NODE_ERROR.getStatusCode(), e.getErrorCode());
    }
  }

  @Test
  public void testCheckRemoveStreamNodeNotExistFail() {
    when(nodeManager.getRegisteredStreamNodes()).thenReturn(Collections.emptyList());
    TSStatus status = handler.checkRemoveStreamNodeRequest(new RemoveStreamNodePlan(location1));
    Assert.assertEquals(TSStatusCode.STREAM_NODE_NOT_EXIST.getStatusCode(), status.getCode());
    verify(streamManager, never()).isClusterEnvironment();
  }

  @Test
  public void testRemoveStreamNodePersistenceSuccess() throws Exception {
    when(consensusManager.write(any())).thenReturn(StatusUtils.OK);
    handler.removeStreamNodePersistence(location1);
    verify(consensusManager).write(any(RemoveStreamNodePlan.class));
  }

  private static TStreamNodeConfiguration configuration(TStreamNodeLocation location) {
    return new TStreamNodeConfiguration(location, new TNodeResource(1, 1024L));
  }
}
