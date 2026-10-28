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

package org.apache.iotdb.confignode.manager;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TNodeResource;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.common.rpc.thrift.TStreamNodeConfiguration;
import org.apache.iotdb.common.rpc.thrift.TStreamNodeLocation;
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant;
import org.apache.iotdb.commons.schema.table.TreeViewSchema;
import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.commons.stream.StreamTask;
import org.apache.iotdb.confignode.i18n.ManagerMessages;
import org.apache.iotdb.confignode.i18n.ProcedureMessages;
import org.apache.iotdb.confignode.manager.load.LoadManager;
import org.apache.iotdb.confignode.manager.node.NodeManager;
import org.apache.iotdb.confignode.manager.stream.StreamManager;
import org.apache.iotdb.confignode.persistence.stream.StreamInfo;
import org.apache.iotdb.confignode.procedure.Procedure;
import org.apache.iotdb.confignode.procedure.ProcedureExecutor;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.env.RemoveDataNodeHandler;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.impl.node.RemoveDataNodesProcedure;
import org.apache.iotdb.confignode.procedure.impl.node.RemoveStreamNodeProcedure;
import org.apache.iotdb.confignode.procedure.impl.region.RegionMigrateProcedure;
import org.apache.iotdb.confignode.procedure.impl.region.RegionMigrationPlan;
import org.apache.iotdb.confignode.procedure.impl.stream.CreateStreamProcedure;
import org.apache.iotdb.confignode.procedure.impl.stream.DropStreamProcedure;
import org.apache.iotdb.confignode.procedure.impl.stream.StartStreamProcedure;
import org.apache.iotdb.confignode.procedure.impl.stream.StopStreamProcedure;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.iotdb.db.service.RegionMigrateService.isFailed;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class ProcedureManagerTest {

  private static ProcedureManager PROCEDURE_MANAGER;

  private static RemoveDataNodeHandler REMOVE_DATA_NODE_HANDLER;

  private static LoadManager LOAD_MANAGER;

  private static NodeManager NODE_MANAGER;

  private static StreamManager STREAM_MANAGER;

  private static final ConcurrentHashMap<Long, Procedure<ConfigNodeProcedureEnv>> procedureMap =
      new ConcurrentHashMap<>();

  private final TStreamNodeLocation streamNodeLocationA =
      new TStreamNodeLocation(5, new TEndPoint("127.0.0.1", 10820));

  private final TStreamNodeLocation streamNodeLocationB =
      new TStreamNodeLocation(6, new TEndPoint("127.0.0.2", 10821));

  private final TConsensusGroupId consensusGroupId =
      new TConsensusGroupId(TConsensusGroupType.DataRegion, 1);

  private final TDataNodeLocation removeDataNodeLocationA =
      new TDataNodeLocation(
          10,
          new TEndPoint("127.0.0.1", 6667),
          new TEndPoint("127.0.0.1", 6668),
          new TEndPoint("127.0.0.1", 6669),
          new TEndPoint("127.0.0.1", 6670),
          new TEndPoint("127.0.0.1", 6671));

  private final TDataNodeLocation removeDataNodeLocationB =
      new TDataNodeLocation(
          11,
          new TEndPoint("127.0.0.1", 6677),
          new TEndPoint("127.0.0.1", 6678),
          new TEndPoint("127.0.0.1", 6679),
          new TEndPoint("127.0.0.1", 6680),
          new TEndPoint("127.0.0.1", 6681));

  private final TDataNodeLocation toDataNodeLocation =
      new TDataNodeLocation(
          12,
          new TEndPoint("127.0.0.1", 6687),
          new TEndPoint("127.0.0.1", 6688),
          new TEndPoint("127.0.0.1", 6689),
          new TEndPoint("127.0.0.1", 6690),
          new TEndPoint("127.0.0.1", 6691));

  private final TDataNodeLocation coordinatorDataNodeLocation =
      new TDataNodeLocation(
          13,
          new TEndPoint("127.0.0.1", 6697),
          new TEndPoint("127.0.0.1", 6698),
          new TEndPoint("127.0.0.1", 6699),
          new TEndPoint("127.0.0.1", 6700),
          new TEndPoint("127.0.0.1", 6701));

  private final List<TDataNodeLocation> removedDataNodes =
      new ArrayList<>(Arrays.asList(removeDataNodeLocationA, removeDataNodeLocationB));

  @BeforeClass
  public static void setUp() throws IOException {
    IManager CONFIG_MANAGER = new ConfigManager();
    IManager CONFIG_MANAGER_SPY = spy(CONFIG_MANAGER);
    ProcedureManager procedureManager = CONFIG_MANAGER.getProcedureManager();
    PROCEDURE_MANAGER = spy(procedureManager);

    ProcedureExecutor<ConfigNodeProcedureEnv> procedureExecutor = PROCEDURE_MANAGER.getExecutor();
    ProcedureExecutor<ConfigNodeProcedureEnv> PROCEDURE_EXECUTOR = spy(procedureExecutor);

    ConfigNodeProcedureEnv env = PROCEDURE_MANAGER.getEnv();
    ConfigNodeProcedureEnv ENV = spy(env);

    RemoveDataNodeHandler removeDataNodeHandler = ENV.getRemoveDataNodeHandler();
    REMOVE_DATA_NODE_HANDLER = spy(removeDataNodeHandler);

    LoadManager loadManager = CONFIG_MANAGER.getLoadManager();
    LOAD_MANAGER = spy(loadManager);
    NODE_MANAGER = mock(NodeManager.class);
    STREAM_MANAGER = mock(StreamManager.class);

    when(PROCEDURE_MANAGER.getExecutor()).thenReturn(PROCEDURE_EXECUTOR);
    when(PROCEDURE_EXECUTOR.getProcedures()).thenReturn(procedureMap);
    when(PROCEDURE_MANAGER.getEnv()).thenReturn(ENV);
    when(ENV.getRemoveDataNodeHandler()).thenReturn(REMOVE_DATA_NODE_HANDLER);

    // Inject the LoadManager spy into the call chain used by checkRemoveDataNodes, so that the
    // node-status stubs below take effect on the production path
    when(PROCEDURE_MANAGER.getConfigManager()).thenReturn(CONFIG_MANAGER_SPY);
    when(CONFIG_MANAGER_SPY.getNodeManager()).thenReturn(NODE_MANAGER);
    when(CONFIG_MANAGER_SPY.getLoadManager()).thenReturn(LOAD_MANAGER);
    when(CONFIG_MANAGER_SPY.getStreamManager()).thenReturn(STREAM_MANAGER);
  }

  @Before
  public void clearProcedures() {
    procedureMap.clear();
  }

  @Test
  public void testCheckRemoveDataNodeWithAnotherRemoveProcedure() {
    Map<Integer, NodeStatus> nodeStatusMap = new HashMap<>();
    nodeStatusMap.put(10, NodeStatus.Running);
    RemoveDataNodesProcedure anotherRemoveProcedure =
        new RemoveDataNodesProcedure(removedDataNodes, nodeStatusMap);
    procedureMap.put(0L, anotherRemoveProcedure);

    TSStatus status = PROCEDURE_MANAGER.checkRemoveDataNodes(removedDataNodes);
    Assert.assertTrue(isFailed(status));
  }

  @Test
  public void testPreSubmissionFailureWithNoProcedureIdIsReportedImmediately() {
    final Procedure<ConfigNodeProcedureEnv> procedure = mock(Procedure.class);
    when(procedure.getProcId()).thenReturn(Procedure.NO_PROC_ID);
    when(procedure.isFinished()).thenReturn(false);
    when(procedure.isFailed()).thenReturn(true);
    when(procedure.getException()).thenReturn(new ProcedureException("conflict"));

    Assert.assertEquals(
        TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode(),
        PROCEDURE_MANAGER.waitingProcedureFinished(procedure, 0).getCode());
  }

  @Test
  public void testCheckRemoveDataNodeWithConflictRegionMigrateProcedure() {
    RegionMigrateProcedure regionMigrateProcedure =
        new RegionMigrateProcedure(
            consensusGroupId,
            removeDataNodeLocationA,
            removeDataNodeLocationB,
            coordinatorDataNodeLocation,
            coordinatorDataNodeLocation);
    procedureMap.put(0L, regionMigrateProcedure);

    Set<TConsensusGroupId> set = new HashSet<>();
    set.add(consensusGroupId);
    when(REMOVE_DATA_NODE_HANDLER.getRemovedDataNodesRegionSet(removedDataNodes)).thenReturn(set);

    TSStatus status = PROCEDURE_MANAGER.checkRemoveDataNodes(removedDataNodes);
    Assert.assertTrue(isFailed(status));
  }

  @Test
  public void testCheckRemoveDataNodeWithRegionMigrateProcedureConflictsWithEachOther() {
    RegionMigrationPlan regionMigrationPlanA =
        new RegionMigrationPlan(consensusGroupId, removeDataNodeLocationA);
    regionMigrationPlanA.setToDataNode(toDataNodeLocation);
    RegionMigrationPlan regionMigrationPlanB =
        new RegionMigrationPlan(consensusGroupId, removeDataNodeLocationB);
    regionMigrationPlanB.setToDataNode(toDataNodeLocation);

    List<RegionMigrationPlan> regionMigrationPlans =
        new ArrayList<>(Arrays.asList(regionMigrationPlanA, regionMigrationPlanB));
    when(REMOVE_DATA_NODE_HANDLER.getRegionMigrationPlans(removedDataNodes))
        .thenReturn(regionMigrationPlans);

    TSStatus status = PROCEDURE_MANAGER.checkRemoveDataNodes(removedDataNodes);
    Assert.assertTrue(isFailed(status));
  }

  @Test
  public void testCheckRemoveDataNodeWithAnotherUnknownDataNode() {
    Set<TDataNodeLocation> relatedDataNodes = new HashSet<>();
    relatedDataNodes.add(removeDataNodeLocationA);
    relatedDataNodes.add(coordinatorDataNodeLocation);

    when(REMOVE_DATA_NODE_HANDLER.getRelatedDataNodeLocations(removeDataNodeLocationA))
        .thenReturn(relatedDataNodes);

    when(LOAD_MANAGER.getNodeStatus(removeDataNodeLocationA.getDataNodeId()))
        .thenReturn(NodeStatus.Running);
    when(LOAD_MANAGER.getNodeStatus(coordinatorDataNodeLocation.getDataNodeId()))
        .thenReturn(NodeStatus.Unknown);

    TSStatus status = PROCEDURE_MANAGER.checkRemoveDataNodes(removedDataNodes);
    Assert.assertTrue(isFailed(status));
    Assert.assertTrue(status.getMessage().contains("unknown"));
  }

  @Test
  public void testCheckRemoveDataNodeWithAnotherStoppedDataNode() {
    Set<TDataNodeLocation> relatedDataNodes = new HashSet<>();
    relatedDataNodes.add(removeDataNodeLocationA);
    relatedDataNodes.add(coordinatorDataNodeLocation);

    when(REMOVE_DATA_NODE_HANDLER.getRelatedDataNodeLocations(removeDataNodeLocationA))
        .thenReturn(relatedDataNodes);

    when(LOAD_MANAGER.getNodeStatus(removeDataNodeLocationA.getDataNodeId()))
        .thenReturn(NodeStatus.Running);
    when(LOAD_MANAGER.getNodeStatus(coordinatorDataNodeLocation.getDataNodeId()))
        .thenReturn(NodeStatus.Stopped);

    TSStatus status = PROCEDURE_MANAGER.checkRemoveDataNodes(removedDataNodes);
    Assert.assertTrue(isFailed(status));
    Assert.assertTrue(status.getMessage().contains("stopped"));
  }

  @Test
  public void testInjectTreeViewSourceAttributes() {
    final Map<String, String> topicAttributes = new HashMap<>();
    final TsTable treeViewTable = new TsTable("tree_view");
    treeViewTable.addProp(TreeViewSchema.TREE_PATH_PATTERN, "root.sg.**");

    ProcedureManager.injectTreeViewSourceAttributes(topicAttributes, treeViewTable);

    Assert.assertEquals(
        Boolean.TRUE.toString(), topicAttributes.get(PipeSourceConstant.SOURCE_CAPTURE_TREE_KEY));
    Assert.assertEquals(
        Boolean.FALSE.toString(), topicAttributes.get(PipeSourceConstant.SOURCE_CAPTURE_TABLE_KEY));
    Assert.assertEquals(
        "root.sg.**", topicAttributes.get(PipeSourceConstant.SOURCE_PATTERN_INCLUSION_KEY));
  }

  @Test
  public void testInjectTreeViewSourceAttributesPreservesUserTreePattern() {
    final Map<String, String> topicAttributes = new HashMap<>();
    topicAttributes.put(PipeSourceConstant.SOURCE_PATTERN_KEY, "root.custom.**");
    final TsTable treeViewTable = new TsTable("tree_view");
    treeViewTable.addProp(TreeViewSchema.TREE_PATH_PATTERN, "root.sg.**");

    ProcedureManager.injectTreeViewSourceAttributes(topicAttributes, treeViewTable);

    Assert.assertEquals(
        Boolean.TRUE.toString(), topicAttributes.get(PipeSourceConstant.SOURCE_CAPTURE_TREE_KEY));
    Assert.assertEquals(
        Boolean.FALSE.toString(), topicAttributes.get(PipeSourceConstant.SOURCE_CAPTURE_TABLE_KEY));
    Assert.assertEquals(
        "root.custom.**", topicAttributes.get(PipeSourceConstant.SOURCE_PATTERN_KEY));
    Assert.assertFalse(
        topicAttributes.containsKey(PipeSourceConstant.SOURCE_PATTERN_INCLUSION_KEY));
  }

  @Test
  public void testCreateStreamFailsWhenNoRunningStreamNode() {
    when(NODE_MANAGER.filterStreamNodeThroughStatus(NodeStatus.Running))
        .thenReturn(Collections.emptyList());
    final StreamTask task = new StreamTask();
    task.setTaskName("stream_no_node");

    TSStatus status = PROCEDURE_MANAGER.createStream(task);
    Assert.assertEquals(
        TSStatusCode.NO_REGISTERED_STREAM_NODE_ERROR.getStatusCode(), status.getCode());
    Assert.assertEquals(
        ManagerMessages.MESSAGE_NO_RUNNING_STREAMNODE_FOUND_F21CB52B, status.getMessage());
  }

  @Test
  public void testGetRemovingStreamNodeIds() {
    procedureMap.put(1L, new RemoveStreamNodeProcedure(streamNodeLocationA));
    Assert.assertTrue(PROCEDURE_MANAGER.getRemovingStreamNodeIds().contains(5));

    final RemoveStreamNodeProcedure finished =
        spy(new RemoveStreamNodeProcedure(streamNodeLocationA));
    doReturn(true).when(finished).isFinished();
    procedureMap.put(1L, finished);
    Assert.assertTrue(PROCEDURE_MANAGER.getRemovingStreamNodeIds().isEmpty());

    procedureMap.put(2L, new RemoveStreamNodeProcedure(null));
    Assert.assertTrue(PROCEDURE_MANAGER.getRemovingStreamNodeIds().isEmpty());
  }

  @Test
  public void testGetStreamNamesUnderOperation() {
    procedureMap.put(1L, new StartStreamProcedure("starting"));
    procedureMap.put(2L, new StopStreamProcedure("stopping"));
    procedureMap.put(3L, new DropStreamProcedure("dropping"));
    final StreamTask creating = new StreamTask();
    creating.setTaskName("creating");
    procedureMap.put(4L, new CreateStreamProcedure(creating));
    final StartStreamProcedure finished = spy(new StartStreamProcedure("finished"));
    doReturn(true).when(finished).isFinished();
    procedureMap.put(5L, finished);

    Assert.assertEquals(
        new HashSet<>(Arrays.asList("starting", "stopping", "dropping")),
        PROCEDURE_MANAGER.getInProgressStreamNames());
  }

  @Test
  public void testStreamProcedureOperationIdentityIncludesPayload() {
    final StreamTask firstTask = new StreamTask();
    firstTask.setTaskName("stream_0");
    firstTask.setCreator("creator_0");
    final StreamTask secondTask = new StreamTask();
    secondTask.setTaskName("stream_0");
    secondTask.setCreator("creator_1");

    Assert.assertTrue(
        new StartStreamProcedure("stream_0").isSameOperation(new StartStreamProcedure("stream_0")));
    Assert.assertFalse(
        new StartStreamProcedure("stream_0").isSameOperation(new StartStreamProcedure("stream_1")));
    Assert.assertFalse(
        new StartStreamProcedure("stream_0").isSameOperation(new StopStreamProcedure("stream_0")));
    Assert.assertTrue(
        new CreateStreamProcedure(firstTask).isSameOperation(new CreateStreamProcedure(firstTask)));
    Assert.assertFalse(
        new CreateStreamProcedure(firstTask)
            .isSameOperation(new CreateStreamProcedure(secondTask)));
  }

  @Test
  public void testCheckRemoveStreamNodeWithAnotherRemoveProcedure() {
    when(NODE_MANAGER.getRegisteredStreamNodes())
        .thenReturn(Collections.singletonList(streamNodeConfiguration(streamNodeLocationA)));
    procedureMap.put(0L, new RemoveStreamNodeProcedure(streamNodeLocationB));

    TSStatus status = PROCEDURE_MANAGER.checkRemoveStreamNode(streamNodeLocationA);
    Assert.assertEquals(TSStatusCode.REMOVE_STREAM_NODE_ERROR.getStatusCode(), status.getCode());
  }

  @Test
  public void testCheckRemoveStreamNodeWithConflictCreateStreamProcedure() {
    when(NODE_MANAGER.getRegisteredStreamNodes())
        .thenReturn(Collections.singletonList(streamNodeConfiguration(streamNodeLocationA)));
    final StreamTask task = new StreamTask();
    task.setTaskName("stream_conflict");
    procedureMap.put(0L, new CreateStreamProcedure(task));

    TSStatus status = PROCEDURE_MANAGER.checkRemoveStreamNode(streamNodeLocationA);
    Assert.assertEquals(TSStatusCode.REMOVE_STREAM_NODE_ERROR.getStatusCode(), status.getCode());
  }

  @Test
  public void testCheckRemoveStreamNodeWithConflictDropStreamProcedure() {
    when(NODE_MANAGER.getRegisteredStreamNodes())
        .thenReturn(Collections.singletonList(streamNodeConfiguration(streamNodeLocationA)));
    procedureMap.put(0L, new DropStreamProcedure("stream_conflict"));

    TSStatus status = PROCEDURE_MANAGER.checkRemoveStreamNode(streamNodeLocationA);
    Assert.assertEquals(TSStatusCode.REMOVE_STREAM_NODE_ERROR.getStatusCode(), status.getCode());
  }

  @Test
  public void testCheckRemoveStreamNodeWithConflictStartStreamProcedure() {
    when(NODE_MANAGER.getRegisteredStreamNodes())
        .thenReturn(Collections.singletonList(streamNodeConfiguration(streamNodeLocationA)));
    procedureMap.put(0L, new StartStreamProcedure("stream_conflict"));

    TSStatus status = PROCEDURE_MANAGER.checkRemoveStreamNode(streamNodeLocationA);
    Assert.assertEquals(TSStatusCode.REMOVE_STREAM_NODE_ERROR.getStatusCode(), status.getCode());
  }

  @Test
  public void testCheckRemoveStreamNodeWithConflictStopStreamProcedure() {
    when(NODE_MANAGER.getRegisteredStreamNodes())
        .thenReturn(Collections.singletonList(streamNodeConfiguration(streamNodeLocationA)));
    procedureMap.put(0L, new StopStreamProcedure("stream_conflict"));

    TSStatus status = PROCEDURE_MANAGER.checkRemoveStreamNode(streamNodeLocationA);
    Assert.assertEquals(TSStatusCode.REMOVE_STREAM_NODE_ERROR.getStatusCode(), status.getCode());
  }

  @Test
  public void testSubmitStreamOperationReturnsDetailedConflict() {
    final StreamTask task = new StreamTask();
    task.setTaskName("stream_conflict");
    procedureMap.put(0L, new CreateStreamProcedure(task));

    final TSStatus status = PROCEDURE_MANAGER.dropStream("stream_conflict");

    Assert.assertEquals(TSStatusCode.OVERLAP_WITH_EXISTING_TASK.getStatusCode(), status.getCode());
    Assert.assertEquals(
        String.format(
            ProcedureMessages
                .MESSAGE_STREAM_OPERATION_ARG_CONFLICTS_WITH_IN_PROGRESS_OPERATION_ARG_FOR_STREAM_ARG_DE215B81,
            ProcedureType.DROP_STREAM_PROCEDURE,
            ProcedureType.CREATE_STREAM_PROCEDURE,
            "stream_conflict"),
        status.getMessage());
  }

  @Test
  public void testSubmitStreamOperationConflictsWithRemoveStreamNode() {
    final RemoveStreamNodeProcedure removeProcedure =
        new RemoveStreamNodeProcedure(streamNodeLocationA);
    removeProcedure.setProcId(7);
    procedureMap.put(0L, removeProcedure);
    final StreamTask task = new StreamTask();
    task.setTaskName("stream_conflict");
    task.setRunningOn("127.0.0.1:10820");
    final StreamInfo streamInfo = new StreamInfo();
    streamInfo.addTask(task);
    when(STREAM_MANAGER.getStreamInfo()).thenReturn(streamInfo);

    final TSStatus status = PROCEDURE_MANAGER.stopStream("stream_conflict");

    Assert.assertEquals(TSStatusCode.OVERLAP_WITH_EXISTING_TASK.getStatusCode(), status.getCode());
    Assert.assertEquals(
        String.format(
            ProcedureMessages
                .MESSAGE_STREAM_OPERATION_ARG_CANNOT_BE_SUBMITTED_WHILE_REMOVESTREAMNODEPROCEDURE_PIDARG_IS_IN_PROGRESS_687C153D,
            ProcedureType.STOP_STREAM_PROCEDURE,
            7L),
        status.getMessage());
  }

  @Test
  public void testStreamOperationOnlyConflictsWithAssignedRemovingNode() {
    final StreamTask task = new StreamTask();
    task.setRunningOn("127.0.0.2:10821");

    Assert.assertFalse(ProcedureManager.isStreamAssignedToNode(task, streamNodeLocationA));
    Assert.assertTrue(ProcedureManager.isStreamAssignedToNode(task, streamNodeLocationB));
    Assert.assertFalse(ProcedureManager.isStreamAssignedToNode(null, streamNodeLocationA));
  }

  @Test
  public void testCheckRemoveStreamNodeRequiresOtherRunningInCluster() {
    when(STREAM_MANAGER.isClusterEnvironment()).thenReturn(true);
    when(NODE_MANAGER.getRegisteredStreamNodes())
        .thenReturn(
            Arrays.asList(
                streamNodeConfiguration(streamNodeLocationA),
                streamNodeConfiguration(streamNodeLocationB)));
    when(LOAD_MANAGER.getNodeStatus(6)).thenReturn(NodeStatus.Unknown);

    TSStatus status = PROCEDURE_MANAGER.checkRemoveStreamNode(streamNodeLocationA);
    Assert.assertEquals(TSStatusCode.REMOVE_STREAM_NODE_ERROR.getStatusCode(), status.getCode());
  }

  @Test
  public void testCheckRemoveStreamNodeSuccessWithOtherRunning() {
    when(STREAM_MANAGER.isClusterEnvironment()).thenReturn(true);
    when(NODE_MANAGER.getRegisteredStreamNodes())
        .thenReturn(
            Arrays.asList(
                streamNodeConfiguration(streamNodeLocationA),
                streamNodeConfiguration(streamNodeLocationB)));
    when(LOAD_MANAGER.getNodeStatus(6)).thenReturn(NodeStatus.Running);

    TSStatus status = PROCEDURE_MANAGER.checkRemoveStreamNode(streamNodeLocationA);
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
  }

  @Test
  public void testCheckRemoveStreamNodeSuccessInSingleNode() {
    when(STREAM_MANAGER.isClusterEnvironment()).thenReturn(false);
    when(NODE_MANAGER.getRegisteredStreamNodes())
        .thenReturn(Collections.singletonList(streamNodeConfiguration(streamNodeLocationA)));
    when(LOAD_MANAGER.getNodeStatus(5)).thenReturn(NodeStatus.Unknown);

    TSStatus status = PROCEDURE_MANAGER.checkRemoveStreamNode(streamNodeLocationA);
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
  }

  private static TStreamNodeConfiguration streamNodeConfiguration(TStreamNodeLocation location) {
    return new TStreamNodeConfiguration(location, new TNodeResource(1, 1024L));
  }
}
