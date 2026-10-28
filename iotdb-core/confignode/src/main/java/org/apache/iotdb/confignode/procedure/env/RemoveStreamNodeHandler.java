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
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.common.rpc.thrift.TStreamNodeConfiguration;
import org.apache.iotdb.common.rpc.thrift.TStreamNodeLocation;
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.commons.stream.StreamTask;
import org.apache.iotdb.confignode.client.async.CnToSnAsyncRequestType;
import org.apache.iotdb.confignode.client.async.CnToSnInternalServiceAsyncRequestManager;
import org.apache.iotdb.confignode.client.async.handlers.StreamNodeAsyncRequestContext;
import org.apache.iotdb.confignode.consensus.request.write.streamnode.RemoveStreamNodePlan;
import org.apache.iotdb.confignode.i18n.ProcedureMessages;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.consensus.exception.ConsensusException;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.iotdb.confignode.conf.ConfigNodeConstant.REMOVE_STREAMNODE_PROCESS;

public class RemoveStreamNodeHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(RemoveStreamNodeHandler.class);

  private final ConfigManager configManager;

  public RemoveStreamNodeHandler(ConfigManager configManager) {
    this.configManager = configManager;
  }

  /**
   * Checks if the RemoveStreamNode request is valid.
   *
   * @param removeStreamNodePlan the RemoveStreamNodePlan to be validated
   * @return SUCCEED_STATUS if the request is valid, otherwise an appropriate error status
   */
  public TSStatus checkRemoveStreamNodeRequest(RemoveStreamNodePlan removeStreamNodePlan) {
    TSStatus status = checkStreamNodeExist(removeStreamNodePlan);
    if (isFailed(status)) {
      return status;
    }
    status = checkStreamTaskStatus(removeStreamNodePlan);
    if (isFailed(status)) {
      return status;
    }
    status = checkAllowRemoveStreamNodes(removeStreamNodePlan);
    return status;
  }

  /**
   * Checks whether the StreamNode specified for deletion exists in the cluster.
   *
   * @param removeStreamNodePlan the RemoveStreamNodePlan containing the StreamNode to be checked
   * @return SUCCEED_STATUS if the specified StreamNode exists in the cluster, STREAM_NODE_NOT_EXIST
   *     otherwise
   */
  public TSStatus checkStreamNodeExist(RemoveStreamNodePlan removeStreamNodePlan) {
    TSStatus status = new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    List<TStreamNodeLocation> registeredStreamNodes =
        configManager.getNodeManager().getRegisteredStreamNodes().stream()
            .map(TStreamNodeConfiguration::getLocation)
            .toList();
    if (!registeredStreamNodes.contains(removeStreamNodePlan.getStreamNodeLocation())) {
      status.setCode(TSStatusCode.STREAM_NODE_NOT_EXIST.getStatusCode());
      status.setMessage(
          ProcedureMessages.MESSAGE_THERE_EXIST_STREAM_NODE_IN_REQUEST_BUT_NOT_IN_CLUSTER_73F04636);
    }
    return status;
  }

  /**
   * Checks whether any StreamTask (regardless of status) references the given StreamNode, i.e. the
   * task's {@link StreamTask#getRunningOn()} equals the node's internal endpoint.
   *
   * @param streamNode the StreamNode to check
   * @return true if any StreamTask references the StreamNode
   */
  private boolean hasStreamTaskOn(TStreamNodeLocation streamNode) {
    final TEndPoint endPoint = streamNode.getInternalEndPoint();
    final String runningOn = endPoint.getIp() + ":" + endPoint.getPort();
    return configManager.getStreamManager().showStreams().stream()
        .map(StreamTask::getRunningOn)
        .anyMatch(runningOn::equals);
  }

  /**
   * Checks whether there is any StreamTask on the StreamNode to be removed.
   *
   * @param removeStreamNodePlan the RemoveStreamNodePlan to be evaluated
   * @return SUCCEED_STATUS if the StreamNode has no StreamTask, STREAM_TASK_EXIST_ON_STREAMNODE
   *     otherwise
   */
  public TSStatus checkStreamTaskStatus(RemoveStreamNodePlan removeStreamNodePlan) {
    TSStatus status = new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    if (configManager.getStreamManager().isClusterEnvironment()) {
      return status;
    }
    TStreamNodeLocation removedStreamNode = removeStreamNodePlan.getStreamNodeLocation();
    if (hasStreamTaskOn(removedStreamNode)) {
      status.setCode(TSStatusCode.STREAM_TASK_EXIST_ON_STREAMNODE.getStatusCode());
      status.setMessage(
          String.format(
              ProcedureMessages
                  .MESSAGE_FAILED_TO_REMOVE_STREAM_NODE_BECAUSE_STREAM_TASK_EXISTS_ON_IT_8582D7D6,
              removedStreamNode));
    }
    return status;
  }

  /**
   * Checks if it is allowed to remove the specified StreamNode from the cluster.
   *
   * @param removeStreamNodePlan the RemoveStreamNodePlan to be evaluated
   * @return SUCCEED_STATUS if the request is valid, otherwise an appropriate error status
   */
  public TSStatus checkAllowRemoveStreamNodes(RemoveStreamNodePlan removeStreamNodePlan) {
    return configManager
        .getProcedureManager()
        .checkRemoveStreamNode(removeStreamNodePlan.getStreamNodeLocation());
  }

  /**
   * Mark the StreamNode as the given status in the ConfigNode's load cache, so that the
   * StreamManager won't assign new tasks to it until the node is removed from the cluster.
   *
   * @param removedStreamNode the StreamNode whose status needs to be changed
   * @param nodeStatus the new status to assign (e.g., Removing)
   */
  public void changeStreamNodeStatus(TStreamNodeLocation removedStreamNode, NodeStatus nodeStatus) {
    LOGGER.info(
        ProcedureMessages.BEGIN_TO_CHANGE_STREAMNODE_STATUS_NODESTATUS,
        REMOVE_STREAMNODE_PROCESS,
        removedStreamNode.getStreamNodeId(),
        nodeStatus);

    // Match the ReadOnly reason assigned by the DataNode's SET_SYSTEM_STATUS RPC.
    String statusReason = nodeStatus == NodeStatus.ReadOnly ? NodeStatus.MANUAL : null;

    // Force updating NodeStatus
    configManager
        .getLoadManager()
        .trySetNodeStatus(removedStreamNode.getStreamNodeId(), nodeStatus, statusReason, true);
  }

  /**
   * Drop the runtime of RUNNING StreamTasks on the StreamNode to be removed, without removing
   * StreamTask metadata. {@code stoppedTasksToRestart} is updated across Phase 1 retries and
   * cleared once Phase 1 succeeds. Throws if any remaining RUNNING task cannot be stopped so the
   * procedure can retry; pre-remove STOPPED tasks are never added.
   *
   * @param removedStreamNode the StreamNode whose StreamTasks need to be dropped
   * @param stoppedTasksToRestart in/out set of task names to restart on the original node if Phase
   *     1 retries are exhausted
   * @throws IoTDBException if any StreamTask cannot be stopped
   */
  public void migrateRuntimeStreamTasks(
      TStreamNodeLocation removedStreamNode, Set<String> stoppedTasksToRestart)
      throws IoTDBException {
    LOGGER.info(
        ProcedureMessages.BEGIN_TO_DROP_RUNTIME_STREAM_TASKS_ON_STREAMNODE,
        REMOVE_STREAMNODE_PROCESS,
        removedStreamNode);
    final TSStatus migrateStreamTasksStatus =
        configManager
            .getStreamManager()
            .migrateRuntimeTasks(removedStreamNode, stoppedTasksToRestart);
    if (migrateStreamTasksStatus.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      throw new IoTDBException(migrateStreamTasksStatus);
    }
  }

  /**
   * Best-effort restart of {@code stoppedTasksToRestart} on their original StreamNode. Called after
   * Phase 1 migrate retries are exhausted; empty when Phase 1 already succeeded.
   */
  public void restartStoppedStreamTasks(Set<String> stoppedTasksToRestart) {
    configManager.getStreamManager().restartStoppedTasks(stoppedTasksToRestart);
  }

  /**
   * Send the stop request to the StreamNode so that it gracefully drains its running tasks and
   * exits, then drop its load cache since the node is about to disappear.
   *
   * @param removedStreamNode the StreamNode to be stopped
   */
  public void stopStreamNode(TStreamNodeLocation removedStreamNode) {
    LOGGER.info(
        ProcedureMessages.BEGIN_TO_STOP_STREAM_NODES_AND_KILL_THE_STREAMNODE_PROCESS,
        REMOVE_STREAMNODE_PROCESS,
        removedStreamNode);

    StreamNodeAsyncRequestContext<Object, TSStatus> stopStreamNodeContext =
        new StreamNodeAsyncRequestContext<>(CnToSnAsyncRequestType.STOP_STREAM_NODE);
    stopStreamNodeContext.putRequest(removedStreamNode.getStreamNodeId(), removedStreamNode);
    stopStreamNodeContext.putNodeLocation(removedStreamNode.getStreamNodeId(), removedStreamNode);

    CnToSnInternalServiceAsyncRequestManager.getInstance()
        .sendAsyncRequestWithRetry(stopStreamNodeContext);

    // Always drop the load cache, even if responseMap is empty (RPC totally failed); otherwise the
    // node is already deleted from NodeInfo while a Running heartbeat cache entry can linger.
    configManager.getLoadManager().removeNodeCache(removedStreamNode.getStreamNodeId());

    for (Map.Entry<Integer, TSStatus> entry : stopStreamNodeContext.getResponseMap().entrySet()) {
      int streamNodeId = entry.getKey();
      if (entry.getValue().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        LOGGER.error(
            ProcedureMessages.STOP_STREAM_NODE_MEETS_ERROR_ERROR_STREAMNODE,
            REMOVE_STREAMNODE_PROCESS,
            entry.getValue());
      } else {
        LOGGER.info(
            ProcedureMessages.STOP_STREAM_NODE_SUCCESS, REMOVE_STREAMNODE_PROCESS, streamNodeId);
      }
    }
  }

  /**
   * Remove the StreamNode record through the consensus layer.
   *
   * <p>Throws an {@link IoTDBException} if the consensus write fails, so the removal procedure
   * fails before reporting completion and the StreamNode stays registered.
   *
   * @param removedStreamNode the StreamNode to be removed
   * @throws IoTDBException if the consensus write fails
   */
  public void removeStreamNodePersistence(TStreamNodeLocation removedStreamNode)
      throws IoTDBException {
    try {
      final TSStatus status =
          configManager.getConsensusManager().write(new RemoveStreamNodePlan(removedStreamNode));
      if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        throw new IoTDBException(status);
      }
    } catch (ConsensusException e) {
      LOGGER.warn(ProcedureMessages.FAILED_IN_THE_WRITE_API_EXECUTING_THE_CONSENSUS_LAYER_DUE, e);
      throw new IoTDBException(e, TSStatusCode.REMOVE_STREAM_NODE_ERROR.getStatusCode());
    }
  }

  private boolean isFailed(TSStatus status) {
    return status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode();
  }
}
