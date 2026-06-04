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

package org.apache.iotdb.streamnode.client;

import org.apache.iotdb.common.rpc.thrift.TConfigNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TExternalServiceListResp;
import org.apache.iotdb.common.rpc.thrift.TFlushReq;
import org.apache.iotdb.common.rpc.thrift.TNodeLocations;
import org.apache.iotdb.common.rpc.thrift.TPipeHeartbeatResp;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.common.rpc.thrift.TSeriesPartitionSlot;
import org.apache.iotdb.common.rpc.thrift.TSetConfigurationReq;
import org.apache.iotdb.common.rpc.thrift.TSetSpaceQuotaReq;
import org.apache.iotdb.common.rpc.thrift.TSetTTLReq;
import org.apache.iotdb.common.rpc.thrift.TSetThrottleQuotaReq;
import org.apache.iotdb.common.rpc.thrift.TShowAppliedConfigurationsResp;
import org.apache.iotdb.common.rpc.thrift.TShowConfigurationResp;
import org.apache.iotdb.common.rpc.thrift.TShowStreamResp;
import org.apache.iotdb.common.rpc.thrift.TShowTTLReq;
import org.apache.iotdb.common.rpc.thrift.TStreamNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TTestConnectionResp;
import org.apache.iotdb.commons.client.ClientManager;
import org.apache.iotdb.commons.client.ThriftClient;
import org.apache.iotdb.commons.client.factory.ThriftClientFactory;
import org.apache.iotdb.commons.client.property.ThriftClientProperty;
import org.apache.iotdb.commons.client.sync.SyncThriftClientWithErrorHandler;
import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.consensus.ConfigRegionId;
import org.apache.iotdb.confignode.rpc.thrift.IConfigNodeRPCService;
import org.apache.iotdb.confignode.rpc.thrift.TAINodeConfigurationResp;
import org.apache.iotdb.confignode.rpc.thrift.TAINodeRegisterReq;
import org.apache.iotdb.confignode.rpc.thrift.TAINodeRegisterResp;
import org.apache.iotdb.confignode.rpc.thrift.TAINodeRemoveReq;
import org.apache.iotdb.confignode.rpc.thrift.TAINodeRestartReq;
import org.apache.iotdb.confignode.rpc.thrift.TAINodeRestartResp;
import org.apache.iotdb.confignode.rpc.thrift.TAddConsensusGroupReq;
import org.apache.iotdb.confignode.rpc.thrift.TAlterEncodingCompressorReq;
import org.apache.iotdb.confignode.rpc.thrift.TAlterLogicalViewReq;
import org.apache.iotdb.confignode.rpc.thrift.TAlterOrDropTableReq;
import org.apache.iotdb.confignode.rpc.thrift.TAlterPipeReq;
import org.apache.iotdb.confignode.rpc.thrift.TAlterSchemaTemplateReq;
import org.apache.iotdb.confignode.rpc.thrift.TAlterTimeSeriesReq;
import org.apache.iotdb.confignode.rpc.thrift.TAuthizedPatternTreeResp;
import org.apache.iotdb.confignode.rpc.thrift.TAuthorizerRelationalReq;
import org.apache.iotdb.confignode.rpc.thrift.TAuthorizerReq;
import org.apache.iotdb.confignode.rpc.thrift.TAuthorizerResp;
import org.apache.iotdb.confignode.rpc.thrift.TCheckUserPrivilegesReq;
import org.apache.iotdb.confignode.rpc.thrift.TCloseConsumerReq;
import org.apache.iotdb.confignode.rpc.thrift.TConfigNodeHeartbeatReq;
import org.apache.iotdb.confignode.rpc.thrift.TConfigNodeHeartbeatResp;
import org.apache.iotdb.confignode.rpc.thrift.TConfigNodeRegisterReq;
import org.apache.iotdb.confignode.rpc.thrift.TConfigNodeRegisterResp;
import org.apache.iotdb.confignode.rpc.thrift.TCountDatabaseResp;
import org.apache.iotdb.confignode.rpc.thrift.TCountTimeSlotListReq;
import org.apache.iotdb.confignode.rpc.thrift.TCountTimeSlotListResp;
import org.apache.iotdb.confignode.rpc.thrift.TCreateCQReq;
import org.apache.iotdb.confignode.rpc.thrift.TCreateConsumerReq;
import org.apache.iotdb.confignode.rpc.thrift.TCreateExternalServiceReq;
import org.apache.iotdb.confignode.rpc.thrift.TCreateFunctionReq;
import org.apache.iotdb.confignode.rpc.thrift.TCreatePipePluginReq;
import org.apache.iotdb.confignode.rpc.thrift.TCreatePipeReq;
import org.apache.iotdb.confignode.rpc.thrift.TCreateSchemaTemplateReq;
import org.apache.iotdb.confignode.rpc.thrift.TCreateStreamReq;
import org.apache.iotdb.confignode.rpc.thrift.TCreateTableViewReq;
import org.apache.iotdb.confignode.rpc.thrift.TCreateTopicReq;
import org.apache.iotdb.confignode.rpc.thrift.TCreateTriggerReq;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeConfigurationResp;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeRegisterReq;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeRegisterResp;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeRemoveReq;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeRemoveResp;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeRestartReq;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeRestartResp;
import org.apache.iotdb.confignode.rpc.thrift.TDataPartitionReq;
import org.apache.iotdb.confignode.rpc.thrift.TDataPartitionTableResp;
import org.apache.iotdb.confignode.rpc.thrift.TDatabaseSchema;
import org.apache.iotdb.confignode.rpc.thrift.TDatabaseSchemaResp;
import org.apache.iotdb.confignode.rpc.thrift.TDeactivateSchemaTemplateReq;
import org.apache.iotdb.confignode.rpc.thrift.TDeleteDatabaseReq;
import org.apache.iotdb.confignode.rpc.thrift.TDeleteDatabasesReq;
import org.apache.iotdb.confignode.rpc.thrift.TDeleteLogicalViewReq;
import org.apache.iotdb.confignode.rpc.thrift.TDeleteTableDeviceReq;
import org.apache.iotdb.confignode.rpc.thrift.TDeleteTableDeviceResp;
import org.apache.iotdb.confignode.rpc.thrift.TDeleteTimeSeriesReq;
import org.apache.iotdb.confignode.rpc.thrift.TDescTable4InformationSchemaResp;
import org.apache.iotdb.confignode.rpc.thrift.TDescTableResp;
import org.apache.iotdb.confignode.rpc.thrift.TDropCQReq;
import org.apache.iotdb.confignode.rpc.thrift.TDropFunctionReq;
import org.apache.iotdb.confignode.rpc.thrift.TDropPipePluginReq;
import org.apache.iotdb.confignode.rpc.thrift.TDropPipeReq;
import org.apache.iotdb.confignode.rpc.thrift.TDropStreamReq;
import org.apache.iotdb.confignode.rpc.thrift.TDropSubscriptionReq;
import org.apache.iotdb.confignode.rpc.thrift.TDropTopicReq;
import org.apache.iotdb.confignode.rpc.thrift.TDropTriggerReq;
import org.apache.iotdb.confignode.rpc.thrift.TExtendRegionReq;
import org.apache.iotdb.confignode.rpc.thrift.TFetchTableResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetAINodeLocationResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetAllPipeInfoResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetAllSubscriptionInfoResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetAllTemplatesResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetAllTopicInfoResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetClusterIdResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetDataNodeLocationsResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetDatabaseReq;
import org.apache.iotdb.confignode.rpc.thrift.TGetJarInListReq;
import org.apache.iotdb.confignode.rpc.thrift.TGetJarInListResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetLocationForTriggerResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetPathsSetTemplatesReq;
import org.apache.iotdb.confignode.rpc.thrift.TGetPathsSetTemplatesResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetPipePluginTableResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetRegionGroupsByTimeReq;
import org.apache.iotdb.confignode.rpc.thrift.TGetRegionGroupsByTimeResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetRegionIdReq;
import org.apache.iotdb.confignode.rpc.thrift.TGetRegionIdResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetSeriesSlotListReq;
import org.apache.iotdb.confignode.rpc.thrift.TGetSeriesSlotListResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetTemplateResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetTimeSlotListReq;
import org.apache.iotdb.confignode.rpc.thrift.TGetTimeSlotListResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetTriggerTableResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetUDFTableResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetUdfTableReq;
import org.apache.iotdb.confignode.rpc.thrift.TLoginReq;
import org.apache.iotdb.confignode.rpc.thrift.TMigrateRegionReq;
import org.apache.iotdb.confignode.rpc.thrift.TPermissionInfoResp;
import org.apache.iotdb.confignode.rpc.thrift.TPipeConfigTransferReq;
import org.apache.iotdb.confignode.rpc.thrift.TPipeConfigTransferResp;
import org.apache.iotdb.confignode.rpc.thrift.TReconstructRegionReq;
import org.apache.iotdb.confignode.rpc.thrift.TRegionRouteMapResp;
import org.apache.iotdb.confignode.rpc.thrift.TRemoveRegionReq;
import org.apache.iotdb.confignode.rpc.thrift.TSchemaNodeManagementReq;
import org.apache.iotdb.confignode.rpc.thrift.TSchemaNodeManagementResp;
import org.apache.iotdb.confignode.rpc.thrift.TSchemaPartitionReq;
import org.apache.iotdb.confignode.rpc.thrift.TSchemaPartitionTableResp;
import org.apache.iotdb.confignode.rpc.thrift.TSetDataNodeStatusReq;
import org.apache.iotdb.confignode.rpc.thrift.TSetDataReplicationFactorReq;
import org.apache.iotdb.confignode.rpc.thrift.TSetSchemaReplicationFactorReq;
import org.apache.iotdb.confignode.rpc.thrift.TSetSchemaTemplateReq;
import org.apache.iotdb.confignode.rpc.thrift.TSetTimePartitionIntervalReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowAINodesResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowCQResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowClusterResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowConfigNodes4InformationSchemaResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowConfigNodesResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowDataNodes4InformationSchemaResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowDataNodesResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowDatabaseResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowPipePluginReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowPipeReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowPipeResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowRegionReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowRegionResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowStreamNodesResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowStreamsReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowSubscriptionReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowSubscriptionResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowTTLResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowTable4InformationSchemaResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowTableResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowThrottleReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowTopicReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowTopicResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowVariablesResp;
import org.apache.iotdb.confignode.rpc.thrift.TSpaceQuotaResp;
import org.apache.iotdb.confignode.rpc.thrift.TStartPipeReq;
import org.apache.iotdb.confignode.rpc.thrift.TStartStreamReq;
import org.apache.iotdb.confignode.rpc.thrift.TStopPipeReq;
import org.apache.iotdb.confignode.rpc.thrift.TStopStreamReq;
import org.apache.iotdb.confignode.rpc.thrift.TStreamNodeRegisterReq;
import org.apache.iotdb.confignode.rpc.thrift.TStreamNodeRegisterResp;
import org.apache.iotdb.confignode.rpc.thrift.TStreamNodeRemoveReq;
import org.apache.iotdb.confignode.rpc.thrift.TStreamNodeRestartReq;
import org.apache.iotdb.confignode.rpc.thrift.TStreamNodeRestartResp;
import org.apache.iotdb.confignode.rpc.thrift.TSubscribeReq;
import org.apache.iotdb.confignode.rpc.thrift.TSystemConfigurationResp;
import org.apache.iotdb.confignode.rpc.thrift.TTestOperation;
import org.apache.iotdb.confignode.rpc.thrift.TThrottleQuotaResp;
import org.apache.iotdb.confignode.rpc.thrift.TUnsetSchemaTemplateReq;
import org.apache.iotdb.confignode.rpc.thrift.TUnsubscribeReq;
import org.apache.iotdb.rpc.DeepCopyRpcTransportFactory;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.streamnode.conf.StreamNodeConfig;
import org.apache.iotdb.streamnode.conf.StreamNodeDescriptor;

import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLHandshakeException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;

public class StreamNodeClient implements IConfigNodeRPCService.Iface, ThriftClient, AutoCloseable {

  private static final Logger LOGGER = LoggerFactory.getLogger(StreamNodeClient.class);

  private static final int RETRY_NUM = 15;

  public static final String MSG_RECONNECTION_FAIL =
      "Fail to connect to any config node. Please check status of ConfigNodes or logs of connected StreamNode";

  private static final String MSG_RECONNECTION_STREAMNODE_FAIL =
      "Failed to connect to ConfigNode %s from StreamNode %s when executing %s, Exception:";

  private static final long RETRY_INTERVAL_MS = 1000L;

  private static final long WAIT_CN_LEADER_ELECTION_INTERVAL_MS = 2000L;

  private final ThriftClientProperty property;

  private IConfigNodeRPCService.Iface client;

  private TTransport transport;

  private TEndPoint configLeader;

  private List<TEndPoint> configNodes;

  private TEndPoint configNode;

  private int cursor = 0;

  private boolean isFirstInitiated;

  private final StreamNodeConfig config = StreamNodeDescriptor.getInstance().getConfig();

  ClientManager<ConfigRegionId, StreamNodeClient> clientManager;

  ConfigRegionId configRegionId = ConfigNodeInfo.CONFIG_REGION_ID;

  private final CommonConfig commonConfig = CommonDescriptor.getInstance().getConfig();

  public StreamNodeClient(
      List<TEndPoint> configNodes,
      ThriftClientProperty property,
      ClientManager<ConfigRegionId, StreamNodeClient> clientManager)
      throws TException {
    this.configNodes = configNodes;
    this.property = property;
    this.clientManager = clientManager;
    // Set the first configNode as configLeader for a tentative connection
    this.configLeader = this.configNodes.get(0);
    this.isFirstInitiated = true;

    connectAndSync();
  }

  public TTransport getTransport() {
    return transport;
  }

  public void connect(TEndPoint endpoint, int timeoutMs) throws TException {
    // Close existing transport before reassigning to prevent connection leaks.
    if (transport != null) {
      transport.close();
    }
    transport =
        commonConfig.isEnableInternalSSL()
            ? DeepCopyRpcTransportFactory.INSTANCE.getTransport(
                endpoint.getIp(),
                endpoint.getPort(),
                timeoutMs,
                commonConfig.getTrustStorePath(),
                commonConfig.getTrustStorePwd(),
                commonConfig.getKeyStorePath(),
                commonConfig.getKeyStorePwd())
            : DeepCopyRpcTransportFactory.INSTANCE.getTransport(
                // As there is a try-catch already, we do not need to use TSocket.wrap
                endpoint.getIp(), endpoint.getPort(), timeoutMs);
    if (!transport.isOpen()) {
      transport.open();
    }
    configNode = endpoint;

    client = new IConfigNodeRPCService.Client(property.getProtocolFactory().getProtocol(transport));
  }

  private void connectAndSync() throws TException {
    try {
      tryToConnect(property.getConnectionTimeoutMs());
    } catch (TException e) {
      // Can not connect to each config node
      syncLatestConfigNodeList();
      tryToConnect(property.getConnectionTimeoutMs());
    }
  }

  private void tryToConnect(int timeoutMs) throws TException {
    TException exception = null;
    if (configLeader != null) {
      try {
        connect(configLeader, timeoutMs);
        return;
      } catch (TException e) {
        logger.warn("The current node leader may have been down {}, try next node", configLeader);
        configLeader = null;
        exception = e;
      }
    } else {
      try {
        // Wait to start the next try
        Thread.sleep(RETRY_INTERVAL_MS);
      } catch (InterruptedException ignore) {
        Thread.currentThread().interrupt();
        logger.warn("Unexpected interruption when waiting to try to connect to ConfigNode");
      }
    }

    for (int tryHostNum = 0; tryHostNum < configNodes.size(); tryHostNum++) {
      cursor = (cursor + 1) % configNodes.size();
      TEndPoint tryEndpoint = configNodes.get(cursor);

      try {
        connect(tryEndpoint, timeoutMs);
        return;
      } catch (TException e) {
        logger.warn("The current node may have been down {},try next node", tryEndpoint);
        exception = e;
      }
    }
    if (exception != null
        && exception.getCause() != null
        && exception.getCause().getCause() != null
        && exception.getCause().getCause() instanceof IOException) {
      throw new TException(exception.getCause().getCause());
    }

    throw new TException(MSG_RECONNECTION_FAIL);
  }

  public void syncLatestConfigNodeList() {
    configNodes = ConfigNodeInfo.getInstance().getLatestConfigNodes();
    cursor = 0;
  }

  @Override
  public void close() {
    clientManager.returnClient(configRegionId, this);
    LOGGER.info("ConfigNodeClient closed");
  }

  @Override
  public void invalidate() {
    Optional.ofNullable(transport).ifPresent(TTransport::close);
  }

  @Override
  public void invalidateAll() {
    clientManager.clear(ConfigNodeInfo.CONFIG_REGION_ID);
  }

  @Override
  public boolean printLogWhenEncounterException() {
    return property.isPrintLogWhenEncounterException();
  }

  private boolean updateConfigNodeLeader(TSStatus status) {
    try {
      if (status.getCode() == TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode()) {
        if (status.isSetRedirectNode()) {
          configLeader =
              new TEndPoint(status.getRedirectNode().getIp(), status.getRedirectNode().getPort());
        } else {
          configLeader = null;
        }
        if (!isFirstInitiated) {
          logger.info(
              "Failed to connect to ConfigNode {} from StreamNode {}, because the current node is not "
                  + "leader or not ready yet, will try again later",
              configNode,
              config.getAddressAndPort());
        }
        return true;
      }
      return false;
    } finally {
      isFirstInitiated = false;
    }
  }

  /**
   * The frame of execute RPC, include logic of retry and exception handling.
   *
   * @param call which rpc should call
   * @param check check the rpc's result
   * @return rpc's result
   * @param <T> the type of rpc result
   * @throws TException if fails more than RETRY_NUM times, throw TException(MSG_RECONNECTION_FAIL)
   */
  private <T> T executeRemoteCallWithRetry(final Operation<T> call, final Predicate<T> check)
      throws TException {
    int detectedNodeNum = 0;
    for (int i = 0; i < RETRY_NUM; i++) {
      try {
        final T result = call.execute();
        if (check.test(result)) {
          return result;
        }
        detectedNodeNum++;
      } catch (TException e) {
        final String message =
            String.format(
                MSG_RECONNECTION_STREAMNODE_FAIL,
                configNode,
                config.getAddressAndPort(),
                Thread.currentThread().getStackTrace()[2].getMethodName());
        logger.warn(message, e);
        configLeader = null;
        if (e.getCause() != null && e.getCause() instanceof SSLHandshakeException) {
          throw e;
        }
      }

      // If we have detected all configNodes and still not return
      if (detectedNodeNum >= configNodes.size()) {
        // Clear count
        detectedNodeNum = 0;
        // Wait to start the next try
        try {
          Thread.sleep(WAIT_CN_LEADER_ELECTION_INTERVAL_MS);
        } catch (InterruptedException ignore) {
          Thread.currentThread().interrupt();
          logger.warn(
              "Unexpected interruption when waiting to try to connect to ConfigNode, may because current node has been down. Will break current execution process to avoid meaningless wait.");
          break;
        }
      }

      connectAndSync();
    }
    throw new TException(MSG_RECONNECTION_FAIL);
  }

  @FunctionalInterface
  private interface Operation<T> {
    T execute() throws TException;
  }

  @Override
  public TGetClusterIdResp getClusterId() throws TException {
    return executeRemoteCallWithRetry(
        () -> client.getClusterId(), resp -> !updateConfigNodeLeader(resp.status));
  }

  @Override
  public TDataNodeRegisterResp registerDataNode(TDataNodeRegisterReq req) throws TException {
    return null;
  }

  @Override
  public TDataNodeRestartResp restartDataNode(TDataNodeRestartReq req) throws TException {
    return null;
  }

  @Override
  public TAINodeRegisterResp registerAINode(TAINodeRegisterReq req) throws TException {
    return null;
  }

  @Override
  public TAINodeRestartResp restartAINode(TAINodeRestartReq req) throws TException {
    return null;
  }

  @Override
  public TSStatus removeAINode(TAINodeRemoveReq req) throws TException {
    return null;
  }

  @Override
  public TShowAINodesResp showAINodes() throws TException {
    return null;
  }

  @Override
  public TAINodeConfigurationResp getAINodeConfiguration(int aiNodeId) throws TException {
    return null;
  }

  @Override
  public TGetAINodeLocationResp getAINodeLocation() throws TException {
    return null;
  }

  @Override
  public TStreamNodeRegisterResp registerStreamNode(TStreamNodeRegisterReq req) throws TException {
    for (int i = 0; i < RETRY_NUM; i++) {
      try {
        TStreamNodeRegisterResp resp = client.registerStreamNode(req);

        if (!updateConfigNodeLeader(resp.status)) {
          return resp;
        }

        // set latest config node list
        List<TEndPoint> newConfigNodes = new ArrayList<>();
        for (TConfigNodeLocation configNodeLocation : resp.getConfigNodeList()) {
          newConfigNodes.add(configNodeLocation.getInternalEndPoint());
        }
        configNodes = newConfigNodes;
      } catch (TException e) {
        String message =
            String.format(
                MSG_RECONNECTION_STREAMNODE_FAIL,
                configNode,
                config.getAddressAndPort(),
                Thread.currentThread().getStackTrace()[1].getMethodName());
        logger.warn(message, e);
        configLeader = null;
      }
      connectAndSync();
    }
    throw new TException(MSG_RECONNECTION_FAIL);
  }

  @Override
  public TStreamNodeRestartResp restartStreamNode(TStreamNodeRestartReq req) throws TException {
    return executeRemoteCallWithRetry(
        () -> client.restartStreamNode(req), resp -> !updateConfigNodeLeader(resp.status));
  }

  @Override
  public TSStatus removeStreamNode(TStreamNodeRemoveReq req) throws TException {
    return executeRemoteCallWithRetry(
        () -> client.removeStreamNode(req), status -> !updateConfigNodeLeader(status));
  }

  @Override
  public TShowStreamNodesResp showStreamNodes() throws TException {
    return null;
  }

  @Override
  public TSStatus reportStreamNodeShutdown(TStreamNodeLocation streamNodeLocation)
      throws TException {
    return executeRemoteCallWithRetry(
        () -> client.reportStreamNodeShutdown(streamNodeLocation),
        status -> !updateConfigNodeLeader(status));
  }

  @Override
  public TSStatus createStream(TCreateStreamReq req) throws TException {
    return null;
  }

  @Override
  public TSStatus dropStream(TDropStreamReq req) throws TException {
    return null;
  }

  @Override
  public TSStatus startStream(TStartStreamReq req) throws TException {
    return null;
  }

  @Override
  public TSStatus stopStream(TStopStreamReq req) throws TException {
    return null;
  }

  @Override
  public TShowStreamResp showStreams(TShowStreamsReq req) throws TException {
    return null;
  }

  @Override
  public TSystemConfigurationResp getSystemConfiguration() throws TException {
    return client.getSystemConfiguration();
  }

  @Override
  public TDataNodeRemoveResp removeDataNode(TDataNodeRemoveReq req) throws TException {
    return null;
  }

  @Override
  public TSStatus reportDataNodeShutdown(TDataNodeLocation dataNodeLocation) throws TException {
    return null;
  }

  @Override
  public TDataNodeConfigurationResp getDataNodeConfiguration(int dataNodeId) throws TException {
    return null;
  }

  @Override
  public TSStatus setDatabase(TDatabaseSchema databaseSchema) throws TException {
    return null;
  }

  @Override
  public TSStatus alterDatabase(TDatabaseSchema databaseSchema) throws TException {
    return null;
  }

  @Override
  public TSStatus deleteDatabase(TDeleteDatabaseReq req) throws TException {
    return null;
  }

  @Override
  public TSStatus deleteDatabases(TDeleteDatabasesReq req) throws TException {
    return null;
  }

  @Override
  public TSStatus setSchemaReplicationFactor(TSetSchemaReplicationFactorReq req) throws TException {
    return null;
  }

  @Override
  public TSStatus setDataReplicationFactor(TSetDataReplicationFactorReq req) throws TException {
    return null;
  }

  @Override
  public TSStatus setTimePartitionInterval(TSetTimePartitionIntervalReq req) throws TException {
    return null;
  }

  @Override
  public TCountDatabaseResp countMatchedDatabases(TGetDatabaseReq req) throws TException {
    return null;
  }

  @Override
  public TDatabaseSchemaResp getMatchedDatabaseSchemas(TGetDatabaseReq req) throws TException {
    return null;
  }

  @Override
  public TSStatus callSpecialProcedure(TTestOperation operation) throws TException {
    return null;
  }

  @Override
  public TSchemaPartitionTableResp getSchemaPartitionTable(TSchemaPartitionReq req)
      throws TException {
    return null;
  }

  @Override
  public TSchemaPartitionTableResp getSchemaPartitionTableWithSlots(
      Map<String, List<TSeriesPartitionSlot>> dbSlotMap) throws TException {
    return null;
  }

  @Override
  public TSchemaPartitionTableResp getOrCreateSchemaPartitionTable(TSchemaPartitionReq req)
      throws TException {
    return null;
  }

  @Override
  public TSchemaPartitionTableResp getOrCreateSchemaPartitionTableWithSlots(
      Map<String, List<TSeriesPartitionSlot>> dbSlotMap) throws TException {
    return null;
  }

  @Override
  public TSchemaNodeManagementResp getSchemaNodeManagementPartition(TSchemaNodeManagementReq req)
      throws TException {
    return null;
  }

  @Override
  public TDataPartitionTableResp getDataPartitionTable(TDataPartitionReq req) throws TException {
    return null;
  }

  @Override
  public TDataPartitionTableResp getOrCreateDataPartitionTable(TDataPartitionReq req)
      throws TException {
    return null;
  }

  @Override
  public TSStatus dataPartitionTableIntegrityCheck() throws TException {
    return null;
  }

  @Override
  public TSStatus operatePermission(TAuthorizerReq req) throws TException {
    return null;
  }

  @Override
  public TSStatus operateRPermission(TAuthorizerRelationalReq req) throws TException {
    return null;
  }

  @Override
  public TAuthorizerResp queryPermission(TAuthorizerReq req) throws TException {
    return null;
  }

  @Override
  public TAuthorizerResp queryRPermission(TAuthorizerRelationalReq req) throws TException {
    return null;
  }

  @Override
  public TPermissionInfoResp login(TLoginReq req) throws TException {
    return null;
  }

  @Override
  public TPermissionInfoResp checkUserPrivileges(TCheckUserPrivilegesReq req) throws TException {
    return null;
  }

  @Override
  public TAuthizedPatternTreeResp fetchAuthizedPatternTree(TCheckUserPrivilegesReq req)
      throws TException {
    return null;
  }

  @Override
  public TPermissionInfoResp checkRoleOfUser(TAuthorizerReq req) throws TException {
    return null;
  }

  @Override
  public TPermissionInfoResp getUser(String userName) throws TException {
    return null;
  }

  @Override
  public TConfigNodeRegisterResp registerConfigNode(TConfigNodeRegisterReq req) throws TException {
    return null;
  }

  @Override
  public TSStatus addConsensusGroup(TAddConsensusGroupReq req) throws TException {
    return null;
  }

  @Override
  public TSStatus notifyRegisterSuccess() throws TException {
    return null;
  }

  @Override
  public TSStatus removeConfigNode(TConfigNodeLocation configNodeLocation) throws TException {
    return null;
  }

  @Override
  public TSStatus deleteConfigNodePeer(TConfigNodeLocation configNodeLocation) throws TException {
    return null;
  }

  @Override
  public TSStatus reportConfigNodeShutdown(TConfigNodeLocation configNodeLocation)
      throws TException {
    return null;
  }

  @Override
  public TSStatus stopAndClearConfigNode(TConfigNodeLocation configNodeLocation) throws TException {
    return null;
  }

  @Override
  public TConfigNodeHeartbeatResp getConfigNodeHeartBeat(TConfigNodeHeartbeatReq req)
      throws TException {
    return null;
  }

  @Override
  public TSStatus createFunction(TCreateFunctionReq req) throws TException {
    return null;
  }

  @Override
  public TSStatus dropFunction(TDropFunctionReq req) throws TException {
    return null;
  }

  @Override
  public TGetUDFTableResp getUDFTable(TGetUdfTableReq req) throws TException {
    return null;
  }

  @Override
  public TGetJarInListResp getUDFJar(TGetJarInListReq req) throws TException {
    return null;
  }

  @Override
  public TSStatus createTrigger(TCreateTriggerReq req) throws TException {
    return null;
  }

  @Override
  public TSStatus dropTrigger(TDropTriggerReq req) throws TException {
    return null;
  }

  @Override
  public TGetLocationForTriggerResp getLocationOfStatefulTrigger(String triggerName)
      throws TException {
    return null;
  }

  @Override
  public TGetTriggerTableResp getTriggerTable() throws TException {
    return null;
  }

  @Override
  public TGetTriggerTableResp getStatefulTriggerTable() throws TException {
    return null;
  }

  @Override
  public TGetJarInListResp getTriggerJar(TGetJarInListReq req) throws TException {
    return null;
  }

  @Override
  public TSStatus createPipePlugin(TCreatePipePluginReq req) throws TException {
    return null;
  }

  @Override
  public TSStatus dropPipePlugin(TDropPipePluginReq req) throws TException {
    return null;
  }

  @Override
  public TGetPipePluginTableResp getPipePluginTable() throws TException {
    return null;
  }

  @Override
  public TGetPipePluginTableResp getPipePluginTableExtended(TShowPipePluginReq req)
      throws TException {
    return null;
  }

  @Override
  public TGetJarInListResp getPipePluginJar(TGetJarInListReq req) throws TException {
    return null;
  }

  @Override
  public TShowTTLResp showTTL(TShowTTLReq req) throws TException {
    return null;
  }

  @Override
  public TSStatus setTTL(TSetTTLReq req) throws TException {
    return null;
  }

  @Override
  public TSStatus merge() throws TException {
    return null;
  }

  @Override
  public TSStatus flush(TFlushReq req) throws TException {
    return null;
  }

  @Override
  public TSStatus clearCache(Set<Integer> cacheClearOptions) throws TException {
    return null;
  }

  @Override
  public TSStatus setConfiguration(TSetConfigurationReq req) throws TException {
    return null;
  }

  @Override
  public TShowConfigurationResp showConfiguration(int nodeId) throws TException {
    return null;
  }

  @Override
  public TShowAppliedConfigurationsResp showAppliedConfigurations(int nodeId) throws TException {
    return null;
  }

  @Override
  public TSStatus startRepairData() throws TException {
    return null;
  }

  @Override
  public TSStatus stopRepairData() throws TException {
    return null;
  }

  @Override
  public TSStatus submitLoadConfigurationTask() throws TException {
    return null;
  }

  @Override
  public TSStatus loadConfiguration() throws TException {
    return null;
  }

  @Override
  public TSStatus setSystemStatus(String status) throws TException {
    return null;
  }

  @Override
  public TSStatus setDataNodeStatus(TSetDataNodeStatusReq req) throws TException {
    return null;
  }

  @Override
  public TSStatus migrateRegion(TMigrateRegionReq req) throws TException {
    return null;
  }

  @Override
  public TSStatus reconstructRegion(TReconstructRegionReq req) throws TException {
    return null;
  }

  @Override
  public TSStatus extendRegion(TExtendRegionReq req) throws TException {
    return null;
  }

  @Override
  public TSStatus removeRegion(TRemoveRegionReq req) throws TException {
    return null;
  }

  @Override
  public TSStatus killQuery(String queryId, int dataNodeId, String allowedUsername)
      throws TException {
    return null;
  }

  @Override
  public TGetDataNodeLocationsResp getReadableDataNodeLocations() throws TException {
    return null;
  }

  @Override
  public TShowClusterResp showCluster() throws TException {
    return null;
  }

  @Override
  public TShowVariablesResp showVariables() throws TException {
    return null;
  }

  @Override
  public TShowDataNodesResp showDataNodes() throws TException {
    return null;
  }

  @Override
  public TShowDataNodes4InformationSchemaResp showDataNodes4InformationSchema() throws TException {
    return null;
  }

  @Override
  public TShowConfigNodesResp showConfigNodes() throws TException {
    return null;
  }

  @Override
  public TShowConfigNodes4InformationSchemaResp showConfigNodes4InformationSchema()
      throws TException {
    return null;
  }

  @Override
  public TShowDatabaseResp showDatabase(TGetDatabaseReq req) throws TException {
    return null;
  }

  @Override
  public TTestConnectionResp submitTestConnectionTask(TNodeLocations nodeLocations)
      throws TException {
    return null;
  }

  @Override
  public TTestConnectionResp submitTestConnectionTaskToLeader() throws TException {
    return null;
  }

  @Override
  public TSStatus testConnectionEmptyRPC() throws TException {
    return null;
  }

  @Override
  public TShowRegionResp showRegion(TShowRegionReq req) throws TException {
    return null;
  }

  @Override
  public TRegionRouteMapResp getLatestRegionRouteMap() throws TException {
    return null;
  }

  @Override
  public TSStatus createSchemaTemplate(TCreateSchemaTemplateReq req) throws TException {
    return null;
  }

  @Override
  public TGetAllTemplatesResp getAllTemplates() throws TException {
    return null;
  }

  @Override
  public TGetTemplateResp getTemplate(String req) throws TException {
    return null;
  }

  @Override
  public TSStatus setSchemaTemplate(TSetSchemaTemplateReq req) throws TException {
    return null;
  }

  @Override
  public TGetPathsSetTemplatesResp getPathsSetTemplate(TGetPathsSetTemplatesReq req)
      throws TException {
    return null;
  }

  @Override
  public TSStatus deactivateSchemaTemplate(TDeactivateSchemaTemplateReq req) throws TException {
    return null;
  }

  @Override
  public TSStatus unsetSchemaTemplate(TUnsetSchemaTemplateReq req) throws TException {
    return null;
  }

  @Override
  public TSStatus dropSchemaTemplate(String req) throws TException {
    return null;
  }

  @Override
  public TSStatus alterSchemaTemplate(TAlterSchemaTemplateReq req) throws TException {
    return null;
  }

  @Override
  public TSStatus alterEncodingCompressor(TAlterEncodingCompressorReq req) throws TException {
    return null;
  }

  @Override
  public TSStatus deleteTimeSeries(TDeleteTimeSeriesReq req) throws TException {
    return null;
  }

  @Override
  public TSStatus alterTimeSeriesDataType(TAlterTimeSeriesReq req) throws TException {
    return null;
  }

  @Override
  public TSStatus deleteLogicalView(TDeleteLogicalViewReq req) throws TException {
    return null;
  }

  @Override
  public TSStatus alterLogicalView(TAlterLogicalViewReq req) throws TException {
    return null;
  }

  @Override
  public TSStatus createPipe(TCreatePipeReq req) throws TException {
    return null;
  }

  @Override
  public TSStatus alterPipe(TAlterPipeReq req) throws TException {
    return null;
  }

  @Override
  public TSStatus startPipe(String pipeName) throws TException {
    return null;
  }

  @Override
  public TSStatus startPipeExtended(TStartPipeReq req) throws TException {
    return null;
  }

  @Override
  public TSStatus stopPipe(String pipeName) throws TException {
    return null;
  }

  @Override
  public TSStatus stopPipeExtended(TStopPipeReq req) throws TException {
    return null;
  }

  @Override
  public TSStatus dropPipe(String pipeName) throws TException {
    return null;
  }

  @Override
  public TSStatus dropPipeExtended(TDropPipeReq req) throws TException {
    return null;
  }

  @Override
  public TShowPipeResp showPipe(TShowPipeReq req) throws TException {
    return null;
  }

  @Override
  public TGetAllPipeInfoResp getAllPipeInfo() throws TException {
    return null;
  }

  @Override
  public TPipeConfigTransferResp handleTransferConfigPlan(TPipeConfigTransferReq req)
      throws TException {
    return null;
  }

  @Override
  public TSStatus handlePipeConfigClientExit(String clientId) throws TException {
    return null;
  }

  @Override
  public TSStatus createTopic(TCreateTopicReq req) throws TException {
    return null;
  }

  @Override
  public TSStatus dropTopic(String topicName) throws TException {
    return null;
  }

  @Override
  public TSStatus dropTopicExtended(TDropTopicReq req) throws TException {
    return null;
  }

  @Override
  public TShowTopicResp showTopic(TShowTopicReq req) throws TException {
    return null;
  }

  @Override
  public TGetAllTopicInfoResp getAllTopicInfo() throws TException {
    return null;
  }

  @Override
  public TSStatus createConsumer(TCreateConsumerReq req) throws TException {
    return null;
  }

  @Override
  public TSStatus closeConsumer(TCloseConsumerReq req) throws TException {
    return null;
  }

  @Override
  public TSStatus createSubscription(TSubscribeReq req) throws TException {
    return null;
  }

  @Override
  public TSStatus dropSubscription(TUnsubscribeReq req) throws TException {
    return null;
  }

  @Override
  public TSStatus dropSubscriptionById(TDropSubscriptionReq req) throws TException {
    return null;
  }

  @Override
  public TShowSubscriptionResp showSubscription(TShowSubscriptionReq req) throws TException {
    return null;
  }

  @Override
  public TGetAllSubscriptionInfoResp getAllSubscriptionInfo() throws TException {
    return null;
  }

  @Override
  public TGetRegionIdResp getRegionId(TGetRegionIdReq req) throws TException {
    return null;
  }

  @Override
  public TGetTimeSlotListResp getTimeSlotList(TGetTimeSlotListReq req) throws TException {
    return null;
  }

  @Override
  public TCountTimeSlotListResp countTimeSlotList(TCountTimeSlotListReq req) throws TException {
    return null;
  }

  @Override
  public TGetSeriesSlotListResp getSeriesSlotList(TGetSeriesSlotListReq req) throws TException {
    return null;
  }

  @Override
  public TGetRegionGroupsByTimeResp getRegionGroupsByTime(TGetRegionGroupsByTimeReq req)
      throws TException {
    return null;
  }

  @Override
  public TSStatus createCQ(TCreateCQReq req) throws TException {
    return null;
  }

  @Override
  public TSStatus dropCQ(TDropCQReq req) throws TException {
    return null;
  }

  @Override
  public TShowCQResp showCQ() throws TException {
    return null;
  }

  @Override
  public TSStatus createExternalService(TCreateExternalServiceReq req) throws TException {
    return null;
  }

  @Override
  public TSStatus startExternalService(int dataNodeId, String serviceName) throws TException {
    return null;
  }

  @Override
  public TSStatus stopExternalService(int dataNodeId, String serviceName) throws TException {
    return null;
  }

  @Override
  public TSStatus dropExternalService(int dataNodeId, String serviceName) throws TException {
    return null;
  }

  @Override
  public TExternalServiceListResp showExternalService(int dataNodeId) throws TException {
    return null;
  }

  @Override
  public TSStatus setSpaceQuota(TSetSpaceQuotaReq req) throws TException {
    return null;
  }

  @Override
  public TSpaceQuotaResp showSpaceQuota(List<String> databases) throws TException {
    return null;
  }

  @Override
  public TSpaceQuotaResp getSpaceQuota() throws TException {
    return null;
  }

  @Override
  public TSStatus setThrottleQuota(TSetThrottleQuotaReq req) throws TException {
    return null;
  }

  @Override
  public TThrottleQuotaResp showThrottleQuota(TShowThrottleReq req) throws TException {
    return null;
  }

  @Override
  public TThrottleQuotaResp getThrottleQuota() throws TException {
    return null;
  }

  @Override
  public TSStatus pushHeartbeat(int dataNodeId, TPipeHeartbeatResp resp) throws TException {
    return null;
  }

  @Override
  public TSStatus createTable(ByteBuffer tableInfo) throws TException {
    return null;
  }

  @Override
  public TSStatus alterOrDropTable(TAlterOrDropTableReq req) throws TException {
    return null;
  }

  @Override
  public TShowTableResp showTables(String database, boolean isDetails) throws TException {
    return null;
  }

  @Override
  public TShowTable4InformationSchemaResp showTables4InformationSchema() throws TException {
    return null;
  }

  @Override
  public TDescTableResp describeTable(String database, String tableName, boolean isDetails)
      throws TException {
    return null;
  }

  @Override
  public TDescTable4InformationSchemaResp descTables4InformationSchema() throws TException {
    return null;
  }

  @Override
  public TFetchTableResp fetchTables(Map<String, Set<String>> fetchTableMap) throws TException {
    return null;
  }

  @Override
  public TDeleteTableDeviceResp deleteDevice(TDeleteTableDeviceReq req) throws TException {
    return null;
  }

  @Override
  public TSStatus createTableView(TCreateTableViewReq req) throws TException {
    return null;
  }

  public static class Factory extends ThriftClientFactory<ConfigRegionId, StreamNodeClient> {

    public Factory(
        ClientManager<ConfigRegionId, StreamNodeClient> clientManager,
        ThriftClientProperty thriftClientProperty) {
      super(clientManager, thriftClientProperty);
    }

    @Override
    public void destroyObject(
        ConfigRegionId configRegionId, PooledObject<StreamNodeClient> pooledObject) {
      pooledObject.getObject().invalidate();
    }

    @Override
    public PooledObject<StreamNodeClient> makeObject(ConfigRegionId configRegionId)
        throws Exception {
      return new DefaultPooledObject<>(
          SyncThriftClientWithErrorHandler.newErrorHandler(
              StreamNodeClient.class,
              StreamNodeClient.class.getConstructor(
                  List.class, thriftClientProperty.getClass(), clientManager.getClass()),
              ConfigNodeInfo.getInstance().getLatestConfigNodes(),
              thriftClientProperty,
              clientManager));
    }

    @Override
    public boolean validateObject(
        ConfigRegionId configRegionId, PooledObject<StreamNodeClient> pooledObject) {
      return Optional.ofNullable(pooledObject.getObject().getTransport())
          .map(TTransport::isOpen)
          .orElse(false);
    }
  }
}
