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
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.common.rpc.thrift.TShowStreamResp;
import org.apache.iotdb.common.rpc.thrift.TStreamNodeLocation;
import org.apache.iotdb.commons.client.AbstractConfigNodeClient;
import org.apache.iotdb.commons.client.AbstractConfigNodeInfo;
import org.apache.iotdb.commons.client.ClientManager;
import org.apache.iotdb.commons.client.factory.ThriftClientFactory;
import org.apache.iotdb.commons.client.property.ThriftClientProperty;
import org.apache.iotdb.commons.client.sync.SyncThriftClientWithErrorHandler;
import org.apache.iotdb.commons.consensus.ConfigRegionId;
import org.apache.iotdb.commons.i18n.ConfigMessages;
import org.apache.iotdb.confignode.rpc.thrift.TAutoSyncViewColumnsReq;
import org.apache.iotdb.confignode.rpc.thrift.TAutoSyncViewColumnsResp;
import org.apache.iotdb.confignode.rpc.thrift.TCreateStreamReq;
import org.apache.iotdb.confignode.rpc.thrift.TDropStreamReq;
import org.apache.iotdb.confignode.rpc.thrift.TGetClusterIdResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowStreamNodesResp;
import org.apache.iotdb.confignode.rpc.thrift.TStartStreamReq;
import org.apache.iotdb.confignode.rpc.thrift.TStopStreamReq;
import org.apache.iotdb.confignode.rpc.thrift.TStreamNodeRegisterReq;
import org.apache.iotdb.confignode.rpc.thrift.TStreamNodeRegisterResp;
import org.apache.iotdb.confignode.rpc.thrift.TStreamNodeRemoveReq;
import org.apache.iotdb.confignode.rpc.thrift.TStreamNodeRestartReq;
import org.apache.iotdb.confignode.rpc.thrift.TStreamNodeRestartResp;
import org.apache.iotdb.streamnode.conf.StreamNodeConfig;
import org.apache.iotdb.streamnode.conf.StreamNodeDescriptor;

import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class ConfigNodeClient extends AbstractConfigNodeClient<ConfigNodeClient> {
  private static final Logger logger = LoggerFactory.getLogger(ConfigNodeClient.class);

  private static final String NODE_TYPE_NAME = "StreamNode";

  public static final String MSG_RECONNECTION_FAIL =
      String.format(ConfigMessages.MSG_RECONNECTION_FAIL, NODE_TYPE_NAME);

  private final StreamNodeConfig config = StreamNodeDescriptor.getInstance().getConfig();

  public ConfigNodeClient(
      List<TEndPoint> configNodes,
      ThriftClientProperty property,
      ClientManager<ConfigRegionId, ConfigNodeClient> clientManager)
      throws TException {
    super(configNodes, property, clientManager);
  }

  @Override
  protected final String getNodeTypeName() {
    return NODE_TYPE_NAME;
  }

  @Override
  protected final TEndPoint getAddressAndPort() {
    return config.getAddressAndPort();
  }

  @Override
  protected final AbstractConfigNodeInfo getConfigNodeInfo() {
    return ConfigNodeInfo.getInstance();
  }

  @Override
  public TGetClusterIdResp getClusterId() throws TException {
    return executeRemoteCallWithRetry(
        () -> client.getClusterId(), resp -> !updateConfigNodeLeader(resp.status));
  }

  // ======================================== STREAM ===========================================

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
                ConfigMessages.MSG_RECONNECTION_NODE_FAIL,
                configNode,
                getNodeTypeName(),
                getAddressAndPort(),
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
    return null;
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
  public TShowStreamResp showStreams() throws TException {
    return null;
  }

  @Override
  public TAutoSyncViewColumnsResp autoSyncViewColumns(TAutoSyncViewColumnsReq req)
      throws TException {
    return null;
  }

  public static class Factory extends ThriftClientFactory<ConfigRegionId, ConfigNodeClient> {

    public Factory(
        ClientManager<ConfigRegionId, ConfigNodeClient> clientManager,
        ThriftClientProperty thriftClientProperty) {
      super(clientManager, thriftClientProperty);
    }

    @Override
    public void destroyObject(
        ConfigRegionId configRegionId, PooledObject<ConfigNodeClient> pooledObject) {
      pooledObject.getObject().invalidate();
    }

    @Override
    public PooledObject<ConfigNodeClient> makeObject(ConfigRegionId configRegionId)
        throws Exception {
      return new DefaultPooledObject<>(
          SyncThriftClientWithErrorHandler.newErrorHandler(
              ConfigNodeClient.class,
              ConfigNodeClient.class.getConstructor(
                  List.class, thriftClientProperty.getClass(), clientManager.getClass()),
              ConfigNodeInfo.getInstance().getLatestConfigNodes(),
              thriftClientProperty,
              clientManager));
    }

    @Override
    public boolean validateObject(
        ConfigRegionId configRegionId, PooledObject<ConfigNodeClient> pooledObject) {
      return Optional.ofNullable(pooledObject.getObject().getTransport())
          .map(TTransport::isOpen)
          .orElse(false);
    }
  }
}
