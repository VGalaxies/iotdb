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

package org.apache.iotdb.commons.client.sync;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.client.ClientManager;
import org.apache.iotdb.commons.client.ThriftClient;
import org.apache.iotdb.commons.client.factory.ThriftClientFactory;
import org.apache.iotdb.commons.client.property.ThriftClientProperty;
import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.rpc.TConfigurationConst;
import org.apache.iotdb.rpc.TimeoutChangeableTransport;
import org.apache.iotdb.streamnode.rpc.thrift.IStreamNodeRPCService;

import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;

public class SyncStreamNodeInternalServiceClient extends IStreamNodeRPCService.Client
    implements ThriftClient, AutoCloseable {

  private final boolean printLogWhenEncounterException;
  private final TEndPoint endpoint;
  private final ClientManager<TEndPoint, SyncStreamNodeInternalServiceClient> clientManager;
  private static final CommonConfig commonConfig = CommonDescriptor.getInstance().getConfig();

  public SyncStreamNodeInternalServiceClient(
      ThriftClientProperty property,
      TEndPoint endpoint,
      ClientManager<TEndPoint, SyncStreamNodeInternalServiceClient> clientManager)
      throws TTransportException {
    super(
        property
            .getProtocolFactory()
            .getProtocol(
                new TSocket(
                    TConfigurationConst.defaultTConfiguration,
                    endpoint.getIp(),
                    endpoint.getPort(),
                    property.getConnectionTimeoutMs())));
    this.printLogWhenEncounterException = property.isPrintLogWhenEncounterException();
    this.endpoint = endpoint;
    this.clientManager = clientManager;
  }

  @Override
  public void close() throws Exception {
    getInputProtocol().getTransport().close();
  }

  @Override
  public void invalidate() {
    if (clientManager != null) {
      clientManager.returnClient(endpoint, this);
    }
  }

  @Override
  public void invalidateAll() {
    if (clientManager != null) {
      clientManager.clear(endpoint);
    }
  }

  @Override
  public boolean printLogWhenEncounterException() {
    return printLogWhenEncounterException;
  }

  @TestOnly
  public TEndPoint getTEndPoint() {
    return endpoint;
  }

  public static class Factory extends

      ThriftClientFactory<TEndPoint, SyncStreamNodeInternalServiceClient> {

    public Factory(
        ClientManager<TEndPoint, SyncStreamNodeInternalServiceClient> clientManager,
        ThriftClientProperty property) {
      super(clientManager, property);
    }

    @Override
    public void destroyObject(
        TEndPoint key, PooledObject<SyncStreamNodeInternalServiceClient> pooledObject)
        throws Exception {
      pooledObject.getObject().close();
    }

    @Override
    public PooledObject<SyncStreamNodeInternalServiceClient> makeObject(TEndPoint endPoint)
        throws Exception {
      return new DefaultPooledObject<>(
          new SyncStreamNodeInternalServiceClient(thriftClientProperty, endPoint, clientManager));
    }

    @Override
    public boolean validateObject(
        TEndPoint key, PooledObject<SyncStreamNodeInternalServiceClient> pooledObject) {
      return pooledObject.getObject().getInputProtocol().getTransport().isOpen();
    }
  }
}
