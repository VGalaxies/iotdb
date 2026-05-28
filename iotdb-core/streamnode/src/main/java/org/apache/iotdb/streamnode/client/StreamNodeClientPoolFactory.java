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

package org.apache.iotdb.streamnode.client;

import org.apache.iotdb.commons.client.ClientManager;
import org.apache.iotdb.commons.client.ClientManagerMetrics;
import org.apache.iotdb.commons.client.IClientPoolFactory;
import org.apache.iotdb.commons.client.property.ClientPoolProperty;
import org.apache.iotdb.commons.client.property.ThriftClientProperty;
import org.apache.iotdb.commons.consensus.ConfigRegionId;
import org.apache.iotdb.streamnode.conf.StreamNodeConfig;
import org.apache.iotdb.streamnode.conf.StreamNodeDescriptor;

import org.apache.commons.pool2.impl.GenericKeyedObjectPool;

public class StreamNodeClientPoolFactory {

  private static final StreamNodeConfig CONF = StreamNodeDescriptor.getInstance().getConfig();

  private StreamNodeClientPoolFactory() {
    // Empty constructor
  }

  public static class ClientPoolFactory
      implements IClientPoolFactory<ConfigRegionId, StreamNodeClient> {

    @Override
    public GenericKeyedObjectPool<ConfigRegionId, StreamNodeClient> createClientPool(
        ClientManager<ConfigRegionId, StreamNodeClient> manager) {
      GenericKeyedObjectPool<ConfigRegionId, StreamNodeClient> clientPool =
          new GenericKeyedObjectPool<>(
              new StreamNodeClient.Factory(
                  manager,
                  new ThriftClientProperty.Builder()
                      .setConnectionTimeoutMs(CONF.getConnectionTimeoutInMS())
                      .setRpcThriftCompressionEnabled(CONF.isRpcThriftCompressionEnable())
                      .build()),
              new ClientPoolProperty.Builder<StreamNodeClient>()
                  .setMaxClientNumForEachNode(CONF.getMaxClientNumForEachNode())
                  .build()
                  .getConfig());
      ClientManagerMetrics.getInstance()
          .registerClientManager(this.getClass().getSimpleName(), clientPool);
      return clientPool;
    }
  }
}
