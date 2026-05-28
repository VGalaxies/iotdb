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

package org.apache.iotdb.confignode.client.async;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.client.ClientPoolFactory;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.async.AsyncStreamNodeInternalServiceClient;
import org.apache.iotdb.confignode.client.async.handlers.heartbeat.StreamNodeHeartbeatHandler;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.streamnode.rpc.thrift.TStreamNodeHeartbeatReq;

/** Asynchronously send heartbeat RPC requests to StreamNodes. */
public class AsyncStreamNodeHeartbeatClientPool {

  private final IClientManager<TEndPoint, AsyncStreamNodeInternalServiceClient> clientManager;

  private AsyncStreamNodeHeartbeatClientPool() {
    clientManager =
        new IClientManager.Factory<TEndPoint, AsyncStreamNodeInternalServiceClient>()
            .createClientManager(
                new ClientPoolFactory.AsyncStreamNodeHeartbeatServiceClientPoolFactory(
                    ConfigNodeDescriptor.getInstance().getConf().getSelectorNumOfClientManager()));
  }

  /**
   * Only used in LoadManager.
   *
   * @param endPoint The specific StreamNode
   */
  public void getStreamNodeHeartBeat(
      TEndPoint endPoint, TStreamNodeHeartbeatReq req, StreamNodeHeartbeatHandler handler) {
    try {
      clientManager.borrowClient(endPoint).getHeartbeat(req, handler);
    } catch (Exception ignore) {
      // Just ignore
    }
  }

  private static class AsyncStreamNodeHeartbeatClientPoolHolder {

    private static final AsyncStreamNodeHeartbeatClientPool INSTANCE =
        new AsyncStreamNodeHeartbeatClientPool();

    private AsyncStreamNodeHeartbeatClientPoolHolder() {
      // Empty constructor
    }
  }

  public static AsyncStreamNodeHeartbeatClientPool getInstance() {
    return AsyncStreamNodeHeartbeatClientPoolHolder.INSTANCE;
  }
}
