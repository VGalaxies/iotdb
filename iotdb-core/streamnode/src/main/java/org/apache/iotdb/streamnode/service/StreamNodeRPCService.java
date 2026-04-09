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

package org.apache.iotdb.streamnode.service;

import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.commons.service.ServiceType;
import org.apache.iotdb.commons.service.ThriftService;
import org.apache.iotdb.commons.service.ThriftServiceThread;
import org.apache.iotdb.rpc.DeepCopyRpcTransportFactory;
import org.apache.iotdb.streamnode.conf.StreamNodeConfig;
import org.apache.iotdb.streamnode.conf.StreamNodeDescriptor;
import org.apache.iotdb.streamnode.rpc.thrift.IStreamNodeRPCService;

public class StreamNodeRPCService extends ThriftService implements StreamNodeRPCServiceMBean {

  private final StreamNodeConfig config = StreamNodeDescriptor.getInstance().getConfig();

  private StreamNodeRPCServiceProcessor processor;

  @Override
  public ServiceType getID() {
    return ServiceType.STREAM_NODE_RPC_SERVICE;
  }

  @Override
  public void initTProcessor() {
    processor = new StreamNodeRPCServiceProcessor();
    super.initSyncedServiceImpl(null);
    super.processor = new IStreamNodeRPCService.Processor<>(processor);
  }

  @Override
  public void initThriftServiceThread() throws IllegalAccessException {
    try {
      thriftServiceThread =
          new ThriftServiceThread(
              super.processor,
              getID().getName(),
              ThreadName.STREAM_NODE_RPC_PROCESSOR.getName(),
              getBindIP(),
              getBindPort(),
              config.getRpcMaxConcurrentClientNum(),
              config.getThriftServerAwaitTimeForStopService(),
              new StreamNodeRPCServiceHandler(),
              config.isRpcThriftCompressionEnable(),
              DeepCopyRpcTransportFactory.INSTANCE);
      thriftServiceThread.setName(ThreadName.STREAM_NODE_RPC_SERVICE.getName());
    } catch (Exception e) {
      throw new IllegalAccessException(
          "Failed to init StreamNode RPC service thread: " + e.getMessage());
    }
  }

  @Override
  public String getBindIP() {
    return config.getSnInternalAddress();
  }

  @Override
  public int getBindPort() {
    return config.getSnInternalPort();
  }

  private static class StreamNodeRPCServiceHolder {
    private static final StreamNodeRPCService INSTANCE = new StreamNodeRPCService();
  }

  public static StreamNodeRPCService getInstance() {
    return StreamNodeRPCServiceHolder.INSTANCE;
  }
}
