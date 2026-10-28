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

package org.apache.iotdb.confignode.manager.node;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TNodeResource;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.common.rpc.thrift.TStreamNodeConfiguration;
import org.apache.iotdb.common.rpc.thrift.TStreamNodeLocation;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.manager.ClusterManager;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.rpc.thrift.TStreamNodeRegisterReq;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class StreamNodeRegistrationTest {

  @Test
  public void testAcceptFirstStreamNodeRegistration() {
    ConfigManager configManager = mockConfigManager(Collections.emptyList());
    TSStatus status =
        ClusterNodeStartUtils.confirmStreamNodeRegistration(registerReq(1), configManager);
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
  }

  @Test
  public void testRejectSecondStreamNodeRegistration() {
    TStreamNodeConfiguration existing =
        new TStreamNodeConfiguration(
            new TStreamNodeLocation(1, new TEndPoint("127.0.0.1", 10820)),
            new TNodeResource(1, 1024L));
    ConfigManager configManager = mockConfigManager(Collections.singletonList(existing));
    TSStatus status =
        ClusterNodeStartUtils.confirmStreamNodeRegistration(registerReq(2), configManager);
    Assert.assertEquals(TSStatusCode.REGISTER_STREAM_NODE_ERROR.getStatusCode(), status.getCode());
  }

  private static ConfigManager mockConfigManager(
      java.util.List<TStreamNodeConfiguration> registered) {
    ConfigManager configManager = mock(ConfigManager.class);
    NodeManager nodeManager = mock(NodeManager.class);
    ClusterManager clusterManager = mock(ClusterManager.class);
    when(configManager.getNodeManager()).thenReturn(nodeManager);
    when(configManager.getClusterManager()).thenReturn(clusterManager);
    when(nodeManager.getRegisteredStreamNodes()).thenReturn(registered);
    when(clusterManager.getClusterIdWithRetry(anyLong())).thenReturn("cluster-id");
    return configManager;
  }

  private static TStreamNodeRegisterReq registerReq(int streamNodeId) {
    TStreamNodeRegisterReq req = new TStreamNodeRegisterReq();
    req.setClusterName(ConfigNodeDescriptor.getInstance().getConf().getClusterName());
    req.setStreamNodeConfiguration(
        new TStreamNodeConfiguration(
            new TStreamNodeLocation(streamNodeId, new TEndPoint("10.0.0." + streamNodeId, 10820)),
            new TNodeResource(1, 1024L)));
    return req;
  }
}
