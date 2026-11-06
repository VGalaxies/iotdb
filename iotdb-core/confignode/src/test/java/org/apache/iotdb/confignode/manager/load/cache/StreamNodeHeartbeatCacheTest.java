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

package org.apache.iotdb.confignode.manager.load.cache;

import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.confignode.manager.load.cache.node.NodeHeartbeatSample;
import org.apache.iotdb.confignode.manager.load.cache.node.StreamNodeHeartbeatCache;
import org.apache.iotdb.streamnode.rpc.thrift.TStreamNodeHeartbeatResp;

import org.junit.Assert;
import org.junit.Test;

public class StreamNodeHeartbeatCacheTest {

  @Test
  public void testNewerStoppingHeartbeatCannotReviveStoppedStreamNode() {
    StreamNodeHeartbeatCache cache = new StreamNodeHeartbeatCache(4);
    cache.trySetNodeStatus(NodeStatus.Stopped, null, false);

    // A heartbeat answered by the stopping StreamNode after it reported the shutdown.
    NodeHeartbeatSample stoppingSample =
        newerHeartbeat(cache, NodeStatus.ReadOnly, NodeStatus.STOPPING);
    cache.cacheHeartbeatSample(stoppingSample);
    // Exercise the transition rule, not the filter for heartbeats older than the shutdown report.
    Assert.assertSame(stoppingSample, cache.getLastSample());
    for (int i = 0; i < 2; i++) {
      cache.updateNodeStatistics();
      Assert.assertEquals(NodeStatus.Stopped, cache.getNodeStatus());
      Assert.assertNull(cache.getNodeStatusReason());
      Assert.assertEquals(Long.MAX_VALUE, cache.getLoadScore());
    }

    // A restarted StreamNode reports Running and is revived.
    cache.cacheHeartbeatSample(newerHeartbeat(cache, NodeStatus.Running, null));
    cache.updateNodeStatistics();
    Assert.assertEquals(NodeStatus.Running, cache.getNodeStatus());
    Assert.assertEquals(0, cache.getLoadScore());
  }

  private static NodeHeartbeatSample newerHeartbeat(
      StreamNodeHeartbeatCache cache, NodeStatus status, String statusReason) {
    return new NodeHeartbeatSample(
        new TStreamNodeHeartbeatResp()
            .setHeartbeatTimestamp(cache.getLastSample().getSampleLogicalTimestamp() + 1)
            .setStatus(status.getStatus())
            .setStatusReason(statusReason));
  }
}
