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

package org.apache.iotdb.confignode.manager.stream;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TNodeResource;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.common.rpc.thrift.TStreamNodeConfiguration;
import org.apache.iotdb.common.rpc.thrift.TStreamNodeLocation;
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.commons.stream.IoTDBTarget;
import org.apache.iotdb.commons.stream.PeriodWindow;
import org.apache.iotdb.commons.stream.StreamNodeTableTypeProvider;
import org.apache.iotdb.commons.stream.StreamProperties;
import org.apache.iotdb.commons.stream.StreamTask;
import org.apache.iotdb.commons.stream.StreamTaskStatus;
import org.apache.iotdb.commons.utils.StatusUtils;
import org.apache.iotdb.confignode.manager.IManager;
import org.apache.iotdb.confignode.manager.consensus.ConsensusManager;
import org.apache.iotdb.confignode.manager.node.NodeManager;
import org.apache.iotdb.confignode.persistence.stream.StreamInfo;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.streamnode.rpc.thrift.TDropTaskOnStreamNodeReq;
import org.apache.iotdb.streamnode.rpc.thrift.TStartTaskOnStreamNodeReq;
import org.apache.iotdb.streamnode.rpc.thrift.TStopTaskOnStreamNodeReq;
import org.apache.iotdb.streamnode.rpc.thrift.TStreamNodeHeartbeatReq;
import org.apache.iotdb.streamnode.rpc.thrift.TStreamNodeHeartbeatResp;
import org.apache.iotdb.streamnode.rpc.thrift.TTaskHeartbeat;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class StreamManagerTest {

  private IManager configManager;
  private StreamInfo streamInfo;
  private TestStreamManager streamManager;
  private List<TStreamNodeConfiguration> registeredStreamNodes;

  @Before
  public void setUp() {
    final ConsensusManager consensusManager = mock(ConsensusManager.class);
    final NodeManager nodeManager = mock(NodeManager.class);
    when(consensusManager.getLeaderTerm()).thenReturn(7L);
    registeredStreamNodes = new ArrayList<>();
    configManager = mock(IManager.class);
    when(configManager.getConsensusManager()).thenReturn(consensusManager);
    when(configManager.getNodeManager()).thenReturn(nodeManager);
    when(nodeManager.getRegisteredStreamNodes())
        .thenAnswer(invocation -> new ArrayList<>(registeredStreamNodes));
    streamInfo = new StreamInfo();
    streamManager = new TestStreamManager(configManager, streamInfo);
  }

  @After
  public void tearDown() {
    streamManager.close();
  }

  @Test
  public void testStartStreamAssignsRegisteredStreamNodeAndRecordsHeartbeat() {
    registerStreamNode(streamNode("127.0.0.1", 10820));
    final StreamTask task = buildTask("stream_0");
    streamInfo.addTask(task);
    Assert.assertEquals(StreamTaskStatus.CREATED, task.getStatus());

    final TSStatus status = streamManager.startStream("stream_0");

    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
    Assert.assertEquals(StreamTaskStatus.RUNNING, task.getStatus());
    Assert.assertEquals("127.0.0.1:10820", task.getRunningOn());
    Assert.assertEquals(1, task.getEpoch());
    Assert.assertEquals(7L, task.getLeaderTerm());
    Assert.assertTrue(task.getCnStartTime() > 0);
    Assert.assertEquals("127.0.0.1", streamManager.startEndpoints.get(0).getIp());
    Assert.assertEquals(10820, streamManager.startEndpoints.get(0).getPort());
    Assert.assertEquals(task.getEpoch(), streamManager.startRequests.get(0).getEpoch());

    final TSStatus staleHeartbeat =
        streamManager.recordHeartbeat(
            "stream_0", task.getEpoch(), task.getCnStartTime() - 1, 7L, task.getRunningOn());
    Assert.assertEquals(TSStatusCode.STREAM_STALE.getStatusCode(), staleHeartbeat.getCode());

    final TSStatus validHeartbeat =
        streamManager.recordHeartbeat(
            "stream_0", task.getEpoch(), task.getCnStartTime(), 7L, task.getRunningOn());
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), validHeartbeat.getCode());
    Assert.assertEquals(StreamTaskStatus.RUNNING, task.getStatus());
    Assert.assertTrue(task.getLastHeartbeatTime() > 0);
  }

  @Test
  public void testStopStreamSendsStopAndAdvancesRuntimeState() {
    registerStreamNode(streamNode("127.0.0.1", 10820));
    final StreamTask task = buildTask("stream_0");
    streamInfo.addTask(task);
    streamManager.startStream("stream_0");
    final int startedEpoch = task.getEpoch();

    final TSStatus status = streamManager.stopStream("stream_0");

    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
    Assert.assertEquals(StreamTaskStatus.STOPPED, task.getStatus());
    Assert.assertEquals(startedEpoch + 1, task.getEpoch());
    Assert.assertEquals("Manually Stopped", task.getLastDownReason());
    Assert.assertTrue(task.getLastDownTime() > 0);
    Assert.assertEquals("127.0.0.1", streamManager.stopEndpoints.get(0).getIp());
    Assert.assertEquals(10820, streamManager.stopEndpoints.get(0).getPort());
    Assert.assertEquals("stream_0", streamManager.stopRequests.get(0).getTaskName());
    Assert.assertEquals(task.getEpoch(), streamManager.stopRequests.get(0).getEpoch());
    Assert.assertEquals(task.getCnStartTime(), streamManager.stopRequests.get(0).getCnStartTime());
  }

  @Test
  public void testDropStreamStopsAndDropsRemoteTaskBeforeRemovingMetadata() {
    registerStreamNode(streamNode("127.0.0.1", 10820));
    final StreamTask task = buildTask("stream_0");
    streamInfo.addTask(task);
    streamManager.startStream("stream_0");
    final int startedEpoch = task.getEpoch();

    final TSStatus status = streamManager.dropStream("stream_0");

    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
    Assert.assertNull(streamInfo.getTask("stream_0"));
    Assert.assertEquals(1, streamManager.stopEndpoints.size());
    Assert.assertEquals(1, streamManager.dropEndpoints.size());
    Assert.assertEquals("127.0.0.1", streamManager.stopEndpoints.get(0).getIp());
    Assert.assertEquals(10820, streamManager.stopEndpoints.get(0).getPort());
    Assert.assertEquals("stream_0", streamManager.stopRequests.get(0).getTaskName());
    Assert.assertEquals(startedEpoch + 1, streamManager.stopRequests.get(0).getEpoch());
    Assert.assertEquals("127.0.0.1", streamManager.dropEndpoints.get(0).getIp());
    Assert.assertEquals(10820, streamManager.dropEndpoints.get(0).getPort());
    Assert.assertEquals("stream_0", streamManager.dropRequests.get(0).getTaskName());
  }

  @Test
  public void testCreatedStreamIsNotStartedByMonitor() {
    registerStreamNode(streamNode("127.0.0.1", 10820));
    final StreamTask task = buildTask("stream_0");
    streamInfo.addTask(task);

    streamManager.runStreamMonitorOnceForTest();

    Assert.assertEquals(StreamTaskStatus.CREATED, task.getStatus());
    Assert.assertEquals("", task.getRunningOn());
    Assert.assertTrue(streamManager.startRequests.isEmpty());
  }

  @Test
  public void testMonitorCollectsStreamNodeHeartbeat() {
    registerStreamNode(streamNode("127.0.0.1", 10820));
    final StreamTask task = buildTask("stream_0");
    streamInfo.addTask(task);
    streamManager.startStream("stream_0");
    task.setLastHeartbeatTime(1L);
    streamManager.heartbeatResponses.add(
        new TStreamNodeHeartbeatResp(System.nanoTime(), NodeStatus.Running.getStatus())
            .setRunningTasks(
                Collections.singletonList(new TTaskHeartbeat("stream_0", task.getEpoch()))));

    streamManager.runStreamMonitorOnceForTest();

    Assert.assertEquals(StreamTaskStatus.RUNNING, task.getStatus());
    Assert.assertEquals("127.0.0.1:10820", task.getRunningOn());
    Assert.assertTrue(task.getLastHeartbeatTime() > 1L);
    Assert.assertFalse(streamManager.heartbeatRequests.isEmpty());
  }

  @Test
  public void testStartStreamUsesRegisteredStreamNodesRoundRobin() {
    registerStreamNode(streamNode("127.0.0.1", 10820));
    registerStreamNode(streamNode("127.0.0.2", 10821));
    streamInfo.addTask(buildTask("stream_0"));
    streamInfo.addTask(buildTask("stream_1"));

    Assert.assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(),
        streamManager.startStream("stream_0").getCode());
    Assert.assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(),
        streamManager.startStream("stream_1").getCode());

    Assert.assertEquals("127.0.0.1", streamManager.startEndpoints.get(0).getIp());
    Assert.assertEquals(10820, streamManager.startEndpoints.get(0).getPort());
    Assert.assertEquals("127.0.0.2", streamManager.startEndpoints.get(1).getIp());
    Assert.assertEquals(10821, streamManager.startEndpoints.get(1).getPort());
  }

  private StreamTask buildTask(final String taskName) {
    final StreamTask task = new StreamTask();
    task.setTaskName(taskName);
    task.setCreator("creator");
    task.setCreationTime(1L);
    task.setWindow(new PeriodWindow(1L, 0L));
    task.setTypeProvider(new StreamNodeTableTypeProvider(Collections.emptyMap()));
    task.setTarget(new IoTDBTarget("db", "target", Collections.singletonList("value")));
    task.setProperties(
        new StreamProperties(-1L, -1L, false, null, -1L, StreamProperties.EventType.WINDOW_CLOSE));
    return task;
  }

  private TStreamNodeConfiguration streamNode(final String ip, final int port) {
    return new TStreamNodeConfiguration(
        new TStreamNodeLocation(-1, new TEndPoint(ip, port)), new TNodeResource(1, 1024L));
  }

  private void registerStreamNode(final TStreamNodeConfiguration streamNodeConfiguration) {
    if (streamNodeConfiguration.getLocation().getStreamNodeId() < 0) {
      streamNodeConfiguration.getLocation().setStreamNodeId(registeredStreamNodes.size());
    }
    registeredStreamNodes.add(streamNodeConfiguration);
  }

  private static class TestStreamManager extends StreamManager {

    private final List<TEndPoint> startEndpoints = new ArrayList<>();
    private final List<TStartTaskOnStreamNodeReq> startRequests = new ArrayList<>();
    private final List<TEndPoint> stopEndpoints = new ArrayList<>();
    private final List<TStopTaskOnStreamNodeReq> stopRequests = new ArrayList<>();
    private final List<TEndPoint> dropEndpoints = new ArrayList<>();
    private final List<TDropTaskOnStreamNodeReq> dropRequests = new ArrayList<>();
    private final List<TStreamNodeHeartbeatReq> heartbeatRequests = new ArrayList<>();
    private final List<TStreamNodeHeartbeatResp> heartbeatResponses = new ArrayList<>();

    private TestStreamManager(final IManager configManager, final StreamInfo streamInfo) {
      super(configManager, streamInfo);
    }

    @Override
    protected TSStatus sendStartTask(
        final TEndPoint endPoint, final TStartTaskOnStreamNodeReq request) {
      startEndpoints.add(endPoint);
      startRequests.add(request);
      return StatusUtils.OK;
    }

    @Override
    protected TSStatus sendStopTask(
        final TEndPoint endPoint, final TStopTaskOnStreamNodeReq request) {
      stopEndpoints.add(endPoint);
      stopRequests.add(request);
      return StatusUtils.OK;
    }

    @Override
    protected TSStatus sendDropTask(
        final TEndPoint endPoint, final TDropTaskOnStreamNodeReq request) {
      dropEndpoints.add(endPoint);
      dropRequests.add(request);
      return StatusUtils.OK;
    }

    @Override
    protected TStreamNodeHeartbeatResp sendHeartbeat(
        final TEndPoint endPoint, final TStreamNodeHeartbeatReq request) {
      heartbeatRequests.add(request);
      return heartbeatResponses.isEmpty()
          ? new TStreamNodeHeartbeatResp(
                  request.getHeartbeatTimestamp(), NodeStatus.Running.getStatus())
              .setRunningTasks(Collections.emptyList())
          : heartbeatResponses.remove(0);
    }
  }
}
