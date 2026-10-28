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
import org.apache.iotdb.commons.stream.IoTDBSubscriptionSource;
import org.apache.iotdb.commons.stream.IoTDBTarget;
import org.apache.iotdb.commons.stream.PeriodWindow;
import org.apache.iotdb.commons.stream.StreamNodeTableTypeProvider;
import org.apache.iotdb.commons.stream.StreamProperties;
import org.apache.iotdb.commons.stream.StreamTask;
import org.apache.iotdb.commons.stream.StreamTaskExpectedStatus;
import org.apache.iotdb.commons.stream.StreamTaskStatus;
import org.apache.iotdb.commons.subscription.meta.topic.TopicMeta;
import org.apache.iotdb.commons.utils.StatusUtils;
import org.apache.iotdb.confignode.consensus.request.write.stream.StartStreamPlan;
import org.apache.iotdb.confignode.consensus.request.write.stream.StopStreamPlan;
import org.apache.iotdb.confignode.consensus.request.write.stream.UpdateStreamEpochPlan;
import org.apache.iotdb.confignode.i18n.ManagerMessages;
import org.apache.iotdb.confignode.manager.IManager;
import org.apache.iotdb.confignode.manager.ProcedureManager;
import org.apache.iotdb.confignode.manager.consensus.ConsensusManager;
import org.apache.iotdb.confignode.manager.load.LoadManager;
import org.apache.iotdb.confignode.manager.load.cache.node.NodeStatistics;
import org.apache.iotdb.confignode.manager.load.subscriber.NodeStatisticsChangeEvent;
import org.apache.iotdb.confignode.manager.node.NodeManager;
import org.apache.iotdb.confignode.manager.subscription.SubscriptionCoordinator;
import org.apache.iotdb.confignode.manager.subscription.SubscriptionManager;
import org.apache.iotdb.confignode.persistence.stream.StreamInfo;
import org.apache.iotdb.confignode.persistence.subscription.SubscriptionInfo;
import org.apache.iotdb.confignode.rpc.thrift.TAlterTopicReq;
import org.apache.iotdb.confignode.rpc.thrift.TCreateTopicReq;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.rpc.subscription.config.TopicConstant;
import org.apache.iotdb.streamnode.rpc.thrift.TDropTaskOnStreamNodeReq;
import org.apache.iotdb.streamnode.rpc.thrift.TStartTaskOnStreamNodeReq;
import org.apache.iotdb.streamnode.rpc.thrift.TStopTaskOnStreamNodeReq;
import org.apache.iotdb.streamnode.rpc.thrift.TStreamNodeHeartbeatReq;
import org.apache.iotdb.streamnode.rpc.thrift.TStreamNodeHeartbeatResp;
import org.apache.iotdb.streamnode.rpc.thrift.TTaskHeartbeat;

import org.apache.tsfile.utils.Pair;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class StreamManagerTest {

  private NodeManager nodeManager;
  private StreamInfo streamInfo;
  private TestStreamManager streamManager;
  private List<TStreamNodeConfiguration> registeredStreamNodes;
  private Set<String> registeredRunningOnMetrics;
  private Set<Integer> removingStreamNodeIds;
  private Set<String> streamNamesUnderOperation;
  private IManager configManager;
  private LoadManager loadManager;
  private SubscriptionInfo subscriptionInfo;
  private ConsensusManager consensusManager;
  private List<String> operationOrder;

  @Before
  public void setUp() throws Exception {
    consensusManager = mock(ConsensusManager.class);
    nodeManager = mock(NodeManager.class);
    final ProcedureManager procedureManager = mock(ProcedureManager.class);
    when(consensusManager.getLeaderTerm()).thenReturn(7L);
    when(consensusManager.isLeaderReady()).thenReturn(true);
    registeredStreamNodes = new ArrayList<>();
    registeredRunningOnMetrics = ConcurrentHashMap.newKeySet();
    removingStreamNodeIds = ConcurrentHashMap.newKeySet();
    streamNamesUnderOperation = ConcurrentHashMap.newKeySet();
    operationOrder = Collections.synchronizedList(new ArrayList<>());
    configManager = mock(IManager.class);
    final SubscriptionManager subscriptionManager = mock(SubscriptionManager.class);
    final SubscriptionCoordinator subscriptionCoordinator = mock(SubscriptionCoordinator.class);
    subscriptionInfo = mock(SubscriptionInfo.class);
    when(configManager.getSubscriptionManager()).thenReturn(subscriptionManager);
    when(subscriptionManager.getSubscriptionCoordinator()).thenReturn(subscriptionCoordinator);
    when(subscriptionCoordinator.getSubscriptionInfo()).thenReturn(subscriptionInfo);
    when(configManager.getConsensusManager()).thenReturn(consensusManager);
    when(configManager.getNodeManager()).thenReturn(nodeManager);
    when(configManager.getProcedureManager()).thenReturn(procedureManager);
    loadManager = mock(LoadManager.class);
    when(configManager.getLoadManager()).thenReturn(loadManager);
    when(loadManager.getNodeStatus(anyInt())).thenReturn(NodeStatus.Running);
    when(nodeManager.getRegisteredStreamNodes())
        .thenAnswer(invocation -> new ArrayList<>(registeredStreamNodes));
    when(nodeManager.filterStreamNodeThroughStatus(NodeStatus.Running))
        .thenAnswer(invocation -> new ArrayList<>(registeredStreamNodes));
    when(procedureManager.getRemovingStreamNodeIds())
        .thenAnswer(invocation -> new HashSet<>(removingStreamNodeIds));
    when(procedureManager.getInProgressStreamNames())
        .thenAnswer(invocation -> new HashSet<>(streamNamesUnderOperation));
    streamInfo = new StreamInfo();
    when(consensusManager.write(any(StartStreamPlan.class)))
        .thenAnswer(invocation -> streamInfo.applyStartStream(invocation.getArgument(0)));
    when(consensusManager.write(any(StopStreamPlan.class)))
        .thenAnswer(invocation -> streamInfo.applyStopStream(invocation.getArgument(0)));
    when(consensusManager.write(any(UpdateStreamEpochPlan.class)))
        .thenAnswer(
            invocation -> {
              final UpdateStreamEpochPlan plan = invocation.getArgument(0);
              operationOrder.add("epoch:" + plan.getEpoch());
              return streamInfo.applyUpdateStreamEpoch(plan);
            });
    streamManager =
        new TestStreamManager(
            configManager, streamInfo, registeredRunningOnMetrics::add, operationOrder);
    // Tests invoke the monitor explicitly. Wait for the scheduled monitor to exit before any
    // test changes mock stubs or task state; even doReturn stubbing races with background calls.
    streamManager.stopStreamMonitor();
    final Field executorField = StreamManager.class.getDeclaredField("streamMonitorExecutor");
    executorField.setAccessible(true);
    final ExecutorService monitorExecutor = (ExecutorService) executorField.get(streamManager);
    monitorExecutor.shutdownNow();
    Assert.assertTrue(monitorExecutor.awaitTermination(10, TimeUnit.SECONDS));
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

    final TSStatus status = startStream("stream_0");

    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
    Assert.assertEquals(StreamTaskStatus.RUNNING, task.getStatus());
    Assert.assertEquals(
        StreamTaskExpectedStatus.RUNNING, streamInfo.getExpectedStatus(task.getTaskName()));
    Assert.assertEquals("127.0.0.1:10820", task.getRunningOn());
    Assert.assertEquals(1, task.getEpoch());
    Assert.assertEquals(7L, task.getLeaderTerm());
    Assert.assertTrue(task.getCnStartTime() > 0);
    Assert.assertEquals("127.0.0.1", streamManager.startEndpoints.get(0).getIp());
    Assert.assertEquals(10820, streamManager.startEndpoints.get(0).getPort());
    Assert.assertEquals(task.getEpoch(), streamManager.startRequests.get(0).getEpoch());
    Assert.assertEquals("epoch:1", operationOrder.get(0));
    Assert.assertEquals("start:1", operationOrder.get(1));

    final TSStatus heartbeatFromEarlierConfigNode =
        streamManager.recordHeartbeat(
            "stream_0", task.getEpoch(), task.getCnStartTime() - 1, 7L, task.getRunningOn());
    Assert.assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(), heartbeatFromEarlierConfigNode.getCode());

    final TSStatus validHeartbeat =
        streamManager.recordHeartbeat(
            "stream_0", task.getEpoch() + 2, task.getCnStartTime(), 7L, task.getRunningOn());
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), validHeartbeat.getCode());
    Assert.assertEquals(StreamTaskStatus.RUNNING, task.getStatus());
    Assert.assertEquals(3, task.getEpoch());
    Assert.assertTrue(task.getLastHeartbeatTime() > 0);
    Assert.assertEquals("epoch:3", operationOrder.get(2));
  }

  /** Verifies that start and heartbeat paths expose newly observed RunningOn metric labels. */
  @Test
  public void testRegisterRunningOnMetrics() {
    registerStreamNode(streamNode("127.0.0.1", 10820));
    final StreamTask task = buildTask("stream_0");
    streamInfo.addTask(task);

    Assert.assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(), startStream("stream_0").getCode());
    Assert.assertTrue(registeredRunningOnMetrics.contains("127.0.0.1:10820"));

    Assert.assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(),
        streamManager
            .recordHeartbeat(
                "stream_0",
                task.getEpoch(),
                task.getCnStartTime(),
                task.getLeaderTerm(),
                "127.0.0.2:10821")
            .getCode());
    Assert.assertTrue(registeredRunningOnMetrics.contains("127.0.0.2:10821"));
  }

  @Test
  public void testStopStreamSendsStopAndAdvancesRuntimeState() {
    registerStreamNode(streamNode("127.0.0.1", 10820));
    final StreamTask task = buildTask("stream_0");
    streamInfo.addTask(task);
    startStream("stream_0");
    operationOrder.clear();
    final int startedEpoch = task.getEpoch();
    final int targetEpoch = startedEpoch + 1;

    final TSStatus status = streamManager.stopStreamTask("stream_0", targetEpoch);
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
    final TSStatus metadataStatus = streamManager.persistStopStream(task, targetEpoch);

    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), metadataStatus.getCode());
    Assert.assertEquals(StreamTaskStatus.STOPPED, task.getStatus());
    Assert.assertEquals(
        StreamTaskExpectedStatus.STOPPED, streamInfo.getExpectedStatus(task.getTaskName()));
    Assert.assertEquals(startedEpoch + 1, task.getEpoch());
    Assert.assertEquals(ManagerMessages.STREAM_MANAGER_MANUALLY_STOPPED, task.getLastDownReason());
    Assert.assertTrue(task.getLastDownTime() > 0);
    Assert.assertEquals("127.0.0.1", streamManager.stopEndpoints.get(0).getIp());
    Assert.assertEquals(10820, streamManager.stopEndpoints.get(0).getPort());
    Assert.assertEquals("stream_0", streamManager.stopRequests.get(0).getTaskName());
    Assert.assertEquals(task.getEpoch(), streamManager.stopRequests.get(0).getEpoch());
    Assert.assertEquals(task.getCnStartTime(), streamManager.stopRequests.get(0).getCnStartTime());
    Assert.assertEquals("epoch:" + targetEpoch, operationOrder.get(0));
    Assert.assertEquals("stop:" + targetEpoch, operationOrder.get(1));
  }

  @Test
  public void testStopStreamDiscoversAndBroadcastsWhenRunningOnIsUnknown() {
    registerStreamNode(streamNode("127.0.0.1", 10820));
    registerStreamNode(streamNode("127.0.0.2", 10821));
    final StreamTask task = buildTask("stream_0");
    task.setStatus(StreamTaskStatus.UNKNOWN);
    task.setEpoch(3);
    task.setLeaderTerm(7L);
    streamInfo.addTask(task);
    task.setStatus(StreamTaskStatus.UNKNOWN);
    streamNamesUnderOperation.add("stream_0");
    streamManager.heartbeatResponses.add(
        new TStreamNodeHeartbeatResp(System.nanoTime(), NodeStatus.Running.getStatus())
            .setRunningTasks(Collections.singletonList(new TTaskHeartbeat("stream_0", 5))));

    final TSStatus status = streamManager.stopStreamTask("stream_0", 4);

    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
    Assert.assertEquals(StreamTaskStatus.STOPPED, task.getStatus());
    Assert.assertEquals(6, task.getEpoch());
    Assert.assertEquals(2, streamManager.stopRequests.size());
    Assert.assertEquals(6, streamManager.stopRequests.get(0).getEpoch());
    Assert.assertEquals(6, streamManager.stopRequests.get(1).getEpoch());
  }

  @Test
  public void testStopStreamSucceedsWithoutRegisteredStreamNodes() {
    final StreamTask task = buildTask("stream_0");
    streamInfo.addTask(task);

    final TSStatus status = streamManager.stopStreamTask("stream_0", 1);

    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
    Assert.assertEquals(StreamTaskStatus.STOPPED, task.getStatus());
    Assert.assertTrue(streamManager.heartbeatRequests.isEmpty());
    Assert.assertTrue(streamManager.stopRequests.isEmpty());
  }

  @Test
  public void testStopStreamIgnoresUnreachableStreamNodeDuringFallback() {
    registerStreamNode(streamNode("127.0.0.1", 10820));
    registerStreamNode(streamNode("127.0.0.2", 10821));
    final StreamTask task = buildTask("stream_0");
    task.setStatus(StreamTaskStatus.UNKNOWN);
    streamInfo.addTask(task);
    task.setStatus(StreamTaskStatus.UNKNOWN);
    streamManager.heartbeatResponses.add(null);
    streamManager.heartbeatResponses.add(
        new TStreamNodeHeartbeatResp(System.nanoTime(), NodeStatus.Running.getStatus())
            .setRunningTasks(Collections.emptyList()));
    streamManager.enqueueStopStatus(
        new TSStatus(TSStatusCode.INTERNAL_REQUEST_RETRY_ERROR.getStatusCode()));

    final TSStatus status = streamManager.stopStreamTask("stream_0", 1);

    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
    Assert.assertEquals(StreamTaskStatus.STOPPED, task.getStatus());
    Assert.assertEquals(List.of(1, 1), streamManager.heartbeatRetryNums);
    Assert.assertEquals(List.of(1, 1), streamManager.stopRetryNums);
  }

  @Test
  public void testStopStreamBroadcastsConcurrently() throws Exception {
    registerStreamNode(streamNode("127.0.0.1", 10820));
    registerStreamNode(streamNode("127.0.0.2", 10821));
    final StreamTask task = buildTask("stream_0");
    streamInfo.addTask(task);
    task.setStatus(StreamTaskStatus.UNKNOWN);
    streamManager.blockBroadcastStops(2);

    final CompletableFuture<TSStatus> stopFuture =
        CompletableFuture.supplyAsync(() -> streamManager.stopStreamTask("stream_0", 1));
    Assert.assertTrue(streamManager.concurrentStopEntered.await(5, TimeUnit.SECONDS));
    streamManager.releaseConcurrentStops.countDown();
    final TSStatus status = stopFuture.get(5, TimeUnit.SECONDS);

    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
    Assert.assertEquals(StreamTaskStatus.STOPPED, task.getStatus());
    Assert.assertEquals(2, streamManager.stopRequests.size());
  }

  @Test
  public void testStopStreamRediscoversAndBroadcastsAfterStaleResponse() {
    registerStreamNode(streamNode("127.0.0.1", 10820));
    final StreamTask task = buildTask("stream_0");
    streamInfo.addTask(task);
    startStream("stream_0");
    streamManager.enqueueStopStatus(new TSStatus(TSStatusCode.STREAM_STALE.getStatusCode()));
    streamManager.heartbeatResponses.add(
        new TStreamNodeHeartbeatResp(System.nanoTime(), NodeStatus.Running.getStatus())
            .setRunningTasks(Collections.singletonList(new TTaskHeartbeat("stream_0", 5))));

    final TSStatus status = streamManager.stopStreamTask("stream_0", task.getEpoch() + 1);

    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
    Assert.assertEquals(StreamTaskStatus.STOPPED, task.getStatus());
    Assert.assertEquals(6, task.getEpoch());
    Assert.assertEquals(2, streamManager.stopRequests.size());
    Assert.assertEquals(2, streamManager.stopRequests.get(0).getEpoch());
    Assert.assertEquals(6, streamManager.stopRequests.get(1).getEpoch());
  }

  @Test
  public void testStartStreamDiscoversExistingRuntimeBeforeDispatch() {
    registerStreamNode(streamNode("127.0.0.1", 10820));
    final StreamTask task = buildTask("stream_0");
    streamInfo.addTask(task);
    task.setEpoch(3);
    streamNamesUnderOperation.add("stream_0");
    streamManager.heartbeatResponses.add(
        new TStreamNodeHeartbeatResp(System.nanoTime(), NodeStatus.Running.getStatus())
            .setRunningTasks(Collections.singletonList(new TTaskHeartbeat("stream_0", 5))));

    final TSStatus status = streamManager.startStreamTask("stream_0");

    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
    Assert.assertEquals(StreamTaskStatus.RUNNING, task.getStatus());
    Assert.assertEquals(5, task.getEpoch());
    Assert.assertEquals("127.0.0.1:10820", task.getRunningOn());
    Assert.assertTrue(streamManager.startRequests.isEmpty());
  }

  @Test
  public void testDropStreamTaskStopsAndDropsRemoteTask() {
    registerStreamNode(streamNode("127.0.0.1", 10820));
    final StreamTask task = buildTask("stream_0");
    streamInfo.addTask(task);
    startStream("stream_0");
    final int startedEpoch = task.getEpoch();

    final TSStatus status = streamManager.dropStreamTask("stream_0");

    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
    Assert.assertSame(task, streamInfo.getTask("stream_0"));
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

  /** Verifies drop is rejected when the owning StreamNode is not Running. */
  @Test
  public void testDropStreamTaskRejectedWhenStreamNodeNotRunning() {
    registerStreamNode(streamNode("127.0.0.1", 10820));
    final StreamTask task = buildTask("stream_0");
    streamInfo.addTask(task);
    startStream("stream_0");
    when(loadManager.getNodeStatus(0)).thenReturn(NodeStatus.Stopped);

    final TSStatus status = streamManager.dropStreamTask("stream_0");

    Assert.assertEquals(TSStatusCode.STREAM_NODE_STOP_ERROR.getStatusCode(), status.getCode());
    Assert.assertNotNull(streamInfo.getTask("stream_0"));
    Assert.assertTrue(streamManager.stopEndpoints.isEmpty());
    Assert.assertTrue(streamManager.dropEndpoints.isEmpty());
  }

  @Test
  public void testCreatedStreamIsNotStartedByMonitor() {
    registerStreamNode(streamNode("127.0.0.1", 10820));
    final StreamTask task = buildTask("stream_0");
    streamInfo.addTask(task);

    streamManager.runStreamMonitorOnceForTest();

    Assert.assertEquals(StreamTaskStatus.CREATED, task.getStatus());
    Assert.assertEquals(
        StreamTaskExpectedStatus.CREATED, streamInfo.getExpectedStatus(task.getTaskName()));
    Assert.assertEquals("", task.getRunningOn());
    Assert.assertTrue(streamManager.startRequests.isEmpty());
    Assert.assertTrue(streamManager.dropRequests.isEmpty());
  }

  @Test
  public void testMonitorKeepsStoppedRuntimeForCreatedStream() {
    registerStreamNode(streamNode("127.0.0.1", 10820));
    final StreamTask task = buildTask("stream_0");
    streamInfo.addTask(task);
    Assert.assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(),
        streamManager.startStreamTask("stream_0").getCode());
    Assert.assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(),
        streamManager.rollbackStartStreamTask(task, task.getEpoch() + 1).getCode());

    streamManager.runStreamMonitorOnceForTest();

    Assert.assertEquals(StreamTaskExpectedStatus.CREATED, streamInfo.getExpectedStatus("stream_0"));
    Assert.assertEquals(StreamTaskStatus.STOPPED, task.getStatus());
    Assert.assertEquals(ManagerMessages.MESSAGE_START_ROLLBACK_B6601816, task.getLastDownReason());
    Assert.assertEquals("127.0.0.1:10820", task.getRunningOn());
    Assert.assertTrue(streamManager.dropRequests.isEmpty());
  }

  @Test
  public void testMonitorRestartsStreamWithRunningExpectedStatus() {
    registerStreamNode(streamNode("127.0.0.1", 10820));
    final List<StreamTask> tasks = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      final StreamTask task = buildTask("stream_" + i);
      tasks.add(task);
      streamInfo.addTask(task);
      streamInfo.applyStartStream(new StartStreamPlan(task.getTaskName(), 1));
    }

    streamManager.runStreamMonitorOnceForTest();

    for (StreamTask task : tasks) {
      Assert.assertEquals(
          StreamTaskExpectedStatus.RUNNING, streamInfo.getExpectedStatus(task.getTaskName()));
      Assert.assertEquals(StreamTaskStatus.RUNNING, task.getStatus());
      Assert.assertEquals("127.0.0.1:10820", task.getRunningOn());
    }
    Assert.assertEquals(5, streamManager.startRequests.size());
    Assert.assertEquals(1, streamManager.heartbeatRequests.size());
  }

  @Test
  public void testMonitorStopsRuntimeWithStoppedExpectedStatus() {
    registerStreamNode(streamNode("127.0.0.1", 10820));
    final StreamTask task = buildTask("stream_0");
    streamInfo.addTask(task);
    startStream("stream_0");
    final int persistedStopEpoch = task.getEpoch() + 1;
    streamInfo.applyStopStream(new StopStreamPlan("stream_0", persistedStopEpoch));
    operationOrder.clear();

    streamManager.runStreamMonitorOnceForTest();

    Assert.assertEquals(StreamTaskExpectedStatus.STOPPED, streamInfo.getExpectedStatus("stream_0"));
    Assert.assertEquals(StreamTaskStatus.STOPPED, task.getStatus());
    Assert.assertEquals(
        ManagerMessages.MESSAGE_STATE_RECONCILIATION_4D36E98B, task.getLastDownReason());
    Assert.assertEquals(persistedStopEpoch, task.getEpoch());
    Assert.assertEquals(1, streamManager.stopRequests.size());
    Assert.assertEquals(persistedStopEpoch, streamManager.stopRequests.get(0).getEpoch());
    Assert.assertEquals(Collections.singletonList("stop:" + persistedStopEpoch), operationOrder);
  }

  @Test
  public void testStartDoesNotDispatchWhenEpochPersistenceFails() throws Exception {
    registerStreamNode(streamNode("127.0.0.1", 10820));
    final StreamTask task = buildTask("stream_0");
    streamInfo.addTask(task);
    final TSStatus failure =
        new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode())
            .setMessage("epoch persistence failed");
    doReturn(failure).when(consensusManager).write(any(UpdateStreamEpochPlan.class));

    final TSStatus status = streamManager.startStreamTask("stream_0");

    Assert.assertSame(failure, status);
    Assert.assertTrue(streamManager.startRequests.isEmpty());
    Assert.assertEquals(0, task.getEpoch());
  }

  @Test
  public void testStartRejectsEpochOverflowBeforeDispatch() {
    registerStreamNode(streamNode("127.0.0.1", 10820));
    final StreamTask task = buildTask("stream_0");
    task.setEpoch(Integer.MAX_VALUE);
    streamInfo.addTask(task);

    final TSStatus status = streamManager.startStreamTask("stream_0");

    Assert.assertEquals(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode(), status.getCode());
    Assert.assertTrue(streamManager.startRequests.isEmpty());
    Assert.assertEquals(Integer.MAX_VALUE, task.getEpoch());
  }

  @Test
  public void testLateHeartbeatReconcilesStoppedExpectedStatus() {
    registerStreamNode(streamNode("127.0.0.1", 10820));
    final StreamTask task = buildTask("stream_0");
    streamInfo.addTask(task);
    startStream("stream_0");
    streamInfo.applyStopStream(new StopStreamPlan("stream_0", task.getEpoch() + 1));
    task.setStatus(StreamTaskStatus.STOPPED);

    Assert.assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(),
        streamManager
            .recordHeartbeat(
                "stream_0",
                task.getEpoch(),
                task.getCnStartTime(),
                task.getLeaderTerm(),
                "127.0.0.1:10820")
            .getCode());
    Assert.assertEquals(StreamTaskStatus.UNKNOWN, task.getStatus());

    streamManager.runStreamMonitorOnceForTest();

    Assert.assertEquals(StreamTaskStatus.STOPPED, task.getStatus());
    Assert.assertEquals(1, streamManager.stopRequests.size());
  }

  @Test
  public void testMonitorAndHeartbeatSkipStreamDuringStopProcedure() {
    registerStreamNode(streamNode("127.0.0.1", 10820));
    final StreamTask task = buildTask("stream_0");
    streamInfo.addTask(task);
    startStream("stream_0");
    final int targetEpoch = task.getEpoch() + 1;
    streamNamesUnderOperation.add("stream_0");

    Assert.assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(),
        streamManager.stopStreamTask("stream_0", targetEpoch).getCode());
    Assert.assertEquals(StreamTaskExpectedStatus.RUNNING, streamInfo.getExpectedStatus("stream_0"));

    streamManager.runStreamMonitorOnceForTest();
    Assert.assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(),
        streamManager
            .recordHeartbeat(
                "stream_0",
                targetEpoch,
                task.getCnStartTime(),
                task.getLeaderTerm(),
                "127.0.0.1:10820")
            .getCode());

    Assert.assertEquals(StreamTaskStatus.STOPPED, task.getStatus());
    Assert.assertEquals(1, streamManager.startRequests.size());
  }

  @Test
  public void testMonitorAndHeartbeatSkipStreamDuringStartProcedure() {
    registerStreamNode(streamNode("127.0.0.1", 10820));
    final StreamTask task = buildTask("stream_0");
    streamInfo.addTask(task);
    startStream("stream_0");
    final int targetEpoch = task.getEpoch() + 1;
    streamManager.stopStreamTask("stream_0", targetEpoch);
    streamManager.persistStopStream(task, targetEpoch);
    streamNamesUnderOperation.add("stream_0");

    Assert.assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(),
        streamManager.startStreamTask("stream_0").getCode());
    Assert.assertEquals(StreamTaskExpectedStatus.STOPPED, streamInfo.getExpectedStatus("stream_0"));

    streamManager.runStreamMonitorOnceForTest();
    Assert.assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(),
        streamManager
            .recordHeartbeat(
                "stream_0",
                task.getEpoch(),
                task.getCnStartTime(),
                task.getLeaderTerm(),
                "127.0.0.1:10820")
            .getCode());

    Assert.assertEquals(StreamTaskStatus.RUNNING, task.getStatus());
    Assert.assertEquals(1, streamManager.stopRequests.size());
  }

  @Test
  public void testStreamBeingDroppedCannotBeStarted() {
    registerStreamNode(streamNode("127.0.0.1", 10820));
    final StreamTask task = buildTask("stream_0");
    streamInfo.addTask(task);

    Assert.assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(),
        streamManager.markStreamBeingDropped("stream_0").getCode());
    Assert.assertEquals(
        TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode(), startStream("stream_0").getCode());

    task.setStatus(StreamTaskStatus.UNKNOWN);
    streamManager.runStreamMonitorOnceForTest();

    Assert.assertEquals(StreamTaskStatus.UNKNOWN, task.getStatus());
    Assert.assertTrue(streamManager.startRequests.isEmpty());

    streamManager.unmarkStreamBeingDropped("stream_0");
    Assert.assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(), startStream("stream_0").getCode());
  }

  @Test
  public void testMonitorCollectsStreamNodeHeartbeat() {
    registerStreamNode(streamNode("127.0.0.1", 10820));
    final StreamTask task = buildTask("stream_0");
    streamInfo.addTask(task);
    startStream("stream_0");
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
  public void testMonitorStopsRuntimeReportingStaleEpoch() {
    registerStreamNode(streamNode("127.0.0.1", 10820));
    final StreamTask task = buildTask("stream_0");
    streamInfo.addTask(task);
    startStream("stream_0");
    streamInfo.applyStartStream(new StartStreamPlan(task.getTaskName(), task.getEpoch() + 1));
    final int currentEpoch = task.getEpoch();
    streamManager.heartbeatResponses.add(
        new TStreamNodeHeartbeatResp(System.nanoTime(), NodeStatus.Running.getStatus())
            .setRunningTasks(
                Collections.singletonList(
                    new TTaskHeartbeat(task.getTaskName(), currentEpoch - 1))));

    streamManager.runStreamMonitorOnceForTest();

    Assert.assertEquals(1, streamManager.stopRequests.size());
    Assert.assertEquals("127.0.0.1", streamManager.stopEndpoints.get(0).getIp());
    Assert.assertEquals(10820, streamManager.stopEndpoints.get(0).getPort());
    Assert.assertEquals(task.getTaskName(), streamManager.stopRequests.get(0).getTaskName());
    Assert.assertEquals(currentEpoch - 1, streamManager.stopRequests.get(0).getEpoch());
    Assert.assertEquals(task.getCnStartTime(), streamManager.stopRequests.get(0).getCnStartTime());
    Assert.assertEquals(StreamTaskStatus.RUNNING, task.getStatus());
  }

  @Test
  public void testReconcileTriggeredWhenRegisteredStreamNodeBecomesRunning() {
    registerStreamNode(streamNode("127.0.0.1", 10820));
    final Map<Integer, Pair<NodeStatistics, NodeStatistics>> changes = new HashMap<>();

    changes.put(
        0,
        new Pair<>(new NodeStatistics(NodeStatus.Unknown), new NodeStatistics(NodeStatus.Running)));
    streamManager.onNodeStatisticsChanged(new NodeStatisticsChangeEvent(changes));
    Assert.assertEquals(1, streamManager.reconcileTriggerCount);

    changes.clear();
    changes.put(
        0,
        new Pair<>(new NodeStatistics(NodeStatus.Running), new NodeStatistics(NodeStatus.Running)));
    changes.put(
        1,
        new Pair<>(new NodeStatistics(NodeStatus.Unknown), new NodeStatistics(NodeStatus.Running)));
    streamManager.onNodeStatisticsChanged(new NodeStatisticsChangeEvent(changes));
    Assert.assertEquals(1, streamManager.reconcileTriggerCount);
  }

  @Test
  public void testStartStreamUsesRegisteredStreamNodesRoundRobin() {
    registerStreamNode(streamNode("127.0.0.1", 10820));
    registerStreamNode(streamNode("127.0.0.2", 10821));
    streamInfo.addTask(buildTask("stream_0"));
    streamInfo.addTask(buildTask("stream_1"));

    Assert.assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(), startStream("stream_0").getCode());
    Assert.assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(), startStream("stream_1").getCode());

    Assert.assertEquals("127.0.0.1", streamManager.startEndpoints.get(0).getIp());
    Assert.assertEquals(10820, streamManager.startEndpoints.get(0).getPort());
    Assert.assertEquals("127.0.0.2", streamManager.startEndpoints.get(1).getIp());
    Assert.assertEquals(10821, streamManager.startEndpoints.get(1).getPort());
  }

  @Test
  public void testMigrateRuntimeTasksInCluster() {
    registerStreamNode(streamNode("127.0.0.1", 10820));
    registerStreamNode(streamNode("127.0.0.2", 10821));
    final StreamTask task = buildTask("stream_0");
    streamInfo.addTask(task);
    Assert.assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(), startStream("stream_0").getCode());
    Assert.assertEquals("127.0.0.1:10820", task.getRunningOn());
    operationOrder.clear();

    removingStreamNodeIds.add(0);
    final int heartbeatCountBeforeMigrate = streamManager.heartbeatRequests.size();
    final Set<String> stoppedTasksToRestart = new LinkedHashSet<>();
    final TSStatus status =
        streamManager.migrateRuntimeTasks(
            registeredStreamNodes.get(0).getLocation(), stoppedTasksToRestart);

    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
    Assert.assertEquals(StreamTaskStatus.RUNNING, task.getStatus());
    Assert.assertEquals(
        ManagerMessages.MESSAGE_STREAMNODE_SHUTDOWN_C83EA302, task.getLastDownReason());
    Assert.assertEquals("127.0.0.2:10821", task.getRunningOn());
    Assert.assertTrue(stoppedTasksToRestart.isEmpty());
    Assert.assertEquals(1, streamManager.stopEndpoints.size());
    Assert.assertEquals(1, streamManager.dropEndpoints.size());
    Assert.assertEquals(2, streamManager.startEndpoints.size());
    Assert.assertEquals("127.0.0.2", streamManager.startEndpoints.get(1).getIp());
    Assert.assertEquals(10821, streamManager.startEndpoints.get(1).getPort());
    Assert.assertEquals(heartbeatCountBeforeMigrate, streamManager.heartbeatRequests.size());
    Assert.assertEquals(List.of("epoch:2", "stop:2", "epoch:3", "start:3"), operationOrder);
  }

  @Test
  public void testMigrateRuntimeTasksClearsRunningOnWhenExpectedStatusIsStopped() {
    registerStreamNode(streamNode("127.0.0.1", 10820));
    registerStreamNode(streamNode("127.0.0.2", 10821));
    final StreamTask task = buildTask("stream_0");
    streamInfo.addTask(task);
    Assert.assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(), startStream("stream_0").getCode());
    streamInfo.applyStopStream(new StopStreamPlan("stream_0", task.getEpoch() + 1));
    // Simulate a runtime heartbeat observed after STOP was persisted but before migration.
    task.setStatus(StreamTaskStatus.RUNNING);
    removingStreamNodeIds.add(0);

    final TSStatus status =
        streamManager.migrateRuntimeTasks(
            registeredStreamNodes.get(0).getLocation(), new LinkedHashSet<>());

    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
    Assert.assertEquals(StreamTaskExpectedStatus.STOPPED, streamInfo.getExpectedStatus("stream_0"));
    Assert.assertEquals(StreamTaskStatus.STOPPED, task.getStatus());
    Assert.assertEquals("", task.getRunningOn());
    Assert.assertEquals(1, streamManager.startEndpoints.size());
    Assert.assertEquals(1, streamManager.stopEndpoints.size());
    Assert.assertEquals(1, streamManager.dropEndpoints.size());

    streamManager.runStreamMonitorOnceForTest();

    Assert.assertEquals(1, streamManager.stopEndpoints.size());
  }

  @Test
  public void testDropStoppedStreamAfterRemovingItsStreamNode() {
    registerStreamNode(streamNode("127.0.0.1", 10820));
    registerStreamNode(streamNode("127.0.0.2", 10821));
    final StreamTask task = buildTask("stream_0");
    streamInfo.addTask(task);
    Assert.assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(), startStream("stream_0").getCode());
    Assert.assertEquals("127.0.0.1:10820", task.getRunningOn());

    final int stopEpoch = task.getEpoch() + 1;
    Assert.assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(),
        streamManager.stopStreamTask("stream_0", stopEpoch).getCode());
    Assert.assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(),
        streamManager.persistStopStream(task, stopEpoch).getCode());

    removingStreamNodeIds.add(0);
    Assert.assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(),
        streamManager
            .migrateRuntimeTasks(registeredStreamNodes.get(0).getLocation(), new LinkedHashSet<>())
            .getCode());
    Assert.assertEquals("", task.getRunningOn());
    Assert.assertTrue(streamManager.dropRequests.isEmpty());
    registeredStreamNodes.remove(0);

    Assert.assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(),
        streamManager.dropStreamTask("stream_0").getCode());
  }

  @Test
  public void testDropStartRollbackStreamAfterRemovingItsStreamNode() {
    registerStreamNode(streamNode("127.0.0.1", 10820));
    registerStreamNode(streamNode("127.0.0.2", 10821));
    final StreamTask task = buildTask("stream_0");
    streamInfo.addTask(task);
    Assert.assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(),
        streamManager.startStreamTask("stream_0").getCode());
    Assert.assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(),
        streamManager.rollbackStartStreamTask(task, task.getEpoch() + 1).getCode());
    Assert.assertEquals(StreamTaskExpectedStatus.CREATED, streamInfo.getExpectedStatus("stream_0"));
    Assert.assertEquals(StreamTaskStatus.STOPPED, task.getStatus());
    Assert.assertEquals("127.0.0.1:10820", task.getRunningOn());

    removingStreamNodeIds.add(0);
    Assert.assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(),
        streamManager
            .migrateRuntimeTasks(registeredStreamNodes.get(0).getLocation(), new LinkedHashSet<>())
            .getCode());
    Assert.assertEquals("", task.getRunningOn());
    Assert.assertTrue(streamManager.dropRequests.isEmpty());
    registeredStreamNodes.remove(0);

    Assert.assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(),
        streamManager.dropStreamTask("stream_0").getCode());
  }

  @Test
  public void testMigrateStoppedTaskToRestartWhenExpectedStatusIsRunning() {
    registerStreamNode(streamNode("127.0.0.1", 10820));
    registerStreamNode(streamNode("127.0.0.2", 10821));
    final StreamTask task = buildTask("stream_0");
    streamInfo.addTask(task);
    Assert.assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(), startStream("stream_0").getCode());
    task.setStatus(StreamTaskStatus.STOPPED);
    final Set<String> stoppedTasksToRestart = new LinkedHashSet<>();
    stoppedTasksToRestart.add(task.getTaskName());

    removingStreamNodeIds.add(0);
    Assert.assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(),
        streamManager
            .migrateRuntimeTasks(registeredStreamNodes.get(0).getLocation(), stoppedTasksToRestart)
            .getCode());

    Assert.assertTrue(stoppedTasksToRestart.isEmpty());
    Assert.assertEquals(StreamTaskStatus.RUNNING, task.getStatus());
    Assert.assertEquals("127.0.0.2:10821", task.getRunningOn());
    Assert.assertEquals(1, streamManager.dropRequests.size());
    Assert.assertEquals(2, streamManager.startRequests.size());
  }

  @Test
  public void testMigrateRuntimeTasksLeavesStoppedTasksWhenStopFails() {
    registerStreamNode(streamNode("127.0.0.1", 10820));
    registerStreamNode(streamNode("127.0.0.2", 10821));
    final StreamTask task0 = buildTask("stream_0");
    final StreamTask task1 = buildTask("stream_1");
    streamInfo.addTask(task0);
    streamInfo.addTask(task1);
    Assert.assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(), startStream("stream_0").getCode());
    Assert.assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(), startStream("stream_1").getCode());
    // Force both tasks onto the StreamNode being migrated.
    task0.setRunningOn("127.0.0.1:10820");
    task1.setRunningOn("127.0.0.1:10820");
    task0.setStatus(StreamTaskStatus.RUNNING);
    task1.setStatus(StreamTaskStatus.RUNNING);

    // tasksToStop is an unordered Set; stop RPC responses are consumed in iteration order.
    // First stop succeeds -> that task becomes STOPPED and is recorded for restart. The second
    // task's direct stop and both fallback broadcast stops fail, aborting Phase 1, so that task
    // stays RUNNING and Phase 2 (drop/start) is skipped.
    streamManager.enqueueStopStatus(StatusUtils.OK);
    for (int i = 0; i < 3; i++) {
      streamManager.enqueueStopStatus(
          new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode())
              .setMessage("stop fail"));
    }

    final Set<String> stoppedTasksToRestart = new LinkedHashSet<>();
    final int startCountBeforeMigrate = streamManager.startEndpoints.size();
    final TSStatus status =
        streamManager.migrateRuntimeTasks(
            registeredStreamNodes.get(0).getLocation(), stoppedTasksToRestart);

    Assert.assertEquals(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode(), status.getCode());
    Assert.assertEquals(1, stoppedTasksToRestart.size());
    final StreamTask stoppedTask =
        "stream_0".equals(stoppedTasksToRestart.iterator().next()) ? task0 : task1;
    final StreamTask remainingTask = stoppedTask == task0 ? task1 : task0;
    Assert.assertEquals(StreamTaskStatus.STOPPED, stoppedTask.getStatus());
    Assert.assertEquals("127.0.0.1:10820", stoppedTask.getRunningOn());
    Assert.assertEquals(StreamTaskStatus.RUNNING, remainingTask.getStatus());
    Assert.assertEquals("127.0.0.1:10820", remainingTask.getRunningOn());
    Assert.assertTrue(streamManager.dropEndpoints.isEmpty());
    Assert.assertEquals(startCountBeforeMigrate, streamManager.startEndpoints.size());
  }

  @Test
  public void testMigrateRuntimeTasksFailsInSingleNodeWhenTaskExists() {
    registerStreamNode(streamNode("127.0.0.1", 10820));
    final StreamTask task = buildTask("stream_0");
    streamInfo.addTask(task);
    Assert.assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(), startStream("stream_0").getCode());

    final TSStatus status =
        streamManager.migrateRuntimeTasks(
            registeredStreamNodes.get(0).getLocation(), new LinkedHashSet<>());

    Assert.assertEquals(
        TSStatusCode.STREAM_TASK_EXIST_ON_STREAMNODE.getStatusCode(), status.getCode());
    Assert.assertTrue(streamManager.stopEndpoints.isEmpty());
    Assert.assertTrue(streamManager.dropEndpoints.isEmpty());
  }

  @Test
  public void testMigrateRuntimeTasksSucceedsInSingleNodeWhenNoTask() {
    registerStreamNode(streamNode("127.0.0.1", 10820));
    final TSStatus status =
        streamManager.migrateRuntimeTasks(
            registeredStreamNodes.get(0).getLocation(), new LinkedHashSet<>());

    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
  }

  @Test
  public void testMigrateRuntimeTasksIgnoresDropFailureAndContinues() {
    registerStreamNode(streamNode("127.0.0.1", 10820));
    registerStreamNode(streamNode("127.0.0.2", 10821));
    final StreamTask task = buildTask("stream_0");
    streamInfo.addTask(task);
    Assert.assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(), startStream("stream_0").getCode());

    streamManager.enqueueDropStatus(
        new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode()).setMessage("drop fail"));
    removingStreamNodeIds.add(0);

    final TSStatus status =
        streamManager.migrateRuntimeTasks(
            registeredStreamNodes.get(0).getLocation(), new LinkedHashSet<>());

    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
    Assert.assertEquals(StreamTaskStatus.RUNNING, task.getStatus());
    Assert.assertEquals("127.0.0.2:10821", task.getRunningOn());
    Assert.assertEquals(1, streamManager.dropEndpoints.size());
  }

  @Test
  public void testStopStreamRejectedWhileBeingMigrated() throws Exception {
    registerStreamNode(streamNode("127.0.0.1", 10820));
    final StreamTask task = buildTask("stream_0");
    streamInfo.addTask(task);
    Assert.assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(), startStream("stream_0").getCode());
    operationOrder.clear();

    markStreamBeingMigrated("stream_0");
    try {
      final TSStatus status = streamManager.stopStreamTask("stream_0", task.getEpoch() + 1);
      Assert.assertEquals(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode(), status.getCode());
      Assert.assertEquals(StreamTaskStatus.RUNNING, task.getStatus());
      Assert.assertTrue(streamManager.stopEndpoints.isEmpty());
    } finally {
      unmarkStreamBeingMigrated("stream_0");
    }
  }

  @Test
  public void testStartStreamRejectedWhileBeingMigrated() throws Exception {
    registerStreamNode(streamNode("127.0.0.1", 10820));
    final StreamTask task = buildTask("stream_0");
    streamInfo.addTask(task);

    markStreamBeingMigrated("stream_0");
    try {
      final TSStatus status = streamManager.startStreamTask("stream_0");
      Assert.assertEquals(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode(), status.getCode());
      Assert.assertEquals(StreamTaskStatus.CREATED, task.getStatus());
      Assert.assertTrue(streamManager.startEndpoints.isEmpty());
    } finally {
      unmarkStreamBeingMigrated("stream_0");
    }
  }

  @Test
  public void testStartRollbackBypassesMigrationGuard() throws Exception {
    registerStreamNode(streamNode("127.0.0.1", 10820));
    final StreamTask task = buildTask("stream_0");
    streamInfo.addTask(task);
    Assert.assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(), startStream("stream_0").getCode());
    operationOrder.clear();

    markStreamBeingMigrated("stream_0");
    try {
      final TSStatus status = streamManager.rollbackStartStreamTask(task, task.getEpoch() + 1);
      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      Assert.assertEquals(StreamTaskStatus.STOPPED, task.getStatus());
      Assert.assertEquals(
          ManagerMessages.MESSAGE_START_ROLLBACK_B6601816, task.getLastDownReason());
      Assert.assertEquals(1, streamManager.stopEndpoints.size());
      Assert.assertEquals(List.of("epoch:2", "stop:2"), operationOrder);
    } finally {
      unmarkStreamBeingMigrated("stream_0");
    }
  }

  @Test
  public void testStartRollbackStopsRuntimeWhenEpochPersistenceFails() throws Exception {
    registerStreamNode(streamNode("127.0.0.1", 10820));
    final StreamTask task = buildTask("stream_0");
    streamInfo.addTask(task);
    Assert.assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(),
        streamManager.startStreamTask("stream_0").getCode());
    Assert.assertEquals(StreamTaskExpectedStatus.CREATED, streamInfo.getExpectedStatus("stream_0"));
    Assert.assertEquals(StreamTaskStatus.RUNNING, task.getStatus());
    doReturn(new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode()))
        .when(consensusManager)
        .write(any(UpdateStreamEpochPlan.class));

    final int persistedEpoch = task.getEpoch();
    final int rollbackEpoch = task.getEpoch() + 1;
    final TSStatus status = streamManager.rollbackStartStreamTask(task, rollbackEpoch);

    Assert.assertEquals(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode(), status.getCode());
    Assert.assertEquals(StreamTaskExpectedStatus.CREATED, streamInfo.getExpectedStatus("stream_0"));
    Assert.assertEquals(StreamTaskStatus.STOPPED, task.getStatus());
    Assert.assertEquals(1, streamManager.stopRequests.size());
    Assert.assertEquals(persistedEpoch, streamManager.stopRequests.get(0).getEpoch());
  }

  @Test
  public void testDropStreamRejectedWhileBeingMigrated() throws Exception {
    registerStreamNode(streamNode("127.0.0.1", 10820));
    streamInfo.addTask(buildTask("stream_0"));

    markStreamBeingMigrated("stream_0");
    try {
      final TSStatus status = streamManager.markStreamBeingDropped("stream_0");
      Assert.assertEquals(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode(), status.getCode());
    } finally {
      unmarkStreamBeingMigrated("stream_0");
    }

    Assert.assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(),
        streamManager.markStreamBeingDropped("stream_0").getCode());
    streamManager.unmarkStreamBeingDropped("stream_0");
  }

  @Test
  public void testAssignStreamToStreamNodeExcludesRemovingIds() {
    registerStreamNode(streamNode("127.0.0.1", 10820));
    registerStreamNode(streamNode("127.0.0.2", 10821));
    streamInfo.addTask(buildTask("stream_0"));
    removingStreamNodeIds.add(0);

    Assert.assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(), startStream("stream_0").getCode());
    Assert.assertEquals("127.0.0.2:10821", streamInfo.getTask("stream_0").getRunningOn());
  }

  @Test
  public void testAssignStreamToStreamNodeReturnsNullWhenAllRemoving() {
    registerStreamNode(streamNode("127.0.0.1", 10820));
    streamInfo.addTask(buildTask("stream_0"));
    removingStreamNodeIds.add(0);

    final TSStatus status = startStream("stream_0");
    Assert.assertEquals(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode(), status.getCode());
    Assert.assertEquals(StreamTaskStatus.CREATED, streamInfo.getTask("stream_0").getStatus());
    Assert.assertTrue(streamManager.startRequests.isEmpty());
  }

  @Test
  public void testAssignStreamToStreamNodeFiltersByStatus() {
    final TStreamNodeConfiguration unknown = streamNode("127.0.0.1", 10820);
    final TStreamNodeConfiguration running = streamNode("127.0.0.2", 10821);
    final TStreamNodeConfiguration removing = streamNode("127.0.0.3", 10822);
    registerStreamNode(unknown);
    registerStreamNode(running);
    registerStreamNode(removing);
    when(nodeManager.filterStreamNodeThroughStatus(NodeStatus.Running))
        .thenReturn(Collections.singletonList(running));
    streamInfo.addTask(buildTask("stream_0"));

    Assert.assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(), startStream("stream_0").getCode());
    Assert.assertEquals("127.0.0.2:10821", streamInfo.getTask("stream_0").getRunningOn());
  }

  /** A newly created topic without owner history starts with epoch 1 on the selected StreamNode. */
  @Test
  public void testStartSubscriptionStreamCreatesAndAssignsTopicOwner() {
    registerStreamNode(streamNode("127.0.0.1", 10820));
    final StreamTask task = buildTask("subscription_stream");
    task.setSource(
        new IoTDBSubscriptionSource(
            "source_db",
            "source_table",
            null,
            Collections.singletonList("value"),
            "127.0.0.1",
            6667,
            "root",
            "root"));
    streamInfo.addTask(task);
    doReturn(StatusUtils.OK).when(configManager).createTopic(any(TCreateTopicReq.class));
    doReturn(StatusUtils.OK).when(configManager).alterTopic(any(TAlterTopicReq.class));
    when(subscriptionInfo.deepCopyTopicMeta("__stream__subscription_stream", true))
        .thenReturn(new TopicMeta("__stream__subscription_stream", 1L, Collections.emptyMap()));

    Assert.assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(), startStream("subscription_stream").getCode());

    final ArgumentCaptor<TCreateTopicReq> createCaptor =
        ArgumentCaptor.forClass(TCreateTopicReq.class);
    final ArgumentCaptor<TAlterTopicReq> alterCaptor =
        ArgumentCaptor.forClass(TAlterTopicReq.class);
    verify(configManager).createTopic(createCaptor.capture());
    verify(configManager).alterTopic(alterCaptor.capture());

    final TCreateTopicReq createRequest = createCaptor.getValue();
    Assert.assertEquals("__stream__subscription_stream", createRequest.getTopicName());
    Assert.assertTrue(createRequest.isIfNotExistsCondition());
    Assert.assertEquals("source_db", createRequest.getTopicAttributes().get("database"));
    Assert.assertEquals("source_table", createRequest.getTopicAttributes().get("table"));
    Assert.assertEquals(
        TopicConstant.MODE_CONSENSUS_VALUE, createRequest.getTopicAttributes().get("mode"));

    final TAlterTopicReq alterRequest = alterCaptor.getValue();
    Assert.assertEquals("__stream__subscription_stream", alterRequest.getTopicName());
    Assert.assertEquals("127.0.0.1:10820", alterRequest.getTopicAttributes().get("owner-id"));
    Assert.assertEquals("1", alterRequest.getTopicAttributes().get("owner-epoch"));
    Assert.assertEquals("table", alterRequest.getTopicAttributes().get("__system.sql-dialect"));
    Assert.assertEquals(1, streamManager.startRequests.size());
  }

  /** A restored task at epoch 0 must advance past the topic's persisted owner epoch 41. */
  @Test
  public void testStartSubscriptionStreamUsesPersistedOwnerEpoch() throws Exception {
    final StreamTask task = prepareSubscriptionTask(41L, false);

    Assert.assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(), startStream(task.getTaskName()).getCode());

    assertSubscriptionEpoch(task, 42);
  }

  /** Clearing an owner retains epoch 41 as the baseline, so the next owner still receives 42. */
  @Test
  public void testStartSubscriptionStreamUsesClearedOwnerHistory() throws Exception {
    final StreamTask task = prepareSubscriptionTask(41L, true);

    Assert.assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(), startStream(task.getTaskName()).getCode());

    assertSubscriptionEpoch(task, 42);
  }

  /** When the task epoch is ahead of topic history, START must also advance past the task epoch. */
  @Test
  public void testStartSubscriptionStreamKeepsTaskEpochMonotonic() throws Exception {
    final StreamTask task = prepareSubscriptionTask(5L, false);
    task.setEpoch(8);

    Assert.assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(), startStream(task.getTaskName()).getCode());

    assertSubscriptionEpoch(task, 9);
  }

  /** A topic removed after CREATE must fail START without altering ownership or dispatching. */
  @Test
  public void testStartSubscriptionStreamRejectsMissingTopicMetadata() {
    final StreamTask task = prepareSubscriptionTask(5L, false);
    when(subscriptionInfo.deepCopyTopicMeta("__stream__subscription_stream", true))
        .thenReturn(null);

    Assert.assertEquals(
        TSStatusCode.TOPIC_NOT_EXIST_ERROR.getStatusCode(),
        startStream(task.getTaskName()).getCode());

    Assert.assertEquals(0, task.getEpoch());
    Assert.assertTrue(streamManager.startRequests.isEmpty());
    verify(configManager, never()).alterTopic(any(TAlterTopicReq.class));
  }

  /** Epochs at the int boundary or beyond must fail without overflow or dispatching a task. */
  @Test
  public void testStartSubscriptionStreamRejectsEpochOverflow() {
    for (final long epoch : new long[] {Integer.MAX_VALUE, Long.MAX_VALUE}) {
      final StreamTask task = prepareSubscriptionTask(epoch, false);

      Assert.assertEquals(
          TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode(),
          startStream(task.getTaskName()).getCode());

      Assert.assertEquals(0, task.getEpoch());
      Assert.assertTrue(streamManager.startRequests.isEmpty());
      streamInfo.removeTask(task.getTaskName());
    }
    verify(configManager, never()).alterTopic(any(TAlterTopicReq.class));
  }

  /** A rejected owner transfer keeps its reserved epoch but prevents runtime dispatch. */
  @Test
  public void testSubscriptionOwnerTransferFailurePreventsTaskStart() {
    final StreamTask task = prepareSubscriptionTask(41L, false);
    final TSStatus failure = new TSStatus(TSStatusCode.ALTER_TOPIC_ERROR.getStatusCode());
    doReturn(failure).when(configManager).alterTopic(any(TAlterTopicReq.class));

    Assert.assertEquals(failure, startStream(task.getTaskName()));

    Assert.assertEquals(42, task.getEpoch());
    Assert.assertEquals(Collections.singletonList("epoch:42"), operationOrder);
    Assert.assertTrue(streamManager.startRequests.isEmpty());
  }

  private StreamTask prepareSubscriptionTask(final long epoch, final boolean clearOwner) {
    if (registeredStreamNodes.isEmpty()) {
      registerStreamNode(streamNode("127.0.0.1", 10820));
    }
    final StreamTask task = buildTask("subscription_stream");
    task.setSource(
        new IoTDBSubscriptionSource(
            "source_db", "source_table", null, null, "127.0.0.1", 6667, "root", "root"));
    streamInfo.addTask(task);
    final TopicMeta topicMeta = new TopicMeta("__stream__subscription_stream", 1L, new HashMap<>());
    topicMeta.transferOwner("previous_owner", epoch);
    if (clearOwner) {
      topicMeta.clearOwner();
    }
    when(subscriptionInfo.deepCopyTopicMeta(topicMeta.getTopicName(), true)).thenReturn(topicMeta);
    doReturn(StatusUtils.OK).when(configManager).createTopic(any(TCreateTopicReq.class));
    doReturn(StatusUtils.OK).when(configManager).alterTopic(any(TAlterTopicReq.class));
    return task;
  }

  private void assertSubscriptionEpoch(final StreamTask task, final int expectedEpoch)
      throws Exception {
    final ArgumentCaptor<TAlterTopicReq> captor = ArgumentCaptor.forClass(TAlterTopicReq.class);
    verify(configManager).alterTopic(captor.capture());
    Assert.assertEquals(
        String.valueOf(expectedEpoch),
        captor.getValue().getTopicAttributes().get(TopicConstant.OWNER_EPOCH_KEY));
    Assert.assertEquals(expectedEpoch, task.getEpoch());
    final TStartTaskOnStreamNodeReq request = streamManager.startRequests.get(0);
    Assert.assertEquals(expectedEpoch, request.getEpoch());
    final StreamTask dispatchedTask =
        StreamTask.deserialize(ByteBuffer.wrap(request.getStreamTask()));
    Assert.assertEquals(
        expectedEpoch, ((IoTDBSubscriptionSource) dispatchedTask.getSource()).getOwnerEpoch());
  }

  /** Verifies a topic preparation failure prevents dispatching an incompletely fenced task. */
  @Test
  public void testSubscriptionTopicPreparationFailurePreventsTaskStart() {
    registerStreamNode(streamNode("127.0.0.1", 10820));
    final StreamTask task = buildTask("subscription_stream");
    task.setSource(
        new IoTDBSubscriptionSource(
            "source_db", "source_table", null, null, "127.0.0.1", 6667, "root", "root"));
    streamInfo.addTask(task);
    final TSStatus failure =
        new TSStatus(TSStatusCode.CREATE_TOPIC_ERROR.getStatusCode()).setMessage("create failed");
    when(configManager.createTopic(any(TCreateTopicReq.class))).thenReturn(failure);

    Assert.assertEquals(
        TSStatusCode.CREATE_TOPIC_ERROR.getStatusCode(),
        startStream("subscription_stream").getCode());
    Assert.assertEquals(StreamTaskStatus.CREATED, task.getStatus());
    Assert.assertTrue(streamManager.startRequests.isEmpty());
    verify(configManager, never()).alterTopic(any(TAlterTopicReq.class));
  }

  private StreamTask buildTask(final String taskName) {
    final StreamTask task = new StreamTask();
    task.setTaskName(taskName);
    task.setCreator("creator");
    task.setCreationTime(1L);
    task.setWindow(new PeriodWindow(0, 1L, 0L));
    task.setTypeProvider(new StreamNodeTableTypeProvider(Collections.emptyMap()));
    task.setTarget(new IoTDBTarget("db", "target", Collections.singletonList("value")));
    task.setProperties(new StreamProperties(-1L, 0, -1L, 0, false));
    return task;
  }

  private TSStatus startStream(final String streamName) {
    final TSStatus runtimeStatus = streamManager.startStreamTask(streamName);
    if (runtimeStatus.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return runtimeStatus;
    }
    final StreamTask task = streamInfo.getTask(streamName);
    Assert.assertNotNull(task);
    return streamManager.persistStartStream(task, task.getEpoch());
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

  @SuppressWarnings("unchecked")
  private void markStreamBeingMigrated(final String streamName) throws Exception {
    final java.lang.reflect.Field field =
        StreamManager.class.getDeclaredField("streamsBeingMigrated");
    field.setAccessible(true);
    ((Set<String>) field.get(streamManager)).add(streamName);
  }

  @SuppressWarnings("unchecked")
  private void unmarkStreamBeingMigrated(final String streamName) throws Exception {
    final java.lang.reflect.Field field =
        StreamManager.class.getDeclaredField("streamsBeingMigrated");
    field.setAccessible(true);
    ((Set<String>) field.get(streamManager)).remove(streamName);
  }

  private static class TestStreamManager extends StreamManager {

    private final List<TEndPoint> startEndpoints = new ArrayList<>();
    private final List<TStartTaskOnStreamNodeReq> startRequests = new ArrayList<>();
    private final List<TEndPoint> stopEndpoints = Collections.synchronizedList(new ArrayList<>());
    private final List<TStopTaskOnStreamNodeReq> stopRequests =
        Collections.synchronizedList(new ArrayList<>());
    private final List<TEndPoint> dropEndpoints = new ArrayList<>();
    private final List<TDropTaskOnStreamNodeReq> dropRequests = new ArrayList<>();
    private final List<TStreamNodeHeartbeatReq> heartbeatRequests = new ArrayList<>();
    private final List<TStreamNodeHeartbeatResp> heartbeatResponses = new ArrayList<>();
    private final List<Integer> heartbeatRetryNums = new ArrayList<>();
    private final List<Integer> stopRetryNums = Collections.synchronizedList(new ArrayList<>());
    private final Deque<TSStatus> stopStatuses = new ConcurrentLinkedDeque<>();
    private final Deque<TSStatus> dropStatuses = new ArrayDeque<>();
    private final List<String> operationOrder;
    private CountDownLatch concurrentStopEntered;
    private CountDownLatch releaseConcurrentStops;
    private int reconcileTriggerCount;

    private void blockBroadcastStops(final int count) {
      concurrentStopEntered = new CountDownLatch(count);
      releaseConcurrentStops = new CountDownLatch(1);
    }

    private void enqueueStopStatus(final TSStatus status) {
      stopStatuses.add(status);
    }

    private void enqueueDropStatus(final TSStatus status) {
      dropStatuses.add(status);
    }

    private TestStreamManager(
        final IManager configManager,
        final StreamInfo streamInfo,
        final Consumer<String> runningOnMetricRegistrar,
        final List<String> operationOrder) {
      super(configManager, streamInfo, runningOnMetricRegistrar);
      this.operationOrder = operationOrder;
    }

    @Override
    public void triggerReconcile() {
      reconcileTriggerCount++;
    }

    @Override
    protected TSStatus sendStartTask(
        final TEndPoint endPoint, final TStartTaskOnStreamNodeReq request) {
      startEndpoints.add(endPoint);
      startRequests.add(request);
      operationOrder.add("start:" + request.getEpoch());
      return StatusUtils.OK;
    }

    @Override
    protected TSStatus sendStopTask(
        final TEndPoint endPoint, final TStopTaskOnStreamNodeReq request) {
      stopEndpoints.add(endPoint);
      stopRequests.add(request);
      operationOrder.add("stop:" + request.getEpoch());
      return stopStatuses.isEmpty() ? StatusUtils.OK : stopStatuses.removeFirst();
    }

    @Override
    protected TSStatus sendStopTask(
        final TEndPoint endPoint, final TStopTaskOnStreamNodeReq request, final int retryNum) {
      stopRetryNums.add(retryNum);
      if (concurrentStopEntered != null) {
        concurrentStopEntered.countDown();
        try {
          if (!releaseConcurrentStops.await(5, TimeUnit.SECONDS)) {
            throw new AssertionError("Timed out waiting to release concurrent stop requests");
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new AssertionError(
              "Interrupted while waiting to release concurrent stop requests", e);
        }
      }
      return sendStopTask(endPoint, request);
    }

    @Override
    protected TSStatus sendDropTask(
        final TEndPoint endPoint, final TDropTaskOnStreamNodeReq request) {
      dropEndpoints.add(endPoint);
      dropRequests.add(request);
      return dropStatuses.isEmpty() ? StatusUtils.OK : dropStatuses.removeFirst();
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

    @Override
    protected TStreamNodeHeartbeatResp sendHeartbeat(
        final TEndPoint endPoint, final TStreamNodeHeartbeatReq request, final int retryNum) {
      heartbeatRetryNums.add(retryNum);
      return sendHeartbeat(endPoint, request);
    }
  }
}
