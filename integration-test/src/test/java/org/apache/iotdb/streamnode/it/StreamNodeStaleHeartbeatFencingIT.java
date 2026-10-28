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

package org.apache.iotdb.streamnode.it;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TNodeResource;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.common.rpc.thrift.TSetConfigurationReq;
import org.apache.iotdb.common.rpc.thrift.TShowAppliedConfigurationsResp;
import org.apache.iotdb.common.rpc.thrift.TShowConfigurationResp;
import org.apache.iotdb.common.rpc.thrift.TStreamNodeConfiguration;
import org.apache.iotdb.common.rpc.thrift.TStreamNodeLocation;
import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.confignode.rpc.thrift.TNodeVersionInfo;
import org.apache.iotdb.confignode.rpc.thrift.TStreamNodeRegisterReq;
import org.apache.iotdb.confignode.rpc.thrift.TStreamNodeRegisterResp;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.it.env.cluster.EnvUtils;
import org.apache.iotdb.it.env.cluster.env.StreamEnv;
import org.apache.iotdb.itbase.category.StreamClusterIT;
import org.apache.iotdb.itbase.env.BaseEnv;
import org.apache.iotdb.rpc.DeepCopyRpcTransportFactory;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.streamnode.rpc.thrift.IStreamNodeRPCService;
import org.apache.iotdb.streamnode.rpc.thrift.TCreateTaskOnStreamNodeReq;
import org.apache.iotdb.streamnode.rpc.thrift.TDropTaskOnStreamNodeReq;
import org.apache.iotdb.streamnode.rpc.thrift.TStartTaskOnStreamNodeReq;
import org.apache.iotdb.streamnode.rpc.thrift.TStopTaskOnStreamNodeReq;
import org.apache.iotdb.streamnode.rpc.thrift.TStreamNodeHeartbeatReq;
import org.apache.iotdb.streamnode.rpc.thrift.TStreamNodeHeartbeatResp;
import org.apache.iotdb.streamnode.rpc.thrift.TTaskHeartbeat;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.awaitility.Awaitility;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Category({StreamClusterIT.class})
public class StreamNodeStaleHeartbeatFencingIT {

  private static final BaseEnv ENV = new StreamEnv(0);
  private static final ControllableStreamNode STREAM_NODE = new ControllableStreamNode();

  private static int streamNodePort;
  private static int streamNodeId;

  @BeforeClass
  public static void setUp() throws Exception {
    ENV.getConfig()
        .getCommonConfig()
        .setSubscriptionEnabled(true)
        .setPipeMemoryManagementEnabled(false)
        .setIsPipeEnableMemoryCheck(false)
        .setDataRegionConsensusProtocolClass(ConsensusFactory.IOT_CONSENSUS);
    ENV.initClusterEnvironment(1, 1, 30);

    streamNodePort = EnvUtils.searchAvailablePorts()[0];
    STREAM_NODE.start(streamNodePort);
    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) ENV.getLeaderConfigNodeConnection()) {
      final TStreamNodeRegisterResp response =
          client.registerStreamNode(
              new TStreamNodeRegisterReq(
                      IoTDBConstant.DEFAULT_CLUSTER_NAME,
                      new TStreamNodeConfiguration(
                          new TStreamNodeLocation(-1, new TEndPoint("127.0.0.1", streamNodePort)),
                          new TNodeResource(1, 1024L)))
                  .setVersionInfo(
                      new TNodeVersionInfo(IoTDBConstant.VERSION, IoTDBConstant.BUILD_INFO)));
      assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), response.getStatus().getCode());
      streamNodeId = response.getStreamNodeId();
    }
    Awaitility.await()
        .atMost(30, TimeUnit.SECONDS)
        .until(StreamNodeStaleHeartbeatFencingIT::isFakeStreamNodeRunning);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    STREAM_NODE.close();
    Files.deleteIfExists(Path.of(EnvUtils.getLockFilePath(streamNodePort)));
    ENV.cleanClusterEnvironment();
  }

  @Test
  public void testDelayedStaleHeartbeatCannotStopNewRuntime() throws Exception {
    final String streamName = "stale_heartbeat_fencing";
    try (Connection connection = ENV.getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE test");
      statement.execute("USE test");
      statement.execute("CREATE TABLE source_table (value INT64 FIELD)");
      statement.execute("CREATE TABLE sink_table (value INT64 FIELD)");
      statement.execute(
          "CREATE STREAM "
              + streamName
              + " FROM source_table TUMBLE(1s) INTO sink_table(time, value)"
              + " SELECT ${START_TIME}, count(*) FROM ${ROWS}");
      statement.execute("START STREAM " + streamName);
      assertEquals(1, STREAM_NODE.getRuntimeEpoch());

      STREAM_NODE.blockNextStreamManagerHeartbeat();
      assertTrue(
          "The StreamManager did not collect the epoch-1 heartbeat",
          STREAM_NODE.awaitBlockedHeartbeat());

      statement.execute("STOP STREAM " + streamName);
      statement.execute("START STREAM " + streamName);
      assertEquals(3, STREAM_NODE.getRuntimeEpoch());
      assertTrue(STREAM_NODE.isRuntimeRunning());

      STREAM_NODE.releaseBlockedHeartbeat();
      assertTrue(
          "The stale-heartbeat cleanup STOP was not received",
          STREAM_NODE.awaitStaleHeartbeatStop());

      assertEquals(
          "Cleanup of an epoch-1 heartbeat must be fenced by epoch 1",
          1,
          STREAM_NODE.getStaleHeartbeatStopEpoch());
      assertEquals(3, STREAM_NODE.getRuntimeEpoch());
      assertTrue("The valid epoch-3 runtime must remain running", STREAM_NODE.isRuntimeRunning());
    }
  }

  private static boolean isFakeStreamNodeRunning() {
    try (Connection connection = ENV.getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("SHOW STREAMNODES")) {
      while (resultSet.next()) {
        if (resultSet.getInt("NodeID") == streamNodeId) {
          return NodeStatus.Running.getStatus().equals(resultSet.getString("Status"));
        }
      }
    } catch (Exception ignored) {
      return false;
    }
    return false;
  }

  private static final class ControllableStreamNode
      implements IStreamNodeRPCService.Iface, AutoCloseable {

    private final AtomicInteger runtimeEpoch = new AtomicInteger(-1);
    private final AtomicBoolean runtimeRunning = new AtomicBoolean(false);
    private final AtomicBoolean blockNextStreamManagerHeartbeat = new AtomicBoolean(false);
    private final AtomicBoolean blockedHeartbeatReleased = new AtomicBoolean(false);
    private final AtomicInteger staleHeartbeatStopEpoch = new AtomicInteger(-1);

    private volatile CountDownLatch blockedHeartbeatCaptured = new CountDownLatch(0);
    private volatile CountDownLatch releaseBlockedHeartbeat = new CountDownLatch(0);
    private volatile CountDownLatch staleHeartbeatStopReceived = new CountDownLatch(0);
    private TServer server;
    private TServerSocket serverSocket;

    private void start(final int port) throws Exception {
      serverSocket = new TServerSocket(port);
      server =
          new TThreadPoolServer(
              new TThreadPoolServer.Args(serverSocket)
                  .processor(new IStreamNodeRPCService.Processor<>(this))
                  .protocolFactory(new TBinaryProtocol.Factory())
                  .transportFactory(DeepCopyRpcTransportFactory.INSTANCE));
      final Thread serverThread = new Thread(server::serve, "ControllableStreamNode");
      serverThread.setDaemon(true);
      serverThread.start();
      Awaitility.await().atMost(10, TimeUnit.SECONDS).until(server::isServing);
    }

    private void blockNextStreamManagerHeartbeat() {
      blockedHeartbeatCaptured = new CountDownLatch(1);
      releaseBlockedHeartbeat = new CountDownLatch(1);
      staleHeartbeatStopReceived = new CountDownLatch(1);
      blockedHeartbeatReleased.set(false);
      staleHeartbeatStopEpoch.set(-1);
      blockNextStreamManagerHeartbeat.set(true);
    }

    private boolean awaitBlockedHeartbeat() throws InterruptedException {
      return blockedHeartbeatCaptured.await(15, TimeUnit.SECONDS);
    }

    private void releaseBlockedHeartbeat() {
      blockedHeartbeatReleased.set(true);
      releaseBlockedHeartbeat.countDown();
    }

    private boolean awaitStaleHeartbeatStop() throws InterruptedException {
      return staleHeartbeatStopReceived.await(15, TimeUnit.SECONDS);
    }

    private int getRuntimeEpoch() {
      return runtimeEpoch.get();
    }

    private boolean isRuntimeRunning() {
      return runtimeRunning.get();
    }

    private int getStaleHeartbeatStopEpoch() {
      return staleHeartbeatStopEpoch.get();
    }

    @Override
    public TStreamNodeHeartbeatResp getHeartbeat(final TStreamNodeHeartbeatReq request)
        throws TException {
      final int reportedEpoch = runtimeRunning.get() ? runtimeEpoch.get() : -1;
      if (request.getCnStartTime() > 0
          && blockNextStreamManagerHeartbeat.compareAndSet(true, false)) {
        blockedHeartbeatCaptured.countDown();
        try {
          if (!releaseBlockedHeartbeat.await(30, TimeUnit.SECONDS)) {
            throw new TException("Timed out waiting to release the blocked heartbeat");
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new TException(e);
        }
      }
      final TStreamNodeHeartbeatResp response =
          new TStreamNodeHeartbeatResp(
              request.getHeartbeatTimestamp(), NodeStatus.Running.getStatus());
      if (reportedEpoch >= 0) {
        response.setRunningTasks(
            Collections.singletonList(
                new TTaskHeartbeat("stale_heartbeat_fencing", reportedEpoch)));
      } else {
        response.setRunningTasks(Collections.emptyList());
      }
      return response;
    }

    @Override
    public TSStatus createTask(final TCreateTaskOnStreamNodeReq request) {
      return success();
    }

    @Override
    public TSStatus startTask(final TStartTaskOnStreamNodeReq request) {
      runtimeEpoch.set(request.getEpoch());
      runtimeRunning.set(true);
      return success();
    }

    @Override
    public TSStatus stopTask(final TStopTaskOnStreamNodeReq request) {
      if (blockedHeartbeatReleased.get()) {
        staleHeartbeatStopEpoch.set(request.getEpoch());
        staleHeartbeatStopReceived.countDown();
      }
      if (request.getEpoch() < runtimeEpoch.get()) {
        return new TSStatus(TSStatusCode.STREAM_STALE.getStatusCode());
      }
      runtimeEpoch.set(request.getEpoch());
      runtimeRunning.set(false);
      return success();
    }

    @Override
    public TSStatus dropTask(final TDropTaskOnStreamNodeReq request) {
      runtimeRunning.set(false);
      return success();
    }

    @Override
    public TSStatus dropAllTasks() {
      runtimeRunning.set(false);
      return success();
    }

    @Override
    public TSStatus setConfiguration(final TSetConfigurationReq request) {
      return success();
    }

    @Override
    public TShowConfigurationResp showConfiguration() {
      return new TShowConfigurationResp(success(), "");
    }

    @Override
    public TShowAppliedConfigurationsResp showAppliedConfigurations() {
      return new TShowAppliedConfigurationsResp(success());
    }

    @Override
    public TSStatus loadConfiguration() {
      return success();
    }

    @Override
    public TSStatus stopStreamNode() {
      return success();
    }

    @Override
    public void close() {
      releaseBlockedHeartbeat.countDown();
      if (server != null) {
        server.stop();
      }
      if (serverSocket != null) {
        serverSocket.close();
      }
    }

    private static TSStatus success() {
      return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    }
  }
}
