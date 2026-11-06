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

package org.apache.iotdb.streamnode;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TStreamNodeLocation;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.exception.ClientManagerException;
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.consensus.ConfigRegionId;
import org.apache.iotdb.commons.service.IService;
import org.apache.iotdb.commons.service.RegisterManager;
import org.apache.iotdb.commons.service.ServiceType;
import org.apache.iotdb.streamnode.client.ConfigNodeClient;
import org.apache.iotdb.streamnode.client.ConfigNodeClientManager;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class StreamNodeShutdownHookTest {

  private final CommonConfig commonConfig = CommonDescriptor.getInstance().getConfig();
  private final List<String> events = new ArrayList<>();

  private NodeStatus previousStatus;
  private String previousReason;
  private Field clientManagerField;
  private Object previousClientManager;
  private StreamNode streamNode;

  @Before
  public void setUp() throws Exception {
    previousStatus = commonConfig.getNodeStatus();
    previousReason = commonConfig.getStatusReason();
    commonConfig.setNodeStatus(NodeStatus.Running);

    // The private constructor also makes this instance the one returned by getInstance().
    final Constructor<StreamNode> constructor = StreamNode.class.getDeclaredConstructor();
    constructor.setAccessible(true);
    streamNode = constructor.newInstance();

    clientManagerField =
        Class.forName(ConfigNodeClientManager.class.getName() + "$ConfigNodeClientManagerHolder")
            .getDeclaredField("INSTANCE");
    clientManagerField.setAccessible(true);
    previousClientManager = clientManagerField.get(null);
  }

  @After
  public void tearDown() throws Exception {
    clientManagerField.set(null, previousClientManager);
    final Field instanceField =
        Class.forName(StreamNode.class.getName() + "$StreamNodeHolder")
            .getDeclaredField("INSTANCE");
    instanceField.setAccessible(true);
    instanceField.set(null, null);
    // Leave ReadOnly first: a ReadOnly-to-ReadOnly update keeps the higher-priority reason.
    commonConfig.setNodeStatus(NodeStatus.Running);
    commonConfig.setNodeStatusWithReason(previousStatus, previousReason);
  }

  @Test
  public void testStoppingStatusPrecedesServiceStopAndShutdownReport() throws Exception {
    final Field registerManagerField = StreamNode.class.getDeclaredField("registerManager");
    registerManagerField.setAccessible(true);
    ((RegisterManager) registerManagerField.get(streamNode)).register(new RecordingRpcService());
    clientManagerField.set(null, new RecordingConfigNodeClientManager());

    new StreamNodeShutdownHook(new TStreamNodeLocation(1, new TEndPoint("127.0.0.1", 10820))).run();

    // An RPC worker can still answer a heartbeat after the shutdown report. The ConfigNode keeps
    // the node Stopped only if that heartbeat reports ReadOnly(Stopping) instead of Running.
    assertEquals(
        Arrays.asList(
            "Stopping RPC service while ReadOnly(Stopping)",
            "Reporting shutdown while ReadOnly(Stopping)"),
        events);
  }

  private void record(final String event) {
    events.add(
        event
            + " while "
            + commonConfig.getNodeStatus().getStatus()
            + "("
            + commonConfig.getStatusReason()
            + ")");
  }

  /** Stands in for the RPC service that StreamNode#stop() closes. */
  private class RecordingRpcService implements IService {

    @Override
    public void start() {
      // Nothing to start.
    }

    @Override
    public void stop() {
      record("Stopping RPC service");
    }

    @Override
    public ServiceType getID() {
      return ServiceType.STREAM_NODE_RPC_SERVICE;
    }
  }

  /** Fails the shutdown report after recording the status it would have been sent with. */
  private class RecordingConfigNodeClientManager
      implements IClientManager<ConfigRegionId, ConfigNodeClient> {

    @Override
    public ConfigNodeClient borrowClient(final ConfigRegionId node) throws ClientManagerException {
      record("Reporting shutdown");
      throw new ClientManagerException("No ConfigNode is available in this test");
    }

    @Override
    public void clear(final ConfigRegionId node) {
      // Nothing to clear.
    }

    @Override
    public void clearAll() {
      // Nothing to clear.
    }

    @Override
    public void close() {
      // Nothing to close.
    }
  }
}
