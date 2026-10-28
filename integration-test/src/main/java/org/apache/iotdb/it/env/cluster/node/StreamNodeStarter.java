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

package org.apache.iotdb.it.env.cluster.node;

import org.apache.iotdb.it.env.cluster.EnvUtils;
import org.apache.iotdb.it.framework.IoTDBTestLogger;
import org.apache.iotdb.itbase.runtime.ParallelRequestDelegate;
import org.apache.iotdb.itbase.runtime.RequestDelegate;

import org.slf4j.Logger;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

import static org.apache.iotdb.it.env.cluster.ClusterConstant.NODE_START_TIMEOUT;

public class StreamNodeStarter {
  private static final Logger logger = IoTDBTestLogger.logger;

  private StreamNodeStarter() {}

  public static List<StreamNodeWrapper> startStreamNode(
      final int streamNodesNum,
      final String seedConfigNode,
      final String dataNodeEndpoints,
      final String testClassName,
      final String testMethodName,
      final int clusterIndex,
      final long startTime,
      final List<String> killPoints,
      final Consumer<StreamNodeWrapper> nodeRegister,
      final Runnable dumpTestJVMSnapshot) {
    final List<String> streamNodeEndpoints = new ArrayList<>();
    List<StreamNodeWrapper> streamNodeWrappers = Collections.synchronizedList(new ArrayList<>());
    final RequestDelegate<Void> streamNodesDelegate =
        new ParallelRequestDelegate<>(streamNodeEndpoints, NODE_START_TIMEOUT, dumpTestJVMSnapshot);

    for (int i = 0; i < streamNodesNum; i++) {
      final StreamNodeWrapper streamNodeWrapper =
          new StreamNodeWrapper(
              seedConfigNode,
              dataNodeEndpoints,
              testClassName,
              testMethodName,
              EnvUtils.searchAvailablePorts(),
              clusterIndex,
              startTime);
      streamNodeWrapper.createNodeDir();
      streamNodeWrapper.setWriteBackRetryForIT();
      streamNodeWrapper.changeConfig();
      streamNodeWrapper.createLogDir();
      streamNodeWrapper.setKillPoints(killPoints);
      streamNodeEndpoints.add(streamNodeWrapper.getIpAndPortString());
      streamNodeWrappers.add(streamNodeWrapper);
      nodeRegister.accept(streamNodeWrapper);
      streamNodesDelegate.addRequest(
          () -> {
            streamNodeWrapper.start();
            return null;
          });
    }
    try {
      streamNodesDelegate.requestAll();
    } catch (final SQLException e) {
      logger.error("Start streamNodes failed", e);
      throw new AssertionError();
    }
    return streamNodeWrappers;
  }
}
