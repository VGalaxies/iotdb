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

package org.apache.iotdb.it.env.cluster.env;

import org.apache.iotdb.it.env.cluster.node.ConfigNodeWrapper;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.env.cluster.node.StreamNodeStarter;
import org.apache.iotdb.it.framework.IoTDBTestLogger;

import org.slf4j.Logger;

import java.util.List;
import java.util.stream.Collectors;

public class StreamEnv extends AbstractEnv {
  private static final Logger logger = IoTDBTestLogger.logger;
  private final int streamNodesNum;

  public StreamEnv(int streamNodesNum) {
    super();
    this.streamNodesNum = streamNodesNum;
  }

  @Override
  public void initClusterEnvironment() {
    initClusterEnvironment(1, 1);
  }

  @Override
  public void initClusterEnvironment(int configNodesNum, int dataNodesNum) {
    super.initEnvironment(configNodesNum, dataNodesNum, 30);
  }

  @Override
  public void initClusterEnvironment(
      int configNodesNum, int dataNodesNum, int testWorkingRetryCount) {
    super.initEnvironment(configNodesNum, dataNodesNum, testWorkingRetryCount);
  }

  @Override
  protected void initExtraNodes(
      final List<ConfigNodeWrapper> configNodeWrappers,
      final List<DataNodeWrapper> dataNodeWrappers,
      final String testClassName) {
    if (streamNodesNum > 0) {
      final String dataNodeEndpoints =
          dataNodeWrappers.stream()
              .map(DataNodeWrapper::getIpAndPortString)
              .collect(Collectors.joining(","));
      StreamNodeStarter.startStreamNode(
          streamNodesNum,
          configNodeWrappers.get(0).getIpAndPortString(),
          dataNodeEndpoints,
          testClassName,
          testMethodName,
          index,
          startTime,
          extraNodeKillPoints,
          this::registerExtraNode,
          this::dumpTestJVMSnapshot);
    }
  }
}
