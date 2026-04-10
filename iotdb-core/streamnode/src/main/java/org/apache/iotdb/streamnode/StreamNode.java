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

import org.apache.iotdb.commons.service.RegisterManager;
import org.apache.iotdb.streamnode.conf.StreamNodeConfig;
import org.apache.iotdb.streamnode.conf.StreamNodeDescriptor;
import org.apache.iotdb.streamnode.service.StreamNodeRPCService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamNode {

  private static final Logger LOGGER = LoggerFactory.getLogger(StreamNode.class);

  private final StreamNodeConfig config = StreamNodeDescriptor.getInstance().getConfig();

  private final RegisterManager registerManager = new RegisterManager();

  private StreamNode() {}

  public void start() throws Exception {
    LOGGER.info("Starting StreamNode...");
    LOGGER.info(
        "StreamNode config: cluster_name={}, sn_internal_address={}, sn_internal_port={},"
            + " sn_seed_config_node={}",
        config.getClusterName(),
        config.getSnInternalAddress(),
        config.getSnInternalPort(),
        config.getSnSeedConfigNode());

    // Step 1: Register with ConfigNode
    // TODO: send registerStreamNode RPC to CN
    LOGGER.info("StreamNode registered with ConfigNode (stub)");

    // Step 2: Start RPC service to receive CN requests
    registerManager.register(StreamNodeRPCService.getInstance());
    LOGGER.info(
        "StreamNode RPC service listening on {}:{}",
        config.getSnInternalAddress(),
        config.getSnInternalPort());

    LOGGER.info("StreamNode started successfully");
  }

  public void stop() {
    LOGGER.info("Stopping StreamNode...");
    registerManager.deregisterAll();
    LOGGER.info("StreamNode stopped");
  }

  public static void main(String[] args) {
    StreamNode streamNode = StreamNodeHolder.INSTANCE;
    try {
      streamNode.start();
      LOGGER.info("StreamNode is running. Press Ctrl+C to stop.");
      Runtime.getRuntime().addShutdownHook(new Thread(streamNode::stop));
      Thread.currentThread().join();
    } catch (Exception e) {
      LOGGER.error("Failed to start StreamNode", e);
      System.exit(1);
    }
  }

  private static class StreamNodeHolder {
    private static final StreamNode INSTANCE = new StreamNode();
  }

  public static StreamNode getInstance() {
    return StreamNodeHolder.INSTANCE;
  }
}
