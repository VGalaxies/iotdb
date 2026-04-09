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

import org.apache.iotdb.streamnode.conf.StreamNodeConfig;
import org.apache.iotdb.streamnode.conf.StreamNodeDescriptor;
import org.apache.iotdb.streamnode.manager.StreamTaskManager;
import org.apache.iotdb.streamnode.service.StreamNodeRPCService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamNode {

  private static final Logger LOGGER = LoggerFactory.getLogger(StreamNode.class);

  private final StreamNodeConfig config;
  private final StreamTaskManager taskManager;
  private StreamNodeRPCService rpcService;

  private StreamNode() {
    this.config = StreamNodeDescriptor.getInstance().getConfig();
    this.taskManager = new StreamTaskManager();
  }

  public void start() throws Exception {
    LOGGER.info("Starting StreamNode...");

    // Step 1: Register with ConfigNode
    // TODO: implement registration RPC
    LOGGER.info("StreamNode registered with ConfigNode");

    // Step 2: Start RPC service
    rpcService = new StreamNodeRPCService(taskManager);
    // TODO: rpcService.start();
    LOGGER.info("StreamNode RPC service started on port {}", config.getInternalPort());

    LOGGER.info("StreamNode started successfully");
  }

  public void stop() {
    LOGGER.info("Stopping StreamNode...");

    // Step 1: Stop all running tasks
    taskManager.dropAllTasks();

    // Step 2: Stop RPC service
    if (rpcService != null) {
      // TODO: rpcService.stop();
    }

    LOGGER.info("StreamNode stopped");
  }

  public StreamTaskManager getTaskManager() {
    return taskManager;
  }

  public StreamNodeConfig getConfig() {
    return config;
  }

  public static void main(String[] args) {
    StreamNode streamNode = new StreamNode();
    try {
      streamNode.start();
      LOGGER.info("StreamNode is running. Press Ctrl+C to stop.");
      Runtime.getRuntime().addShutdownHook(new Thread(streamNode::stop));
      // Keep the main thread alive
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
