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

import org.apache.iotdb.common.rpc.thrift.TStreamNodeLocation;
import org.apache.iotdb.commons.client.exception.ClientManagerException;
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.streamnode.client.ConfigNodeInfo;
import org.apache.iotdb.streamnode.client.StreamNodeClient;
import org.apache.iotdb.streamnode.client.StreamNodeClientManager;
import org.apache.iotdb.streamnode.conf.DirectoryChecker;
import org.apache.iotdb.streamnode.service.StreamNodeRPCService;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamNodeShutdownHook extends Thread {

  private static final Logger logger = LoggerFactory.getLogger(StreamNodeShutdownHook.class);

  private final TStreamNodeLocation nodeLocation;
  private Thread watcherThread;

  public StreamNodeShutdownHook(TStreamNodeLocation nodeLocation) {
    super(ThreadName.STREAMNODE_SHUTDOWN_HOOK.getName());
    this.nodeLocation = nodeLocation;
  }

  private void startWatcher() {
    Thread hookThread = Thread.currentThread();
    watcherThread =
        new Thread(
            () -> {
              while (!Thread.interrupted()) {
                try {
                  Thread.sleep(10000);
                  StackTraceElement[] stackTrace = hookThread.getStackTrace();
                  StringBuilder stackTraceBuilder =
                      new StringBuilder("Stack trace of shutdown hook:\n");
                  for (StackTraceElement traceElement : stackTrace) {
                    stackTraceBuilder.append(traceElement.toString()).append("\n");
                  }
                  logger.info(stackTraceBuilder.toString());
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                  return;
                }
              }
            },
            "ShutdownHookWatcher");
    watcherThread.setDaemon(true);
    watcherThread.start();
  }

  @Override
  public void run() {
    logger.info("StreamNode exiting...");

    startWatcher();
    // Stop rpc service firstly.
    StreamNodeRPCService.getInstance().stop();

    // Reject write operations to make sure all tsfiles will be sealed
    CommonDescriptor.getInstance().getConfig().setStopping(true);
    CommonDescriptor.getInstance().getConfig().setNodeStatus(NodeStatus.ReadOnly);

    // Actually stop all services started by the StreamNode.
    StreamNode.getInstance().stop();

    // Set and report shutdown to cluster ConfigNode-leader
    if (!reportShutdownToConfigNodeLeader()) {
      logger.warn(
          "Failed to report StreamNode's shutdown to ConfigNode. The cluster will still take the current StreamNode as Running for a few seconds.");
    }

    // Clear lock file. All services should be shutdown before this line.
    DirectoryChecker.getInstance().deregisterAll();

    watcherThread.interrupt();
  }

  private boolean reportShutdownToConfigNodeLeader() {
    try (StreamNodeClient client =
        StreamNodeClientManager.getInstance().borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
      return client.reportStreamNodeShutdown(nodeLocation).getCode()
          == TSStatusCode.SUCCESS_STATUS.getStatusCode();
    } catch (ClientManagerException e) {
      logger.error("Failed to borrow ConfigNodeClient", e);
    } catch (TException e) {
      logger.error("Failed to report shutdown", e);
    }
    return false;
  }
}
