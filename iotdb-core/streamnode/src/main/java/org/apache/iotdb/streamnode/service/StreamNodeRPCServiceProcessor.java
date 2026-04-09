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

package org.apache.iotdb.streamnode.service;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.streamnode.manager.StreamTaskManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Processes RPC requests from ConfigNode. Will implement IStreamNodeRPCService.Iface once Thrift
 * code is generated.
 */
public class StreamNodeRPCServiceProcessor {

  private static final Logger LOGGER = LoggerFactory.getLogger(StreamNodeRPCServiceProcessor.class);

  private final StreamTaskManager taskManager;

  public StreamNodeRPCServiceProcessor(StreamTaskManager taskManager) {
    this.taskManager = taskManager;
  }

  // TODO: implement IStreamNodeRPCService.Iface methods once Thrift is generated
  // For now, define the methods that map to the Thrift service

  public TSStatus createTask(byte[] streamTaskBytes, int epoch) {
    LOGGER.info("Received createTask request, epoch={}", epoch);
    // TODO: deserialize StreamTask from bytes
    // return taskManager.createTask(task, epoch);
    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }

  public TSStatus startTask(String taskName, int epoch) {
    LOGGER.info("Received startTask request: {}, epoch={}", taskName, epoch);
    return taskManager.startTask(taskName, epoch);
  }

  public TSStatus stopTask(String taskName) {
    LOGGER.info("Received stopTask request: {}", taskName);
    return taskManager.stopTask(taskName);
  }

  public TSStatus dropTask(String taskName) {
    LOGGER.info("Received dropTask request: {}", taskName);
    return taskManager.dropTask(taskName);
  }

  public TSStatus dropAllTasks() {
    LOGGER.info("Received dropAllTasks request");
    taskManager.dropAllTasks();
    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }
}
