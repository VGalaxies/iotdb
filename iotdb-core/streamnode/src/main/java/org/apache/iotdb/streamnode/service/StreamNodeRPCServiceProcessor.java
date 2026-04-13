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
import org.apache.iotdb.streamnode.rpc.thrift.IStreamNodeRPCService;
import org.apache.iotdb.streamnode.rpc.thrift.TCreateTaskOnStreamNodeReq;
import org.apache.iotdb.streamnode.rpc.thrift.TDropTaskOnStreamNodeReq;
import org.apache.iotdb.streamnode.rpc.thrift.TStartTaskOnStreamNodeReq;
import org.apache.iotdb.streamnode.rpc.thrift.TStopTaskOnStreamNodeReq;
import org.apache.iotdb.streamnode.rpc.thrift.TStreamNodeHeartbeatReq;
import org.apache.iotdb.streamnode.rpc.thrift.TStreamNodeHeartbeatResp;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;

public class StreamNodeRPCServiceProcessor implements IStreamNodeRPCService.Iface {

  private static final Logger LOGGER = LoggerFactory.getLogger(StreamNodeRPCServiceProcessor.class);

  @Override
  public TStreamNodeHeartbeatResp getHeartbeat(TStreamNodeHeartbeatReq req) throws TException {
    LOGGER.debug("Received heartbeat request, timestamp={}", req.getHeartbeatTimestamp());
    TStreamNodeHeartbeatResp resp = new TStreamNodeHeartbeatResp();
    resp.setHeartbeatTimestamp(req.getHeartbeatTimestamp());
    // TODO: collect running task heartbeats from StreamTaskManager
    resp.setRunningTasks(new ArrayList<>());
    return resp;
  }

  @Override
  public TSStatus createTask(TCreateTaskOnStreamNodeReq req) throws TException {
    LOGGER.info("Received createTask request, epoch={}", req.getEpoch());
    // TODO: deserialize StreamTask from req.getStreamTask() and delegate to StreamTaskManager
    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }

  @Override
  public TSStatus startTask(TStartTaskOnStreamNodeReq req) throws TException {
    LOGGER.info("Received startTask request: {}, epoch={}", req.getStreamTask(), req.getEpoch());
    // TODO: delegate to StreamTaskManager.startTask()
    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }

  @Override
  public TSStatus stopTask(TStopTaskOnStreamNodeReq req) throws TException {
    LOGGER.info("Received stopTask request: {}", req.getTaskName());
    // TODO: delegate to StreamTaskManager.stopTask()
    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }

  @Override
  public TSStatus dropTask(TDropTaskOnStreamNodeReq req) throws TException {
    LOGGER.info("Received dropTask request: {}", req.getTaskName());
    // TODO: delegate to StreamTaskManager.dropTask()
    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }

  @Override
  public TSStatus dropAllTasks() throws TException {
    LOGGER.info("Received dropAllTasks request");
    // TODO: delegate to StreamTaskManager.dropAllTasks()
    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }
}
