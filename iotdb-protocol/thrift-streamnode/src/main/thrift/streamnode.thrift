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

namespace java org.apache.iotdb.streamnode.rpc.thrift

include "common.thrift"

struct TStreamNodeHeartbeatReq {
  1: required i64 heartbeatTimestamp
  2: required i64 cnStartTime
}

struct TTaskHeartbeat {
  1: required string taskName
  2: required i32 epoch
}

struct TStreamNodeHeartbeatResp {
  1: required i64 heartbeatTimestamp
  2: required list<TTaskHeartbeat> runningTasks
}

struct TCreateTaskOnStreamNodeReq {
  1: required binary streamTask
  2: required i32 epoch
}

struct TStartTaskOnStreamNodeReq {
  1: required binary streamTask
  2: required i32 epoch
  3: required i64 cnStartTime
}

struct TStopTaskOnStreamNodeReq {
  1: required string taskName
}

struct TDropTaskOnStreamNodeReq {
  1: required string taskName
}

service IStreamNodeRPCService {
  TStreamNodeHeartbeatResp getHeartbeat(1: TStreamNodeHeartbeatReq req)

  common.TSStatus createTask(1: TCreateTaskOnStreamNodeReq req)

  common.TSStatus startTask(1: TStartTaskOnStreamNodeReq req)

  common.TSStatus stopTask(1: TStopTaskOnStreamNodeReq req)

  common.TSStatus dropTask(1: TDropTaskOnStreamNodeReq req)

  common.TSStatus dropAllTasks()
}
