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

package org.apache.iotdb.confignode.procedure.impl.pipe;

public enum PipeTaskOperation {
  CREATE_PIPE,
  START_PIPE,
  STOP_PIPE,
  DROP_PIPE,
  HANDLE_LEADER_CHANGE,
  SYNC_PIPE_META,
  HANDLE_PIPE_META_CHANGE,
  PARSE_HEARTBEAT,
  AUTO_RESTART,
  SUCCESSFUL_RESTART;

  public static boolean isPipeTaskOperationActive(PipeTaskOperation op) {
    return op == CREATE_PIPE || op == START_PIPE || op == STOP_PIPE || op == DROP_PIPE;
  }
}
