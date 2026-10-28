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

package org.apache.iotdb.confignode.client.async;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.common.rpc.thrift.TSetConfigurationReq;
import org.apache.iotdb.common.rpc.thrift.TStreamNodeLocation;
import org.apache.iotdb.commons.client.request.AsyncRequestContext;
import org.apache.iotdb.commons.client.request.AsyncRequestRPCHandler;
import org.apache.iotdb.commons.client.request.StreamNodeInternalServiceRequestManager;
import org.apache.iotdb.commons.exception.UncheckedStartupException;
import org.apache.iotdb.confignode.client.async.handlers.rpc.StreamNodeTSStatusRPCHandler;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.i18n.ConfigNodeMessages;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** Asynchronously send RPC requests to StreamNodes. */
public class CnToSnInternalServiceAsyncRequestManager
    extends StreamNodeInternalServiceRequestManager<CnToSnAsyncRequestType> {

  private CnToSnInternalServiceAsyncRequestManager() {
    super(ConfigNodeDescriptor.getInstance().getConf().getSelectorNumOfClientManager());
  }

  @SuppressWarnings("unchecked")
  @Override
  protected void initActionMapBuilder() {
    actionMapBuilder.put(
        CnToSnAsyncRequestType.SET_CONFIGURATION,
        (req, client, handler) ->
            client.setConfiguration(
                (TSetConfigurationReq) req, (StreamNodeTSStatusRPCHandler) handler));
    actionMapBuilder.put(
        CnToSnAsyncRequestType.LOAD_CONFIGURATION,
        (req, client, handler) -> client.loadConfiguration((StreamNodeTSStatusRPCHandler) handler));
    actionMapBuilder.put(
        CnToSnAsyncRequestType.STOP_STREAM_NODE,
        (req, client, handler) -> client.stopStreamNode((StreamNodeTSStatusRPCHandler) handler));
  }

  @SuppressWarnings("unchecked")
  @Override
  protected AsyncRequestRPCHandler<?, CnToSnAsyncRequestType, TStreamNodeLocation> buildHandler(
      AsyncRequestContext<?, ?, CnToSnAsyncRequestType, TStreamNodeLocation> requestContext,
      int requestId,
      TStreamNodeLocation targetNode) {
    return new StreamNodeTSStatusRPCHandler(
        requestContext.getRequestType(),
        requestId,
        targetNode,
        requestContext.getNodeLocationMap(),
        (Map<Integer, TSStatus>) requestContext.getResponseMap(),
        requestContext.getCountDownLatch());
  }

  @Override
  protected void checkActionMapCompleteness() {
    List<CnToSnAsyncRequestType> lackList =
        Arrays.stream(CnToSnAsyncRequestType.values())
            .filter(type -> !actionMap.containsKey(type))
            .collect(Collectors.toList());
    if (!lackList.isEmpty()) {
      throw new UncheckedStartupException(
          String.format(
              ConfigNodeMessages.THESE_REQUEST_TYPES_SHOULD_BE_ADDED_TO_ACTIONMAP, lackList));
    }
  }

  private static class ClientPoolHolder {

    private static final CnToSnInternalServiceAsyncRequestManager INSTANCE =
        new CnToSnInternalServiceAsyncRequestManager();

    private ClientPoolHolder() {
      // Empty constructor
    }
  }

  public static CnToSnInternalServiceAsyncRequestManager getInstance() {
    return ClientPoolHolder.INSTANCE;
  }
}
