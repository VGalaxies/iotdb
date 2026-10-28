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

package org.apache.iotdb.confignode.client.async.handlers.rpc;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.common.rpc.thrift.TStreamNodeLocation;
import org.apache.iotdb.commons.client.request.AsyncRequestRPCHandler;
import org.apache.iotdb.commons.log.LoggerPeriodicalLogReducer;
import org.apache.iotdb.confignode.client.async.CnToSnAsyncRequestType;
import org.apache.iotdb.confignode.i18n.ConfigNodeMessages;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.ConnectException;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

/** General RPC handler for TSStatus response type on StreamNodes. */
public class StreamNodeTSStatusRPCHandler
    extends AsyncRequestRPCHandler<TSStatus, CnToSnAsyncRequestType, TStreamNodeLocation> {

  private static final Logger LOGGER = LoggerFactory.getLogger(StreamNodeTSStatusRPCHandler.class);

  public StreamNodeTSStatusRPCHandler(
      CnToSnAsyncRequestType requestType,
      int requestId,
      TStreamNodeLocation targetStreamNode,
      Map<Integer, TStreamNodeLocation> streamNodeLocationMap,
      Map<Integer, TSStatus> responseMap,
      CountDownLatch countDownLatch) {
    super(
        requestType,
        requestId,
        targetStreamNode,
        streamNodeLocationMap,
        responseMap,
        countDownLatch);
  }

  @Override
  protected String generateFormattedTargetLocation(TStreamNodeLocation streamNodeLocation) {
    return "{id="
        + streamNodeLocation.getStreamNodeId()
        + ", internalEndPoint="
        + streamNodeLocation.getInternalEndPoint()
        + "}";
  }

  @Override
  public void onComplete(TSStatus response) {
    responseMap.put(requestId, response);

    if (response.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      nodeLocationMap.remove(requestId);
      LOGGER.info(
          ConfigNodeMessages.SUCCESSFULLY_ON_STREAMNODE, requestType, formattedTargetLocation);
    } else {
      logFailure(
          ConfigNodeMessages.FAILED_TO_ON_STREAMNODE_RESPONSE,
          requestType,
          formattedTargetLocation,
          response);
    }

    countDownLatch.countDown();
  }

  @Override
  public void onError(Exception e) {
    String errorMsg =
        String.format(
            ConfigNodeMessages.FAILED_TO_ON_STREAMNODE_EXCEPTION,
            requestType,
            formattedTargetLocation,
            e.getMessage());
    logFailure(errorMsg);
    // the SN throw Exception -> TApplicationException
    // the SN crash -> TTransportException or ConnectException
    int code =
        e instanceof TTransportException || e instanceof ConnectException
            ? TSStatusCode.CAN_NOT_CONNECT_DATANODE.getStatusCode()
            : TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode();

    responseMap.put(requestId, new TSStatus(RpcUtils.getStatus(code, errorMsg)));

    countDownLatch.countDown();
  }

  private void logFailure(final String format, final Object... args) {
    if (!LoggerPeriodicalLogReducer.shouldLog(format, args)) {
      return;
    }
    LOGGER.error(format, args);
  }
}
