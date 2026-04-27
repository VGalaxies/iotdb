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

package org.apache.iotdb.confignode.client.sync;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.ClientPoolFactory;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.sync.SyncStreamNodeClient;
import org.apache.iotdb.commons.exception.UncheckedStartupException;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.streamnode.rpc.thrift.TCreateTaskOnStreamNodeReq;
import org.apache.iotdb.streamnode.rpc.thrift.TDropTaskOnStreamNodeReq;
import org.apache.iotdb.streamnode.rpc.thrift.TStartTaskOnStreamNodeReq;
import org.apache.iotdb.streamnode.rpc.thrift.TStopTaskOnStreamNodeReq;

import com.google.common.collect.ImmutableMap;
import org.apache.ratis.util.function.CheckedBiFunction;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class SyncStreamNodeClientPool {

  private static final Logger LOGGER = LoggerFactory.getLogger(SyncStreamNodeClientPool.class);

  private static final int DEFAULT_RETRY_NUM = 10;

  private final IClientManager<TEndPoint, SyncStreamNodeClient> clientManager;

  protected ImmutableMap<
          CnToSnSyncRequestType, CheckedBiFunction<Object, SyncStreamNodeClient, Object, Exception>>
      actionMap;

  private SyncStreamNodeClientPool() {
    clientManager =
        new IClientManager.Factory<TEndPoint, SyncStreamNodeClient>()
            .createClientManager(new ClientPoolFactory.SyncStreamNodeClientPoolFactory());
    buildActionMap();
    checkActionMapCompleteness();
  }

  private void buildActionMap() {
    ImmutableMap.Builder<
            CnToSnSyncRequestType,
            CheckedBiFunction<Object, SyncStreamNodeClient, Object, Exception>>
        actionMapBuilder = ImmutableMap.builder();
    actionMapBuilder.put(
        CnToSnSyncRequestType.CREATE_TASK,
        (req, client) -> client.createTask((TCreateTaskOnStreamNodeReq) req));
    actionMapBuilder.put(
        CnToSnSyncRequestType.START_TASK,
        (req, client) -> client.startTask((TStartTaskOnStreamNodeReq) req));
    actionMapBuilder.put(
        CnToSnSyncRequestType.STOP_TASK,
        (req, client) -> client.stopTask((TStopTaskOnStreamNodeReq) req));
    actionMapBuilder.put(
        CnToSnSyncRequestType.DROP_TASK,
        (req, client) -> client.dropTask((TDropTaskOnStreamNodeReq) req));
    actionMapBuilder.put(
        CnToSnSyncRequestType.DROP_ALL_TASKS, (req, client) -> client.dropAllTasks());
    actionMap = actionMapBuilder.build();
  }

  private void checkActionMapCompleteness() {
    List<CnToSnSyncRequestType> lackList =
        Arrays.stream(CnToSnSyncRequestType.values())
            .filter(type -> !actionMap.containsKey(type))
            .collect(Collectors.toList());
    if (!lackList.isEmpty()) {
      throw new UncheckedStartupException(
          String.format("These request types should be added to actionMap: %s", lackList));
    }
  }

  public Object sendSyncRequestToStreamNodeWithRetry(
      TEndPoint endPoint, Object req, CnToSnSyncRequestType requestType) {
    Throwable lastException = new TException();
    for (int retry = 0; retry < DEFAULT_RETRY_NUM; retry++) {
      try (SyncStreamNodeClient client = clientManager.borrowClient(endPoint)) {
        return executeSyncRequest(requestType, client, req);
      } catch (Exception e) {
        lastException = e;
        if (retry != DEFAULT_RETRY_NUM - 1) {
          LOGGER.warn(
              "{} failed on StreamNode {}, retrying {}...", requestType, endPoint, retry + 1);
          doRetryWait(retry);
        }
      }
    }
    LOGGER.error("{} failed on StreamNode {}", requestType, endPoint, lastException);
    return new TSStatus(TSStatusCode.INTERNAL_REQUEST_RETRY_ERROR.getStatusCode())
        .setMessage("All retry failed due to: " + lastException.getMessage());
  }

  public Object sendSyncRequestToStreamNodeWithGivenRetry(
      TEndPoint endPoint, Object req, CnToSnSyncRequestType requestType, int retryNum) {
    Throwable lastException = new TException();
    for (int retry = 0; retry < retryNum; retry++) {
      try (SyncStreamNodeClient client = clientManager.borrowClient(endPoint)) {
        return executeSyncRequest(requestType, client, req);
      } catch (Exception e) {
        lastException = e;
        if (retry != retryNum - 1) {
          LOGGER.warn(
              "{} failed on StreamNode {}, retrying {}...", requestType, endPoint, retry + 1);
          doRetryWait(retry);
        }
      }
    }
    LOGGER.error("{} failed on StreamNode {}", requestType, endPoint, lastException);
    return new TSStatus(TSStatusCode.INTERNAL_REQUEST_RETRY_ERROR.getStatusCode())
        .setMessage("All retry failed due to: " + lastException.getMessage());
  }

  private Object executeSyncRequest(
      CnToSnSyncRequestType requestType, SyncStreamNodeClient client, Object req) throws Exception {
    return Objects.requireNonNull(actionMap.get(requestType)).apply(req, client);
  }

  private void doRetryWait(int retryNum) {
    try {
      if (retryNum < 3) {
        TimeUnit.MILLISECONDS.sleep(800L);
      } else if (retryNum < 5) {
        TimeUnit.MILLISECONDS.sleep(100L * (long) Math.pow(2, retryNum));
      } else {
        TimeUnit.MILLISECONDS.sleep(3200L);
      }
    } catch (InterruptedException e) {
      LOGGER.warn("Retry wait failed.", e);
      Thread.currentThread().interrupt();
    }
  }

  private static class ClientPoolHolder {

    private static final SyncStreamNodeClientPool INSTANCE = new SyncStreamNodeClientPool();

    private ClientPoolHolder() {}
  }

  public static SyncStreamNodeClientPool getInstance() {
    return SyncStreamNodeClientPool.ClientPoolHolder.INSTANCE;
  }
}
