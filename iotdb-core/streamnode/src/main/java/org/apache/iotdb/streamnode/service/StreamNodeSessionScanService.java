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

import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.exception.StartupException;
import org.apache.iotdb.commons.service.IService;
import org.apache.iotdb.commons.service.ServiceType;
import org.apache.iotdb.isession.ITableSession;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.isession.pool.ITableSessionPool;
import org.apache.iotdb.session.pool.TableSessionPoolBuilder;
import org.apache.iotdb.streamnode.conf.StreamNodeConfig;
import org.apache.iotdb.streamnode.conf.StreamNodeDescriptor;
import org.apache.iotdb.streamnode.utils.IEventRowsIterator;
import org.apache.iotdb.streamnode.utils.NonMemoryControlStreamTsBlockQueue;
import org.apache.iotdb.streamnode.utils.StreamTsBlockQueue;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.DateUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

public class StreamNodeSessionScanService implements IService {
  private static final Logger LOGGER = LoggerFactory.getLogger(StreamNodeSessionScanService.class);

  private static final StreamNodeConfig config = StreamNodeDescriptor.getInstance().getConfig();
  private static final StreamNodeSessionScanService INSTANCE = new StreamNodeSessionScanService();
  private static final int MAX_ROWS_PER_TS_BLOCK = 1024;

  private ITableSessionPool tableSessionPool;
  private ExecutorService sessionScanExecutor;

  private StreamNodeSessionScanService() {}

  public static StreamNodeSessionScanService getInstance() {
    return INSTANCE;
  }

  @Override
  public synchronized void start() throws StartupException {
    if (tableSessionPool != null || sessionScanExecutor != null) {
      return;
    }
    tableSessionPool =
        new TableSessionPoolBuilder()
            .nodeUrls(config.getSnClusterIngressNodeUrls())
            .user(config.getSnClusterIngressUsername())
            .password(config.getSnClusterIngressPassword())
            .queryTimeoutInMs(0)
            .maxSize(config.getSessionScanConcurrency())
            .build();
    sessionScanExecutor =
        IoTDBThreadPoolFactory.newFixedThreadPool(
            config.getSessionScanConcurrency(), "StreamNode-SessionScan");
  }

  @Override
  public void stop() {
    waitAndStop(0);
  }

  @Override
  public synchronized void waitAndStop(long milliseconds) {
    if (sessionScanExecutor != null) {
      sessionScanExecutor.shutdownNow();
      if (milliseconds > 0) {
        try {
          sessionScanExecutor.awaitTermination(milliseconds, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          LOGGER.warn("Interrupted while stopping session scan executor", e);
        }
      }
      sessionScanExecutor = null;
    }
    if (tableSessionPool != null) {
      tableSessionPool.close();
      tableSessionPool = null;
    }
  }

  @Override
  public ServiceType getID() {
    return ServiceType.STREAM_NODE_SESSION_SCAN_SERVICE;
  }

  public IEventRowsIterator getSessionScanRowsIterator(String sql) {
    return submitSessionScan(sql).iterator();
  }

  public synchronized StreamTsBlockQueue submitSessionScan(String sql) {
    StreamTsBlockQueue tsBlockQueue = new NonMemoryControlStreamTsBlockQueue();
    if (sessionScanExecutor == null) {
      throw new IllegalStateException("StreamNode session scan service is not started");
    }
    sessionScanExecutor.submit(() -> executeQuery(sql, tsBlockQueue));
    return tsBlockQueue;
  }

  private synchronized ITableSessionPool getTableSessionPool() {
    if (tableSessionPool == null) {
      throw new IllegalStateException("StreamNode session scan service is not started");
    }
    return tableSessionPool;
  }

  private void executeQuery(String sql, StreamTsBlockQueue tsBlockQueue) {
    ITableSession session = null;
    SessionDataSet dataSet = null;
    try {
      if (tsBlockQueue.isFinished()) {
        return;
      }
      session = getTableSessionPool().getSession();
      if (tsBlockQueue.isFinished()) {
        return;
      }
      dataSet = session.executeQueryStatement(sql);
      SessionDataSet.DataIterator dataIterator = dataSet.iterator();
      List<TSDataType> dataTypes = parseDataTypes(dataSet.getColumnTypes());
      while (!tsBlockQueue.isFinished()) {
        TsBlock tsBlock = buildNextTsBlock(dataIterator, dataTypes);
        if (tsBlock == null) {
          break;
        }
        waitForFuture(tsBlockQueue.add(tsBlock));
      }
      tsBlockQueue.setNoMoreTsBlocks();
    } catch (Exception e) {
      if (!tsBlockQueue.isFinished()) {
        tsBlockQueue.abort(new RuntimeException("Failed to execute session scan SQL: " + sql, e));
      }
    } finally {
      closeSession(dataSet, session);
    }
  }

  private void waitForFuture(ListenableFuture<?> future) throws Exception {
    if (!future.isDone()) {
      future.get();
    }
  }

  private TsBlock buildNextTsBlock(
      SessionDataSet.DataIterator dataIterator, List<TSDataType> dataTypes) throws Exception {
    TsBlockBuilder builder = new TsBlockBuilder(dataTypes);
    builder.setMaxTsBlockLineNumber(MAX_ROWS_PER_TS_BLOCK);
    while (!builder.isFull() && dataIterator.next()) {
      appendCurrentRow(builder, dataIterator, dataTypes);
    }
    return builder.isEmpty() ? null : builder.build();
  }

  private void appendCurrentRow(
      TsBlockBuilder builder, SessionDataSet.DataIterator dataIterator, List<TSDataType> dataTypes)
      throws Exception {
    builder.getTimeColumnBuilder().writeLong(0L);
    for (int i = 0; i < dataTypes.size(); i++) {
      int columnIndex = i + 1;
      if (dataIterator.isNull(columnIndex)) {
        builder.getColumnBuilder(i).appendNull();
      } else {
        writeValue(builder, dataIterator, dataTypes, i, columnIndex);
      }
    }
    builder.declarePosition();
  }

  private void writeValue(
      TsBlockBuilder builder,
      SessionDataSet.DataIterator dataIterator,
      List<TSDataType> dataTypes,
      int column,
      int dataSetColumn)
      throws Exception {
    switch (dataTypes.get(column)) {
      case BOOLEAN:
        builder.getColumnBuilder(column).writeBoolean(dataIterator.getBoolean(dataSetColumn));
        break;
      case INT32:
        builder.getColumnBuilder(column).writeInt(dataIterator.getInt(dataSetColumn));
        break;
      case DATE:
        builder
            .getColumnBuilder(column)
            .writeInt(DateUtils.parseDateExpressionToInt(dataIterator.getDate(dataSetColumn)));
        break;
      case INT64:
      case TIMESTAMP:
        builder.getColumnBuilder(column).writeLong(dataIterator.getLong(dataSetColumn));
        break;
      case FLOAT:
        builder.getColumnBuilder(column).writeFloat(dataIterator.getFloat(dataSetColumn));
        break;
      case DOUBLE:
        builder.getColumnBuilder(column).writeDouble(dataIterator.getDouble(dataSetColumn));
        break;
      case TEXT:
      case STRING:
        builder
            .getColumnBuilder(column)
            .writeBinary(
                new Binary(dataIterator.getString(dataSetColumn), TSFileConfig.STRING_CHARSET));
        break;
      case BLOB:
        builder.getColumnBuilder(column).writeBinary(dataIterator.getBlob(dataSetColumn));
        break;
      default:
        throw new UnsupportedOperationException(
            "Unsupported session scan result type: " + dataTypes.get(column));
    }
  }

  private void closeSession(SessionDataSet dataSet, ITableSession session) {
    if (dataSet != null) {
      try {
        dataSet.close();
      } catch (Exception e) {
        LOGGER.error("Failed to close session data set", e);
      }
    }
    if (session != null) {
      try {
        session.close();
      } catch (Exception e) {
        LOGGER.error("Failed to close session", e);
      }
    }
  }

  private static List<TSDataType> parseDataTypes(List<String> columnTypes) {
    List<TSDataType> dataTypes = new ArrayList<>(columnTypes.size());
    for (String columnType : columnTypes) {
      dataTypes.add(TSDataType.valueOf(columnType.toUpperCase(Locale.ENGLISH)));
    }
    return dataTypes;
  }
}
