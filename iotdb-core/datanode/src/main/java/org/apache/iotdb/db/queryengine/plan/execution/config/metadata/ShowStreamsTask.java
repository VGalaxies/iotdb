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

package org.apache.iotdb.db.queryengine.plan.execution.config.metadata;

import org.apache.iotdb.common.rpc.thrift.TStreamInfo;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.QualifiedName;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.stream.ShowStreams;
import org.apache.iotdb.commons.queryengine.utils.TimestampPrecisionUtils;
import org.apache.iotdb.commons.schema.column.ColumnHeader;
import org.apache.iotdb.commons.schema.column.ColumnHeaderConstant;
import org.apache.iotdb.db.queryengine.common.header.DatasetHeader;
import org.apache.iotdb.db.queryengine.common.header.DatasetHeaderFactory;
import org.apache.iotdb.db.queryengine.plan.execution.config.ConfigTaskResult;
import org.apache.iotdb.db.queryengine.plan.execution.config.IConfigTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.executor.IConfigTaskExecutor;
import org.apache.iotdb.rpc.TSStatusCode;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.utils.BytesUtils;

import javax.annotation.Nullable;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class ShowStreamsTask implements IConfigTask {

  private final String streamName;

  public ShowStreamsTask(final ShowStreams node) {
    final QualifiedName name = node.getName();
    this.streamName = name == null ? null : name.toString();
  }

  @Override
  public ListenableFuture<ConfigTaskResult> execute(final IConfigTaskExecutor configTaskExecutor)
      throws InterruptedException {
    return configTaskExecutor.showStreams(streamName);
  }

  public static void buildTsBlock(
      final List<TStreamInfo> streamInfoList, final SettableFuture<ConfigTaskResult> future) {
    final List<TSDataType> outputDataTypes =
        ColumnHeaderConstant.showStreamColumnHeaders.stream()
            .map(ColumnHeader::getColumnType)
            .collect(Collectors.toList());
    final TsBlockBuilder builder = new TsBlockBuilder(outputDataTypes);

    for (final TStreamInfo info : streamInfoList) {
      builder.getTimeColumnBuilder().writeLong(0L);
      builder
          .getColumnBuilder(0)
          .writeBinary(BytesUtils.valueOf(valueOrEmpty(info.getStreamName())));
      builder
          .getColumnBuilder(1)
          .writeLong(convertMillisToCurrentPrecision(info.getCreationTime()));
      builder.getColumnBuilder(2).writeBinary(BytesUtils.valueOf(valueOrEmpty(info.getCreator())));
      builder.getColumnBuilder(3).writeBinary(BytesUtils.valueOf(valueOrEmpty(info.getSource())));
      builder
          .getColumnBuilder(4)
          .writeBinary(BytesUtils.valueOf(valueOrEmpty(info.getEventWindow())));
      builder.getColumnBuilder(5).writeBinary(BytesUtils.valueOf(valueOrEmpty(info.getSubQuery())));
      builder.getColumnBuilder(6).writeBinary(BytesUtils.valueOf(valueOrEmpty(info.getTarget())));
      builder.getColumnBuilder(7).writeBinary(BytesUtils.valueOf(valueOrEmpty(info.getStatus())));
      builder
          .getColumnBuilder(8)
          .writeBinary(BytesUtils.valueOf(valueOrEmpty(info.getRunningOn())));
      builder.getColumnBuilder(9).writeLong(convertMillisToCurrentPrecision(info.getLastUpTime()));
      builder
          .getColumnBuilder(10)
          .writeLong(convertMillisToCurrentPrecision(info.getLastDownTime()));
      builder
          .getColumnBuilder(11)
          .writeLong(convertMillisToCurrentPrecision(info.getLastHeartbeatTime()));
      builder
          .getColumnBuilder(12)
          .writeBinary(BytesUtils.valueOf(valueOrEmpty(info.getLastDownReason())));
      builder
          .getColumnBuilder(13)
          .writeBinary(BytesUtils.valueOf(valueOrEmpty(info.getProperties())));
      builder.declarePosition();
    }

    final DatasetHeader datasetHeader = DatasetHeaderFactory.getShowStreamHeader();
    future.set(new ConfigTaskResult(TSStatusCode.SUCCESS_STATUS, builder.build(), datasetHeader));
  }

  private static long convertMillisToCurrentPrecision(final long time) {
    return TimestampPrecisionUtils.convertToCurrPrecision(time, TimeUnit.MILLISECONDS);
  }

  private static String valueOrEmpty(@Nullable final String value) {
    return value == null ? "" : value;
  }
}
