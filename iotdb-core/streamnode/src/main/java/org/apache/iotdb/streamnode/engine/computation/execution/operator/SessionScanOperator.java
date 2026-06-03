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

package org.apache.iotdb.streamnode.engine.computation.execution.operator;

import org.apache.iotdb.calc.execution.operator.AbstractOperator;
import org.apache.iotdb.streamnode.engine.computation.PlaceholderReplacer;
import org.apache.iotdb.streamnode.engine.computation.execution.StreamOperatorContext;
import org.apache.iotdb.streamnode.engine.window.IEventInfo;
import org.apache.iotdb.streamnode.service.StreamNodeSessionScanService;
import org.apache.iotdb.streamnode.utils.IEventRowsIterator;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

public class SessionScanOperator extends AbstractOperator implements EventAwareOperator {
  private static final Logger LOGGER = LoggerFactory.getLogger(SessionScanOperator.class);
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(SessionScanOperator.class);

  private final String sqlTemplate;
  private final PlaceholderReplacer placeholderReplacer = new PlaceholderReplacer();

  private IEventRowsIterator rowsIterator;

  public SessionScanOperator(StreamOperatorContext operatorContext, String sqlTemplate) {
    super.operatorContext = Objects.requireNonNull(operatorContext, "operatorContext is null");
    this.sqlTemplate = Objects.requireNonNull(sqlTemplate, "sqlTemplate is null");
  }

  @Override
  public void bindEventInfo(IEventInfo eventInfo) {
    rowsIterator =
        StreamNodeSessionScanService.getInstance()
            .getSessionScanRowsIterator(placeholderReplacer.replace(sqlTemplate, eventInfo));
  }

  @Override
  public TsBlock next() {
    if (rowsIterator == null || rowsIterator.isFinished()) {
      return null;
    }
    return rowsIterator.next();
  }

  @Override
  public boolean hasNext() {
    return !isFinished();
  }

  @Override
  public StreamOperatorContext getOperatorContext() {
    return (StreamOperatorContext) operatorContext;
  }

  @Override
  public ListenableFuture<?> isBlocked() {
    return rowsIterator == null ? NOT_BLOCKED : rowsIterator.isBlocked();
  }

  @Override
  public void close() {
    if (rowsIterator == null) {
      return;
    }
    try {
      rowsIterator.close();
    } catch (Exception e) {
      LOGGER.error("Failed to close SessionScanOperator", e);
    } finally {
      rowsIterator = null;
    }
  }

  @Override
  public boolean isFinished() {
    return rowsIterator != null && rowsIterator.isFinished();
  }

  @Override
  public long calculateMaxPeekMemory() {
    return calculateMaxReturnSize();
  }

  @Override
  public long calculateMaxReturnSize() {
    return maxReturnSize;
  }

  @Override
  public long calculateRetainedSizeAfterCallingNext() {
    return 0;
  }

  @Override
  public long ramBytesUsed() {
    return INSTANCE_SIZE;
  }
}
