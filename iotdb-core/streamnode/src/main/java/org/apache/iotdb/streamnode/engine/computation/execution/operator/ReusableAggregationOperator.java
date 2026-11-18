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

import org.apache.iotdb.calc.execution.operator.Operator;
import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.AggregationOperator;
import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.TableAggregator;
import org.apache.iotdb.calc.plan.planner.CommonOperatorUtils;
import org.apache.iotdb.commons.stream.PartitionKey;
import org.apache.iotdb.streamnode.engine.computation.StreamComputeTaskContext;
import org.apache.iotdb.streamnode.engine.computation.execution.StreamOperatorContext;
import org.apache.iotdb.streamnode.engine.reuse.SharedBaseSlice;
import org.apache.iotdb.streamnode.engine.reuse.SharedSliceManager;
import org.apache.iotdb.streamnode.engine.reuse.SharedSliceManager.ConsumedSliceSet;
import org.apache.iotdb.streamnode.engine.window.IEventInfo;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.column.RunLengthEncodedColumn;

import javax.annotation.Nullable;

import java.util.Objects;
import java.util.Optional;

public class ReusableAggregationOperator extends AggregationOperator {

  public static final class SliceReuseBinding {
    private final SharedSliceManager manager;
    private final String taskName;
    private final StreamComputeTaskContext computeContext;
    private final @Nullable PartitionKey partitionKey;

    public SliceReuseBinding(
        SharedSliceManager manager,
        String taskName,
        StreamComputeTaskContext computeContext,
        @Nullable PartitionKey partitionKey) {
      this.manager = Objects.requireNonNull(manager);
      this.taskName = Objects.requireNonNull(taskName);
      this.computeContext = Objects.requireNonNull(computeContext);
      this.partitionKey = partitionKey;
    }
  }

  private final @Nullable SliceReuseBinding sliceReuseBinding;
  private boolean sliceCheckDone = false;

  public ReusableAggregationOperator(AggregationOperator aggregationOperator, Operator child) {
    this(
        aggregationOperator,
        child,
        aggregationOperator instanceof ReusableAggregationOperator
            ? ((ReusableAggregationOperator) aggregationOperator).sliceReuseBinding
            : null);
  }

  public ReusableAggregationOperator(
      AggregationOperator aggregationOperator,
      Operator child,
      @Nullable SliceReuseBinding sliceReuseBinding) {
    super(aggregationOperator, child);
    this.sliceReuseBinding = sliceReuseBinding;
  }

  @Override
  protected void resetForReuse() {
    super.resetForReuse();
    sliceCheckDone = false;
  }

  @Override
  public TsBlock next() throws Exception {
    if (finished) {
      return null;
    }
    IEventInfo eventInfo =
        sliceReuseBinding != null ? sliceReuseBinding.computeContext.getCurrentEventInfo() : null;
    if (sliceReuseBinding != null
        && !sliceCheckDone
        && eventInfo != null
        && eventInfo.getStartTime().isPresent()
        && eventInfo.getEndTime().isPresent()) {
      sliceCheckDone = true;
      long winStart = eventInfo.getStartTime().getAsLong();
      long winEnd = eventInfo.getEndTime().getAsLong();
      boolean isRecompute = sliceReuseBinding.computeContext.isRecomputeTask();
      Optional<ConsumedSliceSet> hit =
          sliceReuseBinding.manager.tryAcquireSubSlices(
              sliceReuseBinding.taskName,
              sliceReuseBinding.partitionKey,
              winStart,
              winEnd,
              isRecompute);
      if (hit.isPresent()) {
        ConsumedSliceSet set = hit.get();
        int[] mapping = set.getBinding().getConsumerToProducerAggIndex();
        long mergedRows = 0L;
        for (SharedBaseSlice subSlice : set.getSubSlices()) {
          mergedRows += subSlice.getRowCount();
          Column[] cols = subSlice.getIntermediateColumns();
          for (int i = 0; i < aggregators.size(); i++) {
            int prodIdx = mapping[i];
            if (prodIdx >= 0 && prodIdx < cols.length && cols[prodIdx] != null) {
              aggregators.get(i).getAccumulator().addIntermediate(cols[prodIdx]);
            }
          }
        }
        if (sliceReuseBinding.manager.isRegisteredProducer(sliceReuseBinding.taskName)) {
          Column[] intermediateCols = SharedSliceManager.evaluateIntermediateColumns(aggregators);
          sliceReuseBinding.manager.publishSlice(
              sliceReuseBinding.taskName,
              sliceReuseBinding.partitionKey,
              winStart,
              winEnd,
              mergedRows,
              isRecompute,
              intermediateCols);
        }
        ColumnBuilder[] resultColumnsBuilder = resultBuilder.getValueColumnBuilders();
        Column[] valueColumns = new Column[resultColumnsBuilder.length];
        for (int i = 0; i < aggregators.size(); i++) {
          TableAggregator aggregator = aggregators.get(i);
          aggregator.evaluate(resultColumnsBuilder[i]);
          valueColumns[i] = resultColumnsBuilder[i].build();
        }
        finished = true;
        return TsBlock.wrapBlocksWithoutCopy(
            1,
            new RunLengthEncodedColumn(CommonOperatorUtils.TIME_COLUMN_TEMPLATE, 1),
            valueColumns);
      }
    }

    TsBlock result = super.next();
    if (result != null
        && sliceReuseBinding != null
        && sliceReuseBinding.manager.isRegisteredProducer(sliceReuseBinding.taskName)
        && eventInfo != null
        && eventInfo.getStartTime().isPresent()
        && eventInfo.getEndTime().isPresent()) {
      Column[] intermediateCols = SharedSliceManager.evaluateIntermediateColumns(aggregators);
      sliceReuseBinding.manager.publishSlice(
          sliceReuseBinding.taskName,
          sliceReuseBinding.partitionKey,
          eventInfo.getStartTime().getAsLong(),
          eventInfo.getEndTime().getAsLong(),
          eventInfo.getRowCount().orElse(0L),
          sliceReuseBinding.computeContext.isRecomputeTask(),
          intermediateCols);
    }
    return result;
  }

  @Override
  public void close() throws Exception {
    if (((StreamOperatorContext) getOperatorContext()).getDriverContext().isTaskClosing()) {
      super.close();
      return;
    }
    child.close();
  }
}
