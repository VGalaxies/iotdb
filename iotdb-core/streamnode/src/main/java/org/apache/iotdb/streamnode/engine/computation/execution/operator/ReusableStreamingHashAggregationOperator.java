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

import org.apache.iotdb.calc.execution.operator.CommonOperatorContext;
import org.apache.iotdb.calc.execution.operator.Operator;
import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.grouped.GroupedAggregator;
import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.grouped.StreamingHashAggregationOperator;
import org.apache.iotdb.calc.utils.datastructure.SortKey;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.AggregationNode;
import org.apache.iotdb.streamnode.engine.computation.execution.StreamOperatorContext;

import org.apache.tsfile.read.common.type.Type;

import java.util.Comparator;
import java.util.List;

public class ReusableStreamingHashAggregationOperator extends StreamingHashAggregationOperator {

  private final List<Type> unPreGroupedTypes;
  private final List<Integer> unPreGroupedChannels;
  private final List<GroupedAggregator> aggregators;
  private final AggregationNode.Step step;
  private final int expectedGroups;
  private final long maxPartialMemory;

  public ReusableStreamingHashAggregationOperator(
      CommonOperatorContext operatorContext,
      Operator child,
      List<Integer> preGroupedChannels,
      List<Integer> preGroupedIndexInResult,
      List<Type> unPreGroupedTypes,
      List<Integer> unPreGroupedChannels,
      List<Integer> unPreGroupedIndexInResult,
      Comparator<SortKey> groupKeyComparator,
      List<GroupedAggregator> aggregators,
      AggregationNode.Step step,
      int expectedGroups,
      long maxPartialMemory,
      boolean spillEnabled,
      long unSpillMemoryLimit) {
    super(
        operatorContext,
        child,
        preGroupedChannels,
        preGroupedIndexInResult,
        unPreGroupedTypes,
        unPreGroupedChannels,
        unPreGroupedIndexInResult,
        groupKeyComparator,
        aggregators,
        step,
        expectedGroups,
        maxPartialMemory,
        spillEnabled,
        unSpillMemoryLimit);
    this.unPreGroupedTypes = unPreGroupedTypes;
    this.unPreGroupedChannels = unPreGroupedChannels;
    this.aggregators = aggregators;
    this.step = step;
    this.expectedGroups = expectedGroups;
    this.maxPartialMemory = maxPartialMemory;
  }

  public ReusableStreamingHashAggregationOperator(
      ReusableStreamingHashAggregationOperator streamingHashAggregationOperator, Operator child) {
    super(
        streamingHashAggregationOperator,
        child,
        streamingHashAggregationOperator.unPreGroupedTypes,
        streamingHashAggregationOperator.unPreGroupedChannels,
        streamingHashAggregationOperator.aggregators,
        streamingHashAggregationOperator.step,
        streamingHashAggregationOperator.expectedGroups,
        streamingHashAggregationOperator.maxPartialMemory);
    this.unPreGroupedTypes = streamingHashAggregationOperator.unPreGroupedTypes;
    this.unPreGroupedChannels = streamingHashAggregationOperator.unPreGroupedChannels;
    this.aggregators = streamingHashAggregationOperator.aggregators;
    this.step = streamingHashAggregationOperator.step;
    this.expectedGroups = streamingHashAggregationOperator.expectedGroups;
    this.maxPartialMemory = streamingHashAggregationOperator.maxPartialMemory;
  }

  @Override
  public void close() throws Exception {
    if (((StreamOperatorContext) getOperatorContext()).getDriverContext().isTaskClosing()) {
      super.close();
      return;
    }
    closeAggregationBuilder();
    child.close();
  }
}
