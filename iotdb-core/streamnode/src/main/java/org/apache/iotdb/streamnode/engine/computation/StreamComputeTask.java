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

package org.apache.iotdb.streamnode.engine.computation;

import org.apache.iotdb.calc.execution.operator.CommonOperatorContext;
import org.apache.iotdb.calc.execution.operator.Operator;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.commons.utils.FileUtils;
import org.apache.iotdb.streamnode.engine.computation.execution.operator.EventAwareOperator;
import org.apache.iotdb.streamnode.engine.computation.planner.LocalStreamExecutionPlanContext;
import org.apache.iotdb.streamnode.engine.computation.planner.StreamNodeTableOperatorGenerator;
import org.apache.iotdb.streamnode.engine.window.IEventInfo;
import org.apache.iotdb.streamnode.metrics.StreamComputeMetrics;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.tsfile.read.common.block.TsBlock;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class StreamComputeTask implements IStreamComputeTask {

  private Operator operatorTree;
  private final StreamComputeTaskContext computeContext;
  private final PlanNode planNode;
  private final StreamNodeTableOperatorGenerator operatorGenerator;
  private final LocalStreamExecutionPlanContext planContext;
  private boolean operatorMetricsReported;

  public StreamComputeTask(
      PlanNode planNode,
      StreamNodeTableOperatorGenerator operatorGenerator,
      StreamComputeTaskContext computeContext,
      LocalStreamExecutionPlanContext planContext) {
    this.planNode = Objects.requireNonNull(planNode, "planNode should not be null");
    this.operatorGenerator =
        Objects.requireNonNull(operatorGenerator, "operatorGenerator should not be null");
    this.computeContext =
        Objects.requireNonNull(computeContext, "computeContext should not be null");
    this.planContext = Objects.requireNonNull(planContext, "planContext should not be null");
    this.operatorTree = rebuildOperatorTree();
  }

  @Override
  public void bindEventInfo(IEventInfo eventInfo) {
    Objects.requireNonNull(eventInfo, "eventInfo should not be null");
    computeContext.setCurrentEventInfo(eventInfo);
    for (EventAwareOperator eventAwareOperator : computeContext.getEventAwareOperators()) {
      eventAwareOperator.bindEventInfo(eventInfo);
    }
  }

  @Override
  public void reset() {
    if (planNode == null) {
      return;
    }
    closeOperatorTree();
    cleanTmpFile();
    computeContext.reset();
    computeContext.getMemoryReservationManager().releaseAllReservedMemory();
    operatorTree = rebuildOperatorTree();
  }

  private void cleanTmpFile() {
    if (!computeContext.mayHaveTmpFile()) {
      return;
    }
    FileUtils.deleteFileOrDirectory(
        new File(
            StreamNodeTableOperatorGenerator.getComputeTmpDir(
                planContext.getDriverContext(), computeContext.isRecomputeTask())),
        true);
  }

  private void closeOperatorTree() {
    try {
      operatorTree.close();
    } catch (Exception e) {
      throw new RuntimeException("Failed to close operator tree before reset", e);
    }
  }

  private Operator rebuildOperatorTree() {
    return planNode.accept(operatorGenerator, planContext);
  }

  @Override
  public TsBlock compute() throws Exception {
    long startTime = System.nanoTime();
    try {
      if (!operatorTree.hasNextWithTimer()) {
        return null;
      }
      return operatorTree.nextWithTimer();
    } finally {
      StreamComputeMetrics.getInstance()
          .recordExecutionTime(computeContext.isRecomputeTask(), System.nanoTime() - startTime);
    }
  }

  @Override
  public ListenableFuture<?> isBlocked() {
    return operatorTree.isBlocked();
  }

  @Override
  public boolean isFinished() throws Exception {
    return operatorTree.isFinished();
  }

  public Operator getOperatorTree() {
    return operatorTree;
  }

  @Override
  public StreamComputeTaskContext getStreamComputeTaskContext() {
    return computeContext;
  }

  @Override
  public void close() throws Exception {
    try {
      operatorTree.close();
      recordOperatorMetricsOnce();
    } finally {
      computeContext.getMemoryReservationManager().releaseAllReservedMemory();
      cleanTmpFile();
    }
  }

  private void recordOperatorMetricsOnce() {
    if (operatorMetricsReported) {
      return;
    }
    operatorMetricsReported = true;

    Map<String, long[]> operatorTypeToMetrics = new HashMap<>();
    for (CommonOperatorContext operatorContext : operatorGenerator.getOperatorContexts()) {
      if (operatorContext.getTotalExecutionTimeInNanos() == 0
          && operatorContext.getNextCalledCount() == 0) {
        continue;
      }
      long[] metrics =
          operatorTypeToMetrics.computeIfAbsent(
              operatorContext.getOperatorType(), key -> new long[2]);
      metrics[0] += operatorContext.getTotalExecutionTimeInNanos();
      metrics[1] += operatorContext.getNextCalledCount();
    }

    for (Map.Entry<String, long[]> entry : operatorTypeToMetrics.entrySet()) {
      StreamComputeMetrics.getInstance()
          .recordOperatorExecutionCost(
              entry.getKey(), computeContext.isRecomputeTask(), entry.getValue()[0]);
      StreamComputeMetrics.getInstance()
          .recordOperatorExecutionCount(
              entry.getKey(), computeContext.isRecomputeTask(), entry.getValue()[1]);
    }
  }
}
