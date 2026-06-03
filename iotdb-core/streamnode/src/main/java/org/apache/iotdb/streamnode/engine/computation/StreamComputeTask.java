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

import org.apache.iotdb.calc.execution.operator.Operator;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.streamnode.engine.computation.execution.operator.EventAwareOperator;
import org.apache.iotdb.streamnode.engine.computation.planner.LocalStreamExecutionPlanContext;
import org.apache.iotdb.streamnode.engine.computation.planner.StreamNodeTableOperatorGenerator;
import org.apache.iotdb.streamnode.engine.scheduler.task.StreamDriverContext;
import org.apache.iotdb.streamnode.engine.window.IEventInfo;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.tsfile.read.common.block.TsBlock;

import java.util.Objects;

public class StreamComputeTask implements IStreamComputeTask {

  private Operator operatorTree;
  private final StreamDriverContext driverContext;
  private final PlanNode planNode;
  private final StreamNodeTableOperatorGenerator operatorGenerator;
  private final LocalStreamExecutionPlanContext planContext;

  public StreamComputeTask(
      PlanNode planNode,
      StreamNodeTableOperatorGenerator operatorGenerator,
      LocalStreamExecutionPlanContext planContext) {
    this.planNode = Objects.requireNonNull(planNode, "planNode should not be null");
    this.operatorGenerator =
        Objects.requireNonNull(operatorGenerator, "operatorGenerator should not be null");
    this.planContext = Objects.requireNonNull(planContext, "planContext should not be null");
    this.driverContext = planContext.getDriverContext();
    this.operatorTree = rebuildOperatorTree();
  }

  @Override
  public void bindEventInfo(IEventInfo eventInfo) {
    Objects.requireNonNull(eventInfo, "eventInfo should not be null");
    for (EventAwareOperator eventAwareOperator : driverContext.getEventAwareOperators()) {
      eventAwareOperator.bindEventInfo(eventInfo);
    }
  }

  @Override
  public void reset() {
    if (planNode == null) {
      return;
    }
    closeOperatorTree();
    operatorTree = rebuildOperatorTree();
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
    if (!operatorTree.hasNextWithTimer()) {
      return null;
    }
    return operatorTree.nextWithTimer();
  }

  @Override
  public ListenableFuture<?> isBlocked() {
    return operatorTree.isBlocked();
  }

  @Override
  public boolean isFinished() throws Exception {
    return operatorTree.isFinished();
  }

  @Override
  public void close() throws Exception {
    operatorTree.close();
  }
}
