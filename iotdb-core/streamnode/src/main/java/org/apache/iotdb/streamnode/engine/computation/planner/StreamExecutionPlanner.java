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

package org.apache.iotdb.streamnode.engine.computation.planner;

import org.apache.iotdb.calc.exception.MemoryNotEnoughException;
import org.apache.iotdb.calc.execution.operator.Operator;
import org.apache.iotdb.calc.plan.planner.memory.MemoryReservationManager;
import org.apache.iotdb.commons.memory.IMemoryBlock;
import org.apache.iotdb.commons.memory.MemoryBlockType;
import org.apache.iotdb.commons.queryengine.common.SessionInfo;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.commons.stream.StreamTask;
import org.apache.iotdb.commons.stream.StreamWindow;
import org.apache.iotdb.streamnode.conf.StreamNodeDescriptor;
import org.apache.iotdb.streamnode.engine.computation.StreamComputeTask;
import org.apache.iotdb.streamnode.engine.computation.StreamComputeTaskContext;
import org.apache.iotdb.streamnode.engine.computation.planner.memory.StreamNodePipelineMemoryEstimator;
import org.apache.iotdb.streamnode.engine.computation.planner.memory.ThreadSafeStreamNodeMemoryReservationManager;
import org.apache.iotdb.streamnode.engine.sink.IStreamSinkTask;
import org.apache.iotdb.streamnode.engine.task.AbstractStreamTaskInstance;
import org.apache.iotdb.streamnode.engine.task.StreamSubTask;
import org.apache.iotdb.streamnode.engine.task.StreamSubTaskContext;
import org.apache.iotdb.streamnode.engine.window.WindowEngine;
import org.apache.iotdb.streamnode.i18n.StreamNodeMessages;
import org.apache.iotdb.streamnode.metrics.StreamNodeMemoryMetrics;

import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

public class StreamExecutionPlanner {

  private static final Logger LOGGER = LoggerFactory.getLogger(StreamExecutionPlanner.class);
  private static final IMemoryBlock COMPUTE_OPERATORS_MEMORY_BLOCK;

  static {
    COMPUTE_OPERATORS_MEMORY_BLOCK =
        StreamNodeDescriptor.getInstance()
            .getMemoryConfig()
            .getComputeOperatorsMemoryManager()
            .exactAllocate("ComputeOperators", MemoryBlockType.DYNAMIC);
  }

  private final StreamMetadataImpl metadata = new StreamMetadataImpl();

  public void reserveFromFreeMemoryForCompute(
      final long memoryInBytes, final long reservedBytes, final String contextHolder)
      throws MemoryNotEnoughException {
    if (memoryInBytes <= 0) {
      throw new IllegalArgumentException(
          StreamNodeMessages
              .EXCEPTION_BYTES_TO_RESERVE_FROM_FREE_MEMORY_FOR_COMPUTE_SHOULD_BE_POSITIVE_3BAFFA26);
    }
    if (!COMPUTE_OPERATORS_MEMORY_BLOCK.allocate(memoryInBytes)) {
      throw new MemoryNotEnoughException(
          String.format(
              StreamNodeMessages
                  .EXCEPTION_THERE_IS_NOT_ENOUGH_MEMORY_FOR_STREAM_SUB_TASK_THE_CONTEXTHOLDER_IS_ARG_CURRENT_REMAINING_FREE_MEMORY_IS_ARG_B_ALREADY_RESERVED_MEMORY_FOR_THIS_CONTEXT_IN_TOTAL_IS_ARG_B_THE_MEMORY_REQUESTED_THIS_TIME_IS_ARG_B_B798DFBF,
              contextHolder,
              COMPUTE_OPERATORS_MEMORY_BLOCK.getFreeMemoryInBytes(),
              reservedBytes,
              memoryInBytes));
    }
  }

  public void releaseToFreeMemoryForCompute(final long memoryInBytes) {
    if (memoryInBytes <= 0) {
      throw new IllegalArgumentException(
          StreamNodeMessages
              .EXCEPTION_BYTES_TO_RELEASE_TO_FREE_MEMORY_FOR_COMPUTE_SHOULD_BE_POSITIVE_C3B5454F);
    }
    COMPUTE_OPERATORS_MEMORY_BLOCK.release(memoryInBytes);
  }

  public long getFreeMemoryForCompute() {
    return COMPUTE_OPERATORS_MEMORY_BLOCK.getFreeMemoryInBytes();
  }

  public static StreamExecutionPlanner getInstance() {
    return InstanceHolder.INSTANCE;
  }

  @SuppressWarnings("squid:S1181")
  public StreamSubTask plan(
      StreamSubTaskContext subTaskContext, AbstractStreamTaskInstance taskInstance)
      throws Exception {
    StreamTask taskDefinition = taskInstance.getTaskDefinition();
    PlanNode planNode = deserializePlanNodeTree(taskDefinition.getCalcPlan());
    SessionInfo sessionInfo = taskInstance.getSessionInfo();

    WindowEngine windowEngine = null;
    StreamComputeTask computeTask = null;
    StreamComputeTask recomputeTask = null;
    IStreamSinkTask sinkTask = null;
    try {
      windowEngine = createStreamWindow(taskDefinition, subTaskContext, taskInstance);
      computeTask = createComputeTask(planNode, taskDefinition, sessionInfo, subTaskContext, false);
      recomputeTask =
          createComputeTask(planNode, taskDefinition, sessionInfo, subTaskContext, true);
      reserveMemoryForComputeTasks(computeTask.getOperatorTree(), subTaskContext);
      sinkTask = createSinkTask(taskDefinition, subTaskContext, taskInstance);

      StreamSubTask subTask =
          new StreamSubTask(
              subTaskContext.getPartitionKey(),
              windowEngine,
              computeTask,
              recomputeTask,
              sinkTask,
              taskDefinition.getTaskName(),
              subTaskContext);

      return subTask;
    } catch (Throwable t) {
      subTaskContext.getDriverContext().markTaskClosing();
      if (recomputeTask != null) {
        try {
          recomputeTask.close();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          t.addSuppressed(e);
        } catch (Throwable closeFailure) {
          t.addSuppressed(closeFailure);
        }
      }
      if (computeTask != null) {
        try {
          computeTask.close();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          t.addSuppressed(e);
        } catch (Throwable closeFailure) {
          t.addSuppressed(closeFailure);
        }
      }
      if (sinkTask != null) {
        try {
          sinkTask.stop();
        } catch (Throwable closeFailure) {
          t.addSuppressed(closeFailure);
        }
      }
      if (windowEngine != null) {
        try {
          windowEngine.forceClose();
        } catch (Throwable closeFailure) {
          t.addSuppressed(closeFailure);
        }
      }
      throw t;
    }
  }

  private WindowEngine createStreamWindow(
      StreamTask taskDefinition,
      StreamSubTaskContext subTaskContext,
      AbstractStreamTaskInstance taskInstance) {
    StreamWindow window = taskDefinition.getWindow();
    if (window == null) {
      LOGGER.warn(
          "No window configuration found for stream task: {}", taskDefinition.getTaskName());
      return null;
    }
    return WindowEngine.create(
        window,
        subTaskContext.getDriverContext(),
        taskDefinition.getProperties(),
        taskInstance.getWindowTickScheduler());
  }

  private StreamComputeTask createComputeTask(
      PlanNode planNode,
      StreamTask taskDefinition,
      SessionInfo sessionInfo,
      StreamSubTaskContext subTaskContext,
      boolean isRecompute) {
    StreamComputeTaskContext computeContext = new StreamComputeTaskContext(isRecompute);
    computeContext.setMemoryReservationManager(
        createMemoryReservationManager(subTaskContext, isRecompute));
    LocalStreamExecutionPlanContext planContext =
        new LocalStreamExecutionPlanContext(
            taskDefinition.getTypeProvider(),
            sessionInfo.getZoneId(),
            sessionInfo,
            subTaskContext,
            computeContext.getMemoryReservationManager());
    return new StreamComputeTask(
        planNode,
        new StreamNodeTableOperatorGenerator(metadata, computeContext),
        computeContext,
        planContext);
  }

  private void reserveMemoryForComputeTasks(
      Operator operatorTree, StreamSubTaskContext subTaskContext) {
    StreamNodePipelineMemoryEstimator streamNodePipelineMemoryEstimator =
        new StreamNodePipelineMemoryEstimator(operatorTree, -1);
    long estimatedMemorySize = streamNodePipelineMemoryEstimator.getEstimatedMemoryUsageInBytes();
    StreamNodeMemoryMetrics.getInstance().updateEstimatedMemory(estimatedMemorySize);
    long totalEstimatedMemorySize = estimatedMemorySize * 2;
    if (totalEstimatedMemorySize <= 0) {
      return;
    }
    reserveFromFreeMemoryForCompute(
        totalEstimatedMemorySize,
        0,
        subTaskContext.getTaskName() + "-" + subTaskContext.getPartitionKey());
    try {
      subTaskContext
          .getStateMachine()
          .addStateChangeListener(
              newState -> {
                if (newState.isDone()) {
                  releaseToFreeMemoryForCompute(totalEstimatedMemorySize);
                }
              });
    } catch (Throwable t) {
      try {
        releaseToFreeMemoryForCompute(totalEstimatedMemorySize);
      } catch (Throwable releaseFailure) {
        t.addSuppressed(releaseFailure);
      }
      throw t;
    }
  }

  private MemoryReservationManager createMemoryReservationManager(
      StreamSubTaskContext subTaskContext, boolean isRecomputeTask) {
    return new ThreadSafeStreamNodeMemoryReservationManager(
        subTaskContext.getTaskName()
            + "-"
            + subTaskContext.getPartitionKey()
            + (isRecomputeTask ? "-recompute" : "-compute"));
  }

  private PlanNode deserializePlanNodeTree(ByteBuffer byteBuffer) {
    PlanNode root = PlanNodeType.deserialize(byteBuffer);
    int childrenCount = ReadWriteIOUtils.readInt(byteBuffer);
    for (int i = 0; i < childrenCount; i++) {
      root.addChild(deserializePlanNodeTree(byteBuffer));
    }
    return root;
  }

  private IStreamSinkTask createSinkTask(
      StreamTask taskDefinition,
      StreamSubTaskContext subTaskContext,
      AbstractStreamTaskInstance taskInstance) {
    if (taskDefinition.getTarget() == null) {
      return null;
    }
    return taskInstance.getWriteBackEngine().createSinkSubTask(subTaskContext);
  }

  private static class InstanceHolder {

    private InstanceHolder() {}

    private static final StreamExecutionPlanner INSTANCE = new StreamExecutionPlanner();
  }
}
