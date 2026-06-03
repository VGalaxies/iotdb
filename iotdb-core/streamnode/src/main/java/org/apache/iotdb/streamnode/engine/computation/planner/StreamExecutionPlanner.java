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

import org.apache.iotdb.commons.audit.UserEntity;
import org.apache.iotdb.commons.memory.IMemoryBlock;
import org.apache.iotdb.commons.memory.MemoryBlockType;
import org.apache.iotdb.commons.memory.MemoryManager;
import org.apache.iotdb.commons.queryengine.common.SessionInfo;
import org.apache.iotdb.commons.queryengine.common.SqlDialect;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.commons.stream.PartitionKey;
import org.apache.iotdb.commons.stream.StreamTask;
import org.apache.iotdb.streamnode.engine.computation.IStreamComputeTask;
import org.apache.iotdb.streamnode.engine.computation.StreamComputeTask;
import org.apache.iotdb.streamnode.engine.sink.IStreamSinkTask;
import org.apache.iotdb.streamnode.engine.task.StreamSubTask;
import org.apache.iotdb.streamnode.engine.task.StreamSubTaskContext;
import org.apache.iotdb.streamnode.engine.task.StreamTaskInstance;
import org.apache.iotdb.streamnode.engine.window.WindowEngine;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZoneId;

public class StreamExecutionPlanner {

  private static final Logger LOGGER = LoggerFactory.getLogger(StreamExecutionPlanner.class);
  private static final IMemoryBlock OPERATORS_MEMORY_BLOCK;

  static {
    MemoryManager memoryManager = getMemoryManager();
    OPERATORS_MEMORY_BLOCK = memoryManager.exactAllocate("Operators", MemoryBlockType.DYNAMIC);
  }

  private static MemoryManager getMemoryManager() {
    return new MemoryManager(0);
  }

  private final StreamMetadataImpl metadata = new StreamMetadataImpl();

  public static StreamExecutionPlanner getInstance() {
    return InstanceHolder.INSTANCE;
  }

  public StreamSubTask plan(StreamSubTaskContext subTaskContext, StreamTaskInstance taskInstance) {
    StreamTask taskDefinition = taskInstance.getTaskDefinition();
    PartitionKey partitionKey = subTaskContext.getPartitionKey();
    SessionInfo sessionInfo =
        new SessionInfo(
            0L, new UserEntity(-1, "", ""), ZoneId.systemDefault(), null, SqlDialect.TABLE);
    LocalStreamExecutionPlanContext context =
        new LocalStreamExecutionPlanContext(
            taskDefinition.getTypeProvider(), sessionInfo.getZoneId(), sessionInfo, subTaskContext);

    return new StreamSubTask(
        partitionKey,
        createStreamWindow(subTaskContext),
        createStreamComputeTask(taskDefinition, context),
        createSinkTask(taskDefinition, subTaskContext, taskInstance),
        taskDefinition.getTaskName(),
        subTaskContext,
        context.getDriverContext());
  }

  private WindowEngine createStreamWindow(StreamSubTaskContext subTaskContext) {
    boolean supportsStreamCompute = false;

    return null;
  }

  private IStreamComputeTask createStreamComputeTask(
      StreamTask taskDefinition, LocalStreamExecutionPlanContext context) {
    PlanNode planNode = PlanNodeType.deserialize(taskDefinition.getCalcPlan());
    return new StreamComputeTask(planNode, new StreamNodeTableOperatorGenerator(metadata), context);
  }

  private IStreamSinkTask createSinkTask(
      StreamTask taskDefinition,
      StreamSubTaskContext subTaskContext,
      StreamTaskInstance taskInstance) {
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
