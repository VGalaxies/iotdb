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

package org.apache.iotdb.streamnode.engine.computation.execution;

import org.apache.iotdb.calc.execution.operator.CommonOperatorContext;
import org.apache.iotdb.calc.plan.planner.memory.MemoryReservationManager;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.streamnode.engine.scheduler.task.StreamDriverContext;

public class StreamOperatorContext extends CommonOperatorContext {

  private StreamDriverContext driverContext;

  public StreamOperatorContext(
      int operatorId,
      PlanNodeId planNodeId,
      String operatorType,
      StreamDriverContext driverContext) {
    super(operatorId, planNodeId, operatorType);
    this.driverContext = driverContext;
  }

  @Override
  public MemoryReservationManager getMemoryReservationContext() {
    return driverContext.getSubTaskContext().getMemoryReservationManager();
  }

  @Override
  public int getFragmentId() {
    return 0;
  }

  @Override
  public int getPipelineId() {
    return driverContext.getPipelineId();
  }

  @Override
  public long ramBytesUsed() {
    return 0;
  }

  public StreamDriverContext getDriverContext() {
    return driverContext;
  }

  public void setDriverContext(StreamDriverContext driverContext) {
    this.driverContext = driverContext;
  }
}
