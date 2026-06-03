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

import org.apache.iotdb.calc.plan.planner.ITableOperatorGeneratorContext;
import org.apache.iotdb.calc.plan.planner.memory.MemoryReservationManager;
import org.apache.iotdb.commons.queryengine.common.SessionInfo;
import org.apache.iotdb.commons.stream.StreamNodeTableTypeProvider;
import org.apache.iotdb.streamnode.engine.scheduler.task.StreamDriverContext;
import org.apache.iotdb.streamnode.engine.task.StreamSubTaskContext;

import java.time.ZoneId;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

public class LocalStreamExecutionPlanContext implements ITableOperatorGeneratorContext {

  private final StreamNodeTableTypeProvider tableTypeProvider;
  private final ZoneId zoneId;
  private final SessionInfo sessionInfo;
  private final StreamSubTaskContext subTaskContext;
  private final AtomicInteger nextOperatorId = new AtomicInteger(0);
  private final StreamDriverContext streamDriverContext;

  public LocalStreamExecutionPlanContext(
      StreamNodeTableTypeProvider tableTypeProvider,
      ZoneId zoneId,
      SessionInfo sessionInfo,
      StreamSubTaskContext subTaskContext) {
    this.tableTypeProvider =
        Objects.requireNonNull(tableTypeProvider, "tableTypeProvider should not be null");
    this.zoneId = Objects.requireNonNull(zoneId, "zoneId should not be null");
    this.sessionInfo = Objects.requireNonNull(sessionInfo, "sessionInfo should not be null");
    this.subTaskContext =
        Objects.requireNonNull(subTaskContext, "subTaskContext should not be null");
    this.streamDriverContext = new StreamDriverContext(subTaskContext);
  }

  @Override
  public StreamNodeTableTypeProvider getTableTypeProvider() {
    return tableTypeProvider;
  }

  @Override
  public MemoryReservationManager getMemoryReservationManager() {
    return subTaskContext.getMemoryReservationManager();
  }

  @Override
  public ZoneId getZoneId() {
    return zoneId;
  }

  public SessionInfo getSessionInfo() {
    return sessionInfo;
  }

  public int getNextOperatorId() {
    return nextOperatorId.getAndIncrement();
  }

  public StreamDriverContext getDriverContext() {
    return streamDriverContext;
  }
}
