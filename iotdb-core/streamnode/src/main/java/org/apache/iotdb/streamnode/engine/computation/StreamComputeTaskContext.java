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

import org.apache.iotdb.calc.plan.planner.memory.MemoryReservationManager;
import org.apache.iotdb.streamnode.engine.computation.execution.operator.EventAwareOperator;
import org.apache.iotdb.streamnode.engine.window.IEventInfo;

import java.util.ArrayList;
import java.util.List;

/**
 * Per-compute-task event context. Each {@link StreamComputeTask} owns its own instance so that the
 * event bound to one operator tree never leaks into another (e.g. {@code computedTask} and {@code
 * recomputedTask} each resolve placeholders and source operators against their own event).
 */
public class StreamComputeTaskContext {

  private final boolean isRecomputeTask;

  private MemoryReservationManager memoryReservationManager;

  private IEventInfo currentEventInfo;

  private final List<EventAwareOperator> eventAwareOperators = new ArrayList<>();

  private boolean mayHaveTmpFile = false;

  public StreamComputeTaskContext(boolean isRecomputeTask) {
    this.isRecomputeTask = isRecomputeTask;
  }

  public boolean isRecomputeTask() {
    return isRecomputeTask;
  }

  public MemoryReservationManager getMemoryReservationManager() {
    return memoryReservationManager;
  }

  public void setMemoryReservationManager(MemoryReservationManager memoryReservationManager) {
    this.memoryReservationManager = memoryReservationManager;
  }

  public void setHaveTmpFile(boolean mayHaveTmpFile) {
    this.mayHaveTmpFile = mayHaveTmpFile;
  }

  public boolean mayHaveTmpFile() {
    return mayHaveTmpFile;
  }

  public IEventInfo getCurrentEventInfo() {
    return currentEventInfo;
  }

  public void setCurrentEventInfo(IEventInfo currentEventInfo) {
    this.currentEventInfo = currentEventInfo;
  }

  public void reset() {
    currentEventInfo = null;
  }

  public void addEventAwareOperator(EventAwareOperator eventAwareOperator) {
    eventAwareOperators.add(eventAwareOperator);
  }

  public List<EventAwareOperator> getEventAwareOperators() {
    return eventAwareOperators;
  }
}
