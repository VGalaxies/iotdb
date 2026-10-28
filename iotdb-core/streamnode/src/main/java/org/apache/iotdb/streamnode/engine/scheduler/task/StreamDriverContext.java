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

package org.apache.iotdb.streamnode.engine.scheduler.task;

import org.apache.iotdb.streamnode.engine.task.StreamSubTaskContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

public class StreamDriverContext {

  private static final Logger LOGGER = LoggerFactory.getLogger(StreamDriverContext.class);

  private DriverTaskId driverTaskId;

  private final AtomicBoolean finished = new AtomicBoolean();
  private final AtomicBoolean taskClosing = new AtomicBoolean();

  private final StreamSubTaskContext subTaskContext;

  public StreamDriverContext(StreamSubTaskContext subTaskContext) {
    this.driverTaskId =
        new DriverTaskId(subTaskContext.getTaskName(), subTaskContext.getPartitionKey());
    this.subTaskContext =
        Objects.requireNonNull(subTaskContext, "subTaskContext should not be null");
  }

  public DriverTaskId getDriverTaskId() {
    return driverTaskId;
  }

  public void setDriverTaskId(DriverTaskId driverTaskId) {
    this.driverTaskId = driverTaskId;
  }

  public int getPipelineId() {
    return 0;
  }

  public void failed(Throwable cause) {
    if (finished.compareAndSet(false, true)) {
      subTaskContext.failed(cause);
    }
  }

  public void finished() {
    finished.compareAndSet(false, true);
  }

  public boolean isDone() {
    return finished.get() || subTaskContext.isDone();
  }

  public void markTaskClosing() {
    taskClosing.compareAndSet(false, true);
  }

  public boolean isTaskClosing() {
    return taskClosing.get();
  }

  public StreamSubTaskContext getSubTaskContext() {
    return subTaskContext;
  }
}
