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

package org.apache.iotdb.streamnode.engine.scheduler.queue;

import javax.annotation.concurrent.Immutable;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * This class is inspired by Trino <a
 * href="https://github.com/trinodb/trino/blob/master/core/trino-main/src/main/java/io/trino/execution/executor/Priority.java">...</a>
 * Priority represents the scheduling order of a {@link StreamDriverTask} in the ready queue.
 *
 * <p>Currently this is a flat single-level round-robin model: tasks are ordered by their {@code
 * lastScheduledTime} — the timestamp of their last CPU scheduling. Tasks with smaller {@code
 * lastScheduledTime} finished earlier and waited longer, so they are polled first.
 *
 * <p>This ensures starvation-free fair scheduling among all driver tasks, regardless of their
 * weight or data rate.
 */
@Immutable
public final class Priority {
  /**
   * Occupied time in particular level of this task. The higher this value is, the later the task
   * with this Priority will be polled out by a PriorityQueue.
   */
  private final long lastScheduledTime;

  public Priority(long lastScheduledTime) {
    this.lastScheduledTime = lastScheduledTime;
  }

  public long getLastScheduledTime() {
    return lastScheduledTime;
  }

  @Override
  public String toString() {
    return toStringHelper(this).add("lastScheduledTime", lastScheduledTime).toString();
  }
}
