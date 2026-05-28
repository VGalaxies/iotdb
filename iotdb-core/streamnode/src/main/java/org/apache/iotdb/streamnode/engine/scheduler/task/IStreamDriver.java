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

import org.apache.iotdb.calc.execution.schedule.queue.ID;

import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.Duration;
import org.apache.tsfile.read.common.block.TsBlock;

public interface IStreamDriver {
  /**
   * Used to judge whether this {@link IStreamDriver} should be scheduled for execution anymore.
   *
   * @return true if the {@link IStreamDriver} is finish, otherwise false.
   */
  boolean isFinished();

  /**
   * Run this {@link IStreamDriver} for the given {@code duration} time slice. The actual run time
   * may not equal {@code duration}, the actual run time should be calculated by the caller.
   *
   * @param duration how long should this {@link IStreamDriver} run
   * @return the returned ListenableFuture is used to represent status of this processing. If
   *     isDone() returns true, it means that this {@link IStreamDriver} is not blocked and is ready
   *     for next processing. Otherwise, it means that this {@link IStreamDriver} is blocked and not
   *     ready for next processing.
   */
  @SuppressWarnings("squid:S1452")
  ListenableFuture<?> processFor(Duration duration);

  ListenableFuture<?> push(TsBlock tsBlock, long commitId);

  default long getEstimatedMemorySize() {
    return 0;
  }

  /** Clear resource used by this fragment instance. */
  void close();

  /**
   * Fail current {@link IStreamDriver}.
   *
   * @param t reason cause this failure
   */
  void failed(Throwable t);

  DriverTaskId getDriverTaskId();

  void setDriverTaskId(ID id);
}
