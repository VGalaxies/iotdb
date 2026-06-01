/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.streamnode.engine.sink;

// TODO: Implement memory and backpressure control in future
public class SinkMemoryController {
  private final long pipelineMemoryLimitBytes;
  private final GlobalSinkMemoryController globalController;

  public SinkMemoryController(
      long pipelineMemoryLimitBytes, GlobalSinkMemoryController globalController) {
    this.pipelineMemoryLimitBytes = pipelineMemoryLimitBytes;
    this.globalController = globalController;
  }

  /**
   * Try to reserve memory for this pipeline. Currently always returns true (no memory limit).
   *
   * @param bytes the amount of memory to reserve
   * @return true if reservation succeeded, false otherwise
   */
  public boolean tryReserve(long bytes) {
    return true;
  }

  /**
   * Release reserved memory.
   *
   * @param bytes the amount of memory to release
   * @return true if backpressure was deactivated, false otherwise
   */
  public boolean release(long bytes) {
    return false;
  }

  /**
   * Check if backpressure is active.
   *
   * @return true if backpressure is active, false otherwise
   */
  public boolean isBackpressureActive() {
    return false;
  }
}
