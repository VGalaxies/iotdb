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

public class SinkMemoryController {
  private long usedBytes = 0;
  private final long pipelineMemoryLimitBytes;
  private final GlobalSinkMemoryController globalController;

  public SinkMemoryController(
      long pipelineMemoryLimitBytes, GlobalSinkMemoryController globalController) {
    this.pipelineMemoryLimitBytes = pipelineMemoryLimitBytes;
    this.globalController = globalController;
  }

  /**
   * Maximum bytes a single reservation may request: the pipeline quota. An entry larger than this
   * can never be admitted, even on an empty pipeline.
   */
  public long getAdmissionLimitBytes() {
    return pipelineMemoryLimitBytes;
  }

  /**
   * Whether {@code bytes} alone exceeds the pipeline quota. Such a request must fail hard instead
   * of parking as temporary backpressure, because it can never fit even on an empty pipeline.
   */
  public boolean exceedsAdmissionLimit(long bytes) {
    return bytes > pipelineMemoryLimitBytes;
  }

  /**
   * Try to reserve memory for this pipeline against both the pipeline quota and the shared global
   * write-back pool.
   *
   * @param bytes the amount of memory to reserve
   * @return true if reservation succeeded, false otherwise (the caller parks the entry and retries
   *     after memory is released; permanently oversized requests also return false)
   */
  public synchronized boolean tryReserve(long bytes) {
    // Defense for any caller
    if (exceedsAdmissionLimit(bytes)) {
      return false;
    }
    // Check the pipeline quota before touching the global pool to avoid reserve-then-rollback
    // jitter on the global used counter.
    if (usedBytes + bytes > pipelineMemoryLimitBytes) {
      return false;
    }
    if (!globalController.tryReserve(bytes)) {
      return false;
    }
    usedBytes += bytes;
    return true;
  }

  /**
   * Release reserved memory.
   *
   * @param bytes the amount of memory to release
   * @return true if the global release succeeded, false if the global release was rejected
   */
  public synchronized boolean release(long bytes) {
    if (!globalController.release(bytes)) {
      return false;
    }
    usedBytes -= bytes;
    return true;
  }

  public synchronized long getUsedBytes() {
    return usedBytes;
  }
}
