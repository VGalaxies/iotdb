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

public class SinkPipelineConfig {
  // 64MB
  private long pipelineMemoryLimitBytes = 64 * 1024 * 1024;
  private int maxBatchRows = 8192;
  private long maxBatchMemoryBytes = 1024 * 1024;
  private long maxBatchLingerMs = 50;
  private int writerThreadCount = 4;
  private int drainBatchSize = 64;
  private int maxRetryAttempts = 3;
  private long retryBackoffBaseMs = 1000;

  public long getPipelineMemoryLimitBytes() {
    return pipelineMemoryLimitBytes;
  }

  public SinkPipelineConfig setPipelineMemoryLimitBytes(long pipelineMemoryLimitBytes) {
    this.pipelineMemoryLimitBytes = pipelineMemoryLimitBytes;
    return this;
  }

  public int getMaxBatchRows() {
    return maxBatchRows;
  }

  public SinkPipelineConfig setMaxBatchRows(int maxBatchRows) {
    this.maxBatchRows = maxBatchRows;
    return this;
  }

  public long getMaxBatchMemoryBytes() {
    return maxBatchMemoryBytes;
  }

  public SinkPipelineConfig setMaxBatchMemoryBytes(long maxBatchMemoryBytes) {
    this.maxBatchMemoryBytes = maxBatchMemoryBytes;
    return this;
  }

  public long getMaxBatchLingerMs() {
    return maxBatchLingerMs;
  }

  public SinkPipelineConfig setMaxBatchLingerMs(long maxBatchLingerMs) {
    this.maxBatchLingerMs = maxBatchLingerMs;
    return this;
  }

  public int getWriterThreadCount() {
    return writerThreadCount;
  }

  public SinkPipelineConfig setWriterThreadCount(int writerThreadCount) {
    this.writerThreadCount = writerThreadCount;
    return this;
  }

  public int getDrainBatchSize() {
    return drainBatchSize;
  }

  public SinkPipelineConfig setDrainBatchSize(int drainBatchSize) {
    this.drainBatchSize = drainBatchSize;
    return this;
  }

  public int getMaxRetryAttempts() {
    return maxRetryAttempts;
  }

  public SinkPipelineConfig setMaxRetryAttempts(int maxRetryAttempts) {
    this.maxRetryAttempts = maxRetryAttempts;
    return this;
  }

  public long getRetryBackoffBaseMs() {
    return retryBackoffBaseMs;
  }

  public SinkPipelineConfig setRetryBackoffBaseMs(long retryBackoffBaseMs) {
    this.retryBackoffBaseMs = retryBackoffBaseMs;
    return this;
  }
}
