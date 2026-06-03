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

import com.google.common.util.concurrent.SettableFuture;
import org.apache.tsfile.read.common.block.TsBlock;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class SinkEntry {
  private final Optional<TsBlock> tsBlock;
  // All source data consumed to produce this result, commit ID
  private final List<Long> commitIds;
  // source subTask id
  private final int subTaskId;
  private final long memorySizeInBytes;
  private final long enqueueTimeNanos;
  // Create as needed only when backpressure is applied
  private SettableFuture<Void> backpressureFuture;

  public SinkEntry(
      Optional<TsBlock> block, List<Long> commitIds, int subTaskId, long retainedSizeInBytes) {
    this.tsBlock = Objects.requireNonNull(block, "block should not be null");
    this.commitIds = commitIds;
    this.subTaskId = subTaskId;
    this.memorySizeInBytes = retainedSizeInBytes;
    this.enqueueTimeNanos = System.nanoTime();
  }

  public SettableFuture<Void> getOrCreateBackpressureFuture() {
    if (backpressureFuture == null) {
      backpressureFuture = SettableFuture.create();
    }
    return backpressureFuture;
  }

  public Optional<TsBlock> getTsBlock() {
    return tsBlock;
  }

  public List<Long> getCommitIds() {
    return commitIds;
  }

  public int getSubTaskId() {
    return subTaskId;
  }

  public long getMemorySizeInBytes() {
    return memorySizeInBytes;
  }

  public long getEnqueueTimeNanos() {
    return enqueueTimeNanos;
  }
}
