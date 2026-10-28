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

import java.util.Objects;
import java.util.Optional;

public class SinkEntry {
  private final Optional<TsBlock> tsBlock;

  /** Window that produced this entry; {@code -1} if unbound. */
  private final long windowId;

  /**
   * True if this push marks the window as destroyable: after this and all prior inflight entries
   * flush, source commit may advance. Name kept as eof; meaning is window destroy.
   */
  private final boolean eof;

  /** Owning sink sub-task id. */
  private final int subTaskId;

  private final long memorySizeInBytes;
  private final long enqueueTimeNanos;

  /** Lazily created when backpressure is applied. */
  private SettableFuture<Void> backpressureFuture;

  public SinkEntry(
      Optional<TsBlock> block,
      long windowId,
      boolean eof,
      int subTaskId,
      long retainedSizeInBytes) {
    this.tsBlock = Objects.requireNonNull(block, "block should not be null");
    this.windowId = windowId;
    this.eof = eof;
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

  public long getWindowId() {
    return windowId;
  }

  public boolean isEof() {
    return eof;
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
