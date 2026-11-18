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

package org.apache.iotdb.streamnode.engine.reuse;

import org.apache.tsfile.block.column.Column;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Cached partial-aggregation base slice produced by an upstream tumbling stream and referenced by
 * coarse downstream consumers via reference counting and TTL retention.
 */
public final class SharedBaseSlice {

  private final String producerStream;
  private final String partitionKeyId;
  private final long windowStart;
  private final long windowEnd;
  private final long stateVersion;
  private final long rowCount;
  private final Column[] intermediateColumns;
  private final long publishWallTimeMs;
  private final long expiresAtWallTimeMs;
  private final long estimatedBytes;
  private final AtomicInteger refCount = new AtomicInteger(0);

  public SharedBaseSlice(
      String producerStream,
      String partitionKeyId,
      long windowStart,
      long windowEnd,
      long stateVersion,
      long rowCount,
      Column[] intermediateColumns,
      long publishWallTimeMs,
      long expiresAtWallTimeMs,
      long estimatedBytes) {
    this.producerStream = producerStream;
    this.partitionKeyId = partitionKeyId;
    this.windowStart = windowStart;
    this.windowEnd = windowEnd;
    this.stateVersion = stateVersion;
    this.rowCount = rowCount;
    this.intermediateColumns = intermediateColumns;
    this.publishWallTimeMs = publishWallTimeMs;
    this.expiresAtWallTimeMs = expiresAtWallTimeMs;
    this.estimatedBytes = estimatedBytes;
  }

  public String getProducerStream() {
    return producerStream;
  }

  public String getPartitionKeyId() {
    return partitionKeyId;
  }

  public long getWindowStart() {
    return windowStart;
  }

  public long getWindowEnd() {
    return windowEnd;
  }

  public long getStateVersion() {
    return stateVersion;
  }

  public long getRowCount() {
    return rowCount;
  }

  public Column[] getIntermediateColumns() {
    return intermediateColumns;
  }

  public long getPublishWallTimeMs() {
    return publishWallTimeMs;
  }

  public long getExpiresAtWallTimeMs() {
    return expiresAtWallTimeMs;
  }

  public long getEstimatedBytes() {
    return estimatedBytes;
  }

  public int incrementRefCount() {
    return refCount.incrementAndGet();
  }

  public int decrementRefCount() {
    return refCount.updateAndGet(cur -> cur > 0 ? cur - 1 : 0);
  }

  public int getRefCount() {
    return refCount.get();
  }
}
