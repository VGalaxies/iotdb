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

package org.apache.iotdb.streamnode.engine.window;

import org.apache.iotdb.commons.stream.PartitionKey;
import org.apache.iotdb.streamnode.i18n.StreamNodeMessages;
import org.apache.iotdb.streamnode.utils.IEventRowsIterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.OptionalLong;

public class WindowEventInfo implements IEventInfo {

  private static final Logger LOGGER = LoggerFactory.getLogger(WindowEventInfo.class);

  // Volatile non-final: the single per-window event starts as WINDOW_OPEN (pre-close incremental
  // chunks) and flips to WINDOW_CLOSE when the window closes (markClosed), which the consuming
  // driver may read concurrently.
  private volatile WindowEventType eventType;
  private final long startTime;
  private final long endTime;
  private final PartitionKey partitionKey;
  private final OptionalLong prevTime;
  private final OptionalLong nextTime;
  private volatile long rowCount;
  private final long windowId;
  private final boolean needsCompute;
  private final IEventRowsIterator rowsIterator;

  private boolean needRetry;

  private WindowEventInfo(Builder builder) {
    this.eventType = builder.eventType;
    this.startTime = builder.startTime;
    this.endTime = builder.endTime;
    this.partitionKey = builder.partitionKey;
    this.prevTime = builder.prevTime;
    this.nextTime = builder.nextTime;
    this.windowId = builder.windowId;
    this.needsCompute = builder.needsCompute;
    this.rowsIterator = builder.rowsIterator;
    this.rowCount = builder.rowCount;
  }

  public void addRows(int addedRowCount) {
    if (addedRowCount <= 0) {
      return;
    }
    rowCount += addedRowCount;
  }

  /**
   * Flips this event's type to {@link WindowEventType#WINDOW_CLOSE} when its window closes. Only
   * the pre-close incremental chunks are observable as {@code WINDOW_OPEN}; the flip happens (under
   * the engine lock) before the iterator is marked finished, so a consumer that completes the event
   * rows always observes the closed type.
   */
  public void markClosed() {
    this.eventType = WindowEventType.WINDOW_CLOSE;
  }

  @Override
  public WindowEventType getEventType() {
    return eventType;
  }

  @Override
  public OptionalLong getStartTime() {
    return OptionalLong.of(startTime);
  }

  @Override
  public OptionalLong getEndTime() {
    return OptionalLong.of(endTime);
  }

  @Override
  public OptionalLong getPrevTime() {
    return prevTime;
  }

  @Override
  public OptionalLong getNextTime() {
    return nextTime;
  }

  @Override
  public OptionalLong getRowCount() {
    return OptionalLong.of(rowCount);
  }

  @Override
  public Optional<Object> getPrevValue() {
    return Optional.empty();
  }

  @Override
  public Optional<Object> getCurrentValue() {
    return Optional.empty();
  }

  @Override
  public Optional<Object> getN(int n) {
    return Optional.ofNullable(partitionKey.segmentValue(n - 1));
  }

  @Override
  public long getWindowId() {
    return windowId;
  }

  @Override
  public boolean needsCompute() {
    return needsCompute;
  }

  @Override
  public IEventRowsIterator getRowsIterator() {
    return rowsIterator;
  }

  /**
   * Closes the rows iterator, unpinning its window's row store (and returning the store's data-pool
   * charge when this was the last owner). Idempotent, and safe on an already fully consumed
   * iterator.
   */
  @Override
  public void release() {
    if (rowsIterator == null) {
      return;
    }
    try {
      rowsIterator.close();
    } catch (Exception e) {
      LOGGER.error(StreamNodeMessages.LOG_FAILED_TO_RELEASE_ROWS_ITERATOR_F9D749CD, e);
    }
  }

  @Override
  public boolean needRetry() {
    return needRetry;
  }

  @Override
  public void handleComputeException(Throwable cause) {
    // Mark the event as failed due to bad data (e.g. division by zero). The marking call is left
    // empty for now and will be implemented by the window module.
    needRetry = !(cause instanceof ArithmeticException);
  }

  public static class Builder {
    private PartitionKey partitionKey;
    private WindowEventType eventType = WindowEventType.WINDOW_OPEN;
    private long startTime;
    private long endTime;
    private OptionalLong prevTime = OptionalLong.empty();
    private OptionalLong nextTime = OptionalLong.empty();
    private int rowCount;
    private long windowId = -1;
    private boolean needsCompute = true;
    private IEventRowsIterator rowsIterator;

    public Builder startTime(long startTime) {
      this.startTime = startTime;
      return this;
    }

    public Builder endTime(long endTime) {
      this.endTime = endTime;
      return this;
    }

    public Builder partitionKey(PartitionKey partitionKey) {
      this.partitionKey = partitionKey;
      return this;
    }

    public Builder prevTime(long prevTime) {
      this.prevTime = OptionalLong.of(prevTime);
      return this;
    }

    public Builder nextTime(long nextTime) {
      this.nextTime = OptionalLong.of(nextTime);
      return this;
    }

    public Builder rowCount(int rowCount) {
      this.rowCount = rowCount;
      return this;
    }

    public Builder windowId(long windowId) {
      this.windowId = windowId;
      return this;
    }

    /**
     * Sets whether this event must be executed by the compute task. Defaults to {@code true};
     * compute-free lifecycle markers (e.g. a destroy event that only advances commit) set it to
     * {@code false}.
     */
    public Builder needsCompute(boolean needsCompute) {
      this.needsCompute = needsCompute;
      return this;
    }

    public Builder rowsIterator(IEventRowsIterator rowsIterator) {
      this.rowsIterator = rowsIterator;
      return this;
    }

    public Builder eventType(WindowEventType eventType) {
      this.eventType = eventType;
      return this;
    }

    public WindowEventInfo build() {
      return new WindowEventInfo(this);
    }
  }
}
