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

package org.apache.iotdb.commons.stream;

import org.apache.iotdb.calc.i18n.CalcMessages;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class StreamProperties {
  public static final String WATERMARK = "watermark";
  public static final String ALLOWED_LATENESS = "allowed_lateness";
  public static final String MAX_DELAY_WINDOW_CLOSE = "max_delay_window_close";
  public static final String MAX_IDLE_WINDOW_RETAIN = "max_idle_window_retain";
  public static final String LEFT_CLOSED = "left_closed";

  public static final String SUBSCRIPTION_CONSENSUS_WAL_RETENTION_SIZE_IN_BYTES =
      "subscription_consensus_wal_retention_size_in_bytes";
  public static final String SUBSCRIPTION_CONSENSUS_WAL_RETENTION_TIME_MS =
      "subscription_consensus_wal_retention_time_ms";
  public static final String SUBSCRIPTION_POLL_THREAD_COUNT = "subscription_poll_thread_count";

  public enum EventType {
    WINDOW_OPEN,
    WINDOW_CLOSE
  }

  private long watermark = 0;
  private long allowedLateness = 0;
  private long maxDelayWindowCloseMs = 0;
  private long maxIdleWindowRetainMs = 0;
  private boolean leftClosed = true;

  private Long subscriptionConsensusWalRetentionSizeInBytes;
  private Long subscriptionConsensusWalRetentionTimeMs;
  private Integer subscriptionPollThreadCount;

  public StreamProperties() {}

  public StreamProperties(
      long watermark,
      long allowedLateness,
      long maxDelayWindowCloseMs,
      long maxIdleWindowRetainMs,
      boolean leftClosed) {
    this.watermark = watermark;
    this.allowedLateness = allowedLateness;
    this.maxDelayWindowCloseMs = maxDelayWindowCloseMs;
    this.maxIdleWindowRetainMs = maxIdleWindowRetainMs;
    this.leftClosed = leftClosed;
  }

  public long getWatermark() {
    return watermark;
  }

  public void setWatermark(long watermark) {
    this.watermark = watermark;
  }

  public long getAllowedLateness() {
    return allowedLateness;
  }

  public void setAllowedLateness(long allowedLateness) {
    this.allowedLateness = allowedLateness;
  }

  public long getMaxDelayWindowCloseMs() {
    return maxDelayWindowCloseMs;
  }

  public void setMaxDelayWindowCloseMs(long maxDelayWindowCloseMs) {
    this.maxDelayWindowCloseMs = maxDelayWindowCloseMs;
  }

  public long getMaxIdleWindowRetainMs() {
    return maxIdleWindowRetainMs;
  }

  public void setMaxIdleWindowRetainMs(long maxIdleWindowRetainMs) {
    this.maxIdleWindowRetainMs = maxIdleWindowRetainMs;
  }

  public boolean isLeftClosed() {
    return leftClosed;
  }

  public void setLeftClosed(boolean leftClosed) {
    this.leftClosed = leftClosed;
  }

  public Long getSubscriptionConsensusWalRetentionSizeInBytes() {
    return subscriptionConsensusWalRetentionSizeInBytes;
  }

  public void setSubscriptionConsensusWalRetentionSizeInBytes(
      Long subscriptionConsensusWalRetentionSizeInBytes) {
    this.subscriptionConsensusWalRetentionSizeInBytes =
        subscriptionConsensusWalRetentionSizeInBytes;
  }

  public Long getSubscriptionConsensusWalRetentionTimeMs() {
    return subscriptionConsensusWalRetentionTimeMs;
  }

  public void setSubscriptionConsensusWalRetentionTimeMs(
      Long subscriptionConsensusWalRetentionTimeMs) {
    this.subscriptionConsensusWalRetentionTimeMs = subscriptionConsensusWalRetentionTimeMs;
  }

  public Integer getSubscriptionPollThreadCount() {
    return subscriptionPollThreadCount;
  }

  public void setSubscriptionPollThreadCount(Integer subscriptionPollThreadCount) {
    this.subscriptionPollThreadCount = subscriptionPollThreadCount;
  }

  public void serialize(DataOutputStream stream) throws IOException {
    stream.writeLong(watermark);
    stream.writeLong(allowedLateness);
    stream.writeLong(maxDelayWindowCloseMs);
    stream.writeLong(maxIdleWindowRetainMs);
    stream.writeBoolean(leftClosed);
    // Append optional fields so payloads written by queryStage remain readable by older nodes.
    writeNullableLong(stream, subscriptionConsensusWalRetentionSizeInBytes);
    writeNullableLong(stream, subscriptionConsensusWalRetentionTimeMs);
    writeNullableInteger(stream, subscriptionPollThreadCount);
  }

  public static StreamProperties deserialize(ByteBuffer byteBuffer) throws IOException {
    StreamProperties p = new StreamProperties();
    p.setWatermark(byteBuffer.getLong());
    p.setAllowedLateness(byteBuffer.getLong());
    p.setMaxDelayWindowCloseMs(byteBuffer.getLong());
    p.setMaxIdleWindowRetainMs(byteBuffer.getLong());
    p.setLeftClosed(byteBuffer.get() != 0);
    // Fields may be absent when reading a payload written before subscription retention support.
    if (byteBuffer.hasRemaining()) {
      p.setSubscriptionConsensusWalRetentionSizeInBytes(readNullableLong(byteBuffer));
    }
    if (byteBuffer.hasRemaining()) {
      p.setSubscriptionConsensusWalRetentionTimeMs(readNullableLong(byteBuffer));
    }
    if (byteBuffer.hasRemaining()) {
      p.setSubscriptionPollThreadCount(readNullableInteger(byteBuffer));
    }
    return p;
  }

  @Override
  public String toString() {
    return "StreamProperties{"
        + "watermark="
        + watermark
        + ", allowedLateness="
        + allowedLateness
        + ", maxDelayWindowClose="
        + maxDelayWindowCloseMs
        + ", maxIdleWindowRetain="
        + maxIdleWindowRetainMs
        + ", leftClosed="
        + leftClosed
        + ", subscriptionConsensusWalRetentionSizeInBytes="
        + subscriptionConsensusWalRetentionSizeInBytes
        + ", subscriptionConsensusWalRetentionTimeMs="
        + subscriptionConsensusWalRetentionTimeMs
        + ", subscriptionPollThreadCount="
        + subscriptionPollThreadCount
        + '}';
  }

  private static void writeNullableLong(DataOutputStream stream, Long value) throws IOException {
    if (value == null) {
      stream.writeBoolean(false);
    } else {
      stream.writeBoolean(true);
      stream.writeLong(value);
    }
  }

  private static Long readNullableLong(ByteBuffer byteBuffer) throws IOException {
    if (!byteBuffer.hasRemaining()) {
      throw new IOException(CalcMessages.EXCEPTION_UNEXPECTED_END_OF_STREAM_PROPERTIES_922EBC18);
    }
    if (byteBuffer.get() == 0) {
      return null;
    }
    if (byteBuffer.remaining() < Long.BYTES) {
      throw new IOException(CalcMessages.EXCEPTION_UNEXPECTED_END_OF_STREAM_PROPERTIES_922EBC18);
    }
    return byteBuffer.getLong();
  }

  private static void writeNullableInteger(DataOutputStream stream, Integer value)
      throws IOException {
    if (value == null) {
      stream.writeBoolean(false);
    } else {
      stream.writeBoolean(true);
      stream.writeInt(value);
    }
  }

  private static Integer readNullableInteger(ByteBuffer byteBuffer) throws IOException {
    if (!byteBuffer.hasRemaining()) {
      throw new IOException(CalcMessages.EXCEPTION_UNEXPECTED_END_OF_STREAM_PROPERTIES_922EBC18);
    }
    if (byteBuffer.get() == 0) {
      return null;
    }
    if (byteBuffer.remaining() < Integer.BYTES) {
      throw new IOException(CalcMessages.EXCEPTION_UNEXPECTED_END_OF_STREAM_PROPERTIES_922EBC18);
    }
    return byteBuffer.getInt();
  }
}
