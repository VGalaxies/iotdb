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
import org.apache.iotdb.commons.utils.BasicStructureSerDeUtil;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

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
  public static final String FILL_HISTORY = "fill_history";
  public static final String FILL_HISTORY_START_TIME = "fill_history_start_time";

  // Distributed partition progress alignment (§4.3)
  public static final String ALIGN_PARTITION_WATERMARK = "align_partition_watermark";
  public static final String PARTITION_STALE_TIMEOUT_MS = "partition_stale_timeout_ms";

  // Variation / regime-aware dynamic boundary detection (§4.4)
  public static final String VARIATION_DETECTOR = "variation_detector";
  public static final String VARIATION_THETA = "variation_theta";
  public static final String VARIATION_EPSILON = "variation_epsilon";
  public static final String VARIATION_TAU_MS = "variation_tau_ms";
  public static final String VARIATION_DT_MAX_MS = "variation_dt_max_ms";
  public static final String VARIATION_MIN_SAMPLES = "variation_min_samples";
  public static final String VARIATION_BUFFER_MAX_ROWS = "variation_buffer_max_rows";
  public static final String VARIATION_BUFFER_MAX_MS = "variation_buffer_max_ms";
  public static final String VARIATION_OVERFLOW_POLICY = "variation_overflow_policy";
  public static final String VARIATION_ADAPTIVE_BASELINE = "variation_adaptive_baseline";

  // Multi-granularity shared slice reuse (§4.5)
  public static final String ENABLE_SLICE_REUSE = "enable_slice_reuse";
  public static final String SLICE_RETENTION_MS = "slice_retention_ms";
  public static final String REUSE_SKETCH_TYPE = "reuse_sketch_type";
  public static final String REUSE_KLL_K = "reuse_kll_k";
  public static final String REUSE_COST_THRESHOLD = "reuse_cost_threshold";

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
  private boolean fillHistory = false;
  private Long fillHistoryStartTime;
  private final Map<String, String> extraProperties = new LinkedHashMap<>();

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

  public boolean isFillHistory() {
    return fillHistory;
  }

  public void setFillHistory(boolean fillHistory) {
    this.fillHistory = fillHistory;
  }

  public Long getFillHistoryStartTime() {
    return fillHistoryStartTime;
  }

  public void setFillHistoryStartTime(Long fillHistoryStartTime) {
    this.fillHistoryStartTime = fillHistoryStartTime;
  }

  public Map<String, String> getExtraProperties() {
    return Collections.unmodifiableMap(extraProperties);
  }

  public String getExtraProperty(String key) {
    return key == null ? null : extraProperties.get(key.toLowerCase(Locale.ENGLISH));
  }

  public void putExtraProperty(String key, String value) {
    if (key != null && value != null) {
      extraProperties.put(key.toLowerCase(Locale.ENGLISH), value);
    }
  }

  public void putAllExtraProperties(Map<String, String> values) {
    if (values != null) {
      values.forEach(this::putExtraProperty);
    }
  }

  public boolean getBooleanExtraProperty(String key, boolean defaultValue) {
    String val = getExtraProperty(key);
    return val == null ? defaultValue : Boolean.parseBoolean(val);
  }

  public int getIntExtraProperty(String key, int defaultValue) {
    String val = getExtraProperty(key);
    if (val == null) {
      return defaultValue;
    }
    try {
      return Integer.parseInt(val);
    } catch (NumberFormatException e) {
      return defaultValue;
    }
  }

  public long getLongExtraProperty(String key, long defaultValue) {
    String val = getExtraProperty(key);
    if (val == null) {
      return defaultValue;
    }
    try {
      return Long.parseLong(val);
    } catch (NumberFormatException e) {
      return defaultValue;
    }
  }

  public double getDoubleExtraProperty(String key, double defaultValue) {
    String val = getExtraProperty(key);
    if (val == null) {
      return defaultValue;
    }
    try {
      return Double.parseDouble(val);
    } catch (NumberFormatException e) {
      return defaultValue;
    }
  }

  public String getStringExtraProperty(String key, String defaultValue) {
    String val = getExtraProperty(key);
    return val == null ? defaultValue : val;
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
    stream.writeBoolean(fillHistory);
    writeNullableLong(stream, fillHistoryStartTime);
    stream.writeInt(extraProperties.size());
    for (Map.Entry<String, String> entry : extraProperties.entrySet()) {
      BasicStructureSerDeUtil.write(entry.getKey(), stream);
      BasicStructureSerDeUtil.write(entry.getValue(), stream);
    }
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
    if (byteBuffer.hasRemaining()) {
      p.setFillHistory(byteBuffer.get() != 0);
    }
    if (byteBuffer.hasRemaining()) {
      p.setFillHistoryStartTime(readNullableLong(byteBuffer));
    }
    if (byteBuffer.remaining() >= Integer.BYTES) {
      int extraCount = byteBuffer.getInt();
      if (extraCount < 0) {
        throw new IOException(CalcMessages.EXCEPTION_UNEXPECTED_END_OF_STREAM_PROPERTIES_922EBC18);
      }
      for (int i = 0; i < extraCount; i++) {
        String key = BasicStructureSerDeUtil.readString(byteBuffer);
        String value = BasicStructureSerDeUtil.readString(byteBuffer);
        if (key != null && value != null) {
          p.putExtraProperty(key, value);
        }
      }
    }
    return p;
  }

  @Override
  public String toString() {
    StringBuilder sb =
        new StringBuilder("StreamProperties{")
            .append("watermark=")
            .append(watermark)
            .append(", allowedLateness=")
            .append(allowedLateness)
            .append(", maxDelayWindowClose=")
            .append(maxDelayWindowCloseMs)
            .append(", maxIdleWindowRetain=")
            .append(maxIdleWindowRetainMs)
            .append(", leftClosed=")
            .append(leftClosed)
            .append(", subscriptionConsensusWalRetentionSizeInBytes=")
            .append(subscriptionConsensusWalRetentionSizeInBytes)
            .append(", subscriptionConsensusWalRetentionTimeMs=")
            .append(subscriptionConsensusWalRetentionTimeMs)
            .append(", subscriptionPollThreadCount=")
            .append(subscriptionPollThreadCount)
            .append(", fillHistory=")
            .append(fillHistory)
            .append(", fillHistoryStartTime=")
            .append(fillHistoryStartTime);
    if (!extraProperties.isEmpty()) {
      sb.append(", extraProperties={");
      boolean first = true;
      for (Map.Entry<String, String> entry : extraProperties.entrySet()) {
        if (!first) {
          sb.append("; ");
        }
        first = false;
        sb.append(entry.getKey()).append('=').append(entry.getValue());
      }
      sb.append('}');
    }
    return sb.append('}').toString();
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
