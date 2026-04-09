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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class StreamProperties {

  public enum EventType {
    WINDOW_OPEN,
    WINDOW_CLOSE
  }

  // Field name constants for serialization
  private static final String FIELD_WATERMARK_MS = "watermarkMs";
  private static final String FIELD_EXPIRED_TIME_MS = "expiredTimeMs";
  private static final String FIELD_IGNORE_DISORDER = "ignoreDisorder";
  private static final String FIELD_FILL_HISTORY_START_TIME = "fillHistoryStartTime";
  private static final String FIELD_MAX_DELAY_MS = "maxDelayMs";
  private static final String FIELD_EVENT_TYPE = "eventType";
  private static final String FIELD_END = "end";

  private long watermarkMs; // < 0 means not effective
  private long expiredTimeMs; // < 0 means not effective
  private boolean ignoreDisorder;
  private Long fillHistoryStartTime;
  private long maxDelayMs; // < 0 means not effective
  private EventType eventType;

  public StreamProperties() {}

  public StreamProperties(
      long watermarkMs,
      long expiredTimeMs,
      boolean ignoreDisorder,
      Long fillHistoryStartTime,
      long maxDelayMs,
      EventType eventType) {
    this.watermarkMs = watermarkMs;
    this.expiredTimeMs = expiredTimeMs;
    this.ignoreDisorder = ignoreDisorder;
    this.fillHistoryStartTime = fillHistoryStartTime;
    this.maxDelayMs = maxDelayMs;
    this.eventType = eventType;
  }

  public long getWatermarkMs() {
    return watermarkMs;
  }

  public void setWatermarkMs(long watermarkMs) {
    this.watermarkMs = watermarkMs;
  }

  public long getExpiredTimeMs() {
    return expiredTimeMs;
  }

  public void setExpiredTimeMs(long expiredTimeMs) {
    this.expiredTimeMs = expiredTimeMs;
  }

  public boolean isIgnoreDisorder() {
    return ignoreDisorder;
  }

  public void setIgnoreDisorder(boolean ignoreDisorder) {
    this.ignoreDisorder = ignoreDisorder;
  }

  public Long getFillHistoryStartTime() {
    return fillHistoryStartTime;
  }

  public void setFillHistoryStartTime(Long fillHistoryStartTime) {
    this.fillHistoryStartTime = fillHistoryStartTime;
  }

  public long getMaxDelayMs() {
    return maxDelayMs;
  }

  public void setMaxDelayMs(long maxDelayMs) {
    this.maxDelayMs = maxDelayMs;
  }

  public EventType getEventType() {
    return eventType;
  }

  public void setEventType(EventType eventType) {
    this.eventType = eventType;
  }

  public void serialize(OutputStream outputStream) throws IOException {
    DataOutputStream dataOutputStream = new DataOutputStream(outputStream);
    // Add field names for better readability and future compatibility
    // Serialize watermarkMs (long)
    dataOutputStream.writeUTF(FIELD_WATERMARK_MS);
    dataOutputStream.writeLong(watermarkMs);

    // Serialize expiredTimeMs (long)
    dataOutputStream.writeUTF(FIELD_EXPIRED_TIME_MS);
    dataOutputStream.writeLong(expiredTimeMs);

    // Serialize ignoreDisorder (boolean)
    dataOutputStream.writeUTF(FIELD_IGNORE_DISORDER);
    dataOutputStream.writeBoolean(ignoreDisorder);

    // Serialize fillHistoryStartTime (nullable long)
    dataOutputStream.writeUTF(FIELD_FILL_HISTORY_START_TIME);
    dataOutputStream.writeBoolean(fillHistoryStartTime != null);
    if (fillHistoryStartTime != null) {
      dataOutputStream.writeLong(fillHistoryStartTime);
    }

    // Serialize maxDelayMs (long)
    dataOutputStream.writeUTF(FIELD_MAX_DELAY_MS);
    dataOutputStream.writeLong(maxDelayMs);

    // Serialize eventType (int)
    dataOutputStream.writeUTF(FIELD_EVENT_TYPE);
    dataOutputStream.writeInt(eventType.ordinal());

    // End marker
    dataOutputStream.writeUTF(FIELD_END);
  }

  public static StreamProperties deserialize(InputStream inputStream) throws IOException {
    DataInputStream dataInputStream = new DataInputStream(inputStream);
    StreamProperties properties = new StreamProperties();

    label:
    while (true) {
      String fieldName = dataInputStream.readUTF();
      switch (fieldName) {
        case FIELD_END:
          break label;
        case FIELD_WATERMARK_MS:
          properties.setWatermarkMs(dataInputStream.readLong());
          break;
        case FIELD_EXPIRED_TIME_MS:
          properties.setExpiredTimeMs(dataInputStream.readLong());
          break;
        case FIELD_IGNORE_DISORDER:
          properties.setIgnoreDisorder(dataInputStream.readBoolean());
          break;
        case FIELD_FILL_HISTORY_START_TIME:
          if (dataInputStream.readBoolean()) {
            properties.setFillHistoryStartTime(dataInputStream.readLong());
          }
          break;
        case FIELD_MAX_DELAY_MS:
          properties.setMaxDelayMs(dataInputStream.readLong());
          break;
        case FIELD_EVENT_TYPE:
          int eventTypeOrdinal = dataInputStream.readInt();
          properties.setEventType(EventType.values()[eventTypeOrdinal]);
          break;
        default:
          throw new IOException("Unknown field: " + fieldName);
      }
    }

    return properties;
  }
}
