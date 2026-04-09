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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class StreamProperties {

  public enum EventType {
    WINDOW_OPEN,
    WINDOW_CLOSE
  }

  private Long watermarkMs;
  private Long expiredTimeMs;
  private boolean ignoreDisorder;
  private Long fillHistoryStartTime;
  private Long maxDelayMs;
  private EventType eventType;

  public StreamProperties() {}

  public StreamProperties(
      Long watermarkMs,
      Long expiredTimeMs,
      boolean ignoreDisorder,
      Long fillHistoryStartTime,
      Long maxDelayMs,
      EventType eventType) {
    this.watermarkMs = watermarkMs;
    this.expiredTimeMs = expiredTimeMs;
    this.ignoreDisorder = ignoreDisorder;
    this.fillHistoryStartTime = fillHistoryStartTime;
    this.maxDelayMs = maxDelayMs;
    this.eventType = eventType;
  }

  public Long getWatermarkMs() {
    return watermarkMs;
  }

  public void setWatermarkMs(Long watermarkMs) {
    this.watermarkMs = watermarkMs;
  }

  public Long getExpiredTimeMs() {
    return expiredTimeMs;
  }

  public void setExpiredTimeMs(Long expiredTimeMs) {
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

  public Long getMaxDelayMs() {
    return maxDelayMs;
  }

  public void setMaxDelayMs(Long maxDelayMs) {
    this.maxDelayMs = maxDelayMs;
  }

  public EventType getEventType() {
    return eventType;
  }

  public void setEventType(EventType eventType) {
    this.eventType = eventType;
  }

  public void serialize(OutputStream outputStream) throws IOException {
    // TODO: implement serialization
    throw new UnsupportedOperationException("Not implemented yet");
  }

  public static StreamProperties deserialize(InputStream inputStream) throws IOException {
    // TODO: implement deserialization
    throw new UnsupportedOperationException("Not implemented yet");
  }
}
