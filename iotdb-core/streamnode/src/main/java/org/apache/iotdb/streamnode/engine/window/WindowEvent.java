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

import java.util.List;

public class WindowEvent {

  private final long startTime;
  private final long endTime;
  private final List<Object> data;
  private final int rowCount;

  public WindowEvent(long startTime, long endTime, List<Object> data, int rowCount) {
    this.startTime = startTime;
    this.endTime = endTime;
    this.data = data;
    this.rowCount = rowCount;
  }

  public long getStartTime() {
    return startTime;
  }

  public long getEndTime() {
    return endTime;
  }

  public List<Object> getData() {
    return data;
  }

  public int getRowCount() {
    return rowCount;
  }

  @Override
  public String toString() {
    return "WindowEvent{startTime="
        + startTime
        + ", endTime="
        + endTime
        + ", rowCount="
        + rowCount
        + "}";
  }
}
