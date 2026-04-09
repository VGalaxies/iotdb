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
import java.io.OutputStream;

public class PeriodWindow extends StreamWindow {

  private long periodMs;
  private long originMs;

  public PeriodWindow(long periodMs, long originMs) {
    this.periodMs = periodMs;
    this.originMs = originMs;
  }

  @Override
  public StreamWindowType getType() {
    return StreamWindowType.PERIOD;
  }

  public long getPeriodMs() {
    return periodMs;
  }

  public long getOriginMs() {
    return originMs;
  }

  @Override
  public void serialize(OutputStream outputStream) throws IOException {
    // TODO: implement serialization
    throw new UnsupportedOperationException("Not implemented yet");
  }
}
