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

import org.apache.iotdb.commons.queryengine.utils.TimestampPrecisionUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class TumbleWindow extends StreamWindow {

  private final long size;
  private final long origin;

  public TumbleWindow(long size, long origin) {
    this.size = size;
    this.origin = origin;
  }

  @Override
  public StreamWindowType getType() {
    return StreamWindowType.TUMBLE;
  }

  public long getSize() {
    return size;
  }

  public long getOrigin() {
    return origin;
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    super.serialize(stream);
    stream.writeLong(size);
    stream.writeLong(origin);
  }

  @Override
  public String toString() {
    return "TumbleWindow{"
        + "size="
        + size
        + TimestampPrecisionUtils.TIMESTAMP_PRECISION
        + ", origin="
        + origin
        + TimestampPrecisionUtils.TIMESTAMP_PRECISION
        + '}';
  }

  public static TumbleWindow deserialize(ByteBuffer byteBuffer) throws IOException {
    CommonWindowMembers common = deserializeCommonMembers(byteBuffer);
    long size = byteBuffer.getLong();
    long origin = byteBuffer.getLong();
    TumbleWindow window = new TumbleWindow(size, origin);
    common.applyTo(window);
    return window;
  }
}
