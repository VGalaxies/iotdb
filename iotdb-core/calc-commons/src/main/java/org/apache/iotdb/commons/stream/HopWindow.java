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
import org.apache.iotdb.commons.utils.BasicStructureSerDeUtil;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class HopWindow extends StreamWindow {

  private final String timeColumn;
  private final long size;
  private final long slide;
  private final long origin;

  public HopWindow(String timeColumn, long size, long slide, long origin) {
    this.timeColumn = timeColumn;
    this.size = size;
    this.slide = slide;
    this.origin = origin;
  }

  @Override
  public StreamWindowType getType() {
    return StreamWindowType.HOP;
  }

  public String getTimeColumn() {
    return timeColumn;
  }

  public long getSize() {
    return size;
  }

  public long getSlide() {
    return slide;
  }

  public long getOrigin() {
    return origin;
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    super.serialize(stream);
    BasicStructureSerDeUtil.write(timeColumn, stream);
    stream.writeLong(size);
    stream.writeLong(slide);
    stream.writeLong(origin);
  }

  @Override
  public String toString() {
    return "HopWindow{"
        + "timeColumn="
        + timeColumn
        + ", size="
        + size
        + TimestampPrecisionUtils.TIMESTAMP_PRECISION
        + ", slide="
        + slide
        + TimestampPrecisionUtils.TIMESTAMP_PRECISION
        + ", origin="
        + origin
        + TimestampPrecisionUtils.TIMESTAMP_PRECISION
        + '}';
  }

  public static HopWindow deserialize(ByteBuffer byteBuffer) throws IOException {
    CommonWindowMembers common = deserializeCommonMembers(byteBuffer);
    String timeColumn = BasicStructureSerDeUtil.readString(byteBuffer);
    long sizeMs = byteBuffer.getLong();
    long slideMs = byteBuffer.getLong();
    long originMs = byteBuffer.getLong();
    HopWindow window = new HopWindow(timeColumn, sizeMs, slideMs, originMs);
    common.applyTo(window);
    return window;
  }
}
