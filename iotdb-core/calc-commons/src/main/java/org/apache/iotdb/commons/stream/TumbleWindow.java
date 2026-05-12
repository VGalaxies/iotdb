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

import org.apache.iotdb.commons.utils.BasicStructureSerDeUtil;

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class TumbleWindow extends StreamWindow {

  private String timeColumn;
  private long sizeMs;
  private long originMs;

  public TumbleWindow(String timeColumn, long sizeMs, long originMs) {
    this.timeColumn = timeColumn;
    this.sizeMs = sizeMs;
    this.originMs = originMs;
  }

  @Override
  public StreamWindowType getType() {
    return StreamWindowType.TUMBLE;
  }

  public String getTimeColumn() {
    return timeColumn;
  }

  public long getSizeMs() {
    return sizeMs;
  }

  public long getOriginMs() {
    return originMs;
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(getType().ordinal(), stream);
    BasicStructureSerDeUtil.write(timeColumn, stream);
    stream.writeLong(sizeMs);
    stream.writeLong(originMs);
  }

  public static TumbleWindow deserialize(ByteBuffer byteBuffer) throws IOException {
    String timeColumn = BasicStructureSerDeUtil.readString(byteBuffer);
    long sizeMs = byteBuffer.getLong();
    long originMs = byteBuffer.getLong();
    return new TumbleWindow(timeColumn, sizeMs, originMs);
  }
}
