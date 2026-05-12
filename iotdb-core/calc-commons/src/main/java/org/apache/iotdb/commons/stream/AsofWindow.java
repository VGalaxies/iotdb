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

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class AsofWindow extends StreamWindow {

  public enum AfterMatchMode {
    KEEP,
    CLEAR
  }

  private AfterMatchMode afterMatchMode;

  public AsofWindow(AfterMatchMode afterMatchMode) {
    this.afterMatchMode = afterMatchMode;
  }

  @Override
  public StreamWindowType getType() {
    return StreamWindowType.ASOF;
  }

  public AfterMatchMode getAfterMatchMode() {
    return afterMatchMode;
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(getType().ordinal(), stream);
    stream.writeByte(afterMatchMode.ordinal());
  }

  public static AsofWindow deserialize(ByteBuffer byteBuffer) throws IOException {
    int modeOrdinal = byteBuffer.get();
    if (modeOrdinal < 0 || modeOrdinal >= AfterMatchMode.values().length) {
      throw new IOException("unsupported ASOF after-match mode ordinal: " + modeOrdinal);
    }
    return new AsofWindow(AfterMatchMode.values()[modeOrdinal]);
  }
}
