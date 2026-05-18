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

import static java.util.Objects.requireNonNull;

public class VariationWindow extends StreamWindow {

  private String column;
  private double delta;

  public VariationWindow(String column, double delta) {
    this.column = requireNonNull(column, "column is null");
    this.delta = delta;
  }

  @Override
  public StreamWindowType getType() {
    return StreamWindowType.VARIATION;
  }

  public String getColumn() {
    return column;
  }

  public double getDelta() {
    return delta;
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(getType().ordinal(), stream);
    BasicStructureSerDeUtil.write(column, stream);
    stream.writeDouble(delta);
  }

  public static VariationWindow deserialize(ByteBuffer byteBuffer) throws IOException {
    String column = BasicStructureSerDeUtil.readString(byteBuffer);
    if (column == null) {
      throw new IOException("unexpected null column in variation window payload");
    }
    double delta = byteBuffer.getDouble();
    return new VariationWindow(column, delta);
  }
}
