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
import java.util.List;

public class CapacityWindow extends StreamWindow {

  private int capacity;
  private List<String> columns; // nullable

  public CapacityWindow(int capacity, List<String> columns) {
    this.capacity = capacity;
    this.columns = columns;
  }

  @Override
  public StreamWindowType getType() {
    return StreamWindowType.CAPACITY;
  }

  public int getCapacity() {
    return capacity;
  }

  public List<String> getColumns() {
    return columns;
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(getType().ordinal(), stream);
    stream.writeInt(capacity);
    BasicStructureSerDeUtil.writeNullableStringList(columns, stream);
  }

  public static CapacityWindow deserialize(ByteBuffer byteBuffer) throws IOException {
    int capacity = byteBuffer.getInt();
    List<String> columns = BasicStructureSerDeUtil.readStringList(byteBuffer);
    return new CapacityWindow(capacity, columns);
  }
}
