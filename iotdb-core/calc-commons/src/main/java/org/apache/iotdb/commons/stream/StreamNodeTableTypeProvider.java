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

import org.apache.iotdb.commons.queryengine.plan.analyze.ITableTypeProvider;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.commons.queryengine.plan.relational.utils.TypeUtil;

import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class StreamNodeTableTypeProvider implements ITableTypeProvider {

  private final Map<Symbol, Type> tableModelTypes;

  public StreamNodeTableTypeProvider(Map<Symbol, Type> tableModelTypes) {
    this.tableModelTypes =
        new HashMap<>(requireNonNull(tableModelTypes, "tableModelTypes is null"));
  }

  @Override
  public Type getTableModelType(Symbol symbol) {
    requireNonNull(symbol, "symbol is null");

    Type type = tableModelTypes.get(symbol);
    checkArgument(type != null, "no type found for symbol '%s' in TypeProvider", symbol);

    return type;
  }

  @Override
  public boolean isSymbolExist(Symbol symbol) {
    return tableModelTypes.containsKey(symbol);
  }

  @Override
  public void putTableModelType(Symbol symbol, Type type) {
    requireNonNull(symbol, "symbol is null");

    tableModelTypes.put(symbol, type);
  }

  @Override
  public Map<Symbol, Type> allTableModelTypes() {
    // types may be a HashMap, so creating an ImmutableMap here would add extra cost when allTypes
    // gets called frequently
    return Collections.unmodifiableMap(tableModelTypes);
  }

  public void serialize(ByteBuffer byteBuffer) {
    ReadWriteIOUtils.write(tableModelTypes.size(), byteBuffer);
    for (Map.Entry<Symbol, Type> entry : tableModelTypes.entrySet()) {
      ReadWriteIOUtils.write(entry.getKey().getName(), byteBuffer);
      TypeUtil.serialize(entry.getValue(), byteBuffer);
    }
  }

  public void serialize(DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(tableModelTypes.size(), stream);
    for (Map.Entry<Symbol, Type> entry : tableModelTypes.entrySet()) {
      ReadWriteIOUtils.write(entry.getKey().getName(), stream);
      TypeUtil.serialize(entry.getValue(), stream);
    }
  }

  public static StreamNodeTableTypeProvider deserialize(ByteBuffer byteBuffer) {
    int mapSize = ReadWriteIOUtils.readInt(byteBuffer);
    Map<Symbol, Type> tableModelTypes = new HashMap<>(mapSize);
    while (mapSize > 0) {
      tableModelTypes.put(
          new Symbol(ReadWriteIOUtils.readString(byteBuffer)), TypeUtil.deserialize(byteBuffer));
      mapSize--;
    }
    return new StreamNodeTableTypeProvider(tableModelTypes);
  }
}
