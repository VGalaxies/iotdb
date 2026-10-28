/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied.  See the License for the specific
 * language governing permissions and limitations under the License.
 */

package org.apache.iotdb.commons.stream;

import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.stream.StreamLiteral;
import org.apache.iotdb.commons.utils.BasicStructureSerDeUtil;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public abstract class StreamWindow {

  private List<String> eventInputColumns;

  /**
   * Types of {@link #eventInputColumns}, resolved against the table schema at stream creation and
   * shipped with the task. The window engine's output types must not be derived from the pushed
   * tablets: subscription tablets carry each INSERT's column set, and alternating partial-column
   * INSERTs never produce a tablet with the full scan column set. {@code null} when the stream
   * carries no event scan columns.
   */
  private List<TSDataType> eventInputColumnTypes;

  // Whether relation placeholder ${rows} is used in the calculation plan
  private boolean containsRowsPlaceholder;
  // Literal placeholders used in the calculation plan
  private Set<StreamLiteral.Type> literalPlaceholders = new HashSet<>();

  public abstract StreamWindowType getType();

  public List<String> getEventInputColumns() {
    return eventInputColumns;
  }

  public void setEventInputColumns(List<String> eventInputColumns) {
    this.eventInputColumns = eventInputColumns;
  }

  public List<TSDataType> getEventInputColumnTypes() {
    return eventInputColumnTypes;
  }

  public void setEventInputColumnTypes(List<TSDataType> eventInputColumnTypes) {
    this.eventInputColumnTypes = eventInputColumnTypes;
  }

  public boolean containsRowsPlaceholder() {
    return containsRowsPlaceholder;
  }

  public void setContainsRowsPlaceholder(boolean containsRowsPlaceholder) {
    this.containsRowsPlaceholder = containsRowsPlaceholder;
  }

  public Set<StreamLiteral.Type> getLiteralPlaceholders() {
    return literalPlaceholders;
  }

  public void setLiteralPlaceholders(Set<StreamLiteral.Type> literalPlaceholders) {
    this.literalPlaceholders = literalPlaceholders;
  }

  public void serialize(DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(getType().ordinal(), stream);
    BasicStructureSerDeUtil.writeNullableStringList(eventInputColumns, stream);
    writeNullableTSDataTypeList(eventInputColumnTypes, stream);
    ReadWriteIOUtils.write(containsRowsPlaceholder, stream);
    ReadWriteIOUtils.write(literalPlaceholders.size(), stream);
    for (StreamLiteral.Type type : literalPlaceholders) {
      ReadWriteIOUtils.write(type.ordinal(), stream);
    }
  }

  public static StreamWindow deserialize(ByteBuffer byteBuffer) throws IOException {
    int typeOrdinal = ReadWriteIOUtils.readInt(byteBuffer);
    if (typeOrdinal < 0 || typeOrdinal >= StreamWindowType.values().length) {
      throw new IOException("unsupported stream window type ordinal: " + typeOrdinal);
    }
    StreamWindowType type = StreamWindowType.values()[typeOrdinal];
    switch (type) {
      case PERIOD:
        return PeriodWindow.deserialize(byteBuffer);
      case TUMBLE:
        return TumbleWindow.deserialize(byteBuffer);
      case HOP:
        return HopWindow.deserialize(byteBuffer);
      case VARIATION:
        return VariationWindow.deserialize(byteBuffer);
      case CAPACITY:
        return CapacityWindow.deserialize(byteBuffer);
      case ASOF:
        return AsofWindow.deserialize(byteBuffer);
      default:
        throw new IOException("unsupported stream window type: " + type);
    }
  }

  /**
   * The common window members written by {@link #serialize(DataOutputStream)}, read back by every
   * subclass's {@code deserialize} via {@link #deserializeCommonMembers(ByteBuffer)} before its own
   * fields.
   */
  protected static final class CommonWindowMembers {

    private final List<String> eventInputColumns;
    private final List<TSDataType> eventInputColumnTypes;
    private final boolean containsRowsPlaceholder;
    private final Set<StreamLiteral.Type> literalPlaceholders;

    private CommonWindowMembers(
        List<String> eventInputColumns,
        List<TSDataType> eventInputColumnTypes,
        boolean containsRowsPlaceholder,
        Set<StreamLiteral.Type> literalPlaceholders) {
      this.eventInputColumns = eventInputColumns;
      this.eventInputColumnTypes = eventInputColumnTypes;
      this.containsRowsPlaceholder = containsRowsPlaceholder;
      this.literalPlaceholders = literalPlaceholders;
    }

    /** Applies the members to a freshly constructed (subclass-specific) window. */
    void applyTo(StreamWindow window) {
      window.setEventInputColumns(eventInputColumns);
      window.setEventInputColumnTypes(eventInputColumnTypes);
      window.setContainsRowsPlaceholder(containsRowsPlaceholder);
      window.setLiteralPlaceholders(literalPlaceholders);
    }
  }

  /**
   * Reads the common members (event input columns and their schema-resolved types, rows placeholder
   * flag, literal placeholders) in {@link #serialize(DataOutputStream)}'s order; subclass
   * deserializers call this first, then read their own fields.
   */
  protected static CommonWindowMembers deserializeCommonMembers(ByteBuffer byteBuffer) {
    List<String> eventInputColumns = BasicStructureSerDeUtil.readStringList(byteBuffer);
    List<TSDataType> eventInputColumnTypes = readNullableTSDataTypeList(byteBuffer);
    boolean containsRowsPlaceholder = ReadWriteIOUtils.readBool(byteBuffer);
    int literalPlaceholdersCount = ReadWriteIOUtils.readInt(byteBuffer);
    Set<StreamLiteral.Type> literalPlaceholders = new HashSet<>();
    for (int i = 0; i < literalPlaceholdersCount; i++) {
      int ordinal = ReadWriteIOUtils.readInt(byteBuffer);
      literalPlaceholders.add(StreamLiteral.Type.values()[ordinal]);
    }
    return new CommonWindowMembers(
        eventInputColumns, eventInputColumnTypes, containsRowsPlaceholder, literalPlaceholders);
  }

  /** Writes a nullable {@link TSDataType} list: {@code -1} for null, otherwise size + ordinals. */
  private static void writeNullableTSDataTypeList(
      List<TSDataType> dataTypes, DataOutputStream stream) throws IOException {
    if (dataTypes == null) {
      ReadWriteIOUtils.write(-1, stream);
      return;
    }
    ReadWriteIOUtils.write(dataTypes.size(), stream);
    for (TSDataType dataType : dataTypes) {
      ReadWriteIOUtils.write(dataType.ordinal(), stream);
    }
  }

  /** Reads a list written by {@link #writeNullableTSDataTypeList}; returns null for {@code -1}. */
  private static List<TSDataType> readNullableTSDataTypeList(ByteBuffer byteBuffer) {
    int size = ReadWriteIOUtils.readInt(byteBuffer);
    if (size < 0) {
      return null;
    }
    List<TSDataType> dataTypes = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      int ordinal = ReadWriteIOUtils.readInt(byteBuffer);
      dataTypes.add(TSDataType.values()[ordinal]);
    }
    return dataTypes;
  }
}
