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

package org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.stream;

import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.AstMemoryEstimationHelper;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.CommonQueryAstVisitor;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.IAstVisitor;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Literal;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Node;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.TableExpressionType;
import org.apache.iotdb.commons.queryengine.plan.relational.utils.TypeUtil;

import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class PlaceHolderLiteral extends Literal {
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(PlaceHolderLiteral.class);

  private final Type type;
  private int value;
  private org.apache.tsfile.read.common.type.Type dataType;

  public enum Type {
    PREV_VALUE("prev_value"),
    CURRENT_VALUE("current_value"),
    PREV_TIME("prev_time"),
    NEXT_TIME("next_time"),
    START_TIME("start_time"),
    END_TIME("end_time"),
    ROW_NUM("row_num"),
    N("n");

    private final String name;

    Type(String name) {
      this.name = name;
    }

    public String getName() {
      return name;
    }
  }

  public PlaceHolderLiteral(Type type) {
    super(null);
    this.type = requireNonNull(type, "type is null");
  }

  public Type getType() {
    return type;
  }

  public PlaceHolderLiteral withValue(int value) {
    this.value = value;
    return this;
  }

  public int getValue() {
    return value;
  }

  public org.apache.tsfile.read.common.type.Type getDataType() {
    return dataType;
  }

  public void setDataType(org.apache.tsfile.read.common.type.Type dataType) {
    this.dataType = requireNonNull(dataType, "dataType is null");
  }

  public <R, C> R accept(IAstVisitor<R, C> visitor, C context) {
    return ((CommonQueryAstVisitor<R, C>) visitor).visitPlaceHolderLiteral(this, context);
  }

  @Override
  public Object getTsValue() {
    throw new UnsupportedOperationException("PlaceHolderLiteral doesn't support to insert");
  }

  @Override
  public int hashCode() {
    return Objects.hash(type, value, dataType);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if ((obj == null) || (getClass() != obj.getClass())) {
      return false;
    }
    PlaceHolderLiteral that = (PlaceHolderLiteral) obj;
    return type == that.type && value == that.value && Objects.equals(dataType, that.dataType);
  }

  @Override
  public boolean shallowEquals(Node other) {
    if (!sameClass(this, other)) {
      return false;
    }
    PlaceHolderLiteral that = (PlaceHolderLiteral) other;
    return type == that.type && value == that.value && Objects.equals(dataType, that.dataType);
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(this.type.ordinal(), stream);
    ReadWriteIOUtils.write(this.value, stream);
    ReadWriteIOUtils.write(this.dataType != null, stream);
    if (this.dataType != null) {
      TypeUtil.serialize(this.dataType, stream);
    }
  }

  public PlaceHolderLiteral(ByteBuffer byteBuffer) {
    super(null);
    this.type = Type.values()[(ReadWriteIOUtils.readInt(byteBuffer))];
    this.value = ReadWriteIOUtils.readInt(byteBuffer);
    if (ReadWriteIOUtils.readBool(byteBuffer)) {
      this.dataType = TypeUtil.deserialize(byteBuffer);
    }
  }

  @Override
  public TableExpressionType getExpressionType() {
    return TableExpressionType.PLACEHOLDER_LITERAL;
  }

  @Override
  public long ramBytesUsed() {
    return INSTANCE_SIZE
        + AstMemoryEstimationHelper.getEstimatedSizeOfNodeLocation(getLocationInternal())
        + RamUsageEstimator.sizeOfObject(type)
        + (dataType == null ? 0 : RamUsageEstimator.sizeOfObject(dataType));
  }
}
