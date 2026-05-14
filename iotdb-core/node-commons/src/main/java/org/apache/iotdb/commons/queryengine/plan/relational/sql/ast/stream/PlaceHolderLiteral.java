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
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.TableExpressionType;

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

  public enum Type {
    PREV_VALUE("prev_value"),
    NEXT_VALUE("next_value"),
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

  public <R, C> R accept(IAstVisitor<R, C> visitor, C context) {
    return ((CommonQueryAstVisitor<R, C>) visitor).visitPlaceHolderLiteral(this, context);
  }

  @Override
  public Object getTsValue() {
    return type;
  }

  @Override
  public int hashCode() {
    return Objects.hash(type);
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
    return (type == that.type);
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(this.type.getName(), stream);
  }

  public PlaceHolderLiteral(ByteBuffer byteBuffer) {
    super(null);
    this.type = Type.valueOf(ReadWriteIOUtils.readString(byteBuffer));
  }

  @Override
  public TableExpressionType getExpressionType() {
    return TableExpressionType.PLACEHOLDER_LITERAL;
  }

  @Override
  public long ramBytesUsed() {
    return INSTANCE_SIZE
        + AstMemoryEstimationHelper.getEstimatedSizeOfNodeLocation(getLocationInternal())
        + RamUsageEstimator.sizeOfObject(type);
  }
}
