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
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Identifier;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.LongLiteral;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Node;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.NodeLocation;

import com.google.common.collect.ImmutableList;
import org.apache.tsfile.utils.RamUsageEstimator;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Objects;

public class CapacityEventWindow extends EventWindow {
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(CapacityEventWindow.class);

  public static final String SIZE_PARAMETER_NAME = "SIZE";
  public static final String COLUMNS_PARAMETER_NAME = "COLUMNS";

  private final LongLiteral size;
  @Nullable private final List<Identifier> columns;

  public CapacityEventWindow(
      @Nullable NodeLocation location, LongLiteral size, @Nullable List<Identifier> columns) {
    super(location);
    this.size = size;
    this.columns = columns;
  }

  @Override
  public <R, C> R accept(IAstVisitor<R, C> visitor, C context) {
    return ((CommonQueryAstVisitor<R, C>) visitor).visitCapacityEventWindow(this, context);
  }

  public LongLiteral getSize() {
    return size;
  }

  @Nullable
  public List<Identifier> getColumns() {
    return columns;
  }

  @Override
  public List<? extends Node> getChildren() {
    return ImmutableList.of();
  }

  @Override
  public int hashCode() {
    return Objects.hash(size, columns);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    CapacityEventWindow that = (CapacityEventWindow) obj;
    return Objects.equals(size, that.size) && Objects.equals(columns, that.columns);
  }

  @Override
  public String toString() {
    return "CapacityEventWindow{size=" + size + ", columns=" + columns + "}";
  }

  @Override
  public long ramBytesUsed() {
    return INSTANCE_SIZE
        + AstMemoryEstimationHelper.getEstimatedSizeOfNodeLocation(getLocationInternal())
        + AstMemoryEstimationHelper.getEstimatedSizeOfAccountableObject(size)
        + AstMemoryEstimationHelper.getEstimatedSizeOfNodeList(columns);
  }

  public static List<String> getArgumentNames() {
    return ImmutableList.of(SIZE_PARAMETER_NAME, COLUMNS_PARAMETER_NAME);
  }
}
