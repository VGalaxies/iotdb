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

import org.apache.iotdb.commons.exception.SemanticException;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.AstMemoryEstimationHelper;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.CommonQueryAstVisitor;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.IAstVisitor;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Identifier;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.LongLiteral;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Node;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.NodeLocation;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Row;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.StringLiteral;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.TableFunctionArgument;

import com.google.common.collect.ImmutableList;
import org.apache.tsfile.utils.RamUsageEstimator;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class CapacityEventWindow extends EventWindow {
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(CapacityEventWindow.class);

  private static final String SIZE_PARAMETER_NAME = "SIZE";
  private static final String COLUMNS_PARAMETER_NAME = "COLUMNS";
  private static final List<String> argumentNames =
      ImmutableList.of(SIZE_PARAMETER_NAME, COLUMNS_PARAMETER_NAME);

  private LongLiteral size;
  @Nullable private List<Identifier> columns;

  public CapacityEventWindow(
      @Nullable NodeLocation location, List<TableFunctionArgument> arguments) {
    super(location);
    this.arguments = arguments;
  }

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

  @Override
  public void parseArguments(Map<String, Node> argumentMap) {
    if (!argumentMap.containsKey(SIZE_PARAMETER_NAME)) {
      throw new SemanticException("Capacity event window requires 'size' argument");
    }
    try {
      LongLiteral size = (LongLiteral) argumentMap.get(SIZE_PARAMETER_NAME);
      /*
        The COLUMNS parameter can be specified in two forms:
          1. columns => ('c1', 'c2') - parsed as List<StringLiteral>
          2. columns => (c1, c2) - parsed as List<Identifier>
        Both forms need to be converted to List<Identifier> for TumbleEventWindow
      */
      List<Identifier> columns = null;
      if (argumentMap.containsKey(COLUMNS_PARAMETER_NAME)) {
        Row row = (Row) argumentMap.get(COLUMNS_PARAMETER_NAME);
        columns = new ArrayList<>();
        for (Expression expr : row.getItems()) {
          if (expr instanceof StringLiteral) {
            columns.add(new Identifier(((StringLiteral) expr).getValue()));
          } else if (expr instanceof Identifier) {
            columns.add((Identifier) expr);
          } else {
            throw new ClassCastException();
          }
        }
      }
      this.size = size;
      this.columns = columns;
    } catch (ClassCastException e) {
      throw new SemanticException("Invalid argument type for capacity event window");
    }
  }

  public List<String> getArgumentNames() {
    return argumentNames;
  }
}
