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
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.IAstVisitor;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Identifier;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.LongLiteral;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Node;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.NodeLocation;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.StringLiteral;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Table;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.TableFunctionArgument;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.TimeDurationLiteral;

import com.google.common.collect.ImmutableList;
import org.apache.tsfile.utils.RamUsageEstimator;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;
import java.util.Objects;

public class TumbleEventWindow extends EventWindow {
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(TumbleEventWindow.class);

  private static final String SIZE_PARAMETER_NAME = "SIZE";
  private static final String ORIGIN_PARAMETER_NAME = "ORIGIN";
  private static final String TIME_COL_PARAMETER_NAME = "TIME";

  private static final List<String> argumentNames =
      ImmutableList.of(SIZE_PARAMETER_NAME, ORIGIN_PARAMETER_NAME, TIME_COL_PARAMETER_NAME);

  private TimeDurationLiteral size;
  @Nullable private LongLiteral origin;
  @Nullable private Identifier timeColumn;

  public TumbleEventWindow(@Nullable NodeLocation location, List<TableFunctionArgument> arguments) {
    super(location);
    this.arguments = arguments;
  }

  public TumbleEventWindow(
      @Nullable NodeLocation location,
      TimeDurationLiteral size,
      @Nullable LongLiteral origin,
      @Nullable Identifier timeColumn) {
    super(location);
    this.size = size;
    this.origin = origin;
    this.timeColumn = timeColumn;
  }

  @Override
  public <R, C> R accept(IAstVisitor<R, C> visitor, C context) {
    return ((CommonQueryAstVisitor<R, C>) visitor).visitTumbleEventWindow(this, context);
  }

  @Nullable
  public Identifier getTimeColumn() {
    return timeColumn;
  }

  public TimeDurationLiteral getSize() {
    return size;
  }

  @Nullable
  public LongLiteral getOrigin() {
    return origin;
  }

  @Override
  public List<? extends Node> getChildren() {
    return ImmutableList.of();
  }

  @Override
  public int hashCode() {
    return Objects.hash(timeColumn, size, origin);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    TumbleEventWindow that = (TumbleEventWindow) obj;
    return Objects.equals(timeColumn, that.timeColumn)
        && Objects.equals(size, that.size)
        && Objects.equals(origin, that.origin);
  }

  @Override
  public String toString() {
    return "TumbleEventWindow{size="
        + size
        + ", origin="
        + origin
        + ", timeColumn="
        + timeColumn
        + "}";
  }

  @Override
  public long ramBytesUsed() {
    return INSTANCE_SIZE
        + AstMemoryEstimationHelper.getEstimatedSizeOfAccountableObject(size)
        + AstMemoryEstimationHelper.getEstimatedSizeOfAccountableObject(origin)
        + AstMemoryEstimationHelper.getEstimatedSizeOfAccountableObject(timeColumn);
  }

  @Override
  public void parseArguments(Map<String, Node> argumentMap) {
    if (!argumentMap.containsKey(SIZE_PARAMETER_NAME)) {
      throw new SemanticException("Tumble event window requires 'size' argument");
    }
    try {
      TimeDurationLiteral size = (TimeDurationLiteral) argumentMap.get(SIZE_PARAMETER_NAME);
      LongLiteral origin = (LongLiteral) argumentMap.getOrDefault(ORIGIN_PARAMETER_NAME, null);
      Identifier timeCol = null;
      /*
        The TIME parameter can be specified in two forms:
          1. time => 'ts' - parsed as StringLiteral
          2. time => ts - parsed as Table
        Both forms need to be converted to Identifier for TumbleEventWindow
      */

      if (argumentMap.containsKey(TIME_COL_PARAMETER_NAME)) {
        Node tNode = argumentMap.get(TIME_COL_PARAMETER_NAME);
        if (tNode instanceof StringLiteral) {
          timeCol = new Identifier(((StringLiteral) tNode).getValue());
        } else if (tNode instanceof Table) {
          timeCol = new Identifier(((Table) tNode).getName().toString());
        } else {
          throw new ClassCastException();
        }
      }
      this.size = size;
      this.origin = origin;
      this.timeColumn = timeCol;
    } catch (ClassCastException e) {
      throw new SemanticException("Invalid argument type for tumble event window");
    }
  }

  @Override
  public List<String> getArgumentNames() {
    return argumentNames;
  }
}
