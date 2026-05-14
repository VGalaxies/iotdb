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
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Node;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.NodeLocation;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.TableFunctionArgument;

import com.google.common.collect.ImmutableList;
import org.apache.tsfile.utils.RamUsageEstimator;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;
import java.util.Objects;

public class AsofEventWindow extends EventWindow {
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(AsofEventWindow.class);

  private static final List<String> argumentNames = ImmutableList.of();

  public enum AfterMatchMode {
    KEEP,
    CLEAR
  }

  @Nullable private AfterMatchMode afterMatchMode;

  public AsofEventWindow(@Nullable NodeLocation location, List<TableFunctionArgument> arguments) {
    super(location);
    this.arguments = arguments;
  }

  public AsofEventWindow(@Nullable NodeLocation location, @Nullable AfterMatchMode afterMatchMode) {
    super(location);
    this.afterMatchMode = afterMatchMode;
  }

  @Override
  public <R, C> R accept(IAstVisitor<R, C> visitor, C context) {
    return ((CommonQueryAstVisitor<R, C>) visitor).visitAsofEventWindow(this, context);
  }

  @Nullable
  public AfterMatchMode getAfterMatchMode() {
    return afterMatchMode;
  }

  @Override
  public List<? extends Node> getChildren() {
    return ImmutableList.of();
  }

  @Override
  public int hashCode() {
    return Objects.hash(afterMatchMode);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    AsofEventWindow that = (AsofEventWindow) obj;
    return Objects.equals(afterMatchMode, that.afterMatchMode);
  }

  @Override
  public String toString() {
    return "AsofEventWindow{afterMatchMode=" + afterMatchMode + "}";
  }

  @Override
  public long ramBytesUsed() {
    return INSTANCE_SIZE
        + AstMemoryEstimationHelper.getEstimatedSizeOfNodeLocation(getLocationInternal())
        + RamUsageEstimator.sizeOfObject(afterMatchMode);
  }

  @Override
  public List<String> getArgumentNames() {
    return argumentNames;
  }

  @Override
  public void parseArguments(Map<String, Node> argumentMap) {}
}
