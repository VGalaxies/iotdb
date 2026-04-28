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
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.DoubleLiteral;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Identifier;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Node;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.NodeLocation;

import com.google.common.collect.ImmutableList;
import org.apache.tsfile.utils.RamUsageEstimator;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Objects;

public class VariationEventWindow extends EventWindow {
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(VariationEventWindow.class);

  private final Identifier column;
  @Nullable private final DoubleLiteral delta;

  public VariationEventWindow(
      @Nullable NodeLocation location, Identifier column, @Nullable DoubleLiteral delta) {
    super(location);
    this.column = column;
    this.delta = delta;
  }

  public Identifier getColumn() {
    return column;
  }

  @Nullable
  public DoubleLiteral getDelta() {
    return delta;
  }

  @Override
  public List<? extends Node> getChildren() {
    return ImmutableList.of();
  }

  @Override
  public int hashCode() {
    return Objects.hash(column, delta);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    VariationEventWindow that = (VariationEventWindow) obj;
    return Objects.equals(column, that.column) && Objects.equals(delta, that.delta);
  }

  @Override
  public String toString() {
    return "VariationEventWindow{column=" + column + ", delta=" + delta + "}";
  }

  @Override
  public long ramBytesUsed() {
    return INSTANCE_SIZE
        + AstMemoryEstimationHelper.getEstimatedSizeOfNodeLocation(getLocationInternal())
        + AstMemoryEstimationHelper.getEstimatedSizeOfAccountableObject(column)
        + AstMemoryEstimationHelper.getEstimatedSizeOfAccountableObject(delta);
  }
}
