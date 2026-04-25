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

import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Node;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.NodeLocation;

import com.google.common.collect.ImmutableList;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Objects;

public class CapacityEventWindow extends EventWindow {

  private final Expression capacity;
  @Nullable private final List<Expression> columns;

  public CapacityEventWindow(
      @Nullable NodeLocation location, Expression capacity, @Nullable List<Expression> columns) {
    super(location);
    this.capacity = capacity;
    this.columns = columns;
  }

  public Expression getCapacity() {
    return capacity;
  }

  @Nullable
  public List<Expression> getColumns() {
    return columns;
  }

  @Override
  public List<? extends Node> getChildren() {
    return ImmutableList.of();
  }

  @Override
  public int hashCode() {
    return Objects.hash(capacity, columns);
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
    return Objects.equals(capacity, that.capacity) && Objects.equals(columns, that.columns);
  }

  @Override
  public String toString() {
    return "CapacityEventWindow{capacity=" + capacity + ", columns=" + columns + "}";
  }

  @Override
  public long ramBytesUsed() {
    return 0;
  }
}
