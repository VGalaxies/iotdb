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

package org.apache.iotdb.db.queryengine.plan.relational.sql.ast.stream;

import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Node;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.NodeLocation;

import com.google.common.collect.ImmutableList;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Objects;

public class HopEventWindow extends EventWindow {

  @Nullable private final Expression timeColumn;
  private final Expression size;
  private final Expression slide;
  @Nullable private final Expression origin;

  public HopEventWindow(
      @Nullable NodeLocation location,
      @Nullable Expression timeColumn,
      Expression size,
      Expression slide,
      @Nullable Expression origin) {
    super(location);
    this.timeColumn = timeColumn;
    this.size = size;
    this.slide = slide;
    this.origin = origin;
  }

  @Nullable
  public Expression getTimeColumn() {
    return timeColumn;
  }

  public Expression getSize() {
    return size;
  }

  public Expression getSlide() {
    return slide;
  }

  @Nullable
  public Expression getOrigin() {
    return origin;
  }

  @Override
  public List<? extends Node> getChildren() {
    return ImmutableList.of();
  }

  @Override
  public int hashCode() {
    return Objects.hash(timeColumn, size, slide, origin);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    HopEventWindow that = (HopEventWindow) obj;
    return Objects.equals(timeColumn, that.timeColumn)
        && Objects.equals(size, that.size)
        && Objects.equals(slide, that.slide)
        && Objects.equals(origin, that.origin);
  }

  @Override
  public String toString() {
    return "HopEventWindow{timeColumn="
        + timeColumn
        + ", size="
        + size
        + ", slide="
        + slide
        + ", origin="
        + origin
        + "}";
  }

  @Override
  public long ramBytesUsed() {
    return 0;
  }
}
