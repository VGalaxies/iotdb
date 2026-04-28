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
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Identifier;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.LongLiteral;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Node;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.NodeLocation;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.TimeDurationLiteral;

import com.google.common.collect.ImmutableList;
import org.apache.tsfile.utils.RamUsageEstimator;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Objects;

public class HopEventWindow extends EventWindow {
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(HopEventWindow.class);

  @Nullable private final Identifier timeColumn;
  private final TimeDurationLiteral size;
  private final TimeDurationLiteral slide;
  @Nullable private final LongLiteral origin;

  public HopEventWindow(
      @Nullable NodeLocation location,
      @Nullable Identifier timeColumn,
      TimeDurationLiteral size,
      TimeDurationLiteral slide,
      @Nullable LongLiteral origin) {
    super(location);
    this.timeColumn = timeColumn;
    this.size = size;
    this.slide = slide;
    this.origin = origin;
  }

  @Nullable
  public Identifier getTimeColumn() {
    return timeColumn;
  }

  public TimeDurationLiteral getSize() {
    return size;
  }

  public TimeDurationLiteral getSlide() {
    return slide;
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
    return INSTANCE_SIZE
        + AstMemoryEstimationHelper.getEstimatedSizeOfAccountableObject(size)
        + AstMemoryEstimationHelper.getEstimatedSizeOfAccountableObject(slide)
        + AstMemoryEstimationHelper.getEstimatedSizeOfAccountableObject(timeColumn)
        + AstMemoryEstimationHelper.getEstimatedSizeOfAccountableObject(origin);
  }
}
