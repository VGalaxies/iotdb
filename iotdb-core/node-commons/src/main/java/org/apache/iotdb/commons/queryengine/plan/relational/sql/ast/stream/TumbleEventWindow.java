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
import org.apache.iotdb.commons.i18n.StreamMessages;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.AstMemoryEstimationHelper;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.CommonQueryAstVisitor;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.IAstVisitor;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.LongLiteral;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Node;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.NodeLocation;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.TableFunctionArgument;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.TimeDurationLiteral;
import org.apache.iotdb.commons.queryengine.utils.TimestampPrecisionUtils;

import com.google.common.collect.ImmutableList;
import org.apache.tsfile.utils.RamUsageEstimator;

import javax.annotation.Nullable;

import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class TumbleEventWindow extends EventWindow {
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(TumbleEventWindow.class);

  private static final String SIZE_PARAMETER_NAME = "SIZE";
  private static final String ORIGIN_PARAMETER_NAME = "ORIGIN";

  private static final List<String> argumentNames =
      ImmutableList.of(SIZE_PARAMETER_NAME, ORIGIN_PARAMETER_NAME);

  private TimeDurationLiteral size;
  @Nullable private LongLiteral origin;

  public TumbleEventWindow(@Nullable NodeLocation location, List<TableFunctionArgument> arguments) {
    super(location);
    this.arguments = arguments;
  }

  public TumbleEventWindow(
      @Nullable NodeLocation location, List<TableFunctionArgument> arguments, ZoneId zoneId) {
    super(location, zoneId);
    this.arguments = arguments;
  }

  public TumbleEventWindow(
      @Nullable NodeLocation location, TimeDurationLiteral size, @Nullable LongLiteral origin) {
    super(location);
    this.size = size;
    this.origin = origin;
  }

  @Override
  public <R, C> R accept(IAstVisitor<R, C> visitor, C context) {
    return ((CommonQueryAstVisitor<R, C>) visitor).visitTumbleEventWindow(this, context);
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
    return Objects.hash(size, origin);
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
    return Objects.equals(size, that.size) && Objects.equals(origin, that.origin);
  }

  @Override
  public String toString() {
    return "TumbleEventWindow{size=" + size + ", origin=" + origin + "}";
  }

  @Override
  public long ramBytesUsed() {
    return INSTANCE_SIZE
        + AstMemoryEstimationHelper.getEstimatedSizeOfAccountableObject(size)
        + AstMemoryEstimationHelper.getEstimatedSizeOfAccountableObject(origin);
  }

  @Override
  public void parseArguments(Map<String, Node> argumentMap) {
    if (!argumentMap.containsKey(SIZE_PARAMETER_NAME)) {
      throw new SemanticException(
          StreamMessages.EXCEPTION_TUMBLE_EVENT_WINDOW_REQUIRES_SIZE_ARGUMENT_CDE35080);
    }
    try {
      TimeDurationLiteral size = (TimeDurationLiteral) argumentMap.get(SIZE_PARAMETER_NAME);
      if (size.getValue().getTotalDuration(TimestampPrecisionUtils.currPrecision) <= 0) {
        throw new SemanticException(
            StreamMessages.EXCEPTION_THE_SIZE_OF_THE_TUMBLE_EVENT_WINDOW_MUST_BE_POSITIVE_C09B224F);
      }
      LongLiteral origin = parseOrigin(argumentMap.getOrDefault(ORIGIN_PARAMETER_NAME, null));
      this.size = size;
      this.origin = origin;
    } catch (ClassCastException | IllegalArgumentException e) {
      throw new SemanticException(
          StreamMessages.EXCEPTION_INVALID_ARGUMENT_TYPE_FOR_TUMBLE_EVENT_WINDOW_F91B3B22);
    }
  }

  /**
   * A TUMBLE origin omitted from the statement defaults to 0 (the epoch), anchoring the window grid
   * at the time origin. Resolved here at CREATE STREAM analysis time, symmetric with the PERIOD
   * default.
   */
  @Override
  protected LongLiteral defaultOrigin() {
    return new LongLiteral("0");
  }

  @Override
  public List<String> getArgumentNames() {
    return argumentNames;
  }
}
