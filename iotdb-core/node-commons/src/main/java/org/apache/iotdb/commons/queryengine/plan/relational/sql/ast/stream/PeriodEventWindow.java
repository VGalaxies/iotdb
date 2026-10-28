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
import org.apache.iotdb.commons.utils.CommonDateTimeUtils;

import com.google.common.collect.ImmutableList;
import org.apache.tsfile.utils.RamUsageEstimator;

import javax.annotation.Nullable;

import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

public class PeriodEventWindow extends EventWindow {
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(PeriodEventWindow.class);

  private static final String PERIOD_PARAMETER_NAME = "PERIOD_TIME";
  private static final String ORIGIN_PARAMETER_NAME = "ORIGIN";
  private static final List<String> argumentNames =
      ImmutableList.of(PERIOD_PARAMETER_NAME, ORIGIN_PARAMETER_NAME);

  private TimeDurationLiteral period;
  @Nullable private LongLiteral origin;

  public PeriodEventWindow(@Nullable NodeLocation location, List<TableFunctionArgument> arguments) {
    super(location);
    this.arguments = arguments;
  }

  public PeriodEventWindow(
      @Nullable NodeLocation location, List<TableFunctionArgument> arguments, ZoneId zoneId) {
    super(location, zoneId);
    this.arguments = arguments;
  }

  public PeriodEventWindow(
      @Nullable NodeLocation location, TimeDurationLiteral period, @Nullable LongLiteral origin) {
    super(location);
    this.period = period;
    this.origin = origin;
  }

  @Override
  public <R, C> R accept(IAstVisitor<R, C> visitor, C context) {
    return ((CommonQueryAstVisitor<R, C>) visitor).visitPeriodEventWindow(this, context);
  }

  public TimeDurationLiteral getPeriod() {
    return period;
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
    return Objects.hash(period, origin);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    PeriodEventWindow that = (PeriodEventWindow) obj;
    return Objects.equals(period, that.period) && Objects.equals(origin, that.origin);
  }

  @Override
  public String toString() {
    return "PeriodEventWindow{period=" + period + ", origin=" + origin + "}";
  }

  @Override
  public long ramBytesUsed() {
    return INSTANCE_SIZE
        + AstMemoryEstimationHelper.getEstimatedSizeOfNodeLocation(getLocationInternal())
        + AstMemoryEstimationHelper.getEstimatedSizeOfAccountableObject(period)
        + AstMemoryEstimationHelper.getEstimatedSizeOfAccountableObject(origin);
  }

  @Override
  public void parseArguments(Map<String, Node> argumentMap) {
    if (!argumentMap.containsKey(PERIOD_PARAMETER_NAME)) {
      throw new SemanticException(
          StreamMessages.EXCEPTION_PERIOD_EVENT_WINDOW_REQUIRES_PERIOD_TIME_ARGUMENT_F42284C5);
    }
    try {
      TimeDurationLiteral period = (TimeDurationLiteral) argumentMap.get(PERIOD_PARAMETER_NAME);
      long totalDuration =
          period.getValue().getTotalDuration(TimestampPrecisionUtils.currPrecision);
      if (totalDuration <= 0) {
        throw new SemanticException(
            StreamMessages
                .EXCEPTION_THE_PERIOD_TIME_OF_THE_PERIOD_EVENT_WINDOW_MUST_BE_POSITIVE_F2159F88);
      }
      LongLiteral origin = parseOrigin(argumentMap.getOrDefault(ORIGIN_PARAMETER_NAME, null));
      validateMonthNonMonthMixing(period);
      this.period = period;
      this.origin = origin;
    } catch (ClassCastException | IllegalArgumentException e) {
      throw new SemanticException(
          StreamMessages.EXCEPTION_INVALID_ARGUMENT_TYPE_FOR_PERIOD_EVENT_WINDOW_E9850AC6);
    }
  }

  /**
   * A PERIOD origin omitted from the statement defaults to the current system time. The value is
   * frozen here at CREATE STREAM analysis time, so the persisted stream config keeps a fixed anchor
   * across restarts.
   */
  @Override
  protected LongLiteral defaultOrigin() {
    return new LongLiteral(String.valueOf(CommonDateTimeUtils.currentTime()));
  }

  /**
   * PERIOD floors every origin - an explicit number, a datetime string, or the current time NOW()
   * folds into - down to a whole second, in currPrecision units. The period itself is already
   * required to be a whole number of seconds, so every grid point stays on a second boundary.
   */
  @Override
  protected LongLiteral parseOrigin(@Nullable Node originNode) {
    LongLiteral origin = super.parseOrigin(originNode);
    if (origin == null) {
      return null;
    }
    long oneSecond = TimestampPrecisionUtils.currPrecision.convert(1, TimeUnit.SECONDS);
    long value = origin.getParsedValue();
    long aligned = value - value % oneSecond;
    return aligned == value ? origin : new LongLiteral(String.valueOf(aligned));
  }

  public List<String> getArgumentNames() {
    return argumentNames;
  }
}
