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
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.LongLiteral;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Node;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.NodeLocation;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.TimeDurationLiteral;

import com.google.common.collect.ImmutableList;
import org.apache.tsfile.utils.RamUsageEstimator;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Objects;

public class PeriodEventWindow extends EventWindow {
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(PeriodEventWindow.class);

  public static final String PERIOD_PARAMETER_NAME = "PERIOD";
  public static final String ORIGIN_PARAMETER_NAME = "ORIGIN";

  private final TimeDurationLiteral period;
  @Nullable private final LongLiteral origin;

  public PeriodEventWindow(
      @Nullable NodeLocation location, TimeDurationLiteral period, @Nullable LongLiteral origin) {
    super(location);
    this.period = period;
    this.origin = origin;
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

  public static List<String> getArgumentNames() {
    return ImmutableList.of(PERIOD_PARAMETER_NAME, ORIGIN_PARAMETER_NAME);
  }
}
