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
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.LongLiteral;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Node;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.NodeLocation;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.StringLiteral;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.TableFunctionArgument;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.TimeDurationLiteral;
import org.apache.iotdb.commons.queryengine.utils.DateTimeUtils;

import javax.annotation.Nullable;

import java.time.ZoneId;
import java.util.List;
import java.util.Map;

public abstract class EventWindow extends Node {
  protected List<TableFunctionArgument> arguments;
  protected final ZoneId zoneId;

  protected EventWindow(@Nullable NodeLocation location) {
    this(location, ZoneId.systemDefault());
  }

  protected EventWindow(@Nullable NodeLocation location, ZoneId zoneId) {
    super(location);
    this.zoneId = zoneId;
  }

  public List<TableFunctionArgument> getArguments() {
    return arguments;
  }

  public abstract List<String> getArgumentNames();

  public abstract void parseArguments(Map<String, Node> argumentMap);

  @Nullable
  protected LongLiteral parseOrigin(@Nullable Node originNode) {
    if (originNode == null) {
      return defaultOrigin();
    }
    if (originNode instanceof LongLiteral) {
      return (LongLiteral) originNode;
    }
    if (originNode instanceof StringLiteral) {
      try {
        return new LongLiteral(
            String.valueOf(
                DateTimeUtils.convertDatetimeStrToLong(
                    ((StringLiteral) originNode).getValue(), zoneId)));
      } catch (RuntimeException e) {
        throw new IllegalArgumentException(e);
      }
    }
    throw new SemanticException(
        StreamMessages
            .EXCEPTION_THE_ORIGIN_OF_THE_EVENT_WINDOW_MUST_BE_INTEGER_OR_DATE_TIME_D74A8A7B);
  }

  /**
   * Origin to use when the statement omits it. The default returns {@code null} ("leave unset");
   * PERIOD overrides it with the current system time, TUMBLE overrides it with 0 (the epoch).
   */
  @Nullable
  protected LongLiteral defaultOrigin() {
    return null;
  }

  protected void validateMonthNonMonthMixing(TimeDurationLiteral duration) {
    if (duration.getValue().monthDuration != 0 && duration.getValue().nonMonthDuration != 0) {
      throw new SemanticException(
          StreamMessages
              .EXCEPTION_SIMULTANEOUS_SETTING_OF_MONTHLY_AND_NON_MONTHLY_INTERVALS_IS_NOT_SUPPORTED_C62486CB);
    }
  }
}
