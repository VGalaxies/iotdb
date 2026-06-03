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

package org.apache.iotdb.streamnode.engine.computation.placeholder;

import org.apache.iotdb.calc.transformation.dag.column.leaf.LeafColumnTransformer;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.stream.PlaceHolderLiteral;
import org.apache.iotdb.streamnode.engine.scheduler.task.StreamDriverContext;
import org.apache.iotdb.streamnode.engine.window.IEventInfo;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.read.common.block.column.BinaryColumn;
import org.apache.tsfile.read.common.block.column.BooleanColumn;
import org.apache.tsfile.read.common.block.column.DoubleColumn;
import org.apache.tsfile.read.common.block.column.FloatColumn;
import org.apache.tsfile.read.common.block.column.IntColumn;
import org.apache.tsfile.read.common.block.column.LongColumn;
import org.apache.tsfile.read.common.block.column.NullColumn;
import org.apache.tsfile.read.common.block.column.RunLengthEncodedColumn;
import org.apache.tsfile.utils.Binary;

import java.util.Optional;
import java.util.OptionalLong;

import static org.apache.iotdb.calc.transformation.dag.util.CastFunctionUtils.ERROR_MSG;

public class StreamPlaceholderColumnTransformer extends LeafColumnTransformer {

  private final StreamDriverContext driverContext;
  private final PlaceHolderLiteral placeholder;

  public StreamPlaceholderColumnTransformer(
      StreamDriverContext driverContext, PlaceHolderLiteral placeholder) {
    super(placeholder.getDataType());
    this.driverContext = driverContext;
    this.placeholder = placeholder;
  }

  @Override
  protected void evaluate() {
    Column column = buildOneValueColumn();
    initializeColumnCache(new RunLengthEncodedColumn(column, input.getPositionCount()));
  }

  @Override
  public void evaluateWithSelection(boolean[] selection) {
    ColumnBuilder builder = returnType.createColumnBuilder(selection.length);
    Column column = new RunLengthEncodedColumn(buildOneValueColumn(), input.getPositionCount());
    for (int i = 0; i < selection.length; i++) {
      if (!selection[i] || column.isNull(i)) {
        builder.appendNull();
      } else {
        builder.write(column, i);
      }
    }
    initializeColumnCache(builder.build());
  }

  private Column buildOneValueColumn() {
    IEventInfo currentEventInfo = driverContext.getCurrentEventInfo();
    PlaceHolderLiteral.Type placeholderType = placeholder.getType();
    switch (placeholderType) {
      case START_TIME:
        return buildOneValueLongColumn(currentEventInfo.getStartTime());
      case END_TIME:
        return buildOneValueLongColumn(currentEventInfo.getEndTime());
      case PREV_TIME:
        return buildOneValueLongColumn(currentEventInfo.getPrevTime());
      case NEXT_TIME:
        return buildOneValueLongColumn(currentEventInfo.getNextTime());
      case ROW_NUM:
        return buildOneValueLongColumn(currentEventInfo.getRowCount());
      case PREV_VALUE:
        return buildOneValueColumn(currentEventInfo.getPrevValue());
      case CURRENT_VALUE:
        return buildOneValueColumn(currentEventInfo.getCurrentValue());
      case N:
        int n = placeholder.getValue();
        return buildOneValueColumn(currentEventInfo.getN(n));
      default:
        throw new UnsupportedOperationException("Unsupported type: " + placeholderType);
    }
  }

  private Column buildOneValueLongColumn(OptionalLong optionalValue) {
    if (!optionalValue.isPresent()) {
      return new NullColumn(1);
    } else {
      return new LongColumn(1, Optional.empty(), new long[] {optionalValue.getAsLong()});
    }
  }

  private Column buildOneValueColumn(Optional<Object> optionalValue) {
    if (!optionalValue.isPresent()) {
      return new NullColumn(1);
    }
    switch (returnType.getTypeEnum()) {
      case INT32:
      case DATE:
        int intValue = (Integer) optionalValue.get();
        return new IntColumn(1, Optional.empty(), new int[] {intValue});
      case INT64:
      case TIMESTAMP:
        long longValue = (Long) optionalValue.get();
        return new LongColumn(1, Optional.empty(), new long[] {longValue});
      case FLOAT:
        float floatValue = (Float) optionalValue.get();
        return new FloatColumn(1, Optional.empty(), new float[] {floatValue});
      case DOUBLE:
        double doubleValue = (Double) optionalValue.get();
        return new DoubleColumn(1, Optional.empty(), new double[] {doubleValue});
      case BOOLEAN:
        boolean booleanValue = (Boolean) optionalValue.get();
        return new BooleanColumn(1, Optional.empty(), new boolean[] {booleanValue});
      case TEXT:
      case STRING:
      case BLOB:
        Binary binaryValue = (Binary) optionalValue.get();
        return new BinaryColumn(1, Optional.empty(), new Binary[] {binaryValue});
      default:
        throw new UnsupportedOperationException(String.format(ERROR_MSG, returnType.getTypeEnum()));
    }
  }
}
