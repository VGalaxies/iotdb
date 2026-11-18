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

package org.apache.iotdb.calc.execution.operator.source.relational.aggregation;

import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.approximate.KllDoublesSketch;
import org.apache.iotdb.calc.i18n.CalcMessages;
import org.apache.iotdb.calc.utils.TypeServices;
import org.apache.iotdb.commons.exception.SemanticException;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.read.common.block.column.BinaryColumn;
import org.apache.tsfile.read.common.block.column.BinaryColumnBuilder;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import javax.annotation.Nullable;

import java.nio.ByteBuffer;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * {@link TableAccumulator} for {@code approx_percentile} backed by {@link KllDoublesSketch}.
 * Preserves an optional {@code configuredPercentage} across {@link #reset()} and {@link
 * #addIntermediate(Column)} so multi-quantile consumers (e.g. P50, P90, P99) can safely merge
 * shared base slices without inheriting the producer's target quantile.
 */
public class KllApproxPercentileAccumulator implements TableAccumulator {

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(KllApproxPercentileAccumulator.class);

  private final TSDataType seriesDataType;
  private final int k;
  private final @Nullable Double configuredPercentage;
  private final TypeServices.NumericResultWriter resultWriter;
  private final KllDoublesSketch sketch;
  private double percentage;

  public KllApproxPercentileAccumulator(TSDataType seriesDataType) {
    this(seriesDataType, KllDoublesSketch.DEFAULT_K, null);
  }

  public KllApproxPercentileAccumulator(
      TSDataType seriesDataType, int k, @Nullable Double configuredPercentage) {
    this.seriesDataType = seriesDataType;
    this.k = k;
    this.configuredPercentage = configuredPercentage;
    this.percentage = configuredPercentage != null ? configuredPercentage : 0.5;
    this.sketch = new KllDoublesSketch(k);
    this.resultWriter =
        TypeServices.NUMERIC_RESULT_WRITER_SERVICE.call(Type.fromTsDataType(seriesDataType));
  }

  public @Nullable Double getConfiguredPercentage() {
    return configuredPercentage;
  }

  public KllDoublesSketch getSketch() {
    return sketch;
  }

  @Override
  public long getEstimatedSize() {
    return INSTANCE_SIZE + sketch.getEstimatedSize();
  }

  @Override
  public TableAccumulator copy() {
    return new KllApproxPercentileAccumulator(seriesDataType, k, configuredPercentage);
  }

  @Override
  public void addInput(Column[] arguments, AggregationMask mask) {
    if (arguments.length != 2 && arguments.length != 3) {
      throw new SemanticException(
          String.format(
              CalcMessages.APPROX_PERCENTILE_REQUIRES_TWO_OR_THREE_ARGUMENTS, arguments.length));
    }
    if (configuredPercentage != null) {
      percentage = configuredPercentage;
    } else {
      int pIdx = arguments.length == 2 ? 1 : 2;
      if (!arguments[pIdx].isNull(0)) {
        percentage = arguments[pIdx].getDouble(0);
      }
    }

    Column valueColumn = arguments[0];
    int positionCount = mask.getPositionCount();
    if (mask.isSelectAll()) {
      for (int i = 0; i < valueColumn.getPositionCount(); i++) {
        if (!valueColumn.isNull(i)) {
          sketch.update(readNumeric(valueColumn, i));
        }
      }
    } else {
      int[] selected = mask.getSelectedPositions();
      for (int i = 0; i < positionCount; i++) {
        int pos = selected[i];
        if (!valueColumn.isNull(pos)) {
          sketch.update(readNumeric(valueColumn, pos));
        }
      }
    }
  }

  private double readNumeric(Column valueColumn, int pos) {
    switch (seriesDataType) {
      case INT32:
      case DATE:
        return valueColumn.getInt(pos);
      case INT64:
      case TIMESTAMP:
        return AbstractApproxPercentileAccumulator.toDoubleExact(valueColumn.getLong(pos));
      case FLOAT:
        return valueColumn.getFloat(pos);
      case DOUBLE:
        return valueColumn.getDouble(pos);
      default:
        throw new SemanticException(
            String.format(
                CalcMessages.APPROX_PERCENTILE_REQUIRES_TWO_OR_THREE_ARGUMENTS, seriesDataType));
    }
  }

  @Override
  public void addIntermediate(Column argument) {
    checkArgument(
        argument instanceof BinaryColumn,
        CalcMessages
            .EXCEPTION_INTERMEDIATE_INPUT_AND_OUTPUT_OF_KLL_APPROX_PERCENTILE_SHOULD_BE_BINARYCOLUMN_B270A4EF);
    for (int i = 0; i < argument.getPositionCount(); i++) {
      if (!argument.isNull(i)) {
        byte[] data = argument.getBinary(i).getValues();
        ByteBuffer buffer = ByteBuffer.wrap(data);
        double srcPercentage = ReadWriteIOUtils.readDouble(buffer);
        if (configuredPercentage == null) {
          this.percentage = srcPercentage;
        }
        sketch.merge(KllDoublesSketch.fromByteBuffer(buffer));
      }
    }
  }

  @Override
  public void evaluateIntermediate(ColumnBuilder columnBuilder) {
    checkArgument(
        columnBuilder instanceof BinaryColumnBuilder,
        CalcMessages
            .EXCEPTION_INTERMEDIATE_INPUT_AND_OUTPUT_OF_KLL_APPROX_PERCENTILE_SHOULD_BE_BINARYCOLUMN_B270A4EF);
    if (sketch.isEmpty()) {
      columnBuilder.appendNull();
      return;
    }
    ByteBuffer buffer = ByteBuffer.allocate(8 + sketch.computeSerializedSize());
    ReadWriteIOUtils.write(percentage, buffer);
    sketch.toByteArray(buffer);
    columnBuilder.writeBinary(new Binary(buffer.array()));
  }

  @Override
  public void evaluateFinal(ColumnBuilder columnBuilder) {
    double result = sketch.getQuantile(percentage);
    if (Double.isNaN(result)) {
      columnBuilder.appendNull();
      return;
    }
    resultWriter.write(columnBuilder, result);
  }

  @Override
  public boolean hasFinalResult() {
    return false;
  }

  @Override
  public void addStatistics(Statistics[] statistics) {
    throw new UnsupportedOperationException(
        CalcMessages.EXCEPTION_KLLAPPROXPERCENTILEACCUMULATOR_DOES_NOT_SUPPORT_STATISTICS_055207A6);
  }

  @Override
  public void reset() {
    sketch.reset();
    if (configuredPercentage != null) {
      percentage = configuredPercentage;
    }
  }
}
