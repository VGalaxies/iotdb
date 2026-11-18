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
import org.apache.iotdb.commons.exception.SemanticException;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.column.BinaryColumnBuilder;
import org.apache.tsfile.read.common.block.column.DoubleColumn;
import org.apache.tsfile.read.common.block.column.DoubleColumnBuilder;
import org.apache.tsfile.read.common.block.column.RunLengthEncodedColumn;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Optional;

public class KllDoublesSketchTest {

  @Test
  public void testKllAccuracyAndAssociativeMerge() {
    KllDoublesSketch full = new KllDoublesSketch(200);
    KllDoublesSketch[] subSketches = new KllDoublesSketch[6];
    for (int i = 0; i < 6; i++) {
      subSketches[i] = new KllDoublesSketch(200);
    }

    int total = 60_000;
    for (int i = 0; i < total; i++) {
      double val = i;
      full.update(val);
      subSketches[i / 10_000].update(val);
    }

    KllDoublesSketch merged = new KllDoublesSketch(200);
    for (KllDoublesSketch sub : subSketches) {
      ByteBuffer buf = ByteBuffer.allocate(sub.computeSerializedSize());
      sub.toByteArray(buf);
      buf.flip();
      merged.merge(KllDoublesSketch.fromByteBuffer(buf));
    }

    Assert.assertEquals(total, merged.getN());
    Assert.assertEquals(0.0, merged.getMinValue(), 1e-9);
    Assert.assertEquals(total - 1.0, merged.getMaxValue(), 1e-9);

    for (double q : new double[] {0.1, 0.5, 0.9, 0.99}) {
      double exact = q * (total - 1);
      double estMerged = merged.getQuantile(q);
      double estFull = full.getQuantile(q);
      Assert.assertTrue(
          "Merged quantile error too high at q=" + q + ", est=" + estMerged + ", exact=" + exact,
          Math.abs(estMerged - exact) / total < 0.02);
      Assert.assertTrue(
          "Full quantile error too high at q=" + q + ", est=" + estFull + ", exact=" + exact,
          Math.abs(estFull - exact) / total < 0.02);
    }
  }

  @Test
  public void testMultiQuantileAccumulatorSharesProducerIntermediateSlice() {
    KllApproxPercentileAccumulator p50Producer =
        new KllApproxPercentileAccumulator(TSDataType.DOUBLE, 200, 0.50);
    KllApproxPercentileAccumulator p99Consumer =
        new KllApproxPercentileAccumulator(TSDataType.DOUBLE, 200, 0.99);

    int rows = 1000;
    double[] vals = new double[rows];
    for (int i = 0; i < rows; i++) {
      vals[i] = i + 1;
    }
    Column valCol = new DoubleColumn(rows, Optional.empty(), vals);
    Column pctCol =
        new RunLengthEncodedColumn(
            new DoubleColumn(1, Optional.empty(), new double[] {0.50}), rows);
    p50Producer.addInput(new Column[] {valCol, pctCol}, AggregationMask.createSelectAll(rows));

    BinaryColumnBuilder interBuilder = new BinaryColumnBuilder(null, 1);
    p50Producer.evaluateIntermediate(interBuilder);
    Column intermediateCol = interBuilder.build();

    // Consumer configured for P99 merges producer's P50 intermediate slice without losing 0.99
    p99Consumer.addIntermediate(intermediateCol);
    DoubleColumnBuilder outBuilder = new DoubleColumnBuilder(null, 1);
    p99Consumer.evaluateFinal(outBuilder);
    Column finalCol = outBuilder.build();
    Assert.assertEquals(990.0, finalCol.getDouble(0), 25.0);

    // Reset keeps configuredPercentage
    p99Consumer.reset();
    Assert.assertEquals(Double.valueOf(0.99), p99Consumer.getConfiguredPercentage());
    Assert.assertTrue(p99Consumer.getSketch().isEmpty());
  }

  @Test(expected = SemanticException.class)
  public void testInvalidKRejects() {
    new KllDoublesSketch(4);
  }
}
