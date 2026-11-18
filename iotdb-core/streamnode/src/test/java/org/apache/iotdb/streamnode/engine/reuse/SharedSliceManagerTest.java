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

package org.apache.iotdb.streamnode.engine.reuse;

import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.AggregationMask;
import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.ApproxCountDistinctAccumulator;
import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.AvgAccumulator;
import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.CountAllAccumulator;
import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.KllApproxPercentileAccumulator;
import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.SumAccumulator;
import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.TableAggregator;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.AggregationNode.Step;
import org.apache.iotdb.commons.stream.ListPartitionKey;
import org.apache.iotdb.commons.stream.PartitionKey;
import org.apache.iotdb.streamnode.engine.reuse.SharedSliceManager.ConsumedSliceSet;
import org.apache.iotdb.streamnode.engine.reuse.SliceCompatibilitySignature.AggSpec;
import org.apache.iotdb.streamnode.engine.reuse.SliceCompatibilitySignature.SliceTaskDescriptor;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.column.DoubleColumn;
import org.apache.tsfile.read.common.block.column.DoubleColumnBuilder;
import org.apache.tsfile.read.common.block.column.LongColumnBuilder;
import org.apache.tsfile.read.common.block.column.RunLengthEncodedColumn;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

public class SharedSliceManagerTest {

  private static SliceTaskDescriptor makeDesc(
      String name,
      long windowMs,
      long originMs,
      long watermarkMs,
      long latenessMs,
      String preFilter,
      List<String> partCols,
      String sketchType,
      Double pct,
      long retentionMs,
      double costThreshold) {
    List<AggSpec> aggs =
        Arrays.asList(
            new AggSpec("count_all", Collections.emptyList(), false, null, TSDataType.INT64, false),
            new AggSpec(
                "sum", Collections.singletonList("s1"), false, null, TSDataType.DOUBLE, false),
            new AggSpec(
                "avg", Collections.singletonList("s1"), false, null, TSDataType.DOUBLE, false),
            new AggSpec(
                "approx_count_distinct",
                Collections.singletonList("s1"),
                false,
                null,
                TSDataType.INT64,
                false),
            new AggSpec(
                "approx_percentile",
                Collections.singletonList("s1"),
                false,
                pct,
                TSDataType.DOUBLE,
                false));
    return new SliceTaskDescriptor(
        name,
        "testdb",
        "t1",
        preFilter,
        partCols,
        true,
        windowMs,
        originMs,
        watermarkMs,
        latenessMs,
        0L,
        "",
        sketchType,
        200,
        retentionMs,
        costThreshold,
        true,
        true,
        aggs);
  }

  private static List<TableAggregator> buildRealAggregators(double pct) {
    return Arrays.asList(
        new TableAggregator(
            new CountAllAccumulator(),
            Step.SINGLE,
            TSDataType.INT64,
            Collections.emptyList(),
            OptionalInt.empty()),
        new TableAggregator(
            new SumAccumulator(TSDataType.DOUBLE),
            Step.SINGLE,
            TSDataType.DOUBLE,
            Collections.singletonList(0),
            OptionalInt.empty()),
        new TableAggregator(
            new AvgAccumulator(TSDataType.DOUBLE),
            Step.SINGLE,
            TSDataType.DOUBLE,
            Collections.singletonList(0),
            OptionalInt.empty()),
        new TableAggregator(
            new ApproxCountDistinctAccumulator(TSDataType.DOUBLE),
            Step.SINGLE,
            TSDataType.INT64,
            Collections.singletonList(0),
            OptionalInt.empty()),
        new TableAggregator(
            new KllApproxPercentileAccumulator(TSDataType.DOUBLE, 200, pct),
            Step.SINGLE,
            TSDataType.DOUBLE,
            Arrays.asList(0, 1),
            OptionalInt.empty()));
  }

  @Test
  public void testSix10sTo1mMergeAccuracyKllMultiQuantileAndHll() {
    SharedSliceManager manager = new SharedSliceManager();
    SliceTaskDescriptor prod10s =
        makeDesc(
            "s_10s",
            10_000L,
            0L,
            0L,
            0L,
            "",
            Collections.singletonList("tag1"),
            "kll",
            0.50,
            60_000L,
            0.0);
    SliceTaskDescriptor cons1m =
        makeDesc(
            "s_1m",
            60_000L,
            0L,
            0L,
            0L,
            "",
            Collections.singletonList("tag1"),
            "kll",
            0.99,
            60_000L,
            0.0);
    manager.registerStream(prod10s);
    manager.registerStream(cons1m);

    PartitionKey pk = new ListPartitionKey(Collections.singletonList("d1"));
    long t0 = 1_000_000L;

    List<TableAggregator> prodAggs = buildRealAggregators(0.50);
    List<TableAggregator> consAggs = buildRealAggregators(0.99);
    List<TableAggregator> directAggs = buildRealAggregators(0.99);

    Column pct50Col =
        new RunLengthEncodedColumn(new DoubleColumn(1, Optional.empty(), new double[] {0.50}), 500);
    Column pct99Col =
        new RunLengthEncodedColumn(new DoubleColumn(1, Optional.empty(), new double[] {0.99}), 500);

    for (int sub = 0; sub < 6; sub++) {
      prodAggs.forEach(TableAggregator::reset);
      double[] vals = new double[500];
      for (int i = 0; i < 500; i++) {
        vals[i] = sub * 500 + i + 1;
      }
      Column valCol = new DoubleColumn(500, Optional.empty(), vals);
      AggregationMask mask = AggregationMask.createSelectAll(500);

      for (int k = 0; k < 4; k++) {
        prodAggs.get(k).getAccumulator().addInput(new Column[] {valCol}, mask);
        directAggs.get(k).getAccumulator().addInput(new Column[] {valCol}, mask);
      }
      prodAggs.get(4).getAccumulator().addInput(new Column[] {valCol, pct50Col}, mask);
      directAggs.get(4).getAccumulator().addInput(new Column[] {valCol, pct99Col}, mask);

      Column[] interCols = SharedSliceManager.evaluateIntermediateColumns(prodAggs);
      long ver =
          manager.publishSlice(
              "s_10s", pk, sub * 10_000L, (sub + 1) * 10_000L, 500L, false, interCols, t0);
      Assert.assertEquals(1L, ver);
    }

    Optional<ConsumedSliceSet> acquired =
        manager.tryAcquireSubSlices("s_1m", pk, 0L, 60_000L, false);
    Assert.assertTrue(acquired.isPresent());
    Assert.assertFalse(acquired.get().isDuplicateVersion());
    Assert.assertEquals(6, acquired.get().getSubSlices().size());

    int[] mapping = acquired.get().getBinding().getConsumerToProducerAggIndex();
    for (SharedBaseSlice slice : acquired.get().getSubSlices()) {
      for (int i = 0; i < consAggs.size(); i++) {
        consAggs
            .get(i)
            .getAccumulator()
            .addIntermediate(slice.getIntermediateColumns()[mapping[i]]);
      }
    }

    // Verify exact aggregates match direct full-window compute, and KLL P99/HLL stay accurate
    LongColumnBuilder countMerged = new LongColumnBuilder(null, 1);
    LongColumnBuilder countDirect = new LongColumnBuilder(null, 1);
    consAggs.get(0).evaluate(countMerged);
    directAggs.get(0).evaluate(countDirect);
    Assert.assertEquals(countDirect.build().getLong(0), countMerged.build().getLong(0));

    DoubleColumnBuilder sumMerged = new DoubleColumnBuilder(null, 1);
    DoubleColumnBuilder sumDirect = new DoubleColumnBuilder(null, 1);
    consAggs.get(1).evaluate(sumMerged);
    directAggs.get(1).evaluate(sumDirect);
    Assert.assertEquals(sumDirect.build().getDouble(0), sumMerged.build().getDouble(0), 1e-6);

    DoubleColumnBuilder avgMerged = new DoubleColumnBuilder(null, 1);
    DoubleColumnBuilder avgDirect = new DoubleColumnBuilder(null, 1);
    consAggs.get(2).evaluate(avgMerged);
    directAggs.get(2).evaluate(avgDirect);
    Assert.assertEquals(avgDirect.build().getDouble(0), avgMerged.build().getDouble(0), 1e-6);

    LongColumnBuilder hllMerged = new LongColumnBuilder(null, 1);
    consAggs.get(3).evaluate(hllMerged);
    long estDistinct = hllMerged.build().getLong(0);
    Assert.assertTrue(Math.abs(estDistinct - 3000L) < 150L);

    DoubleColumnBuilder p99Merged = new DoubleColumnBuilder(null, 1);
    consAggs.get(4).evaluate(p99Merged);
    double estP99 = p99Merged.build().getDouble(0);
    // Producer published P50 KLL sketch, consumer evaluates P99 over [1..3000] -> ~2970
    Assert.assertEquals(2970.0, estP99, 60.0);
  }

  @Test
  public void testCompatibilityConstraintsMultiHopAndLiveFallback() {
    SharedSliceManager manager = new SharedSliceManager();
    SliceTaskDescriptor s1s =
        makeDesc("s_1s", 1_000L, 0L, 0L, 0L, "", Collections.emptyList(), "kll", 0.5, 60_000L, 0.0);
    SliceTaskDescriptor s10s =
        makeDesc(
            "s_10s", 10_000L, 0L, 0L, 0L, "", Collections.emptyList(), "kll", 0.9, 60_000L, 0.0);
    SliceTaskDescriptor s1m =
        makeDesc(
            "s_1m", 60_000L, 0L, 0L, 0L, "", Collections.emptyList(), "kll", 0.99, 60_000L, 0.0);
    SliceTaskDescriptor incompatibleOrigin =
        makeDesc(
            "s_1m_shifted",
            60_000L,
            3_500L,
            0L,
            0L,
            "",
            Collections.emptyList(),
            "kll",
            0.99,
            60_000L,
            0.0);
    SliceTaskDescriptor highThreshold =
        makeDesc(
            "s_1m_high_cost",
            60_000L,
            0L,
            0L,
            0L,
            "",
            Collections.emptyList(),
            "kll",
            0.99,
            60_000L,
            0.9999);

    manager.registerStream(s1s);
    manager.registerStream(s10s);
    manager.registerStream(s1m);
    manager.registerStream(incompatibleOrigin);
    manager.registerStream(highThreshold);

    // 10s binds to 1s; 1m binds to 10s (largest compatible base window); shifted origin & high
    // threshold have no binding
    Assert.assertEquals("s_1s", manager.getConsumerBinding("s_10s").getProducerStream());
    Assert.assertEquals("s_10s", manager.getConsumerBinding("s_1m").getProducerStream());
    Assert.assertNull(manager.getConsumerBinding("s_1m_shifted"));
    Assert.assertNull(manager.getConsumerBinding("s_1m_high_cost"));

    // Unregister intermediate 10s -> 1m dynamically re-binds to 1s; unregister 1s -> null
    // (fallback)
    manager.unregisterStream("s_10s");
    Assert.assertEquals("s_1s", manager.getConsumerBinding("s_1m").getProducerStream());
    manager.unregisterStream("s_1s");
    Assert.assertNull(manager.getConsumerBinding("s_1m"));
  }

  @Test
  public void testAlgorithm43VersionDedupLateRecomputeAndTtlEvictionFallback() {
    SharedSliceManager manager = new SharedSliceManager();
    SliceTaskDescriptor p =
        makeDesc(
            "s_10s", 10_000L, 0L, 0L, 5_000L, "", Collections.emptyList(), "kll", 0.5, 1_000L, 0.0);
    SliceTaskDescriptor c =
        makeDesc(
            "s_20s", 20_000L, 0L, 0L, 5_000L, "", Collections.emptyList(), "kll", 0.9, 1_000L, 0.0);
    manager.registerStream(p);
    manager.registerStream(c);

    List<TableAggregator> prodAggs = buildRealAggregators(0.5);
    Column pct50Col =
        new RunLengthEncodedColumn(new DoubleColumn(1, Optional.empty(), new double[] {0.50}), 10);
    Column valCol =
        new DoubleColumn(10, Optional.empty(), new double[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10});
    for (int i = 0; i < 4; i++) {
      prodAggs
          .get(i)
          .getAccumulator()
          .addInput(new Column[] {valCol}, AggregationMask.createSelectAll(10));
    }
    prodAggs
        .get(4)
        .getAccumulator()
        .addInput(new Column[] {valCol, pct50Col}, AggregationMask.createSelectAll(10));

    long now = 10_000L;
    manager.publishSlice(
        "s_10s",
        null,
        0L,
        10_000L,
        10L,
        false,
        SharedSliceManager.evaluateIntermediateColumns(prodAggs),
        now);
    manager.publishSlice(
        "s_10s",
        null,
        10_000L,
        20_000L,
        10L,
        false,
        SharedSliceManager.evaluateIntermediateColumns(prodAggs),
        now);

    // Before consumer consumes, refCount == 1 protects slices from eviction even past retention!
    Assert.assertEquals(0, manager.evictExpired(now + 5_000L));

    Optional<ConsumedSliceSet> firstRead =
        manager.tryAcquireSubSlices("s_20s", null, 0L, 20_000L, false);
    Assert.assertTrue(firstRead.isPresent());
    Assert.assertFalse(firstRead.get().isDuplicateVersion());

    // Replaying recompute with identical versions is flagged duplicateVersion
    Optional<ConsumedSliceSet> replayRead =
        manager.tryAcquireSubSlices("s_20s", null, 0L, 20_000L, true);
    Assert.assertTrue(replayRead.isPresent());
    Assert.assertTrue(replayRead.get().isDuplicateVersion());

    // Publishing late recompute on [0, 10s) bumps stateVersion to 2 -> recompute is NOT duplicate
    long v2 =
        manager.publishSlice(
            "s_10s",
            null,
            0L,
            10_000L,
            12L,
            true,
            SharedSliceManager.evaluateIntermediateColumns(prodAggs),
            now + 100L);
    Assert.assertEquals(2L, v2);
    Optional<ConsumedSliceSet> updatedRead =
        manager.tryAcquireSubSlices("s_20s", null, 0L, 20_000L, true);
    Assert.assertTrue(updatedRead.isPresent());
    Assert.assertFalse(updatedRead.get().isDuplicateVersion());

    // After eviction past retention, manager falls back cleanly (Optional.empty) instead of
    // treating
    // evicted windows as empty
    Assert.assertEquals(2, manager.evictExpired(now + 5_000L));
    Assert.assertFalse(manager.tryAcquireSubSlices("s_20s", null, 0L, 20_000L, false).isPresent());
  }
}
