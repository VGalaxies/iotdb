/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.streamnode.engine.sink;

import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;
import java.util.stream.Collectors;

public class BatchAccumulatorTest {

  @Test
  public void testBuildTabletWithMappedTimeColumn() {
    BatchAccumulator accumulator =
        new BatchAccumulator(
            "target_table",
            Arrays.asList("event_time", "tag_id", "value"),
            Arrays.asList(TSDataType.TIMESTAMP, TSDataType.INT64, TSDataType.INT64),
            Arrays.asList(ColumnCategory.TIME, ColumnCategory.TAG, ColumnCategory.FIELD),
            Arrays.asList(2, 0, 1),
            new SinkPipelineConfig());

    TsBlockBuilder builder =
        new TsBlockBuilder(Arrays.asList(TSDataType.INT64, TSDataType.INT64, TSDataType.TIMESTAMP));
    builder.getTimeColumnBuilder().writeLong(10L);
    builder.getColumnBuilder(0).writeLong(1001L);
    builder.getColumnBuilder(1).writeLong(11L);
    builder.getColumnBuilder(2).writeLong(10000L);
    builder.declarePosition();
    builder.getTimeColumnBuilder().writeLong(20L);
    builder.getColumnBuilder(0).writeLong(1002L);
    builder.getColumnBuilder(1).writeLong(22L);
    builder.getColumnBuilder(2).writeLong(20000L);
    builder.declarePosition();

    accumulator.add(new SinkEntry(Optional.of(builder.build()), -1L, false, 0, 0));

    Tablet tablet = accumulator.buildTabletAndReset().getTablet();

    Assert.assertEquals("target_table", tablet.getTableName());
    Assert.assertEquals(2, tablet.getRowSize());
    Assert.assertEquals(10000L, tablet.getTimestamp(0));
    Assert.assertEquals(20000L, tablet.getTimestamp(1));
    Assert.assertEquals(Arrays.asList("tag_id", "value"), getMeasurementNames(tablet));
    Assert.assertEquals(
        Arrays.asList(ColumnCategory.TAG, ColumnCategory.FIELD), tablet.getColumnTypes());
    Assert.assertEquals(1001L, tablet.getValue(0, 0));
    Assert.assertEquals(11L, tablet.getValue(0, 1));
    Assert.assertEquals(1002L, tablet.getValue(1, 0));
    Assert.assertEquals(22L, tablet.getValue(1, 1));
  }

  @Test
  public void testBuildTabletWithDefaultColumnMapping() {
    BatchAccumulator accumulator =
        new BatchAccumulator(
            "target_table",
            Arrays.asList("event_time", "value"),
            Arrays.asList(TSDataType.TIMESTAMP, TSDataType.INT64),
            Arrays.asList(ColumnCategory.TIME, ColumnCategory.FIELD),
            null,
            new SinkPipelineConfig());

    TsBlock tsBlock = createTwoColumnLongTsBlock(10000L, 11L);

    accumulator.add(new SinkEntry(Optional.of(tsBlock), -1L, false, 0, 0));

    Tablet tablet = accumulator.buildTabletAndReset().getTablet();

    Assert.assertEquals(1, tablet.getRowSize());
    Assert.assertEquals(10000L, tablet.getTimestamp(0));
    Assert.assertEquals(Collections.singletonList("value"), getMeasurementNames(tablet));
    Assert.assertEquals(Collections.singletonList(ColumnCategory.FIELD), tablet.getColumnTypes());
    Assert.assertEquals(11L, tablet.getValue(0, 0));
  }

  @Test
  public void testBuildTabletWithDuplicatedTsBlockColumnMapping() {
    BatchAccumulator accumulator =
        new BatchAccumulator(
            "target_table",
            Arrays.asList("tag_id", "event_time", "value", "value_copy"),
            Arrays.asList(
                TSDataType.INT64, TSDataType.TIMESTAMP, TSDataType.INT64, TSDataType.INT64),
            Arrays.asList(
                ColumnCategory.TAG,
                ColumnCategory.TIME,
                ColumnCategory.FIELD,
                ColumnCategory.FIELD),
            Arrays.asList(1, 0, 2, 2),
            new SinkPipelineConfig());

    TsBlockBuilder builder =
        new TsBlockBuilder(Arrays.asList(TSDataType.TIMESTAMP, TSDataType.INT64, TSDataType.INT64));
    builder.getTimeColumnBuilder().writeLong(1L);
    builder.getColumnBuilder(0).writeLong(10000L);
    builder.getColumnBuilder(1).writeLong(1001L);
    builder.getColumnBuilder(2).writeLong(11L);
    builder.declarePosition();
    builder.getTimeColumnBuilder().writeLong(2L);
    builder.getColumnBuilder(0).writeLong(20000L);
    builder.getColumnBuilder(1).writeLong(1002L);
    builder.getColumnBuilder(2).writeLong(22L);
    builder.declarePosition();

    accumulator.add(new SinkEntry(Optional.of(builder.build()), -1L, false, 0, 0));

    Tablet tablet = accumulator.buildTabletAndReset().getTablet();

    Assert.assertEquals(2, tablet.getRowSize());
    Assert.assertEquals(10000L, tablet.getTimestamp(0));
    Assert.assertEquals(20000L, tablet.getTimestamp(1));
    Assert.assertEquals(
        Arrays.asList("tag_id", "value", "value_copy"), getMeasurementNames(tablet));
    Assert.assertEquals(
        Arrays.asList(ColumnCategory.TAG, ColumnCategory.FIELD, ColumnCategory.FIELD),
        tablet.getColumnTypes());
    Assert.assertEquals(1001L, tablet.getValue(0, 0));
    Assert.assertEquals(11L, tablet.getValue(0, 1));
    Assert.assertEquals(11L, tablet.getValue(0, 2));
    Assert.assertEquals(1002L, tablet.getValue(1, 0));
    Assert.assertEquals(22L, tablet.getValue(1, 1));
    Assert.assertEquals(22L, tablet.getValue(1, 2));
  }

  /**
   * {@code discardAndReset} must hand back every buffered entry (including commit-only entries
   * without a TsBlock) so the caller can settle their memory and commit accounting.
   */
  @Test
  public void testDiscardAndResetReturnsBufferedEntries() {
    BatchAccumulator accumulator =
        new BatchAccumulator(
            "target_table",
            Arrays.asList("event_time", "value"),
            Arrays.asList(TSDataType.TIMESTAMP, TSDataType.INT64),
            Arrays.asList(ColumnCategory.TIME, ColumnCategory.FIELD),
            null,
            new SinkPipelineConfig());

    SinkEntry withBlock =
        new SinkEntry(Optional.of(createTwoColumnLongTsBlock(10000L, 11L)), 1L, true, 0, 64L);
    SinkEntry commitOnly = new SinkEntry(Optional.empty(), 1L, true, 0, 0L);
    accumulator.add(withBlock);
    accumulator.add(commitOnly);
    Assert.assertFalse(accumulator.isEmpty());

    java.util.List<SinkEntry> discarded = accumulator.discardAndReset();

    Assert.assertEquals(2, discarded.size());
    Assert.assertSame(withBlock, discarded.get(0));
    Assert.assertSame(commitOnly, discarded.get(1));
    Assert.assertTrue(accumulator.isEmpty());
  }

  private TsBlock createTwoColumnLongTsBlock(long timeValue, long fieldValue) {
    TsBlockBuilder builder =
        new TsBlockBuilder(Arrays.asList(TSDataType.TIMESTAMP, TSDataType.INT64));
    builder.getTimeColumnBuilder().writeLong(1L);
    builder.getColumnBuilder(0).writeLong(timeValue);
    builder.getColumnBuilder(1).writeLong(fieldValue);
    builder.declarePosition();
    return builder.build();
  }

  private java.util.List<String> getMeasurementNames(Tablet tablet) {
    return tablet.getSchemas().stream()
        .map(IMeasurementSchema::getMeasurementName)
        .collect(Collectors.toList());
  }
}
