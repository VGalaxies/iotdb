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
package org.apache.iotdb.streamnode.engine.dispatcher;

import org.apache.iotdb.commons.stream.ListPartitionKey;
import org.apache.iotdb.commons.stream.PartitionKey;
import org.apache.iotdb.metrics.type.Counter;
import org.apache.iotdb.metrics.type.Histogram;
import org.apache.iotdb.metrics.type.Timer;
import org.apache.iotdb.streamnode.engine.task.StreamSubTask.DataSlice;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.function.BiFunction;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class ColumnPartitionedTabletDispatcherTest {

  @Test
  public void testSinglePartitionKeyProducesOneSlice() {
    final Map<PartitionKey, List<DataSlice>> received = new HashMap<>();
    final ColumnPartitionedTabletDispatcher dispatcher =
        newDispatcher(Collections.singletonList("region"), recordingConsumer(received));

    dispatcher.dispatch(buildTablet(new int[] {1, 1, 1}, new int[] {10, 11, 12}), 7L);

    Assert.assertEquals(1, received.size());
    final List<DataSlice> slices = received.get(partitionKey(1));
    Assert.assertNotNull(slices);
    Assert.assertEquals(1, slices.size());
    assertSlice(slices.get(0), 0, 3, 7L);
  }

  @Test
  public void testMultipleContiguousPartitionGroupsProduceSeparateSlices() {
    final Map<PartitionKey, List<DataSlice>> received = new HashMap<>();
    final ColumnPartitionedTabletDispatcher dispatcher =
        newDispatcher(Collections.singletonList("region"), recordingConsumer(received));

    dispatcher.dispatch(buildTablet(new int[] {1, 1, 2, 2, 3}, new int[] {10, 11, 12, 13, 14}), 8L);

    Assert.assertEquals(3, received.size());
    assertSlice(received.get(partitionKey(1)).get(0), 0, 2, 8L);
    assertSlice(received.get(partitionKey(2)).get(0), 2, 4, 8L);
    assertSlice(received.get(partitionKey(3)).get(0), 4, 5, 8L);
  }

  @Test
  public void testInterleavedRepeatedKeysRouteToSameSubTaskAsSeparateSlices() {
    final Map<PartitionKey, List<DataSlice>> received = new HashMap<>();
    final ColumnPartitionedTabletDispatcher dispatcher =
        newDispatcher(Collections.singletonList("region"), recordingConsumer(received));

    dispatcher.dispatch(buildTablet(new int[] {1, 2, 1, 2}, new int[] {10, 11, 12, 13}), 9L);

    Assert.assertEquals(2, received.size());
    final List<DataSlice> regionOne = received.get(partitionKey(1));
    final List<DataSlice> regionTwo = received.get(partitionKey(2));

    Assert.assertEquals(2, regionOne.size());
    assertSlice(regionOne.get(0), 0, 1, 9L);
    assertSlice(regionOne.get(1), 2, 3, 9L);

    Assert.assertEquals(2, regionTwo.size());
    assertSlice(regionTwo.get(0), 1, 2, 9L);
    assertSlice(regionTwo.get(1), 3, 4, 9L);
  }

  @Test
  public void testMissingPartitionColumnUsesNullSegmentAndDoesNotSplitRows() {
    final Map<PartitionKey, List<DataSlice>> received = new HashMap<>();
    final ColumnPartitionedTabletDispatcher dispatcher =
        newDispatcher(Collections.singletonList("missing_region"), recordingConsumer(received));

    dispatcher.dispatch(buildTablet(new int[] {1, 2, 1, 2}, new int[] {10, 11, 12, 13}), 10L);

    final List<DataSlice> slices = received.get(partitionKey((Object) null));
    Assert.assertNotNull(slices);
    Assert.assertEquals(1, slices.size());
    assertSlice(slices.get(0), 0, 4, 10L);
    Assert.assertNull(slices.get(0).getPartitionKey().segmentValue(0));
  }

  @Test
  public void testMultiColumnPartitionKeysProduceIndependentGroups() {
    final Map<PartitionKey, List<DataSlice>> received = new HashMap<>();
    final ColumnPartitionedTabletDispatcher dispatcher =
        newDispatcher(Arrays.asList("region", "status"), recordingConsumer(received));

    dispatcher.dispatch(
        buildTablet(new int[] {1, 1, 1, 2}, new int[] {0, 0, 1, 1}, new int[] {10, 11, 12, 13}),
        11L);

    Assert.assertEquals(3, received.size());
    assertSlice(received.get(partitionKey(1, 0)).get(0), 0, 2, 11L);
    assertSlice(received.get(partitionKey(1, 1)).get(0), 2, 3, 11L);
    assertSlice(received.get(partitionKey(2, 1)).get(0), 3, 4, 11L);
  }

  @Test
  public void testSplitUsesCopiedPartitionKeyValues() {
    final ColumnPartitionedTabletDispatcher dispatcher =
        newDispatcher(Collections.singletonList("region"), (k, slices) -> null);
    final Tablet tablet = buildTablet(new int[] {1, 1}, new int[] {10, 11});

    final List<DataSlice> slices = dispatcher.split(tablet, 12L);
    final PartitionKey partitionKey = slices.get(0).getPartitionKey();
    final int hash = partitionKey.hashCode();
    tablet.addValue("region", 0, 99);

    Assert.assertEquals(1, partitionKey.segmentValue(0));
    Assert.assertEquals(hash, partitionKey.hashCode());
    Assert.assertEquals(partitionKey(1), partitionKey);
  }

  /** Verifies one five-row tablet split into 2/2/1-row groups updates all split metrics. */
  @Test
  public void testSplitRecordsTabletGroupsAndSliceSizes() {
    final Counter tabletCount = mock(Counter.class);
    final Histogram groupCount = mock(Histogram.class);
    final Histogram sliceSize = mock(Histogram.class);
    final Timer splitLatency = mock(Timer.class);
    final ColumnPartitionedTabletDispatcher dispatcher =
        newDispatcher(Collections.singletonList("region"), (k, slices) -> null);
    dispatcher.setMetrics(tabletCount, groupCount, sliceSize, splitLatency);

    dispatcher.split(buildTablet(new int[] {1, 1, 2, 2, 3}, new int[] {10, 11, 12, 13, 14}), 13L);

    verify(tabletCount).inc();
    verify(groupCount).update(3L);
    verify(sliceSize, times(2)).update(2L);
    verify(sliceSize).update(1L);
    verify(splitLatency).updateNanos(anyLong());
    Assert.assertEquals(5D / 3D, dispatcher.getAverageSliceSize(), 0D);

    dispatcher.split(buildTablet(new int[] {4}, new int[] {15}), 14L);
    verify(tabletCount, times(2)).inc();
    Assert.assertEquals(1.5D, dispatcher.getAverageSliceSize(), 0D);
  }

  private ColumnPartitionedTabletDispatcher newDispatcher(
      final List<String> partitionColumns,
      final BiFunction<PartitionKey, List<DataSlice>, Future<?>> consumer) {
    return new ColumnPartitionedTabletDispatcher(partitionColumns, consumer, null);
  }

  private Tablet buildTablet(final int[] regions, final int[] values) {
    return buildTablet(regions, null, values);
  }

  private Tablet buildTablet(final int[] regions, final int[] statuses, final int[] values) {
    final List<IMeasurementSchema> schemas =
        Arrays.asList(
            new MeasurementSchema("region", TSDataType.INT32),
            new MeasurementSchema("status", TSDataType.INT32),
            new MeasurementSchema("value", TSDataType.INT32));
    final Tablet tablet = new Tablet("testDevice", schemas, regions.length);
    for (int row = 0; row < regions.length; row++) {
      tablet.addTimestamp(row, row);
      tablet.addValue("region", row, regions[row]);
      tablet.addValue("status", row, statuses == null ? 0 : statuses[row]);
      tablet.addValue("value", row, values[row]);
    }
    return tablet;
  }

  private ListPartitionKey partitionKey(final Object... values) {
    return new ListPartitionKey(Arrays.asList(values));
  }

  private void assertSlice(
      final DataSlice slice,
      final int expectedStartRow,
      final int expectedEndRow,
      final long tabletId) {
    Assert.assertEquals(expectedStartRow, slice.getStartRow());
    Assert.assertEquals(expectedEndRow, slice.getEndRow());
    Assert.assertEquals(tabletId, slice.getTabletId());
  }

  private BiFunction<PartitionKey, List<DataSlice>, Future<?>> recordingConsumer(
      final Map<PartitionKey, List<DataSlice>> received) {
    return (key, slices) -> {
      received.computeIfAbsent(key, k -> new ArrayList<>()).addAll(slices);
      return CompletableFuture.completedFuture(null);
    };
  }
}
