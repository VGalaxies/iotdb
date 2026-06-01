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

import org.apache.iotdb.commons.stream.PartitionKey;
import org.apache.iotdb.commons.stream.PeriodWindow;
import org.apache.iotdb.commons.stream.TabletColumnPartitionKey;
import org.apache.iotdb.streamnode.engine.task.StreamSubTask;
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
import java.util.concurrent.atomic.AtomicInteger;

public class ColumnPartitionedTabletDispatcherTest {

  @Test
  public void testSinglePartitionKeyProducesOneSlice() {
    final Map<PartitionKey, RecordingSubTask> subTasks = new HashMap<>();
    final ColumnPartitionedTabletDispatcher dispatcher =
        newDispatcher(Collections.singletonList("region"), subTasks);

    dispatcher.dispatch(buildTablet(new int[] {1, 1, 1}, new int[] {10, 11, 12}), 7L);

    final RecordingSubTask task = subTasks.get(partitionKey(1));
    Assert.assertNotNull(task);
    Assert.assertEquals(1, task.receivedSlices.size());
    assertSlice(task.receivedSlices.get(0), 0, 3, 7L);
  }

  @Test
  public void testMultipleContiguousPartitionGroupsProduceSeparateSlices() {
    final Map<PartitionKey, RecordingSubTask> subTasks = new HashMap<>();
    final ColumnPartitionedTabletDispatcher dispatcher =
        newDispatcher(Collections.singletonList("region"), subTasks);

    dispatcher.dispatch(buildTablet(new int[] {1, 1, 2, 2, 3}, new int[] {10, 11, 12, 13, 14}), 8L);

    Assert.assertEquals(3, subTasks.size());
    assertSlice(subTasks.get(partitionKey(1)).receivedSlices.get(0), 0, 2, 8L);
    assertSlice(subTasks.get(partitionKey(2)).receivedSlices.get(0), 2, 4, 8L);
    assertSlice(subTasks.get(partitionKey(3)).receivedSlices.get(0), 4, 5, 8L);
  }

  @Test
  public void testInterleavedRepeatedKeysRouteToSameSubTaskAsSeparateSlices() {
    final Map<PartitionKey, RecordingSubTask> subTasks = new HashMap<>();
    final ColumnPartitionedTabletDispatcher dispatcher =
        newDispatcher(Collections.singletonList("region"), subTasks);

    dispatcher.dispatch(buildTablet(new int[] {1, 2, 1, 2}, new int[] {10, 11, 12, 13}), 9L);

    Assert.assertEquals(2, subTasks.size());
    final RecordingSubTask regionOne = subTasks.get(partitionKey(1));
    final RecordingSubTask regionTwo = subTasks.get(partitionKey(2));

    Assert.assertEquals(2, regionOne.receivedSlices.size());
    assertSlice(regionOne.receivedSlices.get(0), 0, 1, 9L);
    assertSlice(regionOne.receivedSlices.get(1), 2, 3, 9L);

    Assert.assertEquals(2, regionTwo.receivedSlices.size());
    assertSlice(regionTwo.receivedSlices.get(0), 1, 2, 9L);
    assertSlice(regionTwo.receivedSlices.get(1), 3, 4, 9L);
  }

  @Test
  public void testMissingPartitionColumnUsesNullSegmentAndDoesNotSplitRows() {
    final Map<PartitionKey, RecordingSubTask> subTasks = new HashMap<>();
    final ColumnPartitionedTabletDispatcher dispatcher =
        newDispatcher(Collections.singletonList("missing_region"), subTasks);

    dispatcher.dispatch(buildTablet(new int[] {1, 2, 1, 2}, new int[] {10, 11, 12, 13}), 10L);

    final RecordingSubTask task = subTasks.get(partitionKey((Object) null));
    Assert.assertNotNull(task);
    Assert.assertEquals(1, task.receivedSlices.size());
    assertSlice(task.receivedSlices.get(0), 0, 4, 10L);
    Assert.assertNull(task.receivedSlices.get(0).getPartitionKey().segmentValue(0));
  }

  @Test
  public void testMultiColumnPartitionKeysProduceIndependentGroups() {
    final Map<PartitionKey, RecordingSubTask> subTasks = new HashMap<>();
    final ColumnPartitionedTabletDispatcher dispatcher =
        newDispatcher(Arrays.asList("region", "status"), subTasks);

    dispatcher.dispatch(
        buildTablet(new int[] {1, 1, 1, 2}, new int[] {0, 0, 1, 1}, new int[] {10, 11, 12, 13}),
        11L);

    Assert.assertEquals(3, subTasks.size());
    assertSlice(subTasks.get(partitionKey(1, 0)).receivedSlices.get(0), 0, 2, 11L);
    assertSlice(subTasks.get(partitionKey(1, 1)).receivedSlices.get(0), 2, 3, 11L);
    assertSlice(subTasks.get(partitionKey(2, 1)).receivedSlices.get(0), 3, 4, 11L);
  }

  @Test
  public void testSplitUsesCopiedPartitionKeyValues() {
    final ColumnPartitionedTabletDispatcher dispatcher =
        newDispatcher(Collections.singletonList("region"), new HashMap<>());
    final Tablet tablet = buildTablet(new int[] {1, 1}, new int[] {10, 11});

    final List<DataSlice> slices = dispatcher.split(tablet, 12L);
    final PartitionKey partitionKey = slices.get(0).getPartitionKey();
    final int hash = partitionKey.hashCode();
    tablet.addValue("region", 0, 99);

    Assert.assertEquals(1, partitionKey.segmentValue(0));
    Assert.assertEquals(hash, partitionKey.hashCode());
    Assert.assertEquals(partitionKey(1), partitionKey);
  }

  @Test
  public void testProcessUsesConfigurableConsumerHook() {
    final Map<PartitionKey, RecordingSubTask> subTasks = new HashMap<>();
    final ColumnPartitionedTabletDispatcher dispatcher =
        newDispatcher(Collections.singletonList("region"), subTasks);
    final AtomicInteger consumedSliceCount = new AtomicInteger(0);

    dispatcher.setConsumer((slices, subTask) -> consumedSliceCount.addAndGet(slices.size()));
    dispatcher.process(buildTablet(new int[] {1, 1, 2}, new int[] {10, 11, 12}), 13L);

    Assert.assertEquals(2, subTasks.size());
    Assert.assertEquals(2, consumedSliceCount.get());
    Assert.assertTrue(subTasks.values().stream().allMatch(task -> task.receivedSlices.isEmpty()));
  }

  private ColumnPartitionedTabletDispatcher newDispatcher(
      final List<String> partitionColumns, final Map<PartitionKey, RecordingSubTask> subTasks) {
    return new ColumnPartitionedTabletDispatcher(
        partitionColumns,
        partitionKey -> subTasks.computeIfAbsent(partitionKey, RecordingSubTask::new));
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

  private TabletColumnPartitionKey partitionKey(final Object... values) {
    return new TabletColumnPartitionKey(Arrays.asList(values));
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

  private static class RecordingSubTask extends StreamSubTask {

    private final List<DataSlice> receivedSlices = new ArrayList<>();

    private RecordingSubTask(final PartitionKey partitionKey) {
      super(partitionKey, new PeriodWindow(1, 0));
    }

    @Override
    public Future<Void> offer(final List<DataSlice> dataSlices) {
      receivedSlices.addAll(dataSlices);
      return CompletableFuture.completedFuture(null);
    }
  }
}
