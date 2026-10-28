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

package org.apache.iotdb.streamnode.engine.window;

import org.apache.iotdb.commons.stream.StreamProperties;
import org.apache.iotdb.commons.stream.TumbleWindow;
import org.apache.iotdb.streamnode.engine.task.StreamSubTask.DataSlice;

import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.write.record.Tablet;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collections;
import java.util.TreeMap;

import static org.apache.iotdb.streamnode.engine.window.WindowEngineTestUtils.createDriverContext;

/**
 * Scale regression guards for the two many-devices shapes that stress the per-row and per-device
 * costs of the window row store, print-friendly so a manual run doubles as a benchmark report:
 *
 * <ul>
 *   <li><b>Performance (O(devices) per row)</b>: 10,000 devices x 10 shared timestamps, 100,000
 *       rows pushed as one batch in incremental mode (no {@code ROW_NUM} literal) - every row takes
 *       the pending-count path; a per-row walk over all devices instead made this workload O(rows x
 *       devices), measured at 99.1 s. The guard bounds the wall time far above the row-local cost
 *       and far below the quadratic one.
 *   <li><b>Memory (whole first chunk per device)</b>: 10,000 devices x 1 row in one window - a full
 *       1024-row first chunk per column (plus a skiplist node per row) billed ~16.9 KB/device, ~161
 *       MiB total. The doubling first chunk (16 rows) plus the lazy index must keep the per-device
 *       estimate in the kilobyte range (a fixed skeleton + one 16-row 2-column chunk + the map
 *       entry and key).
 * </ul>
 *
 * The numbers are printed ({@code System.out}) for the manual benchmark run; the asserts are the
 * regression guards and deliberately loose (bounds 10-30x above the current cost) so CI jitter
 * cannot flake them while a genuine regression (99 s / 16.9 KB per device) still fails loudly.
 */
public class TumbleWindowEngineScaleTest {

  private static final long WINDOW_SIZE_MS = 100_000L;
  private static final long ORIGIN_MS = 0L;
  private static final int DEVICE_COUNT = 10_000;

  /**
   * Throughput guard: 10,000 devices sharing 10 timestamps, one 100,000-row batch. Every row runs
   * the per-row pending-count path; watermark 0 (watermark = max timestamp) keeps the incremental
   * emissions firing as the timestamps advance.
   */
  @Test
  public void testMultiDeviceSameTimestampThroughput() {
    int tsCount = 10;
    int rowCount = DEVICE_COUNT * tsCount;

    TumbleWindow window = new TumbleWindow(WINDOW_SIZE_MS, ORIGIN_MS);
    window.setEventInputColumns(Arrays.asList("time", "value"));
    window.setEventInputColumnTypes(Arrays.asList(TSDataType.INT64, TSDataType.INT64));
    window.setContainsRowsPlaceholder(true);
    TumbleWindowEngine engine =
        new TumbleWindowEngine(
            window, createDriverContext(), new StreamProperties(0, 0, 0, 0, true), null);

    // Timestamp-major interleave: all devices' row at ts, then ts+1 - the same-timestamp-across-
    // devices shape that made the per-row device walk quadratic.
    long[] timestamps = new long[rowCount];
    int[] devices = new int[rowCount];
    int row = 0;
    for (int ts = 1; ts <= tsCount; ts++) {
      for (int device = 0; device < DEVICE_COUNT; device++) {
        timestamps[row] = ts;
        devices[row] = device;
        row++;
      }
    }
    DataSlice slice = deviceSlice(timestamps, devices);

    long start = System.nanoTime();
    engine.push(Collections.singletonList(slice));
    long elapsedMs = (System.nanoTime() - start) / 1_000_000;

    WindowRowStore store = onlyStore(engine);
    Assert.assertEquals(rowCount, store.size());
    Assert.assertEquals(DEVICE_COUNT, store.deviceCount());
    System.out.printf(
        "[scale-perf] %d rows / %d devices / %d shared timestamps: %d ms (%.0f rows/s)%n",
        rowCount, DEVICE_COUNT, tsCount, elapsedMs, rowCount / Math.max(1.0, elapsedMs / 1000.0));
    // A per-row device walk on this workload measured 99.1 s. The current path is row-local
    // (O(log n) upsert + O(1) count); 30 s leaves an order of magnitude of CI headroom while still
    // failing a reintroduced walk.
    Assert.assertTrue("push of 100k rows took " + elapsedMs + " ms", elapsedMs < 30_000);
    engine.drop();
  }

  /**
   * Memory guard: 10,000 devices x 1 row, all in one still-open window. One row per device arrives
   * in order, so no device ever builds its (lazy) time index - the per-device memory must be the
   * doubling first chunk plus the charged fixed overhead (the list's skeleton, the map entry and
   * the retained device key), roughly one and a half kilobytes - not a whole 1024-row chunk.
   */
  @Test
  public void testManyDevicesOneRowMemory() {
    TumbleWindow window = new TumbleWindow(WINDOW_SIZE_MS, ORIGIN_MS);
    window.setEventInputColumns(Arrays.asList("time", "value"));
    window.setEventInputColumnTypes(Arrays.asList(TSDataType.INT64, TSDataType.INT64));
    window.setContainsRowsPlaceholder(true);
    TumbleWindowEngine engine =
        new TumbleWindowEngine(
            window, createDriverContext(), new StreamProperties(0, 0, 0, 0, true), null);

    long[] timestamps = new long[DEVICE_COUNT];
    int[] devices = new int[DEVICE_COUNT];
    for (int device = 0; device < DEVICE_COUNT; device++) {
      timestamps[device] = device + 1;
      devices[device] = device;
    }
    long dataPoolBefore = GlobalWindowMemoryController.getInstance().getDataUsedBytes();

    engine.push(Collections.singletonList(deviceSlice(timestamps, devices)));

    WindowRowStore store = onlyStore(engine);
    Assert.assertEquals(DEVICE_COUNT, store.size());
    Assert.assertEquals(DEVICE_COUNT, store.deviceCount());
    long estimate = store.ramSizeEstimate();
    long dataPoolCharged =
        GlobalWindowMemoryController.getInstance().getDataUsedBytes() - dataPoolBefore;
    long perDevice = estimate / DEVICE_COUNT;
    System.out.printf(
        "[scale-mem] %d devices x 1 row: store estimate %d B (%d B/device), data pool charged %d B%n",
        DEVICE_COUNT, estimate, perDevice, dataPoolCharged);
    // Whole 1024-row chunks + per-row index nodes billed ~16.9 KB per device (~161 MiB total).
    // The doubling first chunk plus the charged skeleton keeps a 2-column device around 1.5 KB
    // (deterministic, no CI jitter); 2048 B/device leaves headroom that a whole-chunk regression
    // (16.9 KB) blows through by an order of magnitude.
    Assert.assertTrue("per-device row store estimate " + perDevice + " B", perDevice < 2048);
    engine.drop();
  }

  /**
   * One table-model slice whose per-row device identity comes from a TAG column (row {@code i}
   * belongs to {@code device_<devices[i]>}); the tablet carries an explicit "time" measurement
   * column like every other helper here (the scan column "time" resolves by name).
   */
  private static DataSlice deviceSlice(long[] timestamps, int[] devices) {
    Tablet tablet =
        new Tablet(
            "scale_table",
            Arrays.asList("device_id", "time", "value"),
            Arrays.asList(TSDataType.STRING, TSDataType.INT64, TSDataType.INT64),
            Arrays.asList(ColumnCategory.TAG, ColumnCategory.FIELD, ColumnCategory.FIELD),
            timestamps.length);
    for (int i = 0; i < timestamps.length; i++) {
      tablet.addTimestamp(i, timestamps[i]);
      tablet.addValue(i, 0, "device_" + devices[i]);
      tablet.addValue(i, 1, timestamps[i]);
      tablet.addValue(i, 2, devices[i] * 1_000_000L + timestamps[i]);
    }
    tablet.setRowSize(timestamps.length);
    return new DataSlice(null, tablet, 0, timestamps.length, -1L);
  }

  /**
   * The engine's single window's row store, via reflection ({@code windows} and the state's {@code
   * rowStore} field are private; the store class itself is package-private, so once reached it is
   * readable from this test's package).
   */
  private static WindowRowStore onlyStore(TumbleWindowEngine engine) {
    try {
      Field windowsField = TumbleWindowEngine.class.getDeclaredField("windows");
      windowsField.setAccessible(true);
      TreeMap<?, ?> windows = (TreeMap<?, ?>) windowsField.get(engine);
      Assert.assertEquals(1, windows.size());
      Object state = windows.firstEntry().getValue();
      Field rowStoreField = state.getClass().getDeclaredField("rowStore");
      rowStoreField.setAccessible(true);
      return (WindowRowStore) rowStoreField.get(state);
    } catch (ReflectiveOperationException e) {
      throw new RuntimeException(e);
    }
  }
}
