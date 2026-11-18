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

package org.apache.iotdb.bench;

import org.apache.iotdb.trigger.api.Trigger;
import org.apache.iotdb.trigger.api.TriggerAttributes;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Self-contained single-class synchronous write-path IoTDB {@link Trigger} baseline for Exp-4.
 * Accumulates tumbling-window statistics ({@code count}, {@code sum}, {@code avg}, exact sorted
 * {@code P99}) inline during {@link #fire(Tablet)} and appends window emission records to disk.
 */
public class WindowAggregationSyncTrigger implements Trigger {

  private static final int INITIAL_BUF_CAP = 1024;

  private long windowMs = 10_000L;
  private Path outputPath;
  private final Map<String, long[]> windowMeta = new HashMap<>();
  private final Map<String, double[]> windowVals = new HashMap<>();
  private final Map<String, double[]> windowSum = new HashMap<>();
  private final Object stateLock = new Object();

  @Override
  public void onCreate(TriggerAttributes attributes) throws Exception {
    this.windowMs = attributes.getLongOrDefault("window_ms", 10_000L);
    String pathStr =
        attributes.getStringOrDefault(
            "output_path",
            System.getProperty("java.io.tmpdir") + "/iotdb_sync_trigger_windows.log");
    this.outputPath = Paths.get(pathStr);
    if (outputPath.getParent() != null) {
      Files.createDirectories(outputPath.getParent());
    }
    Files.deleteIfExists(outputPath);
    Files.createFile(outputPath);
  }

  @Override
  public boolean fire(Tablet tablet) throws Exception {
    int rowSize = tablet.getRowSize();
    if (rowSize <= 0) {
      return true;
    }
    List<IMeasurementSchema> schemas = tablet.getSchemas();
    int valColIdx = 0;
    for (int i = 0; i < schemas.size(); i++) {
      if ("val".equalsIgnoreCase(schemas.get(i).getMeasurementName())) {
        valColIdx = i;
        break;
      }
    }
    if (valColIdx >= tablet.getValues().length) {
      return true;
    }

    String deviceId = tablet.getDeviceId();
    long[] timestamps = tablet.getTimestamps();
    TSDataType type = schemas.get(valColIdx).getType();
    Object colArray = tablet.getValues()[valColIdx];
    StringBuilder closedLines = new StringBuilder();

    synchronized (stateLock) {
      long[] meta = windowMeta.get(deviceId);
      double[] vals = windowVals.get(deviceId);
      double[] sumBox = windowSum.get(deviceId);
      if (meta == null) {
        meta = new long[] {Long.MIN_VALUE, 0L};
        vals = new double[INITIAL_BUF_CAP];
        sumBox = new double[] {0.0};
        windowMeta.put(deviceId, meta);
        windowVals.put(deviceId, vals);
        windowSum.put(deviceId, sumBox);
      }

      for (int r = 0; r < rowSize; r++) {
        long ts = timestamps[r];
        long winStart = Math.floorDiv(ts, windowMs) * windowMs;
        double val = extractDouble(type, colArray, r);
        if (meta[0] == Long.MIN_VALUE) {
          meta[0] = winStart;
        } else if (winStart > meta[0]) {
          int cnt = (int) meta[1];
          appendClosedWindow(closedLines, deviceId, meta[0], vals, cnt, sumBox[0]);
          meta[0] = winStart;
          meta[1] = 0L;
          sumBox[0] = 0.0;
        }
        int curCnt = (int) meta[1];
        if (curCnt == vals.length) {
          vals = Arrays.copyOf(vals, vals.length * 2);
          windowVals.put(deviceId, vals);
        }
        vals[curCnt] = val;
        meta[1] = curCnt + 1L;
        sumBox[0] += val;
      }

      if (closedLines.length() > 0) {
        flushLines(closedLines.toString());
      }
    }
    return true;
  }

  private void appendClosedWindow(
      StringBuilder sb, String deviceId, long winStart, double[] vals, int count, double sum) {
    if (count == 0) {
      return;
    }
    double[] copy = Arrays.copyOf(vals, count);
    Arrays.sort(copy);
    int p99Idx = Math.min(copy.length - 1, Math.max(0, (int) Math.ceil(0.99 * copy.length) - 1));
    double avg = sum / count;
    sb.append(winStart)
        .append(',')
        .append(deviceId)
        .append(',')
        .append(count)
        .append(',')
        .append(avg)
        .append(',')
        .append(copy[p99Idx])
        .append(',')
        .append(System.currentTimeMillis())
        .append('\n');
  }

  private void flushLines(String payload) throws IOException {
    try (BufferedWriter writer =
        Files.newBufferedWriter(
            outputPath,
            StandardCharsets.UTF_8,
            StandardOpenOption.CREATE,
            StandardOpenOption.APPEND)) {
      writer.write(payload);
      writer.flush();
    }
  }

  private static double extractDouble(TSDataType type, Object colArray, int row) {
    if (type == TSDataType.DOUBLE) {
      return ((double[]) colArray)[row];
    }
    if (type == TSDataType.FLOAT) {
      return ((float[]) colArray)[row];
    }
    if (type == TSDataType.INT64 || type == TSDataType.TIMESTAMP) {
      return ((long[]) colArray)[row];
    }
    if (type == TSDataType.INT32) {
      return ((int[]) colArray)[row];
    }
    return 0.0;
  }

  @Override
  public void onDrop() throws Exception {
    synchronized (stateLock) {
      windowMeta.clear();
      windowVals.clear();
      windowSum.clear();
    }
  }
}
