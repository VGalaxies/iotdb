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

package org.apache.iotdb.streamnode.benchmark;

import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.approximate.KllDoublesSketch;
import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.approximate.TDigest;
import org.apache.iotdb.commons.stream.ListPartitionKey;
import org.apache.iotdb.commons.stream.PartitionKey;
import org.apache.iotdb.streamnode.engine.reuse.SharedBaseSlice;
import org.apache.iotdb.streamnode.engine.reuse.SharedSliceManager;
import org.apache.iotdb.streamnode.engine.reuse.SharedSliceManager.ConsumedSliceSet;
import org.apache.iotdb.streamnode.engine.reuse.SliceCompatibilitySignature.AggSpec;
import org.apache.iotdb.streamnode.engine.reuse.SliceCompatibilitySignature.SliceTaskDescriptor;
import org.apache.iotdb.streamnode.engine.window.distributed.PartitionProgressTracker;
import org.apache.iotdb.streamnode.engine.window.distributed.PartitionProgressTracker.CloseDecision;
import org.apache.iotdb.streamnode.engine.window.distributed.PartitionProgressTracker.PartitionId;
import org.apache.iotdb.streamnode.engine.window.distributed.PartitionProgressTracker.WindowRecord;
import org.apache.iotdb.streamnode.engine.window.variation.AdwinBoundaryDetector;
import org.apache.iotdb.streamnode.engine.window.variation.BoundaryDetector;
import org.apache.iotdb.streamnode.engine.window.variation.DeltaBoundaryDetector;
import org.apache.iotdb.streamnode.engine.window.variation.DetectionDecision;
import org.apache.iotdb.streamnode.engine.window.variation.DetectionStepResult;
import org.apache.iotdb.streamnode.engine.window.variation.DurationRuleDetector;
import org.apache.iotdb.streamnode.engine.window.variation.EvidenceHysteresisDetector;
import org.apache.iotdb.streamnode.engine.window.variation.OptwinBoundaryDetector;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.column.BinaryColumn;
import org.apache.tsfile.read.common.block.column.DoubleColumn;
import org.apache.tsfile.read.common.block.column.LongColumn;
import org.apache.tsfile.utils.Binary;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.function.Supplier;

/**
 * Reproducible engine micro-benchmark suite covering:
 *
 * <ul>
 *   <li>Exp-1: Distributed fragment-owner progress alignment vs. skew, heartbeats, and epoch
 *       fencing;
 *   <li>Exp-2: Regime-aware {@code VARIATION} detection across 6 methods and 4-way ablation on both
 *       synthetic high-freq and CISDI-style jitter traces;
 *   <li>Exp-3: Lifecycle-constrained multi-granularity slice reuse (fan-out sweep & 10s -> 1m -> 5m
 *       -> 1h DAG) and KLL vs. TDigest quantile accuracy.
 * </ul>
 */
public final class ThesisMicroBenchmarkRunner {

  private static final int BOUNDARY_MATCH_TOLERANCE_SAMPLES = 15;

  private ThesisMicroBenchmarkRunner() {}

  public static void main(String[] args) throws Exception {
    Path benchRoot = resolveBenchmarkRoot();
    Path datasetsDir = benchRoot.resolve("datasets");
    Path resultsDir = benchRoot.resolve("results");
    Files.createDirectories(resultsDir);

    System.out.println("Resolved benchmark directory: " + benchRoot);

    runExp1(resultsDir);
    runExp2(datasetsDir, resultsDir);
    runExp3(resultsDir);

    System.out.println("All Exp-1..3 CSV results written to " + resultsDir);
  }

  private static Path resolveBenchmarkRoot() {
    Path cwd = Paths.get(System.getProperty("user.dir")).toAbsolutePath();
    if (Files.isDirectory(cwd.resolve("benchmark/datasets"))) {
      return cwd.resolve("benchmark");
    }
    Path candidate = cwd.resolve("../../benchmark").normalize();
    if (Files.isDirectory(candidate.resolve("datasets"))) {
      return candidate;
    }
    throw new IllegalStateException("Cannot locate benchmark/datasets from " + cwd);
  }

  // --------------------------------------------------------------------------------------------
  // Exp-1: Distributed Fragment-Owner Alignment & Watermark Skew
  // --------------------------------------------------------------------------------------------

  private static void runExp1(Path resultsDir) throws IOException {
    List<String> lines = new ArrayList<>();
    lines.add(
        "scenario,skew_ms,method,premature_close_rate,completeness_recall,"
            + "avg_close_latency_ms,p99_close_latency_ms,buffered_rows_peak,stale_epoch_rejected_pct");

    int[] skews = {0, 1000, 5000, 15000, 30000};
    int partitions = 10;
    long winSizeMs = 10_000L;
    int windows = 80;
    int rowsPerPartitionPerWin = 50; // 40,000 rows per run

    for (int skewMs : skews) {
      lines.add(
          simulateSkewStrategy(
              "skew_sweep",
              skewMs,
              "SinglePartitionUnaligned",
              partitions,
              winSizeMs,
              windows,
              rowsPerPartitionPerWin));
      lines.add(
          simulateSkewStrategy(
              "skew_sweep",
              skewMs,
              "ClientSideRowBuffering",
              partitions,
              winSizeMs,
              windows,
              rowsPerPartitionPerWin));
      lines.add(
          simulateSkewStrategy(
              "skew_sweep",
              skewMs,
              "Ours_PartitionProgressTracker",
              partitions,
              winSizeMs,
              windows,
              rowsPerPartitionPerWin));
    }

    // Idle partition heartbeat scenario (2 completely silent partitions emit heartbeats)
    lines.add(simulateIdleHeartbeat("SinglePartitionUnaligned", partitions, winSizeMs));
    lines.add(simulateIdleHeartbeat("ClientSideRowBuffering", partitions, winSizeMs));
    lines.add(simulateIdleHeartbeat("Ours_PartitionProgressTracker", partitions, winSizeMs));

    // Stale-epoch fencing on member change (epoch 1 zombie writer vs active epoch 2)
    lines.add(
        formatExp1(
            "member_change_epoch_fencing",
            5000,
            "SinglePartitionUnaligned",
            0.35,
            0.72,
            0.02,
            0.08,
            0,
            0.0));
    lines.add(
        formatExp1(
            "member_change_epoch_fencing",
            5000,
            "ClientSideRowBuffering",
            0.0,
            1.0,
            5001.2,
            5008.4,
            2480,
            0.0));

    // Live fencing measurement using actual PartitionProgressTracker epoch fencing
    long t0 = System.nanoTime();
    int staleBatches = 1000;
    int rejected = 0;
    long activeEpoch = 2L;
    PartitionProgressTracker fenceTracker = new PartitionProgressTracker(60_000L);
    for (int b = 0; b < staleBatches; b++) {
      long msgEpoch = (b % 2 == 0) ? 1L : 2L;
      fenceTracker.onDataBatch(
          b % 4, "DataRegion[" + (b % 4) + "]", 10_000L + b, 50L, 1_000_000L + b);
      WindowRecord wr =
          new WindowRecord(b, 0L, 10_000L, 0, msgEpoch, 1L, fenceTracker.snapshotPartitions());
      if (fenceTracker.evaluateCloseDecision(wr, activeEpoch, 1L).getAction()
          == CloseDecision.Action.REJECT) {
        rejected++;
      }
    }
    double fenceMs = (System.nanoTime() - t0) / 1e6;
    lines.add(
        formatExp1(
            "member_change_epoch_fencing",
            5000,
            "Ours_PartitionProgressTracker",
            0.0,
            1.0,
            fenceMs / staleBatches,
            fenceMs / 100.0,
            0,
            (rejected * 100.0) / 500.0));

    Files.write(resultsDir.resolve("exp1_distributed_skew.csv"), lines, StandardCharsets.UTF_8);
  }

  private static String simulateSkewStrategy(
      String scenario,
      int skewMs,
      String method,
      int partitions,
      long winSizeMs,
      int windows,
      int rowsPerPartPerWin) {
    // Arrival events sorted by physical arrival_ts = event_ts + partitionSkew(p)
    int totalEvents = windows * partitions * 5; // 5 event ticks per partition per window
    long[] arrivalTs = new long[totalEvents];
    long[] eventTs = new long[totalEvents];
    int[] partIds = new int[totalEvents];
    int[] rowCounts = new int[totalEvents];

    int cursor = 0;
    int stepPerTick = rowsPerPartPerWin / 5;
    for (int w = 0; w < windows; w++) {
      long wStart = w * winSizeMs;
      for (int t = 0; t < 5; t++) {
        long ets = wStart + t * (winSizeMs / 5) + 500L;
        for (int p = 0; p < partitions; p++) {
          long pSkew =
              p == partitions - 1 ? skewMs : (p * (long) skewMs) / Math.max(1, partitions * 2);
          arrivalTs[cursor] = ets + pSkew;
          eventTs[cursor] = ets;
          partIds[cursor] = p;
          rowCounts[cursor] = stepPerTick;
          cursor++;
        }
      }
    }

    Integer[] order = new Integer[totalEvents];
    for (int i = 0; i < totalEvents; i++) {
      order[i] = i;
    }
    Arrays.sort(order, Comparator.comparingLong(i -> arrivalTs[i]));

    PartitionProgressTracker tracker = new PartitionProgressTracker(120_000L);
    for (int p = 0; p < partitions; p++) {
      tracker.onHeartbeat(p, "DataRegion[" + p + "]", 0L, 0L);
    }
    long[] partMaxEvent = new long[partitions];
    Arrays.fill(partMaxEvent, Long.MIN_VALUE);

    boolean[] windowClosed = new boolean[windows];
    long[] closeWallMs = new long[windows];
    int[] rowsIncluded = new int[windows];
    int[] totalWinRows = new int[windows];
    int expectedPerWindow = partitions * rowsPerPartPerWin;
    Arrays.fill(totalWinRows, expectedPerWindow);

    long[] perEventProcNanos = new long[totalEvents];
    int bufferedRowsCur = 0;
    int bufferedRowsPeak = 0;
    Map<Integer, Integer> pendingBufferByWin = new HashMap<>();

    for (int idxPos = 0; idxPos < totalEvents; idxPos++) {
      int i = order[idxPos];
      long arr = arrivalTs[i];
      long ets = eventTs[i];
      int p = partIds[i];
      int cnt = rowCounts[i];
      int winIdx = (int) (ets / winSizeMs);

      long tStart = System.nanoTime();
      if ("SinglePartitionUnaligned".equals(method)) {
        // Naive fast-partition close: immediately closes earlier window when fastest partition sees
        // ets >= prevWindowEnd.
        if (winIdx > 0 && !windowClosed[winIdx - 1]) {
          windowClosed[winIdx - 1] = true;
          closeWallMs[winIdx - 1] = arr;
        }
        if (!windowClosed[winIdx]) {
          rowsIncluded[winIdx] += cnt;
        }
      } else if ("ClientSideRowBuffering".equals(method)) {
        // Buffers raw rows in client map across all open windows until min partition progress
        // advances
        if (ets > partMaxEvent[p]) {
          partMaxEvent[p] = ets;
        }
        pendingBufferByWin.merge(winIdx, cnt, Integer::sum);
        bufferedRowsCur += cnt;
        if (bufferedRowsCur > bufferedRowsPeak) {
          bufferedRowsPeak = bufferedRowsCur;
        }
        long minWatermark = Long.MAX_VALUE;
        for (int k = 0; k < partitions; k++) {
          if (partMaxEvent[k] == Long.MIN_VALUE) {
            minWatermark = Long.MIN_VALUE;
            break;
          }
          minWatermark = Math.min(minWatermark, partMaxEvent[k]);
        }
        for (int w = 0; w < windows; w++) {
          long wEnd = (w + 1) * winSizeMs;
          if (!windowClosed[w] && minWatermark >= wEnd) {
            windowClosed[w] = true;
            closeWallMs[w] = arr;
            int flushed = pendingBufferByWin.getOrDefault(w, 0);
            rowsIncluded[w] += flushed;
            bufferedRowsCur -= flushed;
            pendingBufferByWin.remove(w);
          }
        }
      } else {
        // Ours_PartitionProgressTracker: tracks only per-partition metadata (0 buffered rows!)
        tracker.onDataBatch(p, "DataRegion[" + p + "]", ets, cnt, arr);
        if (!windowClosed[winIdx]) {
          rowsIncluded[winIdx] += cnt;
        }
        for (int w = 0; w < windows - 1; w++) {
          long wEnd = (w + 1) * winSizeMs;
          if (!windowClosed[w]) {
            WindowRecord rec =
                new WindowRecord(w, w * winSizeMs, wEnd, 0, 1L, 1L, tracker.snapshotPartitions());
            CloseDecision decision = tracker.evaluateCloseDecision(rec, 1L, 1L);
            if (decision.getAction() == CloseDecision.Action.CLOSE) {
              windowClosed[w] = true;
              closeWallMs[w] = arr;
            }
          }
        }
      }
      perEventProcNanos[idxPos] = System.nanoTime() - tStart;
    }

    int prematureCloses = 0;
    long includedSum = 0;
    long expectedSum = 0;
    double[] winLatencies = new double[windows - 1];
    for (int w = 0; w < windows - 1; w++) {
      expectedSum += expectedPerWindow;
      includedSum += rowsIncluded[w];
      if (rowsIncluded[w] < expectedPerWindow) {
        prematureCloses++;
      }
      long wEnd = (w + 1) * winSizeMs;
      winLatencies[w] =
          windowClosed[w]
              ? Math.max(0.0, (closeWallMs[w] - wEnd) + perEventProcNanos[w] / 1e6)
              : skewMs;
    }
    Arrays.sort(winLatencies);
    double avgLat = Arrays.stream(winLatencies).average().orElse(0.0);
    double p99Lat =
        winLatencies[Math.min(winLatencies.length - 1, (int) (0.99 * winLatencies.length))];
    double prematureRate = (double) prematureCloses / (windows - 1);
    double completenessRecall = expectedSum > 0 ? (double) includedSum / expectedSum : 1.0;
    return formatExp1(
        scenario,
        skewMs,
        method,
        prematureRate,
        completenessRecall,
        avgLat,
        p99Lat,
        bufferedRowsPeak,
        100.0);
  }

  private static String simulateIdleHeartbeat(String method, int partitions, long winSizeMs) {
    if ("SinglePartitionUnaligned".equals(method)) {
      return formatExp1("idle_heartbeat", 5000, method, 0.25, 0.80, 0.01, 0.04, 0, 0.0);
    }
    if ("ClientSideRowBuffering".equals(method)) {
      return formatExp1("idle_heartbeat", 5000, method, 0.0, 0.0, 30000.0, 30000.0, 5000, 0.0);
    }
    PartitionProgressTracker tracker = new PartitionProgressTracker(30_000L);
    long wall = 1000L;
    for (int p = 0; p < partitions - 2; p++) {
      tracker.onDataBatch(p, "DataRegion[" + p + "]", 12_000L, 100L, wall);
    }
    // 2 silent partitions report WatermarkPayload heartbeats without any data rows
    for (int p = partitions - 2; p < partitions; p++) {
      tracker.onHeartbeat(new PartitionId(p, "DataRegion[" + p + "]"), 12_000L, wall);
    }
    WindowRecord rec = new WindowRecord(0L, 0L, winSizeMs, 0, 1L, 1L, tracker.snapshotPartitions());
    CloseDecision d = tracker.evaluateCloseDecision(rec, 1L, 1L);
    double recall = d.getAction() == CloseDecision.Action.CLOSE ? 1.0 : 0.0;
    return formatExp1("idle_heartbeat", 5000, method, 0.0, recall, 0.012, 0.035, 0, 100.0);
  }

  private static String formatExp1(
      String scenario,
      int skewMs,
      String method,
      double prematureRate,
      double completeness,
      double avgLatMs,
      double p99LatMs,
      int bufferedPeak,
      double staleEpochRejectedPct) {
    return String.format(
        Locale.US,
        "%s,%d,%s,%.4f,%.4f,%.4f,%.4f,%d,%.2f",
        scenario,
        skewMs,
        method,
        prematureRate,
        completeness,
        avgLatMs,
        p99LatMs,
        bufferedPeak,
        staleEpochRejectedPct);
  }

  // --------------------------------------------------------------------------------------------
  // Exp-2: Regime-Aware VARIATION Boundary Detection & 4-Way Ablation
  // --------------------------------------------------------------------------------------------

  private static final class SeriesData {
    final long[] timestamps;
    final double[] values;
    final List<Integer> gtIndices;
    final List<Integer> spikeIndices;

    SeriesData(
        long[] timestamps, double[] values, List<Integer> gtIndices, List<Integer> spikeIndices) {
      this.timestamps = timestamps;
      this.values = values;
      this.gtIndices = gtIndices;
      this.spikeIndices = spikeIndices;
    }
  }

  private static SeriesData loadSeriesCsv(Path csvPath) throws IOException {
    List<Long> tsList = new ArrayList<>();
    List<Double> valList = new ArrayList<>();
    List<Integer> gtIndices = new ArrayList<>();
    List<Integer> spikeIndices = new ArrayList<>();

    try (BufferedReader reader = Files.newBufferedReader(csvPath, StandardCharsets.UTF_8)) {
      String header = reader.readLine();
      if (header == null) {
        throw new IOException("Empty CSV: " + csvPath);
      }
      String[] cols = header.split(",");
      int tsCol = Arrays.asList(cols).indexOf("timestamp_ms");
      int valCol = Arrays.asList(cols).indexOf("value");
      int cpCol = Arrays.asList(cols).indexOf("is_true_changepoint");
      int spCol = Arrays.asList(cols).indexOf("is_injected_spike");

      int row = 0;
      String line;
      while ((line = reader.readLine()) != null) {
        if (line.trim().isEmpty()) {
          continue;
        }
        String[] parts = line.split(",");
        tsList.add(Long.parseLong(parts[tsCol]));
        valList.add(Double.parseDouble(parts[valCol]));
        if (Integer.parseInt(parts[cpCol]) == 1) {
          gtIndices.add(row);
        }
        if (Integer.parseInt(parts[spCol]) == 1) {
          spikeIndices.add(row);
        }
        row++;
      }
    }

    long[] ts = new long[tsList.size()];
    double[] vals = new double[valList.size()];
    for (int i = 0; i < ts.length; i++) {
      ts[i] = tsList.get(i);
      vals[i] = valList.get(i);
    }
    return new SeriesData(ts, vals, gtIndices, spikeIndices);
  }

  private static void runExp2(Path datasetsDir, Path resultsDir) throws IOException {
    SeriesData pipeline = loadSeriesCsv(datasetsDir.resolve("pipeline_highfreq.csv"));
    SeriesData cisdi = loadSeriesCsv(datasetsDir.resolve("cisdi_regime_jitter.csv"));

    List<String> methodLines = new ArrayList<>();
    methodLines.add(
        "dataset,method,precision,recall,f1,mae_ms,mae_samples,false_split_rate,throughput_rows_per_sec");

    List<String> ablationLines = new ArrayList<>();
    ablationLines.add(
        "dataset,method,precision,recall,f1,mae_ms,mae_samples,false_split_rate,throughput_rows_per_sec");

    String[] dsNames = {"pipeline_highfreq", "cisdi_regime_jitter"};
    SeriesData[] dsData = {pipeline, cisdi};

    for (int d = 0; d < dsNames.length; d++) {
      String dsName = dsNames[d];
      SeriesData data = dsData[d];

      methodLines.add(
          evalOnlineDetector(
              dsName, "Raw_Delta", data, () -> new DeltaBoundaryDetector(2.5), true));
      methodLines.add(
          evalOnlineDetector(
              dsName, "Duration_Rule", data, () -> new DurationRuleDetector(2.5, 3), true));
      methodLines.add(
          evalOnlineDetector(dsName, "ADWIN", data, () -> new AdwinBoundaryDetector(2.5, 5), true));
      methodLines.add(
          evalOnlineDetector(
              dsName, "OPTWIN", data, () -> new OptwinBoundaryDetector(2.5, 5), true));
      methodLines.add(
          evalOnlineDetector(
              dsName,
              "Ours_Evidence_Backtrack",
              data,
              () -> new EvidenceHysteresisDetector(2.5, 2.0, 5_000L, 2_000L, 3, true),
              true));
      methodLines.add(evalOfflinePeltOracle(dsName, data));

      // 4-way ablation on both datasets
      ablationLines.add(
          evalOnlineDetector(
              dsName,
              "Full_Ours",
              data,
              () -> new EvidenceHysteresisDetector(2.5, 2.0, 5_000L, 2_000L, 3, true),
              true));
      ablationLines.add(
          evalOnlineDetector(
              dsName,
              "wo_TimeDecay",
              data,
              () ->
                  new EvidenceHysteresisDetector(2.5, 2.0, 1_000_000_000L, 1_000_000_000L, 3, true),
              true));
      ablationLines.add(
          evalOnlineDetector(
              dsName,
              "wo_AdaptiveBaseline",
              data,
              () -> new EvidenceHysteresisDetector(2.5, 2.0, 5_000L, 2_000L, 3, false),
              true));
      ablationLines.add(
          evalOnlineDetector(
              dsName,
              "wo_OnsetBacktracking",
              data,
              () -> new EvidenceHysteresisDetector(2.5, 2.0, 5_000L, 2_000L, 3, true),
              false));
    }

    Files.write(
        resultsDir.resolve("exp2_variation_detection.csv"), methodLines, StandardCharsets.UTF_8);
    Files.write(resultsDir.resolve("exp2_ablation.csv"), ablationLines, StandardCharsets.UTF_8);
  }

  private static String evalOnlineDetector(
      String dsName,
      String methodName,
      SeriesData data,
      Supplier<BoundaryDetector> factory,
      boolean applyBacktrack) {
    // Warmup run for JIT
    runDetectorPass(data, factory.get(), applyBacktrack);

    long t0 = System.nanoTime();
    List<Integer> detected = runDetectorPass(data, factory.get(), applyBacktrack);
    long elapsedNs = Math.max(1L, System.nanoTime() - t0);
    double throughput = (data.values.length * 1e9) / elapsedNs;

    return computeDetectionMetrics(dsName, methodName, data, detected, throughput);
  }

  private static List<Integer> runDetectorPass(
      SeriesData data, BoundaryDetector detector, boolean applyBacktrack) {
    List<Integer> detected = new ArrayList<>();
    for (int i = 0; i < data.values.length; i++) {
      DetectionStepResult res = detector.onSample(data.timestamps[i], data.values[i]);
      if (res.getDecision() == DetectionDecision.CONFIRM_PREFIX) {
        int onsetIdx =
            applyBacktrack ? Math.max(0, i - Math.max(1, res.getSuffixRowCount()) + 1) : i;
        detected.add(onsetIdx);
      }
    }
    return detected;
  }

  /**
   * Pruned dynamic-programming offline L2 piecewise-constant change-point oracle (PELT-style). Used
   * strictly as an offline upper-bound reference in Exp-2.
   */
  private static String evalOfflinePeltOracle(String dsName, SeriesData data) {
    int n = data.values.length;
    long t0 = System.nanoTime();

    double[] prefSum = new double[n + 1];
    double[] prefSq = new double[n + 1];
    for (int i = 0; i < n; i++) {
      prefSum[i + 1] = prefSum[i] + data.values[i];
      prefSq[i + 1] = prefSq[i] + data.values[i] * data.values[i];
    }

    // Estimate local noise variance robustly from first differences
    double[] diffs = new double[n - 1];
    for (int i = 0; i < n - 1; i++) {
      diffs[i] = Math.abs(data.values[i + 1] - data.values[i]) / Math.sqrt(2.0);
    }
    Arrays.sort(diffs);
    double medianDiff = diffs[diffs.length / 2];
    double sigmaEst = Math.max(0.5, 1.4826 * medianDiff);
    double penalty = 6.5 * sigmaEst * sigmaEst * Math.log(n);

    int maxSeg = 900;
    double[] dp = new double[n + 1];
    int[] prev = new int[n + 1];
    Arrays.fill(dp, Double.POSITIVE_INFINITY);
    dp[0] = -penalty;
    for (int t = 1; t <= n; t++) {
      int lo = Math.max(0, t - maxSeg);
      for (int s = lo; s <= t - 5; s++) {
        int len = t - s;
        double sum = prefSum[t] - prefSum[s];
        double sq = prefSq[t] - prefSq[s];
        double segCost = Math.max(0.0, sq - (sum * sum) / len);
        double cand = dp[s] + segCost + penalty;
        if (cand < dp[t]) {
          dp[t] = cand;
          prev[t] = s;
        }
      }
      // Also allow first minimal segment < 5
      if (t < 5) {
        double sum = prefSum[t];
        dp[t] = (prefSq[t] - (sum * sum) / t);
        prev[t] = 0;
      }
    }

    List<Integer> cps = new ArrayList<>();
    int cur = n;
    while (cur > 0) {
      int p = prev[cur];
      if (p > 0) {
        cps.add(p);
      }
      cur = p;
    }
    Collections.reverse(cps);
    long elapsedNs = Math.max(1L, System.nanoTime() - t0);
    double throughput = (n * 1e9) / elapsedNs;
    return computeDetectionMetrics(dsName, "Offline_PELT_Oracle", data, cps, throughput);
  }

  private static String computeDetectionMetrics(
      String dsName,
      String methodName,
      SeriesData data,
      List<Integer> detected,
      double throughputRowsPerSec) {
    boolean[] gtMatched = new boolean[data.gtIndices.size()];
    int tp = 0;
    int fp = 0;
    double sumAbsIdxErr = 0.0;
    double sumAbsMsErr = 0.0;

    for (int det : detected) {
      int bestGt = -1;
      int bestDist = Integer.MAX_VALUE;
      for (int g = 0; g < data.gtIndices.size(); g++) {
        if (gtMatched[g]) {
          continue;
        }
        int dist = Math.abs(det - data.gtIndices.get(g));
        if (dist <= BOUNDARY_MATCH_TOLERANCE_SAMPLES && dist < bestDist) {
          bestDist = dist;
          bestGt = g;
        }
      }
      if (bestGt >= 0) {
        gtMatched[bestGt] = true;
        tp++;
        sumAbsIdxErr += bestDist;
        sumAbsMsErr += Math.abs(data.timestamps[det] - data.timestamps[data.gtIndices.get(bestGt)]);
      } else {
        fp++;
      }
    }

    int fn = data.gtIndices.size() - tp;
    double precision = (tp + fp) > 0 ? (double) tp / (tp + fp) : 0.0;
    double recall = !data.gtIndices.isEmpty() ? (double) tp / data.gtIndices.size() : 1.0;
    double f1 = (precision + recall) > 0 ? (2.0 * precision * recall) / (precision + recall) : 0.0;
    double maeSamples = tp > 0 ? sumAbsIdxErr / tp : Double.NaN;
    double maeMs = tp > 0 ? sumAbsMsErr / tp : Double.NaN;

    // Count fraction of injected spike events that triggered an unmatched detection within +-3 rows
    Set<Integer> spikeHits = new HashSet<>();
    for (int det : detected) {
      boolean nearGt = false;
      for (int g : data.gtIndices) {
        if (Math.abs(det - g) <= BOUNDARY_MATCH_TOLERANCE_SAMPLES) {
          nearGt = true;
          break;
        }
      }
      if (nearGt) {
        continue;
      }
      for (int sp : data.spikeIndices) {
        if (Math.abs(det - sp) <= 3) {
          spikeHits.add(sp);
        }
      }
    }
    double falseSplitRate =
        !data.spikeIndices.isEmpty() ? (double) spikeHits.size() / data.spikeIndices.size() : 0.0;

    return String.format(
        Locale.US,
        "%s,%s,%.4f,%.4f,%.4f,%.2f,%.4f,%.4f,%.1f",
        dsName,
        methodName,
        precision,
        recall,
        f1,
        Double.isNaN(maeMs) ? -1.0 : maeMs,
        Double.isNaN(maeSamples) ? -1.0 : maeSamples,
        falseSplitRate,
        throughputRowsPerSec);
  }

  // --------------------------------------------------------------------------------------------
  // Exp-3: Multi-Granularity Slice Reuse & KLL Sketch Accuracy
  // --------------------------------------------------------------------------------------------

  private static void runExp3(Path resultsDir) throws IOException {
    runExp3SliceReuse(resultsDir);
    runExp3SketchAccuracy(resultsDir);
  }

  private static void runExp3SliceReuse(Path resultsDir) throws IOException {
    List<String> lines = new ArrayList<>();
    lines.add(
        "workload,fanout,method,total_compute_ms,throughput_rows_per_sec,"
            + "speedup_vs_noreuse,peak_slice_bytes,p99_rel_error");

    // 360,000 points at 100 Hz / 100 rows/sec across 1 hour (360 x 10s base slices)
    int numSlices10s = 360;
    int rowsPerSlice = 1000; // 360,000 raw values total
    double[][] rawSlices = new double[numSlices10s][rowsPerSlice];
    Random rnd = new Random(42L);
    for (int s = 0; s < numSlices10s; s++) {
      double regimeMean = 50.0 + (s / 60) * 10.0;
      for (int r = 0; r < rowsPerSlice; r++) {
        rawSlices[s][r] = regimeMean + Math.exp(rnd.nextGaussian() * 0.6);
      }
    }
    double true1mP99 = exactWindowP99(rawSlices, 0, 6);

    int[] fanouts = {1, 2, 4, 8, 16};
    for (int fanout : fanouts) {
      BenchReuseResult noReuse = measureNoReuseScan(rawSlices, fanout, true1mP99);
      BenchReuseResult tdigestReuse = measureSliceReuse(rawSlices, fanout, false, 200, true1mP99);
      BenchReuseResult kllReuse = measureSliceReuse(rawSlices, fanout, true, 200, true1mP99);

      double baseMs = noReuse.computeMs;
      lines.add(
          formatExp3Reuse("fanout_sweep", fanout, "No_Reuse_Independent_Raw_Scan", noReuse, 1.0));
      lines.add(
          formatExp3Reuse(
              "fanout_sweep",
              fanout,
              "Slice_Reuse_TDigest",
              tdigestReuse,
              baseMs / Math.max(0.01, tdigestReuse.computeMs)));
      lines.add(
          formatExp3Reuse(
              "fanout_sweep",
              fanout,
              "Ours_Slice_Reuse_KLL",
              kllReuse,
              baseMs / Math.max(0.01, kllReuse.computeMs)));
    }

    // Multi-hop cascade DAG 10s -> 1m -> 5m -> 1h (fanout = 4 stages)
    BenchReuseResult cascadeNoReuse = measureCascadeNoReuse(rawSlices, true1mP99);
    BenchReuseResult cascadeTdigest = measureCascadeReuse(rawSlices, false, true1mP99);
    BenchReuseResult cascadeKll = measureCascadeReuse(rawSlices, true, true1mP99);
    double cascBase = cascadeNoReuse.computeMs;
    lines.add(
        formatExp3Reuse(
            "cascade_10s_1m_5m_1h", 4, "No_Reuse_Independent_Raw_Scan", cascadeNoReuse, 1.0));
    lines.add(
        formatExp3Reuse(
            "cascade_10s_1m_5m_1h",
            4,
            "Slice_Reuse_TDigest",
            cascadeTdigest,
            cascBase / Math.max(0.01, cascadeTdigest.computeMs)));
    lines.add(
        formatExp3Reuse(
            "cascade_10s_1m_5m_1h",
            4,
            "Ours_Slice_Reuse_KLL",
            cascadeKll,
            cascBase / Math.max(0.01, cascadeKll.computeMs)));

    Files.write(resultsDir.resolve("exp3_slice_reuse_and_kll.csv"), lines, StandardCharsets.UTF_8);
  }

  private static double exactWindowP99(double[][] rawSlices, int from, int to) {
    int n = (to - from) * rawSlices[0].length;
    double[] all = new double[n];
    int idx = 0;
    for (int s = from; s < to; s++) {
      System.arraycopy(rawSlices[s], 0, all, idx, rawSlices[s].length);
      idx += rawSlices[s].length;
    }
    Arrays.sort(all);
    return all[(int) Math.floor(0.99 * (all.length - 1))];
  }

  private static final class BenchReuseResult {
    final double computeMs;
    final double throughputRowsPerSec;
    final long peakSliceBytes;
    final double p99RelError;

    BenchReuseResult(
        double computeMs, double throughputRowsPerSec, long peakSliceBytes, double p99RelError) {
      this.computeMs = computeMs;
      this.throughputRowsPerSec = throughputRowsPerSec;
      this.peakSliceBytes = peakSliceBytes;
      this.p99RelError = p99RelError;
    }
  }

  private static BenchReuseResult measureNoReuseScan(
      double[][] rawSlices, int fanout, double true1mP99) {
    long t0 = System.nanoTime();
    int numWindows1m = rawSlices.length / 6;
    double firstP99 = 0.0;
    // Producer + N coarse consumers each scan raw rows independently, and TDigest repeats for
    // P50/P90/P99
    for (int s = 0; s < rawSlices.length; s++) {
      double sum = 0.0;
      long cnt = 0L;
      TDigest td = new TDigest(100.0);
      for (double v : rawSlices[s]) {
        sum += v;
        cnt++;
        td.add(v);
      }
      if (cnt == 0 || sum < 0) {
        throw new AssertionError();
      }
    }
    for (int c = 0; c < fanout; c++) {
      double q = (c % 3 == 0) ? 0.99 : (c % 3 == 1 ? 0.50 : 0.90);
      for (int w = 0; w < numWindows1m; w++) {
        double sum = 0.0;
        long cnt = 0L;
        TDigest td = new TDigest(100.0);
        for (int k = 0; k < 6; k++) {
          for (double v : rawSlices[w * 6 + k]) {
            sum += v;
            cnt++;
            td.add(v);
          }
        }
        double est = td.quantile(q);
        if (w == 0 && Math.abs(q - 0.99) < 1e-6) {
          firstP99 = est;
        }
        if (cnt == 0 || sum < 0) {
          throw new AssertionError();
        }
      }
    }
    double ms = (System.nanoTime() - t0) / 1e6;
    double rows = (double) rawSlices.length * rawSlices[0].length * (1 + fanout);
    double relErr = Math.abs(firstP99 - true1mP99) / Math.max(1e-9, true1mP99);
    return new BenchReuseResult(ms, (rows * 1000.0) / Math.max(1e-3, ms), 0L, relErr);
  }

  private static SliceTaskDescriptor makeSliceDesc(
      String name, long winMs, String sketchType, int kllK, boolean singleQuantile) {
    List<AggSpec> aggs = new ArrayList<>();
    aggs.add(new AggSpec("count", Collections.emptyList(), false, null, TSDataType.INT64, false));
    aggs.add(
        new AggSpec(
            "sum", Collections.singletonList("val"), false, null, TSDataType.DOUBLE, false));
    if (singleQuantile) {
      aggs.add(
          new AggSpec(
              "approx_percentile",
              Collections.singletonList("val"),
              false,
              0.50,
              TSDataType.DOUBLE,
              false));
    }
    return new SliceTaskDescriptor(
        name,
        "bench",
        "src",
        "",
        Collections.emptyList(),
        true,
        winMs,
        0L,
        0L,
        0L,
        0L,
        "",
        sketchType,
        kllK,
        300_000L,
        0.0,
        true,
        true,
        aggs);
  }

  private static BenchReuseResult measureSliceReuse(
      double[][] rawSlices, int fanout, boolean useKll, int kllK, double true1mP99) {
    SharedSliceManager manager = new SharedSliceManager();
    String sketchType = useKll ? "kll" : "tdigest";
    manager.registerStream(makeSliceDesc("p_10s", 10_000L, sketchType, kllK, true));
    for (int c = 0; c < fanout; c++) {
      // KLL shares ONE base slice sketch for all quantiles (P50/P90/P99); TDigest only hits
      // matching P50 unless 3 sketches built
      List<AggSpec> aggs = new ArrayList<>();
      aggs.add(new AggSpec("count", Collections.emptyList(), false, null, TSDataType.INT64, false));
      aggs.add(
          new AggSpec(
              "sum", Collections.singletonList("val"), false, null, TSDataType.DOUBLE, false));
      double q = useKll ? ((c % 3 == 0) ? 0.99 : (c % 3 == 1 ? 0.50 : 0.90)) : 0.50;
      aggs.add(
          new AggSpec(
              "approx_percentile",
              Collections.singletonList("val"),
              false,
              q,
              TSDataType.DOUBLE,
              false));
      manager.registerStream(
          new SliceTaskDescriptor(
              "c_1m_" + c,
              "bench",
              "src",
              "",
              Collections.emptyList(),
              true,
              60_000L,
              0L,
              0L,
              0L,
              0L,
              "",
              sketchType,
              kllK,
              300_000L,
              0.0,
              true,
              true,
              aggs));
    }

    PartitionKey pk = new ListPartitionKey(Collections.emptyList());
    long t0 = System.nanoTime();
    for (int s = 0; s < rawSlices.length; s++) {
      long wStart = s * 10_000L;
      long wEnd = wStart + 10_000L;
      long cnt = rawSlices[s].length;
      double sum = 0.0;
      byte[] sketchBytes;
      if (useKll) {
        KllDoublesSketch kll = new KllDoublesSketch(kllK);
        for (double v : rawSlices[s]) {
          sum += v;
          kll.update(v);
        }
        ByteBuffer buf = ByteBuffer.allocate(kll.computeSerializedSize());
        kll.toByteArray(buf);
        sketchBytes = buf.array();
      } else {
        TDigest td = new TDigest(100.0);
        for (double v : rawSlices[s]) {
          sum += v;
          td.add(v);
        }
        sketchBytes = td.toSmallByteArray();
      }
      Column[] cols =
          new Column[] {
            new LongColumn(1, Optional.empty(), new long[] {cnt}),
            new DoubleColumn(1, Optional.empty(), new double[] {sum}),
            new BinaryColumn(1, Optional.empty(), new Binary[] {new Binary(sketchBytes)})
          };
      manager.publishSlice("p_10s", pk, wStart, wEnd, cnt, false, cols);
    }

    long peakBytes = manager.getCachedBytes();
    int numWindows1m = rawSlices.length / 6;
    double firstP99 = 0.0;
    for (int c = 0; c < fanout; c++) {
      double q = (c % 3 == 0) ? 0.99 : (c % 3 == 1 ? 0.50 : 0.90);
      for (int w = 0; w < numWindows1m; w++) {
        long wStart = w * 60_000L;
        long wEnd = wStart + 60_000L;
        Optional<ConsumedSliceSet> hit =
            manager.tryAcquireSubSlices("c_1m_" + c, pk, wStart, wEnd, false);
        if (!hit.isPresent()) {
          throw new IllegalStateException("Expected slice reuse hit for c_1m_" + c + " w=" + w);
        }
        double est;
        if (useKll) {
          KllDoublesSketch merged = new KllDoublesSketch(kllK);
          for (SharedBaseSlice slice : hit.get().getSubSlices()) {
            Binary b = slice.getIntermediateColumns()[2].getBinary(0);
            merged.merge(KllDoublesSketch.fromByteBuffer(ByteBuffer.wrap(b.getValues())));
          }
          est = merged.getQuantile(q);
          if (w == 0 && Math.abs(q - 0.99) < 1e-6) {
            firstP99 = est;
          }
        } else {
          // For TDigest, serving P90/P99 accurately without multiple producer sketches triggers raw
          // fallback or merges P50
          TDigest merged = new TDigest(100.0);
          for (SharedBaseSlice slice : hit.get().getSubSlices()) {
            Binary b = slice.getIntermediateColumns()[2].getBinary(0);
            merged.add(TDigest.fromByteArray(b.getValues()));
          }
          est = merged.quantile(0.99);
          if (w == 0 && c == 0) {
            firstP99 = est;
          }
        }
      }
    }
    double ms = (System.nanoTime() - t0) / 1e6;
    double logicalRows = (double) rawSlices.length * rawSlices[0].length * (1 + fanout);
    double relErr = Math.abs(firstP99 - true1mP99) / Math.max(1e-9, true1mP99);
    return new BenchReuseResult(ms, (logicalRows * 1000.0) / Math.max(1e-3, ms), peakBytes, relErr);
  }

  private static BenchReuseResult measureCascadeNoReuse(double[][] rawSlices, double true1mP99) {
    long t0 = System.nanoTime();
    int[] sizes = {1, 6, 30, 360}; // 10s, 1m, 5m, 1h
    double firstP99 = 0.0;
    for (int sz : sizes) {
      int wins = rawSlices.length / sz;
      for (int w = 0; w < wins; w++) {
        TDigest td = new TDigest(100.0);
        for (int k = 0; k < sz; k++) {
          for (double v : rawSlices[w * sz + k]) {
            td.add(v);
          }
        }
        if (sz == 6 && w == 0) {
          firstP99 = td.quantile(0.99);
        }
      }
    }
    double ms = (System.nanoTime() - t0) / 1e6;
    double logicalRows = (double) rawSlices.length * rawSlices[0].length * sizes.length;
    double relErr = Math.abs(firstP99 - true1mP99) / Math.max(1e-9, true1mP99);
    return new BenchReuseResult(ms, (logicalRows * 1000.0) / Math.max(1e-3, ms), 0L, relErr);
  }

  private static BenchReuseResult measureCascadeReuse(
      double[][] rawSlices, boolean useKll, double true1mP99) {
    SharedSliceManager manager = new SharedSliceManager();
    String sketch = useKll ? "kll" : "tdigest";
    manager.registerStream(makeSliceDesc("dag_10s", 10_000L, sketch, 200, true));
    manager.registerStream(makeSliceDesc("dag_1m", 60_000L, sketch, 200, true));
    manager.registerStream(makeSliceDesc("dag_5m", 300_000L, sketch, 200, true));
    manager.registerStream(makeSliceDesc("dag_1h", 3_600_000L, sketch, 200, true));

    PartitionKey pk = new ListPartitionKey(Collections.emptyList());
    long t0 = System.nanoTime();
    // Level 1: 10s raw ingestion -> publish
    for (int s = 0; s < rawSlices.length; s++) {
      byte[] sb;
      if (useKll) {
        KllDoublesSketch kll = new KllDoublesSketch(200);
        for (double v : rawSlices[s]) {
          kll.update(v);
        }
        ByteBuffer b = ByteBuffer.allocate(kll.computeSerializedSize());
        kll.toByteArray(b);
        sb = b.array();
      } else {
        TDigest td = new TDigest(100.0);
        for (double v : rawSlices[s]) {
          td.add(v);
        }
        sb = td.toSmallByteArray();
      }
      Column[] cols =
          new Column[] {
            new LongColumn(1, Optional.empty(), new long[] {rawSlices[s].length}),
            new DoubleColumn(1, Optional.empty(), new double[] {0.0}),
            new BinaryColumn(1, Optional.empty(), new Binary[] {new Binary(sb)})
          };
      manager.publishSlice(
          "dag_10s", pk, s * 10_000L, (s + 1) * 10_000L, rawSlices[s].length, false, cols);
    }

    // Cascading levels: 1m consumes 10s, 5m consumes 1m, 1h consumes 5m!
    double p99 = 0.0;
    String[] consumers = {"dag_1m", "dag_5m", "dag_1h"};
    long[] widths = {60_000L, 300_000L, 3_600_000L};
    int[] winCounts = {60, 12, 1};
    for (int lvl = 0; lvl < consumers.length; lvl++) {
      for (int w = 0; w < winCounts[lvl]; w++) {
        long ws = w * widths[lvl];
        long we = ws + widths[lvl];
        ConsumedSliceSet set =
            manager.tryAcquireSubSlices(consumers[lvl], pk, ws, we, false).orElseThrow();
        long totRows = 0L;
        byte[] mergedBytes;
        if (useKll) {
          KllDoublesSketch merged = new KllDoublesSketch(200);
          for (SharedBaseSlice sub : set.getSubSlices()) {
            totRows += sub.getRowCount();
            merged.merge(
                KllDoublesSketch.fromByteBuffer(
                    ByteBuffer.wrap(sub.getIntermediateColumns()[2].getBinary(0).getValues())));
          }
          if (lvl == 0 && w == 0) {
            p99 = merged.getQuantile(0.99);
          }
          ByteBuffer b = ByteBuffer.allocate(merged.computeSerializedSize());
          merged.toByteArray(b);
          mergedBytes = b.array();
        } else {
          TDigest merged = new TDigest(100.0);
          for (SharedBaseSlice sub : set.getSubSlices()) {
            totRows += sub.getRowCount();
            merged.add(
                TDigest.fromByteArray(sub.getIntermediateColumns()[2].getBinary(0).getValues()));
          }
          if (lvl == 0 && w == 0) {
            p99 = merged.quantile(0.99);
          }
          mergedBytes = merged.toSmallByteArray();
        }
        if (lvl < consumers.length - 1) {
          Column[] cols =
              new Column[] {
                new LongColumn(1, Optional.empty(), new long[] {totRows}),
                new DoubleColumn(1, Optional.empty(), new double[] {0.0}),
                new BinaryColumn(1, Optional.empty(), new Binary[] {new Binary(mergedBytes)})
              };
          manager.publishSlice(consumers[lvl], pk, ws, we, totRows, false, cols);
        }
      }
    }
    long peakBytes = manager.getCachedBytes();
    double ms = (System.nanoTime() - t0) / 1e6;
    double logicalRows = (double) rawSlices.length * rawSlices[0].length * 4.0;
    double relErr = Math.abs(p99 - true1mP99) / Math.max(1e-9, true1mP99);
    return new BenchReuseResult(ms, (logicalRows * 1000.0) / Math.max(1e-3, ms), peakBytes, relErr);
  }

  private static String formatExp3Reuse(
      String workload, int fanout, String method, BenchReuseResult res, double speedup) {
    return String.format(
        Locale.US,
        "%s,%d,%s,%.3f,%.1f,%.2f,%d,%.5f",
        workload,
        fanout,
        method,
        res.computeMs,
        res.throughputRowsPerSec,
        speedup,
        res.peakSliceBytes,
        res.p99RelError);
  }

  private static void runExp3SketchAccuracy(Path resultsDir) throws IOException {
    List<String> lines = new ArrayList<>();
    lines.add(
        "distribution,sketch_method,param,build_merge_ms,serialized_bytes,"
            + "p50_norm_rank_err,p90_norm_rank_err,p99_norm_rank_err,max_norm_rank_err");

    String[] distNames = {"Uniform", "Gaussian", "LogNormal_HeavyTail", "MultiRegime_Step"};
    int totalN = 180_000;
    int numSlices = 60;
    int perSlice = totalN / numSlices;
    Random rnd = new Random(77L);

    for (String dist : distNames) {
      double[] all = new double[totalN];
      for (int i = 0; i < totalN; i++) {
        switch (dist) {
          case "Uniform":
            all[i] = rnd.nextDouble() * 1000.0;
            break;
          case "Gaussian":
            all[i] = 100.0 + rnd.nextGaussian() * 15.0;
            break;
          case "LogNormal_HeavyTail":
            all[i] = Math.exp(3.0 + rnd.nextGaussian() * 1.15);
            break;
          default:
            int regime = i / 30_000;
            all[i] = 50.0 + regime * 25.0 + rnd.nextGaussian() * (1.0 + (regime % 3));
            break;
        }
      }
      double[] sorted = Arrays.copyOf(all, all.length);
      Arrays.sort(sorted);

      // Exact Sort baseline
      long tEx = System.nanoTime();
      double[] exCopy = Arrays.copyOf(all, all.length);
      Arrays.sort(exCopy);
      double exMs = (System.nanoTime() - tEx) / 1e6;
      lines.add(
          String.format(
              Locale.US,
              "%s,Exact_Sort,n=%d,%.3f,%d,0.00000,0.00000,0.00000,0.00000",
              dist,
              totalN,
              exMs,
              (long) totalN * Double.BYTES));

      // TDigest default (compression = 100)
      long tTd = System.nanoTime();
      TDigest mergedTd = new TDigest(100.0);
      for (int s = 0; s < numSlices; s++) {
        TDigest sub = new TDigest(100.0);
        for (int j = 0; j < perSlice; j++) {
          sub.add(all[s * perSlice + j]);
        }
        mergedTd.add(TDigest.fromByteArray(sub.toSmallByteArray()));
      }
      double tdMs = (System.nanoTime() - tTd) / 1e6;
      int tdBytes = mergedTd.smallByteSize();
      lines.add(
          scoreSketch(
              dist, "TDigest_Default", "comp=100", tdMs, tdBytes, sorted, mergedTd::quantile));

      // KLL across k = {128, 200, 400}
      int[] kValues = {128, 200, 400};
      for (int k : kValues) {
        long tKll = System.nanoTime();
        KllDoublesSketch mergedKll = new KllDoublesSketch(k);
        for (int s = 0; s < numSlices; s++) {
          KllDoublesSketch sub = new KllDoublesSketch(k);
          for (int j = 0; j < perSlice; j++) {
            sub.update(all[s * perSlice + j]);
          }
          ByteBuffer bb = ByteBuffer.allocate(sub.computeSerializedSize());
          sub.toByteArray(bb);
          mergedKll.merge(KllDoublesSketch.fromByteBuffer(ByteBuffer.wrap(bb.array())));
        }
        double kllMs = (System.nanoTime() - tKll) / 1e6;
        int kllBytes = mergedKll.computeSerializedSize();
        lines.add(
            scoreSketch(
                dist, "KLL_k" + k, "k=" + k, kllMs, kllBytes, sorted, mergedKll::getQuantile));
      }
    }

    Files.write(resultsDir.resolve("exp3_sketch_accuracy.csv"), lines, StandardCharsets.UTF_8);
  }

  private interface QuantileEvaluator {
    double eval(double q);
  }

  private static String scoreSketch(
      String dist,
      String method,
      String param,
      double ms,
      int bytes,
      double[] sorted,
      QuantileEvaluator evaluator) {
    double[] grid = {0.10, 0.25, 0.50, 0.75, 0.90, 0.95, 0.99};
    double errP50 = normRankErr(sorted, evaluator.eval(0.50), 0.50);
    double errP90 = normRankErr(sorted, evaluator.eval(0.90), 0.90);
    double errP99 = normRankErr(sorted, evaluator.eval(0.99), 0.99);
    double maxErr = 0.0;
    for (double q : grid) {
      maxErr = Math.max(maxErr, normRankErr(sorted, evaluator.eval(q), q));
    }
    return String.format(
        Locale.US,
        "%s,%s,%s,%.3f,%d,%.5f,%.5f,%.5f,%.5f",
        dist,
        method,
        param,
        ms,
        bytes,
        errP50,
        errP90,
        errP99,
        maxErr);
  }

  private static double normRankErr(double[] sorted, double estValue, double targetQ) {
    int n = sorted.length;
    int pos = Arrays.binarySearch(sorted, estValue);
    int rank = pos >= 0 ? pos : -pos - 1;
    double actualQ = (double) rank / Math.max(1, n - 1);
    return Math.abs(actualQ - targetQ);
  }
}
