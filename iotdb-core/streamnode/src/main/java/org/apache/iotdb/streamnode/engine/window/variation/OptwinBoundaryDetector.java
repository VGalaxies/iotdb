/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses file
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

package org.apache.iotdb.streamnode.engine.window.variation;

/**
 * Optimal variance-reduction two-segment split detector (OPTWIN-style baseline) for step-regime
 * boundaries.
 */
public class OptwinBoundaryDetector implements BoundaryDetector {

  private static final int MAX_WINDOW = 256;
  private static final double MIN_VARIANCE_REDUCTION = 0.55;

  private final double minDelta;
  private final int minSamples;
  private final double[] ring = new double[MAX_WINDOW];
  private int count;
  private boolean inCandidate;
  private int candidateCount;
  private double refMean = Double.NaN;

  public OptwinBoundaryDetector(double minDelta, int minSamples) {
    this.minDelta = Math.max(0.0, minDelta);
    this.minSamples = Math.max(2, minSamples);
  }

  @Override
  public DetectionStepResult onSample(long timestamp, double value) {
    if (!Double.isFinite(value)) {
      return DetectionStepResult.continueStep();
    }
    if (count < MAX_WINDOW) {
      ring[count++] = value;
    } else {
      System.arraycopy(ring, 1, ring, 0, MAX_WINDOW - 1);
      ring[MAX_WINDOW - 1] = value;
    }
    if (count == 1) {
      refMean = value;
      return DetectionStepResult.continueStep();
    }
    if (count < minSamples * 2) {
      refMean = mean(0, count);
      return DetectionStepResult.continueStep();
    }

    int bestSuffix = findOptimalSplitSuffix();
    if (bestSuffix > 0) {
      int start = Math.max(0, count - bestSuffix);
      System.arraycopy(ring, start, ring, 0, bestSuffix);
      count = bestSuffix;
      refMean = mean(0, count);
      int actual = inCandidate ? Math.min(candidateCount + 1, bestSuffix) : bestSuffix;
      inCandidate = false;
      candidateCount = 0;
      return DetectionStepResult.confirmPrefix(Math.max(1, actual));
    }

    double sigma = stdDev(0, count);
    if (Math.abs(value - refMean) > Math.max(minDelta, 2.0 * sigma)) {
      inCandidate = true;
      candidateCount++;
      return DetectionStepResult.candidateStep();
    }
    if (inCandidate) {
      inCandidate = false;
      candidateCount = 0;
      refMean = mean(0, count);
      return DetectionStepResult.mergeBack();
    }
    refMean = mean(0, count);
    return DetectionStepResult.continueStep();
  }

  private int findOptimalSplitSuffix() {
    double totalMean = mean(0, count);
    double totalSse = sse(0, count, totalMean);
    if (totalSse <= 1e-12) {
      return -1;
    }
    double bestScore = 0.0;
    int bestSuffix = -1;
    for (int split = minSamples; split <= count - minSamples; split++) {
      double m0 = mean(0, split);
      double m1 = mean(split, count);
      if (Math.abs(m1 - m0) < minDelta) {
        continue;
      }
      double splitSse = sse(0, split, m0) + sse(split, count, m1);
      double reduction = (totalSse - splitSse) / totalSse;
      if (reduction >= MIN_VARIANCE_REDUCTION && reduction > bestScore) {
        bestScore = reduction;
        bestSuffix = count - split;
      }
    }
    return bestSuffix;
  }

  private double mean(int from, int to) {
    double sum = 0.0;
    for (int i = from; i < to; i++) {
      sum += ring[i];
    }
    return sum / Math.max(1, to - from);
  }

  private double sse(int from, int to, double m) {
    double acc = 0.0;
    for (int i = from; i < to; i++) {
      double d = ring[i] - m;
      acc += d * d;
    }
    return acc;
  }

  private double stdDev(int from, int to) {
    int n = Math.max(1, to - from);
    return Math.sqrt(sse(from, to, mean(from, to)) / n);
  }

  @Override
  public void reset() {
    count = 0;
    inCandidate = false;
    candidateCount = 0;
    refMean = Double.NaN;
  }

  @Override
  public double getReferenceValue() {
    return refMean;
  }
}
