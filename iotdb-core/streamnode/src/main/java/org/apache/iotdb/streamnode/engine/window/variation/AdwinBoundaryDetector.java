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
 * Adaptive sliding-window Hoeffding cut detector (ADWIN-style baseline) for change-point testing
 * with bounded window history.
 */
public class AdwinBoundaryDetector implements BoundaryDetector {

  private static final int MAX_WINDOW = 256;
  private static final double SIGNIFICANCE_DELTA = 0.05;

  private final double minDelta;
  private final int minSamples;
  private final double[] ring = new double[MAX_WINDOW];
  private int count;
  private boolean inCandidate;
  private int candidateCount;
  private double refMean = Double.NaN;

  public AdwinBoundaryDetector(double minDelta, int minSamples) {
    this.minDelta = Math.max(0.0, minDelta);
    this.minSamples = Math.max(2, minSamples);
  }

  @Override
  public DetectionStepResult onSample(long timestamp, double value) {
    if (!Double.isFinite(value)) {
      return DetectionStepResult.continueStep();
    }
    appendSample(value);
    if (count < minSamples * 2) {
      if (count == 1) {
        refMean = value;
      }
      return DetectionStepResult.continueStep();
    }

    int bestCutSuffix = findHoeffdingCut();
    if (bestCutSuffix > 0) {
      int actualSuffix = inCandidate ? Math.min(candidateCount + 1, bestCutSuffix) : bestCutSuffix;
      retainSuffix(bestCutSuffix);
      inCandidate = false;
      candidateCount = 0;
      return DetectionStepResult.confirmPrefix(Math.max(1, actualSuffix));
    }

    double dev = Math.abs(value - refMean);
    double sigma = computeWindowStd();
    if (dev > Math.max(minDelta, 2.0 * sigma)) {
      inCandidate = true;
      candidateCount++;
      return DetectionStepResult.candidateStep();
    }

    if (inCandidate) {
      inCandidate = false;
      candidateCount = 0;
      refMean = computePrefixMean(count);
      return DetectionStepResult.mergeBack();
    }
    refMean = computePrefixMean(count);
    return DetectionStepResult.continueStep();
  }

  private void appendSample(double value) {
    if (count < MAX_WINDOW) {
      ring[count++] = value;
    } else {
      System.arraycopy(ring, 1, ring, 0, MAX_WINDOW - 1);
      ring[MAX_WINDOW - 1] = value;
    }
  }

  private int findHoeffdingCut() {
    double totalSum = 0.0;
    for (int i = 0; i < count; i++) {
      totalSum += ring[i];
    }
    double prefixSum = 0.0;
    for (int i = 0; i < minSamples - 1; i++) {
      prefixSum += ring[i];
    }
    int bestSuffix = -1;
    double bestDiff = 0.0;
    for (int n0 = minSamples; n0 <= count - minSamples; n0++) {
      prefixSum += ring[n0 - 1];
      int n1 = count - n0;
      double mu0 = prefixSum / n0;
      double mu1 = (totalSum - prefixSum) / n1;
      double pooledVar = Math.max(1e-9, (sse(0, n0, mu0) + sse(n0, count, mu1)) / count);
      double mHarm = 1.0 / (1.0 / n0 + 1.0 / n1);
      double epsCut =
          Math.sqrt((2.0 / mHarm) * pooledVar * Math.log(4.0 * count / SIGNIFICANCE_DELTA));
      double diff = Math.abs(mu0 - mu1);
      if (diff >= Math.max(minDelta, epsCut) && diff > bestDiff) {
        bestDiff = diff;
        bestSuffix = n1;
      }
    }
    return bestSuffix;
  }

  private double sse(int from, int to, double mean) {
    double acc = 0.0;
    for (int i = from; i < to; i++) {
      double d = ring[i] - mean;
      acc += d * d;
    }
    return acc;
  }

  private void retainSuffix(int suffixLen) {
    int start = Math.max(0, count - suffixLen);
    System.arraycopy(ring, start, ring, 0, suffixLen);
    count = suffixLen;
    refMean = computePrefixMean(count);
  }

  private double computeWindowStd() {
    double sum = 0.0;
    double sq = 0.0;
    for (int i = 0; i < count; i++) {
      sum += ring[i];
      sq += ring[i] * ring[i];
    }
    double mean = sum / count;
    return Math.sqrt(Math.max(0.0, (sq / count) - mean * mean));
  }

  private double computePrefixMean(int len) {
    double sum = 0.0;
    for (int i = 0; i < len; i++) {
      sum += ring[i];
    }
    return sum / Math.max(1, len);
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
