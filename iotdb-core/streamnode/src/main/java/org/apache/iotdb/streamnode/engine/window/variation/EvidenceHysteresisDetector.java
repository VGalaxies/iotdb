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

import java.util.Arrays;

/**
 * Section 4.4.1 regime-aware time-weighted evidence + hysteresis detector:
 *
 * <pre>
 * E_n = max(0, rho_n * E_{n-1} + (|v_n - v_ref| - eps) * delta_t_eff)
 * </pre>
 *
 * <p>Combines bounded gap forgetting ({@code rho_n = exp(-dt_n / tau)}), Median/MAD adaptive noise
 * floor updated only outside candidate phases (frozen once a candidate excursion begins), and
 * candidate persistence ({@code minSamples}) with exact onset backtracking.
 */
public class EvidenceHysteresisDetector implements BoundaryDetector {

  private static final int CONFIRMED_WINDOW_CAP = 64;
  private static final double MAD_SCALE = 1.4826;
  private static final double ADAPTIVE_SIGMA_MULTIPLIER = 3.0;

  private final double theta;
  private final double epsilonBase;
  private final long tauMs;
  private final long dtMaxMs;
  private final int minSamples;
  private final boolean adaptiveBaseline;

  private final double[] confirmedBuffer = new double[CONFIRMED_WINDOW_CAP];
  private int confirmedCount;
  private int confirmedCursor;

  private final double[] candidateValues = new double[CONFIRMED_WINDOW_CAP];
  private int candidateValueCount;

  private boolean initialized;
  private double refValue;
  private long lastTimestamp;
  private double evidence;

  private boolean inCandidate;
  private int candidateCount;
  private int excursionCount;
  private int consecutiveExcursionCount;
  private int excursionSign;
  private int consecutiveBaselineReturn;
  private double frozenEpsilon;
  private double candidateSum;

  public EvidenceHysteresisDetector(
      double theta,
      double epsilonBase,
      long tauMs,
      long dtMaxMs,
      int minSamples,
      boolean adaptiveBaseline) {
    this.theta = Math.max(0.0, theta);
    this.epsilonBase = Math.max(0.0, epsilonBase);
    this.tauMs = Math.max(1L, tauMs);
    this.dtMaxMs = Math.max(1L, dtMaxMs);
    this.minSamples = Math.max(1, minSamples);
    this.adaptiveBaseline = adaptiveBaseline;
  }

  @Override
  public DetectionStepResult onSample(long timestamp, double value) {
    if (!Double.isFinite(value)) {
      return DetectionStepResult.continueStep();
    }
    if (!initialized) {
      initialized = true;
      refValue = value;
      lastTimestamp = timestamp;
      evidence = 0.0;
      recordConfirmed(value);
      return DetectionStepResult.continueStep();
    }

    long rawDtMs = Math.max(0L, timestamp - lastTimestamp);
    lastTimestamp = timestamp;
    if (!inCandidate && rawDtMs > dtMaxMs) {
      evidence = 0.0;
    }

    long boundedDtMs = Math.min(rawDtMs, dtMaxMs);
    double rho = Math.exp(-((double) boundedDtMs) / tauMs);
    double dtEff = Math.max(1e-3, boundedDtMs / 1000.0);
    double eps = inCandidate ? frozenEpsilon : computeAdaptiveEpsilon();
    double dev = Math.abs(value - refValue);
    evidence = Math.max(0.0, rho * evidence + (dev - eps) * dtEff);

    if (!inCandidate) {
      if (dev > eps) {
        inCandidate = true;
        frozenEpsilon = eps;
        candidateCount = 1;
        excursionCount = 1;
        consecutiveExcursionCount = 1;
        excursionSign = value >= refValue ? 1 : -1;
        consecutiveBaselineReturn = 0;
        candidateSum = value;
        candidateValueCount = 0;
        recordCandidateValue(value);
        if (minSamples <= 1 && evidence >= theta) {
          resetToNewRegime(value, timestamp);
          return DetectionStepResult.confirmPrefix(1);
        }
        return DetectionStepResult.candidateStep();
      }
      recordConfirmed(value);
      if (adaptiveBaseline) {
        refValue = computeConfirmedMedian();
      }
      return DetectionStepResult.continueStep();
    }

    candidateCount++;
    candidateSum += value;
    recordCandidateValue(value);
    int stepSign = value >= refValue ? 1 : -1;
    boolean oppositeSideExcursion =
        dev > frozenEpsilon && excursionSign != 0 && stepSign != excursionSign;
    if (dev > frozenEpsilon && !oppositeSideExcursion) {
      excursionCount++;
      consecutiveExcursionCount++;
      consecutiveBaselineReturn = 0;
    } else {
      consecutiveExcursionCount = 0;
      consecutiveBaselineReturn++;
    }

    if (consecutiveExcursionCount >= minSamples && evidence >= theta && !oppositeSideExcursion) {
      int suffixRows = candidateCount;
      double nextRef = candidateSum / candidateCount;
      resetToNewRegime(nextRef, timestamp);
      return DetectionStepResult.confirmPrefix(suffixRows);
    }

    if (oppositeSideExcursion
        || (dev <= frozenEpsilon && consecutiveExcursionCount == 0 && candidateCount <= minSamples)
        || (rawDtMs > dtMaxMs && consecutiveBaselineReturn > 0)
        || consecutiveBaselineReturn >= 2
        || (evidence <= 0.0 && consecutiveBaselineReturn >= 1)) {
      for (int i = 0; i < candidateValueCount; i++) {
        recordConfirmed(candidateValues[i]);
      }
      if (adaptiveBaseline) {
        refValue = computeConfirmedMedian();
      }
      inCandidate = false;
      candidateCount = 0;
      excursionCount = 0;
      consecutiveExcursionCount = 0;
      excursionSign = 0;
      consecutiveBaselineReturn = 0;
      candidateValueCount = 0;
      evidence = 0.0;
      return DetectionStepResult.mergeBack();
    }

    return DetectionStepResult.candidateStep();
  }

  private void recordCandidateValue(double value) {
    if (candidateValueCount < CONFIRMED_WINDOW_CAP) {
      candidateValues[candidateValueCount++] = value;
    }
  }

  private void resetToNewRegime(double nextRef, long timestamp) {
    inCandidate = false;
    candidateCount = 0;
    excursionCount = 0;
    consecutiveExcursionCount = 0;
    excursionSign = 0;
    consecutiveBaselineReturn = 0;
    candidateValueCount = 0;
    evidence = 0.0;
    confirmedCount = 0;
    confirmedCursor = 0;
    refValue = nextRef;
    lastTimestamp = timestamp;
    recordConfirmed(nextRef);
  }

  private void recordConfirmed(double value) {
    confirmedBuffer[confirmedCursor] = value;
    confirmedCursor = (confirmedCursor + 1) % CONFIRMED_WINDOW_CAP;
    if (confirmedCount < CONFIRMED_WINDOW_CAP) {
      confirmedCount++;
    }
  }

  private double computeConfirmedMedian() {
    if (confirmedCount == 0) {
      return refValue;
    }
    double[] copy = Arrays.copyOf(confirmedBuffer, confirmedCount);
    Arrays.sort(copy);
    int mid = confirmedCount / 2;
    if ((confirmedCount & 1) == 1) {
      return copy[mid];
    }
    return 0.5 * (copy[mid - 1] + copy[mid]);
  }

  private double computeAdaptiveEpsilon() {
    if (!adaptiveBaseline || confirmedCount < 3) {
      return epsilonBase;
    }
    double[] sorted = Arrays.copyOf(confirmedBuffer, confirmedCount);
    Arrays.sort(sorted);
    int mid = confirmedCount / 2;
    double median =
        ((confirmedCount & 1) == 1) ? sorted[mid] : 0.5 * (sorted[mid - 1] + sorted[mid]);
    double[] absDevs = new double[confirmedCount];
    for (int i = 0; i < confirmedCount; i++) {
      absDevs[i] = Math.abs(confirmedBuffer[i] - median);
    }
    Arrays.sort(absDevs);
    double mad =
        ((confirmedCount & 1) == 1) ? absDevs[mid] : 0.5 * (absDevs[mid - 1] + absDevs[mid]);
    double q25 = sorted[confirmedCount / 4];
    double q75 = sorted[(3 * confirmedCount) / 4];
    double iqrSigma = 0.7413 * (q75 - q25);
    double sigmaHat = Math.max(MAD_SCALE * mad, iqrSigma);
    return Math.max(epsilonBase, ADAPTIVE_SIGMA_MULTIPLIER * sigmaHat);
  }

  @Override
  public void reset() {
    initialized = false;
    refValue = Double.NaN;
    lastTimestamp = 0L;
    evidence = 0.0;
    inCandidate = false;
    candidateCount = 0;
    excursionCount = 0;
    consecutiveExcursionCount = 0;
    excursionSign = 0;
    consecutiveBaselineReturn = 0;
    candidateValueCount = 0;
    frozenEpsilon = epsilonBase;
    candidateSum = 0.0;
    confirmedCount = 0;
    confirmedCursor = 0;
  }

  @Override
  public double getReferenceValue() {
    return refValue;
  }
}
