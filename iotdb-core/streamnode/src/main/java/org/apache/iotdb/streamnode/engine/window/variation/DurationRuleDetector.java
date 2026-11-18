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

/** Fixed N-consecutive-sample duration rule baseline detector (confirms after N deviations). */
public class DurationRuleDetector implements BoundaryDetector {

  private final double delta;
  private final int minSamples;
  private boolean initialized;
  private double refValue;
  private int candidateCount;
  private double candidateSum;

  public DurationRuleDetector(double delta, int minSamples) {
    this.delta = Math.max(0.0, delta);
    this.minSamples = Math.max(1, minSamples);
  }

  @Override
  public DetectionStepResult onSample(long timestamp, double value) {
    if (!Double.isFinite(value)) {
      return DetectionStepResult.continueStep();
    }
    if (!initialized) {
      initialized = true;
      refValue = value;
      return DetectionStepResult.continueStep();
    }
    if (Math.abs(value - refValue) > delta) {
      candidateCount++;
      candidateSum += value;
      if (candidateCount >= minSamples) {
        int suffix = candidateCount;
        refValue = candidateSum / candidateCount;
        candidateCount = 0;
        candidateSum = 0.0;
        return DetectionStepResult.confirmPrefix(suffix);
      }
      return DetectionStepResult.candidateStep();
    }
    if (candidateCount > 0) {
      candidateCount = 0;
      candidateSum = 0.0;
      return DetectionStepResult.mergeBack();
    }
    return DetectionStepResult.continueStep();
  }

  @Override
  public void reset() {
    initialized = false;
    refValue = Double.NaN;
    candidateCount = 0;
    candidateSum = 0.0;
  }

  @Override
  public double getReferenceValue() {
    return refValue;
  }
}
