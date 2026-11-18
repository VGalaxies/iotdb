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

package org.apache.iotdb.streamnode.engine.window.variation;

import java.util.Objects;

/**
 * Result of evaluating one sample against a {@link BoundaryDetector}.
 *
 * <p>On {@link DetectionDecision#CONFIRM_PREFIX}, {@link #getSuffixRowCount()} specifies how many
 * trailing candidate rows (including the current sample) belong to the newly opened regime window.
 */
public final class DetectionStepResult {

  private static final DetectionStepResult CONTINUE_SINGLETON =
      new DetectionStepResult(DetectionDecision.CONTINUE, 0);
  private static final DetectionStepResult CANDIDATE_SINGLETON =
      new DetectionStepResult(DetectionDecision.CANDIDATE, 0);
  private static final DetectionStepResult MERGE_BACK_SINGLETON =
      new DetectionStepResult(DetectionDecision.MERGE_BACK, 0);

  private final DetectionDecision decision;
  private final int suffixRowCount;

  private DetectionStepResult(DetectionDecision decision, int suffixRowCount) {
    this.decision = Objects.requireNonNull(decision);
    this.suffixRowCount = Math.max(0, suffixRowCount);
  }

  public static DetectionStepResult continueStep() {
    return CONTINUE_SINGLETON;
  }

  public static DetectionStepResult candidateStep() {
    return CANDIDATE_SINGLETON;
  }

  public static DetectionStepResult mergeBack() {
    return MERGE_BACK_SINGLETON;
  }

  public static DetectionStepResult confirmPrefix(int suffixRowCount) {
    return new DetectionStepResult(DetectionDecision.CONFIRM_PREFIX, Math.max(1, suffixRowCount));
  }

  public DetectionDecision getDecision() {
    return decision;
  }

  public int getSuffixRowCount() {
    return suffixRowCount;
  }
}
