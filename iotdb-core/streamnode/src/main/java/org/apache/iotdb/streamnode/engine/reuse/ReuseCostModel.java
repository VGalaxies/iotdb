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

package org.apache.iotdb.streamnode.engine.reuse;

/**
 * 4-term cost-benefit evaluator for multi-granularity slice reuse (§4.5): balances avoided raw
 * row-scan cost against slice serialization/deserialization, merge, and bounded retention memory
 * cost.
 */
public final class ReuseCostModel {

  private static final double SCAN_COST_PER_ROW = 1.0;
  private static final double MERGE_DESER_COST_PER_SLICE = 1.5;
  private static final double SERIALIZE_PUBLISH_COST = 1.0;
  private static final double RETENTION_COST_PER_SLICE = 0.05;

  private ReuseCostModel() {}

  public static final class CostDecision {
    private final boolean beneficial;
    private final double netBenefit;
    private final double benefitRatio;

    public CostDecision(boolean beneficial, double netBenefit, double benefitRatio) {
      this.beneficial = beneficial;
      this.netBenefit = netBenefit;
      this.benefitRatio = benefitRatio;
    }

    public boolean isBeneficial() {
      return beneficial;
    }

    public double getNetBenefit() {
      return netBenefit;
    }

    public double getBenefitRatio() {
      return benefitRatio;
    }
  }

  public static CostDecision evaluate(
      int fanOutM, long estimatedRowsPerSubSlice, double costThreshold) {
    if (fanOutM < 2 || estimatedRowsPerSubSlice <= 0) {
      return new CostDecision(false, 0.0, 0.0);
    }
    double scanSavings = fanOutM * estimatedRowsPerSubSlice * SCAN_COST_PER_ROW;
    double reuseOverhead =
        fanOutM * MERGE_DESER_COST_PER_SLICE
            + SERIALIZE_PUBLISH_COST
            + RETENTION_COST_PER_SLICE * Math.max(0, fanOutM - 1);
    double net = scanSavings - reuseOverhead;
    double ratio = net / Math.max(1.0, scanSavings);
    boolean ok = net > 0.0 && ratio >= costThreshold;
    return new CostDecision(ok, net, ratio);
  }
}
