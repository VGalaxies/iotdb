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
package org.apache.iotdb.streamnode.metrics;

import org.apache.iotdb.commons.service.metric.enums.Metric;
import org.apache.iotdb.commons.service.metric.enums.Tag;
import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.impl.DoNothingMetricManager;
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.type.Timer;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;

import java.util.concurrent.TimeUnit;

/** Metrics for StreamNode compute tasks and their operators. */
public class StreamComputeMetrics implements IMetricSet {

  private static final StreamComputeMetrics INSTANCE = new StreamComputeMetrics();

  private static final String DRIVER_INTERNAL_PROCESS = "driver_internal_process";
  private static final String COMPUTE = "compute";
  private static final String RECOMPUTE = "recompute";

  private Timer computeExecutionTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer recomputeExecutionTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private AbstractMetricService metricService;

  private StreamComputeMetrics() {}

  public static StreamComputeMetrics getInstance() {
    return INSTANCE;
  }

  @Override
  public void bindTo(AbstractMetricService metricService) {
    this.metricService = metricService;
    computeExecutionTimer =
        metricService.getOrCreateTimer(
            Metric.QUERY_EXECUTION.toString(),
            MetricLevel.IMPORTANT,
            Tag.STAGE.toString(),
            DRIVER_INTERNAL_PROCESS,
            Tag.TYPE.toString(),
            COMPUTE);
    recomputeExecutionTimer =
        metricService.getOrCreateTimer(
            Metric.QUERY_EXECUTION.toString(),
            MetricLevel.IMPORTANT,
            Tag.STAGE.toString(),
            DRIVER_INTERNAL_PROCESS,
            Tag.TYPE.toString(),
            RECOMPUTE);
  }

  @Override
  public void unbindFrom(AbstractMetricService metricService) {
    metricService.remove(
        MetricType.TIMER,
        Metric.QUERY_EXECUTION.toString(),
        Tag.STAGE.toString(),
        DRIVER_INTERNAL_PROCESS,
        Tag.TYPE.toString(),
        COMPUTE);
    metricService.remove(
        MetricType.TIMER,
        Metric.QUERY_EXECUTION.toString(),
        Tag.STAGE.toString(),
        DRIVER_INTERNAL_PROCESS,
        Tag.TYPE.toString(),
        RECOMPUTE);
    computeExecutionTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
    recomputeExecutionTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
    this.metricService = null;
  }

  public void recordExecutionTime(boolean isRecompute, long costTimeInNanos) {
    if (isRecompute) {
      recomputeExecutionTimer.updateNanos(costTimeInNanos);
    } else {
      computeExecutionTimer.updateNanos(costTimeInNanos);
    }
  }

  public void recordOperatorExecutionCost(
      String operatorType, boolean isRecompute, long costTimeInNanos) {
    if (metricService == null) {
      return;
    }
    metricService.timer(
        costTimeInNanos,
        TimeUnit.NANOSECONDS,
        Metric.OPERATOR_EXECUTION_COST.toString(),
        MetricLevel.IMPORTANT,
        Tag.NAME.toString(),
        operatorType,
        Tag.TYPE.toString(),
        getComputeType(isRecompute));
  }

  public void recordOperatorExecutionCount(String operatorType, boolean isRecompute, long count) {
    if (metricService == null) {
      return;
    }
    metricService.count(
        count,
        Metric.OPERATOR_EXECUTION_COUNT.toString(),
        MetricLevel.IMPORTANT,
        Tag.NAME.toString(),
        operatorType,
        Tag.TYPE.toString(),
        getComputeType(isRecompute));
  }

  private static String getComputeType(boolean isRecompute) {
    return isRecompute ? RECOMPUTE : COMPUTE;
  }
}
