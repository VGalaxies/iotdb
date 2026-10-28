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

import org.apache.iotdb.commons.service.metric.enums.Tag;
import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.impl.DoNothingMetricManager;
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.type.Counter;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;
import org.apache.iotdb.streamnode.engine.scheduler.IStreamTaskScheduler;
import org.apache.iotdb.streamnode.manager.StreamTaskManager;

/**
 * Stream task scheduler metrics. Exposes node-level aggregates of the {@link IStreamTaskScheduler}
 * state: registered/running/blocked DriverTask counts, ready queue backlog, and cumulative
 * transitions into the ABORTED terminal state. Registered with {@code MetricService} via {@code
 * addMetricSet} during StreamNode start-up.
 */
public class StreamSchedulerMetrics implements IMetricSet {

  private static final StreamSchedulerMetrics INSTANCE = new StreamSchedulerMetrics();

  public static final String STREAM_SCHEDULER_REGISTERED_TASKS_TOTAL =
      "stream_scheduler_registered_tasks_total";
  public static final String STREAM_SCHEDULER_RUNNING_TASKS_TOTAL =
      "stream_scheduler_running_tasks_total";
  public static final String STREAM_SCHEDULER_READY_QUEUE_SIZE =
      "stream_scheduler_ready_queue_size";
  public static final String STREAM_SCHEDULER_BLOCKED_TASKS_TOTAL =
      "stream_scheduler_blocked_tasks_total";
  public static final String STREAM_SCHEDULER_ABORTED_TOTAL = "stream_scheduler_aborted_total";

  private static final String TAG_VALUE = "stream_scheduler";

  private Counter abortedTotalCounter = DoNothingMetricManager.DO_NOTHING_COUNTER;

  private StreamSchedulerMetrics() {}

  public static StreamSchedulerMetrics getInstance() {
    return INSTANCE;
  }

  @Override
  public void bindTo(AbstractMetricService metricService) {
    IStreamTaskScheduler scheduler = StreamTaskManager.getInstance().getScheduler();
    metricService.createAutoGauge(
        STREAM_SCHEDULER_REGISTERED_TASKS_TOTAL,
        MetricLevel.IMPORTANT,
        scheduler,
        IStreamTaskScheduler::getTaskCount,
        Tag.NAME.toString(),
        TAG_VALUE);
    metricService.createAutoGauge(
        STREAM_SCHEDULER_RUNNING_TASKS_TOTAL,
        MetricLevel.IMPORTANT,
        scheduler,
        IStreamTaskScheduler::getRunningTaskCount,
        Tag.NAME.toString(),
        TAG_VALUE);
    metricService.createAutoGauge(
        STREAM_SCHEDULER_READY_QUEUE_SIZE,
        MetricLevel.IMPORTANT,
        scheduler,
        IStreamTaskScheduler::getReadyQueueSize,
        Tag.NAME.toString(),
        TAG_VALUE);
    metricService.createAutoGauge(
        STREAM_SCHEDULER_BLOCKED_TASKS_TOTAL,
        MetricLevel.IMPORTANT,
        scheduler,
        IStreamTaskScheduler::getBlockedTaskCount,
        Tag.NAME.toString(),
        TAG_VALUE);
    abortedTotalCounter =
        metricService.getOrCreateCounter(
            STREAM_SCHEDULER_ABORTED_TOTAL, MetricLevel.IMPORTANT, Tag.NAME.toString(), TAG_VALUE);
  }

  @Override
  public void unbindFrom(AbstractMetricService metricService) {
    abortedTotalCounter = DoNothingMetricManager.DO_NOTHING_COUNTER;
    metricService.remove(
        MetricType.AUTO_GAUGE,
        STREAM_SCHEDULER_REGISTERED_TASKS_TOTAL,
        Tag.NAME.toString(),
        TAG_VALUE);
    metricService.remove(
        MetricType.AUTO_GAUGE,
        STREAM_SCHEDULER_RUNNING_TASKS_TOTAL,
        Tag.NAME.toString(),
        TAG_VALUE);
    metricService.remove(
        MetricType.AUTO_GAUGE, STREAM_SCHEDULER_READY_QUEUE_SIZE, Tag.NAME.toString(), TAG_VALUE);
    metricService.remove(
        MetricType.AUTO_GAUGE,
        STREAM_SCHEDULER_BLOCKED_TASKS_TOTAL,
        Tag.NAME.toString(),
        TAG_VALUE);
    metricService.remove(
        MetricType.COUNTER, STREAM_SCHEDULER_ABORTED_TOTAL, Tag.NAME.toString(), TAG_VALUE);
  }

  /**
   * Increments the cumulative DriverTask ABORTED counter. Called by {@code
   * StreamTaskScheduler#clearDriverTask} after a successful state transition into ABORTED, so each
   * task is counted exactly once. Safe to call before {@link #bindTo}.
   */
  public void incrementAbortedTotal() {
    abortedTotalCounter.inc();
  }
}
