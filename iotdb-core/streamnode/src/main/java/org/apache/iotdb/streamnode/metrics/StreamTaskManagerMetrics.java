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
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;
import org.apache.iotdb.streamnode.manager.StreamTaskManager;

/**
 * Stream task management metrics. Exposes node-level aggregates of the {@link StreamTaskManager}
 * state: total stream tasks, currently running stream tasks, and current active sub-task count.
 * Registered with {@code MetricService} via {@code addMetricSet} during StreamNode start-up.
 *
 * <p>{@code stream_sub_task_total} is an AutoGauge backed by {@code
 * StreamTaskManager#getSubTaskTotal()} (sum of {@code subTaskExecutions.size()} across instances).
 * This naturally tracks the current count: increments on sub-task creation, decrements on removal
 * (failure/drop), and stays stable across rebuild (remove + recreate).
 */
public class StreamTaskManagerMetrics implements IMetricSet {

  private static final StreamTaskManagerMetrics INSTANCE = new StreamTaskManagerMetrics();

  public static final String STREAM_TASK_TOTAL = "stream_task_total";
  public static final String STREAM_TASK_RUNNING_TOTAL = "stream_task_running_total";
  public static final String STREAM_SUB_TASK_TOTAL = "stream_sub_task_total";

  private static final String TAG_VALUE = "stream_task";

  private StreamTaskManagerMetrics() {}

  public static StreamTaskManagerMetrics getInstance() {
    return INSTANCE;
  }

  @Override
  public void bindTo(AbstractMetricService metricService) {
    metricService.createAutoGauge(
        STREAM_TASK_TOTAL,
        MetricLevel.IMPORTANT,
        StreamTaskManager.getInstance(),
        StreamTaskManager::getTaskCount,
        Tag.NAME.toString(),
        TAG_VALUE);
    metricService.createAutoGauge(
        STREAM_TASK_RUNNING_TOTAL,
        MetricLevel.IMPORTANT,
        StreamTaskManager.getInstance(),
        StreamTaskManager::getRunningStreamTaskCount,
        Tag.NAME.toString(),
        TAG_VALUE);
    metricService.createAutoGauge(
        STREAM_SUB_TASK_TOTAL,
        MetricLevel.IMPORTANT,
        StreamTaskManager.getInstance(),
        StreamTaskManager::getSubTaskTotal,
        Tag.NAME.toString(),
        TAG_VALUE);
  }

  @Override
  public void unbindFrom(AbstractMetricService metricService) {
    metricService.remove(MetricType.AUTO_GAUGE, STREAM_TASK_TOTAL, Tag.NAME.toString(), TAG_VALUE);
    metricService.remove(
        MetricType.AUTO_GAUGE, STREAM_TASK_RUNNING_TOTAL, Tag.NAME.toString(), TAG_VALUE);
    metricService.remove(
        MetricType.AUTO_GAUGE, STREAM_SUB_TASK_TOTAL, Tag.NAME.toString(), TAG_VALUE);
  }
}
