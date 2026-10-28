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

package org.apache.iotdb.confignode.manager.stream;

import org.apache.iotdb.commons.service.metric.enums.Metric;
import org.apache.iotdb.commons.service.metric.enums.Tag;
import org.apache.iotdb.commons.stream.StreamTaskStatus;
import org.apache.iotdb.confignode.persistence.stream.StreamInfo;
import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;

import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/** Monitoring Stream task metrics. */
public class StreamMetrics implements IMetricSet {

  private final StreamInfo streamInfo;
  private final Set<String> registeredRunningOn = ConcurrentHashMap.newKeySet();

  @SuppressWarnings("java:S3077")
  private volatile AbstractMetricService metricService;

  public StreamMetrics(final StreamInfo streamInfo) {
    this.streamInfo = streamInfo;
  }

  @Override
  public synchronized void bindTo(final AbstractMetricService metricService) {
    this.metricService = metricService;
    for (final StreamTaskStatus status : StreamTaskStatus.values()) {
      metricService.createAutoGauge(
          Metric.STREAM_TASK_STATUS.toString(),
          MetricLevel.IMPORTANT,
          streamInfo,
          info -> info.countTasksByStatus(status),
          Tag.STATUS.toString(),
          status.name().toLowerCase(Locale.ROOT));
    }
    streamInfo.getAllTasks().stream()
        .map(task -> task.getRunningOn())
        .filter(this::isValidRunningOn)
        .forEach(registeredRunningOn::add);
    registeredRunningOn.forEach(this::createRunningOnAutoGauge);
  }

  @Override
  public synchronized void unbindFrom(final AbstractMetricService metricService) {
    for (final StreamTaskStatus status : StreamTaskStatus.values()) {
      metricService.remove(
          MetricType.AUTO_GAUGE,
          Metric.STREAM_TASK_STATUS.toString(),
          Tag.STATUS.toString(),
          status.name().toLowerCase(Locale.ROOT));
    }
    registeredRunningOn.forEach(
        runningOn ->
            metricService.remove(
                MetricType.AUTO_GAUGE,
                Metric.STREAM_TASK_RUNNING_ON.toString(),
                Tag.RUNNING_ON.toString(),
                runningOn));
    this.metricService = null;
  }

  public synchronized void registerRunningOn(final String runningOn) {
    if (!isValidRunningOn(runningOn) || !registeredRunningOn.add(runningOn)) {
      return;
    }
    final AbstractMetricService currentMetricService = metricService;
    if (currentMetricService != null) {
      createRunningOnAutoGauge(runningOn);
    }
  }

  private void createRunningOnAutoGauge(final String runningOn) {
    metricService.createAutoGauge(
        Metric.STREAM_TASK_RUNNING_ON.toString(),
        MetricLevel.IMPORTANT,
        streamInfo,
        info -> info.countTasksByRunningOn(runningOn),
        Tag.RUNNING_ON.toString(),
        runningOn);
  }

  private boolean isValidRunningOn(final String runningOn) {
    return runningOn != null && !runningOn.isEmpty();
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final StreamMetrics that = (StreamMetrics) o;
    return streamInfo.equals(that.streamInfo);
  }

  @Override
  public int hashCode() {
    return Objects.hash(streamInfo);
  }
}
