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
import org.apache.iotdb.metrics.type.Counter;
import org.apache.iotdb.metrics.type.Histogram;
import org.apache.iotdb.metrics.type.Timer;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;
import org.apache.iotdb.streamnode.engine.dispatcher.ColumnPartitionedTabletDispatcher;

import java.util.Objects;

/** Metrics for the tablet partitioning stage of a stream task. */
public class StreamDispatcherMetrics implements IMetricSet {

  private static final String UNKNOWN_TASK_NAME = "unknown";

  private final ColumnPartitionedTabletDispatcher dispatcher;
  private final String taskName;

  private Counter tabletCountCounter = DoNothingMetricManager.DO_NOTHING_COUNTER;
  private Histogram tabletGroupCountHistogram = DoNothingMetricManager.DO_NOTHING_HISTOGRAM;
  private Histogram sliceSizeHistogram = DoNothingMetricManager.DO_NOTHING_HISTOGRAM;
  private Timer splitLatencyTimer = DoNothingMetricManager.DO_NOTHING_TIMER;

  public StreamDispatcherMetrics(
      final ColumnPartitionedTabletDispatcher dispatcher, final String taskName) {
    this.dispatcher = Objects.requireNonNull(dispatcher);
    this.taskName = taskName == null ? UNKNOWN_TASK_NAME : taskName;
  }

  @Override
  public void bindTo(final AbstractMetricService metricService) {
    final String streamTag = Tag.STREAM_NAME.toString();
    final String streamName = taskName;
    tabletCountCounter =
        metricService.getOrCreateCounter(
            Metric.STREAM_DISPATCHER_TABLET_COUNT.toString(),
            MetricLevel.IMPORTANT,
            streamTag,
            streamName);
    tabletGroupCountHistogram =
        metricService.getOrCreateHistogram(
            Metric.STREAM_DISPATCHER_TABLET_GROUP_COUNT.toString(),
            MetricLevel.IMPORTANT,
            streamTag,
            streamName);
    sliceSizeHistogram =
        metricService.getOrCreateHistogram(
            Metric.STREAM_DISPATCHER_SLICE_SIZE.toString(),
            MetricLevel.IMPORTANT,
            streamTag,
            streamName);
    splitLatencyTimer =
        metricService.getOrCreateTimer(
            Metric.STREAM_DISPATCHER_SPLIT_TIME.toString(),
            MetricLevel.IMPORTANT,
            streamTag,
            streamName);
    dispatcher.setMetrics(
        tabletCountCounter, tabletGroupCountHistogram, sliceSizeHistogram, splitLatencyTimer);
    metricService.createAutoGauge(
        Metric.STREAM_DISPATCHER_AVERAGE_SLICE_SIZE.toString(),
        MetricLevel.IMPORTANT,
        dispatcher,
        ColumnPartitionedTabletDispatcher::getAverageSliceSize,
        streamTag,
        streamName);
  }

  @Override
  public void unbindFrom(final AbstractMetricService metricService) {
    dispatcher.setMetrics(
        DoNothingMetricManager.DO_NOTHING_COUNTER,
        DoNothingMetricManager.DO_NOTHING_HISTOGRAM,
        DoNothingMetricManager.DO_NOTHING_HISTOGRAM,
        DoNothingMetricManager.DO_NOTHING_TIMER);
    tabletCountCounter = DoNothingMetricManager.DO_NOTHING_COUNTER;
    tabletGroupCountHistogram = DoNothingMetricManager.DO_NOTHING_HISTOGRAM;
    sliceSizeHistogram = DoNothingMetricManager.DO_NOTHING_HISTOGRAM;
    splitLatencyTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
    final String streamTag = Tag.STREAM_NAME.toString();
    final String streamName = taskName;
    metricService.remove(
        MetricType.COUNTER,
        Metric.STREAM_DISPATCHER_TABLET_COUNT.toString(),
        streamTag,
        streamName);
    metricService.remove(
        MetricType.HISTOGRAM,
        Metric.STREAM_DISPATCHER_TABLET_GROUP_COUNT.toString(),
        streamTag,
        streamName);
    metricService.remove(
        MetricType.HISTOGRAM,
        Metric.STREAM_DISPATCHER_SLICE_SIZE.toString(),
        streamTag,
        streamName);
    metricService.remove(
        MetricType.TIMER, Metric.STREAM_DISPATCHER_SPLIT_TIME.toString(), streamTag, streamName);
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.STREAM_DISPATCHER_AVERAGE_SLICE_SIZE.toString(),
        streamTag,
        streamName);
  }
}
