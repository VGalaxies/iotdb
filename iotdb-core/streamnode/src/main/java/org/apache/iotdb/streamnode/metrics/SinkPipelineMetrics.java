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
import org.apache.iotdb.metrics.type.Timer;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;
import org.apache.iotdb.streamnode.engine.sink.SinkPipeline;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

/** Per-stream-task metrics for the sink write-back pipeline. */
public class SinkPipelineMetrics implements IMetricSet {

  private static final String UNKNOWN_TASK_NAME = "unknown";
  private static final String TYPE_SUCCESS = "success";
  private static final String TYPE_FAIL = "fail";

  private final SinkPipeline pipeline;
  private final String taskName;

  private Counter pushCountCounter = DoNothingMetricManager.DO_NOTHING_COUNTER;
  private Counter flushCountCounter = DoNothingMetricManager.DO_NOTHING_COUNTER;
  private Counter writeSuccessCounter = DoNothingMetricManager.DO_NOTHING_COUNTER;
  private Counter writeFailCounter = DoNothingMetricManager.DO_NOTHING_COUNTER;
  private Counter backpressureCountCounter = DoNothingMetricManager.DO_NOTHING_COUNTER;
  private Timer flushLatencyTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer batchLingerTimer = DoNothingMetricManager.DO_NOTHING_TIMER;

  public SinkPipelineMetrics(final SinkPipeline pipeline, final String taskName) {
    this.pipeline = Objects.requireNonNull(pipeline, "pipeline");
    this.taskName = taskName == null ? UNKNOWN_TASK_NAME : taskName;
  }

  @Override
  public void bindTo(final AbstractMetricService metricService) {
    final String streamTag = Tag.STREAM_NAME.toString();
    final String streamName = taskName;
    pushCountCounter =
        metricService.getOrCreateCounter(
            Metric.STREAM_SINK_PUSH_COUNT.toString(), MetricLevel.IMPORTANT, streamTag, streamName);
    flushCountCounter =
        metricService.getOrCreateCounter(
            Metric.STREAM_SINK_FLUSH_COUNT.toString(),
            MetricLevel.IMPORTANT,
            streamTag,
            streamName);
    writeSuccessCounter =
        metricService.getOrCreateCounter(
            Metric.STREAM_SINK_WRITE_COUNT.toString(),
            MetricLevel.IMPORTANT,
            streamTag,
            streamName,
            Tag.TYPE.toString(),
            TYPE_SUCCESS);
    writeFailCounter =
        metricService.getOrCreateCounter(
            Metric.STREAM_SINK_WRITE_COUNT.toString(),
            MetricLevel.IMPORTANT,
            streamTag,
            streamName,
            Tag.TYPE.toString(),
            TYPE_FAIL);
    backpressureCountCounter =
        metricService.getOrCreateCounter(
            Metric.STREAM_SINK_BACKPRESSURE_COUNT.toString(),
            MetricLevel.IMPORTANT,
            streamTag,
            streamName);
    flushLatencyTimer =
        metricService.getOrCreateTimer(
            Metric.STREAM_SINK_FLUSH_TIME.toString(), MetricLevel.IMPORTANT, streamTag, streamName);
    batchLingerTimer =
        metricService.getOrCreateTimer(
            Metric.STREAM_SINK_BATCH_LINGER_TIME.toString(),
            MetricLevel.IMPORTANT,
            streamTag,
            streamName);
    metricService.createAutoGauge(
        Metric.STREAM_SINK_PIPELINE_MEMORY_BYTES.toString(),
        MetricLevel.IMPORTANT,
        pipeline,
        SinkPipeline::getPipelineUsedMemoryBytes,
        streamTag,
        streamName);
    metricService.createAutoGauge(
        Metric.STREAM_SINK_BACKPRESSURE_ACTIVE.toString(),
        MetricLevel.IMPORTANT,
        pipeline,
        p -> p.isBackpressureActive() ? 1.0 : 0.0,
        streamTag,
        streamName);
    metricService.createAutoGauge(
        Metric.STREAM_SINK_COMMITTED_ID.toString(),
        MetricLevel.IMPORTANT,
        pipeline,
        SinkPipeline::getCommittedId,
        streamTag,
        streamName);
    pipeline.setMetrics(this);
  }

  @Override
  public void unbindFrom(final AbstractMetricService metricService) {
    pipeline.setMetrics(null);
    pushCountCounter = DoNothingMetricManager.DO_NOTHING_COUNTER;
    flushCountCounter = DoNothingMetricManager.DO_NOTHING_COUNTER;
    writeSuccessCounter = DoNothingMetricManager.DO_NOTHING_COUNTER;
    writeFailCounter = DoNothingMetricManager.DO_NOTHING_COUNTER;
    backpressureCountCounter = DoNothingMetricManager.DO_NOTHING_COUNTER;
    flushLatencyTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
    batchLingerTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
    final String streamTag = Tag.STREAM_NAME.toString();
    final String streamName = taskName;
    metricService.remove(
        MetricType.COUNTER, Metric.STREAM_SINK_PUSH_COUNT.toString(), streamTag, streamName);
    metricService.remove(
        MetricType.COUNTER, Metric.STREAM_SINK_FLUSH_COUNT.toString(), streamTag, streamName);
    metricService.remove(
        MetricType.COUNTER,
        Metric.STREAM_SINK_WRITE_COUNT.toString(),
        streamTag,
        streamName,
        Tag.TYPE.toString(),
        TYPE_SUCCESS);
    metricService.remove(
        MetricType.COUNTER,
        Metric.STREAM_SINK_WRITE_COUNT.toString(),
        streamTag,
        streamName,
        Tag.TYPE.toString(),
        TYPE_FAIL);
    metricService.remove(
        MetricType.COUNTER,
        Metric.STREAM_SINK_BACKPRESSURE_COUNT.toString(),
        streamTag,
        streamName);
    metricService.remove(
        MetricType.TIMER, Metric.STREAM_SINK_FLUSH_TIME.toString(), streamTag, streamName);
    metricService.remove(
        MetricType.TIMER, Metric.STREAM_SINK_BATCH_LINGER_TIME.toString(), streamTag, streamName);
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.STREAM_SINK_PIPELINE_MEMORY_BYTES.toString(),
        streamTag,
        streamName);
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.STREAM_SINK_BACKPRESSURE_ACTIVE.toString(),
        streamTag,
        streamName);
    metricService.remove(
        MetricType.AUTO_GAUGE, Metric.STREAM_SINK_COMMITTED_ID.toString(), streamTag, streamName);
  }

  public void recordPush() {
    pushCountCounter.inc();
  }

  public void recordFlush() {
    flushCountCounter.inc();
  }

  public void recordWriteSuccess() {
    writeSuccessCounter.inc();
  }

  public void recordWriteFail() {
    writeFailCounter.inc();
  }

  public void recordBackpressure() {
    backpressureCountCounter.inc();
  }

  public void recordFlushLatency(final long duration, final TimeUnit unit) {
    if (duration >= 0) {
      flushLatencyTimer.update(duration, unit);
    }
  }

  public void recordBatchLinger(final long duration, final TimeUnit unit) {
    if (duration >= 0) {
      batchLingerTimer.update(duration, unit);
    }
  }
}
