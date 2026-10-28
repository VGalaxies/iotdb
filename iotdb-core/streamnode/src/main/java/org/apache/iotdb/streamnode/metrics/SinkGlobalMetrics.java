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
import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;
import org.apache.iotdb.streamnode.engine.sink.GlobalSinkMemoryController;
import org.apache.iotdb.streamnode.engine.sink.GlobalSinkWriterPool;

/** Node-level sink metrics shared by all write-back pipelines. */
public class SinkGlobalMetrics implements IMetricSet {

  private static final SinkGlobalMetrics INSTANCE = new SinkGlobalMetrics();

  private SinkGlobalMetrics() {}

  public static SinkGlobalMetrics getInstance() {
    return INSTANCE;
  }

  @Override
  public void bindTo(final AbstractMetricService metricService) {
    metricService.createAutoGauge(
        Metric.STREAM_SINK_GLOBAL_MEMORY_BYTES.toString(),
        MetricLevel.IMPORTANT,
        GlobalSinkMemoryController.getInstance(),
        GlobalSinkMemoryController::getUsedMemoryInBytes);
    metricService.createAutoGauge(
        Metric.STREAM_SINK_READY_PIPELINE_COUNT.toString(),
        MetricLevel.IMPORTANT,
        GlobalSinkWriterPool.getInstance(),
        GlobalSinkWriterPool::getReadyPipelineCount);
  }

  @Override
  public void unbindFrom(final AbstractMetricService metricService) {
    metricService.remove(MetricType.AUTO_GAUGE, Metric.STREAM_SINK_GLOBAL_MEMORY_BYTES.toString());
    metricService.remove(MetricType.AUTO_GAUGE, Metric.STREAM_SINK_READY_PIPELINE_COUNT.toString());
  }
}
