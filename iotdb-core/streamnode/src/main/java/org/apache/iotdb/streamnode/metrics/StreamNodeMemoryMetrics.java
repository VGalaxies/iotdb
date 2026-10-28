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

import org.apache.iotdb.commons.memory.MemoryManager;
import org.apache.iotdb.commons.service.metric.enums.Metric;
import org.apache.iotdb.commons.service.metric.enums.Tag;
import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.impl.DoNothingMetricManager;
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.type.Histogram;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;
import org.apache.iotdb.streamnode.conf.StreamNodeDescriptor;
import org.apache.iotdb.streamnode.conf.StreamNodeMemoryConfig;
import org.apache.iotdb.streamnode.engine.computation.planner.StreamExecutionPlanner;
import org.apache.iotdb.streamnode.engine.memory.StreamNodeDataExchangeManager;
import org.apache.iotdb.streamnode.engine.memory.StreamNodeMemoryPool;

/** Metrics for StreamNode compute memory managers, planner reservations and exchange memory. */
public class StreamNodeMemoryMetrics implements IMetricSet {

  private static final StreamNodeMemoryMetrics INSTANCE = new StreamNodeMemoryMetrics();

  private static final StreamNodeMemoryConfig MEMORY_CONFIG =
      StreamNodeDescriptor.getInstance().getMemoryConfig();
  private static final StreamNodeMemoryPool MEMORY_POOL =
      StreamNodeDataExchangeManager.getInstance().getExchangeMemoryPool();

  private static final String ON_HEAP = "OnHeap";
  private static final String LEVEL_2 = "2";
  private static final String LEVEL_3 = "3";

  private static final String STREAM_ENGINE_COMPUTE = "StreamEngine-Compute";
  private static final String STREAM_ENGINE_COMPUTE_OPERATORS = "StreamEngine-ComputeOperators";
  private static final String STREAM_ENGINE_COMPUTE_EXCHANGE = "StreamEngine-ComputeExchange";

  private static final String FREE_MEMORY_FOR_OPERATORS = "free_memory_for_operators";
  private static final String ESTIMATED_MEMORY_SIZE = "estimated_memory_size";

  private static final String MAX_BYTES = "max_bytes";
  private static final String REMAINING_BYTES = "remaining_bytes";
  private static final String QUERY_MEMORY_RESERVATION_SIZE = "query_memory_reservation_size";
  private static final String MEMORY_RESERVATION_SIZE = "memory_reservation_size";

  private Histogram estimatedMemoryHistogram = DoNothingMetricManager.DO_NOTHING_HISTOGRAM;

  private StreamNodeMemoryMetrics() {}

  public static StreamNodeMemoryMetrics getInstance() {
    return INSTANCE;
  }

  @Override
  public void bindTo(AbstractMetricService metricService) {
    createMemoryManagerMetrics(
        metricService,
        STREAM_ENGINE_COMPUTE,
        MEMORY_CONFIG.getComputeMemoryManager(),
        LEVEL_2,
        false);
    createMemoryManagerMetrics(
        metricService,
        STREAM_ENGINE_COMPUTE_OPERATORS,
        MEMORY_CONFIG.getComputeOperatorsMemoryManager(),
        LEVEL_3,
        true);
    createMemoryManagerMetrics(
        metricService,
        STREAM_ENGINE_COMPUTE_EXCHANGE,
        MEMORY_CONFIG.getComputeExchangeMemoryManager(),
        LEVEL_3,
        true);

    metricService.createAutoGauge(
        Metric.LOCAL_EXECUTION_PLANNER.toString(),
        MetricLevel.IMPORTANT,
        StreamExecutionPlanner.getInstance(),
        StreamExecutionPlanner::getFreeMemoryForCompute,
        Tag.NAME.toString(),
        FREE_MEMORY_FOR_OPERATORS);
    estimatedMemoryHistogram =
        metricService.getOrCreateHistogram(
            Metric.LOCAL_EXECUTION_PLANNER.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            ESTIMATED_MEMORY_SIZE);

    metricService
        .getOrCreateGauge(
            Metric.MEMORY_POOL.toString(), MetricLevel.IMPORTANT, Tag.NAME.toString(), MAX_BYTES)
        .set(MEMORY_POOL.getTotalMemoryInBytes());
    metricService.createAutoGauge(
        Metric.MEMORY_POOL.toString(),
        MetricLevel.IMPORTANT,
        MEMORY_POOL,
        StreamNodeMemoryPool::getRemainingMemoryInBytes,
        Tag.NAME.toString(),
        REMAINING_BYTES);
    metricService.createAutoGauge(
        Metric.MEMORY_POOL.toString(),
        MetricLevel.IMPORTANT,
        MEMORY_POOL,
        StreamNodeMemoryPool::getQueryMemoryReservationSize,
        Tag.NAME.toString(),
        QUERY_MEMORY_RESERVATION_SIZE);
    metricService.createAutoGauge(
        Metric.MEMORY_POOL.toString(),
        MetricLevel.IMPORTANT,
        MEMORY_POOL,
        StreamNodeMemoryPool::getMemoryReservationSize,
        Tag.NAME.toString(),
        MEMORY_RESERVATION_SIZE);
  }

  @Override
  public void unbindFrom(AbstractMetricService metricService) {
    removeMemoryManagerMetrics(metricService, STREAM_ENGINE_COMPUTE, LEVEL_2, false);
    removeMemoryManagerMetrics(metricService, STREAM_ENGINE_COMPUTE_OPERATORS, LEVEL_3, true);
    removeMemoryManagerMetrics(metricService, STREAM_ENGINE_COMPUTE_EXCHANGE, LEVEL_3, true);

    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.LOCAL_EXECUTION_PLANNER.toString(),
        Tag.NAME.toString(),
        FREE_MEMORY_FOR_OPERATORS);
    metricService.remove(
        MetricType.HISTOGRAM,
        Metric.LOCAL_EXECUTION_PLANNER.toString(),
        Tag.NAME.toString(),
        ESTIMATED_MEMORY_SIZE);
    estimatedMemoryHistogram = DoNothingMetricManager.DO_NOTHING_HISTOGRAM;

    metricService.remove(
        MetricType.GAUGE, Metric.MEMORY_POOL.toString(), Tag.NAME.toString(), MAX_BYTES);
    metricService.remove(
        MetricType.AUTO_GAUGE, Metric.MEMORY_POOL.toString(), Tag.NAME.toString(), REMAINING_BYTES);
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.MEMORY_POOL.toString(),
        Tag.NAME.toString(),
        QUERY_MEMORY_RESERVATION_SIZE);
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.MEMORY_POOL.toString(),
        Tag.NAME.toString(),
        MEMORY_RESERVATION_SIZE);
  }

  public void updateEstimatedMemory(long estimatedMemorySize) {
    estimatedMemoryHistogram.update(estimatedMemorySize);
  }

  private static void createMemoryManagerMetrics(
      AbstractMetricService metricService,
      String name,
      MemoryManager memoryManager,
      String level,
      boolean recordActualSize) {
    metricService.createAutoGauge(
        Metric.MEMORY_THRESHOLD_SIZE.toString(),
        MetricLevel.IMPORTANT,
        memoryManager,
        MemoryManager::getTotalMemorySizeInBytes,
        Tag.NAME.toString(),
        name,
        Tag.TYPE.toString(),
        ON_HEAP,
        Tag.LEVEL.toString(),
        level);
    if (recordActualSize) {
      metricService.createAutoGauge(
          Metric.MEMORY_ACTUAL_SIZE.toString(),
          MetricLevel.IMPORTANT,
          memoryManager,
          MemoryManager::getUsedMemorySizeInBytes,
          Tag.NAME.toString(),
          name,
          Tag.TYPE.toString(),
          ON_HEAP,
          Tag.LEVEL.toString(),
          level);
    }
  }

  private static void removeMemoryManagerMetrics(
      AbstractMetricService metricService, String name, String level, boolean recordActualSize) {
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.MEMORY_THRESHOLD_SIZE.toString(),
        Tag.NAME.toString(),
        name,
        Tag.TYPE.toString(),
        ON_HEAP,
        Tag.LEVEL.toString(),
        level);
    if (recordActualSize) {
      metricService.remove(
          MetricType.AUTO_GAUGE,
          Metric.MEMORY_ACTUAL_SIZE.toString(),
          Tag.NAME.toString(),
          name,
          Tag.TYPE.toString(),
          ON_HEAP,
          Tag.LEVEL.toString(),
          level);
    }
  }
}
