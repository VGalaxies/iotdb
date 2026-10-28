/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.streamnode.metrics;

import org.apache.iotdb.commons.service.metric.enums.Metric;
import org.apache.iotdb.commons.service.metric.enums.Tag;
import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.impl.DoNothingMetricManager;
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.type.Counter;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;
import org.apache.iotdb.streamnode.engine.window.GlobalWindowMemoryController;

import java.util.function.ToDoubleFunction;

/**
 * Metrics of the window engines. Node-level aggregates, updated in place by the engines via the
 * {@code inc*} methods (safe to call before {@link #bindTo}), plus gauges read live from the three
 * memory pools and the backpressure state of {@link GlobalWindowMemoryController}.
 */
public class StreamWindowEngineMetrics implements IMetricSet {

  private static final StreamWindowEngineMetrics INSTANCE = new StreamWindowEngineMetrics();

  private static final GlobalWindowMemoryController CONTROLLER =
      GlobalWindowMemoryController.getInstance();

  private static final String TAG_VALUE = "stream_window_engine";

  // ===== memory pool watermarks (per pool) =====
  private static final String STREAM_ENGINE_WINDOW_INPUT = "StreamEngine-WindowInput";
  private static final String STREAM_ENGINE_WINDOW_DATA = "StreamEngine-WindowData";
  private static final String STREAM_ENGINE_WINDOW_OUTPUT = "StreamEngine-WindowOutput";

  // ===== backpressure state gauges =====
  public static final String STREAM_WINDOW_ENGINE_MEMORY_PAUSED_ENGINES =
      "stream_window_engine_memory_paused_engines";
  public static final String STREAM_WINDOW_ENGINE_INPUT_WAITERS =
      "stream_window_engine_input_waiters";

  // ===== counters =====
  public static final String STREAM_WINDOW_ENGINE_PUSH_TOTAL = "stream_window_engine_push_total";
  public static final String STREAM_WINDOW_ENGINE_PUSH_ROW_TOTAL =
      "stream_window_engine_push_row_total";
  public static final String STREAM_WINDOW_ENGINE_DROPPED_BATCH_TOTAL =
      "stream_window_engine_dropped_batch_total";
  public static final String STREAM_WINDOW_ENGINE_MEMORY_PAUSE_TOTAL =
      "stream_window_engine_memory_pause_total";
  public static final String STREAM_WINDOW_ENGINE_INCREMENTAL_EVENT_TOTAL =
      "stream_window_engine_incremental_event_total";
  public static final String STREAM_WINDOW_ENGINE_CLOSE_EVENT_TOTAL =
      "stream_window_engine_close_event_total";
  public static final String STREAM_WINDOW_ENGINE_RECOMPUTE_EVENT_TOTAL =
      "stream_window_engine_recompute_event_total";
  public static final String STREAM_WINDOW_ENGINE_DESTROY_EVENT_TOTAL =
      "stream_window_engine_destroy_event_total";
  public static final String STREAM_WINDOW_ENGINE_MATERIALIZED_BLOCK_TOTAL =
      "stream_window_engine_materialized_block_total";
  public static final String STREAM_WINDOW_ENGINE_MATERIALIZED_ROW_TOTAL =
      "stream_window_engine_materialized_row_total";
  public static final String STREAM_WINDOW_ENGINE_FORCE_CLOSE_BY_MAX_DELAY_TOTAL =
      "stream_window_engine_force_close_by_max_delay_total";
  public static final String STREAM_WINDOW_ENGINE_FORCE_DESTROY_BY_MAX_IDLE_TOTAL =
      "stream_window_engine_force_destroy_by_max_idle_total";
  public static final String STREAM_WINDOW_ENGINE_FORCE_CLOSE_BY_MEMORY_PRESSURE_TOTAL =
      "stream_window_engine_force_close_by_memory_pressure_total";
  public static final String STREAM_WINDOW_ENGINE_FORCE_DESTROY_BY_MEMORY_PRESSURE_TOTAL =
      "stream_window_engine_force_destroy_by_memory_pressure_total";
  public static final String STREAM_WINDOW_ENGINE_DRIVER_BLOCKED_TOTAL =
      "stream_window_engine_driver_blocked_total";

  private static final String ON_HEAP = "OnHeap";
  private static final String LEVEL_3 = "3";

  private Counter pushTotalCounter = DoNothingMetricManager.DO_NOTHING_COUNTER;
  private Counter pushRowTotalCounter = DoNothingMetricManager.DO_NOTHING_COUNTER;
  private Counter droppedBatchTotalCounter = DoNothingMetricManager.DO_NOTHING_COUNTER;
  private Counter memoryPauseTotalCounter = DoNothingMetricManager.DO_NOTHING_COUNTER;
  private Counter incrementalEventTotalCounter = DoNothingMetricManager.DO_NOTHING_COUNTER;
  private Counter closeEventTotalCounter = DoNothingMetricManager.DO_NOTHING_COUNTER;
  private Counter recomputeEventTotalCounter = DoNothingMetricManager.DO_NOTHING_COUNTER;
  private Counter destroyEventTotalCounter = DoNothingMetricManager.DO_NOTHING_COUNTER;
  private Counter materializedBlockTotalCounter = DoNothingMetricManager.DO_NOTHING_COUNTER;
  private Counter materializedRowTotalCounter = DoNothingMetricManager.DO_NOTHING_COUNTER;
  private Counter forceCloseByMaxDelayTotalCounter = DoNothingMetricManager.DO_NOTHING_COUNTER;
  private Counter forceDestroyByMaxIdleTotalCounter = DoNothingMetricManager.DO_NOTHING_COUNTER;
  private Counter forceCloseByMemoryPressureTotalCounter =
      DoNothingMetricManager.DO_NOTHING_COUNTER;
  private Counter forceDestroyByMemoryPressureTotalCounter =
      DoNothingMetricManager.DO_NOTHING_COUNTER;
  private Counter driverBlockedTotalCounter = DoNothingMetricManager.DO_NOTHING_COUNTER;

  private StreamWindowEngineMetrics() {}

  public static StreamWindowEngineMetrics getInstance() {
    return INSTANCE;
  }

  // ===== in-place updates (engines call these; no-ops before bindTo) =====

  /** Counts one accepted push batch and its rows (buffered or processed inline). */
  public void incPush(long rowCount) {
    pushTotalCounter.inc();
    pushRowTotalCounter.inc(rowCount);
  }

  /** Counts one batch dropped by the per-batch exception protection (the only data-loss path). */
  public void incDroppedBatch() {
    droppedBatchTotalCounter.inc();
  }

  /** Counts one transition of an engine into the memory pause (data/output pool exhaustion). */
  public void incMemoryPause() {
    memoryPauseTotalCounter.inc();
  }

  /** Counts one incremental (pre-close) emission batch recorded on a window event. */
  public void incIncrementalEvent() {
    incrementalEventTotalCounter.inc();
  }

  /** Counts one window event finalized with {@code closed=true}. */
  public void incCloseEvent() {
    closeEventTotalCounter.inc();
  }

  /** Counts one destroy-time full-recompute event. */
  public void incRecomputeEvent() {
    recomputeEventTotalCounter.inc();
  }

  /** Counts one destroy-typed event (compute-free commit marker, or a period-window trigger). */
  public void incDestroyEvent() {
    destroyEventTotalCounter.inc();
  }

  /** Counts one lazily materialized event TsBlock and its rows. */
  public void incMaterialized(long rowCount) {
    materializedBlockTotalCounter.inc();
    materializedRowTotalCounter.inc(rowCount);
  }

  /** Counts one MAX_DELAY_WINDOW_CLOSE-driven forced window close. */
  public void incForceCloseByMaxDelay() {
    forceCloseByMaxDelayTotalCounter.inc();
  }

  /** Counts one MAX_IDLE_WINDOW_RETAIN-driven forced cached-window destroy. */
  public void incForceDestroyByMaxIdle() {
    forceDestroyByMaxIdleTotalCounter.inc();
  }

  /** Counts one memory-pressure rescue forced window close (data-pool pause grace exceeded). */
  public void incForceCloseByMemoryPressure() {
    forceCloseByMemoryPressureTotalCounter.inc();
  }

  /** Counts one memory-pressure rescue forced cached-window destroy (no MAX_IDLE config). */
  public void incForceDestroyByMemoryPressure() {
    forceDestroyByMemoryPressureTotalCounter.inc();
  }

  /** Counts one driver parking on {@code getBlockedFuture} (no compute event available). */
  public void incDriverBlocked() {
    driverBlockedTotalCounter.inc();
  }

  // ===== registration =====

  @Override
  public void bindTo(AbstractMetricService metricService) {
    createMemoryPoolMetrics(
        metricService,
        STREAM_ENGINE_WINDOW_INPUT,
        GlobalWindowMemoryController::getInputUsedBytes,
        GlobalWindowMemoryController::getInputTotalBytes);
    createMemoryPoolMetrics(
        metricService,
        STREAM_ENGINE_WINDOW_DATA,
        GlobalWindowMemoryController::getDataUsedBytes,
        GlobalWindowMemoryController::getDataTotalBytes);
    createMemoryPoolMetrics(
        metricService,
        STREAM_ENGINE_WINDOW_OUTPUT,
        GlobalWindowMemoryController::getOutputUsedBytes,
        GlobalWindowMemoryController::getOutputTotalBytes);

    metricService.createAutoGauge(
        STREAM_WINDOW_ENGINE_MEMORY_PAUSED_ENGINES,
        MetricLevel.IMPORTANT,
        CONTROLLER,
        (ToDoubleFunction<GlobalWindowMemoryController>)
            GlobalWindowMemoryController::getPausedEngineCount,
        Tag.NAME.toString(),
        TAG_VALUE);
    metricService.createAutoGauge(
        STREAM_WINDOW_ENGINE_INPUT_WAITERS,
        MetricLevel.IMPORTANT,
        CONTROLLER,
        (ToDoubleFunction<GlobalWindowMemoryController>)
            GlobalWindowMemoryController::getInputWaiterCount,
        Tag.NAME.toString(),
        TAG_VALUE);

    pushTotalCounter =
        metricService.getOrCreateCounter(
            STREAM_WINDOW_ENGINE_PUSH_TOTAL, MetricLevel.IMPORTANT, Tag.NAME.toString(), TAG_VALUE);
    pushRowTotalCounter =
        metricService.getOrCreateCounter(
            STREAM_WINDOW_ENGINE_PUSH_ROW_TOTAL,
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            TAG_VALUE);
    droppedBatchTotalCounter =
        metricService.getOrCreateCounter(
            STREAM_WINDOW_ENGINE_DROPPED_BATCH_TOTAL,
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            TAG_VALUE);
    memoryPauseTotalCounter =
        metricService.getOrCreateCounter(
            STREAM_WINDOW_ENGINE_MEMORY_PAUSE_TOTAL,
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            TAG_VALUE);
    incrementalEventTotalCounter =
        metricService.getOrCreateCounter(
            STREAM_WINDOW_ENGINE_INCREMENTAL_EVENT_TOTAL,
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            TAG_VALUE);
    closeEventTotalCounter =
        metricService.getOrCreateCounter(
            STREAM_WINDOW_ENGINE_CLOSE_EVENT_TOTAL,
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            TAG_VALUE);
    recomputeEventTotalCounter =
        metricService.getOrCreateCounter(
            STREAM_WINDOW_ENGINE_RECOMPUTE_EVENT_TOTAL,
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            TAG_VALUE);
    destroyEventTotalCounter =
        metricService.getOrCreateCounter(
            STREAM_WINDOW_ENGINE_DESTROY_EVENT_TOTAL,
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            TAG_VALUE);
    materializedBlockTotalCounter =
        metricService.getOrCreateCounter(
            STREAM_WINDOW_ENGINE_MATERIALIZED_BLOCK_TOTAL,
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            TAG_VALUE);
    materializedRowTotalCounter =
        metricService.getOrCreateCounter(
            STREAM_WINDOW_ENGINE_MATERIALIZED_ROW_TOTAL,
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            TAG_VALUE);
    forceCloseByMaxDelayTotalCounter =
        metricService.getOrCreateCounter(
            STREAM_WINDOW_ENGINE_FORCE_CLOSE_BY_MAX_DELAY_TOTAL,
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            TAG_VALUE);
    forceDestroyByMaxIdleTotalCounter =
        metricService.getOrCreateCounter(
            STREAM_WINDOW_ENGINE_FORCE_DESTROY_BY_MAX_IDLE_TOTAL,
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            TAG_VALUE);
    forceCloseByMemoryPressureTotalCounter =
        metricService.getOrCreateCounter(
            STREAM_WINDOW_ENGINE_FORCE_CLOSE_BY_MEMORY_PRESSURE_TOTAL,
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            TAG_VALUE);
    forceDestroyByMemoryPressureTotalCounter =
        metricService.getOrCreateCounter(
            STREAM_WINDOW_ENGINE_FORCE_DESTROY_BY_MEMORY_PRESSURE_TOTAL,
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            TAG_VALUE);
    driverBlockedTotalCounter =
        metricService.getOrCreateCounter(
            STREAM_WINDOW_ENGINE_DRIVER_BLOCKED_TOTAL,
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            TAG_VALUE);
  }

  @Override
  public void unbindFrom(AbstractMetricService metricService) {
    removeMemoryPoolMetrics(metricService, STREAM_ENGINE_WINDOW_INPUT);
    removeMemoryPoolMetrics(metricService, STREAM_ENGINE_WINDOW_DATA);
    removeMemoryPoolMetrics(metricService, STREAM_ENGINE_WINDOW_OUTPUT);
    removeGauge(metricService, STREAM_WINDOW_ENGINE_MEMORY_PAUSED_ENGINES);
    removeGauge(metricService, STREAM_WINDOW_ENGINE_INPUT_WAITERS);

    removeCounter(metricService, STREAM_WINDOW_ENGINE_PUSH_TOTAL);
    removeCounter(metricService, STREAM_WINDOW_ENGINE_PUSH_ROW_TOTAL);
    removeCounter(metricService, STREAM_WINDOW_ENGINE_DROPPED_BATCH_TOTAL);
    removeCounter(metricService, STREAM_WINDOW_ENGINE_MEMORY_PAUSE_TOTAL);
    removeCounter(metricService, STREAM_WINDOW_ENGINE_INCREMENTAL_EVENT_TOTAL);
    removeCounter(metricService, STREAM_WINDOW_ENGINE_CLOSE_EVENT_TOTAL);
    removeCounter(metricService, STREAM_WINDOW_ENGINE_RECOMPUTE_EVENT_TOTAL);
    removeCounter(metricService, STREAM_WINDOW_ENGINE_DESTROY_EVENT_TOTAL);
    removeCounter(metricService, STREAM_WINDOW_ENGINE_MATERIALIZED_BLOCK_TOTAL);
    removeCounter(metricService, STREAM_WINDOW_ENGINE_MATERIALIZED_ROW_TOTAL);
    removeCounter(metricService, STREAM_WINDOW_ENGINE_FORCE_CLOSE_BY_MAX_DELAY_TOTAL);
    removeCounter(metricService, STREAM_WINDOW_ENGINE_FORCE_DESTROY_BY_MAX_IDLE_TOTAL);
    removeCounter(metricService, STREAM_WINDOW_ENGINE_FORCE_CLOSE_BY_MEMORY_PRESSURE_TOTAL);
    removeCounter(metricService, STREAM_WINDOW_ENGINE_FORCE_DESTROY_BY_MEMORY_PRESSURE_TOTAL);
    removeCounter(metricService, STREAM_WINDOW_ENGINE_DRIVER_BLOCKED_TOTAL);

    pushTotalCounter = DoNothingMetricManager.DO_NOTHING_COUNTER;
    pushRowTotalCounter = DoNothingMetricManager.DO_NOTHING_COUNTER;
    droppedBatchTotalCounter = DoNothingMetricManager.DO_NOTHING_COUNTER;
    memoryPauseTotalCounter = DoNothingMetricManager.DO_NOTHING_COUNTER;
    incrementalEventTotalCounter = DoNothingMetricManager.DO_NOTHING_COUNTER;
    closeEventTotalCounter = DoNothingMetricManager.DO_NOTHING_COUNTER;
    recomputeEventTotalCounter = DoNothingMetricManager.DO_NOTHING_COUNTER;
    destroyEventTotalCounter = DoNothingMetricManager.DO_NOTHING_COUNTER;
    materializedBlockTotalCounter = DoNothingMetricManager.DO_NOTHING_COUNTER;
    materializedRowTotalCounter = DoNothingMetricManager.DO_NOTHING_COUNTER;
    forceCloseByMaxDelayTotalCounter = DoNothingMetricManager.DO_NOTHING_COUNTER;
    forceDestroyByMaxIdleTotalCounter = DoNothingMetricManager.DO_NOTHING_COUNTER;
    forceCloseByMemoryPressureTotalCounter = DoNothingMetricManager.DO_NOTHING_COUNTER;
    forceDestroyByMemoryPressureTotalCounter = DoNothingMetricManager.DO_NOTHING_COUNTER;
    driverBlockedTotalCounter = DoNothingMetricManager.DO_NOTHING_COUNTER;
  }

  private static void createMemoryPoolMetrics(
      AbstractMetricService metricService,
      String name,
      ToDoubleFunction<GlobalWindowMemoryController> usedBytes,
      ToDoubleFunction<GlobalWindowMemoryController> totalBytes) {
    metricService.createAutoGauge(
        Metric.MEMORY_ACTUAL_SIZE.toString(),
        MetricLevel.IMPORTANT,
        CONTROLLER,
        usedBytes,
        Tag.NAME.toString(),
        name,
        Tag.TYPE.toString(),
        ON_HEAP,
        Tag.LEVEL.toString(),
        LEVEL_3);
    metricService.createAutoGauge(
        Metric.MEMORY_THRESHOLD_SIZE.toString(),
        MetricLevel.IMPORTANT,
        CONTROLLER,
        totalBytes,
        Tag.NAME.toString(),
        name,
        Tag.TYPE.toString(),
        ON_HEAP,
        Tag.LEVEL.toString(),
        LEVEL_3);
  }

  private static void removeMemoryPoolMetrics(AbstractMetricService metricService, String name) {
    removeGauge(
        metricService,
        Metric.MEMORY_ACTUAL_SIZE.toString(),
        Tag.NAME.toString(),
        name,
        Tag.TYPE.toString(),
        ON_HEAP,
        Tag.LEVEL.toString(),
        LEVEL_3);
    removeGauge(
        metricService,
        Metric.MEMORY_THRESHOLD_SIZE.toString(),
        Tag.NAME.toString(),
        name,
        Tag.TYPE.toString(),
        ON_HEAP,
        Tag.LEVEL.toString(),
        LEVEL_3);
  }

  private static void removeGauge(
      AbstractMetricService metricService, String name, String... tags) {
    metricService.remove(MetricType.AUTO_GAUGE, name, tags);
  }

  private static void removeCounter(AbstractMetricService metricService, String name) {
    metricService.remove(MetricType.COUNTER, name, Tag.NAME.toString(), TAG_VALUE);
  }
}
