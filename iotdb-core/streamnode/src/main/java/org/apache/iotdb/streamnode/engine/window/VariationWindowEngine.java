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

package org.apache.iotdb.streamnode.engine.window;

import org.apache.iotdb.calc.execution.operator.Operator;
import org.apache.iotdb.commons.stream.StreamProperties;
import org.apache.iotdb.commons.stream.StreamWindow;
import org.apache.iotdb.commons.stream.VariationWindow;
import org.apache.iotdb.streamnode.engine.scheduler.task.StreamDriverContext;
import org.apache.iotdb.streamnode.engine.task.StreamSubTask.DataSlice;
import org.apache.iotdb.streamnode.engine.window.PendingSuffixBuffer.BufferedRow;
import org.apache.iotdb.streamnode.engine.window.variation.AdwinBoundaryDetector;
import org.apache.iotdb.streamnode.engine.window.variation.BoundaryDetector;
import org.apache.iotdb.streamnode.engine.window.variation.DeltaBoundaryDetector;
import org.apache.iotdb.streamnode.engine.window.variation.DetectionDecision;
import org.apache.iotdb.streamnode.engine.window.variation.DetectionStepResult;
import org.apache.iotdb.streamnode.engine.window.variation.DurationRuleDetector;
import org.apache.iotdb.streamnode.engine.window.variation.EvidenceHysteresisDetector;
import org.apache.iotdb.streamnode.engine.window.variation.OptwinBoundaryDetector;
import org.apache.iotdb.streamnode.i18n.StreamNodeMessages;
import org.apache.iotdb.streamnode.metrics.StreamWindowEngineMetrics;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.tsfile.common.constant.TsFileConstant;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.DateUtils;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.OptionalLong;
import java.util.Set;

/**
 * Regime-aware {@code VARIATION} window engine combining pluggable boundary detection (defaulting
 * to Section 4.4.1 time-weighted evidence with Median/MAD adaptive noise floor) and a bounded
 * {@link PendingSuffixBuffer} ({@code beta}) with exact onset backtracking and CommitTracker
 * handoff safety.
 */
public class VariationWindowEngine extends WindowEngine {

  private static final Logger LOGGER = LoggerFactory.getLogger(VariationWindowEngine.class);

  private static final double DEFAULT_THETA = 3.0;
  private static final long DEFAULT_TAU_MS = 5_000L;
  private static final long DEFAULT_DT_MAX_MS = 2_000L;
  private static final int DEFAULT_MIN_SAMPLES = 3;
  private static final int DEFAULT_BUFFER_MAX_ROWS = 4096;
  private static final long DEFAULT_BUFFER_MAX_MS = 60_000L;
  private static final String POLICY_FORCE_CLOSE = "force_close";
  private static final String POLICY_MERGE_BACK = "merge_back";

  private final String monitoredColumn;
  private final BoundaryDetector detector;
  private final PendingSuffixBuffer pendingBuffer;
  private final String overflowPolicy;
  private final long maxDelayWindowCloseMs;
  private final WindowRowConverter rowConverter;

  // Monitored column cache keyed by last seen Tablet instance
  private Tablet cachedMonitoredTablet;
  private int cachedMonitoredColIdx = -2;
  private TSDataType cachedMonitoredType;

  // Active confirmed window state
  private WindowRowStore activeStore;
  private long activeWindowId = -1;
  private long activeWindowStart = Long.MIN_VALUE;
  private long activeWindowEnd = Long.MIN_VALUE;
  private int activeRowCount;
  private @Nullable Object activeAnchorValue;
  private @Nullable Object activeLastValue;
  private long closeSystemTime = Long.MAX_VALUE;
  private long lastClosedStartTime = Long.MIN_VALUE;
  private boolean registeredActiveWindowOfSlice;

  // Observability counters
  private long processedRows;
  private long confirmedWindowCount;

  public VariationWindowEngine(
      StreamWindow window,
      StreamDriverContext driverContext,
      StreamProperties streamProperties,
      WindowEngineTickScheduler tickScheduler) {
    super(driverContext, streamProperties, window.getEventInputColumns(), tickScheduler);
    VariationWindow variationWindow = (VariationWindow) window;
    this.monitoredColumn = variationWindow.getColumn();
    double delta = Math.max(0.0, variationWindow.getDelta());

    String detectorName =
        streamProperties != null
            ? streamProperties
                .getStringExtraProperty(StreamProperties.VARIATION_DETECTOR, "evidence")
                .toLowerCase(Locale.ENGLISH)
            : "evidence";
    double defaultTheta = delta > 0 ? delta : DEFAULT_THETA;
    double theta =
        streamProperties != null
            ? streamProperties.getDoubleExtraProperty(
                StreamProperties.VARIATION_THETA, defaultTheta)
            : defaultTheta;
    double epsilon =
        streamProperties != null
            ? streamProperties.getDoubleExtraProperty(StreamProperties.VARIATION_EPSILON, delta)
            : delta;
    long tauMs =
        streamProperties != null
            ? streamProperties.getLongExtraProperty(
                StreamProperties.VARIATION_TAU_MS, DEFAULT_TAU_MS)
            : DEFAULT_TAU_MS;
    long dtMaxMs =
        streamProperties != null
            ? streamProperties.getLongExtraProperty(
                StreamProperties.VARIATION_DT_MAX_MS, DEFAULT_DT_MAX_MS)
            : DEFAULT_DT_MAX_MS;
    int minSamples =
        streamProperties != null
            ? streamProperties.getIntExtraProperty(
                StreamProperties.VARIATION_MIN_SAMPLES, DEFAULT_MIN_SAMPLES)
            : DEFAULT_MIN_SAMPLES;
    boolean adaptiveBaseline =
        streamProperties == null
            || streamProperties.getBooleanExtraProperty(
                StreamProperties.VARIATION_ADAPTIVE_BASELINE, true);

    this.detector =
        createDetector(
            detectorName, delta, theta, epsilon, tauMs, dtMaxMs, minSamples, adaptiveBaseline);

    int bufferMaxRows =
        streamProperties != null
            ? streamProperties.getIntExtraProperty(
                StreamProperties.VARIATION_BUFFER_MAX_ROWS, DEFAULT_BUFFER_MAX_ROWS)
            : DEFAULT_BUFFER_MAX_ROWS;
    long bufferMaxMs =
        streamProperties != null
            ? streamProperties.getLongExtraProperty(
                StreamProperties.VARIATION_BUFFER_MAX_MS, DEFAULT_BUFFER_MAX_MS)
            : DEFAULT_BUFFER_MAX_MS;
    this.overflowPolicy =
        streamProperties != null
            ? streamProperties
                .getStringExtraProperty(
                    StreamProperties.VARIATION_OVERFLOW_POLICY, POLICY_FORCE_CLOSE)
                .toLowerCase(Locale.ENGLISH)
            : POLICY_FORCE_CLOSE;
    this.pendingBuffer = new PendingSuffixBuffer(bufferMaxRows, bufferMaxMs);
    this.maxDelayWindowCloseMs =
        streamProperties != null ? streamProperties.getMaxDelayWindowCloseMs() : 0L;

    this.rowConverter =
        new WindowRowConverter(
            window.containsRowsPlaceholder() && eventScanInputColumns != null
                ? eventScanInputColumns
                : Collections.singletonList(TsFileConstant.TIME_COLUMN_ID),
            window.getEventInputColumnTypes(),
            MAX_WINDOW_BLOCK_ROWS);
    start();
  }

  private static BoundaryDetector createDetector(
      String detectorName,
      double delta,
      double theta,
      double epsilon,
      long tauMs,
      long dtMaxMs,
      int minSamples,
      boolean adaptiveBaseline) {
    switch (detectorName) {
      case "evidence":
        return new EvidenceHysteresisDetector(
            theta, epsilon, tauMs, dtMaxMs, minSamples, adaptiveBaseline);
      case "delta":
        return new DeltaBoundaryDetector(delta);
      case "duration":
        return new DurationRuleDetector(delta, minSamples);
      case "adwin":
        return new AdwinBoundaryDetector(epsilon, minSamples);
      case "optwin":
        return new OptwinBoundaryDetector(epsilon, minSamples);
      default:
        throw new IllegalArgumentException(
            String.format(
                StreamNodeMessages.EXCEPTION_UNSUPPORTED_VARIATION_DETECTOR_ARG_F6EB39D8,
                detectorName));
    }
  }

  @Override
  public ListenableFuture<?> push(List<DataSlice> dataSlices) {
    ListenableFuture<?> blocked = pushAndProcess(dataSlices);
    return blocked != null ? blocked : Operator.NOT_BLOCKED;
  }

  @Override
  protected void processDataSlices(List<DataSlice> dataSlices) {
    long now = nowMillis();
    for (DataSlice dataSlice : dataSlices) {
      registeredActiveWindowOfSlice = false;
      for (int row = dataSlice.getStartRow(); row < dataSlice.getEndRow(); row++) {
        processRow(dataSlice, row, now);
      }
      markTabletGroupDone(dataSlice.getTabletId());
    }
    LOGGER.debug(
        StreamNodeMessages
            .LOG_VARIATIONWINDOWENGINE_PROCESSED_ARG_ROWS_CONFIRMED_WINDOWS_ARG_PENDING_ROWS_ARG_93702A5F,
        processedRows,
        confirmedWindowCount,
        pendingBuffer.size());
    if (activeStore != null && !activeStore.reconcileDataMemory()) {
      pauseOnMemoryPressure(false);
    }
  }

  private void processRow(DataSlice dataSlice, int row, long now) {
    rowConverter.ensureColumnMapping(dataSlice);
    Tablet tablet = dataSlice.getTablet();
    long timestamp = tablet.getTimestamps()[row];
    long tabletId = dataSlice.getTabletId();
    processedRows++;

    if (activeStore == null) {
      openNewActiveStore(allocateWindowId(), tabletId, now);
    } else if (!registeredActiveWindowOfSlice) {
      onWindowOpen(activeWindowId, tabletId);
      registeredActiveWindowOfSlice = true;
    }

    Object boxedValue = extractMonitoredValue(tablet, row);
    double numericValue = toFiniteDouble(boxedValue);
    if (!Double.isFinite(numericValue)) {
      if (pendingBuffer.isEmpty()) {
        upsertIntoActive(dataSlice, row, timestamp, null);
      } else {
        pendingBuffer.append(dataSlice, row, timestamp, null, now);
        checkPendingOverflow(timestamp, now);
      }
      return;
    }

    DetectionStepResult step = detector.onSample(timestamp, numericValue);
    DetectionDecision decision = step.getDecision();
    switch (decision) {
      case CONTINUE:
        if (!pendingBuffer.isEmpty()) {
          mergePendingIntoActive();
        }
        upsertIntoActive(dataSlice, row, timestamp, boxedValue);
        break;
      case CANDIDATE:
        pendingBuffer.append(dataSlice, row, timestamp, boxedValue, now);
        checkPendingOverflow(timestamp, now);
        break;
      case MERGE_BACK:
        mergePendingIntoActive();
        upsertIntoActive(dataSlice, row, timestamp, boxedValue);
        break;
      case CONFIRM_PREFIX:
        pendingBuffer.append(dataSlice, row, timestamp, boxedValue, now);
        commitPrefixAndStartNewWindow(step.getSuffixRowCount(), now);
        break;
      default:
        break;
    }
  }

  private void openNewActiveStore(long windowId, long openingTabletId, long now) {
    activeStore = new WindowRowStore(rowConverter.outputTypes());
    activeWindowId = windowId;
    activeWindowStart = Long.MIN_VALUE;
    activeWindowEnd = Long.MIN_VALUE;
    activeRowCount = 0;
    activeAnchorValue = null;
    activeLastValue = null;
    closeSystemTime = maxDelayWindowCloseMs > 0 ? now + maxDelayWindowCloseMs : Long.MAX_VALUE;
    if (openingTabletId >= 0) {
      onWindowOpen(activeWindowId, openingTabletId);
      registeredActiveWindowOfSlice = true;
    } else {
      registeredActiveWindowOfSlice = false;
    }
  }

  private void upsertIntoActive(
      DataSlice slice, int row, long timestamp, @Nullable Object monitoredValue) {
    rowConverter.upsertInto(activeStore, slice, row);
    activeRowCount = activeStore.size();
    if (activeWindowStart == Long.MIN_VALUE || timestamp < activeWindowStart) {
      activeWindowStart = timestamp;
    }
    if (activeWindowEnd == Long.MIN_VALUE || timestamp > activeWindowEnd) {
      activeWindowEnd = timestamp;
    }
    if (monitoredValue != null) {
      if (activeAnchorValue == null) {
        activeAnchorValue = monitoredValue;
      }
      activeLastValue = monitoredValue;
    }
  }

  private void mergePendingIntoActive() {
    List<BufferedRow> drained = pendingBuffer.drainAll();
    for (BufferedRow bufferedRow : drained) {
      upsertIntoActive(bufferedRow.slice, 0, bufferedRow.timestamp, bufferedRow.monitoredValue);
    }
  }

  private void commitPrefixAndStartNewWindow(int requestedSuffixCount, long now) {
    List<BufferedRow> allPending = pendingBuffer.drainAll();
    if (allPending.isEmpty()) {
      return;
    }
    int total = allPending.size();
    int suffixCount = Math.min(total, Math.max(1, requestedSuffixCount));
    int splitIndex = total - suffixCount;

    for (int i = 0; i < splitIndex; i++) {
      BufferedRow prefixRow = allPending.get(i);
      upsertIntoActive(prefixRow.slice, 0, prefixRow.timestamp, prefixRow.monitoredValue);
    }

    List<BufferedRow> suffixRows = allPending.subList(splitIndex, total);
    BufferedRow triggerRow = suffixRows.get(0);

    // Prepare next window and register every suffix row's tablet BEFORE closing/destroying the
    // active window so CommitTracker never advances past a tablet with uncommitted suffix rows.
    long nextWindowId = allocateWindowId();
    Set<Long> registeredSuffixTablets = new HashSet<>();
    for (BufferedRow suffixRow : suffixRows) {
      long tid = suffixRow.slice.getTabletId();
      if (registeredSuffixTablets.add(tid)) {
        onWindowOpen(nextWindowId, tid);
      }
    }

    if (activeStore != null && !activeStore.isEmpty()) {
      long closeStart = activeWindowStart;
      long closeEnd = activeWindowEnd;
      LOGGER.debug(
          StreamNodeMessages
              .LOG_VARIATIONWINDOWENGINE_CANDIDATE_CONFIRMED_AT_ARG_BACKTRACKED_ARG_ROWS_CLOSE_WINDOW_ARG_ARG_30906950,
          triggerRow.timestamp,
          suffixRows.size(),
          closeStart,
          closeEnd);
      closeAndDestroyActiveWindow(
          closeEnd, OptionalLong.of(triggerRow.timestamp), triggerRow.monitoredValue);
    }

    // Open new active window starting at triggerRow and populate suffix rows
    openNewActiveStore(nextWindowId, -1L, now);
    long currentSliceTablet = -1L;
    for (BufferedRow suffixRow : suffixRows) {
      upsertIntoActive(suffixRow.slice, 0, suffixRow.timestamp, suffixRow.monitoredValue);
      currentSliceTablet = suffixRow.slice.getTabletId();
    }
    // The current incoming slice's last row was appended at the tail of suffixRows; its tablet is
    // already registered on nextWindowId above.
    registeredActiveWindowOfSlice = registeredSuffixTablets.contains(currentSliceTablet);
  }

  private void checkPendingOverflow(long currentEventTime, long now) {
    if (!pendingBuffer.isBudgetExceeded(currentEventTime, now)) {
      return;
    }
    LOGGER.info(
        StreamNodeMessages
            .LOG_VARIATIONWINDOWENGINE_PENDING_BUFFER_BUDGET_EXCEEDED_ROWS_ARG_SPAN_ARGMS_APPLYING_POLICY_ARG_BE56EB8C,
        pendingBuffer.size(),
        pendingBuffer.currentSpanMs(currentEventTime, now),
        overflowPolicy);

    if (POLICY_MERGE_BACK.equals(overflowPolicy)) {
      mergePendingIntoActive();
      detector.reset();
      if (activeLastValue != null && activeWindowEnd != Long.MIN_VALUE) {
        double v = toFiniteDouble(activeLastValue);
        if (Double.isFinite(v)) {
          detector.onSample(activeWindowEnd, v);
        }
      }
      return;
    }

    // force_close policy: close active window up to pre-candidate point and re-seed next window
    List<BufferedRow> buffered = pendingBuffer.drainAll();
    if (buffered.isEmpty()) {
      return;
    }
    BufferedRow head = buffered.get(0);
    long nextWindowId = allocateWindowId();
    Set<Long> registered = new HashSet<>();
    for (BufferedRow br : buffered) {
      long tid = br.slice.getTabletId();
      if (registered.add(tid)) {
        onWindowOpen(nextWindowId, tid);
      }
    }
    if (activeStore != null && !activeStore.isEmpty()) {
      closeAndDestroyActiveWindow(
          activeWindowEnd, OptionalLong.of(head.timestamp), head.monitoredValue);
    }
    openNewActiveStore(nextWindowId, -1L, now);
    detector.reset();
    for (BufferedRow br : buffered) {
      upsertIntoActive(br.slice, 0, br.timestamp, br.monitoredValue);
    }
    double headNum = toFiniteDouble(head.monitoredValue);
    if (Double.isFinite(headNum)) {
      detector.onSample(head.timestamp, headNum);
    }
    registeredActiveWindowOfSlice =
        !buffered.isEmpty()
            && registered.contains(buffered.get(buffered.size() - 1).slice.getTabletId());
  }

  private void closeAndDestroyActiveWindow(
      long endTime, OptionalLong nextTime, @Nullable Object triggerValue) {
    if (activeStore == null || activeStore.isEmpty()) {
      return;
    }
    if (!activeStore.reconcileDataMemory()) {
      pauseOnMemoryPressure(false);
    }
    activeStore.freeze();
    StreamWindowEngineMetrics.getInstance().incCloseEvent();

    WindowEventRowsIterator iterator =
        new WindowEventRowsIterator(rowConverter, activeStore, lock, this);
    iterator.emitRange(Long.MIN_VALUE, Long.MAX_VALUE);
    iterator.markFinished();

    Object prevVal = activeAnchorValue != null ? activeAnchorValue : activeLastValue;
    Object curVal = triggerValue != null ? triggerValue : activeLastValue;

    WindowEventInfo.Builder closeBuilder =
        new WindowEventInfo.Builder()
            .startTime(activeWindowStart)
            .endTime(endTime)
            .prevValue(prevVal)
            .currentValue(curVal)
            .partitionKey(driverContext.getSubTaskContext().getPartitionKey())
            .rowCount(activeRowCount)
            .rowsIterator(iterator)
            .windowId(activeWindowId)
            .eventType(WindowEventType.WINDOW_CLOSE)
            .needsCompute(true);
    if (lastClosedStartTime != Long.MIN_VALUE) {
      closeBuilder.prevTime(lastClosedStartTime);
    }
    if (nextTime.isPresent()) {
      closeBuilder.nextTime(nextTime.getAsLong());
    }
    tryOfferComputeEvent(closeBuilder.build());

    StreamWindowEngineMetrics.getInstance().incDestroyEvent();
    WindowEventInfo.Builder destroyBuilder =
        new WindowEventInfo.Builder()
            .startTime(activeWindowStart)
            .endTime(endTime)
            .prevValue(prevVal)
            .currentValue(curVal)
            .partitionKey(driverContext.getSubTaskContext().getPartitionKey())
            .rowCount(activeRowCount)
            .rowsIterator(null)
            .windowId(activeWindowId)
            .eventType(WindowEventType.WINDOW_DESTROY)
            .needsCompute(false);
    if (lastClosedStartTime != Long.MIN_VALUE) {
      destroyBuilder.prevTime(lastClosedStartTime);
    }
    if (nextTime.isPresent()) {
      destroyBuilder.nextTime(nextTime.getAsLong());
    }
    tryOfferRecomputeEvent(destroyBuilder.build());

    confirmedWindowCount++;
    lastClosedStartTime = activeWindowStart;
    activeStore.releaseOwner();
    activeStore = null;
    activeWindowId = -1;
    activeWindowStart = Long.MIN_VALUE;
    activeWindowEnd = Long.MIN_VALUE;
    activeRowCount = 0;
    activeAnchorValue = null;
    activeLastValue = null;
    closeSystemTime = Long.MAX_VALUE;
  }

  private @Nullable Object extractMonitoredValue(Tablet tablet, int row) {
    if (tablet != cachedMonitoredTablet) {
      cachedMonitoredTablet = tablet;
      cachedMonitoredColIdx = -2;
      cachedMonitoredType = null;
      List<IMeasurementSchema> schemas = tablet.getSchemas();
      for (int i = 0; i < schemas.size(); i++) {
        if (schemas.get(i).getMeasurementName().equalsIgnoreCase(monitoredColumn)) {
          cachedMonitoredColIdx = i;
          cachedMonitoredType = schemas.get(i).getType();
          break;
        }
      }
      if (cachedMonitoredColIdx == -2
          && TsFileConstant.TIME_COLUMN_ID.equalsIgnoreCase(monitoredColumn)) {
        cachedMonitoredColIdx = -1;
        cachedMonitoredType = TSDataType.TIMESTAMP;
      }
    }
    if (cachedMonitoredColIdx == -2 || cachedMonitoredType == null) {
      return null;
    }
    if (cachedMonitoredColIdx == -1) {
      return tablet.getTimestamps()[row];
    }
    if (tablet.isNull(row, cachedMonitoredColIdx)) {
      return null;
    }
    Object colArr = tablet.getValues()[cachedMonitoredColIdx];
    if (colArr == null) {
      return null;
    }
    switch (cachedMonitoredType) {
      case BOOLEAN:
        return ((boolean[]) colArr)[row];
      case INT32:
        return ((int[]) colArr)[row];
      case DATE:
        LocalDate ld = ((LocalDate[]) colArr)[row];
        return ld != null ? DateUtils.parseDateExpressionToInt(ld) : null;
      case INT64:
      case TIMESTAMP:
        return ((long[]) colArr)[row];
      case FLOAT:
        return ((float[]) colArr)[row];
      case DOUBLE:
        return ((double[]) colArr)[row];
      case TEXT:
      case STRING:
      case BLOB:
        Object val = ((Object[]) colArr)[row];
        if (val instanceof Binary) {
          return val;
        }
        if (val instanceof byte[]) {
          return new Binary((byte[]) val);
        }
        if (val instanceof String) {
          return new Binary((String) val, StandardCharsets.UTF_8);
        }
        return null;
      default:
        return null;
    }
  }

  private static double toFiniteDouble(@Nullable Object boxed) {
    if (boxed instanceof Number) {
      double v = ((Number) boxed).doubleValue();
      return Double.isFinite(v) ? v : Double.NaN;
    }
    if (boxed instanceof Boolean) {
      return ((Boolean) boxed) ? 1.0 : 0.0;
    }
    return Double.NaN;
  }

  @Override
  protected void onTick() {
    long now = nowMillis();
    if (!pendingBuffer.isEmpty() && pendingBuffer.isBudgetExceeded(Long.MIN_VALUE, now)) {
      checkPendingOverflow(Long.MIN_VALUE, now);
    }
    if (maxDelayWindowCloseMs > 0
        && activeStore != null
        && !activeStore.isEmpty()
        && now >= closeSystemTime) {
      StreamWindowEngineMetrics.getInstance().incForceCloseByMaxDelay();
      mergePendingIntoActive();
      closeAndDestroyActiveWindow(activeWindowEnd, OptionalLong.empty(), activeLastValue);
      detector.reset();
    }
  }

  @Override
  protected boolean onMemoryPressureRescue() {
    if ((activeStore == null || activeStore.isEmpty()) && pendingBuffer.isEmpty()) {
      return false;
    }
    StreamWindowEngineMetrics.getInstance().incForceCloseByMemoryPressure();
    long closeStart = activeWindowStart;
    LOGGER.warn(
        StreamNodeMessages.LOG_WINDOW_FORCE_CLOSE_BY_MEMORY_PRESSURE_961D31B5,
        getClass().getSimpleName(),
        closeStart,
        DATA_POOL_PAUSE_RESCUE_GRACE_MS);
    if (activeStore == null && !pendingBuffer.isEmpty()) {
      BufferedRow head = pendingBuffer.sliceView().get(0);
      openNewActiveStore(allocateWindowId(), head.slice.getTabletId(), nowMillis());
    }
    mergePendingIntoActive();
    closeAndDestroyActiveWindow(activeWindowEnd, OptionalLong.empty(), activeLastValue);
    detector.reset();
    return true;
  }

  @Override
  public void start() {
    startProcessing();
  }

  @Override
  public void stop() {
    pauseProcessing();
  }

  @Override
  public void drop() {
    markDropped();
    stopProcessing();
    synchronized (lock) {
      pendingBuffer.clear();
      detector.reset();
      if (activeStore != null) {
        activeStore.releaseOwner();
        activeStore = null;
      }
      activeWindowId = -1;
      activeWindowStart = Long.MIN_VALUE;
      activeWindowEnd = Long.MIN_VALUE;
      activeRowCount = 0;
      activeAnchorValue = null;
      activeLastValue = null;
      closeSystemTime = Long.MAX_VALUE;
    }
    clearAllPending();
    LOGGER.info(StreamNodeMessages.LOG_VARIATIONWINDOWENGINE_DROPPED_FEA0FBAC);
  }

  @Override
  public ListenableFuture<?> forceClose() {
    synchronized (lock) {
      stopProcessing();
      drainAllBufferedBatches();
      if (!pendingBuffer.isEmpty() && activeStore == null) {
        BufferedRow head = pendingBuffer.sliceView().get(0);
        openNewActiveStore(allocateWindowId(), head.slice.getTabletId(), nowMillis());
      }
      mergePendingIntoActive();
      if (activeStore != null && !activeStore.isEmpty()) {
        closeAndDestroyActiveWindow(activeWindowEnd, OptionalLong.empty(), activeLastValue);
      }
      detector.reset();
    }
    LOGGER.info(StreamNodeMessages.LOG_VARIATIONWINDOWENGINE_FORCE_CLOSED_B49E9362);
    return Operator.NOT_BLOCKED;
  }
}
