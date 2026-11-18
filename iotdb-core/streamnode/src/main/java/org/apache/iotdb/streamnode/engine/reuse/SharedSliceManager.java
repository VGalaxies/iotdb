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

package org.apache.iotdb.streamnode.engine.reuse;

import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.CountAccumulator;
import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.CountAllAccumulator;
import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.CountIfAccumulator;
import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.ExtremeAccumulator;
import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.MaxAccumulator;
import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.MinAccumulator;
import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.SumAccumulator;
import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.TableAccumulator;
import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.TableAggregator;
import org.apache.iotdb.commons.stream.PartitionKey;
import org.apache.iotdb.streamnode.engine.reuse.ReuseCostModel.CostDecision;
import org.apache.iotdb.streamnode.engine.reuse.SliceCompatibilitySignature.SliceTaskDescriptor;
import org.apache.iotdb.streamnode.i18n.StreamNodeMessages;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.column.BinaryColumnBuilder;
import org.apache.tsfile.read.common.block.column.BooleanColumnBuilder;
import org.apache.tsfile.read.common.block.column.DoubleColumnBuilder;
import org.apache.tsfile.read.common.block.column.FloatColumnBuilder;
import org.apache.tsfile.read.common.block.column.IntColumnBuilder;
import org.apache.tsfile.read.common.block.column.LongColumnBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Lifecycle-constrained multi-granularity base-slice manager (§4.5).
 *
 * <p>Coordinates producer registration, 5-point structural compatibility & 4-term cost-based edge
 * selection in {@link SliceDependencyGraph}, idempotent {@code stateVersion} deduplication &
 * late-recompute cascades (Algorithm 4.3), reference-counted TTL eviction, and guaranteed lossless
 * raw-scan fallback whenever a producer stops or any sub-slice is missing.
 */
public class SharedSliceManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(SharedSliceManager.class);

  public static final long DEFAULT_MAX_CACHE_BYTES = 256L * 1024L * 1024L;
  private static final long DEFAULT_ESTIMATED_ROWS_PER_SLICE = 100L;

  private static final SharedSliceManager INSTANCE = new SharedSliceManager();

  private final long maxCacheBytes;
  private final SliceDependencyGraph dependencyGraph = new SliceDependencyGraph();
  private final Map<String, SliceTaskDescriptor> registeredStreams = new LinkedHashMap<>();
  private final Map<String, ConsumerBinding> consumerBindings = new HashMap<>();
  private final Map<String, Double> observedRowsEma = new HashMap<>();

  // (producerStream, partitionKeyId, windowStart) -> latest slice
  private final Map<SliceKey, SharedBaseSlice> sliceCache = new LinkedHashMap<>();
  // (producerStream, partitionKeyId) -> max closed windowEnd published so far
  private final Map<ProducerPartitionKey, Long> maxClosedEndByPartition = new HashMap<>();
  // (producerStream, partitionKeyId) -> max windowEnd evicted so far (distinguishes evicted from
  // empty)
  private final Map<ProducerPartitionKey, Long> evictedHorizonByPartition = new HashMap<>();
  // (consumerStream, partitionKeyId, windowStart, windowEnd) -> (subWindowStart -> consumedVersion)
  private final Map<ConsumerWindowKey, Map<Long, Long>> consumedVersions = new HashMap<>();

  private long cachedBytes = 0L;

  public SharedSliceManager() {
    this(DEFAULT_MAX_CACHE_BYTES);
  }

  public SharedSliceManager(long maxCacheBytes) {
    this.maxCacheBytes = Math.max(1024L, maxCacheBytes);
  }

  public static SharedSliceManager getInstance() {
    return INSTANCE;
  }

  public static String partitionKeyId(@Nullable PartitionKey key) {
    return key == null ? "" : key.toString();
  }

  public synchronized void registerStream(SliceTaskDescriptor descriptor) {
    Objects.requireNonNull(descriptor);
    registeredStreams.put(descriptor.getTaskName(), descriptor);
    rebuildBindings();
  }

  public synchronized void unregisterStream(String streamName) {
    if (streamName == null) {
      return;
    }
    registeredStreams.remove(streamName);
    dependencyGraph.removeStream(streamName);

    // Drop consumed state and clear refCount pins held by this stream as a consumer
    ConsumerBinding removedConsumerBinding = consumerBindings.remove(streamName);
    if (removedConsumerBinding != null) {
      for (Map.Entry<SliceKey, SharedBaseSlice> entry : sliceCache.entrySet()) {
        if (entry.getKey().producerStream.equals(removedConsumerBinding.getProducerStream())) {
          entry.getValue().decrementRefCount();
        }
      }
    }
    consumedVersions.entrySet().removeIf(e -> e.getKey().consumerStream.equals(streamName));

    // Remove all cached slices & metadata owned by this producer stream
    Iterator<Map.Entry<SliceKey, SharedBaseSlice>> it = sliceCache.entrySet().iterator();
    while (it.hasNext()) {
      Map.Entry<SliceKey, SharedBaseSlice> entry = it.next();
      if (entry.getKey().producerStream.equals(streamName)) {
        cachedBytes = Math.max(0L, cachedBytes - entry.getValue().getEstimatedBytes());
        it.remove();
      }
    }
    maxClosedEndByPartition.entrySet().removeIf(e -> e.getKey().producerStream.equals(streamName));
    evictedHorizonByPartition
        .entrySet()
        .removeIf(e -> e.getKey().producerStream.equals(streamName));
    observedRowsEma.remove(streamName);

    rebuildBindings();
  }

  public synchronized boolean isRegisteredProducer(String streamName) {
    return streamName != null && dependencyGraph.isActiveProducer(streamName);
  }

  public synchronized @Nullable ConsumerBinding getConsumerBinding(String consumerStream) {
    return consumerBindings.get(consumerStream);
  }

  public synchronized Set<String> getBoundConsumers(String producerStream) {
    return dependencyGraph.getConsumers(producerStream);
  }

  public synchronized long getCachedBytes() {
    return cachedBytes;
  }

  public synchronized int getCachedSliceCount() {
    return sliceCache.size();
  }

  public synchronized long publishSlice(
      String producerStream,
      @Nullable PartitionKey partitionKey,
      long windowStart,
      long windowEnd,
      long rowCount,
      boolean isRecompute,
      Column[] intermediateColumns) {
    return publishSlice(
        producerStream,
        partitionKey,
        windowStart,
        windowEnd,
        rowCount,
        isRecompute,
        intermediateColumns,
        System.currentTimeMillis());
  }

  public synchronized long publishSlice(
      String producerStream,
      @Nullable PartitionKey partitionKey,
      long windowStart,
      long windowEnd,
      long rowCount,
      boolean isRecompute,
      Column[] intermediateColumns,
      long nowWallMs) {
    SliceTaskDescriptor desc = registeredStreams.get(producerStream);
    if (desc == null || intermediateColumns == null) {
      return -1L;
    }

    String partId = partitionKeyId(partitionKey);
    SliceKey key = new SliceKey(producerStream, partId, windowStart);
    SharedBaseSlice existing = sliceCache.get(key);
    long newVersion = existing == null ? 1L : existing.getStateVersion() + 1L;

    long estBytes = estimateColumnsBytes(intermediateColumns);
    long retentionMs = Math.max(0L, desc.getSliceRetentionMs());
    SharedBaseSlice slice =
        new SharedBaseSlice(
            producerStream,
            partId,
            windowStart,
            windowEnd,
            newVersion,
            rowCount,
            intermediateColumns,
            nowWallMs,
            nowWallMs + retentionMs,
            estBytes);

    int downstreamCount = dependencyGraph.getConsumers(producerStream).size();
    for (int i = 0; i < downstreamCount; i++) {
      slice.incrementRefCount();
    }

    if (existing != null) {
      cachedBytes = Math.max(0L, cachedBytes - existing.getEstimatedBytes());
    }
    sliceCache.put(key, slice);
    cachedBytes += estBytes;

    // Update partition closed progress & row EMA
    ProducerPartitionKey ppk = new ProducerPartitionKey(producerStream, partId);
    maxClosedEndByPartition.merge(ppk, windowEnd, Math::max);
    double prevEma = observedRowsEma.getOrDefault(producerStream, (double) rowCount);
    observedRowsEma.put(producerStream, 0.7 * prevEma + 0.3 * Math.max(1L, rowCount));

    if (cachedBytes > maxCacheBytes) {
      evictExpired(nowWallMs);
      enforceCacheBudget();
    }
    return newVersion;
  }

  /**
   * Attempts to acquire aligned sub-slices covering {@code [windowStart, windowEnd)} for {@code
   * consumerStream}. Returns {@link Optional#empty()} if no compatible active producer exists, any
   * sub-slice is unavailable (forcing lossless raw-scan fallback), or producer fell behind.
   */
  public synchronized Optional<ConsumedSliceSet> tryAcquireSubSlices(
      String consumerStream,
      @Nullable PartitionKey partitionKey,
      long windowStart,
      long windowEnd,
      boolean isRecompute) {
    ConsumerBinding binding = consumerBindings.get(consumerStream);
    if (binding == null) {
      return Optional.empty();
    }
    String producerStream = binding.getProducerStream();
    SliceTaskDescriptor producerDesc = registeredStreams.get(producerStream);
    SliceTaskDescriptor consumerDesc = registeredStreams.get(consumerStream);
    if (producerDesc == null || consumerDesc == null) {
      return Optional.empty();
    }
    long wp = producerDesc.getWindowSizeMs();
    long span = windowEnd - windowStart;
    if (wp <= 0 || span < 2 * wp || (span % wp) != 0) {
      return Optional.empty();
    }

    String partId = partitionKeyId(partitionKey);
    int m = (int) (span / wp);
    List<SharedBaseSlice> collected = new ArrayList<>(m);
    Map<Long, Long> versionMap = new LinkedHashMap<>(m);

    ProducerPartitionKey ppk = new ProducerPartitionKey(producerStream, partId);
    long producerMaxEnd = maxClosedEndByPartition.getOrDefault(ppk, Long.MIN_VALUE);
    long evictedHorizon = evictedHorizonByPartition.getOrDefault(ppk, Long.MIN_VALUE);

    for (int i = 0; i < m; i++) {
      long subStart = windowStart + (long) i * wp;
      SharedBaseSlice subSlice = sliceCache.get(new SliceKey(producerStream, partId, subStart));
      if (subSlice != null && subSlice.getWindowEnd() == subStart + wp) {
        collected.add(subSlice);
        versionMap.put(subStart, subSlice.getStateVersion());
      } else {
        // Only treat missing slice as genuinely empty when producer passed windowEnd AND this range
        // was never evicted; otherwise fall back losslessly to raw scan!
        if (producerMaxEnd < windowEnd || windowStart < evictedHorizon) {
          return Optional.empty();
        }
      }
    }
    if (collected.isEmpty()) {
      return Optional.empty();
    }

    ConsumerWindowKey cwk = new ConsumerWindowKey(consumerStream, partId, windowStart, windowEnd);
    Map<Long, Long> prevVersions = consumedVersions.get(cwk);
    boolean duplicateReplay = prevVersions != null && prevVersions.equals(versionMap);
    if (duplicateReplay && isRecompute) {
      long maxVer = 0L;
      for (Long v : versionMap.values()) {
        if (v != null && v > maxVer) {
          maxVer = v;
        }
      }
      LOGGER.debug(
          StreamNodeMessages
              .LOG_SHAREDSLICEMANAGER_SKIPPED_DUPLICATE_SLICE_VERSION_ARG_FOR_CONSUMER_ARG_ON_WINDOW_ARG_ARG_82576B32,
          maxVer,
          consumerStream,
          windowStart,
          windowEnd);
    }

    for (SharedBaseSlice slice : collected) {
      Long prevSubVersion = prevVersions == null ? null : prevVersions.get(slice.getWindowStart());
      if (prevSubVersion == null || prevSubVersion.longValue() != slice.getStateVersion()) {
        slice.decrementRefCount();
      }
    }
    consumedVersions.put(cwk, versionMap);

    return Optional.of(new ConsumedSliceSet(binding, collected, duplicateReplay));
  }

  public synchronized int evictExpired(long nowWallMs) {
    int evicted = 0;
    long freedBytes = 0L;
    Iterator<Map.Entry<SliceKey, SharedBaseSlice>> it = sliceCache.entrySet().iterator();
    while (it.hasNext()) {
      Map.Entry<SliceKey, SharedBaseSlice> entry = it.next();
      SharedBaseSlice slice = entry.getValue();
      if (slice.getRefCount() <= 0 && nowWallMs >= slice.getExpiresAtWallTimeMs()) {
        evicted++;
        freedBytes += slice.getEstimatedBytes();
        cachedBytes = Math.max(0L, cachedBytes - slice.getEstimatedBytes());
        ProducerPartitionKey ppk =
            new ProducerPartitionKey(slice.getProducerStream(), slice.getPartitionKeyId());
        evictedHorizonByPartition.merge(ppk, slice.getWindowEnd(), Math::max);
        it.remove();
      }
    }
    if (evicted > 0) {
      LOGGER.debug(
          StreamNodeMessages
              .LOG_SHAREDSLICEMANAGER_EVICTED_ARG_EXPIRED_BASE_SLICES_FREED_ARG_BYTES_65540835,
          evicted,
          freedBytes);
    }
    return evicted;
  }

  private void enforceCacheBudget() {
    Iterator<Map.Entry<SliceKey, SharedBaseSlice>> it = sliceCache.entrySet().iterator();
    while (cachedBytes > maxCacheBytes && it.hasNext()) {
      Map.Entry<SliceKey, SharedBaseSlice> entry = it.next();
      SharedBaseSlice slice = entry.getValue();
      cachedBytes = Math.max(0L, cachedBytes - slice.getEstimatedBytes());
      ProducerPartitionKey ppk =
          new ProducerPartitionKey(slice.getProducerStream(), slice.getPartitionKeyId());
      evictedHorizonByPartition.merge(ppk, slice.getWindowEnd(), Math::max);
      it.remove();
    }
  }

  private void rebuildBindings() {
    consumerBindings.clear();
    List<SliceTaskDescriptor> streams = new ArrayList<>(registeredStreams.values());
    for (SliceTaskDescriptor consumer : streams) {
      SliceTaskDescriptor bestProducer = null;
      int[] bestMapping = null;
      for (SliceTaskDescriptor candidate : streams) {
        if (candidate.getTaskName().equals(consumer.getTaskName())) {
          continue;
        }
        int[] mapping = SliceCompatibilitySignature.matchProducerToConsumer(candidate, consumer);
        if (mapping == null) {
          continue;
        }
        int fanOutM = (int) (consumer.getWindowSizeMs() / candidate.getWindowSizeMs());
        long estRows =
            Math.round(
                observedRowsEma.getOrDefault(
                    candidate.getTaskName(), (double) DEFAULT_ESTIMATED_ROWS_PER_SLICE));
        CostDecision decision =
            ReuseCostModel.evaluate(fanOutM, estRows, consumer.getCostThreshold());
        if (!decision.isBeneficial()) {
          continue;
        }
        if (bestProducer == null || candidate.getWindowSizeMs() > bestProducer.getWindowSizeMs()) {
          bestProducer = candidate;
          bestMapping = mapping;
        }
      }
      if (bestProducer != null
          && dependencyGraph.bindEdge(bestProducer.getTaskName(), consumer.getTaskName())) {
        consumerBindings.put(
            consumer.getTaskName(), new ConsumerBinding(bestProducer.getTaskName(), bestMapping));
      }
    }
  }

  public static Column[] evaluateIntermediateColumns(List<TableAggregator> aggregators) {
    Column[] cols = new Column[aggregators.size()];
    for (int i = 0; i < aggregators.size(); i++) {
      TableAggregator agg = aggregators.get(i);
      ColumnBuilder builder = newIntermediateColumnBuilder(agg);
      agg.getAccumulator().evaluateIntermediate(builder);
      cols[i] = builder.build();
    }
    return cols;
  }

  private static ColumnBuilder newIntermediateColumnBuilder(TableAggregator aggregator) {
    TableAccumulator acc = aggregator.getAccumulator();
    if (acc instanceof CountAccumulator
        || acc instanceof CountAllAccumulator
        || acc instanceof CountIfAccumulator) {
      return new LongColumnBuilder(null, 1);
    }
    if (acc instanceof SumAccumulator) {
      return new DoubleColumnBuilder(null, 1);
    }
    if (acc instanceof MinAccumulator
        || acc instanceof MaxAccumulator
        || acc instanceof ExtremeAccumulator) {
      TSDataType type = aggregator.getType();
      switch (type) {
        case INT32:
        case DATE:
          return new IntColumnBuilder(null, 1);
        case INT64:
        case TIMESTAMP:
          return new LongColumnBuilder(null, 1);
        case FLOAT:
          return new FloatColumnBuilder(null, 1);
        case DOUBLE:
          return new DoubleColumnBuilder(null, 1);
        case BOOLEAN:
          return new BooleanColumnBuilder(null, 1);
        default:
          return new BinaryColumnBuilder(null, 1);
      }
    }
    return new BinaryColumnBuilder(null, 1);
  }

  private static long estimateColumnsBytes(Column[] columns) {
    long total = 64L;
    for (Column col : columns) {
      if (col != null) {
        total += col.getRetainedSizeInBytes();
      }
    }
    return total;
  }

  public static final class ConsumerBinding {
    private final String producerStream;
    private final int[] consumerToProducerAggIndex;

    public ConsumerBinding(String producerStream, int[] consumerToProducerAggIndex) {
      this.producerStream = Objects.requireNonNull(producerStream);
      this.consumerToProducerAggIndex = consumerToProducerAggIndex;
    }

    public String getProducerStream() {
      return producerStream;
    }

    public int[] getConsumerToProducerAggIndex() {
      return consumerToProducerAggIndex;
    }
  }

  public static final class ConsumedSliceSet {
    private final ConsumerBinding binding;
    private final List<SharedBaseSlice> subSlices;
    private final boolean duplicateVersion;

    public ConsumedSliceSet(
        ConsumerBinding binding, List<SharedBaseSlice> subSlices, boolean duplicateVersion) {
      this.binding = binding;
      this.subSlices = Collections.unmodifiableList(subSlices);
      this.duplicateVersion = duplicateVersion;
    }

    public ConsumerBinding getBinding() {
      return binding;
    }

    public List<SharedBaseSlice> getSubSlices() {
      return subSlices;
    }

    public boolean isDuplicateVersion() {
      return duplicateVersion;
    }
  }

  private static final class SliceKey {
    final String producerStream;
    final String partitionKeyId;
    final long windowStart;

    SliceKey(String producerStream, String partitionKeyId, long windowStart) {
      this.producerStream = producerStream;
      this.partitionKeyId = partitionKeyId;
      this.windowStart = windowStart;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof SliceKey)) {
        return false;
      }
      SliceKey that = (SliceKey) o;
      return windowStart == that.windowStart
          && Objects.equals(producerStream, that.producerStream)
          && Objects.equals(partitionKeyId, that.partitionKeyId);
    }

    @Override
    public int hashCode() {
      return Objects.hash(producerStream, partitionKeyId, windowStart);
    }
  }

  private static final class ProducerPartitionKey {
    final String producerStream;
    final String partitionKeyId;

    ProducerPartitionKey(String producerStream, String partitionKeyId) {
      this.producerStream = producerStream;
      this.partitionKeyId = partitionKeyId;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof ProducerPartitionKey)) {
        return false;
      }
      ProducerPartitionKey that = (ProducerPartitionKey) o;
      return Objects.equals(producerStream, that.producerStream)
          && Objects.equals(partitionKeyId, that.partitionKeyId);
    }

    @Override
    public int hashCode() {
      return Objects.hash(producerStream, partitionKeyId);
    }
  }

  private static final class ConsumerWindowKey {
    final String consumerStream;
    final String partitionKeyId;
    final long windowStart;
    final long windowEnd;

    ConsumerWindowKey(
        String consumerStream, String partitionKeyId, long windowStart, long windowEnd) {
      this.consumerStream = consumerStream;
      this.partitionKeyId = partitionKeyId;
      this.windowStart = windowStart;
      this.windowEnd = windowEnd;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof ConsumerWindowKey)) {
        return false;
      }
      ConsumerWindowKey that = (ConsumerWindowKey) o;
      return windowStart == that.windowStart
          && windowEnd == that.windowEnd
          && Objects.equals(consumerStream, that.consumerStream)
          && Objects.equals(partitionKeyId, that.partitionKeyId);
    }

    @Override
    public int hashCode() {
      return Objects.hash(consumerStream, partitionKeyId, windowStart, windowEnd);
    }
  }
}
