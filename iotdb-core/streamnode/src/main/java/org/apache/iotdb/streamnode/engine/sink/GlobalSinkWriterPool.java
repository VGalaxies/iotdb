/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.streamnode.engine.sink;

import org.apache.iotdb.commons.stream.IoTDBTarget;
import org.apache.iotdb.commons.stream.StreamTarget;
import org.apache.iotdb.isession.pool.ITableSessionPool;
import org.apache.iotdb.session.pool.TableSessionPoolBuilder;
import org.apache.iotdb.streamnode.conf.StreamNodeConfig;
import org.apache.iotdb.streamnode.conf.StreamNodeDescriptor;
import org.apache.iotdb.streamnode.exception.NonRetryableException;
import org.apache.iotdb.streamnode.exception.RetryableException;
import org.apache.iotdb.streamnode.i18n.StreamNodeMessages;
import org.apache.iotdb.streamnode.metrics.SinkPipelineMetrics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

public class GlobalSinkWriterPool {
  private static final Logger LOGGER = LoggerFactory.getLogger(GlobalSinkWriterPool.class);
  private static final StreamNodeConfig snConfig = StreamNodeDescriptor.getInstance().getConfig();

  private static volatile GlobalSinkWriterPool instance;

  private final ExecutorService writerExecutor;
  private final ConcurrentHashMap<String, SessionPoolEntry> sessionPoolEntries;
  private final ConcurrentHashMap<String, SinkPipeline> registeredPipelines;

  /**
   * Per-writer-thread ready queues, indexed by shard. {@code readyShards[i]} holds the pipelines
   * that have work in shard {@code i} (queued entries, or a partial batch to flush) and is polled
   * only by writer thread {@code i}. Because each shard is owned by exactly one thread, a pipeline
   * sitting in this queue is never processed concurrently.
   */
  private final List<BlockingQueue<SinkPipeline>> readyShards;

  /**
   * Per-writer-thread de-duplication markers, parallel to {@link #readyShards}: {@code
   * scheduledByShard[i]} contains a pipeline id exactly while that pipeline is queued in {@code
   * readyShards[i]} or being processed by writer thread {@code i}. {@link #notifyShard} adds the id
   * and only enqueues when it was absent, so each pipeline appears at most once per shard; the
   * writer removes the id after processing and re-adds it if work remains, which is what keeps the
   * queue from growing without bound while still guaranteeing no work is left unscheduled.
   */
  private final List<Set<String>> scheduledByShard;

  private final AtomicBoolean running = new AtomicBoolean(false);
  private final int drainBatchSize;
  private final int maxRetryAttempts;
  private final long retryBackoffBaseMs;
  private final int writerThreadCount;
  private final long sleepMsInWriterLoop;

  private static class SessionPoolEntry {
    final TabletWriter tabletWriter;
    final AtomicInteger refCount;

    SessionPoolEntry(TabletWriter tabletWriter) {
      this.tabletWriter = tabletWriter;
      this.refCount = new AtomicInteger(0);
    }
  }

  private GlobalSinkWriterPool(SinkPipelineConfig config) {
    this.sessionPoolEntries = new ConcurrentHashMap<>();
    this.registeredPipelines = new ConcurrentHashMap<>();
    this.drainBatchSize = config.getDrainBatchSize();
    this.maxRetryAttempts = config.getMaxRetryAttempts();
    this.retryBackoffBaseMs = config.getRetryBackoffBaseMs();
    this.writerThreadCount = config.getWriterThreadCount();
    this.sleepMsInWriterLoop = config.getMaxBatchLingerMs() / 2;
    this.readyShards = new ArrayList<>(writerThreadCount);
    this.scheduledByShard = new ArrayList<>(writerThreadCount);
    for (int i = 0; i < writerThreadCount; i++) {
      readyShards.add(new LinkedBlockingQueue<>());
      scheduledByShard.add(ConcurrentHashMap.newKeySet());
    }
    AtomicInteger threadIndex = new AtomicInteger(0);
    this.writerExecutor =
        Executors.newFixedThreadPool(
            writerThreadCount,
            r -> new Thread(r, "stream-sink-writer-" + threadIndex.getAndIncrement()));
  }

  public static GlobalSinkWriterPool getInstance() {
    return getInstance(
        new SinkPipelineConfig()
            .setMaxRetryAttempts(snConfig.getSnWriteBackMaxRetryAttempts())
            .setRetryBackoffBaseMs(snConfig.getSnWriteBackRetryBackoffBaseMs()));
  }

  public static GlobalSinkWriterPool getInstance(SinkPipelineConfig config) {
    if (instance == null) {
      synchronized (GlobalSinkWriterPool.class) {
        if (instance == null) {
          instance = new GlobalSinkWriterPool(config);
        }
      }
    }
    return instance;
  }

  /**
   * Creates a session pool for {@code streamTarget} if absent, then retains it ({@code
   * refCount++}). Must be paired with {@link #releaseSessionPool}.
   */
  public void acquireSessionPool(StreamTarget streamTarget) {
    IoTDBTarget target;
    if (streamTarget instanceof IoTDBTarget) {
      target = (IoTDBTarget) streamTarget;
    } else {
      throw new UnsupportedOperationException("streamTarget type is not IoTDBTarget");
    }

    String poolKey = generatePoolKey(target);
    SessionPoolEntry sessionPoolEntry =
        sessionPoolEntries.compute(
            poolKey,
            (key, entry) -> {
              if (entry == null) {
                ITableSessionPool sessionPool =
                    new TableSessionPoolBuilder()
                        .nodeUrls(parseNodeUrls(target.getNodeUrls()))
                        .user(snConfig.getSnClusterIngressUsername())
                        .password(snConfig.getSnClusterIngressPassword())
                        .maxSize(5)
                        .database(target.getDatabase())
                        .maxRetryCount(snConfig.getSnWriteBackSessionMaxRetryCount())
                        .retryIntervalInMs(snConfig.getSnWriteBackSessionRetryIntervalMs())
                        .connectionTimeoutInMs(snConfig.getSnWriteBackSessionConnectionTimeoutMs())
                        .enableAutoCreateSchema(false)
                        .build();
                LOGGER.info("Created new session pool for target: {}", poolKey);
                entry = new SessionPoolEntry(new TabletWriter(sessionPool));
              }
              entry.refCount.incrementAndGet();
              return entry;
            });

    LOGGER.debug(
        "Session pool {} refCount increased to {}", poolKey, sessionPoolEntry.refCount.get());
  }

  public void releaseSessionPool(StreamTarget streamTarget) {
    IoTDBTarget target;
    if (streamTarget instanceof IoTDBTarget) {
      target = (IoTDBTarget) streamTarget;
    } else {
      throw new UnsupportedOperationException("streamTarget type is not IoTDBTarget");
    }

    String poolKey = generatePoolKey(target);
    // Use compute to ensure thread-safe decrement and cleanup
    sessionPoolEntries.compute(
        poolKey,
        (key, entry) -> {
          if (entry == null) {
            LOGGER.warn("Session pool not found for target: {}", key);
            return null;
          }
          int newRefCount = entry.refCount.decrementAndGet();
          LOGGER.debug("Session pool {} refCount decreased to {}", key, newRefCount);
          if (newRefCount == 0) {
            try {
              entry.tabletWriter.getSessionPool().close();
            } catch (Exception e) {
              LOGGER.warn("Exception while closing the session pool", e);
            }
            LOGGER.info("Removed session pool for target: {}", key);
            return null; // Remove from map by returning null
          }
          return entry; // Keep in map with updated refCount
        });
  }

  private String generatePoolKey(IoTDBTarget target) {
    return target.getNodeUrls() + "@" + target.getDatabase();
  }

  /**
   * Parses the target's nodeUrls into a list of "ip:port" seeds for the session pool.
   *
   * <p>nodeUrls may be a single endpoint (e.g. from the test-only constructor) or a comma-separated
   * list of DataNode endpoints resolved from the cluster; the session pool then fails over across
   * the whole list.
   */
  private static List<String> parseNodeUrls(String nodeUrls) {
    if (nodeUrls == null || nodeUrls.isEmpty()) {
      throw new IllegalArgumentException("IoTDBTarget nodeUrls must not be empty");
    }
    String[] parts = nodeUrls.split(",");
    List<String> urls = new ArrayList<>(parts.length);
    for (String part : parts) {
      String trimmed = part.trim();
      if (!trimmed.isEmpty()) {
        urls.add(trimmed);
      }
    }
    if (urls.isEmpty()) {
      throw new IllegalArgumentException("IoTDBTarget nodeUrls must not be empty");
    }
    return urls;
  }

  public void start() {
    if (!running.compareAndSet(false, true)) {
      return;
    }
    for (int shard = 0; shard < writerThreadCount; shard++) {
      final int writerShard = shard;
      writerExecutor.submit(() -> writerLoop(writerShard));
    }
  }

  /** Number of shards/writer threads; a window is pinned to shard {@code floorMod(windowId, n)}. */
  public int getShardCount() {
    return writerThreadCount;
  }

  /**
   * Sum of ready-queue sizes across writer shards. A pipeline may appear in more than one shard
   * queue, so this is an upper-bound style backlog signal rather than a distinct-pipeline count.
   */
  public int getReadyPipelineCount() {
    int total = 0;
    for (BlockingQueue<SinkPipeline> readyShard : readyShards) {
      total += readyShard.size();
    }
    return total;
  }

  /** Stop a single pipeline and release its session pool */
  public void stop(SinkPipeline pipeline) {
    unregister(pipeline);
    releaseSessionPool(pipeline.getIotdbTarget());
    LOGGER.info("Stopped pipeline: {}", pipeline.getPipelineId());
  }

  /** Stop the writer pool, discard data in the buffer queue */
  public void stopAll() {
    running.set(false);
    writerExecutor.shutdownNow();

    // close session pool
    for (SessionPoolEntry entry : sessionPoolEntries.values()) {
      try {
        entry.tabletWriter.getSessionPool().close();
      } catch (Exception e) {
        LOGGER.warn("Exception while closing the session pool", e);
      }
    }
    sessionPoolEntries.clear();
    LOGGER.info("GlobalSinkWriterPool stopped");
  }

  public void register(SinkPipeline pipeline) {
    registeredPipelines.put(pipeline.getPipelineId(), pipeline);
  }

  public void unregister(SinkPipeline pipeline) {
    registeredPipelines.remove(pipeline.getPipelineId());
    for (Set<String> scheduledShard : scheduledByShard) {
      scheduledShard.remove(pipeline.getPipelineId());
    }
  }

  /** Schedules {@code pipeline}'s shard for its owning writer thread, de-duplicated per shard. */
  public void notifyShard(SinkPipeline pipeline, int shard) {
    if (scheduledByShard.get(shard).add(pipeline.getPipelineId())) {
      readyShards.get(shard).offer(pipeline);
    }
  }

  private void writerLoop(int shard) {
    // Run the linger check on this owner thread only, so it never reads another thread's
    // BatchAccumulator. Doing it on a timer (not only on poll timeout) keeps the linger flush
    // latency bounded even when this shard is continuously busy with other pipelines.
    final long lingerCheckIntervalNanos = TimeUnit.MILLISECONDS.toNanos(sleepMsInWriterLoop);
    long lastLingerCheckNanos = System.nanoTime();
    while (running.get()) {
      try {
        if (System.nanoTime() - lastLingerCheckNanos >= lingerCheckIntervalNanos) {
          flushByTime(shard);
          lastLingerCheckNanos = System.nanoTime();
        }
        SinkPipeline pipeline = readyShards.get(shard).poll(100, TimeUnit.MILLISECONDS);
        if (pipeline == null) {
          continue;
        }
        if (!pipeline.isRunning()) {
          scheduledByShard.get(shard).remove(pipeline.getPipelineId());
          continue;
        }
        processShard(pipeline, shard);
        scheduledByShard.get(shard).remove(pipeline.getPipelineId());
        // Re-arm only if more entries arrived while processing; a partial batch is flushed by the
        // periodic flushByTime() above.
        if (pipeline.isRunning()
            && pipeline.getQueue(shard).hasData()
            && scheduledByShard.get(shard).add(pipeline.getPipelineId())) {
          readyShards.get(shard).offer(pipeline);
        }
      } catch (InterruptedException e) {
        if (!running.get()) {
          Thread.currentThread().interrupt();
          break;
        }
      } catch (Exception e) {
        LOGGER.error("Writer thread error", e);
      }
    }
  }

  private void processShard(SinkPipeline pipeline, int shard) {
    ReentrantLock lock = pipeline.getShardLock(shard);
    lock.lock();
    try {
      // Re-check under the lock: stop() may have claimed the shard while this pipeline was queued;
      // its cleanup owns the buffered entries then.
      if (!pipeline.isRunning()) {
        return;
      }
      List<SinkEntry> batch = new ArrayList<>();
      BatchAccumulator acc = pipeline.getAccumulator(shard);
      int drained = pipeline.getQueue(shard).drainTo(batch, drainBatchSize);
      if (drained != 0) {
        for (SinkEntry entry : batch) {
          acc.add(entry);
          if (acc.shouldFlushBySize()) {
            doFlush(pipeline, shard);
          }
        }
      }
      if (acc.shouldFlushByTime()) {
        doFlush(pipeline, shard);
      }
    } finally {
      lock.unlock();
    }
  }

  /**
   * Flushes the given shard's accumulators whose linger timeout elapsed. Runs on the shard's owning
   * thread, so it never touches another thread's {@link BatchAccumulator}.
   */
  private void flushByTime(int shard) {
    for (SinkPipeline pipeline : registeredPipelines.values()) {
      ReentrantLock lock = pipeline.getShardLock(shard);
      lock.lock();
      try {
        if (pipeline.isRunning() && pipeline.getAccumulator(shard).shouldFlushByTime()) {
          doFlush(pipeline, shard);
        }
      } finally {
        lock.unlock();
      }
    }
  }

  /**
   * Flushes one shard of a pipeline. Callers ({@code processShard}, {@code flushByTime}) must hold
   * {@code pipeline.getShardLock(shard)}, which also excludes {@code SinkPipeline.doStop()}.
   *
   * <p>Zero-row batches still run {@link #onFlushSuccess} inside the try/catch: an EOF entry can
   * close a window and invoke source {@code commitSync()}, which may throw. Leaving that callback
   * outside the catch lets the exception escape {@code processShard} and skip {@code
   * scheduledByShard.remove}, so a concurrently queued entry can never be re-armed.
   */
  private void doFlush(SinkPipeline pipeline, int shard) {
    BatchAccumulator acc = pipeline.getAccumulator(shard);
    FlushBatch batch = acc.buildTabletAndReset();
    boolean hasRows = batch.getTablet().getRowSize() > 0;

    SinkPipelineMetrics metrics = pipeline.getMetrics();
    if (hasRows && metrics != null) {
      metrics.recordFlush();
      metrics.recordBatchLinger(batch.getBatchLingerNanos(), TimeUnit.NANOSECONDS);
    }
    long startNanos = System.nanoTime();
    try {
      if (hasRows) {
        String poolKey = generatePoolKey(pipeline.getIotdbTarget());
        writeWithRetry(batch.getTablet(), poolKey);
        if (metrics != null) {
          metrics.recordFlushLatency(System.nanoTime() - startNanos, TimeUnit.NANOSECONDS);
          metrics.recordWriteSuccess();
        }
      }
      onFlushSuccess(pipeline, batch);
    } catch (Exception e) {
      if (hasRows && metrics != null) {
        metrics.recordFlushLatency(System.nanoTime() - startNanos, TimeUnit.NANOSECONDS);
        metrics.recordWriteFail();
      }
      onFlushFailure(pipeline, batch, e);
    }
  }

  private void writeWithRetry(org.apache.tsfile.write.record.Tablet tablet, String poolKey)
      throws Exception {
    Exception lastException = null;
    SessionPoolEntry entry = sessionPoolEntries.get(poolKey);
    if (entry == null) {
      throw new IllegalStateException("Session pool entry not found for poolKey: " + poolKey);
    }
    TabletWriter writer = entry.tabletWriter;
    for (int attempt = 0; attempt <= maxRetryAttempts; attempt++) {
      try {
        writer.write(tablet);
        return;
      } catch (RetryableException e) {
        lastException = e;
        if (attempt < maxRetryAttempts) {
          long backoffMs = retryBackoffBaseMs * (1L << attempt);
          LOGGER.warn(
              StreamNodeMessages.LOG_WRITE_FAILED_ATTEMPT_ARG_RETRYING_AFTER_ARG_MS_ARG_2B200618,
              attempt + 1,
              backoffMs,
              e.getMessage());
          Thread.sleep(backoffMs);
        }
      } catch (NonRetryableException e) {
        throw e;
      }
    }

    if (lastException != null) {
      LOGGER.error(
          StreamNodeMessages.LOG_WRITE_FAILED_AFTER_ARG_ATTEMPTS_ARG_524C30CF,
          maxRetryAttempts,
          lastException.getMessage());
      throw lastException;
    }
  }

  private void onFlushSuccess(SinkPipeline pipeline, FlushBatch batch) {
    if (pipeline.getMemoryController().release(batch.getMemoryToRelease())) {
      notifyMemoryReleased();
    }
    CommitTracker tracker = pipeline.getCommitTracker();
    if (tracker == null) {
      return;
    }
    // One callback per SinkEntry (not per FlushBatch / row); EOF may close a window. markSettled
    // makes the notification exactly-once against the close/cleanup drop paths.
    for (SinkEntry entry : batch.getEntries()) {
      if (entry.getWindowId() < 0 || !entry.isEof()) {
        continue;
      }
      tracker.onWindowDestroy(entry.getWindowId());
    }
  }

  private void onFlushFailure(SinkPipeline pipeline, FlushBatch batch, Exception cause) {
    if (pipeline.getMemoryController().release(batch.getMemoryToRelease())) {
      notifyMemoryReleased();
    }
    CommitTracker tracker = pipeline.getCommitTracker();
    if (tracker != null) {
      // Best-effort: drop the failed windows from commit gating so later windows can still
      // advance source commit instead of stalling on a window that will never flush.
      for (SinkEntry entry : batch.getEntries()) {
        if (entry.getWindowId() < 0) {
          continue;
        }
        tracker.onWindowFailed(entry.getWindowId());
      }
    }
    LOGGER.error(
        StreamNodeMessages.LOG_FLUSH_FAILED_FOR_PIPELINE_ARG_3275329B,
        pipeline.getPipelineId(),
        cause);
  }

  /**
   * Retries entries parked by any running pipeline after shared write-back memory was released.
   *
   * <p>Must be invoked from every path that returns memory to the global pool (flush success, flush
   * failure, and pipeline stop/discard). A pipeline may have parked entries because the shared
   * global pool rejected the reservation even though its own quota was fine; without a
   * cross-pipeline wake those entries stay blocked forever when no further flush happens.
   *
   * <p>Always calls {@link SinkQueue#tryAdmitParkedEntries()} instead of peeking {@code
   * hasPendingPushes()} first: that peek is not the queue lock, so it can miss an entry that has
   * just failed reservation and is about to park. {@code tryAdmitParkedEntries} shares the enqueue
   * monitor, so it waits until parking finishes and then sees the entry.
   */
  public void notifyMemoryReleased() {
    for (SinkPipeline pipeline : registeredPipelines.values()) {
      if (!pipeline.isRunning()) {
        continue;
      }
      for (int shard = 0; shard < writerThreadCount; shard++) {
        SinkQueue queue = pipeline.getQueue(shard);
        if (queue.tryAdmitParkedEntries()) {
          // Admitted entries now sit in the shard queue; wake its owning writer to drain them.
          notifyShard(pipeline, shard);
        }
      }
    }
  }
}
