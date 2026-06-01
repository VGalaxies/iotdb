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

import org.apache.iotdb.commons.concurrent.threadpool.ScheduledExecutorUtil;
import org.apache.iotdb.commons.stream.IoTDBTarget;
import org.apache.iotdb.commons.stream.StreamTarget;
import org.apache.iotdb.isession.pool.ITableSessionPool;
import org.apache.iotdb.session.pool.TableSessionPoolBuilder;
import org.apache.iotdb.streamnode.exception.NonRetryableException;
import org.apache.iotdb.streamnode.exception.RetryableException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class GlobalSinkWriterPool {
  private static final Logger LOGGER = LoggerFactory.getLogger(GlobalSinkWriterPool.class);

  private static volatile GlobalSinkWriterPool instance;

  private final ScheduledExecutorService scheduler;
  private final ExecutorService writerExecutor;
  private final ConcurrentHashMap<String, SessionPoolEntry> sessionPoolEntries;
  private final ConcurrentHashMap<String, SinkPipeline> registeredPipelines;
  private final ConcurrentHashMap<String, Boolean> inReadyQueue;
  private final LinkedBlockingQueue<SinkPipeline> readyPipelines;
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
    this.readyPipelines = new LinkedBlockingQueue<>();
    this.registeredPipelines = new ConcurrentHashMap<>();
    this.inReadyQueue = new ConcurrentHashMap<>();
    this.drainBatchSize = config.getDrainBatchSize();
    this.maxRetryAttempts = config.getMaxRetryAttempts();
    this.retryBackoffBaseMs = config.getRetryBackoffBaseMs();
    this.writerThreadCount = config.getWriterThreadCount();
    this.sleepMsInWriterLoop = config.getMaxBatchLingerMs() / 2;
    AtomicInteger threadIndex = new AtomicInteger(0);
    this.writerExecutor =
        Executors.newFixedThreadPool(
            writerThreadCount,
            r -> new Thread(r, "stream-sink-writer-" + threadIndex.getAndIncrement()));
    this.scheduler =
        Executors.newSingleThreadScheduledExecutor(
            r -> new Thread(r, "stream-sink-linger-checker"));
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

  /** acquire the session pool, must call releaseSessionPool after using pool */
  public ITableSessionPool acquireSessionPool(StreamTarget streamTarget) {
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
                        .nodeUrls(Collections.singletonList(target.getNodeUrls()))
                        .user("root")
                        .password("root")
                        .maxSize(5)
                        .database(target.getDatabase())
                        .build();
                LOGGER.info("Created new session pool for target: {}", poolKey);
                entry = new SessionPoolEntry(new TabletWriter(sessionPool));
              }
              entry.refCount.incrementAndGet();
              return entry;
            });

    LOGGER.debug(
        "Session pool {} refCount increased to {}", poolKey, sessionPoolEntry.refCount.get());
    return sessionPoolEntry.tabletWriter.getSessionPool();
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
    return target.getNodeUrls() + "." + target.getDatabase();
  }

  public void start() {
    if (!running.compareAndSet(false, true)) {
      return;
    }
    for (int i = 0; i < writerThreadCount; i++) {
      writerExecutor.submit(this::writerLoop);
    }
    startLingerChecker();
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
    scheduler.shutdownNow();
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
    inReadyQueue.remove(pipeline.getPipelineId());
  }

  public void notifyDataAvailable(SinkPipeline pipeline) {
    if (inReadyQueue.putIfAbsent(pipeline.getPipelineId(), Boolean.TRUE) == null) {
      readyPipelines.offer(pipeline);
    }
  }

  private void writerLoop() {
    while (running.get()) {
      try {
        SinkPipeline pipeline = readyPipelines.poll(100, TimeUnit.MILLISECONDS);
        if (pipeline == null) {
          Thread.sleep(sleepMsInWriterLoop);
          continue;
        }
        if (!pipeline.isRunning()) {
          inReadyQueue.remove(pipeline.getPipelineId());
          continue;
        }
        processPipeline(pipeline);
        inReadyQueue.remove(pipeline.getPipelineId());
        if (pipeline.needsImmediateProcessing() && pipeline.isRunning()) {
          notifyDataAvailable(pipeline);
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

  private void processPipeline(SinkPipeline pipeline) {
    List<SinkEntry> batch = new ArrayList<>();
    BatchAccumulator acc = pipeline.getAccumulator();
    int drained = pipeline.drainTo(batch, drainBatchSize);
    if (drained != 0) {
      for (SinkEntry entry : batch) {
        acc.add(entry);
        if (acc.shouldFlushBySize()) {
          doFlush(pipeline);
        }
      }
    }
    if (acc.shouldFlushByTime()) {
      doFlush(pipeline);
    }
  }

  private void startLingerChecker() {
    ScheduledExecutorUtil.safelyScheduleAtFixedRate(
        scheduler,
        () -> {
          for (SinkPipeline p : registeredPipelines.values()) {
            if (p.isRunning() && p.getAccumulator().shouldFlushByTime()) {
              notifyDataAvailable(p);
            }
          }
        },
        10,
        10,
        TimeUnit.MILLISECONDS);
  }

  private void doFlush(SinkPipeline pipeline) {
    BatchAccumulator acc = pipeline.getAccumulator();
    FlushBatch batch = acc.buildTabletAndReset();
    try {
      String poolKey = generatePoolKey(pipeline.getIotdbTarget());
      writeWithRetry(batch.getTablet(), poolKey);
      onFlushSuccess(pipeline, batch);
    } catch (Exception e) {
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
              "Write failed (attempt {}), retrying after {}ms: {}",
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
          "Write failed after {} attempts: {}", maxRetryAttempts, lastException.getMessage());
      throw lastException;
    }
  }

  private void onFlushSuccess(SinkPipeline pipeline, FlushBatch batch) {
    boolean recovered = pipeline.getMemoryController().release(batch.getMemoryToRelease());
    if (recovered) pipeline.getQueue().onMemoryReleased(batch.getMemoryToRelease());
    CommitTracker tracker = pipeline.getCommitTracker();
    for (Map.Entry<Integer, Set<Long>> e : batch.getSubTaskCommitIds().entrySet()) {
      tracker.markCommitted(e.getValue(), e.getKey());
    }
  }

  private void onFlushFailure(SinkPipeline pipeline, FlushBatch batch, Exception cause) {
    boolean recovered = pipeline.getMemoryController().release(batch.getMemoryToRelease());
    if (recovered) pipeline.getQueue().onMemoryReleased(batch.getMemoryToRelease());
    LOGGER.error("Flush failed for pipeline {}", pipeline.getPipelineId(), cause);
  }
}
