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

package org.apache.iotdb.streamnode.engine.window;

import org.apache.iotdb.commons.memory.IMemoryBlock;
import org.apache.iotdb.commons.memory.MemoryBlockType;
import org.apache.iotdb.streamnode.conf.StreamNodeDescriptor;
import org.apache.iotdb.streamnode.conf.StreamNodeMemoryConfig;
import org.apache.iotdb.streamnode.i18n.StreamNodeMessages;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Global memory controller shared by all window engines, mirroring the compute module's
 * charge-on-produce / release-on-consume accounting against three node-level pools:
 *
 * <ul>
 *   <li><b>input</b>: batches buffered by {@code push} while processing is inactive (the only place
 *       raw tablets are held back).
 *   <li><b>data</b>: the windows' row caches ({@code WindowRowStore}), charged via {@code
 *       ramSizeEstimate} deltas and released when the last owner (engine or event iterator pinning
 *       the store) drops it.
 *   <li><b>output</b>: TsBlocks materialized by event row iterators, charged on {@code next()} and
 *       released when the consumer asks for the next block or closes the iterator.
 * </ul>
 *
 * <p>All {@code tryReserve*} methods are best-effort: a {@code false} return means the pool is
 * exhausted and triggers backpressure at the caller (a rate-limited warning is logged here): a
 * failed <b>input</b> reservation blocks the producer on a future from {@link
 * #newInputBlockedFuture()} (completed by the next input release); failed <b>data</b>/<b>output</b>
 * reservations pause the window engines' processing until a release fires the registered {@link
 * #registerMemoryReleaseListener(Runnable) release listeners}, which retry. Data itself is never
 * rejected or lost.
 */
public class GlobalWindowMemoryController {
  private static final Logger LOGGER = LoggerFactory.getLogger(GlobalWindowMemoryController.class);

  /** Minimum interval between two memory-pressure warnings, so exhaustion cannot flood the log. */
  private static final long PRESSURE_WARN_INTERVAL_MS = 1000;

  private static volatile GlobalWindowMemoryController instance;

  private static final IMemoryBlock WINDOW_INPUT_MEMORY_BLOCK;
  private static final IMemoryBlock WINDOW_DATA_MEMORY_BLOCK;
  private static final IMemoryBlock WINDOW_OUTPUT_MEMORY_BLOCK;

  private static final AtomicLong LAST_PRESSURE_WARN_MS = new AtomicLong();

  /**
   * Producers blocked on the input pool, parked on futures handed out by {@link
   * #newInputBlockedFuture()} and completed by the next successful input release. The pool is
   * node-global, so a release by any engine wakes any waiting producer.
   */
  private final ArrayDeque<SettableFuture<Void>> inputWaiters = new ArrayDeque<>();

  /**
   * Engines' resume hooks, invoked (asynchronously, off the releasing thread) after a successful
   * <b>data</b> or <b>output</b> release: a paused engine retries its reservation and either
   * resumes processing or pauses again until the next release.
   */
  private final List<Runnable> releaseListeners = new CopyOnWriteArrayList<>();

  /**
   * Engines currently memory-paused, so releases fire the resume listeners only while at least one
   * engine can actually resume (keeps the per-release notify cost near zero in the common case).
   */
  private final AtomicLong pausedEngineCount = new AtomicLong();

  /**
   * Single daemon thread completing input waiters and firing release listeners. Asynchronous by
   * design: releases happen under an engine's lock, and running a listener inline could otherwise
   * take a different engine's lock (lock-order inversion).
   */
  private static final ExecutorService MEMORY_NOTIFY_EXECUTOR =
      Executors.newSingleThreadExecutor(
          r -> {
            Thread t = new Thread(r, "WindowEngine-MemoryNotify");
            t.setDaemon(true);
            return t;
          });

  static {
    WINDOW_INPUT_MEMORY_BLOCK =
        StreamNodeDescriptor.getInstance()
            .getMemoryConfig()
            .getWindowInputMemoryManager()
            .exactAllocate(StreamNodeMemoryConfig.WINDOW_INPUT_POOL, MemoryBlockType.DYNAMIC);
    WINDOW_DATA_MEMORY_BLOCK =
        StreamNodeDescriptor.getInstance()
            .getMemoryConfig()
            .getWindowDataMemoryManager()
            .exactAllocate(StreamNodeMemoryConfig.WINDOW_DATA_POOL, MemoryBlockType.DYNAMIC);
    WINDOW_OUTPUT_MEMORY_BLOCK =
        StreamNodeDescriptor.getInstance()
            .getMemoryConfig()
            .getWindowOutputMemoryManager()
            .exactAllocate(StreamNodeMemoryConfig.WINDOW_OUTPUT_POOL, MemoryBlockType.DYNAMIC);
  }

  private GlobalWindowMemoryController() {}

  public static GlobalWindowMemoryController getInstance() {
    if (instance == null) {
      synchronized (GlobalWindowMemoryController.class) {
        if (instance == null) {
          instance = new GlobalWindowMemoryController();
        }
      }
    }
    return instance;
  }

  public boolean tryReserveInputMemory(long bytes) {
    synchronized (WINDOW_INPUT_MEMORY_BLOCK) {
      return tryReserve(bytes, WINDOW_INPUT_MEMORY_BLOCK, "input");
    }
  }

  public boolean tryReserveDataMemory(long bytes) {
    synchronized (WINDOW_DATA_MEMORY_BLOCK) {
      return tryReserve(bytes, WINDOW_DATA_MEMORY_BLOCK, "data");
    }
  }

  public boolean tryReserveOutputMemory(long bytes) {
    synchronized (WINDOW_OUTPUT_MEMORY_BLOCK) {
      return tryReserve(bytes, WINDOW_OUTPUT_MEMORY_BLOCK, "output");
    }
  }

  public boolean releaseInputMemory(long bytesToRelease) {
    boolean released;
    synchronized (WINDOW_INPUT_MEMORY_BLOCK) {
      released = release(bytesToRelease, WINDOW_INPUT_MEMORY_BLOCK);
    }
    if (released) {
      scheduleNotify();
    }
    return released;
  }

  public boolean releaseDataMemory(long bytesToRelease) {
    boolean released;
    synchronized (WINDOW_DATA_MEMORY_BLOCK) {
      released = release(bytesToRelease, WINDOW_DATA_MEMORY_BLOCK);
    }
    if (released) {
      scheduleNotify();
    }
    return released;
  }

  public boolean releaseOutputMemory(long bytesToRelease) {
    boolean released;
    synchronized (WINDOW_OUTPUT_MEMORY_BLOCK) {
      released = release(bytesToRelease, WINDOW_OUTPUT_MEMORY_BLOCK);
    }
    if (released) {
      scheduleNotify();
    }
    return released;
  }

  /**
   * Hands the calling producer a future that completes when the input pool next frees memory (any
   * engine's input release). Used when an input reservation failed: the batch itself stays buffered
   * - data is never lost - while the producer is throttled until memory frees.
   */
  public ListenableFuture<?> newInputBlockedFuture() {
    SettableFuture<Void> future = SettableFuture.create();
    synchronized (inputWaiters) {
      inputWaiters.add(future);
    }
    // The pool may have freed up between the failed reservation and this call; check once more.
    scheduleNotify();
    return future;
  }

  /**
   * Registers a hook invoked after every successful <b>data</b> or <b>output</b> release (input
   * releases only wake blocked producers). Engines register their memory-pause resume here; it is
   * run asynchronously on the controller's notify thread, never on the releasing thread.
   */
  public void registerMemoryReleaseListener(Runnable listener) {
    releaseListeners.add(listener);
  }

  public void unregisterMemoryReleaseListener(Runnable listener) {
    releaseListeners.remove(listener);
  }

  /**
   * Called by an engine entering the memory pause (transition only), so that releases start firing
   * the resume listeners.
   */
  public void engineMemoryPaused() {
    pausedEngineCount.incrementAndGet();
  }

  /** Called by an engine leaving the memory pause (transition only). */
  public void engineMemoryResumed() {
    pausedEngineCount.decrementAndGet();
  }

  /**
   * @return the number of engines currently memory-paused (backpressure gauge).
   */
  public long getPausedEngineCount() {
    return pausedEngineCount.get();
  }

  /**
   * @return the number of producers currently parked on the input pool (backpressure gauge).
   */
  public long getInputWaiterCount() {
    synchronized (inputWaiters) {
      return inputWaiters.size();
    }
  }

  /**
   * @return the number of registered engine resume listeners (leak gauge: every live engine holds
   *     exactly one; a count that never falls back after engine teardown means engines leak).
   */
  public int getReleaseListenerCount() {
    return releaseListeners.size();
  }

  /**
   * Wakes blocked producers and fires the release listeners on the notify thread, but only when
   * someone can actually make progress (a producer is parked on the input pool, or at least one
   * engine is memory-paused). Callers typically hold an engine's lock, so the work must not run
   * inline. Residual races (a pause registered just after the check) are covered by the engines'
   * tick-based resume retry.
   */
  private void scheduleNotify() {
    boolean wakeProducers;
    synchronized (inputWaiters) {
      wakeProducers = !inputWaiters.isEmpty();
    }
    boolean resumeEngines = pausedEngineCount.get() > 0;
    if (!wakeProducers && !resumeEngines) {
      return;
    }
    MEMORY_NOTIFY_EXECUTOR.execute(() -> runNotifications(wakeProducers, resumeEngines));
  }

  private void runNotifications(boolean wakeProducers, boolean resumeEngines) {
    if (wakeProducers) {
      wakeInputWaiters();
    }
    if (resumeEngines) {
      for (Runnable listener : releaseListeners) {
        try {
          listener.run();
        } catch (Exception e) {
          LOGGER.warn(StreamNodeMessages.LOG_WINDOW_MEMORY_RELEASE_LISTENER_FAILED_A3F62F67, e);
        }
      }
    }
  }

  private void wakeInputWaiters() {
    synchronized (inputWaiters) {
      if (inputWaiters.isEmpty()) {
        return;
      }
    }
    // Only wake producers when the pool actually has room again; a woken producer re-attempts its
    // reservation and re-parks if still short. This also keeps the self-check scheduled by
    // newInputBlockedFuture() from completing a waiter while the pool is still exhausted.
    if (WINDOW_INPUT_MEMORY_BLOCK.getUsedMemoryInBytes()
        >= WINDOW_INPUT_MEMORY_BLOCK.getTotalMemorySizeInBytes()) {
      return;
    }
    synchronized (inputWaiters) {
      while (!inputWaiters.isEmpty()) {
        inputWaiters.poll().set(null);
      }
    }
  }

  public long getInputUsedBytes() {
    return WINDOW_INPUT_MEMORY_BLOCK.getUsedMemoryInBytes();
  }

  public long getDataUsedBytes() {
    return WINDOW_DATA_MEMORY_BLOCK.getUsedMemoryInBytes();
  }

  public long getOutputUsedBytes() {
    return WINDOW_OUTPUT_MEMORY_BLOCK.getUsedMemoryInBytes();
  }

  public long getInputTotalBytes() {
    return WINDOW_INPUT_MEMORY_BLOCK.getTotalMemorySizeInBytes();
  }

  public long getDataTotalBytes() {
    return WINDOW_DATA_MEMORY_BLOCK.getTotalMemorySizeInBytes();
  }

  public long getOutputTotalBytes() {
    return WINDOW_OUTPUT_MEMORY_BLOCK.getTotalMemorySizeInBytes();
  }

  /**
   * @return true if the output pool still has free capacity.
   */
  public boolean hasOutputHeadroom() {
    return WINDOW_OUTPUT_MEMORY_BLOCK.getUsedMemoryInBytes()
        < WINDOW_OUTPUT_MEMORY_BLOCK.getTotalMemorySizeInBytes();
  }

  private boolean tryReserve(long bytes, IMemoryBlock memoryBlock, String poolName) {
    if (memoryBlock.allocate(bytes)) {
      return true;
    }
    warnMemoryPressure(poolName, bytes, memoryBlock);
    return false;
  }

  private boolean release(long bytesToRelease, IMemoryBlock memoryBlock) {
    long usedBytes = memoryBlock.getUsedMemoryInBytes();
    if (bytesToRelease > usedBytes) {
      return false;
    }
    memoryBlock.release(bytesToRelease);
    return true;
  }

  /** Logs one exhaustion warning per second at most, shared by all three pools. */
  private static void warnMemoryPressure(String poolName, long bytes, IMemoryBlock memoryBlock) {
    long now = System.currentTimeMillis();
    long last = LAST_PRESSURE_WARN_MS.get();
    if (now - last < PRESSURE_WARN_INTERVAL_MS || !LAST_PRESSURE_WARN_MS.compareAndSet(last, now)) {
      return;
    }
    LOGGER.warn(
        StreamNodeMessages.LOG_WINDOW_MEMORY_POOL_EXHAUSTED_RESERVE_FAILED_4FCDE878,
        poolName,
        bytes,
        memoryBlock.getUsedMemoryInBytes(),
        memoryBlock.getTotalMemorySizeInBytes());
  }

  /** Return the window memory blocks to their memory managers. */
  public synchronized void close() {
    if (!WINDOW_INPUT_MEMORY_BLOCK.isReleased()) {
      try {
        WINDOW_INPUT_MEMORY_BLOCK.close();
      } catch (Exception e) {
        LOGGER.warn(StreamNodeMessages.LOG_FAILED_TO_CLOSE_WINDOW_INPUT_BLOCK_AA6C1D2F, e);
      }
    }
    if (!WINDOW_DATA_MEMORY_BLOCK.isReleased()) {
      try {
        WINDOW_DATA_MEMORY_BLOCK.close();
      } catch (Exception e) {
        LOGGER.warn(StreamNodeMessages.LOG_FAILED_TO_CLOSE_WINDOW_DATA_BLOCK_E603CF14, e);
      }
    }
    if (!WINDOW_OUTPUT_MEMORY_BLOCK.isReleased()) {
      try {
        WINDOW_OUTPUT_MEMORY_BLOCK.close();
      } catch (Exception e) {
        LOGGER.warn(StreamNodeMessages.LOG_FAILED_TO_CLOSE_WINDOW_OUTPUT_BLOCK_C1C736A2, e);
      }
    }
  }
}
