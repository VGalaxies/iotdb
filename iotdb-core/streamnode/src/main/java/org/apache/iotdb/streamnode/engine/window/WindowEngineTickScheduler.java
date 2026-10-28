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

import org.apache.iotdb.commons.concurrent.threadpool.ScheduledExecutorUtil;
import org.apache.iotdb.streamnode.i18n.StreamNodeMessages;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Per-StreamTask shared tick thread that drives the system-time triggers (e.g.
 * MAX_DELAY_WINDOW_CLOSE / MAX_IDLE_WINDOW_RETAIN) of all WindowEngines belonging to the same
 * stream task. Each WindowEngine no longer owns a processing thread; data pushed via {@link
 * WindowEngine#push} is processed synchronously on the ingestion thread, so this scheduler's single
 * daemon thread is the only background thread an event-driven engine needs, and it is shared by
 * every engine of the task (one thread per task instead of one thread per device/partition).
 *
 * <p>Engines register themselves via {@link #register} when started and unregister via {@link
 * #unregister} when dropped. The tick loop never blocks: each engine's {@link WindowEngine#tick}
 * runs its {@link WindowEngine#onTick} hook under the engine lock and offers the produced events
 * without waiting, so one slow or failing engine cannot stall the others.
 */
public final class WindowEngineTickScheduler {

  private static final Logger LOGGER = LoggerFactory.getLogger(WindowEngineTickScheduler.class);

  /** Tick interval: how often each registered engine's system-time triggers are evaluated. */
  private static final long TICK_INTERVAL_MS = 50;

  private final String taskName;

  /** Engines currently registered for ticking; live mutated by register/unregister. */
  private final Set<WindowEngine> engines = ConcurrentHashMap.newKeySet();

  private final AtomicBoolean started = new AtomicBoolean(false);

  private final AtomicBoolean stopped = new AtomicBoolean(false);

  private volatile ScheduledFuture<?> tickFuture;

  private volatile ScheduledExecutorService executor;

  public WindowEngineTickScheduler(String taskName) {
    this.taskName = taskName;
  }

  /** Registers an engine for periodic ticking. Idempotent. */
  void register(WindowEngine engine) {
    engines.add(engine);
  }

  /** Unregisters an engine (e.g. it was dropped). Idempotent. */
  void unregister(WindowEngine engine) {
    engines.remove(engine);
  }

  /** Starts the shared tick thread. Idempotent; no-op after {@link #stop()}. */
  public synchronized void start() {
    if (stopped.get() || !started.compareAndSet(false, true)) {
      return;
    }
    executor =
        Executors.newSingleThreadScheduledExecutor(
            r -> {
              Thread t = new Thread(r, "WindowEngine-Tick-" + taskName);
              t.setDaemon(true);
              return t;
            });
    tickFuture =
        ScheduledExecutorUtil.safelyScheduleWithFixedDelay(
            executor, this::tickAll, TICK_INTERVAL_MS, TICK_INTERVAL_MS, TimeUnit.MILLISECONDS);
  }

  /**
   * Stops the tick thread and forgets all registered engines. Intended for the owning stream task's
   * terminal drop; the scheduler cannot be restarted afterwards.
   */
  public synchronized void stop() {
    if (stopped.compareAndSet(false, true)) {
      ScheduledFuture<?> future = tickFuture;
      if (future != null) {
        future.cancel(false);
      }
      ScheduledExecutorService service = executor;
      if (service != null) {
        service.shutdown();
      }
      engines.clear();
    }
  }

  private void tickAll() {
    for (WindowEngine engine : engines) {
      try {
        engine.tick();
      } catch (Throwable t) {
        // One failing engine must never stop the shared thread from ticking the others.
        LOGGER.error(
            StreamNodeMessages.LOG_WINDOW_ENGINE_TICK_FAILED_FOR_TASK_6DB9ACDA, taskName, t);
      }
    }
  }
}
