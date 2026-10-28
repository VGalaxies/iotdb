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

import org.apache.iotdb.streamnode.i18n.StreamNodeMessages;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class SinkQueue {
  private static final Logger LOGGER = LoggerFactory.getLogger(SinkQueue.class);

  private final LinkedBlockingQueue<SinkEntry> queue;
  private final SinkMemoryController memoryController;
  private final ConcurrentLinkedQueue<SinkEntry> pendingPushes;
  private volatile boolean closed = false;
  private volatile Runnable backpressureListener;

  public SinkQueue(SinkMemoryController memoryController) {
    this.queue = new LinkedBlockingQueue<>();
    this.memoryController = memoryController;
    this.pendingPushes = new ConcurrentLinkedQueue<>();
  }

  public void setBackpressureListener(Runnable backpressureListener) {
    this.backpressureListener = backpressureListener;
  }

  /**
   * Admits an entry to the queue, counting it via {@code onEnqueued} before it becomes visible to
   * the writer. The closed check, the count and the enqueue are kept atomic so an entry can never
   * be counted without also being either queued or parked (and thus eventually settled).
   *
   * @return an immediate future if admitted, a backpressure future if parked, a failed future if
   *     the queue is already closed (the entry is neither counted nor retained)
   */
  public synchronized ListenableFuture<Void> enqueue(SinkEntry entry) {
    if (closed) {
      return Futures.immediateFailedFuture(new IllegalStateException("SinkQueue closed"));
    }
    long bytes = entry.getMemorySizeInBytes();
    // Permanently unsustainable: fail hard instead of parking a request that can never fit under
    // the pipeline quota, even on an empty pipeline.
    if (memoryController.exceedsAdmissionLimit(bytes)) {
      long limit = memoryController.getAdmissionLimitBytes();
      LOGGER.error(
          StreamNodeMessages
              .LOG_SINK_ENTRY_SIZE_ARG_BYTES_EXCEEDS_THE_PIPELINE_ADMISSION_LIMIT_ARG_BYTES_REJECTING_WITHOUT_PARKING_47B35576,
          bytes,
          limit);
      return Futures.immediateFailedFuture(
          new IllegalArgumentException(
              String.format(
                  StreamNodeMessages
                      .EXCEPTION_SINK_ENTRY_SIZE_ARG_BYTES_EXCEEDS_THE_PIPELINE_ADMISSION_LIMIT_ARG_BYTES_8F5A20CE,
                  bytes,
                  limit)));
    }
    // Same-shard FIFO: a later entry must not tryReserve and jump the drainable queue while
    // earlier work is already parked. Join the tail; {@link #tryAdmitParkedEntries()} takes this
    // same lock, so a concurrent wake waits until the entry is visible and then admits from the
    // head.
    if (!pendingPushes.isEmpty() || !memoryController.tryReserve(bytes)) {
      SettableFuture<Void> future = entry.getOrCreateBackpressureFuture();
      pendingPushes.offer(entry);
      Runnable listener = backpressureListener;
      if (listener != null) {
        listener.run();
      }
      return future;
    }
    queue.offer(entry);
    return Futures.immediateVoidFuture();
  }

  public int drainTo(List<SinkEntry> target, int maxElements) {
    int drained = 0;
    while (drained < maxElements) {
      SinkEntry entry = queue.poll();
      if (entry == null) {
        break;
      }
      target.add(entry);
      drained++;
    }
    return drained;
  }

  public SinkEntry poll() {
    return queue.poll();
  }

  /**
   * Whether the blocking queue has entries that a writer can drain. Parked (pending) entries are
   * not included: they have no reserved memory yet and must be admitted via {@link
   * #tryAdmitParkedEntries()} before they become drainable work.
   */
  public boolean hasData() {
    return !queue.isEmpty();
  }

  /** Whether there are entries parked because their memory reservation failed. */
  public boolean hasPendingPushes() {
    return !pendingPushes.isEmpty();
  }

  public int size() {
    return queue.size();
  }

  /**
   * Tries to reserve memory for parked entries and move them into the drainable queue in FIFO
   * order. Called after shared memory is released. This method and {@link #enqueue} share the same
   * monitor, so a wake that arrives while an entry is parking blocks until that entry is pending
   * and then observes it.
   *
   * @return true if at least one parked entry was admitted into the queue
   */
  public synchronized boolean tryAdmitParkedEntries() {
    boolean admitted = false;
    while (!pendingPushes.isEmpty()) {
      SinkEntry entry = pendingPushes.peek();
      if (entry == null) {
        break;
      }
      if (memoryController.tryReserve(entry.getMemorySizeInBytes())) {
        pendingPushes.poll();
        queue.offer(entry);
        entry.getOrCreateBackpressureFuture().set(null);
        admitted = true;
      } else {
        break;
      }
    }
    return admitted;
  }

  /**
   * Closes the queue and fails every parked entry (never admitted to the queue, so they will never
   * be flushed). Buffered entries are left for the caller to drain.
   */
  public void close() {
    List<SinkEntry> parked;
    synchronized (this) {
      closed = true;
      parked = new ArrayList<>(pendingPushes);
      pendingPushes.clear();
    }
    // Fail the parked entries' futures outside the monitor: a synchronous listener could otherwise
    // run while holding the queue lock.
    for (SinkEntry entry : parked) {
      entry
          .getOrCreateBackpressureFuture()
          .setException(new IllegalStateException("SinkQueue closed"));
    }
  }
}
