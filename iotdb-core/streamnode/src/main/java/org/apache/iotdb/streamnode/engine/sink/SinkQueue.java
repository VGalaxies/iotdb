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

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class SinkQueue {
  private final LinkedBlockingQueue<SinkEntry> queue;
  private final SinkMemoryController memoryController;
  private final ConcurrentLinkedQueue<SinkEntry> pendingPushes;
  private volatile boolean closed = false;

  public SinkQueue(SinkMemoryController memoryController) {
    this.queue = new LinkedBlockingQueue<>();
    this.memoryController = memoryController;
    this.pendingPushes = new ConcurrentLinkedQueue<>();
  }

  public ListenableFuture<Void> enqueue(SinkEntry entry) {
    if (closed) {
      return Futures.immediateFailedFuture(new IllegalStateException("SinkQueue closed"));
    }
    if (memoryController.tryReserve(entry.getMemorySizeInBytes())) {
      queue.offer(entry);
      return Futures.immediateVoidFuture();
    } else {
      SettableFuture<Void> future = entry.getOrCreateBackpressureFuture();
      pendingPushes.offer(entry);
      return future;
    }
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

  public boolean hasData() {
    return !queue.isEmpty() || !pendingPushes.isEmpty();
  }

  public int size() {
    return queue.size();
  }

  public void onMemoryReleased(long releasedBytes) {
    while (!pendingPushes.isEmpty()) {
      SinkEntry entry = pendingPushes.peek();
      if (entry == null) {
        break;
      }
      if (memoryController.tryReserve(entry.getMemorySizeInBytes())) {
        pendingPushes.poll();
        queue.offer(entry);
        entry.getOrCreateBackpressureFuture().set(null);
      } else {
        break;
      }
    }
  }

  public void close() {
    closed = true;
    SinkEntry entry;
    while ((entry = pendingPushes.poll()) != null) {
      entry
          .getOrCreateBackpressureFuture()
          .setException(new IllegalStateException("SinkQueue closed"));
    }
  }
}
