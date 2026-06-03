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

package org.apache.iotdb.streamnode.utils;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.tsfile.read.common.block.TsBlock;

import java.util.ArrayDeque;
import java.util.Queue;

import static org.apache.iotdb.calc.execution.operator.Operator.NOT_BLOCKED;

public class NonMemoryControlStreamTsBlockQueue implements StreamTsBlockQueue {

  private final Queue<TsBlock> queue = new ArrayDeque<>();

  private SettableFuture<Void> blocked = SettableFuture.create();
  private boolean finished;
  private Throwable abortedCause;

  @Override
  public synchronized ListenableFuture<?> isBlocked() {
    if (!queue.isEmpty() || finished || abortedCause != null) {
      return NOT_BLOCKED;
    }
    return blocked;
  }

  @Override
  public synchronized boolean isEmpty() {
    return queue.isEmpty();
  }

  @Override
  public synchronized boolean isFinished() {
    return finished && queue.isEmpty() && abortedCause == null;
  }

  @Override
  public synchronized ListenableFuture<?> add(TsBlock tsBlock) {
    if (finished) {
      return NOT_BLOCKED;
    }
    queue.add(tsBlock);
    unblock();
    return NOT_BLOCKED;
  }

  @Override
  public synchronized TsBlock remove() {
    if (abortedCause != null) {
      throw new IllegalStateException(abortedCause);
    }
    TsBlock tsBlock = queue.poll();
    if (queue.isEmpty() && !finished && blocked.isDone()) {
      blocked = SettableFuture.create();
    }
    return tsBlock;
  }

  @Override
  public synchronized void setNoMoreTsBlocks() {
    finished = true;
    unblock();
  }

  @Override
  public synchronized void close() {
    finished = true;
    queue.clear();
    unblock();
  }

  @Override
  public synchronized void abort(Throwable t) {
    abortedCause = t;
    finished = true;
    queue.clear();
    if (!blocked.isDone()) {
      blocked.setException(t);
    }
  }

  private void unblock() {
    if (!blocked.isDone()) {
      blocked.set(null);
    }
  }
}
