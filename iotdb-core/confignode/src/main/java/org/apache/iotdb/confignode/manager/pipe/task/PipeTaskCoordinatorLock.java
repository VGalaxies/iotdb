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

package org.apache.iotdb.confignode.manager.pipe.task;

import org.apache.iotdb.confignode.procedure.impl.pipe.PipeTaskOperation;
import org.apache.iotdb.tsfile.utils.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * PipeTaskCoordinatorLock is a cross thread lock for pipe task coordinator. It is used to ensure
 * that only one thread can execute the pipe task coordinator at the same time.
 */
public class PipeTaskCoordinatorLock {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeTaskCoordinatorLock.class);

  private final BlockingDeque<Pair<Long, PipeTaskOperation>> deque = new LinkedBlockingDeque<>(1);
  private final AtomicLong idGenerator = new AtomicLong(0);

  void lock(PipeTaskOperation op) {
    try {
      final long id = idGenerator.incrementAndGet();
      LOGGER.info(
          "PipeTaskCoordinator lock (id: {}, op: {}) waiting for thread {}",
          id,
          op,
          Thread.currentThread().getName());
      deque.put(new Pair<>(id, op));
      LOGGER.info(
          "PipeTaskCoordinator lock (id: {}, op: {}) acquired by thread {}",
          id,
          op,
          Thread.currentThread().getName());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOGGER.error(
          "Interrupted while waiting for PipeTaskCoordinator lock, current thread: {}",
          Thread.currentThread().getName());
    }
  }

  boolean isLockAcquiredByActivePipeTaskOperation() {
    if (!deque.isEmpty()) {
      final Pair<Long, PipeTaskOperation> pair = deque.peek();
      return Objects.nonNull(pair) && PipeTaskOperation.isPipeTaskOperationActive(pair.right);
    }
    return false;
  }

  boolean tryLock(PipeTaskOperation op) {
    try {
      final long id = idGenerator.incrementAndGet();
      LOGGER.info(
          "PipeTaskCoordinator lock (id: {}, op: {}) waiting for thread {}",
          id,
          op,
          Thread.currentThread().getName());
      if (deque.offer(new Pair<>(id, op), 10, TimeUnit.SECONDS)) {
        LOGGER.info(
            "PipeTaskCoordinator lock (id: {}, op: {}) acquired by thread {}",
            id,
            op,
            Thread.currentThread().getName());
        return true;
      } else {
        LOGGER.info(
            "PipeTaskCoordinator lock (id: {}, op: {}) failed to acquire by thread {} because of timeout",
            id,
            op,
            Thread.currentThread().getName());
        return false;
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOGGER.error(
          "Interrupted while waiting for PipeTaskCoordinator lock, current thread: {}",
          Thread.currentThread().getName());
      return false;
    }
  }

  void unlock(PipeTaskOperation op) {
    final Pair<Long, PipeTaskOperation> pair = deque.poll();
    if (pair == null) {
      LOGGER.error(
          "PipeTaskCoordinator lock released by thread {} but the lock is not acquired by any thread",
          Thread.currentThread().getName());
    } else {
      if (pair.right != op) {
        LOGGER.error(
            "PipeTaskCoordinator lock released by thread {} but the lock op is mismatch, expected {}, actual {}",
            Thread.currentThread().getName(),
            pair.right,
            op);
      } else {
        LOGGER.info(
            "PipeTaskCoordinator lock (id: {}, op: {}) released by thread {}",
            pair.left,
            pair.right,
            Thread.currentThread().getName());
      }
    }
  }
}
