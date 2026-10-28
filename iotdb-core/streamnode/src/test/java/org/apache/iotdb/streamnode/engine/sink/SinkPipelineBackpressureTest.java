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

package org.apache.iotdb.streamnode.engine.sink;

import org.apache.iotdb.commons.stream.IoTDBTarget;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Backpressure visibility: a pipeline blocked solely by the shared write-back pool (global
 * exhaustion) must report backpressure as active, not only pipelines that tripped their own quota.
 */
public class SinkPipelineBackpressureTest {

  private static IoTDBTarget newTarget() {
    return new IoTDBTarget(
        "127.0.0.1:6667",
        "test_db",
        "test_table",
        Arrays.asList("time", "s1"),
        Arrays.asList(TSDataType.TIMESTAMP, TSDataType.INT64),
        Arrays.asList(ColumnCategory.TIME, ColumnCategory.FIELD),
        Arrays.asList(0, 1));
  }

  @Test
  public void testGlobalExhaustionActivatesBackpressureAndRecovers() {
    final GlobalSinkMemoryController global = GlobalSinkMemoryController.getInstance();
    SinkPipeline pipeline = null;
    long exhaustedBytes = 0L;
    try {
      // Exhaust the shared write-back pool so a tiny push cannot reserve memory anywhere.
      // 1MB chunks leave a remainder smaller than 1MB; take that too, or a 4KB entry still fits.
      while (global.tryReserve(1024L * 1024L)) {
        exhaustedBytes += 1024L * 1024L;
      }
      final long remainder = global.getFreeMemoryInBytes();
      if (remainder > 0L && global.tryReserve(remainder)) {
        exhaustedBytes += remainder;
      }

      // A generous pipeline quota (10MB) keeps the local quota out of the picture: the park below
      // can only be caused by global exhaustion.
      final SinkPipelineConfig config =
          new SinkPipelineConfig().setPipelineMemoryLimitBytes(10L * 1024L * 1024L);
      pipeline =
          new SinkPipeline(
              "backpressure-test-pipeline",
              newTarget(),
              config,
              GlobalSinkWriterPool.getInstance());

      Assert.assertFalse(pipeline.isBackpressureActive());

      // A push that fits the pipeline quota easily parks solely because the global pool is
      // exhausted; the active gauge must report this blockage even though the local quota was
      // never hit.
      final SinkEntry entry = new SinkEntry(Optional.empty(), 0L, false, 0, 4096L);
      final ListenableFuture<Void> future = pipeline.getQueue(0).enqueue(entry);
      Assert.assertFalse(future.isDone());
      Assert.assertTrue(pipeline.getQueue(0).hasPendingPushes());
      Assert.assertTrue(pipeline.isBackpressureActive());

      // Returning memory to the shared pool (as another pipeline's release would) unblocks it.
      Assert.assertTrue(global.release(exhaustedBytes));
      exhaustedBytes = 0L;
      Assert.assertTrue(pipeline.getQueue(0).tryAdmitParkedEntries());
      Assert.assertTrue(future.isDone());
      Assert.assertFalse(pipeline.isBackpressureActive());
    } finally {
      if (pipeline != null) {
        pipeline.stop();
      }
      if (exhaustedBytes > 0L) {
        global.release(exhaustedBytes);
      }
    }
  }

  /**
   * {@link GlobalSinkWriterPool#notifyMemoryReleased()} must take the queue lock rather than
   * peeking {@code hasPendingPushes()} first. A wake that arrives after reservation failed and
   * before the entry is pending would otherwise skip the queue and leave it stranded.
   */
  @Test
  public void testNotifyMemoryReleasedAdmitsEntryParkedDuringWake() throws Exception {
    CountDownLatch enteredFailingReserve = new CountDownLatch(1);
    CountDownLatch admitBlocked = new CountDownLatch(1);
    AtomicBoolean delayNextFailure = new AtomicBoolean(true);
    SinkMemoryController memoryController =
        new SinkMemoryController(100L, GlobalSinkMemoryController.getInstance()) {
          @Override
          public boolean tryReserve(long bytes) {
            boolean reserved = super.tryReserve(bytes);
            if (!reserved && delayNextFailure.compareAndSet(true, false)) {
              enteredFailingReserve.countDown();
              try {
                Assert.assertTrue(admitBlocked.await(5, TimeUnit.SECONDS));
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                Assert.fail("interrupted while delaying the failing reservation");
              }
            }
            return reserved;
          }
        };

    java.lang.reflect.Constructor<GlobalSinkWriterPool> ctor =
        GlobalSinkWriterPool.class.getDeclaredConstructor(SinkPipelineConfig.class);
    ctor.setAccessible(true);
    SinkPipelineConfig config =
        new SinkPipelineConfig().setWriterThreadCount(1).setPipelineMemoryLimitBytes(100L);
    GlobalSinkWriterPool writerPool = ctor.newInstance(config);
    SinkPipeline pipeline = null;
    try {
      pipeline =
          new SinkPipeline("lost-wake-pipeline", newTarget(), config, writerPool, memoryController);
      Assert.assertTrue(
          pipeline
              .getQueue(0)
              .enqueue(new SinkEntry(Optional.empty(), 1L, false, 0, 100L))
              .isDone());
      // Keep the reservation, drop the entry so stop() does not release it again.
      Assert.assertNotNull(pipeline.getQueue(0).poll());

      final SinkPipeline parkedPipeline = pipeline;
      AtomicReference<ListenableFuture<Void>> parkedFuture = new AtomicReference<>();
      Thread enqueuer =
          new Thread(
              () ->
                  parkedFuture.set(
                      parkedPipeline
                          .getQueue(0)
                          .enqueue(new SinkEntry(Optional.empty(), 2L, false, 0, 1L))),
              "lost-wake-enqueuer");
      enqueuer.start();
      Assert.assertTrue(enteredFailingReserve.await(5, TimeUnit.SECONDS));
      Assert.assertTrue(memoryController.release(100L));

      Thread waker = new Thread(writerPool::notifyMemoryReleased, "lost-wake-notifier");
      waker.start();
      try {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (waker.getState() != Thread.State.BLOCKED) {
          if (System.nanoTime() > deadline) {
            Assert.fail("notifyMemoryReleased did not block on the enqueue lock");
          }
          Thread.yield();
        }
      } finally {
        admitBlocked.countDown();
      }
      enqueuer.join(TimeUnit.SECONDS.toMillis(5));
      waker.join(TimeUnit.SECONDS.toMillis(5));
      Assert.assertFalse(enqueuer.isAlive());
      Assert.assertFalse(waker.isAlive());

      ListenableFuture<Void> future = parkedFuture.get();
      Assert.assertNotNull(future);
      Assert.assertTrue(future.isDone());
      Assert.assertFalse(pipeline.getQueue(0).hasPendingPushes());
      Assert.assertEquals(1, pipeline.getQueue(0).size());
    } finally {
      if (pipeline != null) {
        pipeline.stop();
      }
      writerPool.stopAll();
    }
  }
}
