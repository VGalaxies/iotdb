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
import org.apache.iotdb.isession.ITableSession;
import org.apache.iotdb.isession.pool.ITableSessionPool;
import org.apache.iotdb.rpc.StatementExecutionException;

import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.write.record.Tablet;
import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Regression for empty-EOF flush commit failures escaping {@code processShard}.
 *
 * <p>An EOF-only batch can still call {@code CommitTracker.onWindowDestroy} → source {@code
 * commitSync()}. If that throws outside {@code doFlush}'s try/catch while the flush runs from
 * {@code processShard} (EOF waited in the queue past linger), {@code writerLoop} skips {@code
 * scheduledByShard.remove}. The accumulator is already empty, so a later push sees the stuck
 * scheduled mark and never re-enters the ready queue; timed flush only reads the accumulator, so
 * the shard never recovers.
 */
public class GlobalSinkWriterPoolTest {

  private static final long LINGER_MS = 20L;

  private GlobalSinkWriterPool writerPool;
  private SinkPipeline pipeline;

  @After
  public void tearDown() {
    if (pipeline != null) {
      pipeline.stop();
    }
    if (writerPool != null) {
      writerPool.stopAll();
    }
  }

  @Test
  public void failedBatchRetriesWithoutCommittingAndRecovers() throws Exception {
    prepareFailingPipeline();
    processShard();
    Assert.assertNotNull(pipeline.getPendingBatch(0));
    Assert.assertEquals(0, pipeline.getCommittedId());
    long reserved = pipeline.getPipelineUsedMemoryBytes();
    Assert.assertTrue(reserved > 0);
    flushByTime();
    Assert.assertEquals(0, pipeline.getCommittedId());
    Assert.assertEquals(reserved, pipeline.getPipelineUsedMemoryBytes());
    flushByTime();
    Assert.assertNull(pipeline.getPendingBatch(0));
    Assert.assertEquals(1, pipeline.getCommittedId());
    Assert.assertEquals(0, pipeline.getPipelineUsedMemoryBytes());
  }

  @Test
  public void stopReleasesFailedBatchWithoutCommitting() throws Exception {
    prepareFailingPipeline();
    processShard();
    Assert.assertTrue(pipeline.getPipelineUsedMemoryBytes() > 0);
    pipeline.stop();
    Assert.assertNull(pipeline.getPendingBatch(0));
    Assert.assertEquals(0, pipeline.getPipelineUsedMemoryBytes());
    Assert.assertEquals(0, pipeline.getCommittedId());
  }

  private void prepareFailingPipeline() throws Exception {
    SinkPipelineConfig config =
        new SinkPipelineConfig()
            .setWriterThreadCount(1)
            .setMaxBatchRows(1)
            .setMaxBatchLingerMs(0)
            .setMaxRetryAttempts(0)
            .setRetryBackoffBaseMs(0);
    Constructor<GlobalSinkWriterPool> ctor =
        GlobalSinkWriterPool.class.getDeclaredConstructor(SinkPipelineConfig.class);
    ctor.setAccessible(true);
    writerPool = ctor.newInstance(config);
    IoTDBTarget target =
        new IoTDBTarget(
            "127.0.0.1:6667",
            "test_db",
            "test_table",
            Arrays.asList("time", "s1"),
            Arrays.asList(TSDataType.TIMESTAMP, TSDataType.INT64),
            Arrays.asList(ColumnCategory.TIME, ColumnCategory.FIELD),
            Arrays.asList(0, 1));
    pipeline = new SinkPipeline("retry", target, config, writerPool);
    CommitTracker tracker = new CommitTracker(id -> {});
    pipeline.setCommitTracker(tracker);
    tracker.registerCommitId(1, 1);
    tracker.onWindowOpen(0, 1);
    tracker.onTabletGroupDone(1);

    ITableSessionPool sessions = Mockito.mock(ITableSessionPool.class);
    ITableSession session = Mockito.mock(ITableSession.class);
    Mockito.when(sessions.getSession()).thenReturn(session);
    AtomicInteger attempts = new AtomicInteger();
    Mockito.doAnswer(
            invocation -> {
              Tablet tablet = invocation.getArgument(0);
              Assert.assertEquals(1, tablet.getRowSize());
              if (attempts.incrementAndGet() <= 2) {
                throw new StatementExecutionException("507: Incompatible data type");
              }
              return null;
            })
        .when(session)
        .insert(Mockito.any(Tablet.class));
    Class<?> entryClass = Class.forName(GlobalSinkWriterPool.class.getName() + "$SessionPoolEntry");
    Constructor<?> entryCtor = entryClass.getDeclaredConstructor(TabletWriter.class);
    entryCtor.setAccessible(true);
    Field poolsField = GlobalSinkWriterPool.class.getDeclaredField("sessionPoolEntries");
    poolsField.setAccessible(true);
    @SuppressWarnings("unchecked")
    Map<String, Object> pools = (Map<String, Object>) poolsField.get(writerPool);
    pools.put("127.0.0.1:6667@test_db", entryCtor.newInstance(new TabletWriter(sessions)));

    TsBlockBuilder builder =
        new TsBlockBuilder(Arrays.asList(TSDataType.TIMESTAMP, TSDataType.INT64));
    builder.getTimeColumnBuilder().writeLong(0);
    builder.getColumnBuilder(0).writeLong(10);
    builder.getColumnBuilder(1).writeLong(42);
    builder.declarePosition();
    TsBlock block = builder.build();
    pipeline.push(new SinkEntry(Optional.of(block), 0L, false, 0, block.getRetainedSizeInBytes()));
    pipeline.push(new SinkEntry(Optional.empty(), 0L, true, 0, 0L));
  }

  private void flushByTime() throws Exception {
    Method method = GlobalSinkWriterPool.class.getDeclaredMethod("flushByTime", int.class);
    method.setAccessible(true);
    method.invoke(writerPool, 0);
  }

  private void processShard() throws Exception {
    Method method =
        GlobalSinkWriterPool.class.getDeclaredMethod("processShard", SinkPipeline.class, int.class);
    method.setAccessible(true);
    method.invoke(writerPool, pipeline, 0);
  }

  @Test
  public void emptyEofCommitFailureFromProcessShardStillReschedules() throws Exception {
    SinkPipelineConfig config =
        new SinkPipelineConfig().setWriterThreadCount(1).setMaxBatchLingerMs(LINGER_MS);
    // Reflect private ctor: non-singleton pool so this test owns writer-thread lifecycle without
    // touching production factory APIs.
    java.lang.reflect.Constructor<GlobalSinkWriterPool> ctor =
        GlobalSinkWriterPool.class.getDeclaredConstructor(SinkPipelineConfig.class);
    ctor.setAccessible(true);
    writerPool = ctor.newInstance(config);

    IoTDBTarget target =
        new IoTDBTarget(
            "127.0.0.1:6667",
            "test_db",
            "test_table",
            Arrays.asList("time", "s1"),
            Arrays.asList(TSDataType.TIMESTAMP, TSDataType.INT64),
            Arrays.asList(ColumnCategory.TIME, ColumnCategory.FIELD),
            Arrays.asList(0, 1));
    pipeline = new SinkPipeline("eof-commit-fail", target, config, writerPool);

    AtomicInteger callbacks = new AtomicInteger();
    AtomicBoolean failOnce = new AtomicBoolean(true);
    CommitTracker tracker =
        new CommitTracker(
            commitId -> {
              callbacks.incrementAndGet();
              if (failOnce.compareAndSet(true, false)) {
                throw new RuntimeException("commit failed once");
              }
            });
    pipeline.setCommitTracker(tracker);

    // Window 0 / tablet 1: first EOF destroy fires the failing commit.
    tracker.registerCommitId(1L, 0);
    tracker.onWindowOpen(0L, 1L);
    // Window 1 / tablet 2: must still be flushed after the failing commit.
    tracker.registerCommitId(2L, 0);
    tracker.onWindowOpen(1L, 2L);

    // Push EOF before writers start so it sits in the queue past linger; processShard then drains
    // and flushes immediately via shouldFlushByTime (the stuck-scheduled failure path).
    pipeline.push(new SinkEntry(Optional.empty(), 0L, true, 0, 0L));
    Assert.assertEquals(1, pipeline.getQueue(0).size());
    Thread.sleep(LINGER_MS * 3);

    writerPool.start();

    Awaitility.await().atMost(5, TimeUnit.SECONDS).until(() -> callbacks.get() >= 1);
    Assert.assertEquals(1, callbacks.get());

    // After the failing processShard flush, a stuck scheduled mark would make notifyShard a no-op;
    // the entry stays queued and timed flush never drains the queue.
    pipeline.push(new SinkEntry(Optional.empty(), 1L, true, 0, 0L));

    Awaitility.await()
        .atMost(5, TimeUnit.SECONDS)
        .until(
            () ->
                callbacks.get() >= 2
                    && pipeline.getQueue(0).size() == 0
                    && writerPool.getReadyPipelineCount() == 0);

    Assert.assertEquals(2, callbacks.get());
    Assert.assertEquals(0, pipeline.getQueue(0).size());
    Assert.assertEquals(0, writerPool.getReadyPipelineCount());
  }
}
