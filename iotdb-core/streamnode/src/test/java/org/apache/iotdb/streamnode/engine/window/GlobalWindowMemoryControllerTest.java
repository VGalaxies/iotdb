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

import org.apache.iotdb.commons.stream.CapacityWindow;
import org.apache.iotdb.streamnode.engine.task.StreamSubTask.DataSlice;
import org.apache.iotdb.streamnode.utils.IEventRowsIterator;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.function.BooleanSupplier;

import static org.apache.iotdb.streamnode.engine.window.WindowEngineTestUtils.createDriverContext;

/**
 * Verifies the window engine's accounting against the three global memory pools of {@link
 * GlobalWindowMemoryController}: input (batches buffered while processing is inactive), data (the
 * windows' row caches, held while an event iterator can still pin them) and output (materialized
 * event TsBlocks). Each test works on per-test deltas of the shared singleton's usage counters.
 *
 * <p>The backpressure tests exhaust a pool by reserving all its free capacity, then assert the
 * specified semantics: input exhaustion blocks the producer on the returned future; data/output
 * exhaustion memory-pauses the engine (pushes buffer into the input pool) until a release lifts the
 * pause - re-pausing when the released memory is still not enough.
 */
public class GlobalWindowMemoryControllerTest {

  private static final long AWAIT_TIMEOUT_MS = 5000;

  private final GlobalWindowMemoryController controller =
      GlobalWindowMemoryController.getInstance();

  private CapacityWindowEngine createEngine(int capacity) {
    CapacityWindow window = new CapacityWindow(capacity, null);
    window.setEventInputColumns(Collections.singletonList("col0"));
    // Value caching only happens with ${rows}; these tests assert cached column values.
    window.setContainsRowsPlaceholder(true);
    return new CapacityWindowEngine(window, createDriverContext(), null, null);
  }

  private DataSlice createDataSlice(int rowCount) {
    Tablet tablet =
        new Tablet(
            "root.test.device",
            Collections.singletonList(new MeasurementSchema("col0", TSDataType.INT64)),
            rowCount);
    for (int i = 0; i < rowCount; i++) {
      tablet.getTimestamps()[i] = i * 100L;
      tablet.addValue(i, "col0", (long) i);
    }
    tablet.setRowSize(rowCount);
    return new DataSlice(null, tablet, 0, rowCount, -1L);
  }

  @Test
  public void testInputMemoryChargedWhilePausedAndReleasedOnStart() {
    CapacityWindowEngine engine = createEngine(10);
    engine.stop();
    long baseline = controller.getInputUsedBytes();

    engine.push(Collections.singletonList(createDataSlice(3)));
    Assert.assertTrue(
        "a batch buffered while paused should charge the input pool",
        controller.getInputUsedBytes() > baseline);

    // start() drains the buffered batch: its input charge is returned (the rows now live in the
    // still-open window's row cache, charged to the data pool instead).
    engine.start();
    Assert.assertEquals(
        "draining the buffered batch should return its input charge",
        baseline,
        controller.getInputUsedBytes());
  }

  @Test
  public void testInputMemoryReleasedOnDrop() {
    CapacityWindowEngine engine = createEngine(10);
    engine.stop();
    long baseline = controller.getInputUsedBytes();

    engine.push(Collections.singletonList(createDataSlice(3)));
    Assert.assertTrue(controller.getInputUsedBytes() > baseline);

    // drop() discards the buffered batch without processing it: the charge must come back.
    engine.drop();
    Assert.assertEquals(baseline, controller.getInputUsedBytes());
  }

  @Test
  public void testDataChargeHeldWhileEventPinsStoreAndReleasedWhenConsumed() {
    CapacityWindowEngine engine = createEngine(3);
    long baseline = controller.getDataUsedBytes();

    // One batch exactly fills the window: the event is emitted and its iterator pins the store.
    engine.push(Collections.singletonList(createDataSlice(3)));
    IEventInfo event = engine.pollComputeEvent();
    Assert.assertNotNull(event);
    Assert.assertTrue(
        "a store pinned by an outstanding event iterator should keep its data charge",
        controller.getDataUsedBytes() > baseline);

    // Consuming the event to exhaustion releases the store's data charge.
    IEventRowsIterator iterator = event.getRowsIterator();
    Assert.assertNotNull(iterator.next());
    while (iterator.next() != null) {
      // Drain the remaining blocks.
    }
    Assert.assertTrue(iterator.isFinished());
    Assert.assertEquals(
        "the last owner releasing the store should return its data charge",
        baseline,
        controller.getDataUsedBytes());
  }

  @Test
  public void testUnconsumedQueuedEventsReleasedOnDrop() {
    CapacityWindowEngine engine = createEngine(1);
    long baseline = controller.getDataUsedBytes();

    // 50 single-row windows: each emitted close event's iterator pins its (already
    // engine-released) store, and nothing ever consumes the iterators (e.g. the calc plan
    // references no ${rows}, so no EventScanOperator drains them).
    for (int i = 0; i < 50; i++) {
      engine.push(Collections.singletonList(createDataSlice(1)));
    }
    Assert.assertTrue(
        "unconsumed event iterators should keep their stores charged",
        controller.getDataUsedBytes() > baseline);

    // drop() discards the still-queued events without consuming them: their iterators' pins must
    // be released, or the node-global data pool permanently loses ~24KB per window.
    engine.drop();
    Assert.assertEquals(baseline, controller.getDataUsedBytes());
  }

  @Test
  public void testForceCloseFinalEventsReleasedOnDrop() {
    CapacityWindowEngine engine = createEngine(10);
    long baseline = controller.getDataUsedBytes();

    // forceClose() emits the still-open window's final close event, whose iterator pins the
    // store. The driver that would have consumed it is being torn down, so the event stays queued.
    engine.push(Collections.singletonList(createDataSlice(3)));
    engine.forceClose();
    Assert.assertNotNull(engine.peekComputeEvent());

    engine.drop();
    Assert.assertEquals(
        "the never-consumed final event's iterator pin must be released on drop",
        baseline,
        controller.getDataUsedBytes());
  }

  @Test
  public void testOutputChargeHeldForOutstandingBlockAndReleasedOnNext() {
    CapacityWindowEngine engine = createEngine(3);
    engine.push(Collections.singletonList(createDataSlice(3)));
    IEventInfo event = engine.pollComputeEvent();
    Assert.assertNotNull(event);
    long baseline = controller.getOutputUsedBytes();

    IEventRowsIterator iterator = event.getRowsIterator();
    TsBlock block = iterator.next();
    Assert.assertNotNull(block);
    Assert.assertEquals(3, block.getPositionCount());
    Assert.assertTrue(
        "the materialized TsBlock should charge the output pool",
        controller.getOutputUsedBytes() > baseline);

    // The next call hands the outstanding block's charge back (one-outstanding-block contract)
    // and, the window being closed and its single range fully generated, returns null.
    Assert.assertNull(iterator.next());
    Assert.assertEquals(
        "asking for the next block should return the outstanding output charge",
        baseline,
        controller.getOutputUsedBytes());
  }

  @Test
  public void testInputExhaustionBackpressuresPushUntilRelease() throws Exception {
    CapacityWindowEngine engine = createEngine(10);
    engine.stop();
    long held = exhaustInputPool();
    try {
      // The batch is buffered (uncharged - reception must not lose data), but the producer is
      // handed an incomplete future: it must stop pushing until memory frees.
      ListenableFuture<?> blocked = engine.push(Collections.singletonList(createDataSlice(3)));
      Assert.assertNotNull(blocked);
      Assert.assertFalse("input exhaustion should block the producer", blocked.isDone());

      // Releasing input memory anywhere wakes the parked producer.
      Assert.assertTrue(controller.releaseInputMemory(held));
      await("the producer's future should complete on input release", blocked::isDone);
    } finally {
      controller.releaseInputMemory(held);
      engine.drop();
    }
  }

  @Test
  public void testDataExhaustionPausesProcessingUntilRelease() throws Exception {
    CapacityWindowEngine engine = createEngine(3);
    long held = exhaustDataPool();
    try {
      long inputBaseline = controller.getInputUsedBytes();
      // Data-pool exhaustion: the batch must NOT be processed (no event); it is buffered and
      // charged to the input pool instead.
      engine.push(Collections.singletonList(createDataSlice(3)));
      Assert.assertNull("no event while memory-paused", engine.peekComputeEvent());
      Assert.assertTrue("the engine should be memory-paused", engine.isMemoryPaused());
      Assert.assertTrue(
          "the batch buffered while memory-paused should charge the input pool",
          controller.getInputUsedBytes() > inputBaseline);

      // Releasing less than one batch's headroom cannot resume: the retry's headroom pre-check
      // keeps the pause and the batch stays buffered.
      Assert.assertTrue(controller.releaseDataMemory(8));
      Thread.sleep(300);
      Assert.assertNull("still paused while memory is insufficient", engine.peekComputeEvent());
      Assert.assertTrue(
          "an insufficient release should re-pause the engine", engine.isMemoryPaused());

      // Releasing the rest resumes processing: the buffered batch is drained and emits its event.
      Assert.assertTrue(controller.releaseDataMemory(held - 8));
      await(
          "the buffered batch should be processed after the release",
          () -> engine.peekComputeEvent() != null);
      Assert.assertFalse(engine.isMemoryPaused());
    } finally {
      controller.releaseDataMemory(held);
      engine.drop();
    }
  }

  @Test
  public void testOutputExhaustionPausesProcessingUntilRelease() throws Exception {
    CapacityWindowEngine engine = createEngine(3);
    long held = exhaustOutputPool();
    try {
      // The first window fills and its event is emitted; materializing its block cannot charge the
      // output pool, which memory-pauses the engine (the block itself is still returned).
      engine.push(Collections.singletonList(createDataSlice(3)));
      IEventInfo event = engine.pollComputeEvent();
      Assert.assertNotNull(event);
      IEventRowsIterator iterator = event.getRowsIterator();
      Assert.assertNotNull(iterator.next());
      await("output exhaustion on materialization should pause the engine", engine::isMemoryPaused);

      // While paused, further pushes are only buffered into the input pool - no new events.
      long inputBaseline = controller.getInputUsedBytes();
      engine.push(Collections.singletonList(createDataSlice(3)));
      Assert.assertNull("no new event while output-memory-paused", engine.peekComputeEvent());
      Assert.assertTrue(
          "pushes while output-memory-paused should buffer into the input pool",
          controller.getInputUsedBytes() > inputBaseline);

      // Releasing output memory resumes processing: the buffered batch is drained and emitted.
      Assert.assertTrue(controller.releaseOutputMemory(held));
      await(
          "the buffered batch should be processed after the release",
          () -> engine.peekComputeEvent() != null);
      Assert.assertFalse(engine.isMemoryPaused());
    } finally {
      controller.releaseOutputMemory(held);
      engine.drop();
    }
  }

  /** Reserves all free capacity of the input pool, returning the held bytes for later release. */
  private long exhaustInputPool() {
    long free = controller.getInputTotalBytes() - controller.getInputUsedBytes();
    Assert.assertTrue(controller.tryReserveInputMemory(free));
    return free;
  }

  /** Reserves all free capacity of the data pool, returning the held bytes for later release. */
  private long exhaustDataPool() {
    long free = controller.getDataTotalBytes() - controller.getDataUsedBytes();
    Assert.assertTrue(controller.tryReserveDataMemory(free));
    return free;
  }

  /** Reserves all free capacity of the output pool, returning the held bytes for later release. */
  private long exhaustOutputPool() {
    long free = controller.getOutputTotalBytes() - controller.getOutputUsedBytes();
    Assert.assertTrue(controller.tryReserveOutputMemory(free));
    return free;
  }

  /** Polls the condition every 10ms until it holds, failing the test after the timeout. */
  private static void await(String message, BooleanSupplier condition) throws InterruptedException {
    long deadline = System.currentTimeMillis() + AWAIT_TIMEOUT_MS;
    while (!condition.getAsBoolean()) {
      if (System.currentTimeMillis() > deadline) {
        Assert.fail("timed out waiting for: " + message);
      }
      Thread.sleep(10);
    }
  }
}
