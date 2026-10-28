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

package org.apache.iotdb.streamnode.engine.window;

import org.apache.iotdb.commons.stream.StreamProperties;
import org.apache.iotdb.commons.stream.TumbleWindow;
import org.apache.iotdb.streamnode.engine.task.StreamSubTask.DataSlice;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.iotdb.streamnode.engine.window.WindowEngineTestUtils.createDriverContext;

public class WindowEngineTickSchedulerTest {

  private static final long WINDOW_SIZE_MS = 1000;

  /**
   * Waits for at least {@code minCount} events, draining them as they appear, then drains any
   * remaining.
   */
  private List<IEventInfo> awaitEvents(WindowEngine engine, int minCount) {
    List<IEventInfo> events = new ArrayList<>();
    long deadline = System.currentTimeMillis() + 5000;
    try {
      while (System.currentTimeMillis() < deadline && events.size() < minCount) {
        // A force-closed window emits its close event (compute queue) and its destroy marker
        // (recompute queue) together; collect both.
        IEventInfo event = engine.pollComputeEvent();
        if (event != null) {
          events.add(event);
        }
        IEventInfo reEvent = engine.pollRecomputeEvent();
        if (reEvent != null) {
          events.add(reEvent);
        }
        if (events.size() < minCount) {
          Thread.sleep(5);
        }
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    while (engine.hasPendingComputeEvent()) {
      events.add(engine.pollComputeEvent());
    }
    while (engine.hasPendingRecomputeEvent()) {
      events.add(engine.pollRecomputeEvent());
    }
    return events;
  }

  /** Waits {@code waitMs} and asserts no event appeared in the meantime. */
  private void assertNoEventsShortly(WindowEngine engine, long waitMs) {
    long deadline = System.currentTimeMillis() + waitMs;
    try {
      while (System.currentTimeMillis() < deadline) {
        Assert.assertFalse(engine.hasPendingComputeEvent());
        Assert.assertFalse(engine.hasPendingRecomputeEvent());
        Thread.sleep(5);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    Assert.assertFalse(engine.hasPendingComputeEvent());
    Assert.assertFalse(engine.hasPendingRecomputeEvent());
  }

  private TumbleWindowEngine createEngine(
      WindowEngineTickScheduler scheduler, StreamProperties props) {
    TumbleWindow window = new TumbleWindow(WINDOW_SIZE_MS, 0);
    window.setEventInputColumns(Collections.singletonList("time"));
    window.setContainsRowsPlaceholder(true);
    return new TumbleWindowEngine(window, createDriverContext(), props, scheduler);
  }

  private DataSlice createDataSlice(long[] timestamps) {
    Tablet tablet =
        new Tablet(
            "root.test.device",
            Collections.singletonList(new MeasurementSchema("time", TSDataType.INT64)),
            timestamps.length);
    for (int i = 0; i < timestamps.length; i++) {
      tablet.getTimestamps()[i] = timestamps[i];
      tablet.addValue(i, "time", timestamps[i]);
    }
    tablet.setRowSize(timestamps.length);
    return new DataSlice(null, tablet, 0, timestamps.length, -1L);
  }

  @Test
  public void testSharedSchedulerTicksAllEnginesOfTask() {
    // One scheduler shared by two engines (as two devices / partitions of one stream task): the
    // MAX_DELAY_WINDOW_CLOSE system-time trigger of BOTH engines is driven by the single shared
    // thread, even though no further data arrives to advance any watermark.
    WindowEngineTickScheduler scheduler = new WindowEngineTickScheduler("test-task");
    try {
      StreamProperties props = new StreamProperties(0, 0, 100, 0, true);
      TumbleWindowEngine engine1 = createEngine(scheduler, props);
      TumbleWindowEngine engine2 = createEngine(scheduler, props);

      scheduler.start();
      engine1.push(Collections.singletonList(createDataSlice(new long[] {100L})));
      engine2.push(Collections.singletonList(createDataSlice(new long[] {100L})));
      // push() processes inline (on this thread), so the windows open - and their system-time
      // close deadlines are armed - before the shared scheduler ticks them.

      List<IEventInfo> events1 = awaitEvents(engine1, 2);
      // allowedLateness=0: the tick closes the window and destroys it immediately, emitting the
      // close event followed by its compute-free destroy marker.
      Assert.assertEquals(2, events1.size());
      Assert.assertEquals(WindowEventType.WINDOW_CLOSE, events1.get(0).getEventType());
      Assert.assertEquals(WindowEventType.WINDOW_DESTROY, events1.get(1).getEventType());
      Assert.assertEquals(0L, events1.get(0).getStartTime().getAsLong());
      Assert.assertEquals(1000L, events1.get(0).getEndTime().getAsLong());

      List<IEventInfo> events2 = awaitEvents(engine2, 2);
      Assert.assertEquals(2, events2.size());
      Assert.assertEquals(WindowEventType.WINDOW_CLOSE, events2.get(0).getEventType());
      Assert.assertEquals(WindowEventType.WINDOW_DESTROY, events2.get(1).getEventType());
    } finally {
      scheduler.stop();
    }
  }

  @Test
  public void testPausedEngineIsNotTicked() {
    // While an engine is paused (stop()), the scheduler's ticks must not fire its system-time
    // triggers; resuming (start()) re-enables them.
    WindowEngineTickScheduler scheduler = new WindowEngineTickScheduler("test-task");
    try {
      StreamProperties props = new StreamProperties(0, 0, 100, 0, true);
      TumbleWindowEngine engine = createEngine(scheduler, props);
      scheduler.start();

      // Open a window (closeSystemTime = now + 100), then pause the engine.
      engine.push(Collections.singletonList(createDataSlice(new long[] {100L})));
      engine.stop();
      // The deadline passes while paused: no forced close may fire.
      assertNoEventsShortly(engine, 400);

      // Resuming re-enables ticks; the now-overdue window is force-closed by the shared thread.
      engine.start();
      List<IEventInfo> events = awaitEvents(engine, 2);
      Assert.assertEquals(2, events.size());
      Assert.assertEquals(WindowEventType.WINDOW_CLOSE, events.get(0).getEventType());
      Assert.assertEquals(WindowEventType.WINDOW_DESTROY, events.get(1).getEventType());
    } finally {
      scheduler.stop();
    }
  }

  @Test
  public void testDroppedEngineIsNotTicked() {
    // drop() unregisters the engine: its system-time triggers no longer fire, and its remaining
    // state is cleared.
    WindowEngineTickScheduler scheduler = new WindowEngineTickScheduler("test-task");
    try {
      StreamProperties props = new StreamProperties(0, 0, 100, 0, true);
      TumbleWindowEngine engine = createEngine(scheduler, props);
      scheduler.start();

      engine.push(Collections.singletonList(createDataSlice(new long[] {100L})));
      engine.drop();
      assertNoEventsShortly(engine, 400);
    } finally {
      scheduler.stop();
    }
  }

  @Test
  public void testStopHaltsTicks() {
    // After scheduler.stop() no ticks fire at all; an explicit engine.tick() still works.
    WindowEngineTickScheduler scheduler = new WindowEngineTickScheduler("test-task");
    StreamProperties props = new StreamProperties(0, 0, 100, 0, true);
    TumbleWindowEngine engine = createEngine(scheduler, props);
    scheduler.start();
    engine.push(Collections.singletonList(createDataSlice(new long[] {100L})));
    scheduler.stop();

    // The close deadline passes, but the stopped scheduler never fires it.
    assertNoEventsShortly(engine, 400);
    // A manual tick proves the trigger still works; only the thread is gone.
    engine.tick();
    List<IEventInfo> events = awaitEvents(engine, 2);
    Assert.assertEquals(2, events.size());
    Assert.assertEquals(WindowEventType.WINDOW_CLOSE, events.get(0).getEventType());
    Assert.assertEquals(WindowEventType.WINDOW_DESTROY, events.get(1).getEventType());
  }

  @Test
  public void testStartAndStopAreIdempotent() {
    WindowEngineTickScheduler scheduler = new WindowEngineTickScheduler("test-task");
    scheduler.start();
    scheduler.start();
    scheduler.stop();
    scheduler.stop();
    // A scheduler that was stopped cannot be restarted (the owning task is being dropped).
    scheduler.start();
    StreamProperties props = new StreamProperties(0, 0, 100, 0, true);
    TumbleWindowEngine engine = createEngine(scheduler, props);
    engine.push(Collections.singletonList(createDataSlice(new long[] {100L})));
    assertNoEventsShortly(engine, 300);
  }
}
