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

import org.apache.iotdb.commons.stream.StreamProperties;
import org.apache.iotdb.commons.stream.VariationWindow;
import org.apache.iotdb.streamnode.engine.scheduler.task.StreamDriverContext;
import org.apache.iotdb.streamnode.engine.sink.CommitTracker;
import org.apache.iotdb.streamnode.engine.task.StreamSubTask.DataSlice;
import org.apache.iotdb.streamnode.utils.IEventRowsIterator;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class VariationWindowEngineTest {

  private static DataSlice doubleSlice(long[] ts, double[] vals, long tabletId) {
    Tablet tablet =
        new Tablet(
            "root.test.d1",
            Arrays.asList(
                new MeasurementSchema("time", TSDataType.INT64),
                new MeasurementSchema("s1", TSDataType.DOUBLE)),
            ts.length);
    for (int i = 0; i < ts.length; i++) {
      tablet.getTimestamps()[i] = ts[i];
      tablet.addValue(i, "time", ts[i]);
      tablet.addValue(i, "s1", vals[i]);
    }
    tablet.setRowSize(ts.length);
    return new DataSlice(null, tablet, 0, ts.length, tabletId);
  }

  private static DataSlice intSlice(long[] ts, int[] vals, long tabletId) {
    Tablet tablet =
        new Tablet(
            "root.test.d1",
            Arrays.asList(
                new MeasurementSchema("time", TSDataType.INT64),
                new MeasurementSchema("s1", TSDataType.INT32)),
            ts.length);
    for (int i = 0; i < ts.length; i++) {
      tablet.getTimestamps()[i] = ts[i];
      tablet.addValue(i, "time", ts[i]);
      tablet.addValue(i, "s1", vals[i]);
    }
    tablet.setRowSize(ts.length);
    return new DataSlice(null, tablet, 0, ts.length, tabletId);
  }

  private static VariationWindow newWindow() {
    VariationWindow window = new VariationWindow("s1", 2.0);
    window.setEventInputColumns(Arrays.asList("time", "s1"));
    window.setEventInputColumnTypes(Arrays.asList(TSDataType.TIMESTAMP, TSDataType.DOUBLE));
    window.setContainsRowsPlaceholder(true);
    return window;
  }

  private static List<Long> extractTimestamps(IEventInfo event) {
    List<Long> timestamps = new ArrayList<>();
    IEventRowsIterator it = event.getRowsIterator();
    while (it != null && !it.isFinished()) {
      TsBlock block = it.next();
      if (block != null) {
        for (int i = 0; i < block.getPositionCount(); i++) {
          timestamps.add(block.getColumn(0).getLong(i));
        }
      }
    }
    return timestamps;
  }

  @Test
  public void testEvidenceSpikeSuppressionAndExactOnsetBacktracking() {
    StreamProperties props = new StreamProperties();
    props.putExtraProperty(StreamProperties.VARIATION_DETECTOR, "evidence");
    props.putExtraProperty(StreamProperties.VARIATION_THETA, "4.0");
    props.putExtraProperty(StreamProperties.VARIATION_EPSILON, "2.0");
    props.putExtraProperty(StreamProperties.VARIATION_TAU_MS, "5000");
    props.putExtraProperty(StreamProperties.VARIATION_DT_MAX_MS, "2000");
    props.putExtraProperty(StreamProperties.VARIATION_MIN_SAMPLES, "3");

    VariationWindowEngine engine =
        (VariationWindowEngine)
            WindowEngine.create(
                newWindow(), WindowEngineTestUtils.createDriverContext(), props, null);

    // 0..900ms steady at 10.0, with one isolated outlier spike 80.0 at 500ms, then sustained step
    // to 25.0 starting at 1000ms.
    long[] ts = {0, 100, 200, 300, 400, 500, 600, 700, 800, 900, 1000, 1100, 1200, 1300};
    double[] vals = {
      10.0, 10.2, 9.9, 10.1, 10.0, 80.0, 10.1, 9.8, 10.0, 10.2, 25.0, 25.2, 24.9, 25.1
    };
    engine.push(Collections.singletonList(doubleSlice(ts, vals, 1L)));

    IEventInfo closed = engine.pollComputeEvent();
    Assert.assertNotNull(closed);
    // Spike at 500ms must NOT split the window; backtracking must cut right before onset 1000ms.
    Assert.assertEquals(0L, closed.getStartTime().getAsLong());
    Assert.assertEquals(900L, closed.getEndTime().getAsLong());
    Assert.assertEquals(1000L, closed.getNextTime().getAsLong());
    Assert.assertEquals(10L, closed.getRowCount().getAsLong());
    Assert.assertEquals(10.0, (Double) closed.getPrevValue().get(), 0.5);
    Assert.assertEquals(25.0, (Double) closed.getCurrentValue().get(), 1e-6);
    Assert.assertEquals(
        Arrays.asList(0L, 100L, 200L, 300L, 400L, 500L, 600L, 700L, 800L, 900L),
        extractTimestamps(closed));
    Assert.assertNull(engine.pollComputeEvent());

    // Remaining suffix rows [1000..1300] flush cleanly on forceClose as the second window.
    engine.forceClose();
    IEventInfo second = engine.pollComputeEvent();
    Assert.assertNotNull(second);
    Assert.assertEquals(1000L, second.getStartTime().getAsLong());
    Assert.assertEquals(1300L, second.getEndTime().getAsLong());
    Assert.assertEquals(4L, second.getRowCount().getAsLong());
  }

  @Test
  public void testAdaptiveMedianMadAbsorbsHighJitterBeforeTrueShift() {
    StreamProperties props = new StreamProperties();
    props.putExtraProperty(StreamProperties.VARIATION_DETECTOR, "evidence");
    props.putExtraProperty(StreamProperties.VARIATION_THETA, "5.0");
    props.putExtraProperty(StreamProperties.VARIATION_EPSILON, "1.0");
    props.putExtraProperty(StreamProperties.VARIATION_TAU_MS, "5000");
    props.putExtraProperty(StreamProperties.VARIATION_MIN_SAMPLES, "3");
    props.putExtraProperty(StreamProperties.VARIATION_ADAPTIVE_BASELINE, "true");

    VariationWindowEngine engine =
        (VariationWindowEngine)
            WindowEngine.create(
                newWindow(), WindowEngineTestUtils.createDriverContext(), props, null);

    // High-jitter steady regime around 100 +/- 4 (well above static eps=1.0), then true jump to 135
    long[] ts = new long[20];
    double[] vals = new double[20];
    for (int i = 0; i < 15; i++) {
      ts[i] = i * 500L;
      vals[i] = 100.0 + ((i % 2 == 0) ? 3.5 : -3.5);
    }
    for (int i = 15; i < 20; i++) {
      ts[i] = i * 500L;
      vals[i] = 135.0;
    }
    engine.push(Collections.singletonList(doubleSlice(ts, vals, 1L)));

    IEventInfo closed = engine.pollComputeEvent();
    Assert.assertNotNull(closed);
    Assert.assertEquals(0L, closed.getStartTime().getAsLong());
    // Must not fragment during high jitter [0..7000]
    Assert.assertTrue(closed.getRowCount().getAsLong() >= 14L);
    Assert.assertNull(engine.pollComputeEvent());
  }

  @Test
  public void testPendingBufferOverflowForceCloseAndMergeBackPolicies() {
    // 1. force_close overflow when candidate exceeds buffer_max_rows = 2
    StreamProperties fcProps = new StreamProperties();
    fcProps.putExtraProperty(StreamProperties.VARIATION_DETECTOR, "duration");
    fcProps.putExtraProperty(StreamProperties.VARIATION_MIN_SAMPLES, "10");
    fcProps.putExtraProperty(StreamProperties.VARIATION_BUFFER_MAX_ROWS, "2");
    fcProps.putExtraProperty(StreamProperties.VARIATION_OVERFLOW_POLICY, "force_close");

    VariationWindowEngine fcEngine =
        (VariationWindowEngine)
            WindowEngine.create(
                newWindow(), WindowEngineTestUtils.createDriverContext(), fcProps, null);
    fcEngine.push(
        Collections.singletonList(
            doubleSlice(new long[] {0, 100, 200, 300}, new double[] {10.0, 10.0, 20.0, 20.0}, 1L)));
    IEventInfo fcClosed = fcEngine.pollComputeEvent();
    Assert.assertNotNull(fcClosed);
    Assert.assertEquals(2L, fcClosed.getRowCount().getAsLong());

    // 2. merge_back overflow folds buffered candidate rows back into active window
    StreamProperties mbProps = new StreamProperties();
    mbProps.putExtraProperty(StreamProperties.VARIATION_DETECTOR, "duration");
    mbProps.putExtraProperty(StreamProperties.VARIATION_MIN_SAMPLES, "10");
    mbProps.putExtraProperty(StreamProperties.VARIATION_BUFFER_MAX_ROWS, "2");
    mbProps.putExtraProperty(StreamProperties.VARIATION_OVERFLOW_POLICY, "merge_back");

    VariationWindowEngine mbEngine =
        (VariationWindowEngine)
            WindowEngine.create(
                newWindow(), WindowEngineTestUtils.createDriverContext(), mbProps, null);
    mbEngine.push(
        Collections.singletonList(
            doubleSlice(new long[] {0, 100, 200, 300}, new double[] {10.0, 10.0, 20.0, 20.0}, 1L)));
    Assert.assertNull(mbEngine.pollComputeEvent());
    mbEngine.forceClose();
    IEventInfo mbClosed = mbEngine.pollComputeEvent();
    Assert.assertNotNull(mbClosed);
    Assert.assertEquals(4L, mbClosed.getRowCount().getAsLong());
  }

  @Test
  public void testCommitTrackerSafetyAcrossPrefixConfirmHandoff() {
    AtomicLong committedTablet = new AtomicLong(-1L);
    CommitTracker tracker = new CommitTracker(committedTablet::set);
    StreamDriverContext driverContext = WindowEngineTestUtils.createDriverContext();
    driverContext.getSubTaskContext().setCommitTracker(tracker);

    StreamProperties props = new StreamProperties();
    props.putExtraProperty(StreamProperties.VARIATION_DETECTOR, "evidence");
    props.putExtraProperty(StreamProperties.VARIATION_THETA, "1.0");
    props.putExtraProperty(StreamProperties.VARIATION_EPSILON, "1.0");
    props.putExtraProperty(StreamProperties.VARIATION_MIN_SAMPLES, "3");

    VariationWindowEngine engine =
        (VariationWindowEngine) WindowEngine.create(newWindow(), driverContext, props, null);

    // Slice 1 (tabletId=1): rows 0, 1 steady, row 2 begins candidate excursion!
    tracker.registerCommitId(1L, 1);
    engine.push(
        Collections.singletonList(
            doubleSlice(new long[] {0, 100, 200}, new double[] {10.0, 10.0, 30.0}, 1L)));
    // Slice 2 (tabletId=2): rows 300, 400 confirm shift (backtracked to row 2 from tabletId=1!)
    tracker.registerCommitId(2L, 1);
    engine.push(
        Collections.singletonList(
            doubleSlice(new long[] {300, 400}, new double[] {30.0, 30.0}, 2L)));

    // Drain only window 0's destroy marker: tablet 1 MUST NOT commit yet because row 2 (tablet 1)
    // moved into window 1 which is still open!
    IEventInfo destroy0 = engine.pollRecomputeEvent();
    Assert.assertNotNull(destroy0);
    tracker.onWindowDestroy(destroy0.getWindowId());
    Assert.assertEquals(-1L, committedTablet.get());

    // Closing & destroying window 1 releases both tablet 1 and tablet 2 safely!
    engine.forceClose();
    WindowEngineTestUtils.flushDestroyMarkers(engine, tracker);
    Assert.assertEquals(2L, committedTablet.get());
  }

  @Test
  public void testPluggableDetectorsAndBoxedInt32PlaceholderTypes() {
    for (String detector : Arrays.asList("delta", "duration", "adwin", "optwin", "evidence")) {
      StreamProperties props = new StreamProperties();
      props.putExtraProperty(StreamProperties.VARIATION_DETECTOR, detector);
      props.putExtraProperty(StreamProperties.VARIATION_THETA, "2.0");
      props.putExtraProperty(StreamProperties.VARIATION_MIN_SAMPLES, "2");

      VariationWindow intWin = new VariationWindow("s1", 2.0);
      intWin.setEventInputColumns(Arrays.asList("time", "s1"));
      intWin.setEventInputColumnTypes(Arrays.asList(TSDataType.TIMESTAMP, TSDataType.INT32));
      intWin.setContainsRowsPlaceholder(true);

      VariationWindowEngine engine =
          (VariationWindowEngine)
              WindowEngine.create(intWin, WindowEngineTestUtils.createDriverContext(), props, null);

      long[] ts = new long[10];
      int[] vals = {10, 10, 10, 10, 10, 50, 50, 50, 50, 50};
      for (int i = 0; i < 10; i++) {
        ts[i] = i * 500L;
      }
      engine.push(Collections.singletonList(intSlice(ts, vals, 1L)));

      IEventInfo event = engine.pollComputeEvent();
      Assert.assertNotNull("Expected split for detector=" + detector, event);
      Assert.assertTrue(event.getPrevValue().get() instanceof Integer);
      Assert.assertTrue(event.getCurrentValue().get() instanceof Integer);
      Assert.assertEquals(10, event.getPrevValue().get());
      Assert.assertEquals(50, event.getCurrentValue().get());
    }
  }
}
