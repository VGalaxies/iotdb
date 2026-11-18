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

import org.apache.iotdb.commons.stream.TumbleWindow;
import org.apache.iotdb.streamnode.engine.scheduler.task.StreamDriverContext;
import org.apache.iotdb.streamnode.engine.task.StreamSubTask.DataSlice;
import org.apache.iotdb.streamnode.engine.window.distributed.PartitionProgressTracker;
import org.apache.iotdb.streamnode.engine.window.distributed.PartitionProgressTracker.CloseDecision;
import org.apache.iotdb.streamnode.engine.window.distributed.PartitionProgressTracker.CloseDecision.Action;
import org.apache.iotdb.streamnode.engine.window.distributed.PartitionProgressTracker.CloseDecision.WaitReason;
import org.apache.iotdb.streamnode.engine.window.distributed.PartitionProgressTracker.FragmentState;
import org.apache.iotdb.streamnode.engine.window.distributed.PartitionProgressTracker.PartitionId;
import org.apache.iotdb.streamnode.engine.window.distributed.PartitionProgressTracker.WindowRecord;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

public class PartitionProgressTrackerTest {

  @Test
  public void testAlgorithm41CloseDecisionInvariants() {
    PartitionProgressTracker tracker = new PartitionProgressTracker();
    PartitionId fast = new PartitionId(1, "DataRegion[1]");
    PartitionId slow = new PartitionId(2, "DataRegion[2]");

    // 1. Stale epoch -> REJECT(STALE_EPOCH)
    Map<PartitionId, FragmentState> readyMap = new LinkedHashMap<>();
    readyMap.put(fast, new FragmentState(2000L, 10L, true, false, 100L));
    readyMap.put(slow, new FragmentState(2000L, 5L, true, false, 100L));
    WindowRecord staleEpochRecord = new WindowRecord(1L, 0L, 1000L, 1, 1L, 1L, readyMap);
    CloseDecision dEpoch = tracker.evaluateCloseDecision(staleEpochRecord, 2L, 1L);
    Assert.assertEquals(Action.REJECT, dEpoch.getAction());
    Assert.assertEquals(WaitReason.STALE_EPOCH, dEpoch.getWaitReason());

    // 2. Member version mismatch -> WAIT(MEMBER_VERSION_MISMATCH)
    CloseDecision dVer = tracker.evaluateCloseDecision(staleEpochRecord, 1L, 2L);
    Assert.assertEquals(Action.WAIT, dVer.getAction());
    Assert.assertEquals(WaitReason.MEMBER_VERSION_MISMATCH, dVer.getWaitReason());

    // 3. Fast ahead, slow lagging -> WAIT(FRAGMENT_PROGRESS_LAGGING)
    Map<PartitionId, FragmentState> skewMap = new LinkedHashMap<>();
    skewMap.put(fast, new FragmentState(5000L, 20L, true, false, 100L));
    skewMap.put(slow, new FragmentState(800L, 2L, true, false, 100L));
    WindowRecord skewRecord = new WindowRecord(2L, 0L, 1000L, 1, 1L, 1L, skewMap);
    CloseDecision dLag = tracker.evaluateCloseDecision(skewRecord, 1L, 1L);
    Assert.assertEquals(Action.WAIT, dLag.getAction());
    Assert.assertEquals(WaitReason.FRAGMENT_PROGRESS_LAGGING, dLag.getWaitReason());
    Assert.assertEquals(slow, dLag.getBlockingPartition());

    // 4. Slow has progress >= endTime & rowCount > 0, but stateReady == false -> WAIT(NOT_READY)
    Map<PartitionId, FragmentState> notReadyMap = new LinkedHashMap<>();
    notReadyMap.put(fast, new FragmentState(2000L, 20L, true, false, 100L));
    notReadyMap.put(slow, new FragmentState(1500L, 3L, false, false, 100L));
    WindowRecord notReadyRecord = new WindowRecord(3L, 0L, 1000L, 1, 1L, 1L, notReadyMap);
    CloseDecision dNotReady = tracker.evaluateCloseDecision(notReadyRecord, 1L, 1L);
    Assert.assertEquals(Action.WAIT, dNotReady.getAction());
    Assert.assertEquals(WaitReason.FRAGMENT_STATE_NOT_READY, dNotReady.getWaitReason());
    Assert.assertEquals(slow, dNotReady.getBlockingPartition());

    // 5. Idle partition silent (0 rows, emptyDeclared == false, never reported) -> WAIT(SILENT)
    Map<PartitionId, FragmentState> silentMap = new LinkedHashMap<>();
    silentMap.put(fast, new FragmentState(2000L, 20L, true, false, 100L));
    silentMap.put(slow, new FragmentState(Long.MIN_VALUE, 0L, false, false, 0L));
    WindowRecord silentRecord = new WindowRecord(4L, 0L, 1000L, 1, 1L, 1L, silentMap);
    CloseDecision dSilent = tracker.evaluateCloseDecision(silentRecord, 1L, 1L);
    Assert.assertEquals(Action.WAIT, dSilent.getAction());
    Assert.assertEquals(WaitReason.FRAGMENT_SILENT, dSilent.getWaitReason());
    Assert.assertEquals(slow, dSilent.getBlockingPartition());

    // 6. Idle partition explicitly declares empty via heartbeat >= endTime -> CLOSE
    Map<PartitionId, FragmentState> emptyOkMap = new LinkedHashMap<>();
    emptyOkMap.put(fast, new FragmentState(2000L, 20L, true, false, 100L));
    emptyOkMap.put(slow, new FragmentState(1000L, 0L, false, true, 100L));
    WindowRecord emptyOkRecord = new WindowRecord(5L, 0L, 1000L, 1, 1L, 1L, emptyOkMap);
    CloseDecision dClose = tracker.evaluateCloseDecision(emptyOkRecord, 1L, 1L);
    Assert.assertEquals(Action.CLOSE, dClose.getAction());
    Assert.assertEquals(WaitReason.NONE, dClose.getWaitReason());
  }

  @Test
  public void testAlignedProgressStaleRegionEviction() {
    PartitionProgressTracker tracker = new PartitionProgressTracker(1000L);
    long t0 = System.currentTimeMillis();
    tracker.onDataBatch(1, "DataRegion[1]", 5000L, 10L, t0);
    tracker.onHeartbeat(2, "DataRegion[2]", 1000L, t0 - 2000L);
    // Partition 2 updated 2s ago (> 1s staleTimeoutMs), so only active Partition 1 remains -> 5000
    Assert.assertEquals(5000L, tracker.computeAlignedProgress(t0));

    // Refresh Partition 2 heartbeat at now -> min(5000, 1200) = 1200
    tracker.onHeartbeat(2, "DataRegion[2]", 1200L, t0);
    Assert.assertEquals(1200L, tracker.computeAlignedProgress(t0));
  }

  @Test
  public void testTumbleEngineHoldsFastPartitionUntilSlowHeartbeatAdvances() {
    PartitionProgressTracker tracker = new PartitionProgressTracker(60_000L);
    StreamDriverContext driverContext = WindowEngineTestUtils.createDriverContext();
    driverContext.getSubTaskContext().setPartitionProgressTracker(tracker);

    TumbleWindow window = new TumbleWindow(1000L, 0L);
    window.setEventInputColumns(Collections.singletonList("time"));
    window.setContainsRowsPlaceholder(true);
    TumbleWindowEngine engine = new TumbleWindowEngine(window, driverContext, null, null);

    long now = System.currentTimeMillis();
    // Fast partition is at 5000, but slow partition is still at 1000.
    tracker.onDataBatch(1, "DataRegion[1]", 5000L, 2L, now);
    tracker.onHeartbeat(2, "DataRegion[2]", 1000L, now);

    Tablet tablet =
        new Tablet(
            "root.test.d1",
            Arrays.asList(
                new MeasurementSchema("time", TSDataType.INT64),
                new MeasurementSchema("value", TSDataType.INT64)),
            2);
    tablet.getTimestamps()[0] = 500L;
    tablet.addValue(0, "time", 500L);
    tablet.addValue(0, "value", 10L);
    tablet.getTimestamps()[1] = 3500L;
    tablet.addValue(1, "time", 3500L);
    tablet.addValue(1, "value", 20L);
    tablet.setRowSize(2);

    engine.push(Collections.singletonList(new DataSlice(null, tablet, 0, 2, 1L)));

    // Window [0, 1000) can close (alignedProgress == 1000), while [3000, 4000) stays open!
    IEventInfo first = engine.pollComputeEvent();
    Assert.assertNotNull(first);
    Assert.assertEquals(0L, first.getStartTime().getAsLong());
    Assert.assertEquals(1000L, first.getEndTime().getAsLong());
    Assert.assertNull(engine.pollComputeEvent());

    // Now slow partition advances via empty-declared heartbeat to 4500.
    tracker.onHeartbeat(2, "DataRegion[2]", 4500L, System.currentTimeMillis());
    engine.onTick();

    IEventInfo second = engine.pollComputeEvent();
    Assert.assertNotNull(second);
    Assert.assertEquals(3000L, second.getStartTime().getAsLong());
    Assert.assertEquals(4000L, second.getEndTime().getAsLong());
    Assert.assertEquals(1L, second.getRowCount().getAsLong());
  }
}
