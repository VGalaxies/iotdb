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

import org.apache.iotdb.commons.stream.ListPartitionKey;
import org.apache.iotdb.streamnode.engine.scheduler.task.StreamDriverContext;
import org.apache.iotdb.streamnode.engine.sink.CommitTracker;
import org.apache.iotdb.streamnode.engine.task.StreamSubTaskContext;
import org.apache.iotdb.streamnode.engine.task.StreamSubTaskStateMachine;
import org.apache.iotdb.streamnode.utils.IEventRowsIterator;

import org.apache.tsfile.read.common.block.TsBlock;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class WindowEngineTestUtils {

  public static StreamDriverContext createDriverContext() {
    StreamSubTaskContext subTaskContext =
        new StreamSubTaskContext(
            "test-stream",
            new ListPartitionKey(Collections.emptyList()),
            new StreamSubTaskStateMachine("test-stream-0", Runnable::run));
    subTaskContext.setWindowIdAllocator(new AtomicLong(0));
    return new StreamDriverContext(subTaskContext);
  }

  /**
   * Simulates the driver flushing every queued destroy marker through the commit tracker: each EOF
   * event carrying a real window id releases that window's commit gate. Markers without a window id
   * (e.g. period events) are skipped, exactly as the driver does.
   */
  public static void flushDestroyMarkers(WindowEngine engine, CommitTracker tracker) {
    IEventInfo event;
    while (engine.hasPendingRecomputeEvent()) {
      event = engine.pollRecomputeEvent();
      if (event.getWindowId() >= 0 && event.isEOF()) {
        tracker.onWindowDestroy(event.getWindowId());
      }
    }
  }

  public static void printWindowData(List<IEventInfo> events) {
    for (int eventIndex = 0; eventIndex < events.size(); eventIndex++) {
      IEventInfo event = events.get(eventIndex);
      System.out.println("window: " + eventIndex + " windowId: " + event.getWindowId());

      IEventRowsIterator eventRowsIterator = event.getRowsIterator();
      while (eventRowsIterator != null && !eventRowsIterator.isFinished()) {
        TsBlock tsBlock = eventRowsIterator.next();
        if (tsBlock == null) {
          // The iterator returns null between the TsBlocks of one recorded batch (and between
          // recorded ranges); keep pulling while more data can be generated, and stop when the
          // window is still open with nothing left to materialize (a not-done blocked future).
          if (!eventRowsIterator.isBlocked().isDone()) {
            break;
          }
          continue;
        }
        for (int j = 0; j < tsBlock.getPositionCount(); j++) {
          long timestamp = tsBlock.getTimeColumn().getLong(j);
          long value = tsBlock.getColumn(0).getLong(j);
          System.out.println("timestamp " + timestamp + " value " + value);
        }
      }
    }
  }
}
