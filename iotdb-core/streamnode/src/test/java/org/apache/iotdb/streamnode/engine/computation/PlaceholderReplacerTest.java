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

package org.apache.iotdb.streamnode.engine.computation;

import org.apache.iotdb.commons.stream.ListPartitionKey;
import org.apache.iotdb.commons.stream.PartitionKey;
import org.apache.iotdb.streamnode.engine.window.IEventInfo;
import org.apache.iotdb.streamnode.engine.window.WindowEventType;
import org.apache.iotdb.streamnode.utils.IEventRowsIterator;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Optional;
import java.util.OptionalLong;

public class PlaceholderReplacerTest {

  @Test
  public void testReplaceBracketPlaceholders() {
    PlaceholderReplacer replacer = new PlaceholderReplacer();
    ListPartitionKey key = new ListPartitionKey(Arrays.asList("d1", 7));
    IEventInfo event = new TestEventInfo(100, 200, 10, key);

    String sql = "select ${start_time}, ${END_TIME}, ${ROW_NUM}, ${1}, ${2} from table1";
    String result = replacer.replace(sql, event);

    Assert.assertEquals("select 100, 200, 10, 'd1', 7 from table1", result);
  }

  @Test
  public void testReplacePrevAndNextTimePlaceholders() {
    PlaceholderReplacer replacer = new PlaceholderReplacer();
    IEventInfo event = new TestEventInfo(100, 200, 10, OptionalLong.of(50), OptionalLong.of(300));

    String sql = "select ${prev_time}, ${PREV_TIME}, ${Next_Time} from table1";
    String result = replacer.replace(sql, event);

    Assert.assertEquals("select 50, 50, 300 from table1", result);
  }

  @Test
  public void testReplaceAbsentPrevAndNextTimeWithTypedNull() {
    PlaceholderReplacer replacer = new PlaceholderReplacer();
    // The fixture always carries a start and an end time; only prev_time/next_time can be absent.
    IEventInfo event = new TestEventInfo(100, 200, 10);

    String sql = "select ${start_time}, ${end_time}, ${prev_time}, ${next_time} from table1";
    String result = replacer.replace(sql, event);

    Assert.assertEquals(
        "select 100, 200, CAST(NULL AS TIMESTAMP), CAST(NULL AS TIMESTAMP) from table1", result);
  }

  @Test
  public void testReplaceAbsentRowNumWithTypedNull() {
    PlaceholderReplacer replacer = new PlaceholderReplacer();
    OptionalLong absent = OptionalLong.empty();
    IEventInfo event = new TestEventInfo(100, 200, absent, absent, absent);

    String sql = "select ${row_num}, ${1} from table1";
    String result = replacer.replace(sql, event);

    Assert.assertEquals("select CAST(NULL AS INT64), CAST(NULL AS STRING) from table1", result);
  }

  private static class TestEventInfo implements IEventInfo {

    private final long startTime;
    private final long endTime;
    private final OptionalLong rowCount;
    private final OptionalLong prevTime;
    private final OptionalLong nextTime;
    private final PartitionKey partitionKey;

    private TestEventInfo(long startTime, long endTime, long rowCount) {
      this(
          startTime,
          endTime,
          OptionalLong.of(rowCount),
          OptionalLong.empty(),
          OptionalLong.empty(),
          null);
    }

    private TestEventInfo(long startTime, long endTime, long rowCount, PartitionKey partitionKey) {
      this(
          startTime,
          endTime,
          OptionalLong.of(rowCount),
          OptionalLong.empty(),
          OptionalLong.empty(),
          partitionKey);
    }

    private TestEventInfo(
        long startTime, long endTime, long rowCount, OptionalLong prevTime, OptionalLong nextTime) {
      this(startTime, endTime, OptionalLong.of(rowCount), prevTime, nextTime, null);
    }

    private TestEventInfo(
        long startTime,
        long endTime,
        OptionalLong rowCount,
        OptionalLong prevTime,
        OptionalLong nextTime) {
      this(startTime, endTime, rowCount, prevTime, nextTime, null);
    }

    private TestEventInfo(
        long startTime,
        long endTime,
        OptionalLong rowCount,
        OptionalLong prevTime,
        OptionalLong nextTime,
        PartitionKey partitionKey) {
      this.startTime = startTime;
      this.endTime = endTime;
      this.rowCount = rowCount;
      this.prevTime = prevTime;
      this.nextTime = nextTime;
      this.partitionKey = partitionKey;
    }

    @Override
    public WindowEventType getEventType() {
      return WindowEventType.WINDOW_CLOSE;
    }

    @Override
    public OptionalLong getStartTime() {
      return OptionalLong.of(startTime);
    }

    @Override
    public OptionalLong getEndTime() {
      return OptionalLong.of(endTime);
    }

    @Override
    public OptionalLong getPrevTime() {
      return prevTime;
    }

    @Override
    public OptionalLong getNextTime() {
      return nextTime;
    }

    @Override
    public OptionalLong getRowCount() {
      return rowCount;
    }

    @Override
    public Optional<Object> getPrevValue() {
      return Optional.empty();
    }

    @Override
    public Optional<Object> getCurrentValue() {
      return Optional.empty();
    }

    @Override
    public Optional<Object> getN(int n) {
      if (partitionKey == null) {
        return Optional.empty();
      }
      return Optional.ofNullable(partitionKey.segmentValue(n - 1));
    }

    @Override
    public long getWindowId() {
      return -1;
    }

    @Override
    public boolean needsCompute() {
      return true;
    }

    @Override
    public IEventRowsIterator getRowsIterator() {
      return null;
    }

    @Override
    public boolean needRetry() {
      return false;
    }

    @Override
    public void handleComputeException(Throwable cause) {}
  }
}
