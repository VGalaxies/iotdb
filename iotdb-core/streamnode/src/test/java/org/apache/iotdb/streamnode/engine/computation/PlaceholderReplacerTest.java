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
import org.apache.iotdb.streamnode.engine.window.IEventInfo;
import org.apache.iotdb.streamnode.utils.IEventRowsIterator;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

public class PlaceholderReplacerTest {

  @Test
  public void testReplaceBracketPlaceholders() {
    PlaceholderReplacer replacer = new PlaceholderReplacer();
    IEventInfo event = new TestEventInfo(100, 200, 10);
    ListPartitionKey key = new ListPartitionKey(Arrays.asList("d1", 7));

    String sql = "select ${start_time}, ${END_TIME}, ${ROW_NUM}, ${1}, ${2} from table1";
    String result = replacer.replace(sql, event, key);

    Assert.assertEquals("select 100, 200, 10, 'd1', 7 from table1", result);
  }

  private static class TestEventInfo implements IEventInfo {

    private final long startTime;
    private final long endTime;
    private final long rowCount;

    private TestEventInfo(long startTime, long endTime, long rowCount) {
      this.startTime = startTime;
      this.endTime = endTime;
      this.rowCount = rowCount;
    }

    @Override
    public boolean isClosed() {
      return true;
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
      return OptionalLong.empty();
    }

    @Override
    public OptionalLong getNextTime() {
      return OptionalLong.empty();
    }

    @Override
    public OptionalLong getRowCount() {
      return OptionalLong.of(rowCount);
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
      return Optional.empty();
    }

    @Override
    public List<Long> getCommitIds() {
      return Collections.emptyList();
    }

    @Override
    public IEventRowsIterator getRowsIterator() {
      return null;
    }
  }
}
