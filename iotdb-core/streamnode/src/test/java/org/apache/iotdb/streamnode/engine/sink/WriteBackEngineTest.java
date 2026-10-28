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
import org.apache.iotdb.commons.stream.ListPartitionKey;
import org.apache.iotdb.commons.stream.StreamTask;
import org.apache.iotdb.streamnode.engine.task.StreamSubTaskContext;

import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;

public class WriteBackEngineTest {

  @Before
  public void setUp() {
    // Writer pool is started by StreamNode/StreamTaskManager in production; unit tests start it
    // explicitly because WriteBackEngine no longer owns the global pool lifecycle.
    GlobalSinkWriterPool.getInstance().start();
  }

  private StreamTask createTestStreamTask() {
    StreamTask task = Mockito.mock(StreamTask.class);
    Mockito.when(task.getTaskName()).thenReturn("test_stream");
    Mockito.when(task.getTarget())
        .thenReturn(
            new IoTDBTarget(
                "127.0.0.1:6667",
                "test_db",
                "test_table",
                Arrays.asList("time", "s1", "s2"),
                Arrays.asList(TSDataType.TIMESTAMP, TSDataType.INT64, TSDataType.INT64),
                Arrays.asList(ColumnCategory.TIME, ColumnCategory.FIELD, ColumnCategory.FIELD),
                Arrays.asList(0, 1, 2)));
    return task;
  }

  private TsBlock createInt64LongTsBlock(long[] timestamps, long[] s1Values, long[] s2Values) {
    TsBlockBuilder builder =
        new TsBlockBuilder(Arrays.asList(TSDataType.TIMESTAMP, TSDataType.INT64, TSDataType.INT64));
    for (int i = 0; i < timestamps.length; i++) {
      builder.getTimeColumnBuilder().writeLong(timestamps[i]);
      builder.getColumnBuilder(0).writeLong(timestamps[i]);
      builder.getColumnBuilder(1).writeLong(s1Values[i]);
      builder.getColumnBuilder(2).writeLong(s2Values[i]);
      builder.declarePosition();
    }
    return builder.build();
  }

  private StreamSubTaskContext createSubTaskContext() {
    StreamSubTaskContext subTaskContext = Mockito.mock(StreamSubTaskContext.class);
    Mockito.when(subTaskContext.getPartitionKey())
        .thenReturn(new ListPartitionKey(Collections.singletonList("d0")));
    return subTaskContext;
  }

  @Test
  public void testCreateSinkTaskAfterStart() throws Exception {
    StreamTask task = createTestStreamTask();
    WriteBackEngine engine = new WriteBackEngine(task);

    engine.start();
    StreamSubTaskContext subTaskContext = createSubTaskContext();
    IStreamSinkTask sinkTask = engine.createSinkSubTask(subTaskContext);
    Assert.assertNotNull(sinkTask);
    Assert.assertTrue(sinkTask instanceof StreamSinkTask);
    Assert.assertSame(subTaskContext, ((StreamSinkTask) sinkTask).getSubTaskContext());

    engine.stop();
  }

  @Test
  public void testPushSingleTsBlockAndFlush() throws Exception {
    StreamTask task = createTestStreamTask();
    WriteBackEngine engine = new WriteBackEngine(task);

    engine.start();
    IStreamSinkTask sinkTask = engine.createSinkSubTask(createSubTaskContext());

    long[] timestamps = {1L, 2L, 3L, 4L, 5L};
    long[] s1Values = {100L, 200L, 300L, 400L, 500L};
    long[] s2Values = {10L, 20L, 30L, 40L, 50L};
    TsBlock block = createInt64LongTsBlock(timestamps, s1Values, s2Values);

    sinkTask.push(block, 1L, true);

    Thread.sleep(5_000);
    engine.stop();
  }

  @Test
  public void testPushMultipleTsBlocks() throws Exception {
    StreamTask task = createTestStreamTask();
    WriteBackEngine engine = new WriteBackEngine(task);

    engine.start();
    IStreamSinkTask sinkTask = engine.createSinkSubTask(createSubTaskContext());

    for (int batch = 0; batch < 3; batch++) {
      long[] timestamps = new long[5];
      long[] s1Values = new long[5];
      long[] s2Values = new long[5];
      for (int i = 0; i < 5; i++) {
        timestamps[i] = batch * 100L + i + 1L;
        s1Values[i] = batch * 1000L + i * 100L;
        s2Values[i] = batch * 100L + i * 10L;
      }
      TsBlock block = createInt64LongTsBlock(timestamps, s1Values, s2Values);
      sinkTask.push(block, batch, true);
    }

    Thread.sleep(5_000);
    engine.stop();
  }

  @Test
  public void testPushWithNullValues() throws Exception {
    StreamTask task = createTestStreamTask();
    WriteBackEngine engine = new WriteBackEngine(task);

    engine.start();
    IStreamSinkTask sinkTask = engine.createSinkSubTask(createSubTaskContext());

    TsBlockBuilder builder =
        new TsBlockBuilder(Arrays.asList(TSDataType.TIMESTAMP, TSDataType.INT64, TSDataType.INT64));
    builder.getTimeColumnBuilder().writeLong(1L);
    builder.getColumnBuilder(0).writeLong(1L);
    builder.getColumnBuilder(1).writeLong(100L);
    builder.getColumnBuilder(2).appendNull();
    builder.declarePosition();

    builder.getTimeColumnBuilder().writeLong(2L);
    builder.getColumnBuilder(0).writeLong(2L);
    builder.getColumnBuilder(1).appendNull();
    builder.getColumnBuilder(2).writeLong(200L);
    builder.declarePosition();

    TsBlock block = builder.build();
    sinkTask.push(block, 0L, true);

    Thread.sleep(5_000);
    engine.stop();
  }

  @Test
  public void testStopCleansUp() throws Exception {
    StreamTask task = createTestStreamTask();
    WriteBackEngine engine = new WriteBackEngine(task);

    engine.start();
    IStreamSinkTask sinkTask = engine.createSinkSubTask(createSubTaskContext());

    TsBlock block = createInt64LongTsBlock(new long[] {1L}, new long[] {100L}, new long[] {10L});
    sinkTask.push(block, 0L, true);

    Thread.sleep(10);

    engine.stop();
    sinkTask.stop();
  }
}
