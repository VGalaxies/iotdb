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

package org.apache.iotdb.streamnode.engine.task;

import org.apache.iotdb.commons.stream.PartitionKey;
import org.apache.iotdb.streamnode.engine.task.StreamSubTask.DataSlice;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

public class StreamSubTaskTest {

  private StreamSubTask subTask;
  private PartitionKey partitionKey;

  @Before
  public void setUp() throws Exception {
    partitionKey =
        new PartitionKey() {
          @Override
          public int partitionHash() {
            return 0;
          }

          @Override
          public int segmentNum() {
            return 1;
          }

          @Override
          public Object segmentValue(int segmentIndex) {
            return 0;
          }
        };
    subTask = new StreamSubTask(partitionKey, null, null, null, null, null, null);
  }

  private static final List<IMeasurementSchema> SCHEMAS =
      Arrays.asList(
          new MeasurementSchema("col_bool", TSDataType.BOOLEAN),
          new MeasurementSchema("col_int32", TSDataType.INT32),
          new MeasurementSchema("col_int64", TSDataType.INT64),
          new MeasurementSchema("col_float", TSDataType.FLOAT),
          new MeasurementSchema("col_double", TSDataType.DOUBLE),
          new MeasurementSchema("col_text", TSDataType.TEXT));

  private Tablet createTablet(int rowCount) {
    Tablet tablet = new Tablet("root.test.device", SCHEMAS, rowCount);
    tablet.setTimestamps(new long[rowCount]);
    return tablet;
  }

  private void setTimestamp(Tablet tablet, int rowIndex, long timestamp) {
    tablet.getTimestamps()[rowIndex] = timestamp;
  }

  private void fillRow(
      Tablet tablet, int rowIndex, boolean b, int i32, long i64, float f, double d, String text) {
    Object[] values = tablet.getValues();
    ((boolean[]) values[0])[rowIndex] = b;
    ((int[]) values[1])[rowIndex] = i32;
    ((long[]) values[2])[rowIndex] = i64;
    ((float[]) values[3])[rowIndex] = f;
    ((double[]) values[4])[rowIndex] = d;
    ((Binary[]) values[5])[rowIndex] = new Binary(text.getBytes(StandardCharsets.UTF_8));
  }

  @Test
  public void testOfferDataSlicesUsesMaxCommitId() throws Exception {
    Tablet tablet = createTablet(3);
    setTimestamp(tablet, 0, 1000L);
    fillRow(tablet, 0, true, 10, 100L, 1.0f, 1.1, "a");
    setTimestamp(tablet, 1, 2000L);
    fillRow(tablet, 1, false, 20, 200L, 2.0f, 2.2, "b");
    setTimestamp(tablet, 2, 3000L);
    fillRow(tablet, 2, true, 30, 300L, 3.0f, 3.3, "c");

    StreamSubTask task = new StreamSubTask(partitionKey, null, null, null, null, "test", null);
    task.setConsumer(dataSlices -> java.util.concurrent.CompletableFuture.completedFuture(null));

    task.offer(
            Arrays.asList(
                new DataSlice(partitionKey, tablet, 0, 2, 10L),
                new DataSlice(partitionKey, tablet, 2, 3, 12L)))
        .get(1, TimeUnit.SECONDS);

    assertEquals(12L, task.getCommitId());
  }

  @Test
  public void testOfferDataSlicesDoesNotMoveCommitIdBackward() throws Exception {
    Tablet tablet = createTablet(2);
    setTimestamp(tablet, 0, 1000L);
    fillRow(tablet, 0, true, 10, 100L, 1.0f, 1.1, "a");
    setTimestamp(tablet, 1, 2000L);
    fillRow(tablet, 1, false, 20, 200L, 2.0f, 2.2, "b");

    subTask
        .offer(Collections.singletonList(new DataSlice(partitionKey, tablet, 0, 1, 10L)))
        .get(1, TimeUnit.SECONDS);
    subTask
        .offer(Collections.singletonList(new DataSlice(partitionKey, tablet, 1, 2, 7L)))
        .get(1, TimeUnit.SECONDS);

    assertEquals(10L, subTask.getCommitId());
  }

  @Test
  public void testOfferEmptyDataSlicesCompletesWithoutChangingCommitId() throws Exception {
    subTask.offer(Collections.emptyList()).get(1, TimeUnit.SECONDS);

    assertEquals(-1L, subTask.getCommitId());
  }

  @Test
  public void testOfferNullDataSlicesCompletesWithoutChangingCommitId() throws Exception {
    subTask.offer(null).get(1, TimeUnit.SECONDS);

    assertEquals(-1L, subTask.getCommitId());
  }

  @Test
  public void testSetConsumerNullUsesDefaultConsumer() throws Exception {
    // setConsumer(null) should install a default no-op consumer
    subTask.setConsumer(null);
    Tablet tablet = createTablet(1);
    setTimestamp(tablet, 0, 1000L);
    fillRow(tablet, 0, true, 10, 100L, 1.0f, 1.1, "a");

    subTask
        .offer(Collections.singletonList(new DataSlice(partitionKey, tablet, 0, 1, 5L)))
        .get(1, TimeUnit.SECONDS);

    assertEquals(5L, subTask.getCommitId());
  }

  @Test
  public void testGetPartitionKey() {
    assertEquals(partitionKey, subTask.getPartitionKey());
  }

  @Test
  public void testGetStreamName() {
    StreamSubTask named =
        new StreamSubTask(partitionKey, null, null, null, null, "my-stream", null);
    assertEquals("my-stream", named.getStreamName());
  }

  @Test
  public void testGetWindowEngineReturnsNull() {
    assertNull(subTask.getWindowEngine());
  }

  @Test
  public void testGetComputeTaskReturnsNull() {
    assertNull(subTask.getComputeTask());
  }

  @Test
  public void testGetSinkReturnsNull() {
    assertNull(subTask.getSink());
  }

  @Test
  public void testGetStateMachineReturnsContextStateMachine() {
    StreamSubTaskStateMachine stateMachine = new StreamSubTaskStateMachine("test", Runnable::run);
    StreamSubTaskContext context = new StreamSubTaskContext("test", partitionKey, stateMachine);
    StreamSubTask withContext =
        new StreamSubTask(partitionKey, null, null, null, null, "test", context);
    assertSame(stateMachine, withContext.getStateMachine());
  }

  @Test
  public void testGetContextReturnsNull() {
    assertNull(subTask.getContext());
  }

  @Test
  public void testGetCommitIdInitiallyNegative() {
    assertEquals(-1L, subTask.getCommitId());
  }
}
