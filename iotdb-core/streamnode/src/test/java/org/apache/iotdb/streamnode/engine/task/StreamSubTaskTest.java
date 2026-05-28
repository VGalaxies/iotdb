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
import org.apache.iotdb.commons.stream.TumbleWindow;
import org.apache.iotdb.streamnode.engine.task.StreamSubTask.DataSlice;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class StreamSubTaskTest {

  private StreamSubTask subTask;
  private PartitionKey partitionKey;
  private Method toTsBlockMethod;

  @Before
  public void setUp() throws Exception {
    partitionKey =
        new PartitionKey() {
          @Override
          public int partitionHash() {
            return 0;
          }
        };
    TumbleWindow window = new TumbleWindow("test", 1000, 0);
    subTask = new StreamSubTask(partitionKey, window, null, null, null, "test");

    toTsBlockMethod = StreamSubTask.class.getDeclaredMethod("toTsBlock", List.class);
    toTsBlockMethod.setAccessible(true);
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
  public void testSingleDataSlice() throws Exception {
    Tablet tablet = createTablet(6);
    setTimestamp(tablet, 0, 1000L);
    fillRow(tablet, 0, true, 10, 100L, 1.0f, 1.1, "a");
    setTimestamp(tablet, 1, 2000L);
    fillRow(tablet, 1, false, 20, 200L, 2.0f, 2.2, "b");
    setTimestamp(tablet, 2, 3000L);
    fillRow(tablet, 2, true, 30, 300L, 3.0f, 3.3, "c");
    setTimestamp(tablet, 3, 4000L);
    fillRow(tablet, 3, false, 40, 400L, 4.0f, 4.4, "d");
    setTimestamp(tablet, 4, 5000L);
    fillRow(tablet, 4, true, 50, 500L, 5.0f, 5.5, "e");
    setTimestamp(tablet, 5, 6000L);
    fillRow(tablet, 5, false, 60, 600L, 6.0f, 6.6, "f");

    DataSlice slice = new DataSlice(partitionKey, tablet, 0, 6, 100L);
    List<DataSlice> slices = Collections.singletonList(slice);

    TsBlock result = (TsBlock) toTsBlockMethod.invoke(subTask, slices);

    assertEquals(6, result.getPositionCount());
    assertEquals(6, result.getValueColumnCount());

    assertEquals(1000L, result.getTimeColumn().getLong(0));
    assertEquals(2000L, result.getTimeColumn().getLong(1));
    assertEquals(3000L, result.getTimeColumn().getLong(2));
    assertEquals(6000L, result.getTimeColumn().getLong(5));

    assertFalse(result.getColumn(0).isNull(0));
    assertTrue(result.getColumn(0).getBoolean(0));
    assertFalse(result.getColumn(0).getBoolean(1));
    assertTrue(result.getColumn(0).getBoolean(2));

    assertEquals(10, result.getColumn(1).getInt(0));
    assertEquals(20, result.getColumn(1).getInt(1));
    assertEquals(30, result.getColumn(1).getInt(2));

    assertEquals(100L, result.getColumn(2).getLong(0));
    assertEquals(200L, result.getColumn(2).getLong(1));

    assertEquals(1.0f, result.getColumn(3).getFloat(0), 0.0f);
    assertEquals(2.0f, result.getColumn(3).getFloat(1), 0.0f);

    assertEquals(1.1, result.getColumn(4).getDouble(0), 0.0);
    assertEquals(2.2, result.getColumn(4).getDouble(1), 0.0);

    assertEquals("a", result.getColumn(5).getBinary(0).getStringValue(Charset.defaultCharset()));
    assertEquals("f", result.getColumn(5).getBinary(5).getStringValue(Charset.defaultCharset()));
  }

  @Test
  public void testMultipleDataSlices() throws Exception {
    Tablet tablet = createTablet(6);
    setTimestamp(tablet, 0, 1000L);
    fillRow(tablet, 0, true, 10, 100L, 1.0f, 1.1, "a");
    setTimestamp(tablet, 1, 2000L);
    fillRow(tablet, 1, false, 20, 200L, 2.0f, 2.2, "b");
    setTimestamp(tablet, 2, 3000L);
    fillRow(tablet, 2, true, 30, 300L, 3.0f, 3.3, "c");
    setTimestamp(tablet, 3, 4000L);
    fillRow(tablet, 3, false, 40, 400L, 4.0f, 4.4, "d");
    setTimestamp(tablet, 4, 5000L);
    fillRow(tablet, 4, true, 50, 500L, 5.0f, 5.5, "e");
    setTimestamp(tablet, 5, 6000L);
    fillRow(tablet, 5, false, 60, 600L, 6.0f, 6.6, "f");

    DataSlice slice1 = new DataSlice(partitionKey, tablet, 0, 3, 100L);
    DataSlice slice2 = new DataSlice(partitionKey, tablet, 3, 6, 100L);
    List<DataSlice> slices = Arrays.asList(slice1, slice2);

    TsBlock result = (TsBlock) toTsBlockMethod.invoke(subTask, slices);

    assertEquals(6, result.getPositionCount());
    assertEquals(6, result.getValueColumnCount());

    assertEquals(1000L, result.getTimeColumn().getLong(0));
    assertEquals(3000L, result.getTimeColumn().getLong(2));
    assertEquals(4000L, result.getTimeColumn().getLong(3));
    assertEquals(6000L, result.getTimeColumn().getLong(5));

    assertTrue(result.getColumn(0).getBoolean(0));
    assertFalse(result.getColumn(0).getBoolean(1));
    assertTrue(result.getColumn(0).getBoolean(2));
    assertFalse(result.getColumn(0).getBoolean(3));
    assertTrue(result.getColumn(0).getBoolean(4));
    assertFalse(result.getColumn(0).getBoolean(5));

    assertEquals(10, result.getColumn(1).getInt(0));
    assertEquals(20, result.getColumn(1).getInt(1));
    assertEquals(30, result.getColumn(1).getInt(2));
    assertEquals(40, result.getColumn(1).getInt(3));
    assertEquals(50, result.getColumn(1).getInt(4));
    assertEquals(60, result.getColumn(1).getInt(5));

    assertEquals("a", result.getColumn(5).getBinary(0).getStringValue(Charset.defaultCharset()));
    assertEquals("d", result.getColumn(5).getBinary(3).getStringValue(Charset.defaultCharset()));
    assertEquals("f", result.getColumn(5).getBinary(5).getStringValue(Charset.defaultCharset()));
  }

  @Test
  public void testMultipleDataSlicesWithNullValues() throws Exception {
    Tablet tablet = createTablet(6);
    setTimestamp(tablet, 0, 1000L);
    fillRow(tablet, 0, true, 10, 100L, 1.0f, 1.1, "a");
    setTimestamp(tablet, 1, 2000L);
    fillRow(tablet, 1, false, 20, 200L, 2.0f, 2.2, "b");
    setTimestamp(tablet, 2, 3000L);
    fillRow(tablet, 2, true, 30, 300L, 3.0f, 3.3, "c");
    setTimestamp(tablet, 3, 4000L);
    fillRow(tablet, 3, false, 40, 400L, 4.0f, 4.4, "d");
    setTimestamp(tablet, 4, 5000L);
    fillRow(tablet, 4, true, 50, 500L, 5.0f, 5.5, "e");
    setTimestamp(tablet, 5, 6000L);
    fillRow(tablet, 5, false, 60, 600L, 6.0f, 6.6, "f");

    BitMap int64BitMap = new BitMap(6);
    int64BitMap.mark(1);
    int64BitMap.mark(4);
    BitMap doubleBitMap = new BitMap(6);
    doubleBitMap.mark(0);
    doubleBitMap.mark(5);

    BitMap[] bitMaps = tablet.getBitMaps();
    if (bitMaps == null) {
      bitMaps = new BitMap[tablet.getSchemas().size()];
      Field bitMapsField = Tablet.class.getDeclaredField("bitMaps");
      bitMapsField.setAccessible(true);
      bitMapsField.set(tablet, bitMaps);
    }
    bitMaps[2] = int64BitMap;
    bitMaps[4] = doubleBitMap;

    DataSlice slice1 = new DataSlice(partitionKey, tablet, 0, 3, 100L);
    DataSlice slice2 = new DataSlice(partitionKey, tablet, 3, 6, 100L);
    List<DataSlice> slices = Arrays.asList(slice1, slice2);

    TsBlock result = (TsBlock) toTsBlockMethod.invoke(subTask, slices);

    assertEquals(6, result.getPositionCount());
    assertEquals(6, result.getValueColumnCount());

    assertEquals(1000L, result.getTimeColumn().getLong(0));
    assertEquals(2000L, result.getTimeColumn().getLong(1));
    assertEquals(5000L, result.getTimeColumn().getLong(4));
    assertEquals(6000L, result.getTimeColumn().getLong(5));

    assertFalse(result.getColumn(2).isNull(0));
    assertEquals(100L, result.getColumn(2).getLong(0));
    assertTrue(result.getColumn(2).isNull(1));
    assertFalse(result.getColumn(2).isNull(2));
    assertEquals(300L, result.getColumn(2).getLong(2));
    assertFalse(result.getColumn(2).isNull(3));
    assertEquals(400L, result.getColumn(2).getLong(3));
    assertTrue(result.getColumn(2).isNull(4));
    assertFalse(result.getColumn(2).isNull(5));
    assertEquals(600L, result.getColumn(2).getLong(5));

    assertTrue(result.getColumn(4).isNull(0));
    assertFalse(result.getColumn(4).isNull(1));
    assertEquals(2.2, result.getColumn(4).getDouble(1), 0.0);
    assertTrue(result.getColumn(4).isNull(5));
  }

  @Test
  public void testMultipleSlicesWithPartialNullAcrossColumns() throws Exception {
    Tablet tablet = createTablet(6);
    setTimestamp(tablet, 0, 1000L);
    fillRow(tablet, 0, true, 10, 100L, 1.0f, 1.1, "a");
    setTimestamp(tablet, 1, 2000L);
    fillRow(tablet, 1, false, 20, 200L, 2.0f, 2.2, "b");
    setTimestamp(tablet, 2, 3000L);
    fillRow(tablet, 2, true, 30, 300L, 3.0f, 3.3, "c");
    setTimestamp(tablet, 3, 4000L);
    fillRow(tablet, 3, false, 40, 400L, 4.0f, 4.4, "d");
    setTimestamp(tablet, 4, 5000L);
    fillRow(tablet, 4, true, 50, 500L, 5.0f, 5.5, "e");
    setTimestamp(tablet, 5, 6000L);
    fillRow(tablet, 5, false, 60, 600L, 6.0f, 6.6, "f");

    BitMap boolBitMap = new BitMap(6);
    boolBitMap.mark(1);
    boolBitMap.mark(4);
    BitMap int32BitMap = new BitMap(6);
    int32BitMap.mark(0);
    BitMap textBitMap = new BitMap(6);
    textBitMap.mark(3);
    BitMap doubleBitMap = new BitMap(6);
    doubleBitMap.mark(5);

    BitMap[] bitMaps = tablet.getBitMaps();
    if (bitMaps == null) {
      bitMaps = new BitMap[tablet.getSchemas().size()];
      Field bitMapsField = Tablet.class.getDeclaredField("bitMaps");
      bitMapsField.setAccessible(true);
      bitMapsField.set(tablet, bitMaps);
    }
    bitMaps[0] = boolBitMap;
    bitMaps[1] = int32BitMap;
    bitMaps[4] = doubleBitMap;
    bitMaps[5] = textBitMap;

    DataSlice slice1 = new DataSlice(partitionKey, tablet, 0, 2, 100L);
    DataSlice slice2 = new DataSlice(partitionKey, tablet, 2, 4, 100L);
    DataSlice slice3 = new DataSlice(partitionKey, tablet, 4, 6, 100L);
    List<DataSlice> slices = Arrays.asList(slice1, slice2, slice3);

    TsBlock result = (TsBlock) toTsBlockMethod.invoke(subTask, slices);

    assertEquals(6, result.getPositionCount());
    assertEquals(6, result.getValueColumnCount());

    assertEquals(1000L, result.getTimeColumn().getLong(0));
    assertEquals(2000L, result.getTimeColumn().getLong(1));
    assertEquals(4000L, result.getTimeColumn().getLong(3));
    assertEquals(6000L, result.getTimeColumn().getLong(5));

    assertFalse(result.getColumn(0).isNull(0));
    assertTrue(result.getColumn(0).getBoolean(0));
    assertTrue(result.getColumn(0).isNull(1));
    assertFalse(result.getColumn(0).isNull(2));
    assertTrue(result.getColumn(0).getBoolean(2));
    assertFalse(result.getColumn(0).isNull(3));
    assertFalse(result.getColumn(0).getBoolean(3));
    assertTrue(result.getColumn(0).isNull(4));
    assertFalse(result.getColumn(0).isNull(5));
    assertFalse(result.getColumn(0).getBoolean(5));

    assertTrue(result.getColumn(1).isNull(0));
    assertFalse(result.getColumn(1).isNull(1));
    assertEquals(20, result.getColumn(1).getInt(1));
    assertFalse(result.getColumn(1).isNull(2));
    assertEquals(30, result.getColumn(1).getInt(2));
    assertFalse(result.getColumn(1).isNull(3));
    assertEquals(40, result.getColumn(1).getInt(3));
    assertFalse(result.getColumn(1).isNull(4));
    assertEquals(50, result.getColumn(1).getInt(4));
    assertFalse(result.getColumn(1).isNull(5));
    assertEquals(60, result.getColumn(1).getInt(5));

    assertFalse(result.getColumn(2).isNull(0));
    assertEquals(100L, result.getColumn(2).getLong(0));
    assertFalse(result.getColumn(2).isNull(1));
    assertEquals(200L, result.getColumn(2).getLong(1));
    assertFalse(result.getColumn(2).isNull(2));
    assertFalse(result.getColumn(2).isNull(3));
    assertEquals(400L, result.getColumn(2).getLong(3));
    assertFalse(result.getColumn(2).isNull(4));
    assertFalse(result.getColumn(2).isNull(5));

    assertFalse(result.getColumn(3).isNull(0));
    assertEquals(1.0f, result.getColumn(3).getFloat(0), 0.0f);
    assertFalse(result.getColumn(3).isNull(1));
    assertFalse(result.getColumn(3).isNull(2));
    assertFalse(result.getColumn(3).isNull(3));
    assertFalse(result.getColumn(3).isNull(4));
    assertFalse(result.getColumn(3).isNull(5));

    assertFalse(result.getColumn(4).isNull(0));
    assertEquals(1.1, result.getColumn(4).getDouble(0), 0.0);
    assertFalse(result.getColumn(4).isNull(1));
    assertFalse(result.getColumn(4).isNull(2));
    assertFalse(result.getColumn(4).isNull(3));
    assertFalse(result.getColumn(4).isNull(4));
    assertTrue(result.getColumn(4).isNull(5));

    assertFalse(result.getColumn(5).isNull(0));
    assertEquals("a", result.getColumn(5).getBinary(0).getStringValue(Charset.defaultCharset()));
    assertFalse(result.getColumn(5).isNull(1));
    assertFalse(result.getColumn(5).isNull(2));
    assertEquals("c", result.getColumn(5).getBinary(2).getStringValue(Charset.defaultCharset()));
    assertTrue(result.getColumn(5).isNull(3));
    assertFalse(result.getColumn(5).isNull(4));
    assertFalse(result.getColumn(5).isNull(5));
  }

  @Test
  public void testNonContiguousDataSlices() throws Exception {
    Tablet tablet = createTablet(6);
    setTimestamp(tablet, 0, 1000L);
    fillRow(tablet, 0, true, 10, 100L, 1.0f, 1.1, "a");
    setTimestamp(tablet, 1, 2000L);
    fillRow(tablet, 1, false, 20, 200L, 2.0f, 2.2, "b");
    setTimestamp(tablet, 2, 3000L);
    fillRow(tablet, 2, true, 30, 300L, 3.0f, 3.3, "c");
    setTimestamp(tablet, 3, 4000L);
    fillRow(tablet, 3, false, 40, 400L, 4.0f, 4.4, "d");
    setTimestamp(tablet, 4, 5000L);
    fillRow(tablet, 4, true, 50, 500L, 5.0f, 5.5, "e");
    setTimestamp(tablet, 5, 6000L);
    fillRow(tablet, 5, false, 60, 600L, 6.0f, 6.6, "f");

    DataSlice slice1 = new DataSlice(partitionKey, tablet, 0, 2, 100L);
    DataSlice slice2 = new DataSlice(partitionKey, tablet, 4, 6, 100L);
    List<DataSlice> slices = Arrays.asList(slice1, slice2);

    TsBlock result = (TsBlock) toTsBlockMethod.invoke(subTask, slices);

    assertEquals(4, result.getPositionCount());
    assertEquals(6, result.getValueColumnCount());

    assertEquals(1000L, result.getTimeColumn().getLong(0));
    assertEquals(2000L, result.getTimeColumn().getLong(1));
    assertEquals(5000L, result.getTimeColumn().getLong(2));
    assertEquals(6000L, result.getTimeColumn().getLong(3));

    assertTrue(result.getColumn(0).getBoolean(0));
    assertFalse(result.getColumn(0).getBoolean(1));
    assertTrue(result.getColumn(0).getBoolean(2));
    assertFalse(result.getColumn(0).getBoolean(3));

    assertEquals(10, result.getColumn(1).getInt(0));
    assertEquals(20, result.getColumn(1).getInt(1));
    assertEquals(50, result.getColumn(1).getInt(2));
    assertEquals(60, result.getColumn(1).getInt(3));

    assertEquals(100L, result.getColumn(2).getLong(0));
    assertEquals(200L, result.getColumn(2).getLong(1));
    assertEquals(500L, result.getColumn(2).getLong(2));
    assertEquals(600L, result.getColumn(2).getLong(3));

    assertEquals("a", result.getColumn(5).getBinary(0).getStringValue(Charset.defaultCharset()));
    assertEquals("b", result.getColumn(5).getBinary(1).getStringValue(Charset.defaultCharset()));
    assertEquals("e", result.getColumn(5).getBinary(2).getStringValue(Charset.defaultCharset()));
    assertEquals("f", result.getColumn(5).getBinary(3).getStringValue(Charset.defaultCharset()));
  }
}
