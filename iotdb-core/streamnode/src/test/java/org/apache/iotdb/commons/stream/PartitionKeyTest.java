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

package org.apache.iotdb.commons.stream;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PartitionKeyTest {

  @Test
  public void testColumnValuePartitionKeyEqualsAndHashesArrayContents() {
    final ColumnValuePartitionKey left = new ColumnValuePartitionKey(new Object[] {"region-a", 1});
    final ColumnValuePartitionKey right = new ColumnValuePartitionKey(new Object[] {"region-a", 1});
    final ColumnValuePartitionKey different =
        new ColumnValuePartitionKey(new Object[] {"region-a", 2});

    Assert.assertEquals(left, right);
    Assert.assertEquals(left.hashCode(), right.hashCode());
    Assert.assertNotEquals(left, different);

    final Map<PartitionKey, String> valuesByPartition = new HashMap<>();
    valuesByPartition.put(left, "matched");
    Assert.assertEquals("matched", valuesByPartition.get(right));
  }

  @Test
  public void testColumnValuePartitionKeyIsNullSafe() {
    final ColumnValuePartitionKey left =
        new ColumnValuePartitionKey(new Object[] {"region-a", null});
    final ColumnValuePartitionKey right =
        new ColumnValuePartitionKey(new Object[] {"region-a", null});

    Assert.assertEquals(left, right);
    Assert.assertEquals(left.hashCode(), right.hashCode());
    Assert.assertFalse(left.equals((PartitionKey) null));
  }

  @Test
  public void testColumnValuePartitionKeyDefensivelyCopiesValues() {
    final Object[] values = new Object[] {"region-a"};
    final ColumnValuePartitionKey key = new ColumnValuePartitionKey(values);
    values[0] = "region-b";

    Assert.assertEquals(new ColumnValuePartitionKey(new Object[] {"region-a"}), key);

    final Object[] returnedValues = key.getColumnValues();
    returnedValues[0] = "region-c";
    Assert.assertEquals(new ColumnValuePartitionKey(new Object[] {"region-a"}), key);
  }

  @Test
  public void testTabletPositionPartitionKeyMatchesColumnValueKeyWithMissingColumn() {
    final Tablet tablet = buildTablet(10);
    final PartitionKey tabletKey = new TabletPositionPartitionKey(tablet, 0, new int[] {-1, 0});
    final PartitionKey copiedKey = new ColumnValuePartitionKey(new Object[] {null, 10});

    Assert.assertEquals(tabletKey, copiedKey);
    Assert.assertEquals(copiedKey, tabletKey);
    Assert.assertEquals(tabletKey.hashCode(), copiedKey.hashCode());
  }

  @Test
  public void testListPartitionKeyIsStableAndNullSafe() {
    final List<Object> values = Arrays.asList("region-a", null, 1);
    final ListPartitionKey key = new ListPartitionKey(values);
    final ListPartitionKey same = new ListPartitionKey(values);

    Assert.assertEquals(same, key);
    Assert.assertEquals(same.hashCode(), key.hashCode());
    Assert.assertEquals("region-a", key.segmentValue(0));
    Assert.assertNull(key.segmentValue(1));

    try {
      key.getValues().set(0, "region-b");
      Assert.fail("ListPartitionKey values should be immutable");
    } catch (UnsupportedOperationException expected) {
      // expected
    }

    Assert.assertEquals(new ColumnValuePartitionKey(new Object[] {"region-a", null, 1}), key);
  }

  private Tablet buildTablet(final int value) {
    final java.util.List<IMeasurementSchema> schemas =
        Collections.singletonList(new MeasurementSchema("value", TSDataType.INT32));
    final Tablet tablet = new Tablet("testDevice", schemas, 1);
    tablet.addTimestamp(0, 0L);
    tablet.addValue("value", 0, value);
    return tablet;
  }
}
