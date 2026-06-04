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

package org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.stream;

import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.TableExpressionType;

import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.read.common.type.TypeFactory;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;

import static org.apache.tsfile.enums.TSDataType.DOUBLE;
import static org.apache.tsfile.enums.TSDataType.INT64;
import static org.apache.tsfile.enums.TSDataType.TIMESTAMP;

@RunWith(Parameterized.class)
public class PlaceHolderLiteralSerDeTest {

  private final PlaceHolderLiteral original;

  public PlaceHolderLiteralSerDeTest(PlaceHolderLiteral original) {
    this.original = original;
  }

  @Parameterized.Parameters(name = "{0}")
  public static Collection<Object[]> data() {
    PlaceHolderLiteral nWithValue = new PlaceHolderLiteral(PlaceHolderLiteral.Type.N).withValue(3);
    nWithValue.setDataType(TypeFactory.getType(INT64));

    PlaceHolderLiteral startTimeWithType =
        new PlaceHolderLiteral(PlaceHolderLiteral.Type.START_TIME);
    startTimeWithType.setDataType(TypeFactory.getType(TIMESTAMP));

    PlaceHolderLiteral currentValueWithType =
        new PlaceHolderLiteral(PlaceHolderLiteral.Type.CURRENT_VALUE);
    currentValueWithType.setDataType(TypeFactory.getType(DOUBLE));

    return Arrays.asList(
        new Object[][] {
          {new PlaceHolderLiteral(PlaceHolderLiteral.Type.PREV_VALUE)},
          {new PlaceHolderLiteral(PlaceHolderLiteral.Type.CURRENT_VALUE)},
          {new PlaceHolderLiteral(PlaceHolderLiteral.Type.PREV_TIME)},
          {new PlaceHolderLiteral(PlaceHolderLiteral.Type.NEXT_TIME)},
          {new PlaceHolderLiteral(PlaceHolderLiteral.Type.START_TIME)},
          {new PlaceHolderLiteral(PlaceHolderLiteral.Type.END_TIME)},
          {new PlaceHolderLiteral(PlaceHolderLiteral.Type.ROW_NUM)},
          {new PlaceHolderLiteral(PlaceHolderLiteral.Type.N)},
          {nWithValue},
          {startTimeWithType},
          {currentValueWithType},
        });
  }

  @Test
  public void testDirectSerdeRoundTrip() throws IOException {
    PlaceHolderLiteral restored = roundTripDirect(original);
    assertPlaceHolderLiteralEquals(original, restored);
  }

  @Test
  public void testExpressionSerdeRoundTrip() throws IOException {
    Expression restored = roundTripViaExpression(original);
    Assert.assertTrue(restored instanceof PlaceHolderLiteral);
    assertPlaceHolderLiteralEquals(original, (PlaceHolderLiteral) restored);
  }

  @Test
  public void testExpressionType() {
    Assert.assertEquals(TableExpressionType.PLACEHOLDER_LITERAL, original.getExpressionType());
  }

  private static PlaceHolderLiteral roundTripDirect(PlaceHolderLiteral literal) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    literal.serialize(new DataOutputStream(baos));
    return new PlaceHolderLiteral(ByteBuffer.wrap(baos.toByteArray()));
  }

  private static Expression roundTripViaExpression(PlaceHolderLiteral literal) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    Expression.serialize(literal, new DataOutputStream(baos));
    return Expression.deserialize(ByteBuffer.wrap(baos.toByteArray()));
  }

  private static void assertPlaceHolderLiteralEquals(
      PlaceHolderLiteral expected, PlaceHolderLiteral actual) {
    Assert.assertEquals(expected, actual);
    Assert.assertEquals(expected.getType(), actual.getType());
    Assert.assertEquals(expected.getValue(), actual.getValue());
    assertTypeEquals(expected.getDataType(), actual.getDataType());
  }

  private static void assertTypeEquals(Type expected, Type actual) {
    if (expected == null) {
      Assert.assertNull(actual);
      return;
    }
    Assert.assertNotNull(actual);
    Assert.assertEquals(expected.getTypeEnum(), actual.getTypeEnum());
  }
}
