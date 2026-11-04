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

package org.apache.iotdb.commons.stream;

import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.BooleanLiteral;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.ComparisonExpression;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.FunctionCall;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Identifier;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.InListExpression;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.InPredicate;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.IsNotNullPredicate;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.IsNullPredicate;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.LikePredicate;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.LogicalExpression;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.NotExpression;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.QualifiedName;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.StringLiteral;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class SubscriptionTagFilterFormatterTest {

  @Test
  public void testComparison() {
    Assert.assertEquals(
        "tag1 = \"north\"",
        SubscriptionTagFilterFormatter.format(
            new ComparisonExpression(
                ComparisonExpression.Operator.EQUAL,
                new Identifier("tag1"),
                new StringLiteral("north"))));
    Assert.assertEquals(
        "tag1 <> \"north\"",
        SubscriptionTagFilterFormatter.format(
            new ComparisonExpression(
                ComparisonExpression.Operator.NOT_EQUAL,
                new Identifier("tag1"),
                new StringLiteral("north"))));
  }

  @Test
  public void testLogicalExpressionAndEscape() {
    final LogicalExpression and =
        LogicalExpression.and(
            new ComparisonExpression(
                ComparisonExpression.Operator.EQUAL,
                new Identifier("tag1"),
                new StringLiteral("north")),
            new ComparisonExpression(
                ComparisonExpression.Operator.EQUAL,
                new Identifier("tag2"),
                new StringLiteral("d1")));
    Assert.assertEquals(
        "(tag1 = \"north\" AND tag2 = \"d1\")", SubscriptionTagFilterFormatter.format(and));
    Assert.assertEquals(
        "(NOT tag1 = \"north\")",
        SubscriptionTagFilterFormatter.format(
            new NotExpression(
                new ComparisonExpression(
                    ComparisonExpression.Operator.EQUAL,
                    new Identifier("tag1"),
                    new StringLiteral("north")))));
    Assert.assertEquals(
        "tag1 = \"a\"\"b\"",
        SubscriptionTagFilterFormatter.format(
            new ComparisonExpression(
                ComparisonExpression.Operator.EQUAL,
                new Identifier("tag1"),
                new StringLiteral("a\"b"))));
  }

  @Test
  public void testInLikeAndNullPredicates() {
    Assert.assertEquals(
        "(tag1 IN (\"north\", \"south\"))",
        SubscriptionTagFilterFormatter.format(
            new InPredicate(
                new Identifier("tag1"),
                new InListExpression(
                    Arrays.asList(new StringLiteral("north"), new StringLiteral("south"))))));
    Assert.assertEquals(
        "(tag1 LIKE \"n%\" ESCAPE \"!\")",
        SubscriptionTagFilterFormatter.format(
            new LikePredicate(
                new Identifier("tag1"), new StringLiteral("n%"), new StringLiteral("!"))));
    Assert.assertEquals(
        "(tag1 IS NULL)",
        SubscriptionTagFilterFormatter.format(new IsNullPredicate(new Identifier("tag1"))));
    Assert.assertEquals(
        "(tag1 IS NOT NULL)",
        SubscriptionTagFilterFormatter.format(new IsNotNullPredicate(new Identifier("tag1"))));
  }

  @Test
  public void testRegexpLike() {
    Assert.assertEquals(
        "(tag1 REGEXP \"^n\")",
        SubscriptionTagFilterFormatter.format(
            new FunctionCall(
                QualifiedName.of("regexp_like"),
                Arrays.asList(new Identifier("tag1"), new StringLiteral("^n")))));
  }

  @Test
  public void testBooleanLiteralAndQuotedIdentifier() {
    Assert.assertEquals("true", SubscriptionTagFilterFormatter.format(BooleanLiteral.TRUE_LITERAL));
    Assert.assertEquals(
        "\"tag-1\" = \"north\"",
        SubscriptionTagFilterFormatter.format(
            new ComparisonExpression(
                ComparisonExpression.Operator.EQUAL,
                new Identifier("tag-1", true),
                new StringLiteral("north"))));
    Assert.assertEquals(
        "\"or\" = \"north\"",
        SubscriptionTagFilterFormatter.format(
            new ComparisonExpression(
                ComparisonExpression.Operator.EQUAL,
                new Identifier("or"),
                new StringLiteral("north"))));
  }

  @Test
  public void testUnsupportedExpression() {
    final FunctionCall upper =
        new FunctionCall(QualifiedName.of("upper"), List.of(new Identifier("tag1")));
    Assert.assertThrows(
        IllegalArgumentException.class, () -> SubscriptionTagFilterFormatter.format(upper));
  }
}
