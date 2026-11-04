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

import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.ComparisonExpression;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Identifier;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.StringLiteral;
import org.apache.iotdb.rpc.subscription.config.TopicConstant;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

public class IoTDBSubscriptionSourceTest {

  @Test
  public void testTopicPropertiesContainPreFilterTagFilter() {
    final IoTDBSubscriptionSource source =
        new IoTDBSubscriptionSource(
            "db1",
            "weather",
            new ComparisonExpression(
                ComparisonExpression.Operator.EQUAL,
                new Identifier("region"),
                new StringLiteral("north")),
            Collections.emptyList(),
            "127.0.0.1",
            6667,
            "root",
            "root");
    source.setSubscribeColumns(Arrays.asList("temperature"));

    final Properties properties = source.getTopicProperties(null);
    Assert.assertEquals("region = \"north\"", properties.getProperty(TopicConstant.TAG_FILTER_KEY));
    Assert.assertNotNull(properties.getProperty(TopicConstant.COLUMN_FILTER_KEY));
  }

  @Test
  public void testTopicPropertiesWithoutPreFilterHaveNoTagFilter() {
    final IoTDBSubscriptionSource source =
        new IoTDBSubscriptionSource(
            "db1", "weather", null, Collections.emptyList(), "127.0.0.1", 6667, "root", "root");

    Assert.assertNull(source.getTopicProperties(null).getProperty(TopicConstant.TAG_FILTER_KEY));
  }
}
