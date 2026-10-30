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

import org.apache.iotdb.rpc.subscription.config.TopicConstant;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Properties;

public class IoTDBSubscriptionSourceTest {

  @Test
  public void fillHistoryUsesInitialModeAndStartTime() {
    final StreamProperties properties = new StreamProperties();
    properties.setFillHistory(true);
    properties.setFillHistoryStartTime(1234L);

    final Properties topicProperties = newSource().getTopicProperties(properties);

    Assert.assertEquals(
        TopicConstant.MODE_INITIAL_VALUE, topicProperties.getProperty(TopicConstant.MODE_KEY));
    Assert.assertEquals("1234", topicProperties.getProperty(TopicConstant.START_TIME_KEY));
    Assert.assertEquals(
        "true", topicProperties.getProperty(TopicConstant.RETAIN_PROGRESS_AFTER_UNSUBSCRIBE_KEY));
  }

  @Test
  public void fillHistoryWithoutStartTimeOmitsStartTime() {
    final StreamProperties properties = new StreamProperties();
    properties.setFillHistory(true);

    final Properties topicProperties = newSource().getTopicProperties(properties);

    Assert.assertEquals(
        TopicConstant.MODE_INITIAL_VALUE, topicProperties.getProperty(TopicConstant.MODE_KEY));
    Assert.assertNull(topicProperties.getProperty(TopicConstant.START_TIME_KEY));
  }

  @Test
  public void incrementalModeIsDefault() {
    final Properties topicProperties = newSource().getTopicProperties(new StreamProperties());

    Assert.assertEquals(
        TopicConstant.MODE_CONSENSUS_VALUE, topicProperties.getProperty(TopicConstant.MODE_KEY));
    Assert.assertNull(topicProperties.getProperty(TopicConstant.START_TIME_KEY));
    Assert.assertEquals(
        "true", topicProperties.getProperty(TopicConstant.RETAIN_PROGRESS_AFTER_UNSUBSCRIBE_KEY));
  }

  @Test
  public void columnFilterIsPreservedForFillHistory() {
    final IoTDBSubscriptionSource source = newSource();
    source.setSubscribeColumns(Arrays.asList("s1", "tag1"));
    final StreamProperties properties = new StreamProperties();
    properties.setFillHistory(true);

    final Properties topicProperties = source.getTopicProperties(properties);

    Assert.assertNotNull(topicProperties.getProperty(TopicConstant.COLUMN_FILTER_KEY));
  }

  private static IoTDBSubscriptionSource newSource() {
    return new IoTDBSubscriptionSource(
        "db", "table", null, null, "127.0.0.1", 6667, "root", "root");
  }
}
