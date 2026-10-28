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

package org.apache.iotdb.streamnode.engine.source;

import org.apache.iotdb.commons.stream.IoTDBSubscriptionSource;
import org.apache.iotdb.commons.stream.StreamProperties;
import org.apache.iotdb.commons.stream.StreamSource;
import org.apache.iotdb.session.subscription.consumer.table.SubscriptionTablePullConsumer;
import org.apache.iotdb.streamnode.conf.StreamNodeConfig;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class StreamSourceInstanceTest {

  private IoTDBSubscriptionSource source;
  private StreamNodeConfig nodeConfig;

  @Before
  public void setUp() {
    source =
        new IoTDBSubscriptionSource(
            "testdb", "sensor_data", null, null, "localhost", 6667, "root", "root");
    nodeConfig = mock(StreamNodeConfig.class);
  }

  /** Verifies both compatibility factory entry points create the supported subscription source. */
  @Test
  public void testCreateAndFromSupportedSource() {
    final StreamSourceInstance created =
        StreamSourceInstance.create(source, "created_task", nodeConfig);
    final StreamSourceInstance restored =
        StreamSourceInstance.from(source, "restored_task", nodeConfig);

    Assert.assertTrue(created instanceof IoTDBSubscriptionSourceInstance);
    Assert.assertTrue(restored instanceof IoTDBSubscriptionSourceInstance);
    Assert.assertSame(source, ((IoTDBSubscriptionSourceInstance) created).getSourceConfig());
    Assert.assertSame(source, ((IoTDBSubscriptionSourceInstance) restored).getSourceConfig());
  }

  /** Verifies the factory accepts task-level properties when constructing a subscription source. */
  @Test
  public void testCreateSupportedSourceWithProperties() {
    source.setOwnerEpoch(7L);
    when(nodeConfig.getStreamNodeId()).thenReturn(42);
    when(nodeConfig.getSnInternalAddress()).thenReturn("127.0.0.1");
    when(nodeConfig.getSnInternalPort()).thenReturn(9090);
    final StreamSourceInstance created =
        StreamSourceInstance.create(source, new StreamProperties(), "configured_task", nodeConfig);

    Assert.assertTrue(created instanceof IoTDBSubscriptionSourceInstance);
    Assert.assertSame(source, ((IoTDBSubscriptionSourceInstance) created).getSourceConfig());
    final SubscriptionTablePullConsumer consumer =
        (SubscriptionTablePullConsumer)
            ((IoTDBSubscriptionSourceInstance) created).buildConsumer(0);
    Assert.assertEquals("127.0.0.1:9090", consumer.getOwnerId());
    Assert.assertEquals(Long.valueOf(7L), consumer.getOwnerEpoch());
  }

  /** Verifies an unknown source type fails fast instead of producing a partial source instance. */
  @Test
  public void testCreateRejectsUnsupportedSourceType() {
    final StreamSource unsupportedSource = mock(StreamSource.class);
    when(unsupportedSource.getType()).thenReturn(null);

    try {
      StreamSourceInstance.create(unsupportedSource, "unsupported_task", nodeConfig);
      Assert.fail("Expected unsupported source type to be rejected");
    } catch (final UnsupportedOperationException e) {
      Assert.assertEquals("Unsupported source type: null", e.getMessage());
    }
  }
}
