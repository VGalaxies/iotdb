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
import org.apache.iotdb.rpc.subscription.config.TopicConstant;
import org.apache.iotdb.session.subscription.ISubscriptionTableSession;
import org.apache.iotdb.session.subscription.consumer.ISubscriptionTablePullConsumer;
import org.apache.iotdb.session.subscription.payload.SubscriptionMessage;
import org.apache.iotdb.streamnode.conf.StreamNodeConfig;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link IoTDBSubscriptionSourceInstance}.
 *
 * <p>Uses a testable subclass to inject mock session and consumer, avoiding the need for static
 * mocking of the builder classes.
 */
public class IoTDBSubscriptionSourceInstanceTest {

  private static final String TASK_NAME = "test_task";
  private static final String EXPECTED_TOPIC = "__stream__" + TASK_NAME;
  private static final String DATABASE = "testdb";
  private static final String TABLE = "sensor_data";
  private static final String DEFAULT_USER = "root";
  private static final String DEFAULT_PASSWORD = "root";

  private ISubscriptionTableSession mockSession;
  private ISubscriptionTablePullConsumer mockConsumer;
  private IoTDBSubscriptionSource sourceConfig;
  private StreamNodeConfig nodeConfig;

  private TestableInstance instance;

  // Collected (tablet, index) pairs from dataConsumer
  private final List<Long> receivedIndices = new CopyOnWriteArrayList<>();
  private final List<Tablet> receivedTablets = new CopyOnWriteArrayList<>();

  private final BiConsumer<Tablet, Long> dataConsumer =
      (tablet, idx) -> {
        receivedTablets.add(tablet);
        receivedIndices.add(idx);
      };

  @Before
  public void setUp() throws Exception {
    mockSession = mock(ISubscriptionTableSession.class);
    mockConsumer = mock(ISubscriptionTablePullConsumer.class);

    sourceConfig =
        new IoTDBSubscriptionSource(
            DATABASE, TABLE, null, null, "localhost", 6667, DEFAULT_USER, DEFAULT_PASSWORD);

    nodeConfig = mock(StreamNodeConfig.class);
    when(nodeConfig.getSnInternalAddress()).thenReturn("127.0.0.1");
    when(nodeConfig.getSnInternalPort()).thenReturn(9090);

    // Default: consumer returns empty list (no messages)
    when(mockConsumer.poll(any(Duration.class))).thenReturn(Collections.emptyList());

    instance = new TestableInstance(sourceConfig, TASK_NAME, dataConsumer, nodeConfig);
  }

  @After
  public void tearDown() {
    instance.stop();
  }

  // ======================== Topic creation ========================

  @Test
  public void testTopicCreatedWithCorrectProperties() throws Exception {
    instance.start();

    final ArgumentCaptor<Properties> propsCaptor = ArgumentCaptor.forClass(Properties.class);
    verify(mockSession).open();
    verify(mockSession).createTopicIfNotExists(eq(EXPECTED_TOPIC), propsCaptor.capture());

    final Properties props = propsCaptor.getValue();
    Assert.assertEquals(DATABASE, props.getProperty(TopicConstant.DATABASE_KEY));
    Assert.assertEquals(TABLE, props.getProperty(TopicConstant.TABLE_KEY));
  }

  @Test
  public void testTopicNameHasStreamPrefix() throws Exception {
    instance.start();
    Assert.assertEquals(EXPECTED_TOPIC, instance.getTopicName());
  }

  // ======================== Consumer lifecycle ========================

  @Test
  public void testConsumerOpenedAndSubscribed() throws Exception {
    instance.start();

    verify(mockConsumer).open();
    verify(mockConsumer).subscribe(EXPECTED_TOPIC);
  }

  @Test
  public void testConsumerIdUsesSubscriptionSafeIdentifier() {
    when(nodeConfig.getSnInternalAddress()).thenReturn("127.0.0.1");
    when(nodeConfig.getSnInternalPort()).thenReturn(10820);

    Assert.assertEquals(
        "stream_task_name_127_0_0_1_10820",
        IoTDBSubscriptionSourceInstance.buildConsumerId("task-name", nodeConfig));
  }

  @Test
  public void testStopUnsubscribesAndClosesConsumer() throws Exception {
    instance.start();
    instance.stop();

    verify(mockConsumer).unsubscribe(EXPECTED_TOPIC);
    verify(mockConsumer).close();
  }

  // ======================== Data delivery ========================

  @Test
  public void testTabletsDeliveredToDataConsumerWithMonotonicIndices() throws Exception {
    // dataConsumer is called once per tablet (not per row), so receivedIndices.size()
    // equals the number of tablets delivered, regardless of row count within each tablet.
    final Tablet tablet1 = buildTablet(3);
    final Tablet tablet2 = buildTablet(2);

    final SubscriptionMessage msg1 = mockMessage(Collections.singletonList(tablet1));
    final SubscriptionMessage msg2 = mockMessage(Collections.singletonList(tablet2));

    // First poll returns two messages, subsequent polls return empty
    when(mockConsumer.poll(any(Duration.class)))
        .thenReturn(Arrays.asList(msg1, msg2))
        .thenReturn(Collections.emptyList());

    instance.start();

    // 2 messages × 1 tablet each → 2 dataConsumer calls
    Awaitility.await().atMost(3, TimeUnit.SECONDS).until(() -> receivedIndices.size() >= 2);

    Assert.assertEquals(2, receivedIndices.size());
    Assert.assertSame(tablet1, receivedTablets.get(0));
    Assert.assertSame(tablet2, receivedTablets.get(1));
    // Indices must be strictly increasing
    Assert.assertTrue(
        "Indices must be monotonically increasing",
        receivedIndices.get(1) > receivedIndices.get(0));
  }

  @Test
  public void testCommitWaitsForAllTabletsFromSameMessage() throws Exception {
    final Tablet tablet1 = buildTablet(1);
    final Tablet tablet2 = buildTablet(1);

    final SubscriptionMessage msg = mockMessage(Arrays.asList(tablet1, tablet2));

    when(mockConsumer.poll(any(Duration.class)))
        .thenReturn(Collections.singletonList(msg))
        .thenReturn(Collections.emptyList());

    instance.start();
    Awaitility.await().atMost(3, TimeUnit.SECONDS).until(() -> receivedIndices.size() >= 2);

    instance.commit(1);
    verify(mockConsumer, Mockito.never()).commitSync(any(List.class));

    instance.commit(2);

    @SuppressWarnings("unchecked")
    final ArgumentCaptor<List<SubscriptionMessage>> captor = ArgumentCaptor.forClass(List.class);
    verify(mockConsumer).commitSync(captor.capture());
    Assert.assertEquals(Collections.singletonList(msg), captor.getValue());
  }

  @Test
  public void testEmptyMessageProducesNoPendingCommit() throws Exception {
    // A message with no tablets should not be enqueued
    final SubscriptionMessage emptyMsg = mockMessage(Collections.emptyList());

    when(mockConsumer.poll(any(Duration.class)))
        .thenReturn(Collections.singletonList(emptyMsg))
        .thenReturn(Collections.emptyList());

    instance.start();
    TimeUnit.MILLISECONDS.sleep(600); // let poll loop run at least once

    // commit(0) should commit nothing since no pending entries
    instance.commit(0);
    verify(mockConsumer, Mockito.never()).commitSync(any(List.class));
  }

  @Test
  public void testEmptyMessageDoesNotBlockFollowingMessageCommit() throws Exception {
    final SubscriptionMessage emptyMsg = mockMessage(Collections.emptyList());
    final SubscriptionMessage dataMsg = mockMessage(Collections.singletonList(buildTablet(1)));

    when(mockConsumer.poll(any(Duration.class)))
        .thenReturn(Arrays.asList(emptyMsg, dataMsg))
        .thenReturn(Collections.emptyList());

    instance.start();
    Awaitility.await().atMost(3, TimeUnit.SECONDS).until(() -> receivedIndices.size() >= 1);

    instance.commit(1);

    @SuppressWarnings("unchecked")
    final ArgumentCaptor<List<SubscriptionMessage>> captor = ArgumentCaptor.forClass(List.class);
    verify(mockConsumer).commitSync(captor.capture());
    Assert.assertEquals(Collections.singletonList(dataMsg), captor.getValue());
  }

  @Test
  public void testProcessedMessageReleasesUserData() throws Exception {
    final SubscriptionMessage msg = mockMessage(Collections.singletonList(buildTablet(1)));

    when(mockConsumer.poll(any(Duration.class)))
        .thenReturn(Collections.singletonList(msg))
        .thenReturn(Collections.emptyList());

    instance.start();
    Awaitility.await().atMost(3, TimeUnit.SECONDS).until(() -> receivedIndices.size() >= 1);

    verify(msg, Mockito.timeout(3000)).removeUserData();
  }

  // ======================== Commit logic ========================

  @Test
  public void testCommitDrainsMessagesUpToIndex() throws Exception {
    final Tablet tablet = buildTablet(1);
    final SubscriptionMessage msg1 = mockMessage(Collections.singletonList(tablet));
    final SubscriptionMessage msg2 = mockMessage(Collections.singletonList(tablet));
    final SubscriptionMessage msg3 = mockMessage(Collections.singletonList(tablet));

    when(mockConsumer.poll(any(Duration.class)))
        .thenReturn(Arrays.asList(msg1, msg2, msg3))
        .thenReturn(Collections.emptyList());

    instance.start();
    Awaitility.await().atMost(3, TimeUnit.SECONDS).until(() -> receivedIndices.size() >= 3);

    // Indices assigned: msg1→1, msg2→2, msg3→3
    // commit(2) should commit msg1 and msg2 but not msg3
    instance.commit(2);

    @SuppressWarnings("unchecked")
    final ArgumentCaptor<List<SubscriptionMessage>> captor = ArgumentCaptor.forClass(List.class);
    verify(mockConsumer).commitSync(captor.capture());

    final List<SubscriptionMessage> committed = captor.getValue();
    Assert.assertEquals(2, committed.size());
    Assert.assertTrue(committed.contains(msg1));
    Assert.assertTrue(committed.contains(msg2));
    Assert.assertFalse(committed.contains(msg3));
  }

  @Test
  public void testCommitAllMessages() throws Exception {
    final Tablet tablet = buildTablet(1);
    final SubscriptionMessage msg1 = mockMessage(Collections.singletonList(tablet));
    final SubscriptionMessage msg2 = mockMessage(Collections.singletonList(tablet));

    when(mockConsumer.poll(any(Duration.class)))
        .thenReturn(Arrays.asList(msg1, msg2))
        .thenReturn(Collections.emptyList());

    instance.start();
    Awaitility.await().atMost(3, TimeUnit.SECONDS).until(() -> receivedIndices.size() >= 2);

    instance.commit(Long.MAX_VALUE);

    @SuppressWarnings("unchecked")
    final ArgumentCaptor<List<SubscriptionMessage>> captor = ArgumentCaptor.forClass(List.class);
    verify(mockConsumer).commitSync(captor.capture());
    Assert.assertEquals(2, captor.getValue().size());
  }

  @Test
  public void testCommitWithNoReadyMessagesDoesNotCallConsumer() throws Exception {
    final Tablet tablet = buildTablet(1);
    final SubscriptionMessage msg = mockMessage(Collections.singletonList(tablet));

    when(mockConsumer.poll(any(Duration.class)))
        .thenReturn(Collections.singletonList(msg))
        .thenReturn(Collections.emptyList());

    instance.start();
    Awaitility.await().atMost(3, TimeUnit.SECONDS).until(() -> receivedIndices.size() >= 1);

    // commit(-1) — nothing has maxIndex <= -1
    instance.commit(-1);
    verify(mockConsumer, Mockito.never()).commitSync(any(List.class));
  }

  // ======================== Helpers ========================

  private Tablet buildTablet(final int rowCount) {
    final List<IMeasurementSchema> schemas =
        Collections.singletonList(new MeasurementSchema("value", TSDataType.INT32));
    final Tablet tablet = new Tablet("testDevice", schemas, rowCount);
    for (int i = 0; i < rowCount; i++) {
      tablet.addTimestamp(i, i * 1000L);
      tablet.addValue("value", i, i);
    }
    return tablet;
  }

  private SubscriptionMessage mockMessage(final List<Tablet> tablets) {
    final SubscriptionMessage msg = mock(SubscriptionMessage.class);
    final Iterator<Tablet> iter = tablets.iterator();
    when(msg.getRecordTabletIterator()).thenReturn(iter);
    return msg;
  }

  // ======================== Testable subclass ========================

  private class TestableInstance extends IoTDBSubscriptionSourceInstance {

    TestableInstance(
        final IoTDBSubscriptionSource sourceConfig,
        final String taskName,
        final BiConsumer<Tablet, Long> dataConsumer,
        final StreamNodeConfig nodeConfig) {
      super(sourceConfig, taskName, dataConsumer, nodeConfig);
    }

    @Override
    protected ISubscriptionTableSession buildSession() {
      return mockSession;
    }

    @Override
    protected ISubscriptionTablePullConsumer buildConsumer() {
      return mockConsumer;
    }
  }
}
