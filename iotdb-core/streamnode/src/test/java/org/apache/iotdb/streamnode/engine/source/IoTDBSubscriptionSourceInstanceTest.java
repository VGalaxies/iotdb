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

import org.apache.iotdb.commons.service.metric.MetricService;
import org.apache.iotdb.commons.service.metric.enums.Metric;
import org.apache.iotdb.commons.service.metric.enums.Tag;
import org.apache.iotdb.commons.stream.IoTDBSubscriptionSource;
import org.apache.iotdb.commons.stream.StreamProperties;
import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.config.MetricConfig;
import org.apache.iotdb.metrics.config.MetricConfigDescriptor;
import org.apache.iotdb.metrics.type.Counter;
import org.apache.iotdb.metrics.type.Timer;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.rpc.subscription.exception.SubscriptionConnectionException;
import org.apache.iotdb.session.subscription.consumer.ISubscriptionTablePullConsumer;
import org.apache.iotdb.session.subscription.consumer.table.SubscriptionTablePullConsumer;
import org.apache.iotdb.session.subscription.payload.SubscriptionMessage;
import org.apache.iotdb.streamnode.conf.StreamNodeConfig;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
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

  private static final MetricConfig METRIC_CONFIG =
      MetricConfigDescriptor.getInstance().getMetricConfig();
  private static MetricLevel originalMetricLevel;
  private static AbstractMetricService metricService;

  private ISubscriptionTablePullConsumer mockConsumer;
  private ISubscriptionTablePullConsumer secondMockConsumer;
  private IoTDBSubscriptionSource sourceConfig;
  private StreamProperties streamProperties;
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

  @BeforeClass
  public static void setUpMetricService() {
    originalMetricLevel = METRIC_CONFIG.getMetricLevel();
    METRIC_CONFIG.setMetricLevel(MetricLevel.IMPORTANT);
    metricService = MetricService.getInstance();
    metricService.startService();
  }

  @AfterClass
  public static void tearDownMetricService() {
    metricService.stopService();
    METRIC_CONFIG.setMetricLevel(originalMetricLevel);
  }

  @Before
  public void setUp() throws Exception {
    mockConsumer = mock(ISubscriptionTablePullConsumer.class);
    secondMockConsumer = mock(ISubscriptionTablePullConsumer.class);

    sourceConfig =
        new IoTDBSubscriptionSource(
            DATABASE, TABLE, null, null, "localhost", 6667, DEFAULT_USER, DEFAULT_PASSWORD);

    nodeConfig = mock(StreamNodeConfig.class);
    when(nodeConfig.getSnInternalAddress()).thenReturn("127.0.0.1");
    when(nodeConfig.getSnInternalPort()).thenReturn(9090);

    // Default: consumer returns empty list (no messages)
    when(mockConsumer.poll(any(Duration.class))).thenReturn(Collections.emptyList());

    streamProperties = new StreamProperties();
    streamProperties.setSubscriptionConsensusWalRetentionSizeInBytes(1048576L);
    streamProperties.setSubscriptionConsensusWalRetentionTimeMs(3600000L);
    streamProperties.setSubscriptionPollThreadCount(1);
    instance = new TestableInstance(sourceConfig, streamProperties, TASK_NAME, nodeConfig);
  }

  @After
  public void tearDown() {
    instance.drop();
  }

  @Test
  public void testTopicNameHasStreamPrefix() throws Exception {
    instance.start(dataConsumer);
    Assert.assertEquals(EXPECTED_TOPIC, instance.getTopicName());
  }

  // ======================== Consumer lifecycle ========================

  @Test
  public void testConsumerOpenedAndSubscribed() throws Exception {
    instance.start(dataConsumer);

    verify(mockConsumer).open();
    verify(mockConsumer).subscribe(EXPECTED_TOPIC);
  }

  /** Verifies source metrics record a non-empty poll and are removed when the source stops. */
  @Test
  public void testSubscriptionSourceMetrics() throws Exception {
    final SubscriptionMessage firstMessage = mockMessage(Collections.singletonList(buildTablet(1)));
    final SubscriptionMessage secondMessage =
        mockMessage(Collections.singletonList(buildTablet(1)));
    when(mockConsumer.poll(any(Duration.class)))
        .thenReturn(Arrays.asList(firstMessage, secondMessage))
        .thenReturn(Collections.emptyList());

    instance.start(dataConsumer);
    Awaitility.await().atMost(3, TimeUnit.SECONDS).until(() -> receivedIndices.size() >= 2);

    Assert.assertEquals(
        2.0,
        metricService
            .getAutoGauge(
                Metric.STREAM_SUBSCRIPTION_COMMIT_INDEX.toString(),
                MetricLevel.IMPORTANT,
                Tag.STREAM_NAME.toString(),
                TASK_NAME)
            .getValue(),
        0.0);
    Assert.assertEquals(
        2.0,
        metricService
            .getAutoGauge(
                Metric.STREAM_SUBSCRIPTION_PENDING_MESSAGE_COUNT.toString(),
                MetricLevel.IMPORTANT,
                Tag.STREAM_NAME.toString(),
                TASK_NAME)
            .getValue(),
        0.0);
    final Counter messageCount =
        metricService.getOrCreateCounter(
            Metric.STREAM_SUBSCRIPTION_MESSAGE_COUNT.toString(),
            MetricLevel.IMPORTANT,
            Tag.STREAM_NAME.toString(),
            TASK_NAME);
    final Timer nonEmptyPollTimer =
        metricService.getOrCreateTimer(
            Metric.STREAM_SUBSCRIPTION_NON_EMPTY_POLL_TIME.toString(),
            MetricLevel.IMPORTANT,
            Tag.STREAM_NAME.toString(),
            TASK_NAME);
    Assert.assertEquals(2L, messageCount.getCount());
    Assert.assertEquals(1L, nonEmptyPollTimer.getCount());

    instance.drop();

    Assert.assertEquals(
        0.0,
        metricService
            .getAutoGauge(
                Metric.STREAM_SUBSCRIPTION_COMMIT_INDEX.toString(),
                MetricLevel.IMPORTANT,
                Tag.STREAM_NAME.toString(),
                TASK_NAME)
            .getValue(),
        0.0);
  }

  @Test
  public void testConsumerIdUsesSubscriptionSafeIdentifier() {
    when(nodeConfig.getSnInternalAddress()).thenReturn("127.0.0.1");
    when(nodeConfig.getSnInternalPort()).thenReturn(10820);

    Assert.assertEquals(
        "stream_task_name_127_0_0_1_10820_0",
        IoTDBSubscriptionSourceInstance.buildConsumerId("task-name", nodeConfig, 0));
  }

  /** Verifies missing task and node identity segments use a stable subscription-safe fallback. */
  @Test
  public void testConsumerIdUsesFallbackForMissingSegments() {
    when(nodeConfig.getSnInternalPort()).thenReturn(10820);
    when(nodeConfig.getSnInternalAddress()).thenReturn(null, "");

    Assert.assertEquals(
        "stream_unknown_unknown_10820_0",
        IoTDBSubscriptionSourceInstance.buildConsumerId(null, nodeConfig, 0));
    Assert.assertEquals(
        "stream_unknown_unknown_10820_1",
        IoTDBSubscriptionSourceInstance.buildConsumerId("", nodeConfig, 1));
  }

  /** Verifies the compatibility constructor and real builder preserve the consumer identity. */
  @Test
  public void testCompatibilityConstructorBuildsConfiguredConsumer() {
    final IoTDBSubscriptionSourceInstance sourceInstance =
        new IoTDBSubscriptionSourceInstance(sourceConfig, TASK_NAME, nodeConfig);

    final ISubscriptionTablePullConsumer consumer = sourceInstance.buildConsumer(2);

    Assert.assertSame(sourceConfig, sourceInstance.getSourceConfig());
    Assert.assertEquals("stream_test_task_127_0_0_1_9090_2", consumer.getConsumerId());
    Assert.assertEquals("default_group", consumer.getConsumerGroupId());
  }

  /** Verifies a consumer carries the StreamNode owner identity and scheduling epoch. */
  @Test
  public void testConsumerCarriesOwnerFencingIdentity() {
    when(nodeConfig.getStreamNodeId()).thenReturn(42);
    sourceConfig.setOwnerEpoch(7L);
    final IoTDBSubscriptionSourceInstance sourceInstance =
        new IoTDBSubscriptionSourceInstance(sourceConfig, streamProperties, TASK_NAME, nodeConfig);

    final SubscriptionTablePullConsumer consumer =
        (SubscriptionTablePullConsumer) sourceInstance.buildConsumer(0);

    Assert.assertEquals("127.0.0.1:9090", consumer.getOwnerId());
    Assert.assertEquals(Long.valueOf(7L), consumer.getOwnerEpoch());
  }

  @Test
  public void testPollThreadCountUsesStreamPropertyAndDefaultsToFour() {
    Assert.assertEquals(4, IoTDBSubscriptionSourceInstance.getPollThreadCount(null));
    Assert.assertEquals(
        4, IoTDBSubscriptionSourceInstance.getPollThreadCount(new StreamProperties()));
    streamProperties.setSubscriptionPollThreadCount(0);
    Assert.assertEquals(1, IoTDBSubscriptionSourceInstance.getPollThreadCount(streamProperties));
    streamProperties.setSubscriptionPollThreadCount(3);
    Assert.assertEquals(3, IoTDBSubscriptionSourceInstance.getPollThreadCount(streamProperties));
  }

  @Test
  public void testPollRetryBackoffIsExponentialAndCapped() {
    Assert.assertEquals(1_000L, IoTDBSubscriptionSourceInstance.getPollRetryBackoffMillis(1));
    Assert.assertEquals(2_000L, IoTDBSubscriptionSourceInstance.getPollRetryBackoffMillis(2));
    Assert.assertEquals(30_000L, IoTDBSubscriptionSourceInstance.getPollRetryBackoffMillis(6));
    Assert.assertEquals(
        30_000L, IoTDBSubscriptionSourceInstance.getPollRetryBackoffMillis(Integer.MAX_VALUE));
  }

  @Test
  public void testKnownPollExceptionUsesSummaryWhileUnknownKeepsStackTrace() throws Exception {
    final SubscriptionConnectionException connectionException =
        new SubscriptionConnectionException("connection refused");
    Assert.assertEquals(
        "SubscriptionConnectionException: connection refused",
        IoTDBSubscriptionSourceInstance.getKnownExceptionSummary(connectionException));
    Assert.assertNull(
        IoTDBSubscriptionSourceInstance.getKnownExceptionSummary(
            new IllegalStateException("bad state")));
  }

  @Test
  public void testUpdateUsesLatestSourceAndOwnerEpoch() {
    sourceConfig.setOwnerEpoch(7L);
    final IoTDBSubscriptionSourceInstance sourceInstance =
        new IoTDBSubscriptionSourceInstance(sourceConfig, streamProperties, TASK_NAME, nodeConfig);
    final IoTDBSubscriptionSource updatedSource =
        new IoTDBSubscriptionSource(
            DATABASE, TABLE, null, null, "127.0.0.2", 6668, "updated_user", DEFAULT_PASSWORD);
    updatedSource.setOwnerEpoch(8L);

    sourceInstance.update(updatedSource);

    Assert.assertSame(updatedSource, sourceInstance.getSourceConfig());
    final SubscriptionTablePullConsumer consumer =
        (SubscriptionTablePullConsumer) sourceInstance.buildConsumer(0);
    Assert.assertEquals(Long.valueOf(8L), consumer.getOwnerEpoch());
  }

  @Test
  public void testUpdateOwnerEpochRebuildsConsumersOnRestart() throws Exception {
    instance.start(dataConsumer);
    instance.stop();

    // The task can update the epoch on the same source object. Repeating update must retain
    // the pending rebuild until restart replaces the consumers carrying the old epoch.
    sourceConfig.setOwnerEpoch(8L);
    instance.update(sourceConfig);
    instance.update(sourceConfig);
    instance.start(null);

    verify(mockConsumer).unsubscribe(EXPECTED_TOPIC);
    verify(mockConsumer).close();
    verify(mockConsumer, times(2)).open();
    verify(mockConsumer, times(2)).subscribe(EXPECTED_TOPIC);
  }

  @Test
  public void testUpdateUnchangedSourceKeepsConsumersOnRestart() throws Exception {
    instance.start(dataConsumer);
    instance.stop();
    instance.update(sourceConfig);
    instance.start(null);

    verify(mockConsumer, Mockito.never()).close();
    verify(mockConsumer, times(1)).open();
    verify(mockConsumer, times(1)).subscribe(EXPECTED_TOPIC);
  }

  @Test
  public void testMultiplePollConsumersUseDistinctConsumerIds() {
    Assert.assertNotEquals(
        IoTDBSubscriptionSourceInstance.buildConsumerId(TASK_NAME, nodeConfig, 0),
        IoTDBSubscriptionSourceInstance.buildConsumerId(TASK_NAME, nodeConfig, 1));
  }

  @Test
  public void testStartCreatesConfiguredPollConsumers() throws Exception {
    streamProperties.setSubscriptionPollThreadCount(2);
    instance.start(dataConsumer);

    verify(mockConsumer).open();
    verify(mockConsumer).subscribe(EXPECTED_TOPIC);
    verify(secondMockConsumer).open();
    verify(secondMockConsumer).subscribe(EXPECTED_TOPIC);
  }

  /** Verifies a subscribe failure closes every consumer created before startup aborts. */
  @Test
  public void testStartFailureCleansUpCreatedConsumer() throws Exception {
    streamProperties.setSubscriptionPollThreadCount(2);
    final RuntimeException subscribeFailure = new RuntimeException("subscribe failed");
    doThrow(subscribeFailure).when(secondMockConsumer).subscribe(EXPECTED_TOPIC);

    try {
      instance.start(dataConsumer);
      Assert.fail("Expected subscribe failure to be propagated");
    } catch (final Exception e) {
      Assert.assertSame(subscribeFailure, e);
    }

    verify(mockConsumer).unsubscribe(EXPECTED_TOPIC);
    verify(mockConsumer).close();
    verify(secondMockConsumer).unsubscribe(EXPECTED_TOPIC);
    verify(secondMockConsumer).close();
  }

  @Test
  public void testCommitGroupsMessagesByPollConsumer() throws Exception {
    streamProperties.setSubscriptionPollThreadCount(2);
    final SubscriptionMessage firstMessage = mockMessage(Collections.singletonList(buildTablet(1)));
    final SubscriptionMessage secondMessage =
        mockMessage(Collections.singletonList(buildTablet(1)));
    when(mockConsumer.poll(any(Duration.class)))
        .thenReturn(Collections.singletonList(firstMessage))
        .thenReturn(Collections.emptyList());
    when(secondMockConsumer.poll(any(Duration.class)))
        .thenReturn(Collections.singletonList(secondMessage))
        .thenReturn(Collections.emptyList());

    instance.start(dataConsumer);
    Awaitility.await().atMost(3, TimeUnit.SECONDS).until(() -> receivedIndices.size() >= 2);

    instance.commit(2);

    @SuppressWarnings("unchecked")
    final ArgumentCaptor<List<SubscriptionMessage>> firstCaptor =
        ArgumentCaptor.forClass(List.class);
    @SuppressWarnings("unchecked")
    final ArgumentCaptor<List<SubscriptionMessage>> secondCaptor =
        ArgumentCaptor.forClass(List.class);
    verify(mockConsumer).commitSync(firstCaptor.capture());
    verify(secondMockConsumer).commitSync(secondCaptor.capture());
    Assert.assertEquals(Collections.singletonList(firstMessage), firstCaptor.getValue());
    Assert.assertEquals(Collections.singletonList(secondMessage), secondCaptor.getValue());
  }

  /** Verifies drop releases the subscription consumer instead of retaining it for restart. */
  @Test
  public void testDropUnsubscribesAndClosesConsumer() throws Exception {
    instance.start(dataConsumer);
    instance.drop();

    verify(mockConsumer).unsubscribe(EXPECTED_TOPIC);
    verify(mockConsumer).close();
  }

  /** Verifies start resumes a stopped source without rebuilding its subscription consumer. */
  @Test
  public void testStopAndStartKeepConsumerOpen() throws Exception {
    instance.start(dataConsumer);
    instance.stop();

    verify(mockConsumer, Mockito.never()).unsubscribe(EXPECTED_TOPIC);
    verify(mockConsumer, Mockito.never()).close();

    instance.start(dataConsumer);
    instance.drop();
    verify(mockConsumer, times(1)).open();
    verify(mockConsumer, times(1)).subscribe(EXPECTED_TOPIC);
    verify(mockConsumer).unsubscribe(EXPECTED_TOPIC);
    verify(mockConsumer).close();
  }

  /** Verifies duplicate or out-of-order lifecycle calls are harmless and clean up only once. */
  @Test
  public void testLifecycleOperationsAreIdempotent() throws Exception {
    instance.stop();

    instance.start(dataConsumer);
    instance.start(dataConsumer);
    instance.stop();
    instance.stop();
    instance.start(dataConsumer);
    instance.start(dataConsumer);
    instance.drop();
    instance.drop();

    verify(mockConsumer, times(1)).open();
    verify(mockConsumer, times(1)).subscribe(EXPECTED_TOPIC);
    verify(mockConsumer, times(1)).unsubscribe(EXPECTED_TOPIC);
    verify(mockConsumer, times(1)).close();
  }

  /** Verifies cleanup exceptions from one consumer do not prevent cleanup of another consumer. */
  @Test
  public void testDropIsolatesConsumerCleanupFailures() throws Exception {
    streamProperties.setSubscriptionPollThreadCount(2);
    // Configure failures before poll threads start because Mockito stubbing on a shared mock is not
    // thread-safe.
    doThrow(new RuntimeException("unsubscribe failed"))
        .when(mockConsumer)
        .unsubscribe(EXPECTED_TOPIC);
    doThrow(new RuntimeException("close failed")).when(mockConsumer).close();
    instance.start(dataConsumer);

    instance.drop();

    verify(mockConsumer).close();
    verify(secondMockConsumer).unsubscribe(EXPECTED_TOPIC);
    verify(secondMockConsumer).close();
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

    instance.start(dataConsumer);

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

    instance.start(dataConsumer);
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

    instance.start(dataConsumer);
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

    instance.start(dataConsumer);
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

    instance.start(dataConsumer);
    Awaitility.await().atMost(3, TimeUnit.SECONDS).until(() -> receivedIndices.size() >= 1);

    verify(msg, Mockito.timeout(3000)).removeUserData();
  }

  /** Verifies null and exceptional poll results are retried until a valid message is delivered. */
  @Test
  public void testPollLoopRecoversFromNullAndException() throws Exception {
    final SubscriptionMessage message = mockMessage(Collections.singletonList(buildTablet(1)));
    when(mockConsumer.poll(any(Duration.class)))
        .thenReturn(null)
        .thenThrow(new RuntimeException("temporary poll failure"))
        .thenReturn(Collections.singletonList(message))
        .thenReturn(Collections.emptyList());

    instance.start(dataConsumer);

    Awaitility.await().atMost(3, TimeUnit.SECONDS).until(() -> receivedIndices.size() == 1);
    verify(mockConsumer, atLeast(3)).poll(any(Duration.class));
  }

  /** Verifies a synchronous downstream acknowledgement is retained until its message is ready. */
  @Test
  public void testSynchronousCommitDuringDeliveryIsNotLost() throws Exception {
    final SubscriptionMessage message = mockMessage(Collections.singletonList(buildTablet(1)));
    when(mockConsumer.poll(any(Duration.class)))
        .thenReturn(Collections.singletonList(message))
        .thenReturn(Collections.emptyList());

    instance.start((tablet, index) -> instance.commit(index));

    @SuppressWarnings("unchecked")
    final ArgumentCaptor<List<SubscriptionMessage>> captor = ArgumentCaptor.forClass(List.class);
    verify(mockConsumer, Mockito.timeout(3000)).commitSync(captor.capture());
    Assert.assertEquals(Collections.singletonList(message), captor.getValue());
  }

  /**
   * Verifies downstream delivery failure removes the message instead of committing partial work.
   */
  @Test
  public void testDeliveryFailureRemovesPendingCommit() throws Exception {
    final AtomicBoolean deliveryAttempted = new AtomicBoolean(false);
    final SubscriptionMessage message = mockMessage(Collections.singletonList(buildTablet(1)));
    when(mockConsumer.poll(any(Duration.class)))
        .thenReturn(Collections.singletonList(message))
        .thenReturn(Collections.emptyList());

    instance.start(
        (tablet, index) -> {
          deliveryAttempted.set(true);
          throw new IllegalStateException("downstream rejected tablet");
        });

    Awaitility.await()
        .atMost(3, TimeUnit.SECONDS)
        .until(
            () ->
                deliveryAttempted.get()
                    && metricService
                            .getAutoGauge(
                                Metric.STREAM_SUBSCRIPTION_PENDING_MESSAGE_COUNT.toString(),
                                MetricLevel.IMPORTANT,
                                Tag.STREAM_NAME.toString(),
                                TASK_NAME)
                            .getValue()
                        == 0.0);
    instance.commit(Long.MAX_VALUE);

    verify(mockConsumer, Mockito.never()).commitSync(any(List.class));
    verify(message, Mockito.never()).removeUserData();
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

    instance.start(dataConsumer);
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

    instance.start(dataConsumer);
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

    instance.start(dataConsumer);
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
        final StreamProperties streamProperties,
        final String taskName,
        final StreamNodeConfig nodeConfig) {
      super(sourceConfig, streamProperties, taskName, nodeConfig);
    }

    @Override
    protected ISubscriptionTablePullConsumer buildConsumer(final int pollThreadIndex) {
      return pollThreadIndex == 0 ? mockConsumer : secondMockConsumer;
    }
  }
}
