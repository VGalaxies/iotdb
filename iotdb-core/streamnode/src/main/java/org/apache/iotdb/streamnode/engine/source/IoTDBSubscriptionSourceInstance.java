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

import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.commons.service.metric.MetricService;
import org.apache.iotdb.commons.service.metric.enums.Metric;
import org.apache.iotdb.commons.service.metric.enums.Tag;
import org.apache.iotdb.commons.stream.IoTDBSubscriptionSource;
import org.apache.iotdb.commons.stream.StreamProperties;
import org.apache.iotdb.commons.stream.StreamSource;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.metrics.type.Counter;
import org.apache.iotdb.metrics.type.Timer;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.subscription.exception.SubscriptionConnectionException;
import org.apache.iotdb.rpc.subscription.exception.SubscriptionTimeoutException;
import org.apache.iotdb.session.subscription.consumer.ISubscriptionTablePullConsumer;
import org.apache.iotdb.session.subscription.consumer.table.SubscriptionTablePullConsumerBuilder;
import org.apache.iotdb.session.subscription.payload.SubscriptionMessage;
import org.apache.iotdb.streamnode.conf.StreamNodeConfig;

import org.apache.thrift.transport.TTransportException;
import org.apache.tsfile.write.record.Tablet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.ConnectException;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;

import static org.apache.iotdb.streamnode.i18n.StreamNodeMessages.LOG_COMMITTED_ARG_MESSAGES_UP_TO_INDEX_ARG_FOR_TASK_ARG_4ADEA111;
import static org.apache.iotdb.streamnode.i18n.StreamNodeMessages.LOG_ERROR_CLOSING_CONSUMER_FOR_TASK_ARG_7849F85A;
import static org.apache.iotdb.streamnode.i18n.StreamNodeMessages.LOG_ERROR_IN_POLL_LOOP_FOR_TASK_ARG_RETRYING_AFTER_ARG_MS_ARG_CC0816C0;
import static org.apache.iotdb.streamnode.i18n.StreamNodeMessages.LOG_ERROR_UNSUBSCRIBING_CONSUMER_FOR_TASK_ARG_48E84DCA;
import static org.apache.iotdb.streamnode.i18n.StreamNodeMessages.LOG_POLL_LOOP_EXITED_FOR_TASK_ARG_D704FCD7;
import static org.apache.iotdb.streamnode.i18n.StreamNodeMessages.LOG_POLL_LOOP_STARTED_FOR_TASK_ARG_B3559D6E;
import static org.apache.iotdb.streamnode.i18n.StreamNodeMessages.LOG_STARTING_SUBSCRIPTION_SOURCE_FOR_TASK_ARG_TABLE_ARG_ARG_73C3F127;
import static org.apache.iotdb.streamnode.i18n.StreamNodeMessages.LOG_SUBSCRIPTION_SOURCE_DROPPED_FOR_TASK_ARG_8F7CBCF7;
import static org.apache.iotdb.streamnode.i18n.StreamNodeMessages.LOG_SUBSCRIPTION_SOURCE_RESUMED_FOR_TASK_ARG_147E9AFE;
import static org.apache.iotdb.streamnode.i18n.StreamNodeMessages.LOG_SUBSCRIPTION_SOURCE_STOPPED_FOR_TASK_ARG_0908AD57;

public class IoTDBSubscriptionSourceInstance extends StreamSourceInstance {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(IoTDBSubscriptionSourceInstance.class);

  /** Shared thread pool across all instances — one thread per active source. */
  private static final ExecutorService POLL_EXECUTOR =
      IoTDBThreadPoolFactory.newCachedThreadPoolWithDaemon(
          ThreadName.STREAM_SUBSCRIPTION_POLL.getName());

  private static final Duration POLL_TIMEOUT = Duration.ofMillis(500);
  private static final int DEFAULT_POLL_THREAD_COUNT = 4;
  private static final long POLL_RETRY_BACKOFF_BASE_MS = 1_000L;
  private static final long POLL_RETRY_BACKOFF_MAX_MS = 30_000L;

  private IoTDBSubscriptionSource sourceConfig;
  private final StreamProperties streamProperties;
  private final String taskName;
  private final String topicName;
  private final StreamNodeConfig nodeConfig;
  private volatile long ownerEpoch;
  private boolean rebuildConsumersOnRestart;

  private final List<ConsumerState> consumerStates = new ArrayList<>();

  /** Protects consumer state and poll future lifecycle operations. */
  private final Object consumerStateLock = new Object();

  private final AtomicBoolean running = new AtomicBoolean(false);
  private final AtomicBoolean stopped = new AtomicBoolean(false);
  private final AtomicLong commitIndex = new AtomicLong(0);
  private final AtomicLong requestedCommitIndex = new AtomicLong(-1);
  private final Object pendingCommitLock = new Object();
  private Counter messageCount;
  private Timer nonEmptyPollTimer;

  /**
   * Queue of (message, maxIndex) pairs pending commit. Each entry represents one polled
   * SubscriptionMessage bound to the highest commitIndex assigned to any of its Tablets. The commit
   * loop drains this queue once downstream processing has acknowledged the index.
   */
  private final PriorityBlockingQueue<PendingCommit> pendingCommits =
      new PriorityBlockingQueue<>(11, Comparator.comparingLong(commit -> commit.maxIndex));

  @TestOnly
  public IoTDBSubscriptionSourceInstance(
      final IoTDBSubscriptionSource sourceConfig,
      final String taskName,
      final StreamNodeConfig streamNodeConfig) {
    this(sourceConfig, null, taskName, streamNodeConfig);
  }

  public IoTDBSubscriptionSourceInstance(
      final IoTDBSubscriptionSource sourceConfig,
      final StreamProperties streamProperties,
      final String taskName,
      final StreamNodeConfig streamNodeConfig) {
    this.sourceConfig = sourceConfig;
    this.streamProperties = streamProperties;
    this.taskName = taskName;
    this.topicName = IoTDBSubscriptionSource.getTopicName(taskName);
    this.nodeConfig = streamNodeConfig;
    this.ownerEpoch = sourceConfig.getOwnerEpoch();
  }

  @Override
  public void start(final BiConsumer<Tablet, Long> dataConsumer) throws Exception {
    synchronized (consumerStateLock) {
      if (running.get()) {
        return;
      }
      if (dataConsumer != null) {
        this.dataConsumer = dataConsumer;
      }
      if (stopped.compareAndSet(true, false)) {
        if (rebuildConsumersOnRestart) {
          closeConsumers();
          consumerStates.clear();
          pendingCommits.clear();
          requestedCommitIndex.set(-1);
          try {
            createConsumers();
            rebuildConsumersOnRestart = false;
          } catch (final Exception e) {
            drop();
            throw e;
          }
        }
        startPollLoops();
        LOGGER.info(LOG_SUBSCRIPTION_SOURCE_RESUMED_FOR_TASK_ARG_147E9AFE, taskName);
        return;
      }

      LOGGER.info(
          LOG_STARTING_SUBSCRIPTION_SOURCE_FOR_TASK_ARG_TABLE_ARG_ARG_73C3F127,
          taskName,
          sourceConfig.getDatabase(),
          sourceConfig.getTableName());

      // The ConfigNode creates and assigns the topic before dispatching this task. Build one
      // consumer per configured poll thread so the server can distribute topic partitions across
      // consumers in the same group.
      try {
        createConsumers();
      } catch (final Exception e) {
        drop();
        throw e;
      }

      // 3. Start poll loops in the shared thread pool
      registerMetrics();
      startPollLoops();
    }
    LOGGER.info(LOG_POLL_LOOP_STARTED_FOR_TASK_ARG_B3559D6E, taskName);
  }

  private void createConsumers() {
    for (int i = 0; i < getPollThreadCount(streamProperties); i++) {
      final ISubscriptionTablePullConsumer consumer = buildConsumer(i);
      final ConsumerState state = new ConsumerState(consumer);
      consumerStates.add(state);
      consumer.open();
      consumer.subscribe(topicName);
    }
  }

  private void startPollLoops() {
    running.set(true);
    consumerStates.forEach(state -> state.pollFuture = POLL_EXECUTOR.submit(() -> pollLoop(state)));
  }

  private void pollLoop(final ConsumerState state) {
    int consecutiveFailures = 0;
    while (running.get()) {
      try {
        final long pollStartTime = System.nanoTime();
        final List<SubscriptionMessage> messages = state.consumer.poll(POLL_TIMEOUT);
        consecutiveFailures = 0;
        if (messages == null || messages.isEmpty()) {
          continue;
        }
        nonEmptyPollTimer.updateNanos(System.nanoTime() - pollStartTime);
        messageCount.inc(messages.size());
        for (final SubscriptionMessage message : messages) {
          processMessage(state, message);
        }
      } catch (final Exception e) {
        if (running.get()) {
          final long retryBackoffMs = getPollRetryBackoffMillis(++consecutiveFailures);
          final String exceptionSummary = getKnownExceptionSummary(e);
          if (exceptionSummary == null) {
            LOGGER.warn(
                LOG_ERROR_IN_POLL_LOOP_FOR_TASK_ARG_RETRYING_AFTER_ARG_MS_ARG_CC0816C0,
                taskName,
                retryBackoffMs,
                e.getClass().getSimpleName() + ": " + String.valueOf(e.getMessage()),
                e);
          } else {
            LOGGER.warn(
                LOG_ERROR_IN_POLL_LOOP_FOR_TASK_ARG_RETRYING_AFTER_ARG_MS_ARG_CC0816C0,
                taskName,
                retryBackoffMs,
                exceptionSummary);
          }
          try {
            TimeUnit.MILLISECONDS.sleep(retryBackoffMs);
          } catch (final InterruptedException interruptedException) {
            Thread.currentThread().interrupt();
            break;
          }
        }
      }
    }
    LOGGER.info(LOG_POLL_LOOP_EXITED_FOR_TASK_ARG_D704FCD7, taskName);
  }

  static long getPollRetryBackoffMillis(final int consecutiveFailures) {
    final int shift = Math.min(Math.max(consecutiveFailures - 1, 0), 5);
    return Math.min(POLL_RETRY_BACKOFF_MAX_MS, POLL_RETRY_BACKOFF_BASE_MS << shift);
  }

  static String getKnownExceptionSummary(final Throwable exception) {
    Throwable current = exception;
    while (current != null) {
      if (isKnownPollException(current)) {
        return current.getClass().getSimpleName() + ": " + String.valueOf(current.getMessage());
      }
      current = current.getCause();
    }
    return null;
  }

  private static boolean isKnownPollException(final Throwable exception) {
    return exception instanceof SubscriptionConnectionException
        || exception instanceof SubscriptionTimeoutException
        || exception instanceof IoTDBConnectionException
        || exception instanceof TTransportException
        || exception instanceof ConnectException
        || exception instanceof SocketException
        || exception instanceof SocketTimeoutException
        || exception instanceof UnknownHostException;
  }

  private void processMessage(final ConsumerState state, final SubscriptionMessage message) {
    final List<Tablet> tablets = new ArrayList<>();
    final Iterator<Tablet> tabletIterator = message.getRecordTabletIterator();
    tabletIterator.forEachRemaining(tablets::add);
    if (tablets.isEmpty()) {
      message.removeUserData();
      return;
    }

    // Reserve and enqueue the complete message before dispatching any tablet. Downstream commit
    // callbacks can run synchronously, so registering it first prevents a fast callback from
    // racing ahead of the queue insertion and losing the commit.
    final long firstIndex;
    final PendingCommit pendingCommit;
    synchronized (pendingCommitLock) {
      firstIndex = commitIndex.getAndAdd(tablets.size()) + 1;
      pendingCommit = new PendingCommit(state.consumer, message, firstIndex + tablets.size() - 1);
      pendingCommits.put(pendingCommit);
    }
    try {
      for (int i = 0; i < tablets.size(); i++) {
        dataConsumer.accept(tablets.get(i), firstIndex + i);
      }
      message.removeUserData();
      pendingCommit.ready = true;
      drainReadyCommits();
    } catch (final RuntimeException | Error e) {
      pendingCommits.remove(pendingCommit);
      throw e;
    }
  }

  @Override
  public void stop() {
    synchronized (consumerStateLock) {
      if (!running.compareAndSet(true, false)) {
        return;
      }
      stopped.set(true);
      consumerStates.forEach(
          state -> {
            if (state.pollFuture != null) {
              state.pollFuture.cancel(true);
              state.pollFuture = null;
            }
          });
    }
    LOGGER.info(LOG_SUBSCRIPTION_SOURCE_STOPPED_FOR_TASK_ARG_0908AD57, taskName);
  }

  @Override
  public void drop() {
    synchronized (consumerStateLock) {
      running.set(false);
      stopped.set(false);
      closeConsumers();
      consumerStates.clear();
      rebuildConsumersOnRestart = false;
    }
    pendingCommits.clear();
    requestedCommitIndex.set(-1);
    deregisterMetrics();
    LOGGER.info(LOG_SUBSCRIPTION_SOURCE_DROPPED_FOR_TASK_ARG_8F7CBCF7, taskName);
  }

  private void registerMetrics() {
    MetricService.getInstance()
        .createAutoGauge(
            Metric.STREAM_SUBSCRIPTION_COMMIT_INDEX.toString(),
            MetricLevel.IMPORTANT,
            commitIndex,
            AtomicLong::get,
            Tag.STREAM_NAME.toString(),
            taskName);
    MetricService.getInstance()
        .createAutoGauge(
            Metric.STREAM_SUBSCRIPTION_PENDING_MESSAGE_COUNT.toString(),
            MetricLevel.IMPORTANT,
            pendingCommits,
            PriorityBlockingQueue::size,
            Tag.STREAM_NAME.toString(),
            taskName);
    nonEmptyPollTimer =
        MetricService.getInstance()
            .getOrCreateTimer(
                Metric.STREAM_SUBSCRIPTION_NON_EMPTY_POLL_TIME.toString(),
                MetricLevel.IMPORTANT,
                Tag.STREAM_NAME.toString(),
                taskName);
    messageCount =
        MetricService.getInstance()
            .getOrCreateCounter(
                Metric.STREAM_SUBSCRIPTION_MESSAGE_COUNT.toString(),
                MetricLevel.IMPORTANT,
                Tag.STREAM_NAME.toString(),
                taskName);
  }

  private void deregisterMetrics() {
    MetricService.getInstance()
        .remove(
            MetricType.AUTO_GAUGE,
            Metric.STREAM_SUBSCRIPTION_COMMIT_INDEX.toString(),
            Tag.STREAM_NAME.toString(),
            taskName);
    MetricService.getInstance()
        .remove(
            MetricType.AUTO_GAUGE,
            Metric.STREAM_SUBSCRIPTION_PENDING_MESSAGE_COUNT.toString(),
            Tag.STREAM_NAME.toString(),
            taskName);
    MetricService.getInstance()
        .remove(
            MetricType.TIMER,
            Metric.STREAM_SUBSCRIPTION_NON_EMPTY_POLL_TIME.toString(),
            Tag.STREAM_NAME.toString(),
            taskName);
    MetricService.getInstance()
        .remove(
            MetricType.COUNTER,
            Metric.STREAM_SUBSCRIPTION_MESSAGE_COUNT.toString(),
            Tag.STREAM_NAME.toString(),
            taskName);
  }

  @Override
  public synchronized void commit(final long idx) {
    // The commit callback can arrive while a poll thread is dispatching a message. Retain the
    // highest requested index and drain it again when that message becomes ready.
    requestedCommitIndex.accumulateAndGet(idx, Math::max);
    drainReadyCommits();
  }

  private synchronized void drainReadyCommits() {
    final Map<ISubscriptionTablePullConsumer, List<SubscriptionMessage>> toCommit =
        new IdentityHashMap<>();
    synchronized (pendingCommitLock) {
      PendingCommit head;
      while ((head = pendingCommits.peek()) != null
          && head.maxIndex <= requestedCommitIndex.get()
          && head.ready) {
        head = pendingCommits.poll();
        if (head != null) {
          toCommit.computeIfAbsent(head.consumer, ignored -> new ArrayList<>()).add(head.message);
        }
      }
    }
    if (!toCommit.isEmpty()) {
      int totalMessageCount = 0;
      for (Map.Entry<ISubscriptionTablePullConsumer, List<SubscriptionMessage>> entry :
          toCommit.entrySet()) {
        entry.getKey().commitSync(entry.getValue());
        totalMessageCount += entry.getValue().size();
      }
      LOGGER.debug(
          LOG_COMMITTED_ARG_MESSAGES_UP_TO_INDEX_ARG_FOR_TASK_ARG_4ADEA111,
          totalMessageCount,
          requestedCommitIndex.get(),
          taskName);
    }
  }

  /** Creates the pull consumer for the given poll thread. Overridable for testing. */
  protected ISubscriptionTablePullConsumer buildConsumer(final int pollThreadIndex) {
    final SubscriptionTablePullConsumerBuilder builder =
        new SubscriptionTablePullConsumerBuilder()
            .host(sourceConfig.getHost())
            .port(sourceConfig.getRpcPort())
            .username(sourceConfig.getUser())
            .encryptedPassword(sourceConfig.getEncryptedPassword())
            .consumerGroupId("default_group")
            .consumerId(buildConsumerId(taskName, nodeConfig, pollThreadIndex))
            .autoCommit(false);
    if (ownerEpoch >= 0) {
      builder.ownerId(buildOwnerId(nodeConfig)).ownerEpoch(ownerEpoch);
    }
    return builder.build();
  }

  static String buildOwnerId(final StreamNodeConfig nodeConfig) {
    return String.valueOf(nodeConfig.getSnInternalAddress() + ":" + nodeConfig.getSnInternalPort());
  }

  @Override
  public void update(final StreamSource source) {
    final IoTDBSubscriptionSource updatedSource = (IoTDBSubscriptionSource) source;
    synchronized (consumerStateLock) {
      final long updatedOwnerEpoch = updatedSource.getOwnerEpoch();
      if (sourceConfig == updatedSource && ownerEpoch == updatedOwnerEpoch) {
        return;
      }
      sourceConfig = updatedSource;
      ownerEpoch = updatedOwnerEpoch;
      rebuildConsumersOnRestart = !consumerStates.isEmpty();
    }
  }

  private void closeConsumers() {
    consumerStates.forEach(
        state -> {
          if (state.pollFuture != null) {
            state.pollFuture.cancel(true);
            state.pollFuture = null;
          }
          try {
            state.consumer.unsubscribe(topicName);
          } catch (final Exception e) {
            LOGGER.warn(LOG_ERROR_UNSUBSCRIBING_CONSUMER_FOR_TASK_ARG_48E84DCA, taskName, e);
          }
          try {
            state.consumer.close();
          } catch (final Exception e) {
            LOGGER.warn(LOG_ERROR_CLOSING_CONSUMER_FOR_TASK_ARG_7849F85A, taskName, e);
          }
        });
  }

  static int getPollThreadCount(final StreamProperties streamProperties) {
    if (streamProperties == null || streamProperties.getSubscriptionPollThreadCount() == null) {
      return DEFAULT_POLL_THREAD_COUNT;
    }
    return Math.max(streamProperties.getSubscriptionPollThreadCount(), 1);
  }

  static String buildConsumerId(
      final String taskName, final StreamNodeConfig nodeConfig, final int pollThreadIndex) {
    return "stream_"
        + sanitizeIdentifierSegment(taskName)
        + "_"
        + sanitizeIdentifierSegment(nodeConfig.getSnInternalAddress())
        + "_"
        + nodeConfig.getSnInternalPort()
        + "_"
        + pollThreadIndex;
  }

  private static class ConsumerState {
    private final ISubscriptionTablePullConsumer consumer;
    private Future<?> pollFuture;

    private ConsumerState(final ISubscriptionTablePullConsumer consumer) {
      this.consumer = consumer;
    }
  }

  private static class PendingCommit {
    private final ISubscriptionTablePullConsumer consumer;
    private final SubscriptionMessage message;
    private final long maxIndex;
    private volatile boolean ready;

    private PendingCommit(
        final ISubscriptionTablePullConsumer consumer,
        final SubscriptionMessage message,
        final long maxIndex) {
      this.consumer = consumer;
      this.message = message;
      this.maxIndex = maxIndex;
    }
  }

  private static String sanitizeIdentifierSegment(final String rawSegment) {
    if (rawSegment == null || rawSegment.isEmpty()) {
      return "unknown";
    }

    final StringBuilder builder = new StringBuilder(rawSegment.length());
    for (int i = 0; i < rawSegment.length(); i++) {
      final char ch = rawSegment.charAt(i);
      if (ch == '_' || Character.isLetterOrDigit(ch)) {
        builder.append(ch);
      } else {
        builder.append('_');
      }
    }
    return builder.length() == 0 ? "unknown" : builder.toString();
  }

  @TestOnly
  public IoTDBSubscriptionSource getSourceConfig() {
    return sourceConfig;
  }

  @TestOnly
  public String getTopicName() {
    return topicName;
  }
}
