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
import org.apache.iotdb.commons.stream.IoTDBSubscriptionSource;
import org.apache.iotdb.rpc.subscription.config.TopicConstant;
import org.apache.iotdb.session.subscription.ISubscriptionTableSession;
import org.apache.iotdb.session.subscription.SubscriptionTableSessionBuilder;
import org.apache.iotdb.session.subscription.consumer.ISubscriptionTablePullConsumer;
import org.apache.iotdb.session.subscription.consumer.table.SubscriptionTablePullConsumerBuilder;
import org.apache.iotdb.session.subscription.payload.SubscriptionMessage;
import org.apache.iotdb.streamnode.conf.StreamNodeConfig;

import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.write.record.Tablet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;

public class IoTDBSubscriptionSourceInstance extends StreamSourceInstance {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(IoTDBSubscriptionSourceInstance.class);

  /** Prefix for all stream-managed subscription topics. */
  private static final String STREAM_TOPIC_PREFIX = "__stream__";

  /** Shared thread pool across all instances — one thread per active source. */
  private static final ExecutorService POLL_EXECUTOR =
      IoTDBThreadPoolFactory.newCachedThreadPoolWithDaemon(
          ThreadName.STREAM_SUBSCRIPTION_POLL.getName());

  private static final Duration POLL_TIMEOUT = Duration.ofMillis(500);

  private final IoTDBSubscriptionSource sourceConfig;
  private final String taskName;
  private final String topicName;
  private final StreamNodeConfig nodeConfig;

  private volatile ISubscriptionTablePullConsumer consumer;
  private final AtomicBoolean running = new AtomicBoolean(false);
  private final AtomicLong commitIndex = new AtomicLong(0);
  private Future<?> pollFuture;

  /**
   * Queue of (message, maxIndex) pairs pending commit. Each entry represents one polled
   * SubscriptionMessage bound to the highest commitIndex assigned to any of its Tablets. The commit
   * loop drains this queue once downstream processing has acknowledged the index.
   */
  private final LinkedBlockingQueue<Pair<SubscriptionMessage, Long>> pendingCommits =
      new LinkedBlockingQueue<>();

  public IoTDBSubscriptionSourceInstance(
      final IoTDBSubscriptionSource sourceConfig,
      final String taskName,
      final BiConsumer<Tablet, Long> dataConsumer,
      final StreamNodeConfig streamNodeConfig) {
    this.sourceConfig = sourceConfig;
    this.taskName = taskName;
    this.topicName = STREAM_TOPIC_PREFIX + taskName;
    this.dataConsumer = dataConsumer;
    this.nodeConfig = streamNodeConfig;
  }

  @Override
  public void start() throws Exception {
    LOGGER.info(
        "Starting subscription source for task {}, table: {}.{}",
        taskName,
        sourceConfig.getDatabase(),
        sourceConfig.getTableName());

    // 1. Create topic if not exists
    try (final ISubscriptionTableSession session = buildSession()) {
      session.open();
      final Properties topicProperties = new Properties();
      topicProperties.setProperty(TopicConstant.DATABASE_KEY, sourceConfig.getDatabase());
      topicProperties.setProperty(TopicConstant.TABLE_KEY, sourceConfig.getTableName());
      // preFilter is not yet supported as a topic property; applied at consumer side in future
      session.createTopicIfNotExists(topicName, topicProperties);
      LOGGER.info("Topic '{}' created (or already exists) for task {}", topicName, taskName);
    }

    // 2. Build and open consumer
    consumer = buildConsumer();
    consumer.open();
    consumer.subscribe(topicName);

    // 3. Start poll loop in shared thread pool
    running.set(true);
    pollFuture = POLL_EXECUTOR.submit(this::pollLoop);
    LOGGER.info("Poll loop started for task {}", taskName);
  }

  private void pollLoop() {
    while (running.get()) {
      try {
        final List<SubscriptionMessage> messages = consumer.poll(POLL_TIMEOUT);
        if (messages == null || messages.isEmpty()) {
          continue;
        }
        for (final SubscriptionMessage message : messages) {
          long maxIdx = -1;
          final Iterator<Tablet> tablets = message.getRecordTabletIterator();
          while (tablets.hasNext()) {
            final long idx = commitIndex.incrementAndGet();
            dataConsumer.accept(tablets.next(), idx);
            if (idx > maxIdx) {
              maxIdx = idx;
            }
          }
          if (maxIdx >= 0) {
            pendingCommits.put(new Pair<>(message, maxIdx));
          }
        }
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      } catch (final Exception e) {
        if (running.get()) {
          LOGGER.warn("Error in poll loop for task {}, will retry", taskName, e);
        }
      }
    }
    LOGGER.info("Poll loop exited for task {}", taskName);
  }

  @Override
  public void stop() {
    running.set(false);
    if (pollFuture != null) {
      pollFuture.cancel(true);
    }
    if (consumer != null) {
      try {
        consumer.unsubscribe(topicName);
        consumer.close();
      } catch (final Exception e) {
        LOGGER.warn("Error closing consumer for task {}", taskName, e);
      }
    }
    LOGGER.info("Subscription source stopped for task {}", taskName);
  }

  @Override
  public synchronized void commit(final long idx) {
    // Drain all pending entries whose maxIndex <= idx and commit them to the subscription
    final List<SubscriptionMessage> toCommit = new ArrayList<>();
    Pair<SubscriptionMessage, Long> head;
    while ((head = pendingCommits.peek()) != null && head.right <= idx) {
      head = pendingCommits.poll();
      if (head != null) {
        toCommit.add(head.left);
      }
    }
    if (!toCommit.isEmpty()) {
      consumer.commitSync(toCommit);
      LOGGER.debug(
          "Committed {} messages up to index {} for task {}", toCommit.size(), idx, taskName);
    }
  }

  /** Creates the pull consumer. Overridable for testing. */
  protected ISubscriptionTablePullConsumer buildConsumer() {
    return new SubscriptionTablePullConsumerBuilder()
        .host(sourceConfig.getHost())
        .port(sourceConfig.getRpcPort())
        .username(sourceConfig.getUser())
        // TODO: use encrypted password after supporting it in session builder
        .password(sourceConfig.getEncryptedPassword())
        .consumerGroupId("default_group")
        .consumerId(nodeConfig.getSnInternalAddress() + ":" + nodeConfig.getSnInternalPort())
        .autoCommit(false)
        .build();
  }

  /** Creates the session used for topic management. Overridable for testing. */
  protected ISubscriptionTableSession buildSession() throws Exception {
    return new SubscriptionTableSessionBuilder()
        .host(sourceConfig.getHost())
        .port(sourceConfig.getRpcPort())
        .username(sourceConfig.getUser())
        // TODO: use encrypted password after supporting it in session builder
        .password(sourceConfig.getEncryptedPassword())
        .build();
  }

  public IoTDBSubscriptionSource getSourceConfig() {
    return sourceConfig;
  }

  public String getTopicName() {
    return topicName;
  }
}
