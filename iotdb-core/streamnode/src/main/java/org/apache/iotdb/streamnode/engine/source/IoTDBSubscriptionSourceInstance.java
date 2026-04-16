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
import org.apache.iotdb.streamnode.conf.StreamNodeDescriptor;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.write.record.Tablet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
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
   * SubscriptionMessage bound to the highest commitIndex assigned to any of its Tablets.
   * The commit loop drains this queue once downstream processing has acknowledged the index.
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
    try (final ISubscriptionTableSession session =
        new SubscriptionTableSessionBuilder()
            .host(sourceConfig.getHost())
            .port(sourceConfig.getRpcPort())
            .username(sourceConfig.getUser())
            .password(sourceConfig.getEncryptedPassword())
            .build()) {
      session.open();
      final Properties topicProperties = new Properties();
      topicProperties.setProperty(TopicConstant.DATABASE_KEY, sourceConfig.getDatabase());
      topicProperties.setProperty(TopicConstant.TABLE_KEY, sourceConfig.getTableName());
      // TODO: preFilter is not yet supported as a topic property;
      session.createTopicIfNotExists(topicName, topicProperties);
      LOGGER.info("Topic '{}' created (or already exists) for task {}", topicName, taskName);
    }

    // TODO: Create subscription consumer, subscribe to topic, start polling
  }

  @Override
  public void stop() throws Exception {
    // TODO: Unsubscribe and close consumer
    LOGGER.info("Stopping subscription source for task {}", taskName);
  }

  @Override
  public void commit(final long commitIndex) throws Exception {
    // TODO: Commit offset to subscription
    LOGGER.debug("Committed offset {} for task {}", commitIndex, taskName);
  }

  public IoTDBSubscriptionSource getSourceConfig() {
    return sourceConfig;
  }

  public String getTopicName() {
    return topicName;
  }
}
