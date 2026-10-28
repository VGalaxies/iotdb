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

package org.apache.iotdb.streamnode.it;

import org.apache.iotdb.commons.schema.table.InformationSchema;
import org.apache.iotdb.commons.stream.IoTDBSubscriptionSource;
import org.apache.iotdb.isession.ITableSession;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.session.subscription.ISubscriptionTableSession;
import org.apache.iotdb.session.subscription.SubscriptionTableSessionBuilder;
import org.apache.iotdb.session.subscription.consumer.base.SubscriptionExecutorServiceManager;

import org.awaitility.Awaitility;
import org.awaitility.core.ConditionFactory;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public abstract class AbstractStreamNodeIT {

  private final List<String> createdTopicNames = new ArrayList<>();

  protected static final ConditionFactory AWAIT =
      Awaitility.await()
          .pollInSameThread()
          .pollDelay(1, TimeUnit.SECONDS)
          .pollInterval(1, TimeUnit.SECONDS)
          .atMost(600, TimeUnit.SECONDS);

  @BeforeClass
  public static void setUpCluster() throws Exception {
    SubscriptionExecutorServiceManager.setControlFlowExecutorCorePoolSize(1);
    SubscriptionExecutorServiceManager.setUpstreamDataFlowExecutorCorePoolSize(1);
    SubscriptionExecutorServiceManager.setDownstreamDataFlowExecutorCorePoolSize(1);

    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setSubscriptionEnabled(true)
        .setPipeMemoryManagementEnabled(false)
        .setIsPipeEnableMemoryCheck(false)
        // Subscription topics use mode=consensus, which requires IoTConsensus on data regions.
        .setDataRegionConsensusProtocolClass("org.apache.iotdb.consensus.iot.IoTConsensus");

    EnvFactory.getEnv().initClusterEnvironment();
  }

  @After
  public void cleanUpTestData() throws Exception {
    final String host = EnvFactory.getEnv().getIP();
    final int port = Integer.parseInt(EnvFactory.getEnv().getPort());
    try (final ISubscriptionTableSession session =
        new SubscriptionTableSessionBuilder().host(host).port(port).build()) {
      for (final String topicName : createdTopicNames) {
        session.dropTopic(topicName);
      }
    } finally {
      createdTopicNames.clear();
    }

    try (final ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      final List<String> streamNames = new ArrayList<>();
      try (final SessionDataSet dataSet = session.executeQueryStatement("SHOW STREAMS")) {
        while (dataSet.hasNext()) {
          streamNames.add(dataSet.next().getFields().get(0).getStringValue());
        }
      }
      for (final String streamName : streamNames) {
        session.executeNonQueryStatement("DROP STREAM " + streamName);
      }

      final List<String> databaseNames = new ArrayList<>();
      try (final SessionDataSet dataSet = session.executeQueryStatement("SHOW DATABASES")) {
        while (dataSet.hasNext()) {
          final String databaseName = dataSet.next().getFields().get(0).getStringValue();
          if (!InformationSchema.INFORMATION_DATABASE.equals(databaseName)) {
            databaseNames.add(databaseName);
          }
        }
      }
      for (final String databaseName : databaseNames) {
        session.executeNonQueryStatement("DROP DATABASE " + databaseName);
      }
    }
  }

  protected final void createSubscriptionTopic(
      final IoTDBSubscriptionSource source, final String taskName) throws Exception {
    final String topicName = IoTDBSubscriptionSource.getTopicName(taskName);
    final String host = EnvFactory.getEnv().getIP();
    final int port = Integer.parseInt(EnvFactory.getEnv().getPort());
    try (final ISubscriptionTableSession session =
        new SubscriptionTableSessionBuilder().host(host).port(port).build()) {
      session.createTopic(topicName, source.getTopicProperties(null));
    }
    createdTopicNames.add(topicName);
  }

  @AfterClass
  public static void tearDownCluster() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }
}
