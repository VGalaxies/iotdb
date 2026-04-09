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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IoTDBSubscriptionSourceInstance extends StreamSourceInstance {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(IoTDBSubscriptionSourceInstance.class);

  private final IoTDBSubscriptionSource sourceConfig;

  public IoTDBSubscriptionSourceInstance(IoTDBSubscriptionSource sourceConfig) {
    this.sourceConfig = sourceConfig;
  }

  @Override
  public void start() throws Exception {
    // TODO: Create subscription consumer, subscribe to topic, start polling
    LOGGER.info(
        "Starting subscription source for table: {}.{}",
        sourceConfig.getDatabase(),
        sourceConfig.getTableName());
  }

  @Override
  public void stop() throws Exception {
    // TODO: Unsubscribe and close consumer
    LOGGER.info(
        "Stopping subscription source for table: {}.{}",
        sourceConfig.getDatabase(),
        sourceConfig.getTableName());
  }

  @Override
  public void commit(long commitIndex) throws Exception {
    // TODO: Commit offset to subscription
    LOGGER.debug("Committed offset: {}", commitIndex);
  }

  public IoTDBSubscriptionSource getSourceConfig() {
    return sourceConfig;
  }
}
