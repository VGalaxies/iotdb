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
import org.apache.iotdb.commons.stream.StreamSourceType;
import org.apache.iotdb.streamnode.conf.StreamNodeConfig;

import org.apache.tsfile.write.record.Tablet;

import java.util.function.BiConsumer;

public abstract class StreamSourceInstance {

  protected BiConsumer<Tablet, Long> dataConsumer;

  public abstract void start(BiConsumer<Tablet, Long> dataConsumer) throws Exception;

  // Pause execution when it is running.
  public abstract void stop() throws Exception;

  // Instance deletion, perform destruction operations
  public abstract void drop() throws Exception;

  public abstract void commit(long commitIndex);

  /** Applies the latest source definition before restarting a stopped instance. */
  public abstract void update(StreamSource source);

  public static StreamSourceInstance create(
      StreamSource source, String taskName, StreamNodeConfig nodeConfig) {
    return create(source, null, taskName, nodeConfig);
  }

  public static StreamSourceInstance create(
      StreamSource source,
      StreamProperties streamProperties,
      String taskName,
      StreamNodeConfig nodeConfig) {
    if (source.getType() == StreamSourceType.IOTDB_SUBSCRIPTION) {
      return new IoTDBSubscriptionSourceInstance(
          (IoTDBSubscriptionSource) source, streamProperties, taskName, nodeConfig);
    }
    throw new UnsupportedOperationException("Unsupported source type: " + source.getType());
  }

  public static StreamSourceInstance from(
      StreamSource source, String taskName, StreamNodeConfig nodeConfig) {
    return create(source, taskName, nodeConfig);
  }
}
