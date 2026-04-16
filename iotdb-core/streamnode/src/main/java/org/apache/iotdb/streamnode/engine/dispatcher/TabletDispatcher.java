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

package org.apache.iotdb.streamnode.engine.dispatcher;

import java.util.function.Function;
import org.apache.iotdb.commons.stream.IoTDBSubscriptionSource;
import org.apache.iotdb.commons.stream.PartitionKey;
import org.apache.iotdb.commons.stream.StreamSource;
import org.apache.iotdb.streamnode.engine.task.StreamSubTask;

import java.util.Map;
import org.apache.tsfile.write.record.Tablet;

public abstract class TabletDispatcher {

  protected Function<PartitionKey, StreamSubTask> subTaskMapper;

  /**
   * Dispatch a data batch to sub-tasks based on partition keys. In iteration 1, the data batch is
   * represented as an Object (will be Tablet in future).
   */
  public abstract void dispatch(
      Tablet tablet, long tabletId);

  public static TabletDispatcher create(StreamSource source, Function<PartitionKey, StreamSubTask> subTaskMapper) {
    if (source instanceof IoTDBSubscriptionSource) {
      IoTDBSubscriptionSource subSource = (IoTDBSubscriptionSource) source;
      return new ColumnPartitionedTabletDispatcher(subSource.getPartitionColumns(), subTaskMapper);
    }
    throw new UnsupportedOperationException("Unsupported source type: " + source.getType());
  }
}
