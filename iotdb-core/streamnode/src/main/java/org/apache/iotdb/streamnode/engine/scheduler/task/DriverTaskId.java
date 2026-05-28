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

package org.apache.iotdb.streamnode.engine.scheduler.task;

import org.apache.iotdb.calc.execution.schedule.queue.ID;
import org.apache.iotdb.commons.stream.PartitionKey;

import org.antlr.v4.runtime.misc.NotNull;

import java.util.Objects;

/** the class of id of the pipeline driver task. */
public class DriverTaskId implements ID, Comparable<DriverTaskId> {

  // TODO Create another field to store id of driver level
  // Currently, we just save pipelineId in driverTask since it's one-to-one relation.
  private final String id;
  private final PartitionKey partitionKey;
  private final String fullId;
  private static final String EMPTY_FULL_ID = "EmptyFullId";

  public DriverTaskId(String streamName, PartitionKey partitionKey) {
    this.id = streamName;
    this.partitionKey = partitionKey;
    this.fullId = String.format("%s.%s", id == null ? EMPTY_FULL_ID : id, partitionKey.toString());
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, partitionKey);
  }

  @Override
  public boolean equals(Object o) {
    return o instanceof DriverTaskId
        && ((DriverTaskId) o).id.equals(id)
        && ((DriverTaskId) o).partitionKey.equals(partitionKey);
  }

  public String toString() {
    return fullId;
  }

  public String getFullId() {
    return fullId;
  }

  public String getId() {
    return id;
  }

  // Default comparator of DriverTaskId: first by id, then by partitionKey
  @Override
  public int compareTo(@NotNull DriverTaskId o) {
    int idCompare = String.CASE_INSENSITIVE_ORDER.compare(this.id, o.getId());
    if (idCompare != 0) {
      return idCompare;
    }
    // If ids are equal, compare by partitionKey
    if (this.partitionKey == null && o.partitionKey == null) {
      return 0;
    }
    if (this.partitionKey == null) {
      return -1;
    }
    if (o.partitionKey == null) {
      return 1;
    }
    if (this.partitionKey.equals(o.partitionKey)) {
      return 0;
    }
    return 1;
  }
}
