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

package org.apache.iotdb.confignode.consensus.request.write.stream;

import org.apache.iotdb.commons.stream.StreamTask;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

public class CreateStreamPlan extends ConfigPhysicalPlan {

  private StreamTask streamTask;

  public CreateStreamPlan() {
    super(ConfigPhysicalPlanType.CreateStream);
  }

  public CreateStreamPlan(final StreamTask streamTask) {
    super(ConfigPhysicalPlanType.CreateStream);
    this.streamTask = streamTask;
  }

  public StreamTask getStreamTask() {
    return streamTask;
  }

  @Override
  protected void serializeImpl(final DataOutputStream stream) throws IOException {
    stream.writeShort(getType().getPlanType());
    streamTask.serialize(stream);
  }

  @Override
  protected void deserializeImpl(final ByteBuffer buffer) throws IOException {
    streamTask = StreamTask.deserialize(buffer);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    final CreateStreamPlan that = (CreateStreamPlan) o;
    return Objects.equals(streamTask.getTaskName(), that.streamTask.getTaskName());
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), streamTask.getTaskName());
  }
}
