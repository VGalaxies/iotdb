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

import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

public class UpdateStreamEpochPlan extends ConfigPhysicalPlan {

  private String streamName;
  private int epoch;

  public UpdateStreamEpochPlan() {
    super(ConfigPhysicalPlanType.UpdateStreamEpoch);
  }

  public UpdateStreamEpochPlan(final String streamName, final int epoch) {
    this();
    this.streamName = streamName;
    this.epoch = epoch;
  }

  public String getStreamName() {
    return streamName;
  }

  public int getEpoch() {
    return epoch;
  }

  @Override
  protected void serializeImpl(final DataOutputStream stream) throws IOException {
    stream.writeShort(getType().getPlanType());
    ReadWriteIOUtils.write(streamName, stream);
    stream.writeInt(epoch);
  }

  @Override
  protected void deserializeImpl(final ByteBuffer buffer) {
    streamName = ReadWriteIOUtils.readString(buffer);
    epoch = buffer.getInt();
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
    final UpdateStreamEpochPlan that = (UpdateStreamEpochPlan) o;
    return epoch == that.epoch && Objects.equals(streamName, that.streamName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), streamName, epoch);
  }
}
