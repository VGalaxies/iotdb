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

package org.apache.iotdb.confignode.procedure.impl.stream;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.commons.stream.IoTDBSubscriptionSource;
import org.apache.iotdb.confignode.consensus.request.write.stream.DropStreamPlan;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.impl.subscription.topic.DropTopicProcedure;
import org.apache.iotdb.confignode.procedure.state.stream.DropStreamState;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;
import org.apache.iotdb.consensus.exception.ConsensusException;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class DropStreamProcedure extends AbstractStreamProcedure<DropStreamState> {

  private static final Logger LOGGER = LoggerFactory.getLogger(DropStreamProcedure.class);

  private String streamName;

  public DropStreamProcedure() {
    super();
  }

  public DropStreamProcedure(final String streamName) {
    super();
    this.streamName = streamName;
  }

  @Override
  public String getStreamName() {
    return streamName;
  }

  @Override
  protected Object getOperationPayload() {
    return null;
  }

  @Override
  protected Flow executeFromState(final ConfigNodeProcedureEnv env, final DropStreamState state)
      throws InterruptedException {
    final long startTime = System.currentTimeMillis();
    try {
      switch (state) {
        case CHECK_EXISTENCE:
          LOGGER.info("Checking and marking stream {} before drop", streamName);
          checkExistenceAndMarkBeingDropped(env);
          break;
        case DROP_RUNTIME_AND_TOPIC:
          dropRuntimeTaskAndTopic(env);
          break;
        case DROP_STREAM_METADATA:
          LOGGER.info("Writing drop of stream {} to consensus layer", streamName);
          writeStreamMetadataToConsensus(env);
          return Flow.NO_MORE_STATE;
        default:
          setFailure(new ProcedureException("Unrecognized DropStreamState: " + state));
          return Flow.NO_MORE_STATE;
      }
      return Flow.HAS_MORE_STATE;
    } finally {
      LOGGER.info(
          "DropStream-{}-{} costs {}ms", streamName, state, System.currentTimeMillis() - startTime);
    }
  }

  private void checkExistenceAndMarkBeingDropped(final ConfigNodeProcedureEnv env) {
    final TSStatus status =
        env.getConfigManager().getStreamManager().markStreamBeingDropped(streamName);
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      setFailure(new ProcedureException(new IoTDBException(status)));
      return;
    }
    setNextState(DropStreamState.DROP_RUNTIME_AND_TOPIC);
  }

  private void dropRuntimeTaskAndTopic(final ConfigNodeProcedureEnv env) {
    final TSStatus dropRuntimeStatus =
        env.getConfigManager().getStreamManager().dropStreamTask(streamName);
    if (dropRuntimeStatus.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      setFailure(new ProcedureException(new IoTDBException(dropRuntimeStatus)));
      return;
    }

    if (env.getConfigManager().getStreamManager().isSubscriptionSource(streamName)) {
      // Use a child procedure so Topic metadata and DataNode state are removed with the same
      // retry and failure handling as a user-issued DROP TOPIC. Stream Topics use the table model.
      addChildProcedure(
          new DropTopicProcedure(IoTDBSubscriptionSource.getTopicName(streamName), true));
    }

    setNextState(DropStreamState.DROP_STREAM_METADATA);
  }

  private void writeStreamMetadataToConsensus(final ConfigNodeProcedureEnv env) {
    TSStatus status;
    try {
      status = env.getConfigManager().getConsensusManager().write(new DropStreamPlan(streamName));
    } catch (final ConsensusException e) {
      LOGGER.warn("Failed in the write API executing the consensus layer due to: ", e);
      status = new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
      status.setMessage(e.getMessage());
    }
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      setFailure(new ProcedureException(new IoTDBException(status)));
      return;
    }
    env.getConfigManager().getStreamManager().unmarkStreamBeingDropped(streamName);
  }

  @Override
  protected void rollbackState(final ConfigNodeProcedureEnv env, final DropStreamState state)
      throws IOException, InterruptedException, ProcedureException {
    env.getConfigManager().getStreamManager().unmarkStreamBeingDropped(streamName);
  }

  @Override
  protected boolean isRollbackSupported(final DropStreamState state) {
    return true;
  }

  @Override
  protected DropStreamState getState(final int stateId) {
    return DropStreamState.values()[stateId];
  }

  @Override
  protected int getStateId(final DropStreamState state) {
    return state.ordinal();
  }

  @Override
  protected DropStreamState getInitialState() {
    return DropStreamState.CHECK_EXISTENCE;
  }

  @Override
  public void serialize(final DataOutputStream stream) throws IOException {
    stream.writeShort(ProcedureType.DROP_STREAM_PROCEDURE.getTypeCode());
    super.serialize(stream);
    ReadWriteIOUtils.write(streamName, stream);
  }

  @Override
  public void deserialize(final ByteBuffer byteBuffer) {
    super.deserialize(byteBuffer);
    streamName = ReadWriteIOUtils.readString(byteBuffer);
  }
}
