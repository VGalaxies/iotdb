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
import org.apache.iotdb.commons.stream.StreamTask;
import org.apache.iotdb.confignode.consensus.request.write.stream.CreateStreamPlan;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.impl.StateMachineProcedure;
import org.apache.iotdb.confignode.procedure.state.stream.CreateStreamState;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;
import org.apache.iotdb.consensus.exception.ConsensusException;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class CreateStreamProcedure
    extends StateMachineProcedure<ConfigNodeProcedureEnv, CreateStreamState> {

  private static final Logger LOGGER = LoggerFactory.getLogger(CreateStreamProcedure.class);

  private StreamTask streamTask;

  public CreateStreamProcedure() {
    super();
  }

  public CreateStreamProcedure(final StreamTask streamTask) {
    super();
    this.streamTask = streamTask;
  }

  public String getStreamName() {
    return streamTask.getTaskName();
  }

  public StreamTask getStreamTask() {
    return streamTask;
  }

  @Override
  protected Flow executeFromState(final ConfigNodeProcedureEnv env, final CreateStreamState state)
      throws InterruptedException {
    final long startTime = System.currentTimeMillis();
    try {
      switch (state) {
        case CHECK_EXISTENCE:
          LOGGER.info("Checking existence of stream {}", streamTask.getTaskName());
          checkExistence(env);
          break;
        case WRITE_TO_CONSENSUS:
          LOGGER.info("Writing stream {} to consensus layer", streamTask.getTaskName());
          writeToConsensus(env);
          return Flow.NO_MORE_STATE;
        default:
          setFailure(new ProcedureException("Unrecognized CreateStreamState: " + state));
          return Flow.NO_MORE_STATE;
      }
      return Flow.HAS_MORE_STATE;
    } finally {
      LOGGER.info(
          "CreateStream-{}-{} costs {}ms",
          streamTask.getTaskName(),
          state,
          System.currentTimeMillis() - startTime);
    }
  }

  private void checkExistence(final ConfigNodeProcedureEnv env) {
    final String taskName = streamTask.getTaskName();
    if (env.getConfigManager().getStreamManager().getStreamInfo().getTask(taskName) != null) {
      setFailure(
          new ProcedureException(
              new IoTDBException(
                  "Stream '" + taskName + "' already exists.",
                  TSStatusCode.STREAM_ALREADY_EXISTS.getStatusCode())));
      return;
    }
    setNextState(CreateStreamState.WRITE_TO_CONSENSUS);
  }

  private void writeToConsensus(final ConfigNodeProcedureEnv env) {
    TSStatus status;
    try {
      status = env.getConfigManager().getConsensusManager().write(new CreateStreamPlan(streamTask));
    } catch (final ConsensusException e) {
      LOGGER.warn("Failed in the write API executing the consensus layer due to: ", e);
      status = new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
      status.setMessage(e.getMessage());
    }
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      setFailure(new ProcedureException(new IoTDBException(status)));
    }
  }

  @Override
  protected void rollbackState(final ConfigNodeProcedureEnv env, final CreateStreamState state)
      throws IOException, InterruptedException, ProcedureException {
    // CHECK_EXISTENCE made no changes; WRITE_TO_CONSENSUS is the terminal state — no rollback
  }

  @Override
  protected boolean isRollbackSupported(final CreateStreamState state) {
    return false;
  }

  @Override
  protected CreateStreamState getState(final int stateId) {
    return CreateStreamState.values()[stateId];
  }

  @Override
  protected int getStateId(final CreateStreamState state) {
    return state.ordinal();
  }

  @Override
  protected CreateStreamState getInitialState() {
    return CreateStreamState.CHECK_EXISTENCE;
  }

  @Override
  public void serialize(final DataOutputStream stream) throws IOException {
    stream.writeShort(ProcedureType.CREATE_STREAM_PROCEDURE.getTypeCode());
    super.serialize(stream);
    streamTask.serialize(stream);
  }

  @Override
  public void deserialize(final ByteBuffer byteBuffer) {
    super.deserialize(byteBuffer);
    try {
      streamTask = StreamTask.deserialize(byteBuffer);
    } catch (final IOException e) {
      throw new RuntimeException("Failed to deserialize StreamTask in CreateStreamProcedure", e);
    }
  }
}
