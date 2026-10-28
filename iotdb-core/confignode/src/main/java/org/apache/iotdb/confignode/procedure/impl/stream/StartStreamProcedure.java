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
import org.apache.iotdb.commons.stream.StreamTaskExpectedStatus;
import org.apache.iotdb.commons.stream.StreamTaskStatus;
import org.apache.iotdb.confignode.i18n.ManagerMessages;
import org.apache.iotdb.confignode.i18n.ProcedureMessages;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.state.stream.StartStreamState;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class StartStreamProcedure extends AbstractStreamProcedure<StartStreamState> {

  private static final Logger LOGGER = LoggerFactory.getLogger(StartStreamProcedure.class);

  private String streamName;
  private StreamTaskExpectedStatus previousExpectedStatus;
  private int runtimeEpoch = -1;

  public StartStreamProcedure() {
    super();
  }

  public StartStreamProcedure(final String streamName) {
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
  protected Flow executeFromState(final ConfigNodeProcedureEnv env, final StartStreamState state) {
    switch (state) {
      case CHECK_EXISTENCE:
        return checkExistence(env);
      case START_RUNTIME:
        return startRuntime(env);
      case START_STREAM_METADATA:
        return startStreamMetadata(env);
      default:
        setFailure(
            new ProcedureException(
                String.format(
                    ProcedureMessages.EXCEPTION_UNEXPECTED_STARTSTREAMPROCEDURE_STATE_ARG_D5F3448B,
                    state)));
        return Flow.NO_MORE_STATE;
    }
  }

  private Flow checkExistence(final ConfigNodeProcedureEnv env) {
    final StreamTask task = getTask(env);
    if (task == null) {
      return failStreamNotFound();
    }
    previousExpectedStatus =
        env.getConfigManager().getStreamManager().getStreamInfo().getExpectedStatus(streamName);
    if (previousExpectedStatus == StreamTaskExpectedStatus.RUNNING
        && task.getStatus() == StreamTaskStatus.RUNNING) {
      return Flow.NO_MORE_STATE;
    }
    if (!env.getConfigManager().getStreamManager().hasAvailableStreamNode()) {
      final TSStatus status =
          new TSStatus(TSStatusCode.NO_REGISTERED_STREAM_NODE_ERROR.getStatusCode())
              .setMessage(ManagerMessages.MESSAGE_NO_RUNNING_STREAMNODE_FOUND_F21CB52B);
      setFailure(new ProcedureException(new IoTDBException(status)));
      return Flow.NO_MORE_STATE;
    }
    setNextState(StartStreamState.START_RUNTIME);
    return Flow.HAS_MORE_STATE;
  }

  private Flow startStreamMetadata(final ConfigNodeProcedureEnv env) {
    final StreamTask task = getTask(env);
    if (task == null) {
      return failStreamNotFound();
    }
    if (previousExpectedStatus == StreamTaskExpectedStatus.RUNNING) {
      return Flow.NO_MORE_STATE;
    }
    final TSStatus status =
        env.getConfigManager()
            .getStreamManager()
            .persistStartStream(task, Math.max(task.getEpoch(), runtimeEpoch));
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      rollbackRuntime(env, task);
      setFailure(new ProcedureException(new IoTDBException(status)));
      return Flow.NO_MORE_STATE;
    }
    return Flow.NO_MORE_STATE;
  }

  private Flow startRuntime(final ConfigNodeProcedureEnv env) {
    final TSStatus status = env.getConfigManager().getStreamManager().startStreamTask(streamName);
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      setFailure(new ProcedureException(new IoTDBException(status)));
      return Flow.NO_MORE_STATE;
    }
    final StreamTask task = getTask(env);
    if (task == null) {
      return failStreamNotFound();
    }
    runtimeEpoch = task.getEpoch();
    setNextState(StartStreamState.START_STREAM_METADATA);
    return Flow.HAS_MORE_STATE;
  }

  private StreamTask getTask(final ConfigNodeProcedureEnv env) {
    return env.getConfigManager().getStreamManager().getStreamInfo().getTask(streamName);
  }

  private Flow failStreamNotFound() {
    setFailure(
        new ProcedureException(
            new IoTDBException(
                String.format(ManagerMessages.STREAM_NOT_FOUND, streamName),
                TSStatusCode.STREAM_NOT_EXIST.getStatusCode())));
    return Flow.NO_MORE_STATE;
  }

  private void rollbackRuntime(final ConfigNodeProcedureEnv env, final StreamTask task) {
    final int currentEpoch = Math.max(task.getEpoch(), runtimeEpoch);
    final int rollbackEpoch =
        currentEpoch == Integer.MAX_VALUE
            ? currentEpoch
            : StreamTask.incrementEpochChecked(currentEpoch);
    // Keep compensation identical for first starts and restarts. If the metadata write failed for
    // a newly created stream, the stopped task is intentionally accepted as a small StreamNode
    // residue. A later START can reuse it when scheduled to the same node, and StreamNode shutdown
    // eventually removes it.
    final TSStatus status =
        env.getConfigManager().getStreamManager().rollbackStartStreamTask(task, rollbackEpoch);
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      LOGGER.warn(
          ProcedureMessages.LOG_FAILED_TO_ROLL_BACK_RUNTIME_FOR_STREAMTASK_ARG_STATUS_ARG_DC57901A,
          streamName,
          status);
    }
  }

  @Override
  protected void rollbackState(final ConfigNodeProcedureEnv env, final StartStreamState state) {
    // Compensation is attempted before the metadata failure is reported.
  }

  @Override
  protected boolean isRollbackSupported(final StartStreamState state) {
    return false;
  }

  @Override
  protected StartStreamState getState(final int stateId) {
    return StartStreamState.values()[stateId];
  }

  @Override
  protected int getStateId(final StartStreamState state) {
    return state.ordinal();
  }

  @Override
  protected StartStreamState getInitialState() {
    return StartStreamState.CHECK_EXISTENCE;
  }

  @Override
  public void serialize(final DataOutputStream stream) throws IOException {
    stream.writeShort(ProcedureType.START_STREAM_PROCEDURE.getTypeCode());
    super.serialize(stream);
    ReadWriteIOUtils.write(streamName, stream);
    stream.writeInt(previousExpectedStatus == null ? -1 : previousExpectedStatus.ordinal());
    stream.writeInt(runtimeEpoch);
  }

  @Override
  public void deserialize(final ByteBuffer byteBuffer) {
    super.deserialize(byteBuffer);
    streamName = ReadWriteIOUtils.readString(byteBuffer);
    final int expectedStatusOrdinal = byteBuffer.getInt();
    previousExpectedStatus =
        expectedStatusOrdinal < 0 ? null : StreamTaskExpectedStatus.values()[expectedStatusOrdinal];
    runtimeEpoch = byteBuffer.getInt();
  }
}
