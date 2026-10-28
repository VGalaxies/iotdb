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
import org.apache.iotdb.confignode.procedure.state.stream.StopStreamState;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class StopStreamProcedure extends AbstractStreamProcedure<StopStreamState> {

  private String streamName;
  private int targetEpoch = -1;
  private StreamTaskExpectedStatus previousExpectedStatus;

  public StopStreamProcedure() {
    super();
  }

  public StopStreamProcedure(final String streamName) {
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
  protected Flow executeFromState(final ConfigNodeProcedureEnv env, final StopStreamState state) {
    switch (state) {
      case CHECK_EXISTENCE:
        return checkExistence(env);
      case STOP_RUNTIME:
        return stopRuntime(env);
      case STOP_STREAM_METADATA:
        return stopStreamMetadata(env);
      default:
        setFailure(
            new ProcedureException(
                String.format(
                    ProcedureMessages.EXCEPTION_UNEXPECTED_STOPSTREAMPROCEDURE_STATE_ARG_FE20B787,
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
    if (previousExpectedStatus == StreamTaskExpectedStatus.STOPPED
        && task.getStatus() == StreamTaskStatus.STOPPED) {
      return Flow.NO_MORE_STATE;
    }
    if (task.getEpoch() == Integer.MAX_VALUE) {
      setFailure(
          new ProcedureException(
              new IoTDBException(
                  String.format(
                      ManagerMessages
                          .MESSAGE_CANNOT_ADVANCE_STREAM_ARG_TASK_EPOCH_HAS_REACHED_THE_MAXIMUM_SUPPORTED_VALUE_ED9FA887,
                      streamName),
                  TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode())));
      return Flow.NO_MORE_STATE;
    }
    targetEpoch = StreamTask.incrementEpochChecked(task.getEpoch());
    setNextState(StopStreamState.STOP_RUNTIME);
    return Flow.HAS_MORE_STATE;
  }

  private Flow stopStreamMetadata(final ConfigNodeProcedureEnv env) {
    final StreamTask task = getTask(env);
    if (task == null) {
      return failStreamNotFound();
    }
    if (previousExpectedStatus == StreamTaskExpectedStatus.STOPPED) {
      return Flow.NO_MORE_STATE;
    }
    final TSStatus status =
        env.getConfigManager().getStreamManager().persistStopStream(task, targetEpoch);
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      rollbackRuntime(env);
      setFailure(new ProcedureException(new IoTDBException(status)));
      return Flow.NO_MORE_STATE;
    }
    return Flow.NO_MORE_STATE;
  }

  private Flow stopRuntime(final ConfigNodeProcedureEnv env) {
    final TSStatus status =
        env.getConfigManager().getStreamManager().stopStreamTask(streamName, targetEpoch);
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      setFailure(new ProcedureException(new IoTDBException(status)));
      return Flow.NO_MORE_STATE;
    }
    final StreamTask task = getTask(env);
    if (task == null) {
      return failStreamNotFound();
    }
    targetEpoch = Math.max(targetEpoch, task.getEpoch());
    setNextState(StopStreamState.STOP_STREAM_METADATA);
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

  private void rollbackRuntime(final ConfigNodeProcedureEnv env) {
    if (previousExpectedStatus == StreamTaskExpectedStatus.RUNNING) {
      final StreamTask task = getTask(env);
      if (task != null) {
        env.getConfigManager().getStreamManager().rollbackStopStreamTask(task);
      }
    }
  }

  @Override
  protected void rollbackState(final ConfigNodeProcedureEnv env, final StopStreamState state) {
    // Compensation is attempted before the metadata failure is reported.
  }

  @Override
  protected boolean isRollbackSupported(final StopStreamState state) {
    return false;
  }

  @Override
  protected StopStreamState getState(final int stateId) {
    return StopStreamState.values()[stateId];
  }

  @Override
  protected int getStateId(final StopStreamState state) {
    return state.ordinal();
  }

  @Override
  protected StopStreamState getInitialState() {
    return StopStreamState.CHECK_EXISTENCE;
  }

  @Override
  public void serialize(final DataOutputStream stream) throws IOException {
    stream.writeShort(ProcedureType.STOP_STREAM_PROCEDURE.getTypeCode());
    super.serialize(stream);
    ReadWriteIOUtils.write(streamName, stream);
    stream.writeInt(targetEpoch);
    stream.writeInt(previousExpectedStatus == null ? -1 : previousExpectedStatus.ordinal());
  }

  @Override
  public void deserialize(final ByteBuffer byteBuffer) {
    super.deserialize(byteBuffer);
    streamName = ReadWriteIOUtils.readString(byteBuffer);
    targetEpoch = byteBuffer.getInt();
    final int expectedStatusOrdinal = byteBuffer.getInt();
    previousExpectedStatus =
        expectedStatusOrdinal < 0 ? null : StreamTaskExpectedStatus.values()[expectedStatusOrdinal];
  }
}
