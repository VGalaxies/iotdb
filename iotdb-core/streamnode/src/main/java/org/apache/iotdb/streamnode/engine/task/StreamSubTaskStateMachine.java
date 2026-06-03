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

package org.apache.iotdb.streamnode.engine.task;

import org.apache.iotdb.calc.execution.StateMachine;
import org.apache.iotdb.calc.execution.StateMachine.StateChangeListener;

import java.util.EnumSet;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;

import static java.util.Objects.requireNonNull;

public class StreamSubTaskStateMachine {

  private final String subTaskId;
  private final StateMachine<StreamSubTaskState> state;
  private final LinkedBlockingQueue<Throwable> failureCauses = new LinkedBlockingQueue<>();

  public StreamSubTaskStateMachine(String subTaskId, Executor executor) {
    this.subTaskId = requireNonNull(subTaskId, "subTaskId should not be null");
    requireNonNull(executor, "executor should not be null");
    this.state =
        new StateMachine<>(
            "StreamSubTask " + subTaskId,
            executor,
            StreamSubTaskState.CREATED,
            EnumSet.of(
                StreamSubTaskState.STOPPED, StreamSubTaskState.FAILED, StreamSubTaskState.DROPPED));
  }

  public StreamSubTaskState getState() {
    return state.get();
  }

  public boolean isDone() {
    return getState().isDone();
  }

  public Optional<Throwable> getFailureCause() {
    return Optional.ofNullable(failureCauses.peek());
  }

  public LinkedBlockingQueue<Throwable> getFailureCauses() {
    return failureCauses;
  }

  public void addStateChangeListener(StateChangeListener<StreamSubTaskState> stateChangeListener) {
    state.addStateChangeListener(stateChangeListener);
  }

  public boolean start() {
    return state.setIf(
        StreamSubTaskState.RUNNING, currentState -> currentState == StreamSubTaskState.CREATED);
  }

  public boolean stop() {
    return transitionToDoneState(StreamSubTaskState.STOPPED);
  }

  public boolean markStopping() {
    return state.setIf(
        StreamSubTaskState.STOPPING,
        currentState ->
            currentState == StreamSubTaskState.CREATED
                || currentState == StreamSubTaskState.RUNNING);
  }

  public boolean drop() {
    return transitionToDoneState(StreamSubTaskState.DROPPED);
  }

  public boolean failed(Throwable cause) {
    failureCauses.add(cause);
    return transitionToDoneState(StreamSubTaskState.FAILED);
  }

  private boolean transitionToDoneState(StreamSubTaskState newState) {
    if (!newState.isDone()) {
      throw new IllegalArgumentException("State is not done: " + newState);
    }
    return state.setIf(newState, currentState -> !currentState.isDone());
  }

  @Override
  public String toString() {
    return "StreamSubTaskStateMachine{"
        + "subTaskId='"
        + subTaskId
        + '\''
        + ", state="
        + getState()
        + '}';
  }
}
