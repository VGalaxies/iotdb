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

package org.apache.iotdb.streamnode.engine.window;

import org.apache.iotdb.streamnode.utils.IEventRowsIterator;

import java.util.Optional;
import java.util.OptionalLong;

public interface IEventInfo {
  /** Type of this window event. */
  WindowEventType getEventType();

  /**
   * Convenience for {@link WindowEventType#WINDOW_DESTROY}. Only destroy events may advance source
   * commit after sink flush.
   */
  default boolean isEOF() {
    return getEventType() == WindowEventType.WINDOW_DESTROY;
  }

  /**
   * Whether this event must be bound to the compute task and executed.
   *
   * <p>This is an explicit per-event decision made by the window engine when it builds the event.
   * It is independent of {@link WindowEventType} and of whether {@link #getRowsIterator()} is null:
   * a period window fetches its data at execution time with a session scan, so it must be computed
   * even when it carries no buffered rows, while a pure destroy marker that only advances the sink
   * or source commit sets this to {@code false}.
   */
  boolean needsCompute();

  OptionalLong getStartTime();

  OptionalLong getEndTime();

  OptionalLong getPrevTime();

  OptionalLong getNextTime();

  OptionalLong getRowCount();

  Optional<Object> getPrevValue();

  Optional<Object> getCurrentValue();

  Optional<Object> getN(int n);

  /** Task-scoped window id assigned when the window first received data. */
  long getWindowId();

  IEventRowsIterator getRowsIterator();

  /**
   * Handles an exception thrown while computing the current event. Implementations derive {@code
   * needRetry} from the exception type: exceptions caused by bad data that prevent the computation
   * from continuing (e.g. {@link ArithmeticException} from a division by zero) are not retried and
   * mark the event so the window module can later drop, alert or record it. The marking itself is
   * left empty for now and will be implemented by the window module.
   *
   * @param cause the exception thrown while computing the event
   */
  void handleComputeException(Throwable cause);

  /**
   * Returns whether the current event should be retried after a compute failure. Returns {@code
   * false} for events failed due to bad data (e.g. division by zero).
   */
  boolean needRetry();

  /**
   * Releases the resources this event holds on its consumers' behalf - e.g. its row iterator's pin
   * on the window data pool. Must be called whenever the event is discarded without being fully
   * consumed (a driver slot clearing it, a queue purge, a subtask teardown); a fully consumed event
   * may also be released, the call is idempotent. The default implementation holds no resources.
   */
  default void release() {}
}
