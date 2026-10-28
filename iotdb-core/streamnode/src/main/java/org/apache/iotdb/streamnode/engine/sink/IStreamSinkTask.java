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

package org.apache.iotdb.streamnode.engine.sink;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.tsfile.read.common.block.TsBlock;

public interface IStreamSinkTask {
  /**
   * Pushes a computed result into the sink queue.
   *
   * @param block result rows; may be null/empty for a commit-only EOF push
   * @param windowId window that produced this result
   * @param eof whether this push marks the window destroyable (allows commit advance after flush)
   * @return usually an immediate future; a blocked future under backpressure
   */
  ListenableFuture<?> push(TsBlock block, long windowId, boolean eof);

  /** Stops the task and rejects further pushes. */
  void stop();
}
