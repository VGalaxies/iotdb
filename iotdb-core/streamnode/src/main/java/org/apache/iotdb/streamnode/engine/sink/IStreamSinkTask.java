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

import java.util.List;

public interface IStreamSinkTask {
  /**
   * Push the computed results into the sink queue.
   *
   * @param block The result
   * @param commitIds The set of commit IDs of all source data consumed to produce this result. A
   *     single computation may require multiple batches of source data (multiple commit IDs) to
   *     obtain the result. Can be empty, indicating that a batch of source data generates multiple
   *     batches of results, and the last batch of results can only be committed when the sink is
   *     processed
   * @return Normally returns immediateVoidFuture (zero-cost, non-blocking); Return a blocked future
   *     during backpressure, and automatically set it once backpressure is released
   */
  ListenableFuture<?> push(TsBlock block, List<Long> commitIds);

  /** stop the task, and refuse push. */
  void stop();
}
