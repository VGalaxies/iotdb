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

package org.apache.iotdb.streamnode.engine.memory;

import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.memory.MemoryBlockType;
import org.apache.iotdb.streamnode.conf.StreamNodeDescriptor;
import org.apache.iotdb.streamnode.conf.StreamNodeMemoryConfig;
import org.apache.iotdb.streamnode.utils.MemoryControlStreamTsBlockQueue;
import org.apache.iotdb.streamnode.utils.StreamTsBlockQueue;

import java.util.concurrent.ExecutorService;

/**
 * Manages the stream engine's exchange (TsBlock buffering) memory. It holds the shared {@link
 * StreamNodeMemoryPool} backed by the compute exchange block and an executor for the queue's
 * asynchronous listeners. This is the lightweight counterpart of DataNode's {@code
 * LocalMemoryManager} + {@code MPPDataExchangeManager}: StreamNode has no cross-node shuffle or
 * handle registry, so only pool + executor + queue creation is needed.
 */
public class StreamNodeDataExchangeManager {

  private final StreamNodeMemoryPool exchangeMemoryPool;
  private final ExecutorService executorService;

  private StreamNodeDataExchangeManager() {
    this.exchangeMemoryPool =
        new StreamNodeMemoryPool(
            StreamNodeDescriptor.getInstance()
                .getMemoryConfig()
                .getComputeExchangeMemoryManager()
                .exactAllocate(
                    StreamNodeMemoryConfig.COMPUTE_EXCHANGE_POOL, MemoryBlockType.DYNAMIC));
    this.executorService = IoTDBThreadPoolFactory.newSingleThreadExecutor("StreamNode-Exchange");
  }

  public static StreamNodeDataExchangeManager getInstance() {
    return StreamNodeExchangeManagerHolder.INSTANCE;
  }

  public StreamTsBlockQueue createTsBlockQueue(
      String taskName, String partitionKey, String planNodeId) {
    return new MemoryControlStreamTsBlockQueue(
        exchangeMemoryPool, taskName, partitionKey, planNodeId, executorService);
  }

  public void deRegisterSubTask(String taskName, String partitionKey) {
    exchangeMemoryPool.deRegisterSubTask(taskName, partitionKey, true);
  }

  public StreamNodeMemoryPool getExchangeMemoryPool() {
    return exchangeMemoryPool;
  }

  private static class StreamNodeExchangeManagerHolder {
    private static final StreamNodeDataExchangeManager INSTANCE =
        new StreamNodeDataExchangeManager();
  }
}
