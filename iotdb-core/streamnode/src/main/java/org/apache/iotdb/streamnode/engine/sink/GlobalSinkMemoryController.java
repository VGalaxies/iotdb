/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.streamnode.engine.sink;

import org.apache.iotdb.commons.memory.IMemoryBlock;
import org.apache.iotdb.commons.memory.MemoryBlockType;
import org.apache.iotdb.streamnode.conf.StreamNodeDescriptor;
import org.apache.iotdb.streamnode.conf.StreamNodeMemoryConfig;
import org.apache.iotdb.streamnode.i18n.StreamNodeMessages;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Global Memory Controller — Shared by All Sink Pipelines */
public class GlobalSinkMemoryController {
  private static final Logger LOGGER = LoggerFactory.getLogger(GlobalSinkMemoryController.class);

  private final IMemoryBlock sinkMemoryBlocks;

  private static volatile GlobalSinkMemoryController instance;

  private GlobalSinkMemoryController() {
    this.sinkMemoryBlocks =
        StreamNodeDescriptor.getInstance()
            .getMemoryConfig()
            .getWriteBackMemoryManager()
            .exactAllocate(StreamNodeMemoryConfig.WRITE_BACK_POOL, MemoryBlockType.DYNAMIC);
  }

  public static GlobalSinkMemoryController getInstance() {
    if (instance == null) {
      synchronized (GlobalSinkMemoryController.class) {
        if (instance == null) {
          instance = new GlobalSinkMemoryController();
        }
      }
    }
    return instance;
  }

  /**
   * Try to reserve memory.
   *
   * @param bytes the amount of memory to reserve
   * @return true if reservation succeeded, false otherwise
   */
  public boolean tryReserve(long bytes) {
    if (sinkMemoryBlocks.allocate(bytes)) {
      return true;
    }
    LOGGER.warn(
        StreamNodeMessages
            .LOG_FAILED_TO_RESERVE_ARG_BYTES_FROM_WRITE_BACK_MEMORY_BLOCK_USED_ARG_FREE_ARG_56E2E8B9,
        bytes,
        sinkMemoryBlocks.getUsedMemoryInBytes(),
        sinkMemoryBlocks.getFreeMemoryInBytes());
    return false;
  }

  /**
   * Release reserved memory.
   *
   * @param bytesToRelease the amount of memory to release (requested release size, not remaining
   *     used memory after release)
   * @return false if {@code bytesToRelease} is larger than currently used memory (release is
   *     skipped); true otherwise
   */
  public boolean release(long bytesToRelease) {
    long usedBytes = sinkMemoryBlocks.getUsedMemoryInBytes();
    if (bytesToRelease > usedBytes) {
      LOGGER.warn(
          StreamNodeMessages
              .LOG_FAILED_TO_RELEASE_ARG_BYTES_TO_WRITE_BACK_MEMORY_BLOCK_USED_ARG_2EC89C4E,
          bytesToRelease,
          usedBytes);
      return false;
    }
    sinkMemoryBlocks.release(bytesToRelease);
    return true;
  }

  public long getUsedMemoryInBytes() {
    return sinkMemoryBlocks.getUsedMemoryInBytes();
  }

  public long getFreeMemoryInBytes() {
    return sinkMemoryBlocks.getFreeMemoryInBytes();
  }

  /** Return the write-back memory block to the memory manager. */
  public synchronized void close() {
    if (sinkMemoryBlocks.isReleased()) {
      return;
    }
    try {
      sinkMemoryBlocks.close();
      LOGGER.info(StreamNodeMessages.LOG_WRITE_BACK_MEMORY_BLOCK_CLOSED_439D5463);
    } catch (Exception e) {
      LOGGER.warn(
          StreamNodeMessages.LOG_EXCEPTION_WHILE_CLOSING_WRITE_BACK_MEMORY_BLOCK_D028F30B, e);
    }
  }
}
