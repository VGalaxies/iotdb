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

package org.apache.iotdb.streamnode.conf;

import org.apache.iotdb.commons.memory.MemoryConfig;
import org.apache.iotdb.commons.memory.MemoryManager;
import org.apache.iotdb.streamnode.i18n.StreamNodeMessages;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Node-level memory configuration for the stream engine.
 *
 * <p>The whole heap is taken as the stream engine memory, and then divided into four node-level
 * shared pools by module proportion: subscription, window, compute and write back. The module
 * proportion carries an extra free component that only participates in the denominator, so the
 * corresponding memory is left unallocated as a reserve (same as DataNode's free memory
 * proportion). Each module manager only bounds the total memory of its module across all stream
 * tasks; consumers obtain a memory block from the manager via {@code exactAllocate} and track their
 * own reservations, just like DataNode's {@code NotThreadSafeMemoryReservationManager} does against
 * the operators block.
 */
public class StreamNodeMemoryConfig {

  private static final Logger LOGGER = LoggerFactory.getLogger(StreamNodeMemoryConfig.class);

  public static final String SUBSCRIPTION_POOL = "Subscription";
  public static final String WINDOW_POOL = "Window";
  public static final String COMPUTE_POOL = "Compute";
  public static final String WRITE_BACK_POOL = "WriteBack";
  public static final String COMPUTE_OPERATORS_POOL = "ComputeOperators";
  public static final String COMPUTE_EXCHANGE_POOL = "ComputeExchange";
  public static final String WINDOW_INPUT_POOL = "WindowInput";
  public static final String WINDOW_DATA_POOL = "WindowData";
  public static final String WINDOW_OUTPUT_POOL = "WindowOutput";

  private static final String ON_HEAP_MEMORY_MANAGER = "OnHeap";
  private static final String STREAM_ENGINE_MEMORY_MANAGER = "StreamEngine";

  private static final String STREAM_TASK_MEMORY_PROPORTION = "stream_task_memory_proportion";
  private static final String STREAM_COMPUTE_MEMORY_PROPORTION = "stream_compute_memory_proportion";
  private static final String STREAM_WINDOW_MEMORY_PROPORTION = "stream_window_memory_proportion";

  private static final String DEFAULT_STREAM_TASK_MEMORY_PROPORTION = "1:3:1:1:1";
  private static final String DEFAULT_STREAM_COMPUTE_MEMORY_PROPORTION = "4:1";
  private static final String DEFAULT_STREAM_WINDOW_MEMORY_PROPORTION = "1:3:1";

  private static final int MODULE_POOL_COUNT = 4;
  private static final int PROPORTION_COUNT = MODULE_POOL_COUNT + 1;

  private MemoryManager streamEngineMemoryManager;

  private MemoryManager subscriptionMemoryManager;
  private MemoryManager windowMemoryManager;
  private MemoryManager windowInputMemoryManager;
  private MemoryManager windowDataMemoryManager;
  private MemoryManager windowOutputMemoryManager;
  private MemoryManager computeMemoryManager;
  private MemoryManager computeOperatorsMemoryManager;
  private MemoryManager computeExchangeMemoryManager;
  private MemoryManager writeBackMemoryManager;

  private long exchangeMaxBytesCanReserve;

  public void init(Properties properties, int sessionScanConcurrency) {
    long streamMemorySize = Runtime.getRuntime().maxMemory();
    int[] moduleProportions = parseModuleProportions(properties);
    int proportionSum = 0;
    for (int proportion : moduleProportions) {
      proportionSum += proportion;
    }

    MemoryManager onHeapMemoryManager =
        MemoryConfig.global()
            .getOrCreateMemoryManager(ON_HEAP_MEMORY_MANAGER, Runtime.getRuntime().maxMemory());
    streamEngineMemoryManager =
        onHeapMemoryManager.getOrCreateMemoryManager(
            STREAM_ENGINE_MEMORY_MANAGER, streamMemorySize);

    long subscriptionMemorySize = streamMemorySize * moduleProportions[0] / proportionSum;
    long windowMemorySize = streamMemorySize * moduleProportions[1] / proportionSum;
    long computeMemorySize = streamMemorySize * moduleProportions[2] / proportionSum;
    long writeBackMemorySize = streamMemorySize * moduleProportions[3] / proportionSum;

    subscriptionMemoryManager =
        streamEngineMemoryManager.getOrCreateMemoryManager(
            SUBSCRIPTION_POOL, subscriptionMemorySize);
    windowMemoryManager =
        streamEngineMemoryManager.getOrCreateMemoryManager(WINDOW_POOL, windowMemorySize);
    computeMemoryManager =
        streamEngineMemoryManager.getOrCreateMemoryManager(COMPUTE_POOL, computeMemorySize);

    int[] windowProportions = parseWindowProportion(properties);
    int windowProportionSum = windowProportions[0] + windowProportions[1] + windowProportions[2];
    long windowInputMemorySize = windowMemorySize * windowProportions[0] / windowProportionSum;
    long windowDataMemorySize = windowMemorySize * windowProportions[1] / windowProportionSum;
    long windowOutputMemorySize = windowMemorySize * windowProportions[2] / windowProportionSum;
    windowInputMemoryManager =
        windowMemoryManager.getOrCreateMemoryManager(WINDOW_INPUT_POOL, windowInputMemorySize);
    windowDataMemoryManager =
        windowMemoryManager.getOrCreateMemoryManager(WINDOW_DATA_POOL, windowDataMemorySize);
    windowOutputMemoryManager =
        windowMemoryManager.getOrCreateMemoryManager(WINDOW_OUTPUT_POOL, windowOutputMemorySize);

    int[] computeProportions = parseComputeProportion(properties);
    int computeProportionSum = computeProportions[0] + computeProportions[1];
    long computeOperatorsMemorySize =
        computeMemorySize * computeProportions[0] / computeProportionSum;
    long computeExchangeMemorySize =
        computeMemorySize * computeProportions[1] / computeProportionSum;
    computeOperatorsMemoryManager =
        computeMemoryManager.getOrCreateMemoryManager(
            COMPUTE_OPERATORS_POOL, computeOperatorsMemorySize);
    computeExchangeMemoryManager =
        computeMemoryManager.getOrCreateMemoryManager(
            COMPUTE_EXCHANGE_POOL, computeExchangeMemorySize);
    exchangeMaxBytesCanReserve = Math.max(1L, computeExchangeMemorySize / sessionScanConcurrency);

    writeBackMemoryManager =
        streamEngineMemoryManager.getOrCreateMemoryManager(WRITE_BACK_POOL, writeBackMemorySize);

    LOGGER.info(
        StreamNodeMessages
            .LOG_STREAMNODE_MEMORY_INITIALIZED_STREAMENGINE_ARG_BYTES_SUBSCRIPTION_ARG_BYTES_WINDOW_ARG_BYTES_COMPUTE_ARG_BYTES_COMPUTEOPERATORS_ARG_BYTES_COMPUTEEXCHANGE_ARG_BYTES_WRITEBACK_ARG_BYTES_BB173A54,
        streamMemorySize,
        subscriptionMemorySize,
        windowMemorySize,
        computeMemorySize,
        computeOperatorsMemorySize,
        computeExchangeMemorySize,
        writeBackMemorySize);
  }

  private int[] parseModuleProportions(Properties properties) {
    String value =
        properties.getProperty(
            STREAM_TASK_MEMORY_PROPORTION, DEFAULT_STREAM_TASK_MEMORY_PROPORTION);
    try {
      return resolveModuleProportions(value);
    } catch (NumberFormatException e) {
      LOGGER.warn(
          StreamNodeMessages.LOG_INVALID_ARG_ARG_FALLBACK_TO_DEFAULT_ARG_C6E6C2CE,
          STREAM_TASK_MEMORY_PROPORTION,
          value,
          DEFAULT_STREAM_TASK_MEMORY_PROPORTION);
      return resolveModuleProportions(DEFAULT_STREAM_TASK_MEMORY_PROPORTION);
    }
  }

  private int[] resolveModuleProportions(String proportion) {
    String[] parts = proportion.split(":");
    if (parts.length != PROPORTION_COUNT) {
      throw new NumberFormatException(
          String.format(
              StreamNodeMessages.EXCEPTION_EXPECT_ARG_COMPONENTS_F362221D, PROPORTION_COUNT));
    }
    int[] proportions = new int[PROPORTION_COUNT];
    int sum = 0;
    for (int i = 0; i < PROPORTION_COUNT; i++) {
      proportions[i] = Integer.parseInt(parts[i].trim());
      if (proportions[i] < 0) {
        throw new NumberFormatException(StreamNodeMessages.EXCEPTION_NEGATIVE_COMPONENT_6DE6B3BA);
      }
      sum += proportions[i];
    }
    if (sum <= 0) {
      throw new NumberFormatException(StreamNodeMessages.EXCEPTION_SUM_MUST_BE_POSITIVE_8916A153);
    }
    return proportions;
  }

  private int[] parseWindowProportion(Properties properties) {
    String value =
        properties.getProperty(
            STREAM_WINDOW_MEMORY_PROPORTION, DEFAULT_STREAM_WINDOW_MEMORY_PROPORTION);
    try {
      return resolveWindowProportion(value);
    } catch (NumberFormatException e) {
      LOGGER.warn(
          StreamNodeMessages.LOG_INVALID_ARG_ARG_FALLBACK_TO_DEFAULT_ARG_C6E6C2CE,
          STREAM_WINDOW_MEMORY_PROPORTION,
          value,
          DEFAULT_STREAM_WINDOW_MEMORY_PROPORTION);
      return resolveWindowProportion(DEFAULT_STREAM_WINDOW_MEMORY_PROPORTION);
    }
  }

  private int[] resolveWindowProportion(String proportion) {
    String[] parts = proportion.split(":");
    if (parts.length != 3) {
      throw new NumberFormatException(
          String.format(StreamNodeMessages.EXCEPTION_EXPECT_ARG_COMPONENTS_F362221D, 3));
    }
    int[] proportions = new int[3];
    int sum = 0;
    for (int i = 0; i < proportions.length; i++) {
      proportions[i] = Integer.parseInt(parts[i].trim());
      if (proportions[i] < 0) {
        throw new NumberFormatException(StreamNodeMessages.EXCEPTION_NEGATIVE_COMPONENT_6DE6B3BA);
      }
      sum += proportions[i];
    }
    if (sum <= 0) {
      throw new NumberFormatException(StreamNodeMessages.EXCEPTION_SUM_MUST_BE_POSITIVE_8916A153);
    }
    return proportions;
  }

  private int[] parseComputeProportion(Properties properties) {
    String value =
        properties.getProperty(
            STREAM_COMPUTE_MEMORY_PROPORTION, DEFAULT_STREAM_COMPUTE_MEMORY_PROPORTION);
    try {
      return resolveComputeProportion(value);
    } catch (NumberFormatException e) {
      LOGGER.warn(
          StreamNodeMessages.LOG_INVALID_ARG_ARG_FALLBACK_TO_DEFAULT_ARG_C6E6C2CE,
          STREAM_COMPUTE_MEMORY_PROPORTION,
          value,
          DEFAULT_STREAM_COMPUTE_MEMORY_PROPORTION);
      return resolveComputeProportion(DEFAULT_STREAM_COMPUTE_MEMORY_PROPORTION);
    }
  }

  private int[] resolveComputeProportion(String proportion) {
    String[] parts = proportion.split(":");
    if (parts.length != 2) {
      throw new NumberFormatException(
          String.format(StreamNodeMessages.EXCEPTION_EXPECT_ARG_COMPONENTS_F362221D, 2));
    }
    int[] proportions = new int[2];
    int sum = 0;
    for (int i = 0; i < proportions.length; i++) {
      proportions[i] = Integer.parseInt(parts[i].trim());
      if (proportions[i] < 0) {
        throw new NumberFormatException(StreamNodeMessages.EXCEPTION_NEGATIVE_COMPONENT_6DE6B3BA);
      }
      sum += proportions[i];
    }
    if (sum <= 0) {
      throw new NumberFormatException(StreamNodeMessages.EXCEPTION_SUM_MUST_BE_POSITIVE_8916A153);
    }
    return proportions;
  }

  public MemoryManager getSubscriptionMemoryManager() {
    return subscriptionMemoryManager;
  }

  public MemoryManager getWindowMemoryManager() {
    return windowMemoryManager;
  }

  public MemoryManager getComputeMemoryManager() {
    return computeMemoryManager;
  }

  public MemoryManager getWindowInputMemoryManager() {
    return windowInputMemoryManager;
  }

  public MemoryManager getWindowDataMemoryManager() {
    return windowDataMemoryManager;
  }

  public MemoryManager getWindowOutputMemoryManager() {
    return windowOutputMemoryManager;
  }

  public MemoryManager getComputeOperatorsMemoryManager() {
    return computeOperatorsMemoryManager;
  }

  public MemoryManager getComputeExchangeMemoryManager() {
    return computeExchangeMemoryManager;
  }

  public MemoryManager getWriteBackMemoryManager() {
    return writeBackMemoryManager;
  }

  public long getExchangeMaxBytesCanReserve() {
    return exchangeMaxBytesCanReserve;
  }

  public MemoryManager getStreamEngineMemoryManager() {
    return streamEngineMemoryManager;
  }
}
