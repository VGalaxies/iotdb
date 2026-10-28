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

package org.apache.iotdb.streamnode.i18n;

public final class StreamNodeMessages {

  private StreamNodeMessages() {}

  public static final String STARTING_STREAMNODE = "Starting StreamNode {}";
  public static final String STREAMNODE_PREPARED_SUCCESSFULLY =
      "The StreamNode is prepared successfully, which takes {} ms";
  public static final String STREAMNODE_ENV_VARS =
      "IoTDB-StreamNode environment variables:{}";
  public static final String MESSAGE_STALE_STOP_REQUEST_FOR_TASK_ARG_4309B991 =
      "Stale stop request for task %s";

  // ---------------------------------------------------------------------------
  // startup – StreamNode
  // ---------------------------------------------------------------------------
  public static final String STREAMNODE_STARTING = "Starting StreamNode...";
  public static final String
      STREAMNODE_CONFIG_DETAILS =
          "StreamNode config: cluster_name={}, sn_internal_address={}, sn_internal_port={}, sn_seed_config_node={}";
  public static final String STREAMNODE_FIRST_START =
      "StreamNode is starting for the first time...";
  public static final String STREAMNODE_RESTARTING = "StreamNode is restarting...";
  public static final String STREAMNODE_CONFIGURATION =
      "StreamNode configuration: {}";
  public static final String
      STREAMNODE_SETUP_SUCCESSFULLY =
          "Congratulations, IoTDB StreamNode is set up successfully. Now, enjoy yourself!";
  public static final String FAIL_TO_START_SERVER = "Fail to start server";
  public static final String STREAMNODE_STARTED =
      "StreamNode started successfully";
  public static final String MEET_ERROR_STARTING_UP =
      "Meet error while starting up.";
  public static final String IOTDB_STREAMNODE_HAS_STARTED =
      "IoTDB StreamNode has started.";
  public static final String STREAMNODE_RPC_SERVICE_LISTENING =
      "StreamNode RPC service listening on {}:{}";
  public static final String SETTING_UP_STREAMNODE =
      "Setting up IoTDB StreamNode...";
  public static final String SUCCESSFULLY_SETUP_INTERNAL_SERVICES =
      "Successfully setup internal services.";
  public static final String PULLING_SYSTEM_CONFIGURATIONS =
      "Pulling system configurations from the ConfigNode-leader...";
  public static final String
      CANNOT_PULL_SYSTEM_CONFIGURATIONS =
          "Cannot pull system configurations from ConfigNode-leader, because: {}";
  public static final String
      CANNOT_PULL_SYSTEM_CONFIGURATIONS_AFTER_RETRIES =
          "Cannot pull system configurations from ConfigNode-leader after {} retries.";
  public static final String
      PULL_SYSTEM_CONFIGURATIONS_SUCCESSFULLY =
          "Successfully pull system configurations from ConfigNode-leader, which takes {} ms";
  public static final String SENDING_REGISTER_REQUEST =
      "Sending register request to ConfigNode-leader...";
  public static final String CANNOT_REGISTER_TO_CLUSTER =
      "Cannot register to the cluster, because: {}";
  public static final String CANNOT_REGISTER_AFTER_RETRIES =
      "Cannot register into cluster after {} retries.";
  public static final String
      PRECHECK_PASSED =
          "Successfully pass the precheck, will do the formal registration soon.";
  public static final String
      REGISTER_TO_CLUSTER_SUCCESSFULLY =
          "Successfully register to the cluster: {} , which takes {} ms.";
  public static final String SENDING_RESTART_REQUEST =
      "Sending restart request to ConfigNode-leader...";
  public static final String
      CANNOT_SEND_RESTART_REQUEST =
          "Cannot send restart request to the ConfigNode-leader, because: {}";
  public static final String
      CANNOT_SEND_RESTART_REQUEST_AFTER_RETRIES =
          "Cannot send restart StreamNode request to ConfigNode-leader after {} retries.";
  public static final String
      RESTART_REQUEST_ACCEPTED =
          "Restart request to cluster: {} is accepted, which takes {} ms.";
  public static final String STREAMNODE_DEFAULT_CHARSET =
      "IoTDB-StreamNode default charset is: {}";
  public static final String
      EXCEPTION_CANNOT_PULL_SYSTEM_CONFIGURATIONS =
          "Cannot pull system configurations from ConfigNode-leader. Please check whether the sn_seed_config_node in iotdb-system.properties is correct or alive.";
  public static final String
      EXCEPTION_CANNOT_REGISTER_INTO_CLUSTER =
          "Cannot register into the cluster. Please check whether the sn_seed_config_node in iotdb-system.properties is correct or alive.";
  public static final String EXCEPTION_CANNOT_REGISTER_TO_CLUSTER =
      "Cannot register to the cluster.";
  public static final String
      EXCEPTION_CANNOT_SEND_RESTART_REQUEST =
          "Cannot send restart StreamNode request to ConfigNode-leader. Please check whether the sn_seed_config_node in iotdb-system.properties is correct or alive.";

  // ---------------------------------------------------------------------------
  // service – StreamNodeShutdownHook
  // ---------------------------------------------------------------------------
  public static final String STREAMNODE_EXITING = "StreamNode exiting...";
  public static final String FAILED_BORROW_CONFIG_NODE_CLIENT =
          "Failed to borrow ConfigNodeClient";
  public static final String FAILED_REPORT_SHUTDOWN = "Failed to report shutdown";
  public static final String
      FAILED_TO_REPORT_STREAMNODE_SHUTDOWN_TO_CONFIGNODE_3B5822EB =
          "Failed to report StreamNode's shutdown to ConfigNode. The cluster will still take the "
              + "current StreamNode as Running for a few seconds.";

  // ---------------------------------------------------------------------------
  // service – StreamNodeRPCServiceProcessor
  // ---------------------------------------------------------------------------
  public static final String EXECUTE_STREAM_NODE_STOP = "Execute stopStreamNode RPC method";
  public static final String INTERRUPTED_STREAM_NODE_STOP =
      "Meets InterruptedException in stopStreamNode RPC method";
  public static final String STOP_STREAM_NODE_ERROR = "Stop StreamNode error";
  public static final String STOP_STREAM_NODE_SUCCEED =
      "Stop StreamNode succeed";
  public static final String
      MISC_LOG_EXECUTING_SYSTEM_EXIT_0_IN_STOPANDCLEARSTREAMNODE_RPC_METHOD_C907CFF5 =
          "Executing system.exit(0) in stopStreamNode RPC method after 30 seconds";

  public static final String LOG_STARTING_SUBSCRIPTION_SOURCE_FOR_TASK_ARG_TABLE_ARG_ARG_73C3F127 =
      "Starting subscription source for task {}, table: {}.{}";
  public static final String LOG_TOPIC_ARG_CREATED_OR_ALREADY_EXISTS_FOR_TASK_ARG_5C92523C =
      "Topic '{}' created (or already exists) for task {}";
  public static final String LOG_POLL_LOOP_STARTED_FOR_TASK_ARG_B3559D6E =
      "Poll loop started for task {}";
  public static final String
      LOG_ERROR_IN_POLL_LOOP_FOR_TASK_ARG_RETRYING_AFTER_ARG_MS_ARG_CC0816C0 =
          "Error in poll loop for task {}, retrying after {} ms: {}";
  public static final String LOG_POLL_LOOP_EXITED_FOR_TASK_ARG_D704FCD7 =
      "Poll loop exited for task {}";
  public static final String LOG_ERROR_UNSUBSCRIBING_CONSUMER_FOR_TASK_ARG_48E84DCA =
      "Error unsubscribing consumer for task {}";
  public static final String LOG_ERROR_CLOSING_CONSUMER_FOR_TASK_ARG_7849F85A =
      "Error closing consumer for task {}";
  public static final String LOG_SUBSCRIPTION_SOURCE_STOPPED_FOR_TASK_ARG_0908AD57 =
      "Subscription source stopped for task {}";
  public static final String LOG_SUBSCRIPTION_SOURCE_DROPPED_FOR_TASK_ARG_8F7CBCF7 =
      "Subscription source dropped for task {}";
  public static final String LOG_COMMITTED_ARG_MESSAGES_UP_TO_INDEX_ARG_FOR_TASK_ARG_4ADEA111 =
      "Committed {} messages up to index {} for task {}";
  public static final String LOG_SUBSCRIPTION_SOURCE_RESUMED_FOR_TASK_ARG_147E9AFE =
      "Subscription source resumed for task {}";
  public static final String LOG_MEMORY_LEAK_DETECTED_WHEN_DE_REGISTERING_SUB_TASK_ARG_ARG_1C6BCE9E =
      "Memory leak detected when de-registering sub task {}-{}";

  public static final String EXCEPTION_BYTES_TO_RESERVE_FROM_FREE_MEMORY_FOR_COMPUTE_SHOULD_BE_POSITIVE_3BAFFA26 =
      "Bytes to reserve from free memory for compute should be positive";
  public static final String
      EXCEPTION_THERE_IS_NOT_ENOUGH_MEMORY_FOR_STREAM_SUB_TASK_THE_CONTEXTHOLDER_IS_ARG_CURRENT_REMAINING_FREE_MEMORY_IS_ARG_B_ALREADY_RESERVED_MEMORY_FOR_THIS_CONTEXT_IN_TOTAL_IS_ARG_B_THE_MEMORY_REQUESTED_THIS_TIME_IS_ARG_B_B798DFBF =
          "There is not enough memory for stream sub task, the contextHolder is %s, "
              + "current remaining free memory is %dB, already reserved memory for this "
              + "context in total is %dB, the memory requested this time is %dB";
  public static final String EXCEPTION_BYTES_TO_RELEASE_TO_FREE_MEMORY_FOR_COMPUTE_SHOULD_BE_POSITIVE_C3B5454F =
      "Bytes to release to free memory for compute should be positive";

  public static final String
      LOG_STREAMNODE_MEMORY_INITIALIZED_STREAMENGINE_ARG_BYTES_SUBSCRIPTION_ARG_BYTES_WINDOW_ARG_BYTES_COMPUTE_ARG_BYTES_COMPUTEOPERATORS_ARG_BYTES_COMPUTEEXCHANGE_ARG_BYTES_WRITEBACK_ARG_BYTES_BB173A54 =
          "StreamNode memory initialized: streamEngine={} bytes, subscription={} bytes, "
              + "window={} bytes, compute={} bytes, computeOperators={} bytes, "
              + "computeExchange={} bytes, writeBack={} bytes";
  public static final String LOG_INVALID_ARG_ARG_FALLBACK_TO_DEFAULT_ARG_C6E6C2CE =
      "Invalid {}={}, fallback to default {}";
  public static final String EXCEPTION_EXPECT_ARG_COMPONENTS_F362221D = "expect %d components";
  public static final String EXCEPTION_NEGATIVE_COMPONENT_6DE6B3BA = "negative component";
  public static final String EXCEPTION_SUM_MUST_BE_POSITIVE_8916A153 = "sum must be positive";

  public static final String EXCEPTION_BYTESTORESERVE_SHOULD_BE_POSITIVE_F7A74BA0 =
      "bytesToReserve should be positive";
  public static final String
      EXCEPTION_CANNOT_RESERVE_ARG_BYTES_FOR_PLAN_NODE_ARG_MAX_BYTES_CAN_RESERVE_IS_ARG_9885F340 =
          "Cannot reserve %d bytes for plan node %s, max bytes can reserve is %d";
  public static final String EXCEPTION_BYTES_TO_FREE_SHOULD_BE_POSITIVE_B330BE00 =
      "bytes to free should be positive";
  public static final String EXCEPTION_FREE_MORE_MEMORY_THAN_HAS_BEEN_RESERVED_52C0122E =
      "Free more memory than has been reserved";
  public static final String EXCEPTION_RELATED_MEMORY_RESERVATION_CANNOT_BE_NULL_WHEN_FREEING_68A8477C =
      "Related memory reservation cannot be null when freeing";
  public static final String
      EXCEPTION_PLAN_NODE_RELATED_MEMORY_IS_NOT_ZERO_WHEN_TRYING_TO_DEREGISTER_SUB_TASK_ARG_ARG_ARG_F4DED57C =
          "Plan node related memory is not zero when trying to deregister sub task %s-%s: %s";

  public static final String EXCEPTION_RELEASED_BYTES_HAS_BEEN_LARGER_THAN_RESERVED_69DE5487 =
      "Released bytes has been larger than reserved!";
  public static final String EXCEPTION_MEMORYCONTROLSTREAMTSBLOCKQUEUE_IS_FULL_0EBB374E =
      "MemoryControlStreamTsBlockQueue is full";
  public static final String LOG_FAILED_TO_PLAN_SUB_TASK_FOR_TASK_ARG_AND_PARTITION_ARG_5DDA0008 =
      "Failed to plan sub-task for task {} and partition {}";

  public static final String
      LOG_FAILED_TO_RESERVE_ARG_BYTES_FROM_WRITE_BACK_MEMORY_BLOCK_USED_ARG_FREE_ARG_56E2E8B9 =
          "Failed to reserve {} bytes from write-back memory block, used={} free={}";
  public static final String
      LOG_FAILED_TO_RELEASE_ARG_BYTES_TO_WRITE_BACK_MEMORY_BLOCK_USED_ARG_2EC89C4E =
          "Failed to release {} bytes to write-back memory block, used={}";
  public static final String LOG_EXCEPTION_WHILE_CLOSING_WRITE_BACK_MEMORY_BLOCK_D028F30B =
      "Exception while closing write-back memory block";
  public static final String LOG_WRITE_BACK_MEMORY_BLOCK_CLOSED_439D5463 =
      "Write-back memory block closed";
  public static final String LOG_WRITE_FAILED_ATTEMPT_ARG_RETRYING_AFTER_ARG_MS_ARG_2B200618 =
      "Write failed (attempt {}), retrying after {}ms: {}";
  public static final String LOG_WRITE_FAILED_AFTER_ARG_ATTEMPTS_ARG_524C30CF =
      "Write failed after {} attempts: {}";
  public static final String LOG_FLUSH_FAILED_FOR_PIPELINE_ARG_3275329B =
      "Flush failed for pipeline {}";
  public static final String
      LOG_WINDOW_ARG_WRITE_BACK_FAILED_REMOVED_FROM_COMMIT_GATING_AND_ADVANCED_ALLOW_WATERMARK_TO_ARG_D3B309DD =
          "Window {} write-back failed, removed from commit gating and advanced allow watermark to {}";
  public static final String
      EXCEPTION_SINK_ENTRY_SIZE_ARG_BYTES_EXCEEDS_THE_PIPELINE_ADMISSION_LIMIT_ARG_BYTES_8F5A20CE =
          "Sink entry size %s bytes exceeds the pipeline admission limit (%s bytes)";
  public static final String
      LOG_SINK_ENTRY_SIZE_ARG_BYTES_EXCEEDS_THE_PIPELINE_ADMISSION_LIMIT_ARG_BYTES_REJECTING_WITHOUT_PARKING_47B35576 =
          "Sink entry size {} bytes exceeds the pipeline admission limit ({} bytes); rejecting without parking";

  // ---------------------------------------------------------------------------
  // window engine
  // ---------------------------------------------------------------------------
  public static final String
      LOG_CAPACITY_WINDOW_PROCESSED_BATCH_ROWS_ARG_1309F37E =
          "CapacityWindowEngine processed a batch, current window rows: {}";
  public static final String LOG_CAPACITY_WINDOW_DROPPED_488FBEBE =
      "CapacityWindowEngine dropped";
  public static final String LOG_CAPACITY_WINDOW_FORCE_CLOSED_57A2067F =
      "CapacityWindowEngine force closed";
  public static final String LOG_WINDOW_MEMORY_RELEASE_LISTENER_FAILED_A3F62F67 =
      "Window memory release listener failed";
  public static final String
      LOG_ERRORS_DROPPING_WINDOW_ENGINE_OF_SUB_TASK_614AFE2C =
          "Errors occurred while dropping the window engine of stream sub task {}.";
  public static final String
      LOG_WINDOW_FORCE_CLOSE_BY_MEMORY_PRESSURE_961D31B5 =
          "Window engine {} force-closed window {} under data-pool memory pressure "
              + "(pause exceeded {} ms)";
  public static final String
      LOG_WINDOW_FORCE_DESTROY_BY_MEMORY_PRESSURE_731C3E95 =
          "Window engine {} force-destroyed closed window {} under data-pool memory pressure "
              + "(pause exceeded {} ms)";
  public static final String
      LOG_WINDOW_MEMORY_POOL_EXHAUSTED_RESERVE_FAILED_4FCDE878 =
          "Window {} memory pool exhausted: failed to reserve {} bytes (used {}/{}); the caller "
              + "applies backpressure";
  public static final String
      LOG_FAILED_TO_CLOSE_WINDOW_INPUT_BLOCK_AA6C1D2F =
          "Failed to close the window input memory block";
  public static final String LOG_FAILED_TO_CLOSE_WINDOW_DATA_BLOCK_E603CF14 =
      "Failed to close the window data memory block";
  public static final String
      LOG_FAILED_TO_CLOSE_WINDOW_OUTPUT_BLOCK_C1C736A2 =
          "Failed to close the window output memory block";
  public static final String
      LOG_PERIOD_WINDOW_MONTH_SCHEDULER_STARTING_8277C994 =
          "PeriodWindowEngine month-mode scheduler starting";
  public static final String
      LOG_PERIOD_WINDOW_PERIODMS_NOT_SET_B688316F =
          "PeriodWindowEngine: periodMs is not set, scheduler will not start";
  public static final String
      LOG_PERIOD_WINDOW_SCHEDULER_STARTING_FIRST_TRIGGER_6DF9AD81 =
          "PeriodWindowEngine scheduler starting, first trigger in {}ms, window: [{}, {})";
  public static final String
      LOG_PERIOD_WINDOW_MONTH_RESCHEDULE_REJECTED_18C381FF =
          "PeriodWindowEngine month scheduler rejected a re-schedule";
  public static final String LOG_PERIOD_WINDOW_SCHEDULER_STOPPED_0E3BC3D5 =
      "PeriodWindowEngine scheduler stopped";
  public static final String
      LOG_PERIOD_WINDOW_PERIODIC_TRIGGER_ADVANCED_D214BE38 =
          "PeriodWindowEngine periodic trigger, advanced to window: [{}, {})";
  public static final String LOG_FAILED_TO_ADD_EVENT_TO_QUEUE_62D83E1B =
      "Failed to add event to queue";
  public static final String LOG_PERIOD_WINDOW_DROPPED_870E46A8 =
      "PeriodWindowEngine dropped";
  public static final String LOG_PERIOD_WINDOW_FORCE_CLOSED_1650B493 =
      "PeriodWindowEngine force closed";
  public static final String EXCEPTION_PERIOD_WINDOW_DOES_NOT_ACCEPT_DATA_F936860F =
      "PeriodWindowEngine does not accept data";
  public static final String
      LOG_TUMBLE_WINDOW_PROCESSED_ROWS_OPEN_WINDOWS_59851869 =
          "TumbleWindowEngine processed {} rows, open windows: {}";
  public static final String LOG_TUMBLE_WINDOW_DROPPED_C71A44A2 =
      "TumbleWindowEngine dropped";
  public static final String LOG_TUMBLE_WINDOW_FORCE_CLOSED_31857093 =
      "TumbleWindowEngine force closed";
  public static final String
      LOG_FAILED_TO_PROCESS_PUSHED_BATCH_CD3572E3 =
          "{} failed to process a pushed batch, dropping the failed batch";
  public static final String
      LOG_FAILED_TO_REPORT_GROUP_DONE_FOR_TABLET_A82B9F2A =
          "Failed to report group-done for tablet {} of a dropped batch";
  public static final String
      LOG_MEMORY_PAUSED_BY_POOL_EXHAUSTION_8BB73B2A =
          "{} memory-paused by {} pool exhaustion (data pool used {}/{} bytes, output pool {}/{});"
              + " processing halts until memory frees, further pushes buffer and backpressure the"
              + " producer";
  public static final String EXCEPTION_UNSUPPORTED_WINDOW_TYPE_E2440AEF =
      "Unsupported window type: %s";
  public static final String LOG_WINDOW_ENGINE_TICK_FAILED_FOR_TASK_6DB9ACDA =
      "WindowEngine tick failed for task {}";
  public static final String
      LOG_FAILED_TO_RELEASE_ROWS_ITERATOR_F9D749CD =
          "Failed to release the rows iterator of a window event";
  public static final String
      EXCEPTION_EVENT_SCAN_COLUMN_MISSING_IN_FIRST_TABLET_12D68452 =
          "Event scan column '%s' is missing from the first pushed tablet, so its type is unknown;"
              + " push a tablet carrying it first";
  public static final String
      EXCEPTION_EVENT_SCAN_COLUMN_TYPE_COUNT_MISMATCH_B770DE42 =
          "Event scan column type count %d does not match the scan column count %d";
  public static final String
      LOG_FAILED_TO_COMPUTE_WINDOW_ARG_ARG_ARG_7E4CA205 =
          "Failed to compute window [{}, {}) of stream {}, the result of this window will not be output";
  public static final String EXCEPTION_COLUMN_VALUE_READ_WITHOUT_NULL_CHECK_AT_INDEX_ARG_B5EDBDD7 =
      "column value read at index %s without a null check";
}
