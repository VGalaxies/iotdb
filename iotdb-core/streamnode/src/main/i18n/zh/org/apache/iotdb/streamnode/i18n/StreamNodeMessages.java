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

  public static final String STARTING_STREAMNODE = "正在启动 StreamNode {}";
  public static final String STREAMNODE_PREPARED_SUCCESSFULLY =
      "StreamNode 准备就绪，耗时 {} ms";
  public static final String STREAMNODE_ENV_VARS =
      "IoTDB-StreamNode 环境变量:{}";
  public static final String MESSAGE_STALE_STOP_REQUEST_FOR_TASK_ARG_4309B991 =
      "任务 %s 的 STOP 请求已过期";

  // ---------------------------------------------------------------------------
  // startup – StreamNode
  // ---------------------------------------------------------------------------
  public static final String STREAMNODE_STARTING = "正在启动 StreamNode...";
  public static final String
      STREAMNODE_CONFIG_DETAILS =
          "StreamNode 配置：cluster_name={}，sn_internal_address={}，sn_internal_port={}，sn_seed_config_node={}";
  public static final String STREAMNODE_FIRST_START =
      "StreamNode 首次启动...";
  public static final String STREAMNODE_RESTARTING = "StreamNode 正在重启...";
  public static final String STREAMNODE_CONFIGURATION =
      "StreamNode 配置：{}";
  public static final String
      STREAMNODE_SETUP_SUCCESSFULLY =
          "恭喜，IoTDB StreamNode 设置成功。尽情使用吧！";
  public static final String FAIL_TO_START_SERVER = "启动服务器失败";
  public static final String STREAMNODE_STARTED =
      "StreamNode 启动成功";
  public static final String MEET_ERROR_STARTING_UP =
      "启动时遇到错误。";
  public static final String IOTDB_STREAMNODE_HAS_STARTED =
      "IoTDB StreamNode 已启动。";
  public static final String STREAMNODE_RPC_SERVICE_LISTENING =
      "StreamNode RPC 服务正在监听 {}:{}";
  public static final String SETTING_UP_STREAMNODE =
      "正在设置 IoTDB StreamNode...";
  public static final String SUCCESSFULLY_SETUP_INTERNAL_SERVICES =
      "内部服务设置成功。";
  public static final String PULLING_SYSTEM_CONFIGURATIONS =
      "正在从 ConfigNode-leader 拉取系统配置...";
  public static final String
      CANNOT_PULL_SYSTEM_CONFIGURATIONS =
          "无法从 ConfigNode-leader 拉取系统配置，原因：{}";
  public static final String
      CANNOT_PULL_SYSTEM_CONFIGURATIONS_AFTER_RETRIES =
          "重试 {} 次后仍无法从 ConfigNode-leader 拉取系统配置。";
  public static final String
      PULL_SYSTEM_CONFIGURATIONS_SUCCESSFULLY =
          "成功从 ConfigNode-leader 拉取系统配置，耗时 {} ms";
  public static final String SENDING_REGISTER_REQUEST =
      "正在向 ConfigNode-leader 发送注册请求...";
  public static final String CANNOT_REGISTER_TO_CLUSTER =
      "无法注册到集群，原因：{}";
  public static final String CANNOT_REGISTER_AFTER_RETRIES =
      "重试 {} 次后仍无法注册到集群。";
  public static final String
      PRECHECK_PASSED =
          "成功通过预检查，即将进行正式注册。";
  public static final String
      REGISTER_TO_CLUSTER_SUCCESSFULLY =
          "成功注册到集群：{}，耗时 {} ms。";
  public static final String SENDING_RESTART_REQUEST =
      "正在向 ConfigNode-leader 发送重启请求...";
  public static final String
      CANNOT_SEND_RESTART_REQUEST =
          "无法向 ConfigNode-leader 发送重启请求，原因：{}";
  public static final String
      CANNOT_SEND_RESTART_REQUEST_AFTER_RETRIES =
          "重试 {} 次后仍无法向 ConfigNode-leader 发送 StreamNode 重启请求。";
  public static final String
      RESTART_REQUEST_ACCEPTED =
          "集群重启请求：{} 已被接受，耗时 {} ms。";
  public static final String STREAMNODE_DEFAULT_CHARSET =
      "IoTDB-StreamNode 默认字符集为：{}";
  public static final String
      EXCEPTION_CANNOT_PULL_SYSTEM_CONFIGURATIONS =
          "无法从 ConfigNode-leader 拉取系统配置。请检查 iotdb-system.properties 中的 sn_seed_config_node 是否正确或可用。";
  public static final String
      EXCEPTION_CANNOT_REGISTER_INTO_CLUSTER =
          "无法注册到集群。请检查 iotdb-system.properties 中的 sn_seed_config_node 是否正确或可用。";
  public static final String EXCEPTION_CANNOT_REGISTER_TO_CLUSTER =
      "无法注册到集群。";
  public static final String
      EXCEPTION_CANNOT_SEND_RESTART_REQUEST =
          "无法向 ConfigNode-leader 发送 StreamNode 重启请求。请检查 iotdb-system.properties 中的 sn_seed_config_node 是否正确或可用。";

  // ---------------------------------------------------------------------------
  // service – StreamNodeShutdownHook
  // ---------------------------------------------------------------------------
  public static final String STREAMNODE_EXITING = "StreamNode 正在退出...";
  public static final String FAILED_BORROW_CONFIG_NODE_CLIENT =
          "借用 ConfigNodeClient 失败";
  public static final String FAILED_REPORT_SHUTDOWN = "上报关闭状态失败";
  public static final String
      FAILED_TO_REPORT_STREAMNODE_SHUTDOWN_TO_CONFIGNODE_3B5822EB =
          "无法向 ConfigNode 上报 StreamNode 关闭状态。集群在接下来的几秒内仍会将当前 StreamNode 视为 Running。";

  // ---------------------------------------------------------------------------
  // service – StreamNodeRPCServiceProcessor
  // ---------------------------------------------------------------------------
  public static final String EXECUTE_STREAM_NODE_STOP = "执行 stopStreamNode RPC 方法";
  public static final String INTERRUPTED_STREAM_NODE_STOP =
      "在 stopStreamNode RPC 方法中遇到中断异常";
  public static final String STOP_STREAM_NODE_ERROR = "停止 StreamNode 错误";
  public static final String STOP_STREAM_NODE_SUCCEED = "停止 StreamNode 成功";
  public static final String
      MISC_LOG_EXECUTING_SYSTEM_EXIT_0_IN_STOPANDCLEARSTREAMNODE_RPC_METHOD_C907CFF5 =
          "stopStreamNode RPC 方法将在 30 秒后执行 system.exit(0)";

  public static final String LOG_STARTING_SUBSCRIPTION_SOURCE_FOR_TASK_ARG_TABLE_ARG_ARG_73C3F127 =
      "正在启动任务 {} 的订阅数据源，表：{}.{}";
  public static final String LOG_TOPIC_ARG_CREATED_OR_ALREADY_EXISTS_FOR_TASK_ARG_5C92523C =
      "Topic '{}' 已为任务 {} 创建或已存在";
  public static final String LOG_POLL_LOOP_STARTED_FOR_TASK_ARG_B3559D6E =
      "任务 {} 的 poll 循环已启动";
  public static final String
      LOG_ERROR_IN_POLL_LOOP_FOR_TASK_ARG_RETRYING_AFTER_ARG_MS_ARG_CC0816C0 =
          "任务 {} 的 poll 循环发生错误，将在 {} ms 后重试：{}";
  public static final String LOG_POLL_LOOP_EXITED_FOR_TASK_ARG_D704FCD7 =
      "任务 {} 的 poll 循环已退出";
  public static final String LOG_ERROR_UNSUBSCRIBING_CONSUMER_FOR_TASK_ARG_48E84DCA =
      "取消任务 {} 的 consumer 订阅时发生错误";
  public static final String LOG_ERROR_CLOSING_CONSUMER_FOR_TASK_ARG_7849F85A =
      "关闭任务 {} 的 consumer 时发生错误";
  public static final String LOG_SUBSCRIPTION_SOURCE_STOPPED_FOR_TASK_ARG_0908AD57 =
      "任务 {} 的订阅数据源已停止";
  public static final String LOG_SUBSCRIPTION_SOURCE_DROPPED_FOR_TASK_ARG_8F7CBCF7 =
      "任务 {} 的订阅数据源已删除";
  public static final String LOG_COMMITTED_ARG_MESSAGES_UP_TO_INDEX_ARG_FOR_TASK_ARG_4ADEA111 =
      "已提交 {} 条消息，索引截至 {}，任务 {}";
  public static final String LOG_SUBSCRIPTION_SOURCE_RESUMED_FOR_TASK_ARG_147E9AFE =
      "任务 {} 的订阅数据源已恢复";
  public static final String LOG_MEMORY_LEAK_DETECTED_WHEN_DE_REGISTERING_SUB_TASK_ARG_ARG_1C6BCE9E =
      "注销 sub task {}-{} 时检测到内存泄漏";

  public static final String EXCEPTION_BYTES_TO_RESERVE_FROM_FREE_MEMORY_FOR_COMPUTE_SHOULD_BE_POSITIVE_3BAFFA26 =
      "为 compute 从空闲内存预留的字节数必须为正";
  public static final String
      EXCEPTION_THERE_IS_NOT_ENOUGH_MEMORY_FOR_STREAM_SUB_TASK_THE_CONTEXTHOLDER_IS_ARG_CURRENT_REMAINING_FREE_MEMORY_IS_ARG_B_ALREADY_RESERVED_MEMORY_FOR_THIS_CONTEXT_IN_TOTAL_IS_ARG_B_THE_MEMORY_REQUESTED_THIS_TIME_IS_ARG_B_B798DFBF =
          "流子任务内存不足，contextHolder 为 %s，当前剩余空闲内存为 %dB，该 context 已预留总内存为 %dB，本次请求内存为 %dB";
  public static final String EXCEPTION_BYTES_TO_RELEASE_TO_FREE_MEMORY_FOR_COMPUTE_SHOULD_BE_POSITIVE_C3B5454F =
      "为 compute 释放到空闲内存的字节数必须为正";

  public static final String
      LOG_STREAMNODE_MEMORY_INITIALIZED_STREAMENGINE_ARG_BYTES_SUBSCRIPTION_ARG_BYTES_WINDOW_ARG_BYTES_COMPUTE_ARG_BYTES_COMPUTEOPERATORS_ARG_BYTES_COMPUTEEXCHANGE_ARG_BYTES_WRITEBACK_ARG_BYTES_BB173A54 =
          "StreamNode 内存已初始化：streamEngine={} 字节，subscription={} 字节，window={} 字节，"
              + "compute={} 字节，computeOperators={} 字节，computeExchange={} 字节，writeBack={} 字节";
  public static final String LOG_INVALID_ARG_ARG_FALLBACK_TO_DEFAULT_ARG_C6E6C2CE =
      "无效的 {}={}，回退到默认值 {}";
  public static final String EXCEPTION_EXPECT_ARG_COMPONENTS_F362221D = "期望 %d 个分量";
  public static final String EXCEPTION_NEGATIVE_COMPONENT_6DE6B3BA = "存在负分量";
  public static final String EXCEPTION_SUM_MUST_BE_POSITIVE_8916A153 = "总和必须为正";

  public static final String EXCEPTION_BYTESTORESERVE_SHOULD_BE_POSITIVE_F7A74BA0 =
      "bytesToReserve 必须为正";
  public static final String
      EXCEPTION_CANNOT_RESERVE_ARG_BYTES_FOR_PLAN_NODE_ARG_MAX_BYTES_CAN_RESERVE_IS_ARG_9885F340 =
          "无法预留 %d 字节给 plan node %s，最大可预留字节数为 %d";
  public static final String EXCEPTION_BYTES_TO_FREE_SHOULD_BE_POSITIVE_B330BE00 =
      "要释放的字节数必须为正";
  public static final String EXCEPTION_FREE_MORE_MEMORY_THAN_HAS_BEEN_RESERVED_52C0122E =
      "释放的内存超过了已预留的内存";
  public static final String EXCEPTION_RELATED_MEMORY_RESERVATION_CANNOT_BE_NULL_WHEN_FREEING_68A8477C =
      "释放内存时，相关内存预留不能为 null";
  public static final String
      EXCEPTION_PLAN_NODE_RELATED_MEMORY_IS_NOT_ZERO_WHEN_TRYING_TO_DEREGISTER_SUB_TASK_ARG_ARG_ARG_F4DED57C =
          "注销子任务 %s-%s 时，plan node 相关内存不为 0：%s";

  public static final String EXCEPTION_RELEASED_BYTES_HAS_BEEN_LARGER_THAN_RESERVED_69DE5487 =
      "释放的字节数已超过预留的字节数！";
  public static final String EXCEPTION_MEMORYCONTROLSTREAMTSBLOCKQUEUE_IS_FULL_0EBB374E =
      "MemoryControlStreamTsBlockQueue 已满";
  public static final String LOG_FAILED_TO_PLAN_SUB_TASK_FOR_TASK_ARG_AND_PARTITION_ARG_5DDA0008 =
      "为任务 {} 和分区 {} 规划子任务失败";

  public static final String
      LOG_FAILED_TO_RESERVE_ARG_BYTES_FROM_WRITE_BACK_MEMORY_BLOCK_USED_ARG_FREE_ARG_56E2E8B9 =
          "从 write-back 内存块预留 {} 字节失败，used={} free={}";
  public static final String
      LOG_FAILED_TO_RELEASE_ARG_BYTES_TO_WRITE_BACK_MEMORY_BLOCK_USED_ARG_2EC89C4E =
          "向 write-back 内存块释放 {} 字节失败，used={}";
  public static final String LOG_EXCEPTION_WHILE_CLOSING_WRITE_BACK_MEMORY_BLOCK_D028F30B =
      "关闭 write-back 内存块时发生异常";
  public static final String LOG_WRITE_BACK_MEMORY_BLOCK_CLOSED_439D5463 = "Write-back 内存块已关闭";
  public static final String LOG_WRITE_FAILED_ATTEMPT_ARG_RETRYING_AFTER_ARG_MS_ARG_2B200618 =
      "写入失败（第 {} 次尝试），{}ms 后重试：{}";
  public static final String LOG_WRITE_FAILED_AFTER_ARG_ATTEMPTS_ARG_524C30CF =
      "写入在 {} 次尝试后失败：{}";
  public static final String LOG_FLUSH_FAILED_FOR_PIPELINE_ARG_3275329B =
      "Pipeline {} flush 失败";
  public static final String
      LOG_WINDOW_ARG_WRITE_BACK_FAILED_REMOVED_FROM_COMMIT_GATING_AND_ADVANCED_ALLOW_WATERMARK_TO_ARG_D3B309DD =
          "窗口 {} write-back 失败，已将其从 commit 门控中移除并将允许水位抬升至 {}";
  public static final String
      EXCEPTION_SINK_ENTRY_SIZE_ARG_BYTES_EXCEEDS_THE_PIPELINE_ADMISSION_LIMIT_ARG_BYTES_8F5A20CE =
          "Sink entry 大小 %s 字节超过 pipeline 准入上限（%s 字节）";
  public static final String
      LOG_SINK_ENTRY_SIZE_ARG_BYTES_EXCEEDS_THE_PIPELINE_ADMISSION_LIMIT_ARG_BYTES_REJECTING_WITHOUT_PARKING_47B35576 =
          "Sink entry 大小 {} 字节超过 pipeline 准入上限（{} 字节）；拒绝且不停泊";

  // ---------------------------------------------------------------------------
  // window engine
  // ---------------------------------------------------------------------------
  public static final String
      LOG_CAPACITY_WINDOW_PROCESSED_BATCH_ROWS_ARG_1309F37E =
          "CapacityWindowEngine 已处理一个批次，当前窗口行数: {}";
  public static final String LOG_CAPACITY_WINDOW_DROPPED_488FBEBE =
      "CapacityWindowEngine 已删除";
  public static final String LOG_CAPACITY_WINDOW_FORCE_CLOSED_57A2067F =
      "CapacityWindowEngine 已强制关闭";
  public static final String LOG_WINDOW_MEMORY_RELEASE_LISTENER_FAILED_A3F62F67 =
      "窗口内存释放监听器执行失败";
  public static final String
      LOG_ERRORS_DROPPING_WINDOW_ENGINE_OF_SUB_TASK_614AFE2C =
          "释放流子任务 {} 的窗口引擎时发生错误";
  public static final String
      LOG_WINDOW_FORCE_CLOSE_BY_MEMORY_PRESSURE_961D31B5 =
          "窗口引擎 {} 因 data 池内存压力强制关闭窗口 {}（暂停已超过 {} ms）";
  public static final String
      LOG_WINDOW_FORCE_DESTROY_BY_MEMORY_PRESSURE_731C3E95 =
          "窗口引擎 {} 因 data 池内存压力强制销毁已关闭窗口 {}（暂停已超过 {} ms）";
  public static final String
      LOG_WINDOW_MEMORY_POOL_EXHAUSTED_RESERVE_FAILED_4FCDE878 =
          "窗口 {} 内存池已耗尽: 预留 {} 字节失败（已用 {}/{}）; 调用方将施加背压";
  public static final String
      LOG_FAILED_TO_CLOSE_WINDOW_INPUT_BLOCK_AA6C1D2F =
          "关闭窗口 input 内存块失败";
  public static final String LOG_FAILED_TO_CLOSE_WINDOW_DATA_BLOCK_E603CF14 =
      "关闭窗口 data 内存块失败";
  public static final String
      LOG_FAILED_TO_CLOSE_WINDOW_OUTPUT_BLOCK_C1C736A2 =
          "关闭窗口 output 内存块失败";
  public static final String
      LOG_PERIOD_WINDOW_MONTH_SCHEDULER_STARTING_8277C994 =
          "PeriodWindowEngine 月模式调度器启动中";
  public static final String
      LOG_PERIOD_WINDOW_PERIODMS_NOT_SET_B688316F =
          "PeriodWindowEngine: periodMs 未设置，调度器将不会启动";
  public static final String
      LOG_PERIOD_WINDOW_SCHEDULER_STARTING_FIRST_TRIGGER_6DF9AD81 =
          "PeriodWindowEngine 调度器启动，首次触发在 {}ms 后，窗口: [{}, {})";
  public static final String
      LOG_PERIOD_WINDOW_MONTH_RESCHEDULE_REJECTED_18C381FF =
          "PeriodWindowEngine 月调度器拒绝了重新调度请求";
  public static final String LOG_PERIOD_WINDOW_SCHEDULER_STOPPED_0E3BC3D5 =
      "PeriodWindowEngine 调度器已停止";
  public static final String
      LOG_PERIOD_WINDOW_PERIODIC_TRIGGER_ADVANCED_D214BE38 =
          "PeriodWindowEngine 周期触发，已推进到窗口: [{}, {})";
  public static final String LOG_FAILED_TO_ADD_EVENT_TO_QUEUE_62D83E1B =
      "向队列添加事件失败";
  public static final String LOG_PERIOD_WINDOW_DROPPED_870E46A8 =
      "PeriodWindowEngine 已删除";
  public static final String LOG_PERIOD_WINDOW_FORCE_CLOSED_1650B493 =
      "PeriodWindowEngine 已强制关闭";
  public static final String EXCEPTION_PERIOD_WINDOW_DOES_NOT_ACCEPT_DATA_F936860F =
      "PeriodWindowEngine 不接受推送数据";
  public static final String
      LOG_TUMBLE_WINDOW_PROCESSED_ROWS_OPEN_WINDOWS_59851869 =
          "TumbleWindowEngine 已处理 {} 行，打开的窗口数: {}";
  public static final String LOG_TUMBLE_WINDOW_DROPPED_C71A44A2 =
      "TumbleWindowEngine 已删除";
  public static final String LOG_TUMBLE_WINDOW_FORCE_CLOSED_31857093 =
      "TumbleWindowEngine 已强制关闭";
  public static final String
      LOG_FAILED_TO_PROCESS_PUSHED_BATCH_CD3572E3 =
          "{} 处理推送批次失败，丢弃该失败批次";
  public static final String
      LOG_FAILED_TO_REPORT_GROUP_DONE_FOR_TABLET_A82B9F2A =
          "为丢弃批次中的 tablet {} 上报 group-done 失败";
  public static final String
      LOG_MEMORY_PAUSED_BY_POOL_EXHAUSTION_8BB73B2A =
          "{} 因 {} 池耗尽而内存暂停（data 池已用 {}/{} 字节，output 池 {}/{} 字节）; 处理暂停直至内存释放，后续推送将缓冲并对生产方施加背压";
  public static final String EXCEPTION_UNSUPPORTED_WINDOW_TYPE_E2440AEF =
      "不支持的窗口类型: %s";
  public static final String LOG_WINDOW_ENGINE_TICK_FAILED_FOR_TASK_6DB9ACDA =
      "任务 {} 的 WindowEngine tick 失败";
  public static final String
      LOG_FAILED_TO_RELEASE_ROWS_ITERATOR_F9D749CD =
          "释放窗口事件的行迭代器失败";
  public static final String
      EXCEPTION_EVENT_SCAN_COLUMN_MISSING_IN_FIRST_TABLET_12D68452 =
          "事件扫描列 '%s' 未出现在首个推送的 tablet 中，其类型未知; 请先推送包含该列的 tablet";
  public static final String
      EXCEPTION_EVENT_SCAN_COLUMN_TYPE_COUNT_MISMATCH_B770DE42 =
          "事件扫描列类型个数 %d 与扫描列个数 %d 不一致";
  public static final String
      LOG_FAILED_TO_COMPUTE_WINDOW_ARG_ARG_ARG_7E4CA205 =
          "窗口 [{}, {})（流 {}）计算失败，该窗口的结果将不会输出";
  public static final String EXCEPTION_COLUMN_VALUE_READ_WITHOUT_NULL_CHECK_AT_INDEX_ARG_B5EDBDD7 =
      "读取索引为 %s 的列值前未做 null 检查";
}
