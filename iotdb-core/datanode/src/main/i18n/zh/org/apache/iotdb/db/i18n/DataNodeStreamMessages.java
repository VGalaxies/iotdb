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

package org.apache.iotdb.db.i18n;

public final class DataNodeStreamMessages {

  // --- Stream rows placeholder ---

  public static final String EXCEPTION_PLACEHOLDER_ROWS_MUST_BE_USED_WITH_EVENT_WINDOW_E3EACBB9 =
      "${rows} 占位符必须与事件窗口一起使用";
  public static final String EXCEPTION_PLACEHOLDER_ROWS_CANNOT_BE_USED_WITH_PERIOD_EVENT_WINDOW_1667B0B2 =
      "${rows} 占位符不能与 Period 事件窗口一起使用";
  public static final String EXCEPTION_PLACEHOLDER_ROWS_REQUIRES_A_STREAM_SOURCE_TABLE_EF99E714 =
      "${rows} 占位符需要流计算源表";
  public static final String EXCEPTION_PLACEHOLDER_ROWS_CAN_APPEAR_AT_MOST_ONCE_IN_THE_CALCULATION_PLAN_8693DFC4 =
      "${rows} 占位符在流计算查询计划中最多只能出现一次";
  public static final String EXCEPTION_PLACEHOLDER_ROWS_CANNOT_PARTICIPATE_IN_A_JOIN_OPERATION_D6F83512 =
      "${rows} 占位符不能参与 JOIN 操作";
  public static final String EXCEPTION_NON_ATTRIBUTE_COLUMNS_WITH_ROWS_PLACEHOLDER_BE816EFA =
      "使用 ${rows} 占位符时，计算计划不能引用 ATTRIBUTE 列，但 %s 是 ATTRIBUTE 列";

  // --- Stream source ---

  public static final String EXCEPTION_THE_SOURCE_TABLE_OF_THE_STREAM_CANNOT_BE_A_SYSTEM_TABLE_OR_A_VIEW_44C037BC =
      "流计算的源表不能是系统表或视图";

  // --- Event window ---

  public static final String EXCEPTION_PERIOD_EVENT_WINDOW_CANNOT_BE_USED_WITH_FROM_CLAUSE_D1549FCF =
      "Period 事件窗口不能与 FROM 子句一起使用";
  public static final String EXCEPTION_PERIOD_EVENT_WINDOW_CANNOT_BE_USED_WITH_PARTITION_BY_38B93B69 =
      "Period 事件窗口不能与 PARTITION BY 一起使用";
  public static final String EXCEPTION_TUMBLE_EVENT_WINDOW_MUST_BE_USED_WITH_FROM_CLAUSE_C77B71B9 =
      "Tumble 事件窗口必须与 FROM 子句一起使用";
  public static final String EXCEPTION_EXCEEDING_ARGUMENTS_PROVIDED_D43758F6 =
      "提供的参数过多";

  // --- PreFilter / PARTITION BY ---

  public static final String PREFILTER_CLAUSE = "PreFilter 子句";
  public static final String
      EXCEPTION_PREFILTER_CLAUSE_OF_THE_STREAM_SOURCE_IS_NOT_SUPPORTED_YET_7BCA6A84 =
          "流源暂不支持 PreFilter 子句";
  public static final String EXCEPTION_PREFILTER_CLAUSE_MUST_EVALUATE_TO_BOOLEAN_TYPE_D79AF560 =
      "PreFilter 子句的计算结果必须为布尔值，实际类型为 %s";
  public static final String EXCEPTION_PARTITION_BY_MUST_NOT_BE_ORDINAL_POSITIONS_E3191E38 =
      "PARTITION BY 不能使用序数位置";
  public static final String EXCEPTION_PARTITION_BY_MUST_BE_COLUMN_REFERENCE_FF33719A =
      "PARTITION BY 必须是列引用";

  // --- Sink table ---
  public static final String EXCEPTION_SINK_TABLE_MUST_BE_DIFFERENT_FROM_THE_SOURCE_TABLE_91B401CC =
      "输出表不能与源表相同";

  // --- Stream properties (WITH) ---

  public static final String
      EXCEPTION_STREAM_PROPERTY_ARG_MUST_BE_A_TIME_DURATION_E_G_1S_OR_AN_INTEGER_BUT_GOT_ARG_DEE25F8B =
          "流属性 %s 必须是时间时长（如 1s）或整数，但当前为 %s";
  public static final String
      EXCEPTION_STREAM_PROPERTY_ARG_MUST_BE_AT_LEAST_5_SECONDS_BUT_GOT_ARG_AF49F29C =
          "流属性 %s 必须至少为 5 秒，但当前为 %s";
  public static final String
      EXCEPTION_STREAM_PROPERTY_ARG_MUST_BE_A_BOOLEAN_BUT_GOT_ARG_DA94CDE1 =
          "流属性 %s 必须是布尔值，但当前为 %s";
  public static final String EXCEPTION_UNRECOGNIZED_STREAM_PROPERTY_ARG_11A5C18E =
      "无法识别的流属性：%s";
  public static final String
      EXCEPTION_STREAM_PROPERTY_ARG_ONLY_SUPPORTS_DURATION_UNITS_ARG_BUT_GOT_ARG_C90305A1 =
          "流属性 %s 仅支持持续时间单位 %s，但当前为 %s";
  public static final String
      EXCEPTION_EVENT_WINDOW_ARG_ARGUMENT_ARG_ONLY_SUPPORTS_DURATION_UNITS_ARG_BUT_GOT_ARG_6F47BEF1 =
          "事件窗口 %s 的参数 %s 仅支持持续时间单位 %s，但当前为 %s";
}
