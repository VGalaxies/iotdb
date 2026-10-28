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

package org.apache.iotdb.commons.i18n;

public final class StreamMessages {

  // --- Event window ---

  public static final String
      EXCEPTION_THE_ORIGIN_OF_THE_EVENT_WINDOW_MUST_BE_INTEGER_OR_DATE_TIME_D74A8A7B =
          "事件窗口的 origin 必须是整数或日期时间";
  public static final String
      EXCEPTION_SIMULTANEOUS_SETTING_OF_MONTHLY_AND_NON_MONTHLY_INTERVALS_IS_NOT_SUPPORTED_C62486CB =
          "不支持同时设置按月和非按月的时间间隔。";

  // --- Period event window ---

  public static final String EXCEPTION_PERIOD_EVENT_WINDOW_REQUIRES_PERIOD_TIME_ARGUMENT_F42284C5 =
      "Period 事件窗口必须提供 'period_time' 参数";
  public static final String
      EXCEPTION_THE_PERIOD_TIME_OF_THE_PERIOD_EVENT_WINDOW_MUST_BE_POSITIVE_F2159F88 =
          "Period 事件窗口的 period_time 必须为正数";
  public static final String EXCEPTION_INVALID_ARGUMENT_TYPE_FOR_PERIOD_EVENT_WINDOW_E9850AC6 =
      "Period 事件窗口的参数类型无效";

  // --- Tumble event window ---

  public static final String EXCEPTION_TUMBLE_EVENT_WINDOW_REQUIRES_SIZE_ARGUMENT_CDE35080 =
      "Tumble 事件窗口必须提供 'size' 参数";
  public static final String
      EXCEPTION_THE_SIZE_OF_THE_TUMBLE_EVENT_WINDOW_MUST_BE_POSITIVE_C09B224F =
          "Tumble 事件窗口的 size 必须为正数";
  public static final String EXCEPTION_INVALID_ARGUMENT_TYPE_FOR_TUMBLE_EVENT_WINDOW_F91B3B22 =
      "Tumble 事件窗口的参数类型无效";

  // --- Capacity event window ---

  public static final String EXCEPTION_CAPACITY_EVENT_WINDOW_REQUIRES_SIZE_ARGUMENT_F4711FED =
      "Capacity 事件窗口必须提供 'size' 参数";
  public static final String EXCEPTION_INVALID_ARGUMENT_TYPE_FOR_CAPACITY_EVENT_WINDOW_F6DA37DE =
      "Capacity 事件窗口的参数类型无效";

  // --- Variation event window ---

  public static final String EXCEPTION_VARIATION_EVENT_WINDOW_REQUIRES_COL_ARGUMENT_8D9E7630 =
      "Variation 事件窗口必须提供 'col' 参数";
  public static final String EXCEPTION_INVALID_ARGUMENT_TYPE_FOR_VARIATION_EVENT_WINDOW_481BB5BB =
      "Variation 事件窗口的参数类型无效";
}
