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
      "Placeholder ${rows} must be used with an event window";
  public static final String EXCEPTION_PLACEHOLDER_ROWS_CANNOT_BE_USED_WITH_PERIOD_EVENT_WINDOW_1667B0B2 =
      "Placeholder ${rows} cannot be used with period event window";
  public static final String EXCEPTION_PLACEHOLDER_ROWS_REQUIRES_A_STREAM_SOURCE_TABLE_EF99E714 =
      "Placeholder ${rows} requires a stream source table";
  public static final String EXCEPTION_PLACEHOLDER_ROWS_CAN_APPEAR_AT_MOST_ONCE_IN_THE_CALCULATION_PLAN_8693DFC4 =
      "The ${rows} placeholder can appear at most once in the calculation plan";
  public static final String EXCEPTION_PLACEHOLDER_ROWS_CANNOT_PARTICIPATE_IN_A_JOIN_OPERATION_D6F83512 =
      "The ${rows} placeholder cannot participate in a JOIN operation";
  public static final String EXCEPTION_NON_ATTRIBUTE_COLUMNS_WITH_ROWS_PLACEHOLDER_BE816EFA =
      "The calculation plan with ${rows} placeholder must not reference ATTRIBUTE columns, but %s is an ATTRIBUTE column";

  // --- Stream source ---

  public static final String EXCEPTION_THE_SOURCE_TABLE_OF_THE_STREAM_CANNOT_BE_A_SYSTEM_TABLE_OR_A_VIEW_44C037BC =
      "The source table of the stream cannot be a system table or a view";

  // --- Event window ---

  public static final String EXCEPTION_PERIOD_EVENT_WINDOW_CANNOT_BE_USED_WITH_FROM_CLAUSE_D1549FCF =
      "Period event window cannot be used with FROM clause";
  public static final String EXCEPTION_PERIOD_EVENT_WINDOW_CANNOT_BE_USED_WITH_PARTITION_BY_38B93B69 =
      "Period event window cannot be used with PARTITION BY";
  public static final String EXCEPTION_TUMBLE_EVENT_WINDOW_MUST_BE_USED_WITH_FROM_CLAUSE_C77B71B9 =
      "Tumble event window must be used with FROM clause";
  public static final String EXCEPTION_EXCEEDING_ARGUMENTS_PROVIDED_D43758F6 =
      "Exceeding arguments provided";

  // --- PreFilter / PARTITION BY ---

  public static final String PREFILTER_CLAUSE = "PreFilter clause";
  public static final String
      EXCEPTION_PREFILTER_CLAUSE_OF_THE_STREAM_SOURCE_IS_NOT_SUPPORTED_YET_7BCA6A84 =
          "PreFilter clause of the stream source is not supported yet";
  public static final String EXCEPTION_PREFILTER_CLAUSE_MUST_EVALUATE_TO_BOOLEAN_TYPE_D79AF560 =
      "PreFilter clause must evaluate to a boolean: actual type %s";
  public static final String EXCEPTION_PARTITION_BY_MUST_NOT_BE_ORDINAL_POSITIONS_E3191E38 =
      "PARTITION BY must not be ordinal positions";
  public static final String EXCEPTION_PARTITION_BY_MUST_BE_COLUMN_REFERENCE_FF33719A =
      "PARTITION BY must be a column reference";

  // --- Sink table ---
  public static final String EXCEPTION_SINK_TABLE_MUST_BE_DIFFERENT_FROM_THE_SOURCE_TABLE_91B401CC =
      "Sink table must be different from the source table";

  // --- Stream properties (WITH) ---

  public static final String
      EXCEPTION_STREAM_PROPERTY_ARG_MUST_BE_A_TIME_DURATION_E_G_1S_OR_AN_INTEGER_BUT_GOT_ARG_DEE25F8B =
          "Stream property %s must be a time duration (e.g. 1s) or an integer, but got %s";
  public static final String
      EXCEPTION_STREAM_PROPERTY_ARG_MUST_BE_AT_LEAST_5_SECONDS_BUT_GOT_ARG_AF49F29C =
          "Stream property %s must be at least 5 seconds, but got %s";
  public static final String
      EXCEPTION_STREAM_PROPERTY_ARG_MUST_BE_A_BOOLEAN_BUT_GOT_ARG_DA94CDE1 =
          "Stream property %s must be a boolean, but got %s";
  public static final String
      EXCEPTION_STREAM_PROPERTY_ARG_MUST_BE_A_DATE_TIME_STRING_OR_AN_INTEGER_BUT_GOT_ARG_5E7A1F30 =
          "Stream property %s must be a date-time string or an integer timestamp, but got %s";
  public static final String
      EXCEPTION_STREAM_PROPERTY_ARG_REQUIRES_FILL_HISTORY_TRUE_6B1C4D2E =
          "Stream property %s requires fill_history = true";
  public static final String
      EXCEPTION_STREAM_PROPERTY_ARG_CANNOT_BE_USED_WITH_FILL_HISTORY_3F8B6E52 =
          "Stream property %s cannot be used together with fill_history";
  public static final String
      EXCEPTION_FAILED_TO_PARSE_STREAM_PROPERTY_ARG_AS_DATE_TIME_ARG_7D4E2B63 =
          "Failed to parse stream property %s = %s as a date-time: %s";
  public static final String EXCEPTION_UNRECOGNIZED_STREAM_PROPERTY_ARG_11A5C18E =
      "Unrecognized stream property: %s";
  public static final String
      EXCEPTION_STREAM_PROPERTY_ARG_ONLY_SUPPORTS_DURATION_UNITS_ARG_BUT_GOT_ARG_C90305A1 =
          "Stream property %s only supports duration units %s, but got %s";
  public static final String
      EXCEPTION_EVENT_WINDOW_ARG_ARGUMENT_ARG_ONLY_SUPPORTS_DURATION_UNITS_ARG_BUT_GOT_ARG_6F47BEF1 =
          "Event window %s argument %s only supports duration units %s, but got %s";
}
