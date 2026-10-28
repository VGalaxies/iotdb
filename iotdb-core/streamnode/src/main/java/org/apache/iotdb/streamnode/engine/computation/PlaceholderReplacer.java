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

package org.apache.iotdb.streamnode.engine.computation;

import org.apache.iotdb.streamnode.engine.window.IEventInfo;

import java.util.OptionalLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PlaceholderReplacer {

  private static final Pattern INDEX_PLACEHOLDER_PATTERN = Pattern.compile("\\$\\{(\\d+)\\}");

  /** SQL type of the time placeholders, as declared by the relational analyzer. */
  private static final String TIMESTAMP = "TIMESTAMP";

  /** SQL type of the row count placeholder, as declared by the relational analyzer. */
  private static final String INT64 = "INT64";

  public String replace(String subQuery, IEventInfo event) {
    if (subQuery == null) {
      return null;
    }
    String result = subQuery;
    result = replaceTimePlaceholder(result, "start_time", event.getStartTime());
    result = replaceTimePlaceholder(result, "end_time", event.getEndTime());
    result = replaceNamedPlaceholder(result, "row_num", toSqlLiteral(event.getRowCount(), INT64));
    result = replaceTimePlaceholder(result, "prev_time", event.getPrevTime());
    result = replaceTimePlaceholder(result, "next_time", event.getNextTime());
    result = replaceIndexedPlaceholder(result, event);
    return result;
  }

  /**
   * Replaces a time placeholder with the event's value. An absent value becomes a typed NULL (e.g.
   * {@code CAST(NULL AS TIMESTAMP)}): a bare NULL makes the DataNode build a typeless NullColumn,
   * which then fails TsBlock serialization in the session scan.
   */
  private String replaceTimePlaceholder(String sql, String name, OptionalLong value) {
    return replaceNamedPlaceholder(sql, name, toSqlLiteral(value, TIMESTAMP));
  }

  private String replaceNamedPlaceholder(String sql, String name, String value) {
    Pattern pattern = Pattern.compile("\\$\\{\\s*" + name + "\\s*}", Pattern.CASE_INSENSITIVE);
    return pattern.matcher(sql).replaceAll(Matcher.quoteReplacement(value));
  }

  private String replaceIndexedPlaceholder(String sql, IEventInfo event) {
    Matcher matcher = INDEX_PLACEHOLDER_PATTERN.matcher(sql);
    StringBuffer output = new StringBuffer();
    while (matcher.find()) {
      int index = Integer.parseInt(matcher.group(1));
      Object replacementValue = event.getN(index).orElse(null);
      matcher.appendReplacement(output, Matcher.quoteReplacement(toSqlLiteral(replacementValue)));
    }
    matcher.appendTail(output);
    return output.toString();
  }

  private String toSqlLiteral(Object value) {
    if (value == null) {
      // The index placeholder resolves to a PARTITION BY column, which must be a STRING TAG column.
      // A bare NULL would make the DataNode build a typeless NullColumn and fail TsBlock
      // serialization, so the absent value keeps its type.
      return "CAST(NULL AS STRING)";
    }
    if (value instanceof Number || value instanceof Boolean) {
      return String.valueOf(value);
    }
    String escaped = String.valueOf(value).replace("'", "''");
    return "'" + escaped + "'";
  }

  /**
   * Renders a placeholder value as a SQL literal. An absent value becomes a typed NULL so that the
   * resulting column keeps a concrete type.
   */
  private String toSqlLiteral(OptionalLong value, String sqlType) {
    return value.isPresent() ? Long.toString(value.getAsLong()) : "CAST(NULL AS " + sqlType + ")";
  }
}
