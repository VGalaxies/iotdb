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

import org.apache.iotdb.commons.stream.ColumnPartitionKey;
import org.apache.iotdb.commons.stream.PartitionKey;
import org.apache.iotdb.streamnode.engine.window.IEventInfo;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PlaceholderReplacer {

  private static final Pattern INDEX_PLACEHOLDER_PATTERN = Pattern.compile("\\$\\{(\\d+)\\}");

  public String replace(String subQuery, IEventInfo event) {
    return replace(subQuery, event, null);
  }

  public String replace(String subQuery, IEventInfo event, PartitionKey partitionKey) {
    if (subQuery == null) {
      return null;
    }
    String result = subQuery;
    result = replaceNamedPlaceholder(result, "start_time", toSqlLiteral(event.getStartTime()));
    result = replaceNamedPlaceholder(result, "end_time", toSqlLiteral(event.getEndTime()));
    result = replaceNamedPlaceholder(result, "row_num", toSqlLiteral(event.getRowCount()));
    result = replaceIndexedPlaceholder(result, partitionKey);
    return result;
  }

  private String replaceNamedPlaceholder(String sql, String name, String value) {
    Pattern pattern = Pattern.compile("\\$\\{\\s*" + name + "\\s*}", Pattern.CASE_INSENSITIVE);
    return pattern.matcher(sql).replaceAll(Matcher.quoteReplacement(value));
  }

  private String replaceIndexedPlaceholder(String sql, PartitionKey partitionKey) {
    List<Object> partitionValues = extractPartitionValues(partitionKey);
    return replaceIndexPattern(sql, INDEX_PLACEHOLDER_PATTERN, partitionValues);
  }

  private String replaceIndexPattern(String sql, Pattern pattern, List<Object> values) {
    Matcher matcher = pattern.matcher(sql);
    StringBuffer output = new StringBuffer();
    while (matcher.find()) {
      int index = Integer.parseInt(matcher.group(1));
      Object replacementValue = index > 0 && index <= values.size() ? values.get(index - 1) : null;
      matcher.appendReplacement(output, Matcher.quoteReplacement(toSqlLiteral(replacementValue)));
    }
    matcher.appendTail(output);
    return output.toString();
  }

  private List<Object> extractPartitionValues(PartitionKey partitionKey) {
    if (!(partitionKey instanceof ColumnPartitionKey)) {
      return new ArrayList<>();
    }
    Map<String, Object> columnValues = ((ColumnPartitionKey) partitionKey).getColumnValues();
    if (columnValues == null || columnValues.isEmpty()) {
      return new ArrayList<>();
    }
    return new ArrayList<>(columnValues.values());
  }

  private String toSqlLiteral(Object value) {
    if (value == null) {
      return "NULL";
    }
    if (value instanceof Number || value instanceof Boolean) {
      return String.valueOf(value);
    }
    String escaped = String.valueOf(value).replace("'", "''");
    return "'" + escaped + "'";
  }

  private String toSqlLiteral(OptionalLong value) {
    return value.isPresent() ? Long.toString(value.getAsLong()) : "NULL";
  }
}
