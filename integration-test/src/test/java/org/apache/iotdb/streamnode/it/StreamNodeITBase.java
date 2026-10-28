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

package org.apache.iotdb.streamnode.it;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertTrue;

/** Shared JDBC helpers for StreamNode integration tests. */
public abstract class StreamNodeITBase {

  protected static Map<String, Object> queryStreamRow(Statement statement, String streamName)
      throws SQLException {
    try (ResultSet rs =
        statement.executeQuery(
            "SELECT * FROM information_schema.streams WHERE stream_name = '" + streamName + "'")) {
      if (!rs.next()) {
        return null;
      }
      Map<String, Object> row = new LinkedHashMap<>();
      ResultSetMetaData metadata = rs.getMetaData();
      for (int i = 1; i <= metadata.getColumnCount(); i++) {
        row.put(metadata.getColumnLabel(i), rs.getObject(i));
      }
      return row;
    }
  }

  protected static List<Map<String, Object>> queryRows(Statement statement, String sql)
      throws SQLException {
    try (ResultSet rs = statement.executeQuery(sql)) {
      List<Map<String, Object>> rows = new ArrayList<>();
      ResultSetMetaData metadata = rs.getMetaData();
      while (rs.next()) {
        Map<String, Object> row = new LinkedHashMap<>();
        for (int i = 1; i <= metadata.getColumnCount(); i++) {
          row.put(metadata.getColumnLabel(i), rs.getObject(i));
        }
        rows.add(row);
      }
      return rows;
    }
  }

  protected static long asLong(Object value) {
    if (value instanceof Number) {
      return ((Number) value).longValue();
    }
    if (value instanceof Timestamp) {
      return ((Timestamp) value).getTime();
    }
    throw new AssertionError("Expected a numeric/timestamp value but got: " + value);
  }

  protected static long asLongOrDefault(Object value) {
    return value == null ? 0L : asLong(value);
  }

  protected static int countRows(Statement statement, String qualifiedTableName)
      throws SQLException {
    try (ResultSet rs = statement.executeQuery("SELECT count(*) FROM " + qualifiedTableName)) {
      assertTrue(rs.next());
      return rs.getInt(1);
    }
  }
}
