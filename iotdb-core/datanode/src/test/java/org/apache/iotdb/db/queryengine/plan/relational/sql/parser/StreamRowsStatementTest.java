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

package org.apache.iotdb.db.queryengine.plan.relational.sql.parser;

import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Statement;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.stream.CreateStream;
import org.apache.iotdb.db.protocol.session.IClientSession;
import org.apache.iotdb.db.protocol.session.InternalClientSession;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.time.ZoneId;

public class StreamRowsStatementTest {

  private SqlParser sqlParser;
  private IClientSession clientSession;

  @Before
  public void setUp() {
    sqlParser = new SqlParser();
    clientSession = new InternalClientSession("testClient");
  }

  @Test
  public void testStringRowsJoinWithTable() {
    // Test ${rows} JOIN with other tables
    Statement statement =
        sqlParser.createStatement(
            "CREATE STREAM s1 FROM table1 "
                + "TUMBLE(size => 1h) "
                + "INTO table2 "
                + "SELECT ${start_time}, t1.id, t2.s1 "
                + "FROM ${rows} t1 JOIN t2 ON t1.id = t2.id",
            ZoneId.systemDefault(),
            clientSession);

    Assert.assertTrue("Statement should be CreateStream", statement instanceof CreateStream);
    CreateStream createStream = (CreateStream) statement;
    Assert.assertNotNull("Query should not be null", createStream.getQuery());
  }

  @Test
  public void testStreamWithWhereClause() {
    // Test CREATE STREAM with WHERE clause filtering ${rows}
    Statement statement =
        sqlParser.createStatement(
            "CREATE STREAM s1 FROM table1 "
                + "TUMBLE(size => 1h) "
                + "INTO table2 "
                + "SELECT ${start_time}, t1.id, t1.value "
                + "FROM ${rows} t1 "
                + "WHERE t1.value > 100",
            ZoneId.systemDefault(),
            clientSession);

    Assert.assertTrue("Statement should be CreateStream", statement instanceof CreateStream);
  }

  @Test
  public void testStreamRowsWithAggregation() {
    // Test ${rows} with aggregation functions
    Statement statement =
        sqlParser.createStatement(
            "CREATE STREAM s1 FROM table1 "
                + "TUMBLE(size => 1h) "
                + "INTO table2 "
                + "SELECT ${start_time}, t1.category, COUNT(*), AVG(t1.value) "
                + "FROM ${rows} t1 "
                + "GROUP BY t1.category",
            ZoneId.systemDefault(),
            clientSession);

    Assert.assertTrue("Statement should be CreateStream", statement instanceof CreateStream);
  }

  @Test
  public void testStreamRowsWithAlias() {
    // Test ${rows} with explicit AS alias
    Statement statement =
        sqlParser.createStatement(
            "CREATE STREAM s1 FROM table1 "
                + "TUMBLE(size => 1h) "
                + "INTO table2 "
                + "SELECT ${start_time}, rows.id, rows.value "
                + "FROM ${rows} AS rows",
            ZoneId.systemDefault(),
            clientSession);

    Assert.assertTrue("Statement should be CreateStream", statement instanceof CreateStream);
  }

  @Test
  public void testStreamRowsWithoutAlias() {
    // Test ${rows} without explicit alias - should work
    Statement statement =
        sqlParser.createStatement(
            "CREATE STREAM s1 FROM table1 "
                + "TUMBLE(size => 1h) "
                + "INTO table2 "
                + "SELECT ${start_time}, id, value "
                + "FROM ${rows}",
            ZoneId.systemDefault(),
            clientSession);

    Assert.assertTrue("Statement should be CreateStream", statement instanceof CreateStream);
  }
}
