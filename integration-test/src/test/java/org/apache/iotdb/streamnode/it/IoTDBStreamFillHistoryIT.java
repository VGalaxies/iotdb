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

import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.it.env.StreamEnvFactory;
import org.apache.iotdb.itbase.category.StreamClusterIT;
import org.apache.iotdb.itbase.env.BaseEnv;

import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

@Category({StreamClusterIT.class})
public class IoTDBStreamFillHistoryIT {

  private static final BaseEnv ENV = StreamEnvFactory.getEnv(1);
  private static final String DATABASE = "stream_fill_history";
  private static final String STREAM = "stream_fill_history_task";

  @BeforeClass
  public static void setUp() throws Exception {
    ENV.getConfig()
        .getCommonConfig()
        .setSubscriptionEnabled(true)
        .setPipeMemoryManagementEnabled(false)
        .setDataRegionConsensusProtocolClass(ConsensusFactory.IOT_CONSENSUS);
    ENV.initClusterEnvironment(1, 1, 30);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    ENV.cleanClusterEnvironment();
  }

  @After
  public void cleanUp() throws Exception {
    try (Connection connection = ENV.getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("DROP STREAM IF EXISTS " + STREAM);
      statement.execute("DROP DATABASE IF EXISTS " + DATABASE);
    }
  }

  @Test
  public void testFillHistoryThenRealtime() throws Exception {
    try (Connection connection = ENV.getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      prepareTables(statement);
      statement.execute("INSERT INTO source_table(time, value) VALUES (1000, 1), (1001, 2)");
      statement.execute("FLUSH");

      createStream(statement, "fill_history = true, fill_history_start_time = 1000");
      statement.execute("START STREAM " + STREAM);

      awaitSinkValue(statement, 1000, 1);
      statement.execute("INSERT INTO source_table(time, value) VALUES (1002, 3), (1003, 4)");
      awaitSinkValue(statement, 1002, 3);
    }
  }

  @Test
  public void testFillHistoryStartTimeFiltersHistory() throws Exception {
    try (Connection connection = ENV.getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      prepareTables(statement);
      statement.execute(
          "INSERT INTO source_table(time, value) VALUES (1000, 1), (1002, 2), (1003, 3)");
      statement.execute("FLUSH");

      createStream(statement, "fill_history = true, fill_history_start_time = 1002");
      statement.execute("START STREAM " + STREAM);

      awaitSinkValue(statement, 1002, 2);
      assertNoSinkRow(statement, 1000);
    }
  }

  @Test
  public void testFillHistoryStartTimeBeforeEarliestUsesEarliestData() throws Exception {
    try (Connection connection = ENV.getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      prepareTables(statement);
      statement.execute("INSERT INTO source_table(time, value) VALUES (1000, 1), (1001, 2)");
      statement.execute("FLUSH");

      createStream(statement, "fill_history = true, fill_history_start_time = 0");
      statement.execute("START STREAM " + STREAM);

      awaitSinkValue(statement, 1000, 1);
    }
  }

  @Test
  public void testFillHistoryFalseIgnoresHistoricalData() throws Exception {
    try (Connection connection = ENV.getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      prepareTables(statement);
      statement.execute("INSERT INTO source_table(time, value) VALUES (1000, 1), (1001, 2)");
      statement.execute("FLUSH");

      createStream(statement, "fill_history = false");
      statement.execute("START STREAM " + STREAM);
      assertNoSinkRows(statement);

      statement.execute("INSERT INTO source_table(time, value) VALUES (1002, 3), (1003, 4)");
      awaitSinkValue(statement, 1002, 3);
    }
  }

  @Test
  public void testInvalidFillHistoryOptionsAreRejected() throws Exception {
    try (Connection connection = ENV.getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      prepareTables(statement);
      assertThrows(
          SQLException.class, () -> createStream(statement, "fill_history_start_time = 1000"));
      assertThrows(
          SQLException.class,
          () ->
              createStream(
                  statement, "fill_history = true, fill_history_start_time = " + Long.MAX_VALUE));
    }
  }

  private static void prepareTables(final Statement statement) throws SQLException {
    statement.execute("CREATE DATABASE " + DATABASE);
    statement.execute("USE " + DATABASE);
    statement.execute("CREATE TABLE source_table (value INT64 FIELD)");
    statement.execute("CREATE TABLE sink_table (value INT64 FIELD)");
  }

  private static void createStream(final Statement statement, final String properties)
      throws SQLException {
    statement.execute(
        "CREATE STREAM "
            + STREAM
            + " FROM source_table TUMBLE(1ms, 0)"
            + " INTO sink_table(time, value)"
            + " SELECT ${START_TIME}, max(value) FROM ${ROWS}"
            + " WITH ("
            + properties
            + ", max_delay_window_close = 5s)");
  }

  private static void awaitSinkValue(
      final Statement statement, final long time, final long expected) {
    Awaitility.await()
        .pollInSameThread()
        .pollInterval(1, TimeUnit.SECONDS)
        .atMost(120, TimeUnit.SECONDS)
        .untilAsserted(
            () -> {
              try (ResultSet resultSet =
                  statement.executeQuery("SELECT value FROM sink_table WHERE time = " + time)) {
                assertTrue("Missing sink row at time " + time, resultSet.next());
                assertEquals(expected, resultSet.getLong("value"));
                assertFalse(resultSet.next());
              }
            });
  }

  private static void assertNoSinkRow(final Statement statement, final long time)
      throws SQLException {
    try (ResultSet resultSet =
        statement.executeQuery("SELECT value FROM sink_table WHERE time = " + time)) {
      assertFalse("Unexpected sink row at time " + time, resultSet.next());
    }
  }

  private static void assertNoSinkRows(final Statement statement) throws SQLException {
    try (ResultSet resultSet = statement.executeQuery("SELECT count(*) FROM sink_table")) {
      assertTrue(resultSet.next());
      assertEquals(0, resultSet.getInt(1));
    }
  }
}
