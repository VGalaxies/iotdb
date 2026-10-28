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

import org.apache.iotdb.commons.schema.column.ColumnHeaderConstant;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.it.env.StreamEnvFactory;
import org.apache.iotdb.itbase.category.StreamClusterIT;
import org.apache.iotdb.itbase.env.BaseEnv;

import org.awaitility.Awaitility;
import org.awaitility.core.ConditionFactory;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Integration tests for START STREAM (spec cases START-01/02/03/04 of the stream-computing
 * requirements doc). Assertions follow the spec, so failures pinpoint the gaps that the
 * implementation must close (e.g. the spec only defines RUNNING/STOPPED statuses, while the current
 * implementation reports CREATED for a stream that was never started).
 */
@Category({StreamClusterIT.class})
public class StreamNodeStartStreamIT extends StreamNodeITBase {

  private static final ConditionFactory AWAIT =
      Awaitility.await()
          .pollInSameThread()
          .pollDelay(1, TimeUnit.SECONDS)
          .pollInterval(1, TimeUnit.SECONDS)
          .atMost(20, TimeUnit.SECONDS);

  private static final ConditionFactory OUTPUT_AWAIT =
      Awaitility.await()
          .pollInSameThread()
          .pollDelay(1, TimeUnit.SECONDS)
          .pollInterval(1, TimeUnit.SECONDS)
          .atMost(30, TimeUnit.SECONDS);

  private static final String[] TIME_COLUMNS = {
    ColumnHeaderConstant.CREATION_TIME_TABLE_MODEL,
    ColumnHeaderConstant.STREAM_LAST_UP_TIME_TABLE_MODEL,
    ColumnHeaderConstant.STREAM_LAST_DOWN_TIME_TABLE_MODEL,
    ColumnHeaderConstant.STREAM_LAST_HEARTBEAT_TIME_TABLE_MODEL
  };

  @BeforeClass
  public static void setUp() throws Exception {
    // Init 1C1D1S cluster environment (1 ConfigNode, 1 DataNode, 1 StreamNode).
    StreamEnvFactory.getEnv(1)
        .getConfig()
        .getCommonConfig()
        .setSubscriptionEnabled(true)
        .setDataRegionConsensusProtocolClass(ConsensusFactory.IOT_CONSENSUS);
    StreamEnvFactory.getEnv(1).initClusterEnvironment(1, 1, 30);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    StreamEnvFactory.getEnv().cleanClusterEnvironment();
  }

  @After
  public void cleanUpTestDatabase() throws Exception {
    try (Connection connection =
            StreamEnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      List<String> streamNames = new ArrayList<>();
      try (ResultSet rs = statement.executeQuery("show streams")) {
        while (rs.next()) {
          streamNames.add(rs.getString(ColumnHeaderConstant.STREAM_NAME_TABLE_MODEL));
        }
      } catch (Exception ignored) {
        // no streams registered
      }
      for (String name : streamNames) {
        try {
          statement.execute("DROP STREAM " + name);
        } catch (Exception ignored) {
          // best-effort cleanup
        }
      }
      try {
        statement.execute("DROP DATABASE test");
      } catch (Exception ignored) {
        // database may not exist if a test failed before creating it
      }
    }
  }

  /**
   * START-01: a newly created stream (TUMBLE and PERIOD) does not start automatically (status
   * STOPPED, no output for valid input / passed trigger points); after START STREAM it enters
   * RUNNING with last_up_time reflecting this transition, and new triggers produce the correct
   * output per the stream definition.
   */
  @Test
  public void testStartTumbleAndPeriodStreamsManually() throws Exception {
    try (Connection connection =
            StreamEnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE test");
      statement.execute("USE test");

      verifyTumbleStreamManualStart(statement);
      verifyPeriodStreamManualStart(statement);
    }
  }

  /**
   * START-02: two STOPPED streams are prepared (create + explicit STOP), then only stream_a is
   * started: stream_a enters RUNNING with an updated last_up_time; stream_b keeps STOPPED and its
   * status and time fields are unchanged by the command.
   */
  @Test
  public void testStartOnlyStartsTheSpecifiedStream() throws Exception {
    final String streamA = "stream_a";
    final String streamB = "stream_b";
    try (Connection connection =
            StreamEnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE test");
      statement.execute("USE test");
      statement.execute(
          "CREATE TABLE src_t (time TIMESTAMP TIME, device_id STRING TAG, value INT64 FIELD)");
      statement.execute("CREATE TABLE sink_a (time TIMESTAMP TIME, value INT64 FIELD)");
      statement.execute("CREATE TABLE sink_b (time TIMESTAMP TIME, value INT64 FIELD)");
      statement.execute(
          "CREATE STREAM "
              + streamA
              + " FROM test.src_t TUMBLE(size => 1h) INTO test.sink_a(time, value)"
              + " SELECT ${START_TIME}, count(*) FROM ${ROWS}");
      statement.execute(
          "CREATE STREAM "
              + streamB
              + " FROM test.src_t TUMBLE(size => 1h) INTO test.sink_b(time, value)"
              + " SELECT ${START_TIME}, count(*) FROM ${ROWS}");

      AWAIT.untilAsserted(
          () -> {
            assertNotNull(queryStreamRow(statement, streamA));
            assertNotNull(queryStreamRow(statement, streamB));
          });

      // Preparation: place both streams in STOPPED before the scenario.
      statement.execute("STOP STREAM " + streamA);
      statement.execute("STOP STREAM " + streamB);
      AWAIT.untilAsserted(
          () -> {
            assertEquals(
                "stream_a should be STOPPED after the preparation STOP",
                "STOPPED",
                queryStreamRow(statement, streamA)
                    .get(ColumnHeaderConstant.STREAM_STATUS_TABLE_MODEL));
            assertEquals(
                "stream_b should be STOPPED after the preparation STOP",
                "STOPPED",
                queryStreamRow(statement, streamB)
                    .get(ColumnHeaderConstant.STREAM_STATUS_TABLE_MODEL));
          });
      Map<String, Object> rowABefore = queryStreamRow(statement, streamA);
      Map<String, Object> rowBBefore = queryStreamRow(statement, streamB);

      // Only start stream_a.
      long startMoment = System.currentTimeMillis();
      statement.execute("START STREAM " + streamA);

      AWAIT.untilAsserted(
          () ->
              assertEquals(
                  "Only stream_a should be started by START STREAM stream_a",
                  "RUNNING",
                  queryStreamRow(statement, streamA)
                      .get(ColumnHeaderConstant.STREAM_STATUS_TABLE_MODEL)));

      Map<String, Object> rowAAfter = queryStreamRow(statement, streamA);
      long lastUpA = asLong(rowAAfter.get(ColumnHeaderConstant.STREAM_LAST_UP_TIME_TABLE_MODEL));
      assertTrue(
          "stream_a's last_up_time should reflect this START transition",
          lastUpA >= startMoment - 2000L && lastUpA <= System.currentTimeMillis() + 2000L);
      assertTrue(
          "stream_a's last_up_time should be updated by this START",
          lastUpA
              > asLongOrDefault(
                  rowABefore.get(ColumnHeaderConstant.STREAM_LAST_UP_TIME_TABLE_MODEL)));

      // stream_b keeps STOPPED; its status and time fields are unaffected by the command.
      assertStreamUnchanged(statement, streamB, rowBBefore, "starting stream_a");
      assertEquals(
          "stream_b must remain STOPPED when only stream_a is started",
          "STOPPED",
          queryStreamRow(statement, streamB).get(ColumnHeaderConstant.STREAM_STATUS_TABLE_MODEL));
    }
  }

  /** Verifies that a stream's status, time fields and running_on are unchanged. */
  private void assertStreamUnchanged(
      Statement statement, String streamName, Map<String, Object> rowBefore, String cause)
      throws SQLException {
    Map<String, Object> rowAfter = queryStreamRow(statement, streamName);
    assertNotNull("Stream " + streamName + " should still be visible", rowAfter);
    assertEquals(
        cause + " must not change the status of " + streamName,
        rowBefore.get(ColumnHeaderConstant.STREAM_STATUS_TABLE_MODEL),
        rowAfter.get(ColumnHeaderConstant.STREAM_STATUS_TABLE_MODEL));
    for (String timeColumn : TIME_COLUMNS) {
      assertEquals(
          cause + " must not change " + streamName + "'s " + timeColumn,
          asLongOrDefault(rowBefore.get(timeColumn)),
          asLongOrDefault(rowAfter.get(timeColumn)));
    }
    assertTrue(
        cause + " must not assign " + streamName + " to a StreamNode",
        rowAfter.get(ColumnHeaderConstant.STREAM_RUNNING_ON_TABLE_MODEL) == null
            || rowAfter
                .get(ColumnHeaderConstant.STREAM_RUNNING_ON_TABLE_MODEL)
                .toString()
                .isEmpty());
  }

  /**
   * START-03: starting a non-existent stream fails with "Stream missing_stream does not exist" and
   * does not create a stream record.
   */
  @Test
  public void testStartNonExistentStreamFails() throws Exception {
    try (Connection connection =
            StreamEnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      assertNull(
          "missing_stream should not exist before the failed START",
          queryStreamRow(statement, "missing_stream"));

      boolean failed = false;
      try {
        statement.execute("START STREAM missing_stream");
      } catch (SQLException e) {
        failed = true;
        assertTrue(
            "The failure message should contain the spec text but was: " + e.getMessage(),
            e.getMessage().contains("Stream not found: missing_stream"));
      }
      assertTrue("START STREAM on a non-existent stream must fail", failed);

      assertNull(
          "A failed START must not create a new stream record",
          queryStreamRow(statement, "missing_stream"));
    }
  }

  /**
   * START-04: START STREAM without a stream_name is rejected by the grammar and leaves existing
   * streams unchanged. The spec does not define the exact syntax-error text, so only the rejection
   * itself is asserted.
   */
  @Test
  public void testStartWithoutStreamNameIsRejected() throws Exception {
    final String streamName = "syntax_check_stream";
    try (Connection connection =
            StreamEnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE test");
      statement.execute("USE test");
      statement.execute(
          "CREATE TABLE src_t (time TIMESTAMP TIME, device_id STRING TAG, value INT64 FIELD)");
      statement.execute("CREATE TABLE sink_t (time TIMESTAMP TIME, value INT64 FIELD)");
      statement.execute(
          "CREATE STREAM "
              + streamName
              + " FROM test.src_t TUMBLE(size => 1h) INTO test.sink_t(time, value)"
              + " SELECT ${START_TIME}, count(*) FROM ${ROWS}");

      AWAIT.untilAsserted(() -> assertNotNull(queryStreamRow(statement, streamName)));
      Map<String, Object> rowBefore = queryStreamRow(statement, streamName);

      boolean rejected = false;
      try {
        statement.execute("START STREAM");
      } catch (SQLException e) {
        // The spec does not define the syntax-error message; any rejection is acceptable.
        rejected = true;
      }
      assertTrue("START STREAM without stream_name must be rejected", rejected);

      Map<String, Object> rowAfter = queryStreamRow(statement, streamName);
      assertEquals(
          "A rejected START must not change the stream status",
          rowBefore.get(ColumnHeaderConstant.STREAM_STATUS_TABLE_MODEL),
          rowAfter.get(ColumnHeaderConstant.STREAM_STATUS_TABLE_MODEL));
      for (String timeColumn : TIME_COLUMNS) {
        assertEquals(
            "A rejected START must not change " + timeColumn,
            asLongOrDefault(rowBefore.get(timeColumn)),
            asLongOrDefault(rowAfter.get(timeColumn)));
      }
    }
  }

  /**
   * TUMBLE part of START-01. Windows are event-time driven: a window closes once data with a larger
   * event time arrives (watermark), so the pre-start input deliberately contains a
   * watermark-pushing row - a running task would emit output almost immediately.
   */
  private void verifyTumbleStreamManualStart(Statement statement) throws Exception {
    final String streamName = "start_tumble_stream";
    statement.execute(
        "CREATE TABLE tumble_src (time TIMESTAMP TIME, device_id STRING TAG, value INT64 FIELD)");
    statement.execute("CREATE TABLE tumble_sink (time TIMESTAMP TIME, value INT64 FIELD)");
    long tBase = System.currentTimeMillis() + 1000;
    statement.execute(
        String.format(
            "CREATE STREAM %s FROM test.tumble_src TUMBLE(%dms,%d) INTO test.tumble_sink(time,"
                + " value) SELECT ${START_TIME}, count(*) FROM ${ROWS}",
            streamName, 5000, tBase));

    AWAIT.untilAsserted(() -> assertNotNull(queryStreamRow(statement, streamName)));
    Map<String, Object> rowBefore = queryStreamRow(statement, streamName);

    // Valid input while not started: two rows in window 1 [tBase, tBase+5s) plus a row in
    // window 2 that advances the watermark past window 1's end, so a running task would close
    // window 1 and emit (tBase, 2).
    statement.execute(
        String.format(
            "INSERT INTO test.tumble_src (time, device_id, value) VALUES (%d, 'd1', 1),"
                + " (%d, 'd1', 2)",
            tBase + 1000, tBase + 2000));
    statement.execute(
        String.format(
            "INSERT INTO test.tumble_src (time, device_id, value) VALUES (%d, 'd1', 9)",
            tBase + 6000));
    TimeUnit.SECONDS.sleep(1);
    assertEquals(
        "A stream that was never started must not produce any output",
        0,
        countRows(statement, "test.tumble_sink"));
    assertEquals(
        "A newly created stream must be CREATED before START",
        "CREATED",
        rowBefore.get(ColumnHeaderConstant.STREAM_STATUS_TABLE_MODEL));

    long startMoment = System.currentTimeMillis();
    statement.execute("START STREAM " + streamName);

    AWAIT.untilAsserted(
        () ->
            assertEquals(
                "The TUMBLE stream should be RUNNING after START STREAM",
                "RUNNING",
                queryStreamRow(statement, streamName)
                    .get(ColumnHeaderConstant.STREAM_STATUS_TABLE_MODEL)));

    Map<String, Object> rowRunning = queryStreamRow(statement, streamName);
    long lastUpTime = asLong(rowRunning.get(ColumnHeaderConstant.STREAM_LAST_UP_TIME_TABLE_MODEL));
    assertTrue(
        "last_up_time should represent this transition into RUNNING",
        lastUpTime >= startMoment - 2000L && lastUpTime <= System.currentTimeMillis() + 2000L);
    assertTrue(
        "last_up_time should be updated by this START",
        lastUpTime
            > asLongOrDefault(rowBefore.get(ColumnHeaderConstant.STREAM_LAST_UP_TIME_TABLE_MODEL)));

    // New valid input after START: two fresh rows in window 3 [tBase+10s, tBase+15s) plus a
    // watermark-pushing row in window 4; window 3 must emit exactly (tBase+10000, 2).
    statement.execute(
        String.format(
            "INSERT INTO test.tumble_src (time, device_id, value) VALUES (%d, 'd1', 10),"
                + " (%d, 'd1', 20)",
            tBase + 11000, tBase + 12000));
    statement.execute(
        String.format(
            "INSERT INTO test.tumble_src (time, device_id, value) VALUES (%d, 'd1', 30)",
            tBase + 16000));
    OUTPUT_AWAIT.untilAsserted(
        () -> {
          List<Map<String, Object>> sinkRows =
              queryRows(statement, "SELECT time, value FROM test.tumble_sink");
          assertEquals("Exactly one window output is expected after START", 1, sinkRows.size());
          assertEquals(tBase + 10000, asLong(sinkRows.get(0).get("time")));
          assertEquals(2L, asLong(sinkRows.get(0).get("value")));
        });
  }

  /**
   * PERIOD part of START-01. Period windows fire on system time, so waiting past the origin proves
   * that future trigger points produce no output while the stream is not started.
   */
  private void verifyPeriodStreamManualStart(Statement statement) throws Exception {
    final String streamName = "start_period_stream";
    statement.execute(
        "CREATE TABLE electricity_meter (time TIMESTAMP TIME, meter_id STRING TAG,"
            + " total_consumption FLOAT FIELD)");
    statement.execute(
        "CREATE TABLE electricity_meter_stats (time TIMESTAMP TIME, meter_id STRING TAG,"
            + " hourly_consumption FLOAT FIELD)");
    long origin = System.currentTimeMillis() / 1000 * 1000 + 2000;
    statement.execute(
        String.format(
            "CREATE STREAM %s PERIOD(%ds,%d) INTO electricity_meter_stats(time, meter_id,"
                + " hourly_consumption) SELECT ${START_TIME}, meter_id,"
                + " last(total_consumption) - first(total_consumption) FROM electricity_meter"
                + " WHERE time >= ${START_TIME} and time <= ${END_TIME} GROUP BY meter_id",
            streamName, 1, origin));

    AWAIT.untilAsserted(() -> assertNotNull(queryStreamRow(statement, streamName)));
    Map<String, Object> rowBefore = queryStreamRow(statement, streamName);

    // Valid input while not started, then wait for several 1s trigger points to pass.
    // Both rows carry the same value so that a hypothetical recomputation of these windows
    // would contribute a delta of 0 to the later assertions.
    statement.execute(
        String.format(
            "INSERT INTO test.electricity_meter (time, meter_id, total_consumption) VALUES"
                + " (%d, 'M1', 50.5), (%d, 'M1', 50.5)",
            origin + 1000, origin + 2000));
    waitUntil(origin + 3000);
    assertEquals(
        "A stream that was never started must not produce any output",
        0,
        countRows(statement, "test.electricity_meter_stats"));
    assertEquals(
        "A newly created stream must be CREATED before START",
        "CREATED",
        rowBefore.get(ColumnHeaderConstant.STREAM_STATUS_TABLE_MODEL));

    long startMoment = System.currentTimeMillis();
    statement.execute("START STREAM " + streamName);

    AWAIT.untilAsserted(
        () ->
            assertEquals(
                "The PERIOD stream should be RUNNING after START STREAM",
                "RUNNING",
                queryStreamRow(statement, streamName)
                    .get(ColumnHeaderConstant.STREAM_STATUS_TABLE_MODEL)));

    Map<String, Object> rowRunning = queryStreamRow(statement, streamName);
    long lastUpTime = asLong(rowRunning.get(ColumnHeaderConstant.STREAM_LAST_UP_TIME_TABLE_MODEL));
    assertTrue(
        "last_up_time should represent this transition into RUNNING",
        lastUpTime >= startMoment - 2000L && lastUpTime <= System.currentTimeMillis() + 2000L);
    assertTrue(
        "last_up_time should be updated by this START",
        lastUpTime
            > asLongOrDefault(rowBefore.get(ColumnHeaderConstant.STREAM_LAST_UP_TIME_TABLE_MODEL)));

    // New valid input after START, aligned to the period grid and in the future. Same layout as
    // the reference periodic demo: per-meter last-first summed over the fired windows.
    long dataBase = origin + ((System.currentTimeMillis() - origin) / 1000 + 3) * 1000;
    // Data points are 100ms apart so every meter has multiple points within a
    // single 1s PERIOD window: last-first is then non-zero (e.g. M1 = 500.5 - 100.5).
    long t1 = dataBase;
    long t2 = dataBase + 100;
    long t3 = dataBase + 200;
    long t4 = dataBase + 1100;
    long t5 = dataBase + 1200;
    long t6 = dataBase + 1500;
    statement.execute(
        String.format(
            "INSERT INTO test.electricity_meter (time, meter_id, total_consumption) VALUES"
                + " (%d, 'M1', 100.5), (%d, 'M2', 110.5),"
                + " (%d, 'M1', 200.5), (%d, 'M2', 210.5),"
                + " (%d, 'M1', 300.7), (%d, 'M2', 310.7),"
                + " (%d, 'M1', 400.7), (%d, 'M2', 410.7),"
                + " (%d, 'M1', 500.5), (%d, 'M2', 510.5),"
                + " (%d, 'M2', 610.5)",
            t1, t1, t2, t2, t3, t3, t4, t4, t5, t5, t6));
    OUTPUT_AWAIT.untilAsserted(
        () -> {
          Map<String, Float> consumptionPerMeter = new HashMap<>();
          try (ResultSet rs =
              statement.executeQuery(
                  "select meter_id, hourly_consumption from test.electricity_meter_stats")) {
            while (rs.next()) {
              consumptionPerMeter.merge(
                  rs.getString("meter_id"), rs.getFloat("hourly_consumption"), Float::sum);
            }
          }
          assertEquals(2, consumptionPerMeter.size());
          assertEquals(300, consumptionPerMeter.get("M1"), 0.01f);
          assertEquals(400, consumptionPerMeter.get("M2"), 0.01f);
        });
  }

  private static void waitUntil(long wallClockMillis) throws InterruptedException {
    long delta = wallClockMillis - System.currentTimeMillis();
    if (delta > 0) {
      TimeUnit.MILLISECONDS.sleep(delta);
    }
  }
}
