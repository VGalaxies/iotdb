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
import org.awaitility.core.ConditionFactory;
import org.awaitility.core.ConditionTimeoutException;
import org.awaitility.core.ThrowingRunnable;
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
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Guards the row identity of a TUMBLE window. A table row's primary key is (all TAG columns, time),
 * so rows of different devices sharing a timestamp are distinct rows: the window store must keep
 * them apart (keyed upsert), and a lazily materialized close event must deliver an equal-timestamp
 * run in full even when the materialization block boundary (1024 rows) falls inside the run.
 * Covered both without partitioning and with PARTITION BY a TAG column - whose streams must
 * additionally survive the source table gaining a new TAG column mid-stream, since the row identity
 * is derived per row from the tablet's TAG columns at push time, not frozen at stream creation.
 */
@Category({StreamClusterIT.class})
public class IoTDBStreamTumbleRowIdentityIT {

  private static final ConditionFactory AWAIT =
      Awaitility.await()
          .pollInSameThread()
          .pollDelay(200, TimeUnit.MILLISECONDS)
          .pollInterval(500, TimeUnit.MILLISECONDS)
          .atMost(30, TimeUnit.SECONDS);

  /** One TUMBLE window's materialization block carries at most this many rows. */
  private static final int MAX_WINDOW_BLOCK_ROWS = 1024;

  @BeforeClass
  public static void setUp() throws Exception {
    // Init 1C1D1S cluster environment (1 ConfigNode, 1 DataNode, 1 StreamNode)
    StreamEnvFactory.getEnv(1)
        .getConfig()
        .getCommonConfig()
        .setSubscriptionEnabled(true)
        .setDataRegionConsensusProtocolClass(ConsensusFactory.IOT_CONSENSUS)
        .setDataRegionGroupExtensionPolicy("CUSTOM")
        .setDefaultDataRegionGroupNumPerDatabase(1);
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
          streamNames.add(rs.getString("stream_name"));
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
   * Rows of different devices sharing a timestamp in one TUMBLE window must stay distinct. The
   * stream is NOT partitioned by the TAG column, so all devices land in the same window engine; the
   * window store must key rows by (device_id, time) - otherwise the two rows splice into one and
   * the per-device GROUP BY sees a single merged row (count/sum/max lose the other device's data).
   */
  @Test
  public void testTumbleRowsOfDifferentDevicesAtSameTimestampStayDistinct() throws Exception {
    try (Connection connection =
            StreamEnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE test");
      statement.execute("USE test");

      statement.execute(
          "CREATE TABLE src_tag("
              + "  time TIMESTAMP TIME,"
              + "  device_id STRING TAG,"
              + "  value INT64 FIELD)");
      statement.execute(
          "CREATE TABLE sink_tag("
              + "  time TIMESTAMP TIME,"
              + "  device_id STRING TAG,"
              + "  max_value INT64 FIELD)");

      String streamName = "stream_tag_key";
      long windowSizeMs = 5000;
      // Anchor the window grid at now+2s so the window closes while the test runs.
      long baseTime = System.currentTimeMillis() + 2000;
      statement.execute(
          String.format(
              "CREATE STREAM %s"
                  + "  FROM src_tag"
                  + "  TUMBLE(%dms,%d)"
                  + "  INTO sink_tag(time, device_id, max_value)"
                  + "  SELECT ${START_TIME}, device_id, max(value) as max_value FROM ${ROWS}"
                  + "  GROUP BY device_id"
                  + "  WITH (subscription_poll_thread_count = 1)",
              streamName, windowSizeMs, baseTime));
      statement.execute(String.format("start stream %s", streamName));

      // Two devices, one shared timestamp: the merged-key bug splices them into one row.
      statement.execute(
          String.format(
              "INSERT INTO src_tag (time, device_id, value) VALUES (%d, 'd1', 10), (%d, 'd2', 20)",
              baseTime, baseTime));
      // A row in a later window closes [baseTime, baseTime+windowSize) and flushes the result.
      statement.execute(
          String.format(
              "INSERT INTO src_tag (time, device_id, value) VALUES (%d, 'trigger', 0)",
              baseTime + 2 * windowSizeMs));

      final AtomicInteger triggerRound = new AtomicInteger();
      awaitWithDiagnostics(
          () -> {
            Map<String, Long> maxPerDevice = new HashMap<>();
            try (ResultSet rs =
                statement.executeQuery(
                    String.format(
                        "select device_id, max_value from sink_tag where time = %d", baseTime))) {
              while (rs.next()) {
                maxPerDevice.put(rs.getString("device_id"), rs.getLong("max_value"));
              }
            }
            if (maxPerDevice.size() < 2) {
              insertWatermarkTrigger(statement, "src_tag", baseTime, windowSizeMs, triggerRound);
            }
            assertEquals(
                "rows of different devices at the same timestamp must stay distinct",
                2,
                maxPerDevice.size());
            assertEquals(Long.valueOf(10), maxPerDevice.get("d1"));
            assertEquals(Long.valueOf(20), maxPerDevice.get("d2"));
          });
    }
  }

  /**
   * A close event materializes the window rows lazily in blocks of at most 1024 rows. When more
   * than 1024 rows share one timestamp (one row per device), the block boundary falls inside the
   * equal-timestamp run: the resume cursor must pick up the un-emitted tail of the run, not skip to
   * the first row after the timestamp. Skipping the tail silently dropped the run's remaining
   * devices from the per-device GROUP BY.
   */
  @Test
  public void testTumbleSameTimestampRunSplitAcrossBlocksIsFullyDelivered() throws Exception {
    int deviceCount = MAX_WINDOW_BLOCK_ROWS + 76;
    try (Connection connection =
            StreamEnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE test");
      statement.execute("USE test");

      statement.execute(
          "CREATE TABLE src_block("
              + "  time TIMESTAMP TIME,"
              + "  device_id STRING TAG,"
              + "  value INT64 FIELD)");
      statement.execute(
          "CREATE TABLE sink_block("
              + "  time TIMESTAMP TIME,"
              + "  device_id STRING TAG,"
              + "  max_value INT64 FIELD)");

      String streamName = "stream_block_split";
      long windowSizeMs = 5000;
      long baseTime = System.currentTimeMillis() + 2000;
      statement.execute(
          String.format(
              "CREATE STREAM %s"
                  + "  FROM src_block"
                  + "  TUMBLE(%dms,%d)"
                  + "  INTO sink_block(time, device_id, max_value)"
                  + "  SELECT ${START_TIME}, device_id, max(value) as max_value FROM ${ROWS}"
                  + "  GROUP BY device_id"
                  + "  WITH (subscription_poll_thread_count = 1)",
              streamName, windowSizeMs, baseTime));
      statement.execute(String.format("start stream %s", streamName));

      // One chunked multi-row INSERT per 100 devices keeps each statement small; the devices all
      // share one timestamp, so the store holds deviceCount distinct keyed rows. The leading
      // separator is decided per chunk, not per global index, or the 2nd..Nth statement starts
      // with "VALUES ," (a parse error).
      StringBuilder devices = new StringBuilder();
      for (int i = 0; i < deviceCount; i++) {
        devices.append(devices.length() == 0 ? "" : ",");
        devices.append(String.format("(%d, 'd%04d', %d)", baseTime, i, MAX_WINDOW_BLOCK_ROWS));
        if ((i + 1) % 100 == 0 || i == deviceCount - 1) {
          statement.execute(
              String.format("INSERT INTO src_block (time, device_id, value) VALUES %s", devices));
          devices.setLength(0);
        }
      }
      // A row in a later window closes [baseTime, baseTime+windowSize) and flushes the result.
      statement.execute(
          String.format(
              "INSERT INTO src_block (time, device_id, value) VALUES (%d, 'trigger', 0)",
              baseTime + 2 * windowSizeMs));

      final AtomicInteger triggerRound = new AtomicInteger();
      awaitWithDiagnostics(
          () -> {
            int deliveredDevices = 0;
            try (ResultSet rs =
                statement.executeQuery(
                    String.format("select count(*) from sink_block where time = %d", baseTime))) {
              assertTrue(rs.next());
              deliveredDevices = rs.getInt(1);
            }
            if (deliveredDevices < deviceCount) {
              insertWatermarkTrigger(statement, "src_block", baseTime, windowSizeMs, triggerRound);
            }
            assertEquals(
                "an equal-timestamp run split across materialization blocks must be fully"
                    + " delivered (expected one sink row per device)",
                deviceCount,
                deliveredDevices);
          });
    }
  }

  /**
   * A stream PARTITION BY a TAG column must keep computing correctly when the source table gains a
   * NEW TAG column mid-stream. The row identity is derived per row from the tablet's TAG columns at
   * push time (not frozen at stream creation), so a row written before the addition (identity
   * {@code [src, d1]}) and rows written after with different new-tag values (identities {@code
   * [src, d1, r1]}, {@code [src, d1, r2]}) are distinct rows of the SAME partition: a
   * same-timestamp same-device group must aggregate all of them, not a merged subset, while rows
   * sharing the full identity (same device, same new tag, same time) still merge into exactly one
   * row. The partition key (device_id) must stay resolvable per row, and the subscription column
   * filter must keep delivering the new TAG column (TAG columns are exempt from column-filter
   * pruning).
   */
  @Test
  public void testTumblePartitionedStreamSurvivesSourceTagColumnAddition() throws Exception {
    try (Connection connection =
            StreamEnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE test");
      statement.execute("USE test");

      statement.execute(
          "CREATE TABLE src_alter("
              + "  time TIMESTAMP TIME,"
              + "  device_id STRING TAG,"
              + "  value INT64 FIELD)");
      statement.execute(
          "CREATE TABLE sink_alter("
              + "  time TIMESTAMP TIME,"
              + "  device_id STRING TAG,"
              // sum(INT64) is typed DOUBLE by the analyzer, so the sink column matches it.
              + "  sum_value DOUBLE FIELD)");

      String streamName = "stream_tag_addition";
      long windowSizeMs = 5000;
      // Anchor the window grid at now+2s so both windows under test close while the test runs.
      long baseTime = System.currentTimeMillis() + 2000;
      // ${1} is the partition column's per-partition value; the un-grouped aggregate then computes
      // over one partition's window rows only, which is the per-device grouping (the form every
      // existing PARTITION BY IT uses - a bare partition-column reference plus GROUP BY is not a
      // supported combination in a partitioned stream's compute plan).
      statement.execute(
          String.format(
              "CREATE STREAM %s"
                  + "  FROM src_alter PARTITION BY device_id"
                  + "  TUMBLE(%dms,%d)"
                  + "  INTO sink_alter(time, device_id, sum_value)"
                  + "  SELECT ${START_TIME}, ${1}, sum(value) FROM ${ROWS}"
                  + "  WITH (subscription_poll_thread_count = 1)",
              streamName, windowSizeMs, baseTime));
      statement.execute(String.format("start stream %s", streamName));

      // Window 1: rows written while the table's only TAG column is device_id.
      statement.execute(
          String.format(
              "INSERT INTO src_alter (time, device_id, value) VALUES (%d, 'd1', 1), (%d, 'd2', 2)",
              baseTime, baseTime));

      // The source table gains a TAG column mid-stream.
      statement.execute("ALTER TABLE src_alter ADD COLUMN region STRING TAG");

      // Same timestamps and devices, distinct new-tag values: each device now holds one
      // pre-addition row plus one row per new-tag value - three distinct rows that all land in
      // device d1's partition and d2's partition respectively.
      statement.execute(
          String.format(
              "INSERT INTO src_alter (time, device_id, region, value) VALUES"
                  + " (%d, 'd1', 'r1', 10), (%d, 'd1', 'r2', 20), (%d, 'd2', 'r3', 30)",
              baseTime, baseTime, baseTime));

      // Window 2: rows written entirely after the addition. The two 'r1' rows share the full row
      // identity (same device, same new tag, same time) and merge into exactly one row; the 'r2'
      // row stays distinct.
      long secondWindowTime = baseTime + windowSizeMs;
      statement.execute(
          String.format(
              "INSERT INTO src_alter (time, device_id, region, value) VALUES"
                  + " (%d, 'd1', 'r1', 100), (%d, 'd1', 'r1', 100), (%d, 'd1', 'r2', 200),"
                  + " (%d, 'd2', 'r3', 70)",
              secondWindowTime, secondWindowTime, secondWindowTime, secondWindowTime));

      // Rows in a later window advance the watermarks past both windows under test. The watermark
      // is per-partition, so each partition under test needs its own probe row - a foreign device
      // ('trigger') would advance only that foreign partition and never close d1's/d2's windows.
      long closeProbeTime = baseTime + 2 * windowSizeMs;
      statement.execute(
          String.format(
              "INSERT INTO src_alter (time, device_id, region, value) VALUES"
                  + " (%d, 'd1', 'rx', 0), (%d, 'd2', 'ry', 0)",
              closeProbeTime, closeProbeTime));

      final AtomicInteger triggerRound = new AtomicInteger();
      awaitWithDiagnostics(
          () -> {
            Map<String, Double> firstWindowSums = sumsAt(statement, "sink_alter", baseTime);
            Map<String, Double> secondWindowSums =
                sumsAt(statement, "sink_alter", secondWindowTime);
            if (firstWindowSums.size() < 2 || secondWindowSums.size() < 2) {
              insertTagAdditionWatermarkProbe(
                  statement, "src_alter", baseTime, windowSizeMs, triggerRound);
            }
            assertEquals(
                "the pre-addition row and the per-new-tag rows must all aggregate (no merged"
                    + " subset) after the source table gains a TAG column",
                2,
                firstWindowSums.size());
            assertEquals(1.0 + 10.0 + 20.0, firstWindowSums.get("d1"), 0.0);
            assertEquals(2.0 + 30.0, firstWindowSums.get("d2"), 0.0);
            assertEquals(2, secondWindowSums.size());
            assertEquals(100.0 + 200.0, secondWindowSums.get("d1"), 0.0);
            assertEquals(70.0, secondWindowSums.get("d2"), 0.0);
          });
    }
  }

  /** The sink rows of one window: device id to its aggregated value. */
  private static Map<String, Double> sumsAt(
      final Statement statement, final String sinkTable, final long time) throws SQLException {
    Map<String, Double> sums = new HashMap<>();
    try (ResultSet rs =
        statement.executeQuery(
            String.format(
                "select device_id, sum_value from %s where time = %d", sinkTable, time))) {
      while (rs.next()) {
        sums.put(rs.getString("device_id"), rs.getDouble("sum_value"));
      }
    }
    return sums;
  }

  /**
   * Watermark probes for the source table whose TAG columns include region: one zero-value row per
   * partition under test (the watermark is per-partition, so each partition needs its own probe to
   * close its windows), one window later per round, with the new TAG column always specified (the
   * row must stay a valid primary key after the ALTER). Same converging re-arm semantics as {@link
   * #insertWatermarkTrigger(Statement, String, long, long, AtomicInteger)}.
   */
  private static void insertTagAdditionWatermarkProbe(
      final Statement statement,
      final String srcTable,
      final long baseTime,
      final long windowSizeMs,
      final AtomicInteger round)
      throws SQLException {
    final long probeTime = baseTime + (2 + round.getAndIncrement()) * windowSizeMs;
    statement.execute(
        String.format(
            "INSERT INTO %s (time, device_id, region, value) VALUES"
                + " (%d, 'd1', 'rx', 0), (%d, 'd2', 'ry', 0)",
            srcTable, probeTime, probeTime));
  }

  /**
   * Inserts a row in a window later than the one under test, advancing the watermark one window
   * further per round. With a single poll thread the subscription may deliver the region holding
   * the trigger row before other regions' earlier-committed rows: the window closes and destroys
   * while the late rows are still in flight, and they land in a re-created window that only another
   * watermark advance can close (with maxDelayWindowCloseMs left at 0 there is no system-time
   * fallback). Re-arming the trigger on every failed assertion round makes the assertion converge
   * on that delivery order instead of flaking on it.
   */
  private static void insertWatermarkTrigger(
      final Statement statement,
      final String srcTable,
      final long baseTime,
      final long windowSizeMs,
      final AtomicInteger round)
      throws SQLException {
    statement.execute(
        String.format(
            "INSERT INTO %s (time, device_id, value) VALUES (%d, 'trigger', 0)",
            srcTable, baseTime + (2 + round.getAndIncrement()) * windowSizeMs));
  }

  /**
   * Awaits the assertion; on timeout, dumps every node's JVM thread snapshot (JMX) so the next
   * failing run pinpoints a stuck thread instead of leaving only an assertion message. The dumps
   * land next to the node logs in cluster-logs/&lt;IT&gt;/&lt;run&gt;/ as
   * awaitTimeout_&lt;node&gt;-threads.dump.
   */
  private static void awaitWithDiagnostics(final ThrowingRunnable assertion) {
    try {
      AWAIT.untilAsserted(assertion);
    } catch (final ConditionTimeoutException e) {
      StreamEnvFactory.getEnv(1)
          .getNodeWrapperList()
          .forEach(node -> node.dumpJVMSnapshot("awaitTimeout"));
      throw e;
    }
  }
}
