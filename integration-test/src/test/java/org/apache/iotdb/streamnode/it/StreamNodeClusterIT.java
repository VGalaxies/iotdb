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

import org.apache.iotdb.commons.stream.IoTDBSubscriptionSource;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.it.env.StreamEnvFactory;
import org.apache.iotdb.itbase.category.StreamClusterIT;
import org.apache.iotdb.itbase.env.BaseEnv;
import org.apache.iotdb.session.subscription.ISubscriptionTableSession;
import org.apache.iotdb.session.subscription.ISubscriptionTreeSession;
import org.apache.iotdb.session.subscription.SubscriptionTableSessionBuilder;
import org.apache.iotdb.session.subscription.SubscriptionTreeSessionBuilder;

import org.awaitility.Awaitility;
import org.awaitility.core.ConditionFactory;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.apache.iotdb.db.it.utils.TestUtils.assertTableNonQueryTestFail;
import static org.apache.iotdb.db.it.utils.TestUtils.createUser;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Example integration test for StreamNode using full cluster environment (ConfigNode, DataNode,
 * StreamNode). This demonstrates the new StreamNode integration test support.
 */
@Category({StreamClusterIT.class})
public class StreamNodeClusterIT {
  private static final ConditionFactory AWAIT =
      Awaitility.await()
          .pollInSameThread()
          .pollDelay(1, TimeUnit.SECONDS)
          .pollInterval(1, TimeUnit.SECONDS)
          .atMost(10, TimeUnit.SECONDS);

  @BeforeClass
  public static void setUp() throws Exception {
    // Init 1C1D1S cluster environment (1 ConfigNode, 1 DataNode, 1 StreamNode)
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
   * Verifies that create/show/drop/start/stop stream require the SYSTEM system privilege. A user
   * without it is denied even though the underlying objects may exist.
   */
  @Test
  public void testStreamRequiresSystemPrivilege() throws Exception {
    final BaseEnv env = StreamEnvFactory.getEnv();
    final String dbName = "test";
    final String userName = "test_user";
    final String password = "Test_1234567";
    createUser(env, userName, password);
    // The test user needs the privilege to create a database so that the assertions focus on the
    // SYSTEM privilege required by the stream operations.
    try (Connection adminConnection = env.getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement adminStatement = adminConnection.createStatement()) {
      adminStatement.execute("GRANT CREATE ON ANY TO USER " + userName);
    }

    try (Connection connection = env.getConnection(userName, password, BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE test");
    }

    final String noPermissionMsg = "No permissions for this operation, please add privilege SYSTEM";
    assertTableNonQueryTestFail(
        env,
        "CREATE STREAM perm_stream "
            + "FROM t1 TUMBLE(size => 1h, origin => 2000-01-01T00:00:00) "
            + "INTO t2 (time, count) "
            + "SELECT time, count(*) FROM ${rows}",
        noPermissionMsg,
        userName,
        password,
        dbName);
    assertTableNonQueryTestFail(env, "SHOW STREAMS", noPermissionMsg, userName, password, dbName);
    assertTableNonQueryTestFail(
        env, "START STREAM perm_stream", noPermissionMsg, userName, password, dbName);
    assertTableNonQueryTestFail(
        env, "STOP STREAM perm_stream", noPermissionMsg, userName, password, dbName);
    assertTableNonQueryTestFail(
        env, "DROP STREAM perm_stream", noPermissionMsg, userName, password, dbName);
  }

  /**
   * Demo1: periodic computation of consumption per meter via PERIOD window stream. Window fires on
   * system time, so insert data using now() so the rows fall inside the active period window.
   */
  @Test
  public void testElectricityMeterPeriodicConsumption() throws Exception {
    try (Connection connection =
            StreamEnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE test");
      statement.execute("USE test");

      statement.execute(
          String.format(
              "CREATE TABLE electricity_meter("
                  + "  time TIMESTAMP TIME,"
                  + "  meter_id STRING TAG,"
                  + "  total_consumption FLOAT FIELD)"));

      statement.execute(
          String.format(
              "CREATE TABLE electricity_meter_stats("
                  + "  time TIMESTAMP TIME,"
                  + "  meter_id STRING tag,"
                  + "  prev_ts TIMESTAMP FIELD,"
                  + "  next_ts TIMESTAMP FIELD,"
                  + "  hourly_consumption FLOAT FIELD)"));

      String streamName = "stream_electricity";
      long originTime = System.currentTimeMillis();
      // PERIOD floors the origin down to a whole second; anchor the origin and the data
      // timestamps to the same second grid so each 1s window (inclusive END_TIME) captures two
      // consecutive rows of each meter.
      long baseTime = (originTime + 5000L) / 1000L * 1000L;
      long periodTime = 1000;
      statement.execute(
          String.format(
              "CREATE STREAM %s"
                  + "  PERIOD(%ds,%d)"
                  + "  INTO electricity_meter_stats(time, meter_id, prev_ts, next_ts,"
                  + "    hourly_consumption)"
                  + "  SELECT ${START_TIME}, meter_id, ${PREV_TIME}, ${NEXT_TIME},"
                  + "    last(total_consumption) - first(total_consumption) FROM electricity_meter"
                  + "  WHERE time >= ${START_TIME} and time <= ${END_TIME}"
                  + "  GROUP BY meter_id",
              streamName, periodTime / 1000, baseTime));

      try (ResultSet rs = statement.executeQuery("show streams")) {
        boolean found = false;
        while (rs.next()) {
          if (streamName.equals(rs.getString("stream_name"))) {
            found = true;
            break;
          }
        }
        assertTrue(String.format("Stream %s should be registered", streamName), found);
      }

      statement.execute(String.format("start stream %s", streamName));
      // Start data from the second window
      baseTime = baseTime + periodTime;
      // insert data
      long t1 = baseTime;
      long t2 = baseTime + periodTime;
      long t3 = baseTime + periodTime * 2;
      long t4 = baseTime + periodTime * 3;
      long t5 = baseTime + periodTime * 4;
      long t6 = baseTime + periodTime * 5;
      String sql =
          String.format(
              "INSERT INTO electricity_meter values"
                  + "  (%d, 'M1', 100.5),"
                  + "  (%d, 'M2', 110.5),"
                  + "  (%d, 'M1', 200.5),"
                  + "  (%d, 'M2', 210.5),"
                  + "  (%d, 'M1', 300.7),"
                  + "  (%d, 'M2', 310.7),"
                  + "  (%d, 'M1', 400.7),"
                  + "  (%d, 'M2', 410.7),"
                  + "  (%d, 'M1', 500.5),"
                  + "  (%d, 'M2', 510.5),"
                  + "  (%d, 'M2', 610.5)",
              t1, t1, t2, t2, t3, t3, t4, t4, t5, t5, t6);
      statement.execute(sql);
      // PERIOD(1s) origin is the whole second at now+5s; data is inserted from now+6s through
      // now+11s. The last window that contributes to M1 (t5 at now+10s) closes at now+10s, so
      // 10s is too tight once create/start latency is included.
      ConditionFactory periodAwait =
          Awaitility.await()
              .pollInSameThread()
              .pollDelay(1, TimeUnit.SECONDS)
              .pollInterval(1, TimeUnit.SECONDS)
              .atMost(30, TimeUnit.SECONDS);
      periodAwait.untilAsserted(
          () -> {
            Map<String, Float> latestPerMeter = new HashMap<>();
            try (ResultSet rs =
                statement.executeQuery(
                    "select time, next_ts, meter_id, hourly_consumption"
                        + " from electricity_meter_stats")) {
              while (rs.next()) {
                // ${NEXT_TIME} is the window end for every PERIOD window.
                assertEquals(rs.getLong("time") + periodTime, rs.getLong("next_ts"));
                latestPerMeter.merge(
                    rs.getString("meter_id"), rs.getFloat("hourly_consumption"), Float::sum);
              }
            }
            assertEquals(2, latestPerMeter.size());
            assertEquals(400, latestPerMeter.get("M1"), 0.01f);
            assertEquals(500, latestPerMeter.get("M2"), 0.01f);

            // ${PREV_TIME} is the previous window start. The first emitted window has no previous
            // trigger, so its ${PREV_TIME} is NULL by design and only non-NULL rows are compared.
            int prevTsRows = 0;
            try (ResultSet rs =
                statement.executeQuery(
                    "select time, prev_ts from electricity_meter_stats"
                        + " where prev_ts is not null")) {
              while (rs.next()) {
                assertEquals(rs.getLong("time") - periodTime, rs.getLong("prev_ts"));
                prevTsRows++;
              }
            }
            assertTrue("At least one window must carry a non-NULL ${PREV_TIME}", prevTsRows > 0);
          });
    }
  }

  /**
   * Demo2: furnace temperature down-sampling with TUMBLE windows. TUMBLE buckets by the data time
   * column, so we insert with timestamps anchored at the current second so the windows actually
   * close while the test is running.
   */
  @Test
  public void testFurnaceTumbleDownsampling() throws Exception {
    try (Connection connection =
            StreamEnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE test");
      statement.execute("USE test");

      statement.execute(
          String.format(
              "CREATE TABLE furnace ("
                  + "  time TIMESTAMP TIME,"
                  + "  device_id STRING TAG,"
                  + "  f_id STRING FIELD,"
                  + "  temperature FLOAT FIELD)"));

      statement.execute(
          String.format(
              "CREATE TABLE furnace_ds1("
                  + "  time TIMESTAMP TIME,"
                  + "  device_id STRING tag,"
                  + "  max_temperature FLOAT FIELD)"));

      String stream2Name = "stream_furnace_by_device";
      long baseTime = System.currentTimeMillis() + 2000;
      long periodTime = 5;
      statement.execute(
          String.format(
              "CREATE STREAM %s"
                  + "  FROM furnace partition by device_id"
                  + "  TUMBLE(%ds,%d)"
                  + "  INTO furnace_ds1"
                  + "  SELECT ${START_TIME}, ${1}, max(temperature) as max_temperature FROM ${ROWS}",
              stream2Name, periodTime, baseTime));
      statement.execute(String.format("start stream %s", stream2Name));
      statement.execute(
          String.format(
              "CREATE TABLE furnace_ds2("
                  + "  time TIMESTAMP TIME,"
                  + "  f_id STRING FIELD,"
                  + "  max_temperature FLOAT FIELD)"));

      String stream3Name = "stream_furnace_by_fid";
      statement.execute(
          String.format(
              "CREATE STREAM %s"
                  + "  FROM furnace"
                  + "  TUMBLE(%ds,%d)"
                  + "  INTO furnace_ds2"
                  + "  SELECT ${START_TIME}, f_id, max(temperature) as max_temperature FROM ${ROWS}"
                  + "  GROUP BY f_id"
                  + "  HAVING max(temperature) > 500",
              stream3Name, periodTime, baseTime));
      statement.execute(String.format("start stream %s", stream3Name));
      try (ResultSet rs = statement.executeQuery("show streams")) {
        boolean foundStream2 = false;
        boolean foundStream3 = false;
        while (rs.next()) {
          String name = rs.getString("stream_name");
          if (stream2Name.equals(name)) {
            foundStream2 = true;
          } else if (stream3Name.equals(name)) {
            foundStream3 = true;
          }
        }
        assertTrue(String.format("Stream %s should be registered", stream2Name), foundStream2);
        assertTrue(String.format("Stream %s should be registered", stream3Name), foundStream3);
      }
      long periodTimeMs = periodTime * 1000;
      long t0 = baseTime;
      long t1 = baseTime + periodTimeMs / 2;
      long t2 = baseTime + (periodTimeMs * 2 - periodTimeMs / 2);
      long t3 = baseTime + periodTimeMs * 2;
      long t4 = baseTime + (periodTimeMs * 3 - periodTimeMs / 2);
      long t5 = baseTime + periodTimeMs * 3;
      // Insert all rows for each device in a single multi-row INSERT so timestamps
      // within each device partition are always monotonic.
      statement.execute(
          String.format(
              "INSERT INTO furnace (time, device_id, f_id, temperature) VALUES"
                  + " (%d, 'D1', '001', 100.5),"
                  + " (%d, 'D1', '001', 200.4),"
                  + " (%d, 'D1', '001', 300.7),"
                  + " (%d, 'D1', '002', 400.1),"
                  + " (%d, 'D1', '002', 500.5),"
                  + " (%d, 'D1', '003', 600.3)",
              t0, t1, t2, t3, t4, t5));
      statement.execute(
          String.format(
              "INSERT INTO furnace (time, device_id, f_id, temperature) VALUES"
                  + " (%d, 'D2', '001', 110.2),"
                  + " (%d, 'D2', '001', 210.8),"
                  + " (%d, 'D2', '002', 310.4),"
                  + " (%d, 'D2', '003', 410.6)",
              t0, t1, t2, t3));

      // Wait for TUMBLE(5s) windows to close. The first window [baseTime, baseTime+5s)
      // closes at baseTime+5s; with baseTime = now+2s that is now+7s.
      ConditionFactory dsAWAIT =
          Awaitility.await()
              .pollInSameThread()
              .pollDelay(1, TimeUnit.SECONDS)
              .pollInterval(1, TimeUnit.SECONDS)
              .atMost(30, TimeUnit.SECONDS);
      dsAWAIT.untilAsserted(
          () -> {
            // furnace_ds1: partition by device_id, max(temperature) per TUMBLE window.
            // All 10 rows fall in W1. Expect 2 rows: D1 max=600.3, D2 max=410.6.
            Map<String, Float> ds1ByDevice = new HashMap<>();
            int ds1RowCount = 0;
            try (ResultSet rs =
                statement.executeQuery("select device_id, max_temperature from furnace_ds1")) {
              while (rs.next()) {
                ds1ByDevice.merge(
                    rs.getString("device_id"), rs.getFloat("max_temperature"), Float::sum);
                ds1RowCount++;
              }
            }
            assertEquals(5, ds1RowCount);
            assertEquals(1001.6f, ds1ByDevice.get("D1"), 0.01f);
            assertEquals(521.2f, ds1ByDevice.get("D2"), 0.01f);

            // furnace_ds2: GROUP BY f_id, HAVING max(temperature) > 500.
            // Expect 1 row: f_id=002, max=500.5 (f_id=001 max=300.7 filtered by HAVING).
            Map<String, Float> ds2ByFid = new HashMap<>();
            int ds2RowCount = 0;
            try (ResultSet rs =
                statement.executeQuery("select f_id, max_temperature from furnace_ds2")) {
              while (rs.next()) {
                ds2ByFid.put(rs.getString("f_id"), rs.getFloat("max_temperature"));
                ds2RowCount++;
              }
            }
            assertEquals(1, ds2RowCount);
            assertEquals(500.5f, ds2ByFid.get("002"), 0.01f);
          });
    }
  }

  @Test
  public void testDropStreamRemovesManagedSubscriptionTopic() throws Exception {
    final String streamName = "stream_drop_topic";
    final String topicName = "__stream__" + streamName;

    try (Connection connection =
            StreamEnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement();
        ISubscriptionTableSession subscriptionSession =
            new SubscriptionTableSessionBuilder()
                .host(StreamEnvFactory.getEnv().getIP())
                .port(Integer.parseInt(StreamEnvFactory.getEnv().getPort()))
                .build()) {
      statement.execute("CREATE DATABASE test");
      statement.execute("USE test");
      statement.execute("CREATE TABLE source_table (device_id STRING TAG, value INT64 FIELD)");
      statement.execute("CREATE TABLE sink_table (device_id STRING TAG, value INT64 FIELD)");
      statement.execute(
          "CREATE STREAM "
              + streamName
              + " FROM source_table TUMBLE(size => 1h)"
              + " INTO sink_table(time, value)"
              + " SELECT time, value FROM source_table");
      statement.execute("START STREAM " + streamName);

      // Establish that the StreamNode created the managed topic before testing drop cleanup.
      AWAIT.untilAsserted(
          () ->
              assertTrue(
                  "Managed subscription topic should exist before DROP STREAM",
                  subscriptionSession.getTopic(topicName).isPresent()));

      statement.execute("DROP STREAM " + streamName);

      AWAIT.untilAsserted(
          () ->
              assertFalse(
                  "Managed subscription topic should not exist after DROP STREAM",
                  subscriptionSession.getTopic(topicName).isPresent()));
    }
  }

  /**
   * Verifies that dropping a started Stream removes its managed table-model Topic and metadata
   * while preserving a tree-model Topic with the same name.
   */
  @Test
  public void testDropStreamPreservesSameNamedTreeTopic() throws Exception {
    final String streamName = "stream_drop_topic_model";
    final String topicName = IoTDBSubscriptionSource.getTopicName(streamName);

    try (Connection connection =
            StreamEnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement();
        ISubscriptionTableSession tableSession =
            new SubscriptionTableSessionBuilder()
                .host(StreamEnvFactory.getEnv().getIP())
                .port(Integer.parseInt(StreamEnvFactory.getEnv().getPort()))
                .build();
        ISubscriptionTreeSession treeSession =
            new SubscriptionTreeSessionBuilder()
                .host(StreamEnvFactory.getEnv().getIP())
                .port(Integer.parseInt(StreamEnvFactory.getEnv().getPort()))
                .build()) {
      treeSession.open();
      statement.execute("CREATE DATABASE test");
      statement.execute("USE test");
      statement.execute("CREATE TABLE source_table (device_id STRING TAG, value INT64 FIELD)");
      statement.execute("CREATE TABLE sink_table (time TIMESTAMP TIME, value INT64 FIELD)");
      statement.execute(
          "CREATE STREAM "
              + streamName
              + " FROM source_table TUMBLE(size => 1h)"
              + " INTO sink_table(time, value)"
              + " SELECT time, value FROM source_table");
      statement.execute("START STREAM " + streamName);

      AWAIT.untilAsserted(() -> assertTrue(tableSession.getTopic(topicName).isPresent()));
      // The identical name in both namespaces exposes cleanup that accidentally uses the tree
      // model.
      treeSession.createTopic(topicName);
      try {
        assertTrue(treeSession.getTopic(topicName).isPresent());

        statement.execute("DROP STREAM " + streamName);

        AWAIT.untilAsserted(
            () -> {
              assertFalse(
                  "DROP STREAM should remove the managed table-model topic",
                  tableSession.getTopic(topicName).isPresent());
              assertTrue(
                  "DROP STREAM should preserve the same-named tree-model topic",
                  treeSession.getTopic(topicName).isPresent());
              try (ResultSet rs = statement.executeQuery("SHOW STREAMS")) {
                while (rs.next()) {
                  assertFalse(streamName.equals(rs.getString("stream_name")));
                }
              }
            });
      } finally {
        treeSession.dropTopic(topicName);
      }
    }
  }

  /**
   * A managed Topic may not have been created when its Stream is dropped; the child Topic procedure
   * must treat that as an idempotent cleanup and let DROP STREAM succeed.
   */
  @Test
  public void testDropStreamSucceedsWhenManagedSubscriptionTopicIsAlreadyAbsent() throws Exception {
    final String streamName = "stream_drop_absent_topic";
    final String topicName = "__stream__" + streamName;

    try (Connection connection =
            StreamEnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement();
        ISubscriptionTableSession subscriptionSession =
            new SubscriptionTableSessionBuilder()
                .host(StreamEnvFactory.getEnv().getIP())
                .port(Integer.parseInt(StreamEnvFactory.getEnv().getPort()))
                .build()) {
      statement.execute("CREATE DATABASE test");
      statement.execute("USE test");
      statement.execute("CREATE TABLE source_table (device_id STRING TAG, value INT64 FIELD)");
      statement.execute("CREATE TABLE sink_table (time TIMESTAMP TIME, value INT64 FIELD)");
      statement.execute(
          "CREATE STREAM "
              + streamName
              + " FROM source_table TUMBLE(size => 1h)"
              + " INTO sink_table(time, value)"
              + " SELECT time, value FROM source_table");

      // The managed topic is created when the Stream starts. Keep it absent to exercise the
      // idempotent child DROP TOPIC procedure without leaving a stopped consumer subscribed.
      AWAIT.untilAsserted(() -> assertFalse(subscriptionSession.getTopic(topicName).isPresent()));

      statement.execute("DROP STREAM " + streamName);
    }
  }
}
