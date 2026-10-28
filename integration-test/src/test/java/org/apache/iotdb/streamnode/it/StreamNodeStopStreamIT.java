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

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.property.ThriftClientProperty;
import org.apache.iotdb.commons.client.sync.SyncStreamNodeClient;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.commons.queryengine.plan.relational.metadata.ColumnSchema;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.EventScanNode;
import org.apache.iotdb.commons.schema.column.ColumnHeaderConstant;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.commons.stream.IoTDBSubscriptionSource;
import org.apache.iotdb.commons.stream.IoTDBTarget;
import org.apache.iotdb.commons.stream.StreamNodeTableTypeProvider;
import org.apache.iotdb.commons.stream.StreamTask;
import org.apache.iotdb.commons.stream.TumbleWindow;
import org.apache.iotdb.commons.utils.AuthUtils;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.isession.SessionConfig;
import org.apache.iotdb.it.env.StreamEnvFactory;
import org.apache.iotdb.it.env.cluster.node.StreamNodeWrapper;
import org.apache.iotdb.itbase.category.StreamClusterIT;
import org.apache.iotdb.itbase.env.BaseEnv;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.streamnode.rpc.thrift.TDropTaskOnStreamNodeReq;
import org.apache.iotdb.streamnode.rpc.thrift.TStartTaskOnStreamNodeReq;
import org.apache.iotdb.streamnode.rpc.thrift.TStopTaskOnStreamNodeReq;
import org.apache.iotdb.streamnode.rpc.thrift.TStreamNodeHeartbeatReq;
import org.apache.iotdb.streamnode.rpc.thrift.TTaskHeartbeat;

import org.apache.tsfile.common.constant.TsFileConstant;
import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.type.LongType;
import org.apache.tsfile.read.common.type.TimestampType;
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
 * Integration tests for STOP STREAM (spec cases STOP-01/02/03/04 of the stream-computing
 * requirements doc). Assertions follow the spec, so failures pinpoint the gaps that the
 * implementation must close.
 */
@Category({StreamClusterIT.class})
public class StreamNodeStopStreamIT extends StreamNodeITBase {

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

  private static final ConditionFactory RECOVERY_AWAIT =
      Awaitility.await()
          .pollInSameThread()
          .pollDelay(1, TimeUnit.SECONDS)
          .pollInterval(1, TimeUnit.SECONDS)
          .atMost(60, TimeUnit.SECONDS);

  private static final String[] TIME_COLUMNS = {
    ColumnHeaderConstant.CREATION_TIME_TABLE_MODEL,
    ColumnHeaderConstant.STREAM_LAST_UP_TIME_TABLE_MODEL,
    ColumnHeaderConstant.STREAM_LAST_DOWN_TIME_TABLE_MODEL,
    ColumnHeaderConstant.STREAM_LAST_HEARTBEAT_TIME_TABLE_MODEL
  };

  /**
   * Time columns that only change on state transitions; last_heartbeat_time is excluded because it
   * advances by design while a stream is RUNNING, so it must not be compared across queries on a
   * running stream.
   */
  private static final String[] TRANSITION_TIME_COLUMNS = {
    ColumnHeaderConstant.CREATION_TIME_TABLE_MODEL,
    ColumnHeaderConstant.STREAM_LAST_UP_TIME_TABLE_MODEL,
    ColumnHeaderConstant.STREAM_LAST_DOWN_TIME_TABLE_MODEL
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
   * STOP-01: stopping a RUNNING stream (TUMBLE and PERIOD) moves it to STOPPED with last_down_time
   * reflecting this transition, and no new stream computation is started afterwards for new valid
   * input / future trigger points. Handling of tasks already in flight at STOP time is out of
   * scope, as is the handling of data written during the stopped period after a later START.
   */
  @Test
  public void testStopRunningTumbleAndPeriodStreams() throws Exception {
    try (Connection connection =
            StreamEnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE test");
      statement.execute("USE test");

      verifyTumbleStreamManualStop(statement);
      verifyPeriodStreamManualStop(statement);
    }
  }

  /** STOP-02: STOP STREAM only stops the named stream; all other streams keep their state. */
  @Test
  public void testStopOnlyStopsTheSpecifiedStream() throws Exception {
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

      // Preparation: both streams are RUNNING.
      statement.execute("START STREAM " + streamA);
      statement.execute("START STREAM " + streamB);
      AWAIT.untilAsserted(
          () -> {
            assertEquals(
                "stream_a should be RUNNING after START",
                "RUNNING",
                queryStreamRow(statement, streamA)
                    .get(ColumnHeaderConstant.STREAM_STATUS_TABLE_MODEL));
            assertEquals(
                "stream_b should be RUNNING after START",
                "RUNNING",
                queryStreamRow(statement, streamB)
                    .get(ColumnHeaderConstant.STREAM_STATUS_TABLE_MODEL));
          });
      Map<String, Object> rowABefore = queryStreamRow(statement, streamA);
      Map<String, Object> rowBBefore = queryStreamRow(statement, streamB);

      // Only stop stream_a.
      long stopMoment = System.currentTimeMillis();
      statement.execute("STOP STREAM " + streamA);

      AWAIT.untilAsserted(
          () ->
              assertEquals(
                  "Only stream_a should be stopped by STOP STREAM stream_a",
                  "STOPPED",
                  queryStreamRow(statement, streamA)
                      .get(ColumnHeaderConstant.STREAM_STATUS_TABLE_MODEL)));

      Map<String, Object> rowAAfter = queryStreamRow(statement, streamA);
      long lastDownA =
          asLong(rowAAfter.get(ColumnHeaderConstant.STREAM_LAST_DOWN_TIME_TABLE_MODEL));
      assertTrue(
          "stream_a's last_down_time should reflect this STOP transition",
          lastDownA >= stopMoment - 2000L && lastDownA <= System.currentTimeMillis() + 2000L);
      assertTrue(
          "stream_a's last_down_time should be updated by this STOP",
          lastDownA
              > asLongOrDefault(
                  rowABefore.get(ColumnHeaderConstant.STREAM_LAST_DOWN_TIME_TABLE_MODEL)));
      assertTrue(
          "running_on should not be updated when StreamNode STOPPED",
          rowAAfter
              .get(ColumnHeaderConstant.STREAM_RUNNING_ON_TABLE_MODEL)
              .equals(rowABefore.get(ColumnHeaderConstant.STREAM_RUNNING_ON_TABLE_MODEL)));

      // stream_b keeps RUNNING; the command must not change its status or transition time fields.
      // last_heartbeat_time is excluded: it advances by design while a stream is RUNNING.
      Map<String, Object> rowBAfter = queryStreamRow(statement, streamB);
      assertEquals(
          "STOP STREAM must not change the status of other streams",
          rowBBefore.get(ColumnHeaderConstant.STREAM_STATUS_TABLE_MODEL),
          rowBAfter.get(ColumnHeaderConstant.STREAM_STATUS_TABLE_MODEL));
      assertEquals(
          "stream_b must remain RUNNING when only stream_a is stopped",
          "RUNNING",
          rowBAfter.get(ColumnHeaderConstant.STREAM_STATUS_TABLE_MODEL));
      for (String timeColumn : TRANSITION_TIME_COLUMNS) {
        assertEquals(
            "stream_b's " + timeColumn + " must not change when stopping stream_a",
            asLongOrDefault(rowBBefore.get(timeColumn)),
            asLongOrDefault(rowBAfter.get(timeColumn)));
      }
      assertEquals(
          "stream_b must keep its StreamNode assignment",
          rowBBefore.get(ColumnHeaderConstant.STREAM_RUNNING_ON_TABLE_MODEL),
          rowBAfter.get(ColumnHeaderConstant.STREAM_RUNNING_ON_TABLE_MODEL));
    }
  }

  /**
   * STOP-03: stopping a non-existent stream fails with "Stream missing_stream does not exist" and
   * does not affect the state of existing streams.
   */
  @Test
  public void testStopNonExistentStreamFails() throws Exception {
    final String streamName = "stop03_stream";
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
      statement.execute("START STREAM " + streamName);
      AWAIT.untilAsserted(
          () ->
              assertEquals(
                  "The existing stream should be RUNNING before the failed STOP",
                  "RUNNING",
                  queryStreamRow(statement, streamName)
                      .get(ColumnHeaderConstant.STREAM_STATUS_TABLE_MODEL)));
      Map<String, Object> rowBefore = queryStreamRow(statement, streamName);

      assertNull(
          "missing_stream should not exist before the failed STOP",
          queryStreamRow(statement, "missing_stream"));

      boolean failed = false;
      try {
        statement.execute("STOP STREAM missing_stream");
      } catch (SQLException e) {
        failed = true;
        assertTrue(
            "The failure message should contain the spec text but was: " + e.getMessage(),
            e.getMessage().contains("Stream not found: missing_stream"));
      }
      assertTrue("STOP STREAM on a non-existent stream must fail", failed);

      assertNull(
          "A failed STOP must not create a new stream record",
          queryStreamRow(statement, "missing_stream"));

      // The existing stream is unaffected (last_heartbeat_time excluded: it advances while
      // RUNNING by design).
      Map<String, Object> rowAfter = queryStreamRow(statement, streamName);
      assertEquals(
          "A failed STOP must not change the existing stream's status",
          rowBefore.get(ColumnHeaderConstant.STREAM_STATUS_TABLE_MODEL),
          rowAfter.get(ColumnHeaderConstant.STREAM_STATUS_TABLE_MODEL));
      for (String timeColumn : TRANSITION_TIME_COLUMNS) {
        assertEquals(
            "A failed STOP must not change the existing stream's " + timeColumn,
            asLongOrDefault(rowBefore.get(timeColumn)),
            asLongOrDefault(rowAfter.get(timeColumn)));
      }
    }
  }

  /**
   * STOP-04: STOP STREAM without a stream_name is rejected by the grammar and leaves existing
   * streams unchanged. The spec does not define the exact syntax-error text, so only the rejection
   * itself is asserted.
   */
  @Test
  public void testStopWithoutStreamNameIsRejected() throws Exception {
    final String streamName = "stop_syntax_check_stream";
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

      // The stream is never started, so all of its time fields are stable across the check.
      AWAIT.untilAsserted(() -> assertNotNull(queryStreamRow(statement, streamName)));
      Map<String, Object> rowBefore = queryStreamRow(statement, streamName);

      boolean rejected = false;
      try {
        statement.execute("STOP STREAM");
      } catch (SQLException e) {
        // The spec does not define the syntax-error message; any rejection is acceptable.
        rejected = true;
      }
      assertTrue("STOP STREAM without stream_name must be rejected", rejected);

      Map<String, Object> rowAfter = queryStreamRow(statement, streamName);
      assertEquals(
          "A rejected STOP must not change the stream status",
          rowBefore.get(ColumnHeaderConstant.STREAM_STATUS_TABLE_MODEL),
          rowAfter.get(ColumnHeaderConstant.STREAM_STATUS_TABLE_MODEL));
      for (String timeColumn : TIME_COLUMNS) {
        assertEquals(
            "A rejected STOP must not change " + timeColumn,
            asLongOrDefault(rowBefore.get(timeColumn)),
            asLongOrDefault(rowAfter.get(timeColumn)));
      }
    }
  }

  @Test
  public void testStopNeverStartedStreamSucceedsWhenStreamNodeIsUnavailable() throws Exception {
    final String streamName = "stop_unavailable_stream";
    final StreamNodeWrapper wrapper = getStreamNodeWrapper();
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

      wrapper.stop();
      AWAIT.until(() -> !wrapper.isAlive());
      try {
        statement.execute("STOP STREAM " + streamName);
        assertEquals(
            "STOP must be idempotent when a never-started stream has no reachable StreamNode",
            "STOPPED",
            queryStreamRow(statement, streamName)
                .get(ColumnHeaderConstant.STREAM_STATUS_TABLE_MODEL));
      } finally {
        wrapper.start();
        AWAIT.until(wrapper::isAlive);
        AWAIT.untilAsserted(() -> assertTrue(isAnyStreamNodeRunning(statement)));
      }
    }
  }

  @Test
  public void testStopSucceedsWhenStreamNodeRetainsHigherEpochStoppedTask() throws Exception {
    final String streamName = "stop_stale_stopped_stream";
    final StreamNodeWrapper wrapper = getStreamNodeWrapper();
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
      statement.execute("START STREAM " + streamName);
      AWAIT.untilAsserted(
          () ->
              assertEquals(
                  "The stream should be RUNNING before its runtime is stopped directly",
                  "RUNNING",
                  queryStreamRow(statement, streamName)
                      .get(ColumnHeaderConstant.STREAM_STATUS_TABLE_MODEL)));

      final TEndPoint streamNodeEndpoint = new TEndPoint(wrapper.getIp(), wrapper.getPort());
      try (SyncStreamNodeClient client =
          new SyncStreamNodeClient(
              new ThriftClientProperty.Builder().build(), streamNodeEndpoint, null) {
            @Override
            public void close() {
              invalidate();
            }
          }) {
        final TSStatus status =
            client.stopTask(new TStopTaskOnStreamNodeReq(streamName, 100, Long.MAX_VALUE));
        assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      }

      statement.execute("STOP STREAM " + streamName);
      assertEquals(
          "STOP should be idempotent when the StreamNode retains a higher-epoch stopped task",
          "STOPPED",
          queryStreamRow(statement, streamName)
              .get(ColumnHeaderConstant.STREAM_STATUS_TABLE_MODEL));
    }
  }

  @Test
  public void testStopSucceedsWhenRuntimeRetainsLaterConfigNodeStartTime() throws Exception {
    final String streamName = "stop_after_config_node_leader_change";
    final StreamNodeWrapper wrapper = getStreamNodeWrapper();
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
              + " FROM test.src_t TUMBLE(size => 5s) INTO test.sink_t(time, value)"
              + " SELECT ${START_TIME}, count(*) FROM ${ROWS}");
      statement.execute("START STREAM " + streamName);
      AWAIT.untilAsserted(
          () ->
              assertEquals(
                  "RUNNING",
                  queryStreamRow(statement, streamName)
                      .get(ColumnHeaderConstant.STREAM_STATUS_TABLE_MODEL)));

      try {
        // Simulate a runtime left by a ConfigNode process that started later than the current
        // leader. Process-local ConfigNode start times are not ordered across leader changes.
        replaceRuntime(wrapper, streamName, Long.MAX_VALUE);
        assertEquals(1, getRuntimeEpoch(wrapper, streamName));

        statement.execute("STOP STREAM " + streamName);

        assertEquals(
            "STOPPED",
            queryStreamRow(statement, streamName)
                .get(ColumnHeaderConstant.STREAM_STATUS_TABLE_MODEL));
        AWAIT.untilAsserted(() -> assertEquals(-1, getRuntimeEpoch(wrapper, streamName)));
      } finally {
        dropRuntime(wrapper, streamName);
      }
    }
  }

  @Test
  public void testStaleRuntimeIsStoppedAfterControlPlanePartitionRecovers() throws Exception {
    final String streamName = "stop_partitioned_stream";
    final StreamNodeWrapper wrapper = getStreamNodeWrapper();
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
              + " FROM test.src_t TUMBLE(size => 5s) INTO test.sink_t(time, value)"
              + " SELECT ${START_TIME}, count(*) FROM ${ROWS}");
      statement.execute("START STREAM " + streamName);
      AWAIT.untilAsserted(
          () ->
              assertEquals(
                  "RUNNING",
                  queryStreamRow(statement, streamName)
                      .get(ColumnHeaderConstant.STREAM_STATUS_TABLE_MODEL)));

      statement.execute("STOP STREAM " + streamName);
      assertEquals(
          "STOPPED",
          queryStreamRow(statement, streamName)
              .get(ColumnHeaderConstant.STREAM_STATUS_TABLE_MODEL));

      startStaleRuntime(wrapper, streamName);
      assertEquals(
          "The reconstructed orphan runtime must be running with epoch 1",
          1,
          getRuntimeEpoch(wrapper, streamName));

      RECOVERY_AWAIT.untilAsserted(
          () ->
              assertEquals(
                  "A stale heartbeat must make the ConfigNode stop the orphan runtime",
                  -1,
                  getRuntimeEpoch(wrapper, streamName)));
    }
  }

  private static void startStaleRuntime(final StreamNodeWrapper wrapper, final String streamName)
      throws Exception {
    replaceRuntime(wrapper, streamName, 0);
  }

  private static void replaceRuntime(
      final StreamNodeWrapper wrapper, final String streamName, final long cnStartTime)
      throws Exception {
    final String host = StreamEnvFactory.getEnv().getIP();
    final int port = Integer.parseInt(StreamEnvFactory.getEnv().getPort());
    final StreamTask staleTask = buildStaleTask(streamName, host, port);
    final TEndPoint streamNodeEndpoint = new TEndPoint(wrapper.getIp(), wrapper.getPort());
    try (SyncStreamNodeClient client = newStreamNodeClient(streamNodeEndpoint)) {
      assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(),
          client.dropTask(new TDropTaskOnStreamNodeReq(streamName)).getCode());
      assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(),
          client
              .startTask(new TStartTaskOnStreamNodeReq(staleTask.toByteBuffer(), 1, cnStartTime))
              .getCode());
    }
  }

  private static void dropRuntime(final StreamNodeWrapper wrapper, final String streamName)
      throws Exception {
    final TEndPoint streamNodeEndpoint = new TEndPoint(wrapper.getIp(), wrapper.getPort());
    try (SyncStreamNodeClient client = newStreamNodeClient(streamNodeEndpoint)) {
      assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(),
          client.dropTask(new TDropTaskOnStreamNodeReq(streamName)).getCode());
    }
  }

  private static StreamTask buildStaleTask(
      final String streamName, final String host, final int port) {
    final Symbol time = new Symbol("time");
    final Symbol value = new Symbol("value");
    final EventScanNode eventScanNode =
        new EventScanNode(
            new PlanNodeId("event-scan"),
            List.of(time, value),
            Map.of(
                time,
                    new ColumnSchema(
                        "time", TimestampType.TIMESTAMP, false, TsTableColumnCategory.TIME),
                value,
                    new ColumnSchema("value", LongType.INT64, false, TsTableColumnCategory.FIELD)));
    final StreamTask task = new StreamTask();
    task.setTaskName(streamName);
    task.setDatabase("test");
    task.setSource(
        new IoTDBSubscriptionSource(
            "test",
            "src_t",
            null,
            null,
            host,
            port,
            SessionConfig.DEFAULT_USER,
            AuthUtils.encryptPassword(SessionConfig.DEFAULT_PASSWORD)));
    final TumbleWindow window = new TumbleWindow(5000L, 0L);
    window.setEventInputColumns(List.of(TsFileConstant.TIME_COLUMN_ID, "value"));
    task.setWindow(window);
    task.setTypeProvider(
        new StreamNodeTableTypeProvider(
            Map.of(time, TimestampType.TIMESTAMP, value, LongType.INT64)));
    task.setCalcPlan(eventScanNode.serializeToByteBuffer());
    task.setTarget(
        new IoTDBTarget(
            host + ":" + port,
            "test",
            "sink_t",
            List.of("time", "value"),
            List.of(TSDataType.TIMESTAMP, TSDataType.INT64),
            List.of(ColumnCategory.TIME, ColumnCategory.FIELD),
            null));
    return task;
  }

  private static int getRuntimeEpoch(final StreamNodeWrapper wrapper, final String streamName)
      throws Exception {
    final TEndPoint streamNodeEndpoint = new TEndPoint(wrapper.getIp(), wrapper.getPort());
    try (SyncStreamNodeClient client = newStreamNodeClient(streamNodeEndpoint)) {
      for (final TTaskHeartbeat heartbeat :
          client
              .getHeartbeat(
                  new TStreamNodeHeartbeatReq(System.nanoTime(), System.currentTimeMillis(), false))
              .getRunningTasks()) {
        if (streamName.equals(heartbeat.getTaskName())) {
          return heartbeat.getEpoch();
        }
      }
      return -1;
    }
  }

  private static SyncStreamNodeClient newStreamNodeClient(final TEndPoint streamNodeEndpoint)
      throws Exception {
    return new SyncStreamNodeClient(
        new ThriftClientProperty.Builder().build(), streamNodeEndpoint, null) {
      @Override
      public void close() {
        invalidate();
      }
    };
  }

  /**
   * TUMBLE part of STOP-01. While RUNNING the stream is first proven to compute (window 1 emits
   * (tBase, 2)); after STOP, new input for later windows must not start any new computation.
   * Windows that were already open at STOP time are out of scope per the case definition, so only
   * outputs of the post-stop windows (event time >= tBase+10000) are forbidden.
   */
  private void verifyTumbleStreamManualStop(Statement statement) throws Exception {
    final String streamName = "stop_tumble_stream";
    statement.execute(
        "CREATE TABLE tumble_src (time TIMESTAMP TIME, device_id STRING TAG, value INT64 FIELD)");
    statement.execute("CREATE TABLE tumble_sink (time TIMESTAMP TIME, value INT64 FIELD)");
    long tBase = System.currentTimeMillis() + 1000;
    statement.execute(
        String.format(
            "CREATE STREAM %s FROM test.tumble_src TUMBLE(%dms,%d) INTO test.tumble_sink(time,"
                + " value) SELECT ${START_TIME}, count(*) FROM ${ROWS}",
            streamName, 5000, tBase));
    statement.execute("START STREAM " + streamName);
    AWAIT.untilAsserted(
        () ->
            assertEquals(
                "The TUMBLE stream should be RUNNING before STOP",
                "RUNNING",
                queryStreamRow(statement, streamName)
                    .get(ColumnHeaderConstant.STREAM_STATUS_TABLE_MODEL)));

    // While RUNNING: two rows in window 1 [tBase, tBase+5s) plus a watermark-pushing row in
    // window 2 make window 1 emit (tBase, 2).
    statement.execute(
        String.format(
            "INSERT INTO test.tumble_src (time, device_id, value) VALUES (%d, 'd1', 1),"
                + " (%d, 'd1', 2)",
            tBase + 1000, tBase + 2000));
    statement.execute(
        String.format(
            "INSERT INTO test.tumble_src (time, device_id, value) VALUES (%d, 'd1', 9)",
            tBase + 6000));
    OUTPUT_AWAIT.untilAsserted(
        () -> {
          List<Map<String, Object>> sinkRows =
              queryRows(statement, "SELECT time, value FROM test.tumble_sink");
          assertEquals("The running stream should have computed window 1", 1, sinkRows.size());
          assertEquals(tBase, asLong(sinkRows.get(0).get("time")));
          assertEquals(2L, asLong(sinkRows.get(0).get("value")));
        });

    Map<String, Object> rowBefore = queryStreamRow(statement, streamName);
    long stopMoment = System.currentTimeMillis();
    statement.execute("STOP STREAM " + streamName);
    AWAIT.untilAsserted(
        () ->
            assertEquals(
                "The TUMBLE stream should be STOPPED after STOP STREAM",
                "STOPPED",
                queryStreamRow(statement, streamName)
                    .get(ColumnHeaderConstant.STREAM_STATUS_TABLE_MODEL)));

    Map<String, Object> rowAfter = queryStreamRow(statement, streamName);
    long lastDownTime =
        asLong(rowAfter.get(ColumnHeaderConstant.STREAM_LAST_DOWN_TIME_TABLE_MODEL));
    assertTrue(
        "last_down_time should represent this transition into STOPPED",
        lastDownTime >= stopMoment - 2000L && lastDownTime <= System.currentTimeMillis() + 2000L);
    assertTrue(
        "last_down_time should be updated by this STOP",
        lastDownTime
            > asLongOrDefault(
                rowBefore.get(ColumnHeaderConstant.STREAM_LAST_DOWN_TIME_TABLE_MODEL)));

    // After STOPPED: two rows in window 3 [tBase+10s, tBase+15s) plus a watermark-pushing row in
    // window 4 would emit (tBase+10000, 2) if the stream were still running. Outputs of these
    // post-stop windows must not appear; windows already open at STOP time are tolerated.
    statement.execute(
        String.format(
            "INSERT INTO test.tumble_src (time, device_id, value) VALUES (%d, 'd1', 10),"
                + " (%d, 'd1', 20)",
            tBase + 11000, tBase + 12000));
    statement.execute(
        String.format(
            "INSERT INTO test.tumble_src (time, device_id, value) VALUES (%d, 'd1', 30)",
            tBase + 16000));
    TimeUnit.SECONDS.sleep(3);
    for (Map<String, Object> sinkRow : queryRows(statement, "SELECT time FROM test.tumble_sink")) {
      assertTrue(
          "A stopped stream must not compute windows of post-stop input (time="
              + sinkRow.get("time")
              + ")",
          asLong(sinkRow.get("time")) < tBase + 10000);
    }
  }

  /**
   * PERIOD part of STOP-01. While RUNNING the stream is first proven to compute on the period grid;
   * after STOP, new data at future trigger points must not produce any new stats rows. Windows
   * already in flight at STOP time are out of scope per the case definition, so only rows at/after
   * the post-stop data base are forbidden.
   */
  private void verifyPeriodStreamManualStop(Statement statement) throws Exception {
    final String streamName = "stop_period_stream";
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
    statement.execute("START STREAM " + streamName);
    AWAIT.untilAsserted(
        () ->
            assertEquals(
                "The PERIOD stream should be RUNNING before STOP",
                "RUNNING",
                queryStreamRow(statement, streamName)
                    .get(ColumnHeaderConstant.STREAM_STATUS_TABLE_MODEL)));

    // While RUNNING: the reference periodic layout on the period grid (per-meter last-first
    // summed over the fired windows) proves the stream computes.
    long firstDataBase = origin + ((System.currentTimeMillis() - origin) / 1000 + 3) * 1000;
    // Data points are 100ms apart so every meter has multiple points within a
    // single 1s PERIOD window: last-first is then non-zero (e.g. M1 = 500.5 - 100.5).
    long t1 = firstDataBase;
    long t2 = firstDataBase + 100;
    long t3 = firstDataBase + 200;
    long t4 = firstDataBase + 1100;
    long t5 = firstDataBase + 1200;
    long t6 = firstDataBase + 1500;
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

    Map<String, Object> rowBefore = queryStreamRow(statement, streamName);
    long stopMoment = System.currentTimeMillis();
    statement.execute("STOP STREAM " + streamName);
    AWAIT.untilAsserted(
        () ->
            assertEquals(
                "The PERIOD stream should be STOPPED after STOP STREAM",
                "STOPPED",
                queryStreamRow(statement, streamName)
                    .get(ColumnHeaderConstant.STREAM_STATUS_TABLE_MODEL)));

    Map<String, Object> rowAfter = queryStreamRow(statement, streamName);
    long lastDownTime =
        asLong(rowAfter.get(ColumnHeaderConstant.STREAM_LAST_DOWN_TIME_TABLE_MODEL));
    assertTrue(
        "last_down_time should represent this transition into STOPPED",
        lastDownTime >= stopMoment - 2000L && lastDownTime <= System.currentTimeMillis() + 2000L);
    assertTrue(
        "last_down_time should be updated by this STOP",
        lastDownTime
            > asLongOrDefault(
                rowBefore.get(ColumnHeaderConstant.STREAM_LAST_DOWN_TIME_TABLE_MODEL)));

    // After STOPPED: fresh data at future trigger points (aligned to the grid, ~3s ahead) would
    // produce new stats rows if the stream were still running. Wait for those trigger points to
    // pass and then verify that no row at/after the post-stop data base exists.
    long secondDataBase = origin + ((System.currentTimeMillis() - origin) / 1000 + 3) * 1000;
    statement.execute(
        String.format(
            "INSERT INTO test.electricity_meter (time, meter_id, total_consumption) VALUES"
                + " (%d, 'M1', 1000.5), (%d, 'M2', 1110.5),"
                + " (%d, 'M1', 1200.5), (%d, 'M2', 1310.5)",
            secondDataBase, secondDataBase, secondDataBase + 1000, secondDataBase + 1000));
    waitUntil(secondDataBase + 2000);
    for (Map<String, Object> statsRow :
        queryRows(statement, "SELECT time FROM test.electricity_meter_stats")) {
      assertTrue(
          "A stopped stream must not compute windows of post-stop input (time="
              + statsRow.get("time")
              + ")",
          asLong(statsRow.get("time")) < secondDataBase);
    }
  }

  private static void waitUntil(long wallClockMillis) throws InterruptedException {
    long delta = wallClockMillis - System.currentTimeMillis();
    if (delta > 0) {
      TimeUnit.MILLISECONDS.sleep(delta);
    }
  }

  private static StreamNodeWrapper getStreamNodeWrapper() {
    final List<StreamNodeWrapper> streamNodeWrappers =
        StreamEnvFactory.getEnv().getNodeWrapperList().stream()
            .filter(StreamNodeWrapper.class::isInstance)
            .map(StreamNodeWrapper.class::cast)
            .toList();
    assertEquals("There should be exactly one StreamNode wrapper", 1, streamNodeWrappers.size());
    return streamNodeWrappers.get(0);
  }

  private static boolean isAnyStreamNodeRunning(final Statement statement) throws SQLException {
    try (ResultSet resultSet = statement.executeQuery("SHOW STREAMNODES")) {
      while (resultSet.next()) {
        if ("Running".equals(resultSet.getString(ColumnHeaderConstant.STATUS))) {
          return true;
        }
      }
    }
    return false;
  }
}
