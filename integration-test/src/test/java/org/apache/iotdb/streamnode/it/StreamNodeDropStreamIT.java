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
import org.apache.iotdb.db.storageengine.dataregion.wal.io.WALFileVersion;
import org.apache.iotdb.it.env.StreamEnvFactory;
import org.apache.iotdb.it.env.cluster.env.AbstractEnv;
import org.apache.iotdb.it.env.cluster.node.AbstractNodeWrapper;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.itbase.category.StreamClusterIT;
import org.apache.iotdb.itbase.env.BaseEnv;
import org.apache.iotdb.session.subscription.ISubscriptionTableSession;
import org.apache.iotdb.session.subscription.SubscriptionTableSessionBuilder;

import org.awaitility.Awaitility;
import org.awaitility.core.ConditionFactory;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.File;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Integration tests for DROP STREAM (spec cases DROP-01..06 of the stream-computing requirements
 * doc). Assertions follow the spec, so failures pinpoint the gaps that the implementation must
 * close.
 */
@Category({StreamClusterIT.class})
public class StreamNodeDropStreamIT extends StreamNodeITBase {

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

  // The DataNode deletes outdated WAL files every 2s (see setUp), so 30s covers ~15 cleanup
  // cycles; if the baseline segments still survive that long, the WAL is genuinely retained.
  private static final ConditionFactory WAL_AWAIT =
      Awaitility.await()
          .pollInSameThread()
          .pollDelay(1, TimeUnit.SECONDS)
          .pollInterval(2, TimeUnit.SECONDS)
          .atMost(30, TimeUnit.SECONDS);

  @BeforeClass
  public static void setUp() throws Exception {
    // Init 1C1D1S cluster environment (1 ConfigNode, 1 DataNode, 1 StreamNode).
    // Check deletion promptly after DROP releases retention and FLUSH makes the WAL obsolete.
    StreamEnvFactory.getEnv(1).getConfig().getDataNodeConfig().setDeleteWalFilesPeriodInMs(2000L);
    StreamEnvFactory.getEnv(1)
        .getConfig()
        .getCommonConfig()
        .setSubscriptionEnabled(true)
        // Keep source, warmup and sink writes on one region, including on high-core CI machines.
        .setDataRegionGroupExtensionPolicy("CUSTOM")
        .setDefaultDataRegionGroupNumPerDatabase(1)
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
      for (String database : new String[] {"test", "db1"}) {
        try {
          statement.execute("DROP DATABASE " + database);
        } catch (Exception ignored) {
          // database may not exist if a test failed before creating it
        }
      }
    }
  }

  /**
   * DROP-01: dropping an existing stream succeeds and removes it from information_schema.streams.
   */
  @Test
  public void testDropExistingStream() throws Exception {
    final String streamName = "stream1";
    try (Connection connection =
            StreamEnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      createSimpleStream(statement, streamName);
      AWAIT.untilAsserted(
          () ->
              assertNotNull(
                  "The stream should be queryable before the DROP",
                  queryStreamRow(statement, streamName)));

      statement.execute("DROP STREAM " + streamName);

      AWAIT.untilAsserted(
          () ->
              assertNull(
                  "The dropped stream must no longer exist in information_schema.streams",
                  queryStreamRow(statement, streamName)));
    }
  }

  /** DROP-02: dropping an existing stream with IF EXISTS succeeds. */
  @Test
  public void testDropExistingStreamWithIfExists() throws Exception {
    final String streamName = "stream1";
    try (Connection connection =
            StreamEnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      createSimpleStream(statement, streamName);
      AWAIT.untilAsserted(
          () ->
              assertNotNull(
                  "The stream should be queryable before the DROP",
                  queryStreamRow(statement, streamName)));

      statement.execute("DROP STREAM IF EXISTS " + streamName);

      AWAIT.untilAsserted(
          () ->
              assertNull(
                  "The dropped stream must no longer exist in information_schema.streams",
                  queryStreamRow(statement, streamName)));
    }
  }

  /**
   * DROP-03: dropping a non-existent stream without IF EXISTS fails with "Stream xxx does not
   * exist" and does not create a stream record.
   */
  @Test
  public void testDropNonExistentStreamFails() throws Exception {
    try (Connection connection =
            StreamEnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      assertNull(
          "missing_stream should not exist before the failed DROP",
          queryStreamRow(statement, "missing_stream"));

      boolean failed = false;
      try {
        statement.execute("DROP STREAM missing_stream");
      } catch (SQLException e) {
        failed = true;
        assertTrue(
            "The failure message should contain the spec text but was: " + e.getMessage(),
            e.getMessage().contains("Stream not found: missing_stream"));
      }
      assertTrue("DROP STREAM on a non-existent stream must fail", failed);

      assertNull(
          "A failed DROP must not create a new stream record",
          queryStreamRow(statement, "missing_stream"));
    }
  }

  /**
   * DROP-04: dropping a non-existent stream with IF EXISTS does not report a not-exist error and
   * does not create any object.
   */
  @Test
  public void testDropNonExistentStreamWithIfExistsSucceeds() throws Exception {
    try (Connection connection =
            StreamEnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      assertNull(
          "missing_stream should not exist before the DROP",
          queryStreamRow(statement, "missing_stream"));

      // Must not throw a not-exist error.
      statement.execute("DROP STREAM IF EXISTS missing_stream");

      assertNull(
          "DROP IF EXISTS on a non-existent stream must not create any object",
          queryStreamRow(statement, "missing_stream"));
    }
  }

  /**
   * DROP-05: a qualified stream name (db1.stream1) is rejected - stream names are global
   * unqualified identifiers. The exact rejection message is not asserted (review-agreed behavior).
   */
  @Test
  public void testDropQualifiedStreamNameIsRejected() throws Exception {
    final String streamName = "stream1";
    try (Connection connection =
            StreamEnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE db1");
      statement.execute("USE db1");
      statement.execute(
          "CREATE TABLE src_t (time TIMESTAMP TIME, device_id STRING TAG, value INT64 FIELD)");
      statement.execute("CREATE TABLE sink_t (time TIMESTAMP TIME, value INT64 FIELD)");
      statement.execute(
          "CREATE STREAM "
              + streamName
              + " FROM db1.src_t TUMBLE(size => 1h) INTO db1.sink_t(time, value)"
              + " SELECT ${START_TIME}, count(*) FROM ${ROWS}");
      AWAIT.untilAsserted(
          () ->
              assertNotNull(
                  "The unqualified stream1 should be queryable before the qualified DROP attempt",
                  queryStreamRow(statement, streamName)));

      boolean rejected = false;
      try {
        statement.execute("DROP STREAM db1." + streamName);
      } catch (SQLException e) {
        // The grammar only accepts an unqualified identifier; any rejection is acceptable.
        rejected = true;
      }
      assertTrue("DROP STREAM with a qualified stream name must be rejected", rejected);

      assertNotNull(
          "The rejected DROP must not affect the unqualified stream",
          queryStreamRow(statement, streamName));
    }
  }

  /**
   * DROP-06: dropping a running stream removes it from information_schema.streams, cancels its
   * managed subscription, stops further consumption of subsequent source writes, and releases the
   * WAL retention held by the subscription: the data-bearing WAL segments recorded at DROP time are
   * deleted once their data is flushed and no subscription retains them. Empty current WAL files
   * are allowed to remain.
   */
  @Test
  public void testDropCancelsSubscriptionAndReleasesWal() throws Exception {
    final String streamName = "drop06_stream";
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
      statement.execute(
          "CREATE TABLE tumble_src (time TIMESTAMP TIME, device_id STRING TAG, value INT64 FIELD)");
      statement.execute("CREATE TABLE tumble_sink (time TIMESTAMP TIME, value INT64 FIELD)");
      // Create the database's IoTConsensus DataRegion before the managed topic subscribes; table
      // metadata alone does not create a region for the subscription queue to bind to. Use the
      // source table so the WAL baseline below covers the same region that the stream consumes.
      statement.execute("INSERT INTO test.tumble_src (time, device_id, value) VALUES (1, 'd1', 1)");
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
                  "The stream should be RUNNING before the DROP",
                  "RUNNING",
                  queryStreamRow(statement, streamName)
                      .get(ColumnHeaderConstant.STREAM_STATUS_TABLE_MODEL)));

      // The managed subscription (topic) exists while the stream runs.
      AWAIT.untilAsserted(
          () ->
              assertTrue(
                  "The managed subscription topic should exist while the stream runs",
                  subscriptionSession.getTopic(topicName).isPresent()));

      // Topic creation precedes consumer initialization; wait until the source queue is ready so
      // the first write cannot race subscription startup and disappear from this test's window.
      AWAIT.untilAsserted(
          () -> {
            try (ResultSet resultSet =
                statement.executeQuery("SHOW SUBSCRIPTIONS DETAILS ON " + topicName)) {
              assertTrue("The stream subscription queue should be present", resultSet.next());
              assertTrue(
                  "The stream subscription queue should be active",
                  resultSet.getBoolean(ColumnHeaderConstant.SUBSCRIPTION_ACTIVE));
              assertTrue(
                  "The stream subscription queue should be initialized",
                  resultSet.getBoolean(ColumnHeaderConstant.SUBSCRIPTION_INITIALIZED));
            }
          });

      // While RUNNING, prove the stream consumes and computes: two rows in window 1
      // [tBase, tBase+5s) plus a watermark-pushing row in window 2 make window 1 emit
      // (tBase, 2).
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

      // A region keeps its current WAL file even after cleanup. Ignore files containing only the
      // V3 header, and require a nonempty baseline so the release assertion cannot pass vacuously.
      Set<String> walBaseline = new HashSet<>();
      listWalFilesWithSize()
          .forEach(
              (name, size) -> {
                if (size > WALFileVersion.V3.getVersionBytes().length) {
                  walBaseline.add(name);
                }
              });
      assertFalse("The WAL baseline must include data-bearing segments", walBaseline.isEmpty());
      statement.execute("DROP STREAM " + streamName);

      // The stream disappears from information_schema.streams.
      AWAIT.untilAsserted(
          () ->
              assertNull(
                  "The dropped stream must no longer exist in information_schema.streams",
                  queryStreamRow(statement, streamName)));

      // The managed subscription is cancelled: its topic is removed.
      AWAIT.untilAsserted(
          () ->
              assertFalse(
                  "The managed subscription topic must be removed by DROP STREAM",
                  subscriptionSession.getTopic(topicName).isPresent()));

      // Subsequent source writes are not consumed: two rows in window 3 [tBase+10s, tBase+15s)
      // plus a watermark-pushing row in window 4 would emit (tBase+10000, 2) if the stream were
      // still running. Windows already in flight at DROP time are out of scope, so only outputs
      // of the post-drop windows are forbidden.
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
      for (Map<String, Object> sinkRow :
          queryRows(statement, "SELECT time FROM test.tumble_sink")) {
        assertTrue(
            "A dropped stream must not compute windows of post-drop input (time="
                + sinkRow.get("time")
                + ")",
            asLong(sinkRow.get("time")) < tBase + 10000);
      }

      // FLUSH releases the memtables. The periodic deletion task then rolls a nonempty current
      // WAL file automatically, so data-bearing baseline files can be deleted without filler
      // writes.
      statement.execute("FLUSH");
      WAL_AWAIT.untilAsserted(
          () -> {
            Set<String> remaining = new HashSet<>(walBaseline);
            remaining.retainAll(listWalFileNames());
            assertTrue(
                "WAL segments recorded at DROP time must be released after the subscription is"
                    + " cancelled and the data is flushed, but still present: "
                    + remaining
                    + " ; current files: "
                    + describeWalFiles(),
                remaining.isEmpty());
          });
    }
  }

  /** Creates database test with source/sink tables and a simple TUMBLE stream (not started). */
  private void createSimpleStream(Statement statement, String streamName) throws SQLException {
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
  }

  /**
   * Lists all WAL data segment file names under the (single) DataNode's WAL directory. The WAL
   * directory contains per-region subdirectories (one per database, e.g. {@code test-1}) that hold
   * the actual {@code *.wal} files; both the top-level directory and each subdirectory are scanned.
   */
  private Set<String> listWalFileNames() {
    for (AbstractNodeWrapper wrapper :
        ((AbstractEnv) StreamEnvFactory.getEnv()).getNodeWrapperList()) {
      if (wrapper instanceof DataNodeWrapper dataNode) {
        Set<String> result = new HashSet<>();
        collectWalFileNames(new File(dataNode.getWalDir()), "", result);
        return result;
      }
    }
    return Collections.emptySet();
  }

  private void collectWalFileNames(File dir, String prefix, Set<String> result) {
    File[] entries = dir.listFiles();
    if (entries == null) {
      return;
    }
    for (File entry : entries) {
      if (entry.isDirectory()) {
        collectWalFileNames(entry, prefix + entry.getName() + "/", result);
      } else if (entry.getName().endsWith(".wal")) {
        result.add(prefix + entry.getName());
      }
    }
  }

  /** Maps each WAL segment file (recursively) to its size in bytes. */
  private Map<String, Long> listWalFilesWithSize() {
    for (AbstractNodeWrapper wrapper :
        ((AbstractEnv) StreamEnvFactory.getEnv()).getNodeWrapperList()) {
      if (wrapper instanceof DataNodeWrapper dataNode) {
        Map<String, Long> result = new LinkedHashMap<>();
        collectWalFilesWithSize(new File(dataNode.getWalDir()), "", result);
        return result;
      }
    }
    return Collections.emptyMap();
  }

  private void collectWalFilesWithSize(File dir, String prefix, Map<String, Long> result) {
    File[] entries = dir.listFiles();
    if (entries == null) {
      return;
    }
    for (File entry : entries) {
      if (entry.isDirectory()) {
        collectWalFilesWithSize(entry, prefix + entry.getName() + "/", result);
      } else if (entry.getName().endsWith(".wal")) {
        result.put(prefix + entry.getName(), entry.length());
      }
    }
  }

  /** Human-readable listing of the WAL segment files with their sizes. */
  private String describeWalFiles() {
    StringBuilder sb = new StringBuilder();
    listWalFilesWithSize()
        .forEach((name, size) -> sb.append(name).append(':').append(size).append("B "));
    return sb.toString().trim();
  }
}
