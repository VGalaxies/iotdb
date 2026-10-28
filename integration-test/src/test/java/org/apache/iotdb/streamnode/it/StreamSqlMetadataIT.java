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

import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.schema.column.ColumnHeaderConstant;
import org.apache.iotdb.commons.schema.table.InformationSchema;
import org.apache.iotdb.commons.stream.StreamProperties;
import org.apache.iotdb.isession.ITableSession;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.it.env.StreamEnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;

import org.apache.tsfile.read.common.RowRecord;
import org.awaitility.Awaitility;
import org.awaitility.core.ConditionFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class})
public class StreamSqlMetadataIT {

  private static final String DATABASE_NAME = "stream_sql_metadata_db";
  private static final String SOURCE_TABLE = "stream_sql_source";
  private static final String SINK_TABLE = "stream_sql_sink";
  private static final String STREAM_NAME = "stream_sql_metadata_it";

  protected static final ConditionFactory AWAIT =
      Awaitility.await()
          .pollInSameThread()
          .pollDelay(1, TimeUnit.SECONDS)
          .pollInterval(1, TimeUnit.SECONDS)
          .atMost(600, TimeUnit.SECONDS);

  @BeforeClass
  public static void setUpCluster() throws Exception {
    StreamEnvFactory.getEnv(1)
        .getConfig()
        .getCommonConfig()
        .setSubscriptionEnabled(true)
        .setPipeMemoryManagementEnabled(false)
        .setIsPipeEnableMemoryCheck(false)
        // Subscription topics use mode=consensus, which requires IoTConsensus on data regions.
        .setDataRegionConsensusProtocolClass("org.apache.iotdb.consensus.iot.IoTConsensus");

    StreamEnvFactory.getEnv(1).initClusterEnvironment();
  }

  @After
  public void cleanUpTestData() throws Exception {
    try (final ITableSession session = StreamEnvFactory.getEnv(1).getTableSessionConnection()) {
      final List<String> streamNames = new ArrayList<>();
      try (final SessionDataSet dataSet = session.executeQueryStatement("SHOW STREAMS")) {
        while (dataSet.hasNext()) {
          streamNames.add(dataSet.next().getFields().get(0).getStringValue());
        }
      }
      for (final String streamName : streamNames) {
        session.executeNonQueryStatement("DROP STREAM " + streamName);
      }

      final List<String> databaseNames = new ArrayList<>();
      try (final SessionDataSet dataSet = session.executeQueryStatement("SHOW DATABASES")) {
        while (dataSet.hasNext()) {
          final String databaseName = dataSet.next().getFields().get(0).getStringValue();
          if (!InformationSchema.INFORMATION_DATABASE.equals(databaseName)) {
            databaseNames.add(databaseName);
          }
        }
      }
      for (final String databaseName : databaseNames) {
        session.executeNonQueryStatement("DROP DATABASE " + databaseName);
      }
    }
  }

  @Test
  public void testCreateStreamSqlWritesQueryableMetadataWithoutStartingTask() throws Exception {
    try (final ITableSession session = StreamEnvFactory.getEnv(1).getTableSessionConnection()) {
      createTables(session);

      createStream(session);

      AWAIT.untilAsserted(
          () -> {
            final StreamMetadata metadata = queryStreamMetadata(session);
            Assert.assertNotNull("Stream metadata should be queryable by SQL", metadata);
            Assert.assertEquals(STREAM_NAME, metadata.streamName);
            Assert.assertEquals(
                "Create-stream SQL metadata path should not start runtime execution",
                "CREATED",
                metadata.status);
            Assert.assertTrue(
                "Non-started stream should not be assigned to a StreamNode",
                metadata.runningOn == null || metadata.runningOn.isEmpty());
          });
    }
  }

  @Test
  public void testShowStreamsIncludesPropertiesAsLastColumn() throws Exception {
    try (final ITableSession session = StreamEnvFactory.getEnv(1).getTableSessionConnection()) {
      createTables(session);
      createStream(session);

      try (final SessionDataSet dataSet = session.executeQueryStatement("SHOW STREAMS")) {
        Assert.assertEquals(
            ColumnHeaderConstant.showStreamColumnHeaders.size(), dataSet.getColumnNames().size());
        for (int i = 0; i < ColumnHeaderConstant.showStreamColumnHeaders.size(); i++) {
          Assert.assertEquals(
              ColumnHeaderConstant.showStreamColumnHeaders.get(i).getColumnName(),
              dataSet.getColumnNames().get(i));
        }

        boolean found = false;
        while (dataSet.hasNext()) {
          final RowRecord row = dataSet.next();
          if (STREAM_NAME.equals(row.getFields().get(0).getStringValue())) {
            found = true;
            Assert.assertEquals(14, row.getFields().size());
            Assert.assertEquals(
                expectedStreamProperties(), row.getFields().get(13).getStringValue());
          }
        }
        Assert.assertTrue("Created stream should be returned by SHOW STREAMS", found);
      }

      try (final SessionDataSet dataSet =
          session.executeQueryStatement(
              "SELECT * FROM information_schema.streams WHERE stream_name = '"
                  + STREAM_NAME
                  + "'")) {
        Assert.assertEquals(
            ColumnHeaderConstant.showStreamColumnHeaders.size(), dataSet.getColumnNames().size());
        for (int i = 0; i < ColumnHeaderConstant.showStreamColumnHeaders.size(); i++) {
          Assert.assertEquals(
              ColumnHeaderConstant.showStreamColumnHeaders.get(i).getColumnName(),
              dataSet.getColumnNames().get(i));
        }
        Assert.assertTrue(dataSet.hasNext());
        final RowRecord row = dataSet.next();
        Assert.assertEquals(STREAM_NAME, row.getFields().get(0).getStringValue());
        Assert.assertEquals(14, row.getFields().size());
        Assert.assertEquals(expectedStreamProperties(), row.getFields().get(13).getStringValue());
        Assert.assertFalse(dataSet.hasNext());
      }
    }
  }

  private String expectedStreamProperties() {
    final StreamProperties expectedProperties = new StreamProperties();
    expectedProperties.setSubscriptionConsensusWalRetentionSizeInBytes(
        CommonDescriptor.getInstance()
            .getConfig()
            .getSubscriptionConsensusWalRetentionSizeInBytes());
    expectedProperties.setSubscriptionConsensusWalRetentionTimeMs(
        CommonDescriptor.getInstance().getConfig().getSubscriptionConsensusWalRetentionTimeMs());
    return expectedProperties.toString();
  }

  @Test
  public void testCreatePeriodStreamWithStartTimePlaceholderKeepsQueryableMetadata()
      throws Exception {
    final String databaseName = "stream_period_placeholder_db";
    final String sourceTable = "t1";
    final String sinkTable = "rows_calc";
    final String streamName = "stream_period_placeholder_it";

    try (final ITableSession session = StreamEnvFactory.getEnv(1).getTableSessionConnection()) {
      session.executeNonQueryStatement("CREATE DATABASE " + databaseName);
      session.executeNonQueryStatement("USE " + databaseName);
      session.executeNonQueryStatement(
          "CREATE TABLE "
              + sourceTable
              + " (time TIMESTAMP TIME, device_id STRING TAG, value FLOAT FIELD)");
      session.executeNonQueryStatement(
          "CREATE TABLE " + sinkTable + " (time TIMESTAMP TIME, rows INT64 FIELD)");

      session.executeNonQueryStatement(
          "CREATE STREAM "
              + streamName
              + " PERIOD(1h) "
              + "INTO "
              + databaseName
              + "."
              + sinkTable
              + "(time, rows) "
              + "SELECT ${START_TIME}, count(*) FROM "
              + databaseName
              + "."
              + sourceTable);

      AWAIT.untilAsserted(
          () -> {
            final StreamMetadata metadata = queryStreamMetadata(session, streamName);
            Assert.assertNotNull("Stream metadata should be queryable after create", metadata);
            Assert.assertEquals("CREATED", metadata.status);
            Assert.assertTrue(metadata.target, metadata.target.contains("IoTDBTarget"));
            Assert.assertTrue(metadata.target, metadata.target.contains("tableName='" + sinkTable));
            Assert.assertFalse(metadata.source, metadata.source.contains("@"));
            Assert.assertFalse(metadata.target, metadata.target.contains("@"));
          });
    }
  }

  @Test
  public void testDropStreamSqlRemovesQueryableMetadata() throws Exception {
    try (final ITableSession session = StreamEnvFactory.getEnv(1).getTableSessionConnection()) {
      createTables(session);
      createStream(session);

      AWAIT.untilAsserted(
          () ->
              Assert.assertNotNull(
                  "Stream metadata should exist before drop",
                  queryStreamMetadata(session, STREAM_NAME)));

      session.executeNonQueryStatement("DROP STREAM " + STREAM_NAME);

      AWAIT.untilAsserted(
          () ->
              Assert.assertNull(
                  "Dropped stream metadata should not be queryable",
                  queryStreamMetadata(session, STREAM_NAME)));
    }
  }

  /** Verifies that IF NOT EXISTS makes repeated CREATE STREAM idempotent. */
  @Test
  public void testCreateStreamIfNotExistsIgnoresExistingStream() throws Exception {
    try (final ITableSession session = StreamEnvFactory.getEnv(1).getTableSessionConnection()) {
      createTables(session);
      createStream(session);

      createStream(session, true);

      Assert.assertNotNull(queryStreamMetadata(session, STREAM_NAME));
    }
  }

  /** Verifies that IF EXISTS makes DROP STREAM succeed when the stream is absent. */
  @Test
  public void testDropStreamIfExistsIgnoresMissingStream() throws Exception {
    try (final ITableSession session = StreamEnvFactory.getEnv(1).getTableSessionConnection()) {
      session.executeNonQueryStatement("DROP STREAM IF EXISTS " + STREAM_NAME);
    }
  }

  @Test
  public void testStopStreamSqlUpdatesQueryableMetadata() throws Exception {
    try (final ITableSession session = StreamEnvFactory.getEnv(1).getTableSessionConnection()) {
      createTables(session);
      createStream(session);

      session.executeNonQueryStatement("STOP STREAM " + STREAM_NAME);

      AWAIT.untilAsserted(
          () -> {
            final StreamMetadata metadata = queryStreamMetadata(session, STREAM_NAME);
            Assert.assertNotNull("Stream metadata should still exist after stop", metadata);
            Assert.assertEquals("STOPPED", metadata.status);
            Assert.assertTrue(
                "Stopped stream should not be assigned to a StreamNode",
                metadata.runningOn == null || metadata.runningOn.isEmpty());
          });
    }
  }

  private void createStream(final ITableSession session) throws Exception {
    createStream(session, false);
  }

  private void createStream(final ITableSession session, final boolean ifNotExists)
      throws Exception {
    session.executeNonQueryStatement(
        "CREATE STREAM "
            + (ifNotExists ? "IF NOT EXISTS " : "")
            + STREAM_NAME
            + " FROM "
            + DATABASE_NAME
            + "."
            + SOURCE_TABLE
            + " TUMBLE(size => 1h, origin => 2000-01-01T00:00:00) "
            + "INTO "
            + DATABASE_NAME
            + "."
            + SINK_TABLE
            + "(time, value) "
            + "SELECT time, value FROM "
            + DATABASE_NAME
            + "."
            + SOURCE_TABLE);
  }

  private void createTables(final ITableSession session) throws Exception {
    session.executeNonQueryStatement("CREATE DATABASE " + DATABASE_NAME);
    session.executeNonQueryStatement("USE " + DATABASE_NAME);
    session.executeNonQueryStatement(
        "CREATE TABLE " + SOURCE_TABLE + " (device_id STRING TAG, value INT64 FIELD)");
    session.executeNonQueryStatement(
        "CREATE TABLE " + SINK_TABLE + " (device_id STRING TAG, value INT64 FIELD)");
    session.executeNonQueryStatement(
        "INSERT INTO " + SOURCE_TABLE + " (time, device_id, value) VALUES (1, 'device_0', 1)");
    session.executeNonQueryStatement("FLUSH");
  }

  private StreamMetadata queryStreamMetadata(final ITableSession session) throws Exception {
    return queryStreamMetadata(session, STREAM_NAME);
  }

  private StreamMetadata queryStreamMetadata(final ITableSession session, final String streamName)
      throws Exception {
    try (final SessionDataSet dataSet =
        session.executeQueryStatement(
            "SELECT stream_name, source, target, status, running_on FROM information_schema.streams "
                + "WHERE stream_name = '"
                + streamName
                + "'")) {
      if (!dataSet.hasNext()) {
        return null;
      }
      final RowRecord row = dataSet.next();
      Assert.assertFalse("Stream metadata query should return one row", dataSet.hasNext());
      return new StreamMetadata(
          row.getFields().get(0).getStringValue(),
          row.getFields().get(1).getStringValue(),
          row.getFields().get(2).getStringValue(),
          row.getFields().get(3).getStringValue(),
          row.getFields().get(4).getStringValue());
    }
  }

  private static class StreamMetadata {

    private final String streamName;
    private final String source;
    private final String target;
    private final String status;
    private final String runningOn;

    private StreamMetadata(
        final String streamName,
        final String source,
        final String target,
        final String status,
        final String runningOn) {
      this.streamName = streamName;
      this.source = source;
      this.target = target;
      this.status = status;
      this.runningOn = runningOn;
    }
  }
}
