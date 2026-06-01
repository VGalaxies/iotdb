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

import org.apache.iotdb.isession.ITableSession;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;

import org.apache.tsfile.read.common.RowRecord;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class})
public class StreamSqlMetadataIT extends AbstractStreamNodeIT {

  private static final String DATABASE_NAME = "stream_sql_metadata_db";
  private static final String SOURCE_TABLE = "stream_sql_source";
  private static final String SINK_TABLE = "stream_sql_sink";
  private static final String STREAM_NAME = "stream_sql_metadata_it";

  @Test
  public void testCreateStreamSqlWritesQueryableMetadataWithoutStartingTask() throws Exception {
    try (final ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
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
  public void testDropStreamSqlRemovesQueryableMetadata() throws Exception {
    try (final ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      createTables(session);
      createStream(session);

      AWAIT.untilAsserted(
          () ->
              Assert.assertNotNull(
                  "Stream metadata should exist before drop", queryStreamMetadata(session)));

      session.executeNonQueryStatement("DROP STREAM " + STREAM_NAME);

      AWAIT.untilAsserted(
          () ->
              Assert.assertNull(
                  "Dropped stream metadata should not be queryable", queryStreamMetadata(session)));
    }
  }

  @Test
  public void testStopStreamSqlUpdatesQueryableMetadata() throws Exception {
    try (final ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      createTables(session);
      createStream(session);

      session.executeNonQueryStatement("STOP STREAM " + STREAM_NAME);

      AWAIT.untilAsserted(
          () -> {
            final StreamMetadata metadata = queryStreamMetadata(session);
            Assert.assertNotNull("Stream metadata should still exist after stop", metadata);
            Assert.assertEquals("STOPPED", metadata.status);
            Assert.assertTrue(
                "Stopped stream should not be assigned to a StreamNode",
                metadata.runningOn == null || metadata.runningOn.isEmpty());
          });
    }
  }

  @Test
  public void testStartStreamSqlReachesConfigNodeRuntimePath() throws Exception {
    try (final ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      createTables(session);
      createStream(session);

      final Exception exception =
          Assert.assertThrows(
              Exception.class,
              () -> session.executeNonQueryStatement("START STREAM " + STREAM_NAME));
      Assert.assertTrue(
          exception.getMessage(), exception.getMessage().contains("No available StreamNode"));

      final StreamMetadata metadata = queryStreamMetadata(session);
      Assert.assertNotNull("Failed start should keep stream metadata", metadata);
      Assert.assertEquals("CREATED", metadata.status);
    }
  }

  private void createStream(final ITableSession session) throws Exception {
    session.executeNonQueryStatement(
        "CREATE STREAM "
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
    try (final SessionDataSet dataSet =
        session.executeQueryStatement(
            "SELECT stream_name, status, running_on FROM information_schema.streams "
                + "WHERE stream_name = '"
                + STREAM_NAME
                + "'")) {
      if (!dataSet.hasNext()) {
        return null;
      }
      final RowRecord row = dataSet.next();
      Assert.assertFalse("Stream metadata query should return one row", dataSet.hasNext());
      return new StreamMetadata(
          row.getFields().get(0).getStringValue(),
          row.getFields().get(1).getStringValue(),
          row.getFields().get(2).getStringValue());
    }
  }

  private static class StreamMetadata {

    private final String streamName;
    private final String status;
    private final String runningOn;

    private StreamMetadata(final String streamName, final String status, final String runningOn) {
      this.streamName = streamName;
      this.status = status;
      this.runningOn = runningOn;
    }
  }
}
