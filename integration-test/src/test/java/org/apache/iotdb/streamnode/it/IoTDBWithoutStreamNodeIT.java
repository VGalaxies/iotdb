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

import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class})
public class IoTDBWithoutStreamNodeIT extends AbstractStreamNodeIT {

  private static final String DATABASE_NAME = "stream_create_without_node_db";
  private static final String SOURCE_TABLE = "stream_sql_source";
  private static final String SINK_TABLE = "stream_sql_sink";
  private static final String STREAM_NAME = "stream_create_without_node_it";

  @Test
  public void testCreateStreamSqlFailsWhenNoRegisteredStreamNode() throws Exception {
    try (final ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      session.executeNonQueryStatement("CREATE DATABASE " + DATABASE_NAME);
      session.executeNonQueryStatement("USE " + DATABASE_NAME);
      session.executeNonQueryStatement(
          "CREATE TABLE " + SOURCE_TABLE + " (device_id STRING TAG, value INT64 FIELD)");
      session.executeNonQueryStatement(
          "CREATE TABLE " + SINK_TABLE + " (device_id STRING TAG, value INT64 FIELD)");

      final Exception exception =
          Assert.assertThrows(
              Exception.class,
              () ->
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
                          + SOURCE_TABLE));
      Assert.assertTrue(
          exception.getMessage(), exception.getMessage().contains("No running StreamNode found"));

      try (final SessionDataSet dataSet =
          session.executeQueryStatement(
              "SELECT stream_name FROM information_schema.streams WHERE stream_name = '"
                  + STREAM_NAME
                  + "'")) {
        Assert.assertFalse("Failed create should not persist stream metadata", dataSet.hasNext());
      }
    }
  }
}
