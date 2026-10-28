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

import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.BooleanLiteral;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.LongLiteral;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.TimeDurationLiteral;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.stream.CreateStream;
import org.apache.iotdb.db.protocol.session.IClientSession;
import org.apache.iotdb.db.protocol.session.InternalClientSession;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.time.ZoneId;
import java.util.Map;

public class StreamPropertiesStatementTest {

  private SqlParser sqlParser;
  private IClientSession clientSession;

  @Before
  public void setUp() {
    sqlParser = new SqlParser();
    clientSession = new InternalClientSession("testClient");
  }

  @Test
  public void testCreateStreamWithAllProperties() {
    final CreateStream statement =
        (CreateStream)
            sqlParser.createStatement(
                "CREATE STREAM s FROM testdb.t1 "
                    + "TUMBLE(size => 1h, origin => 2000-01-01T00:00:00) "
                    + "INTO testdb.t2(time, s1) "
                    + "SELECT ${start_time}, count(*) FROM ${rows} "
                    + "WITH (watermark = 10s, allowed_lateness = 5m, "
                    + "max_delay_window_close = 1h, max_idle_window_retain = 2h, "
                    + "left_closed = false)",
                ZoneId.systemDefault(),
                clientSession);

    Assert.assertNotNull(statement);
    Map<String, Expression> properties = statement.getProperties();
    Assert.assertNotNull(properties);
    Assert.assertEquals(5, properties.size());

    // Verify watermark is TimeDurationLiteral
    TimeDurationLiteral watermark = (TimeDurationLiteral) properties.get("watermark");
    Assert.assertNotNull(watermark.getValue());

    // Verify allowed_lateness is TimeDurationLiteral
    TimeDurationLiteral allowedLateness = (TimeDurationLiteral) properties.get("allowed_lateness");
    Assert.assertNotNull(allowedLateness.getValue());

    // Verify max_delay_window_close is TimeDurationLiteral
    TimeDurationLiteral maxDelayWindowClose =
        (TimeDurationLiteral) properties.get("max_delay_window_close");
    Assert.assertNotNull(maxDelayWindowClose.getValue());

    // Verify max_idle_window_retain is TimeDurationLiteral
    TimeDurationLiteral maxIdleWindowRetain =
        (TimeDurationLiteral) properties.get("max_idle_window_retain");
    Assert.assertNotNull(maxIdleWindowRetain.getValue());

    // Verify left_closed is BooleanLiteral
    BooleanLiteral leftClosed = (BooleanLiteral) properties.get("left_closed");
    Assert.assertFalse(leftClosed.getValue());
  }

  @Test
  public void testCreateStreamWithNoProperties() {
    final CreateStream statement =
        (CreateStream)
            sqlParser.createStatement(
                "CREATE STREAM s FROM testdb.t1 "
                    + "TUMBLE(size => 1h) "
                    + "INTO testdb.t2(time, s1) "
                    + "SELECT ${start_time}, count(*) FROM ${rows}",
                ZoneId.systemDefault(),
                clientSession);

    Assert.assertNotNull(statement);
    Map<String, Expression> properties = statement.getProperties();
    // Without WITH clause, properties should be empty
    Assert.assertNotNull(properties);
    Assert.assertTrue(properties.isEmpty());
  }

  @Test
  public void testCreateStreamWithLeftClosedTrue() {
    final CreateStream statement =
        (CreateStream)
            sqlParser.createStatement(
                "CREATE STREAM s FROM testdb.t1 "
                    + "TUMBLE(size => 1h) "
                    + "INTO testdb.t2(time, s1) "
                    + "SELECT ${start_time}, count(*) FROM ${rows} "
                    + "WITH (left_closed = true)",
                ZoneId.systemDefault(),
                clientSession);

    Assert.assertNotNull(statement);
    Map<String, Expression> properties = statement.getProperties();
    Assert.assertNotNull(properties);
    Assert.assertEquals(1, properties.size());

    BooleanLiteral leftClosed = (BooleanLiteral) properties.get("left_closed");
    Assert.assertNotNull(leftClosed);
    Assert.assertTrue(leftClosed.getValue());
  }

  @Test
  public void testPropertyNamesAreLowerCase() {
    final CreateStream statement =
        (CreateStream)
            sqlParser.createStatement(
                "CREATE STREAM s FROM testdb.t1 "
                    + "TUMBLE(size => 1h) "
                    + "INTO testdb.t2(time, s1) "
                    + "SELECT ${start_time}, count(*) FROM ${rows} "
                    + "WITH (Watermark = 10s, ALLOWED_LATENESS = 5m)",
                ZoneId.systemDefault(),
                clientSession);

    Assert.assertNotNull(statement);
    Map<String, Expression> properties = statement.getProperties();
    Assert.assertNotNull(properties);

    // Property names should be lowercased
    Assert.assertTrue(properties.containsKey("watermark"));
    Assert.assertTrue(properties.containsKey("allowed_lateness"));
    Assert.assertFalse(properties.containsKey("Watermark"));
    Assert.assertFalse(properties.containsKey("ALLOWED_LATENESS"));
  }

  @Test
  public void testCreateStreamWithMixedLiteralTypes() {
    final CreateStream statement =
        (CreateStream)
            sqlParser.createStatement(
                "CREATE STREAM s FROM testdb.t1 "
                    + "TUMBLE(size => 1h) "
                    + "INTO testdb.t2(time, s1) "
                    + "SELECT ${start_time}, count(*) FROM ${rows} "
                    + "WITH (watermark = 1000, allowed_lateness = 5m, left_closed = false)",
                ZoneId.systemDefault(),
                clientSession);

    Assert.assertNotNull(statement);
    Map<String, Expression> properties = statement.getProperties();
    Assert.assertNotNull(properties);
    Assert.assertEquals(3, properties.size());

    // Verify different types and their values
    LongLiteral watermark = (LongLiteral) properties.get("watermark");
    Assert.assertNotNull(watermark);
    Assert.assertEquals(1000L, watermark.getParsedValue());

    TimeDurationLiteral allowedLateness = (TimeDurationLiteral) properties.get("allowed_lateness");
    Assert.assertNotNull(allowedLateness);
    Assert.assertEquals(300_000L, allowedLateness.getValue().nonMonthDuration);

    BooleanLiteral leftClosed = (BooleanLiteral) properties.get("left_closed");
    Assert.assertNotNull(leftClosed);
    Assert.assertFalse(leftClosed.getValue());
  }
}
