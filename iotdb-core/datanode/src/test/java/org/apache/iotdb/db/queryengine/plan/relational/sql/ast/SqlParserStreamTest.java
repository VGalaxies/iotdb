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

package org.apache.iotdb.db.queryengine.plan.relational.sql.ast;

import org.apache.iotdb.commons.queryengine.plan.relational.sql.parser.ParsingException;
import org.apache.iotdb.db.queryengine.plan.relational.sql.parser.SqlParser;

import org.junit.Test;

import java.time.ZoneId;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class SqlParserStreamTest {

  @Test
  public void testParseStreamPlaceHolderError() {
    String sql =
        "create stream s1 from testdb.t1 "
            + "period(period => 1h, origin => 2000-01-01T00:00:00) "
            + "into testdb.t2(time, count) "
            + "select ${curr_time}, count(*) from testdb.t1";
    String errMsg = "mismatched input 'curr_time'";
    testParseError(sql, errMsg);
  }

  @Test
  public void testParsePeriodWindowMissingArgumentError() {
    String sql =
        "create stream s1 from testdb.t1 "
            + "period(origin => 2000-01-01T00:00:00) "
            + "into testdb.t2(time, count) "
            + "select ${start_time}, count(*) from testtb.t1";
    String errMsg = "Period event window requires 'period' argument";
    testParseError(sql, errMsg);
  }

  @Test
  public void testParsePeriodWindowInvalidArgumentError() {
    String sql =
        "create stream s1 from testdb.t1 "
            + "period(period => 1h, origin => 'c1') "
            + "into testdb.t2(time, count) "
            + "select ${start_time}, count(*) from testtb.t1";
    String errMsg = "Invalid argument type for period event window";
    testParseError(sql, errMsg);
  }

  @Test
  public void testParsePeriodWindowArgsPassedByPosition() {
    String sql =
        "create stream s1 from testdb.t1 "
            + "period(1h, 2000-01-01T00:00:00) "
            + "into testdb.t2(time, count) "
            + "select ${start_time}, count(*) from testtb.t1";
    testParseSuccess(sql);
  }

  @Test
  public void testParseTumbleWindowMissingArgumentError() {
    String sql =
        "create stream s1 from testdb.t1 "
            + "Tumble(origin => 2000-01-01T00:00:00) "
            + "into testdb.t2(time, count) "
            + "select ${start_time}, count(*) from ${rows}";
    String errMsg = "Tumble event window requires 'size' argument";
    testParseError(sql, errMsg);
  }

  @Test
  public void testParseTumbleWindowDuplicateArgumentError() {
    String sql =
        "create stream s1 from testdb.t1 "
            + "Tumble(size => 1h, size => 1h, origin => 2000-01-01T00:00:00) "
            + "into testdb.t2(time, count) "
            + "select ${start_time}, count(*) from ${rows}";
    String errMsg = "Duplicate argument name: 'size'";
    testParseError(sql, errMsg);
  }

  @Test
  public void testParseTumbleWindowExceedingArgs() {
    String sql =
        "create stream s1 from testdb.t1 "
            + "tumble(1h, 2000-01-01T00:00:00, c1, 5) "
            + "into testdb.t2(time, count) "
            + "select ${start_time}, count(*) from ${rows}";
    testParseError(sql, "Exceeding arguments provided");
  }

  @Test
  public void testParseTumbleWindowArgsPassedByPosition() {
    String sql =
        "create stream s1 from testdb.t1 "
            + "tumble(1h, 2000-01-01T00:00:00, c1) "
            + "into testdb.t2(time, count) "
            + "select ${start_time}, count(*) from ${rows}";
    testParseSuccess(sql);
  }

  @Test
  public void testParseTumbleWindowStringTimeCol() {
    String sql =
        "create stream s1 from testdb.t1 "
            + "tumble(time => 'c1', size => 1h, origin => 2000-01-01T00:00:00) "
            + "into testdb.t2(time, count) "
            + "select ${start_time}, count(*) from ${rows}";
    testParseSuccess(sql);
  }

  @Test
  public void testParseCapacityWindowInvalidArgumentError() {
    String sql =
        "create stream s1 from testdb.t1 "
            + "capacity(size => 10, columns => (1, 2)) "
            + "into testdb.t2(time, count) "
            + "select ${start_time}, count(*) from ${rows}";
    String errMsg = "Invalid argument type for capacity event window";
    testParseError(sql, errMsg);
  }

  @Test
  public void testParseCapacityWindowExceedingArgs() {
    String sql =
        "create stream s1 from testdb.t1 "
            + "capacity(10, (c1, c2), 3) "
            + "into testdb.t2(time, count) "
            + "select ${start_time}, count(*) from ${rows}";
    testParseError(sql, "Exceeding arguments provided");
  }

  @Test
  public void testParseCapacityWindowArgsPassedByPosition() {
    String sql =
        "create stream s1 from testdb.t1 "
            + "capacity(10, (c1, c2)) "
            + "into testdb.t2(time, count) "
            + "select ${start_time}, count(*) from ${rows}";
    testParseSuccess(sql);
  }

  @Test
  public void testParseCapacityWindowStringColumns() {
    String sql =
        "create stream s1 from testdb.t1 "
            + "capacity(size => 10, columns => ('c1', 'c2')) "
            + "into testdb.t2(time, count) "
            + "select ${start_time}, count(*) from ${rows}";
    testParseSuccess(sql);
  }

  @Test
  public void testParseUnknownWindowError() {
    String sql =
        "create stream s1 from testdb.t1 "
            + "count(size => 10) "
            + "into testdb.t2(time, count) "
            + "select ${start_time}, count(*) from ${rows}";
    String errMsg = "Unknown stream event window";
    testParseError(sql, errMsg);
  }

  private void testParseSuccess(String sql) {
    SqlParser sqlParser = new SqlParser();
    ExecutorService executor = Executors.newSingleThreadExecutor();
    Future<?> future =
        executor.submit(
            () -> {
              try {
                sqlParser.createStatement(sql, ZoneId.systemDefault(), null);
              } catch (ParsingException e) {
                fail("Unexpected ParsingException to be thrown");
              }
            });

    try {
      // The parsing should fail quickly. If it hangs (OOM), this timeout will trigger.
      future.get(5, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      fail("Interrupted");
    } catch (ExecutionException e) {
      fail("Unexpected exception: " + e.getCause());
    } catch (TimeoutException e) {
      fail("Parsing timed out - potential endless loop/OOM detected");
    } finally {
      executor.shutdownNow();
    }
  }

  private void testParseError(String sql, String errMsg) {
    SqlParser sqlParser = new SqlParser();
    ExecutorService executor = Executors.newSingleThreadExecutor();
    Future<?> future =
        executor.submit(
            () -> {
              try {
                sqlParser.createStatement(sql, ZoneId.systemDefault(), null);
                fail("Expected ParsingException to be thrown");
              } catch (ParsingException e) {
                assertTrue(e.getMessage().contains(errMsg));
              }
            });

    try {
      // The parsing should fail quickly. If it hangs (OOM), this timeout will trigger.
      future.get(5, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      fail("Interrupted");
    } catch (ExecutionException e) {
      // If parsing exception propagates here (which it shouldn't as it's caught in
      // the task), handle it
      if (e.getCause() instanceof ParsingException) {
        assertTrue(e.getCause().getMessage().contains(errMsg));
      } else {
        fail("Unexpected exception: " + e.getCause());
      }
    } catch (TimeoutException e) {
      fail("Parsing timed out - potential endless loop/OOM detected");
    } finally {
      executor.shutdownNow();
    }
  }
}
