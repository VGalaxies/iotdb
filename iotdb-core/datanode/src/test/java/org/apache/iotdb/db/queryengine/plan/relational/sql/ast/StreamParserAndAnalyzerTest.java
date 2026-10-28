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

import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.SemanticException;
import org.apache.iotdb.commons.queryengine.common.SessionInfo;
import org.apache.iotdb.commons.queryengine.common.SqlDialect;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Statement;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.stream.CreateStream;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.stream.PeriodEventWindow;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.stream.TumbleEventWindow;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.parser.ParsingException;
import org.apache.iotdb.commons.queryengine.plan.relational.type.InternalTypeManager;
import org.apache.iotdb.commons.utils.CommonDateTimeUtils;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.Analysis;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.Analyzer;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.StatementAnalyzerFactory;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.TestMetadata;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.Metadata;
import org.apache.iotdb.db.queryengine.plan.relational.security.AccessControl;
import org.apache.iotdb.db.queryengine.plan.relational.security.AllowAllAccessControl;
import org.apache.iotdb.db.queryengine.plan.relational.sql.parser.SqlParser;
import org.apache.iotdb.db.queryengine.plan.relational.sql.rewrite.StatementRewriteFactory;

import org.junit.Test;

import java.time.ZoneId;
import java.util.Collections;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.apache.iotdb.db.queryengine.execution.warnings.WarningCollector.NOOP;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class StreamParserAndAnalyzerTest {
  private static final AccessControl nopAccessControl = new AllowAllAccessControl();

  private final String database = "db";
  private final QueryId queryId = new QueryId("test_stream");
  private final SessionInfo sessionInfo =
      new SessionInfo(
          1L,
          "iotdb-user",
          ZoneId.systemDefault(),
          IoTDBConstant.ClientVersion.V_1_0,
          database,
          SqlDialect.TABLE);
  private final Metadata metadata = new TestMetadata();

  @Test
  public void testParseStreamPlaceHolderError() {
    String sql =
        "create stream s1 from testdb.table1 "
            + "period(period_time => 1h, origin => 2000-01-01T00:00:00) "
            + "into testdb.table2(time, s1) "
            + "select ${curr_time}, count(*) from testdb.table1";
    String errMsg = "mismatched input 'curr_time'";
    testParseError(sql, errMsg);
  }

  @Test
  public void testParseSinkTableError() {
    String sql =
        "create stream s1 "
            + "period(period_time => 1h, origin => 2000-01-01T00:00:00) "
            + "into testdb.table2 "
            + "select ${start_time}, count(*) from testdb.table1";
    String errMsg =
        "Insert query has mismatched column types: Table: [TIMESTAMP, STRING, STRING, STRING, STRING, STRING, INT64, INT64, DOUBLE, TIMESTAMP], Query: [TIMESTAMP, INT64]";
    testAnalyzeError(sql, errMsg);
  }

  @Test
  public void testParsePeriodWindowMissingArgumentError() {
    String sql =
        "create stream s1 from testdb.table1 "
            + "period(origin => 2000-01-01T00:00:00) "
            + "into testdb.table2(time, s1) "
            + "select ${start_time}, count(*) from testtb.t1";
    String errMsg = "Period event window requires 'period_time' argument";
    testAnalyzeError(sql, errMsg);
  }

  @Test
  public void testParsePeriodWindowInvalidArgumentError() {
    String sql =
        "create stream s1 from testdb.table1 "
            + "period(period_time => 1h, origin => 'c1') "
            + "into testdb.table2(time, s1) "
            + "select ${start_time}, count(*) from testtb.t1";
    String errMsg = "Invalid argument type for period event window";
    testAnalyzeError(sql, errMsg);
  }

  @Test
  public void testParsePeriodWindowArgsPassedByPosition() {
    String sql =
        "create stream s1 "
            + "period(1h, 2000-01-01T00:00:00) "
            + "into testdb.table2(time, s1) "
            + "select ${start_time}, count(*) from testtb.t1";
    testAnalyzeSuccess(sql);
  }

  @Test
  public void testParsePeriodWindowStringOrigin() {
    String sql =
        "create stream s1 "
            + "period(period_time => 1h, origin => '2026-06-16T01:54:24.724Z') "
            + "into testdb.table2(time, s1) "
            + "select ${start_time}, count(*) from testtb.t1";
    testAnalyzeSuccess(sql);
  }

  @Test
  public void testParsePeriodWindowDoubleOrigin() {
    String sql =
        "create stream s1 "
            + "period(period_time => 1h, origin => 1.5) "
            + "into testdb.table2(time, s1) "
            + "select ${start_time}, count(*) from testtb.t1";
    String errMsg = "The origin of the event window must be integer or date time";
    testAnalyzeError(sql, errMsg);
  }

  @Test
  public void testParsePeriodWindowOmittedOriginDefaultsToNowAlignedToSecond() {
    String sql =
        "create stream s1 "
            + "period(period_time => 1h) "
            + "into testdb.table2(time, s1) "
            + "select ${start_time}, count(*) from testtb.t1";

    long before = CommonDateTimeUtils.currentTime();
    PeriodEventWindow window = (PeriodEventWindow) analyzeCreateStream(sql, "ms").getEventWindow();
    long after = CommonDateTimeUtils.currentTime();

    // Requirement: an omitted PERIOD origin defaults to the current system time, aligned down to
    // the second (ms precision: a whole multiple of 1000) and frozen at CREATE STREAM analysis
    // time, so the persisted stream config keeps a fixed anchor across restarts.
    assertNotNull(window.getOrigin());
    long origin = window.getOrigin().getParsedValue();
    assertEquals(0L, origin % 1000L);
    assertTrue(origin >= before - 1000L);
    assertTrue(origin <= after + 1000L);
  }

  @Test
  public void testParsePeriodWindowNowOriginAlignedToSecond() {
    String sql =
        "create stream s1 "
            + "period(period_time => 1h, origin => now()) "
            + "into testdb.table2(time, s1) "
            + "select ${start_time}, count(*) from testtb.t1";

    long before = CommonDateTimeUtils.currentTime();
    PeriodEventWindow window = (PeriodEventWindow) analyzeCreateStream(sql, "ms").getEventWindow();
    long after = CommonDateTimeUtils.currentTime();

    // The general expression path folds now() into a parse-time literal of the raw current time;
    // PERIOD then floors it down to a whole second like any other origin form.
    assertNotNull(window.getOrigin());
    long origin = window.getOrigin().getParsedValue();
    assertEquals(0L, origin % 1000L);
    assertTrue(origin >= before - 1000L);
    assertTrue(origin <= after + 1000L);
  }

  @Test
  public void testParsePeriodWindowSubSecondOriginsFlooredToSecond() {
    // Every origin form - number literal, datetime string, now() (which the parser folds into a
    // literal) - is floored down to a whole second, so the window grid never starts mid-second.
    String sqlLiteral =
        "create stream s1 "
            + "period(period_time => 1h, origin => 1234567890123) "
            + "into testdb.table2(time, s1) "
            + "select ${start_time}, count(*) from testtb.t1";
    PeriodEventWindow window =
        (PeriodEventWindow) analyzeCreateStream(sqlLiteral, "ms").getEventWindow();
    assertEquals(1234567890000L, window.getOrigin().getParsedValue());

    String sqlString =
        "create stream s1 "
            + "period(period_time => 1h, origin => '2026-06-16T01:54:24.724Z') "
            + "into testdb.table2(time, s1) "
            + "select ${start_time}, count(*) from testtb.t1";
    window = (PeriodEventWindow) analyzeCreateStream(sqlString, "ms").getEventWindow();
    assertEquals(1781574864000L, window.getOrigin().getParsedValue());
  }

  @Test
  public void testParseTumbleWindowNowOriginIsRawCurrentTime() {
    String sql =
        "create stream s1 from testdb.table1 "
            + "tumble(size => 1h, origin => now()) "
            + "into testdb.table2(time, s1) "
            + "select ${start_time}, count(*) from ${rows}";

    // currentTime() returns the configured precision's units, so the bounds must be sampled
    // under the same ns precision the analysis runs in (analyzeCreateStream's own save/restore
    // nests safely inside this try).
    String originalPrecision = CommonDescriptor.getInstance().getConfig().getTimestampPrecision();
    try {
      CommonDescriptor.getInstance().getConfig().setTimestampPrecision("ns");
      long before = CommonDateTimeUtils.currentTime();
      TumbleEventWindow window =
          (TumbleEventWindow) analyzeCreateStream(sql, "ns").getEventWindow();
      long after = CommonDateTimeUtils.currentTime();

      // Requirement: a TUMBLE NOW() origin is the raw current system time - unlike PERIOD it is
      // NOT aligned to the second. The general expression path folds now() into a parse-time
      // literal of the current time (currPrecision units), which is exactly the raw semantics. In
      // ns precision the sub-second part is effectively never zero.
      assertNotNull(window.getOrigin());
      long origin = window.getOrigin().getParsedValue();
      assertNotEquals(0L, origin % 1_000_000_000L);
      assertTrue(origin >= before - 1_000_000_000L);
      assertTrue(origin <= after + 1_000_000_000L);
    } finally {
      CommonDescriptor.getInstance().getConfig().setTimestampPrecision(originalPrecision);
    }
  }

  @Test
  public void testParseTumbleWindowOmittedOriginDefaultsToZero() {
    String sql =
        "create stream s1 from testdb.table1 "
            + "tumble(size => 1h) "
            + "into testdb.table2(time, s1) "
            + "select ${start_time}, count(*) from ${rows}";

    // Requirement: an omitted TUMBLE origin defaults to 0 (the epoch), resolved at CREATE STREAM
    // analysis time - unlike PERIOD, which defaults to the current system time aligned to the
    // second.
    TumbleEventWindow window = (TumbleEventWindow) analyzeCreateStream(sql, "ms").getEventWindow();
    assertNotNull(window.getOrigin());
    assertEquals(0L, window.getOrigin().getParsedValue());
  }

  @Test
  public void testParsePeriodWindowMsUnitError() {
    String sql =
        "create stream s1 "
            + "period(period_time => 1s 100ms) "
            + "into testdb.table2(time, s1) "
            + "select ${start_time}, count(*) from testtb.t1";
    String errMsg = "only supports duration units s/m/h/d/w/mo";
    testAnalyzeError(sql, errMsg);
  }

  @Test
  public void testParsePeriodWindowNonPositivePeriodError() {
    String sql =
        "create stream s1 "
            + "period(period_time => 0s) "
            + "into testdb.table2(time, s1) "
            + "select ${start_time}, count(*) from testtb.t1";
    String errMsg = "The period_time of the Period event window must be positive";
    testAnalyzeError(sql, errMsg);
  }

  @Test
  public void testParsePeriodWindowExactOneSecondInMsUnitError() {
    String sql =
        "create stream s1 "
            + "period(period_time => 1000ms, origin => 2000-01-01T00:00:00) "
            + "into testdb.table2(time, s1) "
            + "select ${start_time}, count(*) from testtb.t1";
    testAnalyzeError(sql, "only supports duration units s/m/h/d/w/mo");
  }

  @Test
  public void testParseStreamSourceSystemTableError() {
    String sql =
        "create stream s1 from information_schema.tables "
            + "tumble(size => 1h, origin => 2000-01-01T00:00:00) "
            + "into testdb.table2(time, s1) "
            + "select ${start_time}, count(*) from information_schema.tables";
    String errMsg = "The source table of the stream cannot be a system table or a view";
    testAnalyzeError(sql, errMsg);
  }

  @Test
  public void testParseStreamSourceTreeViewError() {
    String sql =
        "create stream s1 from tree_view.\"root.test.device_view\" "
            + "tumble(size => 1h, origin => 2000-01-01T00:00:00) "
            + "into testdb.table2(time, s1) "
            + "select ${start_time}, count(*) from tree_view.\"root.test.device_view\"";
    String errMsg = "The source table of the stream cannot be a system table or a view";
    testAnalyzeError(sql, errMsg);
  }

  @Test
  public void testPeriodWindowWithConfiguredTimestampPrecision() {
    String sql =
        "create stream s1 "
            + "period(period_time => 1h, origin => '1970-01-01T00:00:01Z') "
            + "into testdb.table2(time, s1) "
            + "select ${start_time}, count(*) from testtb.t1";

    PeriodEventWindow msWindow =
        (PeriodEventWindow) analyzeCreateStream(sql, "ms").getEventWindow();
    PeriodEventWindow nsWindow =
        (PeriodEventWindow) analyzeCreateStream(sql, "ns").getEventWindow();

    long msPeriod = msWindow.getPeriod().getValue().getTotalDuration(TimeUnit.MILLISECONDS);
    long nsPeriod = nsWindow.getPeriod().getValue().getTotalDuration(TimeUnit.NANOSECONDS);
    long msOrigin = msWindow.getOrigin().getParsedValue();
    long nsOrigin = nsWindow.getOrigin().getParsedValue();

    assertEquals(3_600_000L, msPeriod);
    assertEquals(1_000L, msOrigin);
    assertEquals(3_600_000_000_000L, nsPeriod);
    assertEquals(1_000_000_000L, nsOrigin);
    assertNotEquals(msPeriod, nsPeriod);
    assertNotEquals(msOrigin, nsOrigin);
  }

  @Test
  public void testAnalyzePeriodWindowWithPartitionByError() {
    String sql =
        "create stream s1 from testdb.table1 partition by tag1 "
            + "period(period_time => 1h, origin => 2000-01-01T00:00:00) "
            + "into testdb.table2(time, s1) "
            + "select ${start_time}, count(*) from testdb.table1";
    testAnalyzeError(sql, "Period event window cannot be used with FROM clause");
  }

  @Test
  public void testParseTumbleWindowMissingArgumentError() {
    String sql =
        "create stream s1 from testdb.table1 "
            + "Tumble(origin => 2000-01-01T00:00:00) "
            + "into testdb.table2(time, s1) "
            + "select ${start_time}, count(*) from ${rows}";
    String errMsg = "Tumble event window requires 'size' argument";
    testAnalyzeError(sql, errMsg);
  }

  @Test
  public void testParseTumbleWindowNonPositiveSizeError() {
    String sql =
        "create stream s1 from testdb.table1 "
            + "tumble(size => 0s, origin => 2000-01-01T00:00:00) "
            + "into testdb.table2(time, s1) "
            + "select ${start_time}, count(*) from ${rows}";
    String errMsg = "The size of the Tumble event window must be positive";
    testAnalyzeError(sql, errMsg);
  }

  @Test
  public void testParseTumbleWindowDuplicateArgumentError() {
    String sql =
        "create stream s1 from testdb.table1 "
            + "Tumble(size => 1h, size => 1h, origin => 2000-01-01T00:00:00) "
            + "into testdb.table2(time, s1) "
            + "select ${start_time}, count(*) from ${rows}";
    String errMsg = "Duplicate argument name: 'size'";
    testAnalyzeError(sql, errMsg);
  }

  @Test
  public void testParseTumbleWindowExceedingArgs() {
    String sql =
        "create stream s1 from testdb.table1 "
            + "tumble(1h, 2000-01-01T00:00:00, 5) "
            + "into testdb.table2(time, s1) "
            + "select ${start_time}, count(*) from ${rows}";
    testAnalyzeError(sql, "Exceeding arguments provided");
  }

  @Test
  public void testParseTumbleWindowArgsPassedByPosition() {
    String sql =
        "create stream s1 from testdb.table1 "
            + "tumble(1h, 2000-01-01T00:00:00) "
            + "into testdb.table2(time, s1) "
            + "select ${start_time}, count(*) from ${rows}";
    testAnalyzeSuccess(sql);
  }

  @Test
  public void testParseTumbleWindowStringOrigin() {
    String sql =
        "create stream s1 from testdb.table1 "
            + "tumble(size => 1h, origin => '2026-06-16T01:54:24.724Z') "
            + "into testdb.table2(time, s1) "
            + "select ${start_time}, count(*) from ${rows}";
    testAnalyzeSuccess(sql);
  }

  @Test
  public void testTumbleWindowWithConfiguredTimestampPrecision() {
    String sql =
        "create stream s1 from testdb.table1 "
            + "tumble(size => 1h, origin => '1970-01-01T00:00:01Z', time => c1) "
            + "into testdb.table2(time, s1) "
            + "select ${start_time}, count(*) from ${rows}";

    TumbleEventWindow msWindow =
        (TumbleEventWindow) analyzeCreateStream(sql, "ms").getEventWindow();
    TumbleEventWindow nsWindow =
        (TumbleEventWindow) analyzeCreateStream(sql, "ns").getEventWindow();

    long msSize = msWindow.getSize().getValue().getTotalDuration(TimeUnit.MILLISECONDS);
    long nsSize = nsWindow.getSize().getValue().getTotalDuration(TimeUnit.NANOSECONDS);
    long msOrigin = msWindow.getOrigin().getParsedValue();
    long nsOrigin = nsWindow.getOrigin().getParsedValue();

    assertEquals(3_600_000L, msSize);
    assertEquals(1_000L, msOrigin);
    assertEquals(3_600_000_000_000L, nsSize);
    assertEquals(1_000_000_000L, nsOrigin);
    assertNotEquals(msSize, nsSize);
    assertNotEquals(msOrigin, nsOrigin);
  }

  @Test
  public void testAnalyzeRowsCanReferenceSourceTableColumn() {
    String sql =
        "create stream s1 from testdb.table1 "
            + "tumble(size => 1h, origin => 2000-01-01T00:00:00) "
            + "into testdb.table2(time, s3, s1) "
            + "select ${start_time}, sin(${row_num}), s1 from ${rows}";
    testAnalyzeSuccess(sql);
  }

  @Test
  public void testAnalyzeVariationPlaceholderTypes() {
    String sql =
        "create stream s1 from testdb.table1 partition by tag1, tag2, tag3 "
            + "variation(col => s1, delta => 1.0) "
            + "into testdb.table2(time, s1, tag1, tag2, tag3, s2) "
            + "select ${start_time}, ${prev_value}, ${1}, ${2}, ${3}, ${current_value} from ${rows}";
    testAnalyzeSuccess(sql);
  }

  @Test
  public void testAnalyzeStreamSinkMissingTimeColumnError() {
    String sql =
        "create stream s1 from testdb.table1 partition by tag1 "
            + "tumble(size => 1m) "
            + "into testdb.table2(s1) "
            + "select s1 from ${rows}";
    testAnalyzeError(sql, "time column can not be null");
  }

  @Test
  public void testAnalyzeStreamSinkMissingFieldColumnError() {
    String sql =
        "create stream s1 from testdb.table1 partition by tag1 "
            + "tumble(size => 1m) "
            + "into testdb.table2(time) "
            + "select ${start_time} from ${rows}";
    testAnalyzeError(sql, "No Field column present");
  }

  @Test
  public void testAnalyzeStreamSinkSameAsSourceTableError() {
    String sql =
        "create stream s1 from testdb.table1 "
            + "tumble(size => 1h, origin => 2000-01-01T00:00:00) "
            + "into testdb.table1(time, s1) "
            + "select ${start_time}, count(*) from ${rows}";
    testAnalyzeError(sql, "Sink table must be different from the source table");
  }

  @Test
  public void testAnalyzeStreamSinkSpecifiedColumnCountMismatchError() {
    String sql =
        "create stream s1 from testdb.table1 "
            + "tumble(size => 1h, origin => 2000-01-01T00:00:00) "
            + "into testdb.table2(time, s1) "
            + "select ${start_time}, s1, s2 from ${rows}";
    testAnalyzeError(
        sql,
        "Insert query has mismatched column types: Table: [TIMESTAMP, INT64], Query: [TIMESTAMP, INT64, INT64]");
  }

  @Test
  public void testAnalyzeStreamSinkTypeMismatchError() {
    String sql =
        "create stream s1 from testdb.table1 "
            + "tumble(size => 1h, origin => 2000-01-01T00:00:00) "
            + "into testdb.table2(time, s3) "
            + "select ${start_time}, s1 from ${rows}";
    testAnalyzeError(
        sql,
        "Insert query has mismatched column types: Table: [TIMESTAMP, DOUBLE], Query: [TIMESTAMP, INT64]");
  }

  @Test
  public void testAnalyzeStreamSinkDuplicateColumnError() {
    String sql =
        "create stream s1 from testdb.table1 "
            + "tumble(size => 1h, origin => 2000-01-01T00:00:00) "
            + "into testdb.table2(time, s1, s1) "
            + "select ${start_time}, s1, s1 from ${rows}";
    testAnalyzeError(sql, "Insert contains duplicated column name 's1'");
  }

  @Test
  public void testAnalyzePrevValueRequiresVariationWindow() {
    String sql =
        "create stream s1 from testdb.table1 "
            + "tumble(size => 1h, origin => 2000-01-01T00:00:00) "
            + "into testdb.table2(time, s1) "
            + "select ${prev_value} from ${rows}";
    testAnalyzeError(sql, "Placeholder ${prev_value} can only be used with variation event window");
  }

  @Test
  public void testAnalyzeIndexedPlaceholderOutOfRange() {
    String sql =
        "create stream s1 from testdb.table1 partition by tag1, tag2, tag3 "
            + "variation(col => s1, delta => 1.0) "
            + "into testdb.table2(s1, tag1, tag2, tag3, s2) "
            + "select ${prev_value}, ${1}, ${2}, ${3}, ${4} from ${rows}";
    testAnalyzeError(sql, "N must be between 1 and partition by size 3 (inclusive), but got 4");
  }

  @Test
  public void testParseCapacityWindowInvalidArgumentError() {
    String sql =
        "create stream s1 from testdb.table1 "
            + "capacity(size => 10, columns => (1, 2)) "
            + "into testdb.table2(time, s1) "
            + "select ${start_time}, count(*) from ${rows}";
    String errMsg = "Invalid argument type for capacity event window";
    testAnalyzeError(sql, errMsg);
  }

  @Test
  public void testParseCapacityWindowExceedingArgs() {
    String sql =
        "create stream s1 from testdb.table1 "
            + "capacity(10, (c1, c2), 3) "
            + "into testdb.table2(time, s1) "
            + "select ${start_time}, count(*) from ${rows}";
    testAnalyzeError(sql, "Exceeding arguments provided");
  }

  @Test
  public void testParseCapacityWindowArgsPassedByPosition() {
    String sql =
        "create stream s1 from testdb.table1 "
            + "capacity(10, (c1, c2)) "
            + "into testdb.table2(time, s1) "
            + "select ${start_time}, count(*) from ${rows}";
    testAnalyzeSuccess(sql);
  }

  @Test
  public void testParseCapacityWindowStringColumns() {
    String sql =
        "create stream s1 from testdb.table1 "
            + "capacity(size => 10, columns => ('c1', 'c2')) "
            + "into testdb.table2(time, s1) "
            + "select ${start_time}, count(*) from ${rows}";
    testAnalyzeSuccess(sql);
  }

  @Test
  public void testParseUnknownWindowError() {
    String sql =
        "create stream s1 from testdb.table1 "
            + "count(size => 10) "
            + "into testdb.table2(time, s1) "
            + "select ${start_time}, count(*) from ${rows}";
    String errMsg = "Unknown stream event window";
    testParseError(sql, errMsg);
  }

  @Test
  public void testPeriodWindowWithFromClauseError() {
    String sql =
        "create stream s1 from testdb.table1 "
            + "period(period_time => 1h, origin => 2000-01-01T00:00:00) "
            + "into testdb.table2(time, s1) "
            + "select ${start_time}, count(*) from testdb.table1";
    testAnalyzeError(sql, "Period event window cannot be used with FROM clause");
  }

  @Test
  public void testTumbleWindowWithoutFromClauseError() {
    String sql =
        "create stream s1 "
            + "tumble(size => 1h, origin => 2000-01-01T00:00:00) "
            + "into testdb.table2(time, s1) "
            + "select ${start_time}, count(*) from ${rows}";
    testAnalyzeError(sql, "Tumble event window must be used with FROM clause");
  }

  @Test
  public void testPeriodWindowMonthNonMonthMixingError() {
    String sql =
        "create stream s1 "
            + "period(period_time => 1mo 1d, origin => 2000-01-01T00:00:00) "
            + "into testdb.table2(time, s1) "
            + "select ${start_time}, count(*)";
    testAnalyzeError(
        sql, "Simultaneous setting of monthly and non-monthly intervals is not supported");
  }

  @Test
  public void testPeriodWindowDurationUnits() {
    String sqlTemplate =
        "create stream s1 "
            + "period(period_time => %s, origin => 2000-01-01T00:00:00) "
            + "into testdb.table2(time, s1) "
            + "select ${start_time}, count(*)";
    for (String duration : new String[] {"1s", "1m", "1h", "1d", "1w", "1mo"}) {
      testAnalyzeSuccess(String.format(sqlTemplate, duration));
    }

    String errMsg =
        "Event window period argument period_time only supports duration units s/m/h/d/w/mo";
    for (String duration : new String[] {"1ns", "1us", "1ms", "1y"}) {
      testAnalyzeError(String.format(sqlTemplate, duration), errMsg);
    }
  }

  @Test
  public void testTumbleWindowDurationUnits() {
    String sqlTemplate =
        "create stream s1 from testdb.table1 "
            + "tumble(size => %s, origin => 2000-01-01T00:00:00) "
            + "into testdb.table2(time, s1) "
            + "select ${start_time}, count(*) from ${rows}";
    for (String duration :
        new String[] {"1000000ns", "1000us", "1ms", "1s", "1m", "1h", "1d", "1w"}) {
      testAnalyzeSuccess(String.format(sqlTemplate, duration));
    }

    String errMsg =
        "Event window tumble argument size only supports duration units ns/us/ms/s/m/h/d/w";
    for (String duration : new String[] {"1mo", "1y"}) {
      testAnalyzeError(String.format(sqlTemplate, duration), errMsg);
    }
  }

  @Test
  public void testTumbleWindowSizeMonthUnitError() {
    String sql =
        "create stream s1 from testdb.table1 "
            + "tumble(size => 1mo, origin => 2000-01-01T00:00:00) "
            + "into testdb.table2(time, s1) "
            + "select ${start_time}, count(*) from ${rows}";
    testAnalyzeError(sql, "only supports duration units ns/us/ms/s/m/h/d/w");
  }

  @Test
  public void testTumbleWindowSizeYearUnitError() {
    String sql =
        "create stream s1 from testdb.table1 "
            + "tumble(size => 1y, origin => 2000-01-01T00:00:00) "
            + "into testdb.table2(time, s1) "
            + "select ${start_time}, count(*) from ${rows}";
    testAnalyzeError(sql, "only supports duration units ns/us/ms/s/m/h/d/w");
  }

  @Test
  public void testTumbleWindowSizeMonthNonMonthMixingError() {
    String sql =
        "create stream s1 from testdb.table1 "
            + "tumble(size => 1mo 1d, origin => 2000-01-01T00:00:00) "
            + "into testdb.table2(time, s1) "
            + "select ${start_time}, count(*) from ${rows}";
    testAnalyzeError(sql, "only supports duration units ns/us/ms/s/m/h/d/w");
  }

  @Test
  public void testPeriodWindowWithRowNumPlaceholderError() {
    String sql =
        "create stream s1 "
            + "period(period_time => 1h, origin => 2000-01-01T00:00:00) "
            + "into testdb.table2(time, s1) "
            + "select ${start_time}, ${row_num}";
    testAnalyzeError(sql, "Placeholder ${row_num} cannot be used with period event window");
  }

  @Test
  public void testPeriodWindowWithNPlaceholderError() {
    String sql =
        "create stream s1 "
            + "period(period_time => 1h, origin => 2000-01-01T00:00:00) "
            + "into testdb.table2(time, s1) "
            + "select ${start_time}, ${1}";
    testAnalyzeError(sql, "Placeholder ${n} cannot be used with period event window");
  }

  @Test
  public void testPartitionByWithOrdinalPositionError() {
    String sql =
        "create stream s1 from testdb.table1 partition by 1 "
            + "tumble(size => 1h, origin => 2000-01-01T00:00:00) "
            + "into testdb.table2(time, s1) "
            + "select ${start_time}, s1 from ${rows}";
    testAnalyzeError(sql, "PARTITION BY must not be ordinal positions");
  }

  @Test
  public void testStreamSourcePreFilterError() {
    // the stream source does not support a pre-filter yet
    String sql =
        "create stream s1 from testdb.table1 where s1 > 0 "
            + "tumble(size => 1h, origin => 2000-01-01T00:00:00) "
            + "into testdb.table2(time, s1) "
            + "select ${start_time}, s1 from ${rows}";
    testAnalyzeError(sql, "PreFilter clause of the stream source is not supported yet");
  }

  @Test
  public void testStreamLiteralInNormalQueryError() {
    // Test that stream placeholders cannot be used in normal (non-stream) queries
    String sql = "select ${start_time}, count(*) from testdb.table1";
    testAnalyzeError(
        sql,
        "Stream placeholder ${start_time} can only be used in stream queries with event window");
  }

  @Test
  public void testPartitionByWithNonTagColumnError() {
    // Test that PARTITION BY must use TAG columns, not ATTRIBUTE columns
    String sql =
        "create stream s1 from testdb.table1 partition by attr1 "
            + "tumble(size => 1h, origin => 2000-01-01T00:00:00) "
            + "into testdb.table2(time, s1) "
            + "select ${start_time}, s1 from ${rows}";
    testAnalyzeError(sql, "PARTITION BY column must be TAG column, but attr1 is ATTRIBUTE column");
  }

  @Test
  public void testCalcPlanWithAttributeColumnInSelectError() {
    // Test that the calculation plan with ${rows} must not reference ATTRIBUTE columns
    String sql =
        "create stream s1 from testdb.table1 "
            + "tumble(size => 1h, origin => 2000-01-01T00:00:00) "
            + "into testdb.table2(time, attr1, s1) "
            + "select ${start_time}, attr1, s1 from ${rows} t1";
    testAnalyzeError(
        sql, "The calculation plan with ${rows} placeholder must not reference ATTRIBUTE columns");
  }

  @Test
  public void testCalcPlanWithAttributeColumnInWhereError() {
    // Test that the calculation plan with ${rows} must not reference ATTRIBUTE columns
    String sql =
        "create stream s1 from testdb.table1 "
            + "tumble(size => 1h, origin => 2000-01-01T00:00:00) "
            + "into testdb.table2(time, s1) "
            + "select ${start_time}, s1 from ${rows} t1 where t1.attr2 = 'x'";
    testAnalyzeError(
        sql, "The calculation plan with ${rows} placeholder must not reference ATTRIBUTE columns");
  }

  @Test
  public void testCalcPlanWithAttributeColumnSelectStarFromRowsPlaceholder() {
    String sql =
        "create stream s1 from testdb.table1 "
            + "tumble(size => 1h, origin => 2000-01-01T00:00:00) "
            + "into testdb.table2(time, tag1, tag2, tag3, s1, s2, s3) "
            + "select * from ${rows}";
    testAnalyzeError(
        sql, "The calculation plan with ${rows} placeholder must not reference ATTRIBUTE columns");
  }

  @Test
  public void testCalcPlanWithAttributeColumnInGroupByError() {
    String sql =
        "create stream s1 from testdb.table1 "
            + "tumble(size => 1h, origin => 2000-01-01T00:00:00) "
            + "into testdb.table2(time, s1) "
            + "select ${start_time}, count(*) from ${rows} group by attr1";
    testAnalyzeError(
        sql, "The calculation plan with ${rows} placeholder must not reference ATTRIBUTE columns");
  }

  @Test
  public void testCalcPlanWithAttributeColumnInOrderByError() {
    String sql =
        "create stream s1 from testdb.table1 "
            + "tumble(size => 1h, origin => 2000-01-01T00:00:00) "
            + "into testdb.table2(time, s1) "
            + "select ${start_time}, s1 from ${rows} order by attr1";
    testAnalyzeError(
        sql, "The calculation plan with ${rows} placeholder must not reference ATTRIBUTE columns");
  }

  @Test
  public void testCalcPlanWithAttributeColumnInJoinOnError() {
    String sql =
        "create stream s1 from testdb.table1 "
            + "tumble(size => 1h, origin => 2000-01-01T00:00:00) "
            + "into testdb.table2(time, s1) "
            + "select ${start_time}, s1 from ${rows} t1 "
            + "where t1.tag1 in (select t2.attr1 from testdb.table2 t2)";
    testAnalyzeError(
        sql, "The calculation plan with ${rows} placeholder must not reference ATTRIBUTE columns");
  }

  @Test
  public void testRowsPlaceholderInJoinError() {
    String sql =
        "create stream s1 from testdb.table1 "
            + "tumble(size => 1h, origin => 2000-01-01T00:00:00) "
            + "into testdb.table2(time, s1) "
            + "select ${start_time}, t1.s1 from ${rows} t1 join testdb.table2 t2 on t1.attr1 = t2.attr1";
    testAnalyzeError(sql, "The ${rows} placeholder cannot participate in a JOIN operation");
  }

  @Test
  public void testRowsPlaceholderInImplicitJoinError() {
    String sql =
        "create stream s1 from testdb.table1 "
            + "tumble(size => 1h, origin => 2000-01-01T00:00:00) "
            + "into testdb.table2(time, s1) "
            + "select ${start_time}, t1.s1 from ${rows} t1, ${rows} t2 "
            + "where t1.tag1 = t2.tag1";
    testAnalyzeError(sql, "The ${rows} placeholder cannot participate in a JOIN operation");
  }

  @Test
  public void testCalcPlanWithAttributeColumnOnRowsPlaceholder() {
    // ATTRIBUTE columns may still be referenced when the calculation plan does not use ${rows}
    String sql =
        "create stream s1 from testdb.table1 "
            + "tumble(size => 1h, origin => 2000-01-01T00:00:00) "
            + "into testdb.table2(time, s1) "
            + "select ${start_time}, last(s1) from testdb.table1 where attr1 = 'x'";
    testAnalyzeSuccess(sql);
  }

  @Test
  public void testAnalyzeStreamPropertiesSuccess() {
    // watermark/allowed_lateness accept a duration or an integer; max_delay_window_close and
    // max_idle_window_retain must be whole seconds of at least 5 seconds (5s is the boundary)
    String sql =
        "create stream s1 from testdb.table1 "
            + "tumble(size => 1h, origin => 2000-01-01T00:00:00) "
            + "into testdb.table2(time, s1) "
            + "select ${start_time}, count(*) from ${rows} "
            + "with (watermark = 10s, allowed_lateness = 500, max_delay_window_close = 1h, "
            + "max_idle_window_retain = 5s, left_closed = true)";
    testAnalyzeSuccess(sql);
  }

  @Test
  public void testAnalyzeStreamPropertyDurationUnitsSuccess() {
    String sqlPrefix =
        "create stream s1 from testdb.table1 "
            + "tumble(size => 1h, origin => 2000-01-01T00:00:00) "
            + "into testdb.table2(time, s1) "
            + "select ${start_time}, count(*) from ${rows} ";
    testAnalyzeSuccess(sqlPrefix + "with (watermark = 1d, allowed_lateness = 30m)");
    testAnalyzeSuccess(sqlPrefix + "with (watermark = 1day, allowed_lateness = 30minute)");
    testAnalyzeSuccess(sqlPrefix + "with (watermark = 1000000ns, allowed_lateness = 1000us)");
    testAnalyzeSuccess(
        sqlPrefix + "with (max_delay_window_close = 5s, max_idle_window_retain = 1d)");
  }

  @Test
  public void testAnalyzeStreamPropertyNonDurationValueError() {
    String sqlPrefix =
        "create stream s1 from testdb.table1 "
            + "tumble(size => 1h, origin => 2000-01-01T00:00:00) "
            + "into testdb.table2(time, s1) "
            + "select ${start_time}, count(*) from ${rows} ";
    String errMsg = "Stream property watermark must be a time duration";
    testAnalyzeError(sqlPrefix + "with (watermark = 'abc')", errMsg);
    testAnalyzeError(sqlPrefix + "with (watermark = 1.5)", errMsg);
  }

  @Test
  public void testAnalyzeStreamPropertyUnsupportedDurationUnitError() {
    String sqlPrefix =
        "create stream s1 from testdb.table1 "
            + "tumble(size => 1h, origin => 2000-01-01T00:00:00) "
            + "into testdb.table2(time, s1) "
            + "select ${start_time}, count(*) from ${rows} ";
    String highPrecisionErrMsg = "only supports duration units ns/us/ms/s/m/h/d";
    String millisecondErrMsg = "only supports duration units s/m/h/d";

    testAnalyzeError(sqlPrefix + "with (watermark = 1mo)", highPrecisionErrMsg);
    testAnalyzeError(sqlPrefix + "with (allowed_lateness = 1y)", highPrecisionErrMsg);
    testAnalyzeError(sqlPrefix + "with (max_delay_window_close = 1ns)", millisecondErrMsg);
    testAnalyzeError(sqlPrefix + "with (max_idle_window_retain = 1us)", millisecondErrMsg);
  }

  @Test
  public void testAnalyzeUnknownStreamPropertyError() {
    String sqlPrefix =
        "create stream s1 from testdb.table1 "
            + "tumble(size => 1h, origin => 2000-01-01T00:00:00) "
            + "into testdb.table2(time, s1) "
            + "select ${start_time}, count(*) from ${rows} ";

    testAnalyzeError(sqlPrefix + "with (foo = 1)", "Unrecognized stream property: foo");
    testAnalyzeError(sqlPrefix + "with (watermak = 2m)", "Unrecognized stream property: watermak");
  }

  @Test
  public void testAnalyzeStreamPropertyMonthUnitError() {
    String sql =
        "create stream s1 from testdb.table1 "
            + "tumble(size => 1h, origin => 2000-01-01T00:00:00) "
            + "into testdb.table2(time, s1) "
            + "select ${start_time}, count(*) from ${rows} "
            + "with (watermark = 1mo 1d)";
    String errMsg = "only supports duration units ns/us/ms/s/m/h/d";
    testAnalyzeError(sql, errMsg);
  }

  @Test
  public void testAnalyzeStreamPropertyMaxDelayWindowCloseTooShortError() {
    // a bare integer is interpreted in the current timestamp precision (ms)
    String sql =
        "create stream s1 from testdb.table1 "
            + "tumble(size => 1h, origin => 2000-01-01T00:00:00) "
            + "into testdb.table2(time, s1) "
            + "select ${start_time}, count(*) from ${rows} "
            + "with (max_delay_window_close = 3000)";
    String errMsg = "Stream property max_delay_window_close must be at least 5 seconds";
    testAnalyzeError(sql, errMsg);
  }

  @Test
  public void testAnalyzeStreamPropertyMaxDelayWindowCloseMsUnitError() {
    String sql =
        "create stream s1 from testdb.table1 "
            + "tumble(size => 1h, origin => 2000-01-01T00:00:00) "
            + "into testdb.table2(time, s1) "
            + "select ${start_time}, count(*) from ${rows} "
            + "with (max_delay_window_close = 5s 500ms)";
    String errMsg = "only supports duration units s/m/h/d";
    testAnalyzeError(sql, errMsg);
  }

  @Test
  public void testAnalyzeStreamPropertyMaxIdleWindowRetainTooShortError() {
    String sql =
        "create stream s1 from testdb.table1 "
            + "tumble(size => 1h, origin => 2000-01-01T00:00:00) "
            + "into testdb.table2(time, s1) "
            + "select ${start_time}, count(*) from ${rows} "
            + "with (max_idle_window_retain = 4s)";
    testAnalyzeError(sql, "Stream property max_idle_window_retain must be at least 5 seconds");
  }

  @Test
  public void testAnalyzeStreamPropertyLeftClosedMustBeBooleanError() {
    String sql =
        "create stream s1 from testdb.table1 "
            + "tumble(size => 1h, origin => 2000-01-01T00:00:00) "
            + "into testdb.table2(time, s1) "
            + "select ${start_time}, count(*) from ${rows} "
            + "with (left_closed = 1)";
    String errMsg = "Stream property left_closed must be a boolean";
    testAnalyzeError(sql, errMsg);
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
      future.get(5, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      fail("Interrupted");
    } catch (ExecutionException e) {
      if (e.getCause() instanceof ParsingException) {
        assertTrue(e.getCause().getMessage().contains(errMsg));
      } else {
        fail("Unexpected exception: " + e.getCause());
      }
    } catch (TimeoutException e) {
      fail("Timed out - potential endless loop/OOM detected");
    } finally {
      executor.shutdownNow();
    }
  }

  private void testAnalyzeError(String sql, String errMsg) {
    MPPQueryContext context = new MPPQueryContext("", queryId, sessionInfo, null, null);
    SqlParser sqlParser = new SqlParser();
    Analyzer analyzer = createAnalyzer(metadata, context, sqlParser, sessionInfo);

    ExecutorService executor = Executors.newSingleThreadExecutor();
    Future<?> future =
        executor.submit(
            () -> {
              try {
                Statement statement = sqlParser.createStatement(sql, ZoneId.systemDefault(), null);
                analyzer.analyze(statement);
                fail("Expected SemanticException to be thrown");
              } catch (SemanticException e) {
                assertTrue(e.getMessage().contains(errMsg));
              }
            });

    try {
      future.get(5, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      fail("Interrupted");
    } catch (ExecutionException e) {
      if (e.getCause() instanceof SemanticException) {
        assertTrue(e.getCause().getMessage().contains(errMsg));
      } else {
        fail("Unexpected exception: " + e.getCause());
      }
    } catch (TimeoutException e) {
      fail("Timed out - potential endless loop/OOM detected");
    } finally {
      executor.shutdownNow();
    }
  }

  private void testAnalyzeSuccess(String sql) {
    MPPQueryContext context = new MPPQueryContext("", queryId, sessionInfo, null, null);
    SqlParser sqlParser = new SqlParser();
    Analyzer analyzer = createAnalyzer(metadata, context, sqlParser, sessionInfo);

    ExecutorService executor = Executors.newSingleThreadExecutor();
    Future<?> future =
        executor.submit(
            () -> {
              try {
                Statement statement = sqlParser.createStatement(sql, ZoneId.systemDefault(), null);
                analyzer.analyze(statement);
              } catch (ParsingException e) {
                fail("Unexpected ParsingException to be thrown");
              }
            });

    try {
      future.get(5, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      fail("Interrupted");
    } catch (ExecutionException e) {
      fail("Unexpected exception: " + e.getCause());
    } catch (TimeoutException e) {
      fail("Timed out - potential endless loop/OOM detected");
    } finally {
      executor.shutdownNow();
    }
  }

  private CreateStream analyzeCreateStream(String sql, String timestampPrecision) {
    String originalPrecision = CommonDescriptor.getInstance().getConfig().getTimestampPrecision();
    try {
      CommonDescriptor.getInstance().getConfig().setTimestampPrecision(timestampPrecision);
      MPPQueryContext context = new MPPQueryContext("", queryId, sessionInfo, null, null);
      SqlParser sqlParser = new SqlParser();
      Analyzer analyzer = createAnalyzer(metadata, context, sqlParser, sessionInfo);
      Statement statement = sqlParser.createStatement(sql, ZoneId.systemDefault(), null);
      Analysis analysis = analyzer.analyze(statement);
      return (CreateStream) analysis.getStatement();
    } finally {
      CommonDescriptor.getInstance().getConfig().setTimestampPrecision(originalPrecision);
    }
  }

  public static Analyzer createAnalyzer(
      final Metadata metadata,
      final MPPQueryContext context,
      final SqlParser sqlParser,
      final SessionInfo session) {
    final StatementAnalyzerFactory statementAnalyzerFactory =
        new StatementAnalyzerFactory(
            metadata, sqlParser, nopAccessControl, new InternalTypeManager());

    return new Analyzer(
        context,
        session,
        statementAnalyzerFactory,
        Collections.emptyList(),
        Collections.emptyMap(),
        new StatementRewriteFactory().getStatementRewrite(),
        NOOP);
  }
}
