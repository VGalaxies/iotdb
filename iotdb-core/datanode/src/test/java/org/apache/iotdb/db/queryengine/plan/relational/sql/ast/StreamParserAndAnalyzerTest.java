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

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.SemanticException;
import org.apache.iotdb.commons.queryengine.common.SessionInfo;
import org.apache.iotdb.commons.queryengine.common.SqlDialect;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Statement;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.parser.ParsingException;
import org.apache.iotdb.commons.queryengine.plan.relational.type.InternalTypeManager;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.QueryId;
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
        "create stream s1 from testdb.t1 "
            + "period(period => 1h, origin => 2000-01-01T00:00:00) "
            + "into testdb.t2(time, s1) "
            + "select ${curr_time}, count(*) from testdb.t1";
    String errMsg = "mismatched input 'curr_time'";
    testParseError(sql, errMsg);
  }

  @Test
  public void testParseSinkTableError() {
    String sql =
        "create stream s1 from testdb.t1 "
            + "period(period => 1h, origin => 2000-01-01T00:00:00) "
            + "into testdb.t2 "
            + "select ${start_time}, count(*) from testdb.t1";
    String errMsg = "calculation plan has 2 columns, but sink table has 10 columns";
    testAnalyzeError(sql, errMsg);
  }

  @Test
  public void testParsePeriodWindowMissingArgumentError() {
    String sql =
        "create stream s1 from testdb.t1 "
            + "period(origin => 2000-01-01T00:00:00) "
            + "into testdb.t2(time, s1) "
            + "select ${start_time}, count(*) from testtb.t1";
    String errMsg = "Period event window requires 'period' argument";
    testAnalyzeError(sql, errMsg);
  }

  @Test
  public void testParsePeriodWindowInvalidArgumentError() {
    String sql =
        "create stream s1 from testdb.t1 "
            + "period(period => 1h, origin => 'c1') "
            + "into testdb.t2(time, s1) "
            + "select ${start_time}, count(*) from testtb.t1";
    String errMsg = "Invalid argument type for period event window";
    testAnalyzeError(sql, errMsg);
  }

  @Test
  public void testParsePeriodWindowArgsPassedByPosition() {
    String sql =
        "create stream s1 from testdb.t1 "
            + "period(1h, 2000-01-01T00:00:00) "
            + "into testdb.t2(time, s1) "
            + "select ${start_time}, count(*) from testtb.t1";
    testAnalyzeSuccess(sql);
  }

  @Test
  public void testParseTumbleWindowMissingArgumentError() {
    String sql =
        "create stream s1 from testdb.t1 "
            + "Tumble(origin => 2000-01-01T00:00:00) "
            + "into testdb.t2(time, s1) "
            + "select ${start_time}, count(*) from ${rows}";
    String errMsg = "Tumble event window requires 'size' argument";
    testAnalyzeError(sql, errMsg);
  }

  @Test
  public void testParseTumbleWindowDuplicateArgumentError() {
    String sql =
        "create stream s1 from testdb.t1 "
            + "Tumble(size => 1h, size => 1h, origin => 2000-01-01T00:00:00) "
            + "into testdb.t2(time, s1) "
            + "select ${start_time}, count(*) from ${rows}";
    String errMsg = "Duplicate argument name: 'size'";
    testAnalyzeError(sql, errMsg);
  }

  @Test
  public void testParseTumbleWindowExceedingArgs() {
    String sql =
        "create stream s1 from testdb.t1 "
            + "tumble(1h, 2000-01-01T00:00:00, c1, 5) "
            + "into testdb.t2(time, s1) "
            + "select ${start_time}, count(*) from ${rows}";
    testAnalyzeError(sql, "Exceeding arguments provided");
  }

  @Test
  public void testParseTumbleWindowArgsPassedByPosition() {
    String sql =
        "create stream s1 from testdb.t1 "
            + "tumble(1h, 2000-01-01T00:00:00, c1) "
            + "into testdb.t2(time, s1) "
            + "select ${start_time}, count(*) from ${rows}";
    testAnalyzeSuccess(sql);
  }

  @Test
  public void testParseTumbleWindowStringTimeCol() {
    String sql =
        "create stream s1 from testdb.t1 "
            + "tumble(time => 'c1', size => 1h, origin => 2000-01-01T00:00:00) "
            + "into testdb.t2(time, s1) "
            + "select ${start_time}, count(*) from ${rows}";
    testAnalyzeSuccess(sql);
  }

  @Test
  public void testParseCapacityWindowInvalidArgumentError() {
    String sql =
        "create stream s1 from testdb.t1 "
            + "capacity(size => 10, columns => (1, 2)) "
            + "into testdb.t2(time, s1) "
            + "select ${start_time}, count(*) from ${rows}";
    String errMsg = "Invalid argument type for capacity event window";
    testAnalyzeError(sql, errMsg);
  }

  @Test
  public void testParseCapacityWindowExceedingArgs() {
    String sql =
        "create stream s1 from testdb.t1 "
            + "capacity(10, (c1, c2), 3) "
            + "into testdb.t2(time, s1) "
            + "select ${start_time}, count(*) from ${rows}";
    testAnalyzeError(sql, "Exceeding arguments provided");
  }

  @Test
  public void testParseCapacityWindowArgsPassedByPosition() {
    String sql =
        "create stream s1 from testdb.t1 "
            + "capacity(10, (c1, c2)) "
            + "into testdb.t2(time, s1) "
            + "select ${start_time}, count(*) from ${rows}";
    testAnalyzeSuccess(sql);
  }

  @Test
  public void testParseCapacityWindowStringColumns() {
    String sql =
        "create stream s1 from testdb.t1 "
            + "capacity(size => 10, columns => ('c1', 'c2')) "
            + "into testdb.t2(time, s1) "
            + "select ${start_time}, count(*) from ${rows}";
    testAnalyzeSuccess(sql);
  }

  @Test
  public void testParseUnknownWindowError() {
    String sql =
        "create stream s1 from testdb.t1 "
            + "count(size => 10) "
            + "into testdb.t2(time, s1) "
            + "select ${start_time}, count(*) from ${rows}";
    String errMsg = "Unknown stream event window";
    testParseError(sql, errMsg);
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
