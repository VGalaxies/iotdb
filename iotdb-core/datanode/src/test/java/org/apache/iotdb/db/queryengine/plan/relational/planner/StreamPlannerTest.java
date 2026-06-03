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

package org.apache.iotdb.db.queryengine.plan.relational.planner;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.queryengine.common.SessionInfo;
import org.apache.iotdb.commons.queryengine.common.SqlDialect;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.commons.queryengine.plan.relational.metadata.ColumnSchema;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.EventScanNode;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.stream.CreateStream;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.stream.PlaceHolderLiteral;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.commons.stream.IoTDBSubscriptionSource;
import org.apache.iotdb.commons.stream.IoTDBTarget;
import org.apache.iotdb.commons.stream.StreamTask;
import org.apache.iotdb.db.protocol.session.IClientSession;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.plan.execution.config.IConfigTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.TableConfigTaskVisitor;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.relational.stream.CreateStreamTask;
import org.apache.iotdb.db.queryengine.plan.planner.plan.LogicalQueryPlan;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.Analysis;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.TestMetadata;
import org.apache.iotdb.db.queryengine.plan.relational.planner.ir.ExpressionRewriter;
import org.apache.iotdb.db.queryengine.plan.relational.planner.ir.ExpressionTreeRewriter;
import org.apache.iotdb.db.queryengine.plan.relational.security.AllowAllAccessControl;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.apache.tsfile.enums.TSDataType.INT64;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.same;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class StreamPlannerTest {

  @Test
  public void testCreateStreamLogicalPlanWithRowsContainsEventScanNode() {
    PlanTester planTester = new PlanTester();
    String sql =
        "create stream s_rows from testdb.t1 "
            + "tumble(size => 1h, origin => 2000-01-01T00:00:00) "
            + "into testdb.t2(time, s1) "
            + "select ${start_time}, count(*) from ${rows}";

    LogicalQueryPlan logicalQueryPlan = planTester.createPlan(sql);
    Assert.assertTrue(containsNode(logicalQueryPlan.getRootNode(), EventScanNode.class));
  }

  @Test
  public void testCreateStreamLogicalPlanWithoutRowsContainsNoEventScanNode() {
    PlanTester planTester = new PlanTester();
    String sql =
        "create stream s_session from testdb.t1 "
            + "where s1 > 1 "
            + "tumble(size => 1h, origin => 2000-01-01T00:00:00) "
            + "into testdb.t2(time, s1) "
            + "select time, s1 from testdb.t1";

    LogicalQueryPlan logicalQueryPlan = planTester.createPlan(sql);
    Assert.assertFalse(containsNode(logicalQueryPlan.getRootNode(), EventScanNode.class));
  }

  @Test
  public void testCreateStreamTaskWithoutRowsUsesSessionScanNodeCalcPlan() {
    String sql =
        "create stream s_session from testdb.t1 "
            + "tumble(size => 1h, origin => 2000-01-01T00:00:00) "
            + "into testdb.t2(time, s1) "
            + "select time, s1 from testdb.t1";
    MPPQueryContext context = newTestContext(sql);
    Analysis analysis = PlanTester.analyze(sql, new TestMetadata(), context);
    CreateStream createStream = (CreateStream) analysis.getStatement();

    TableModelPlanner streamQueryPlanner = Mockito.mock(TableModelPlanner.class);
    when(streamQueryPlanner.analyze(context)).thenReturn(analysis);
    when(streamQueryPlanner.getSymbolAllocator()).thenReturn(new SymbolAllocator());

    TableConfigTaskVisitor visitor =
        new TableConfigTaskVisitor(
                Mockito.mock(IClientSession.class),
                new TestMetadata(),
                new AllowAllAccessControl(),
                new org.apache.iotdb.commons.queryengine.plan.relational.type.InternalTypeManager())
            .withStreamQueryPlanner(streamQueryPlanner);

    IConfigTask configTask = visitor.visitCreateStream(createStream, context);
    Assert.assertTrue(configTask instanceof CreateStreamTask);
    CreateStreamTask createStreamTask = (CreateStreamTask) configTask;

    ByteBuffer calcPlan = createStreamTask.getStreamTask().getCalcPlan().duplicate();
    Assert.assertEquals(PlanNodeType.SESSION_SCAN_NODE.getNodeType(), calcPlan.getShort());
    Assert.assertTrue(createStreamTask.getStreamTask().getSubQuery().contains("SELECT"));
    verify(streamQueryPlanner, never()).doLogicalPlan(same(analysis), same(context));
  }

  @Test
  public void testCreateStreamTaskWithRowsUsesLogicalPlannerCalcPlan() {
    String sql =
        "create stream s_rows from testdb.t1 "
            + "tumble(size => 1h, origin => 2000-01-01T00:00:00) "
            + "into testdb.t2(time, s1) "
            + "select ${start_time}, count(*) from ${rows}";
    MPPQueryContext context = newTestContext(sql);
    Analysis analysis = PlanTester.analyze(sql, new TestMetadata(), context);
    CreateStream createStream = (CreateStream) analysis.getStatement();

    TableModelPlanner streamQueryPlanner = Mockito.mock(TableModelPlanner.class);
    when(streamQueryPlanner.analyze(context)).thenReturn(analysis);
    when(streamQueryPlanner.getSymbolAllocator()).thenReturn(new SymbolAllocator());
    Symbol startTime = new Symbol("start_time");
    Symbol rowNum = new Symbol("row_num");
    List<Symbol> outputSymbols = Arrays.asList(startTime, rowNum);
    Map<Symbol, ColumnSchema> assignments = new LinkedHashMap<>();
    assignments.put(
        startTime,
        new ColumnSchema(
            "start_time",
            org.apache.tsfile.read.common.type.TypeFactory.getType(INT64),
            false,
            TsTableColumnCategory.TIME));
    assignments.put(
        rowNum,
        new ColumnSchema(
            "row_num",
            org.apache.tsfile.read.common.type.TypeFactory.getType(INT64),
            false,
            TsTableColumnCategory.FIELD));
    when(streamQueryPlanner.doLogicalPlan(same(analysis), same(context)))
        .thenReturn(
            new LogicalQueryPlan(
                context,
                new EventScanNode(new PlanNodeId("event-scan"), outputSymbols, assignments)));

    TableConfigTaskVisitor visitor =
        new TableConfigTaskVisitor(
                Mockito.mock(IClientSession.class),
                new TestMetadata(),
                new AllowAllAccessControl(),
                new org.apache.iotdb.commons.queryengine.plan.relational.type.InternalTypeManager())
            .withStreamQueryPlanner(streamQueryPlanner);

    IConfigTask configTask = visitor.visitCreateStream(createStream, context);
    Assert.assertTrue(configTask instanceof CreateStreamTask);
    CreateStreamTask createStreamTask = (CreateStreamTask) configTask;

    ByteBuffer calcPlan = createStreamTask.getStreamTask().getCalcPlan().duplicate();
    Assert.assertEquals(PlanNodeType.EVENT_SCAN_NODE.getNodeType(), calcPlan.getShort());
    IoTDBSubscriptionSource source =
        (IoTDBSubscriptionSource) createStreamTask.getStreamTask().getSource();
    Assert.assertEquals(Arrays.asList("start_time", "row_num"), source.getPartitionColumns());
    verify(streamQueryPlanner, times(1)).doLogicalPlan(same(analysis), same(context));
  }

  @Test
  public void testCreateStreamTaskWithRowsUsesPrunedColumnsFromEventScanNodeAfterSerde()
      throws IOException {
    String sql =
        "create stream s_rows from testdb.t1 "
            + "tumble(size => 1h, origin => 2000-01-01T00:00:00) "
            + "into testdb.t2(time, s1) "
            + "select ${start_time}, count(*) from ${rows}";
    MPPQueryContext context = newTestContext(sql);
    Analysis analysis = PlanTester.analyze(sql, new TestMetadata(), context);
    CreateStream createStream = (CreateStream) analysis.getStatement();

    TableModelPlanner streamQueryPlanner = Mockito.mock(TableModelPlanner.class);
    when(streamQueryPlanner.analyze(context)).thenReturn(analysis);
    when(streamQueryPlanner.getSymbolAllocator()).thenReturn(new SymbolAllocator());
    Symbol startTime = new Symbol("start_time");
    Symbol rowNum = new Symbol("row_num");
    List<Symbol> outputSymbols = Arrays.asList(startTime, rowNum);
    Map<Symbol, ColumnSchema> assignments = new LinkedHashMap<>();
    assignments.put(
        startTime,
        new ColumnSchema(
            "start_time",
            org.apache.tsfile.read.common.type.TypeFactory.getType(INT64),
            false,
            TsTableColumnCategory.TIME));
    assignments.put(
        rowNum,
        new ColumnSchema(
            "row_num",
            org.apache.tsfile.read.common.type.TypeFactory.getType(INT64),
            false,
            TsTableColumnCategory.FIELD));
    when(streamQueryPlanner.doLogicalPlan(same(analysis), same(context)))
        .thenReturn(
            new LogicalQueryPlan(
                context,
                new EventScanNode(new PlanNodeId("event-scan-serde"), outputSymbols, assignments)));

    TableConfigTaskVisitor visitor =
        new TableConfigTaskVisitor(
                Mockito.mock(IClientSession.class),
                new TestMetadata(),
                new AllowAllAccessControl(),
                new org.apache.iotdb.commons.queryengine.plan.relational.type.InternalTypeManager())
            .withStreamQueryPlanner(streamQueryPlanner);

    IConfigTask configTask = visitor.visitCreateStream(createStream, context);
    Assert.assertTrue(configTask instanceof CreateStreamTask);
    StreamTask streamTask = ((CreateStreamTask) configTask).getStreamTask();
    IoTDBSubscriptionSource source = (IoTDBSubscriptionSource) streamTask.getSource();
    Assert.assertEquals(Arrays.asList("start_time", "row_num"), source.getPartitionColumns());

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    streamTask.serialize(dos);
    dos.flush();
    StreamTask restored = StreamTask.deserialize(ByteBuffer.wrap(baos.toByteArray()));

    IoTDBSubscriptionSource restoredSource = (IoTDBSubscriptionSource) restored.getSource();
    Assert.assertEquals(
        Arrays.asList("start_time", "row_num"), restoredSource.getPartitionColumns());
  }

  @Test
  public void testExpressionTreeRewriterSupportsPlaceholderLiteral() {
    PlaceHolderLiteral placeholder = new PlaceHolderLiteral(PlaceHolderLiteral.Type.START_TIME);
    PlaceHolderLiteral rewritten =
        ExpressionTreeRewriter.rewriteWith(new ExpressionRewriter<Void>() {}, placeholder);
    Assert.assertSame(placeholder, rewritten);
  }

  @Test
  public void testCreateStreamTaskFromSqlAndStreamTaskSerdeRoundTrip() throws IOException {
    String sql =
        "create stream s_session from testdb.t1 "
            + "tumble(size => 1h, origin => 2000-01-01T00:00:00) "
            + "into testdb.t2(time, s1) "
            + "select time, s1 from testdb.t1";
    MPPQueryContext context = newTestContext(sql);
    Analysis analysis = PlanTester.analyze(sql, new TestMetadata(), context);
    CreateStream createStream = (CreateStream) analysis.getStatement();

    TableModelPlanner streamQueryPlanner = Mockito.mock(TableModelPlanner.class);
    when(streamQueryPlanner.analyze(context)).thenReturn(analysis);
    when(streamQueryPlanner.getSymbolAllocator()).thenReturn(new SymbolAllocator());

    TableConfigTaskVisitor visitor =
        new TableConfigTaskVisitor(
                Mockito.mock(IClientSession.class),
                new TestMetadata(),
                new AllowAllAccessControl(),
                new org.apache.iotdb.commons.queryengine.plan.relational.type.InternalTypeManager())
            .withStreamQueryPlanner(streamQueryPlanner);

    IConfigTask configTask = visitor.visitCreateStream(createStream, context);
    Assert.assertTrue(configTask instanceof CreateStreamTask);
    StreamTask streamTask = ((CreateStreamTask) configTask).getStreamTask();

    Assert.assertEquals("s_session", streamTask.getTaskName());
    Assert.assertEquals("testdb", streamTask.getDatabase());
    Assert.assertNotNull(streamTask.getSource());
    Assert.assertNotNull(streamTask.getWindow());
    Assert.assertNotNull(streamTask.getTarget());
    Assert.assertTrue(streamTask.getSubQuery().contains("SELECT"));
    ByteBuffer originalCalcPlan = streamTask.getCalcPlan().duplicate();
    Assert.assertEquals(PlanNodeType.SESSION_SCAN_NODE.getNodeType(), originalCalcPlan.getShort());

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    streamTask.serialize(dos);
    dos.flush();
    StreamTask restored = StreamTask.deserialize(ByteBuffer.wrap(baos.toByteArray()));

    Assert.assertEquals(streamTask.getTaskName(), restored.getTaskName());
    Assert.assertEquals(streamTask.getDatabase(), restored.getDatabase());
    Assert.assertEquals(streamTask.getCreator(), restored.getCreator());
    Assert.assertEquals(streamTask.getSubQuery(), restored.getSubQuery());
    Assert.assertEquals(streamTask.getEpoch(), restored.getEpoch());
    Assert.assertEquals(streamTask.getWindow().getType(), restored.getWindow().getType());

    IoTDBTarget expectedTarget = (IoTDBTarget) streamTask.getTarget();
    IoTDBTarget actualTarget = (IoTDBTarget) restored.getTarget();
    Assert.assertEquals(expectedTarget.getDatabase(), actualTarget.getDatabase());
    Assert.assertEquals(expectedTarget.getTableName(), actualTarget.getTableName());
    Assert.assertEquals(expectedTarget.getColumnNames(), actualTarget.getColumnNames());

    assertCalcPlanBytesEqual(streamTask.getCalcPlan(), restored.getCalcPlan());
  }

  private static MPPQueryContext newTestContext(String sql) {
    SessionInfo sessionInfo =
        new SessionInfo(
            1L,
            "test_user",
            ZoneId.systemDefault(),
            IoTDBConstant.ClientVersion.V_1_0,
            "testdb",
            SqlDialect.TABLE);
    return new MPPQueryContext(sql, new QueryId("stream_planner_test"), sessionInfo, null, null);
  }

  private static boolean containsNode(PlanNode node, Class<?> clazz) {
    if (clazz.isInstance(node)) {
      return true;
    }
    for (PlanNode child : node.getChildren()) {
      if (containsNode(child, clazz)) {
        return true;
      }
    }
    return false;
  }

  private static void assertCalcPlanBytesEqual(ByteBuffer expected, ByteBuffer actual) {
    ByteBuffer e = expected.duplicate();
    ByteBuffer a = actual.duplicate();
    e.rewind();
    a.rewind();
    Assert.assertEquals(e.remaining(), a.remaining());
    byte[] eb = new byte[e.remaining()];
    byte[] ab = new byte[a.remaining()];
    e.get(eb);
    a.get(ab);
    Assert.assertArrayEquals(eb, ab);
  }
}
