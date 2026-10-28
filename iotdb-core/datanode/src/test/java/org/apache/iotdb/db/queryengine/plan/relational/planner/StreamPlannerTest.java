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

import org.apache.iotdb.commons.auth.entity.User;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.SemanticException;
import org.apache.iotdb.commons.queryengine.common.SessionInfo;
import org.apache.iotdb.commons.queryengine.common.SqlDialect;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.commons.queryengine.plan.relational.metadata.ColumnSchema;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.EventScanNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.OutputNode;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.stream.CreateStream;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.stream.StreamLiteral;
import org.apache.iotdb.commons.schema.column.ColumnHeader;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.commons.stream.IoTDBSubscriptionSource;
import org.apache.iotdb.commons.stream.IoTDBTarget;
import org.apache.iotdb.commons.stream.StreamTask;
import org.apache.iotdb.commons.stream.StreamWindow;
import org.apache.iotdb.commons.stream.TumbleWindow;
import org.apache.iotdb.db.auth.AuthorityChecker;
import org.apache.iotdb.db.auth.IAuthorityFetcher;
import org.apache.iotdb.db.i18n.DataNodeStreamMessages;
import org.apache.iotdb.db.protocol.session.IClientSession;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.common.header.DatasetHeader;
import org.apache.iotdb.db.queryengine.plan.execution.config.IConfigTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.TableConfigTaskVisitor;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.relational.stream.CreateStreamTask;
import org.apache.iotdb.db.queryengine.plan.planner.plan.LogicalQueryPlan;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.Analysis;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.RelationType;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.TestMetadata;
import org.apache.iotdb.db.queryengine.plan.relational.planner.ir.ExpressionRewriter;
import org.apache.iotdb.db.queryengine.plan.relational.planner.ir.ExpressionTreeRewriter;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.PruneTableScanColumns;
import org.apache.iotdb.db.queryengine.plan.relational.security.AllowAllAccessControl;

import org.apache.tsfile.common.constant.TsFileConstant;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.type.TypeEnum;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.tsfile.enums.TSDataType.INT64;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.same;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class StreamPlannerTest {

  /**
   * Inject a mocked {@link IAuthorityFetcher} so that {@code buildStreamTask} can resolve the
   * current user's encrypted password without a running ConfigNode.
   */
  private static void mockCurrentUser() {
    User user = Mockito.mock(User.class);
    when(user.getPassword()).thenReturn("encrypted-password");
    IAuthorityFetcher fetcher = Mockito.mock(IAuthorityFetcher.class);
    when(fetcher.getUser(anyString(), anyBoolean())).thenReturn(user);
    AuthorityChecker.setAuthorityFetcher(fetcher);
  }

  @Test
  public void testCreateStreamLogicalPlanWithRowsContainsEventScanNode() {
    PlanTester planTester = new PlanTester();
    String sql =
        "create stream s_rows from testdb.table1 "
            + "tumble(size => 1h, origin => 2000-01-01T00:00:00) "
            + "into testdb.table2(time, s1) "
            + "select ${start_time}, count(*) from ${rows}";

    LogicalQueryPlan logicalQueryPlan = planTester.createPlan(sql);
    Assert.assertTrue(containsNode(logicalQueryPlan.getRootNode(), EventScanNode.class));
  }

  @Test
  public void testCreateStreamWithRowsCanReferenceSourceColumnAndKeepOutputs() {
    PlanTester planTester = new PlanTester();
    String sql =
        "create stream s_rows from testdb.table1 "
            + "tumble(size => 1h, origin => 2000-01-01T00:00:00) "
            + "into testdb.table2(time, s3, s1) "
            + "select ${start_time}, sin(${row_num}), s1 from ${rows}";

    Analysis analysis = PlanTester.analyze(sql, new TestMetadata(), newTestContext(sql));
    Assert.assertTrue(analysis.containsRowsPlaceholder());
    Assert.assertEquals(3, analysis.getOutputDescriptor().getVisibleFieldCount());

    LogicalQueryPlan logicalQueryPlan = planTester.createPlan(sql);
    OutputNode outputNode = (OutputNode) logicalQueryPlan.getRootNode();
    Assert.assertEquals(3, outputNode.getOutputSymbols().size());

    EventScanNode eventScanNode =
        findFirstNode(logicalQueryPlan.getRootNode(), EventScanNode.class);
    Assert.assertNotNull(eventScanNode);
    Assert.assertTrue(
        eventScanNode.getAssignments().values().stream()
            .map(ColumnSchema::getName)
            .anyMatch(name -> name.equals("s1")));
  }

  @Test
  public void testCreateStreamLogicalPlanWithoutRowsContainsNoEventScanNode() {
    PlanTester planTester = new PlanTester();
    String sql =
        "create stream s_session from testdb.table1 "
            + "tumble(size => 1h, origin => 2000-01-01T00:00:00) "
            + "into testdb.table2(time, s1) "
            + "select time, s1 from testdb.table1";

    LogicalQueryPlan logicalQueryPlan = planTester.createPlan(sql);
    Assert.assertFalse(containsNode(logicalQueryPlan.getRootNode(), EventScanNode.class));
  }

  /** Verifies CREATE STREAM applies the subscription WAL retention configuration from SQL. */
  @Test
  public void testCreateStreamTaskWithoutRowsUsesSessionScanNodeCalcPlan() {
    String sql =
        "create stream s_session from testdb.table1 "
            + "tumble(size => 1h, origin => 2000-01-01T00:00:00) "
            + "into testdb.table2(time, s1) "
            + "select time, s1 from testdb.table1 "
            + "with (subscription_consensus_wal_retention_size_in_bytes = -1, "
            + "subscription_consensus_wal_retention_time_ms = -1, "
            + "subscription_poll_thread_count = 3)";
    MPPQueryContext context = newTestContext(sql);
    Analysis analysis = PlanTester.analyze(sql, new TestMetadata(), context);
    CreateStream createStream = (CreateStream) analysis.getStatement();
    analysis.setRespDatasetHeader(
        new DatasetHeader(
            Arrays.asList(
                new ColumnHeader("start_time", INT64), new ColumnHeader("row_num", INT64)),
            true));

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

    mockCurrentUser();
    IConfigTask configTask = visitor.visitCreateStream(createStream, context);
    Assert.assertTrue(configTask instanceof CreateStreamTask);
    CreateStreamTask createStreamTask = (CreateStreamTask) configTask;

    ByteBuffer calcPlan = createStreamTask.getStreamTask().getCalcPlan().duplicate();
    Assert.assertEquals(PlanNodeType.SESSION_SCAN_NODE.getNodeType(), calcPlan.getShort());
    Assert.assertTrue(createStreamTask.getStreamTask().getSubQuery().contains("SELECT"));
    Assert.assertEquals(
        Long.valueOf(-1),
        createStreamTask
            .getStreamTask()
            .getProperties()
            .getSubscriptionConsensusWalRetentionSizeInBytes());
    Assert.assertEquals(
        Long.valueOf(-1),
        createStreamTask
            .getStreamTask()
            .getProperties()
            .getSubscriptionConsensusWalRetentionTimeMs());
    Assert.assertEquals(
        Integer.valueOf(3),
        createStreamTask.getStreamTask().getProperties().getSubscriptionPollThreadCount());
    verify(streamQueryPlanner, never()).doLogicalPlan(same(analysis), same(context));
  }

  @Test
  public void testCreateStreamTaskWithRowsUsesLogicalPlannerCalcPlan() {
    String sql =
        "create stream s_rows from testdb.table1 "
            + "tumble(size => 1h, origin => 2000-01-01T00:00:00) "
            + "into testdb.table2(time, s1) "
            + "select ${start_time}, count(*) from ${rows}";
    MPPQueryContext context = newTestContext(sql);
    Analysis analysis = PlanTester.analyze(sql, new TestMetadata(), context);
    CreateStream createStream = (CreateStream) analysis.getStatement();
    analysis.setRespDatasetHeader(
        new DatasetHeader(
            Arrays.asList(
                new ColumnHeader("start_time", INT64), new ColumnHeader("row_num", INT64)),
            true));

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
    EventScanNode eventScanNode =
        new EventScanNode(new PlanNodeId("event-scan"), outputSymbols, assignments);
    when(streamQueryPlanner.doLogicalPlan(same(analysis), same(context)))
        .thenReturn(
            new LogicalQueryPlan(
                context,
                new OutputNode(
                    new PlanNodeId("output"),
                    eventScanNode,
                    Arrays.asList("start_time", "row_num"),
                    outputSymbols)));

    TableConfigTaskVisitor visitor =
        new TableConfigTaskVisitor(
                Mockito.mock(IClientSession.class),
                new TestMetadata(),
                new AllowAllAccessControl(),
                new org.apache.iotdb.commons.queryengine.plan.relational.type.InternalTypeManager())
            .withStreamQueryPlanner(streamQueryPlanner);

    mockCurrentUser();
    IConfigTask configTask = visitor.visitCreateStream(createStream, context);
    Assert.assertTrue(configTask instanceof CreateStreamTask);
    CreateStreamTask createStreamTask = (CreateStreamTask) configTask;

    ByteBuffer calcPlan = createStreamTask.getStreamTask().getCalcPlan().duplicate();
    Assert.assertEquals(PlanNodeType.TABLE_OUTPUT_NODE.getNodeType(), calcPlan.getShort());
    IoTDBSubscriptionSource source =
        (IoTDBSubscriptionSource) createStreamTask.getStreamTask().getSource();
    Assert.assertTrue(source.getPartitionColumns().isEmpty());
    Assert.assertEquals(
        Arrays.asList(TsFileConstant.TIME_COLUMN_ID, "row_num"),
        createStreamTask.getStreamTask().getWindow().getEventInputColumns());
    verify(streamQueryPlanner, times(1)).doLogicalPlan(same(analysis), same(context));
  }

  @Test
  public void testCreateStreamTaskWithRowsUsesPrunedColumnsFromEventScanNodeAfterSerde()
      throws IOException {
    String sql =
        "create stream s_rows from testdb.table1 "
            + "tumble(size => 1h, origin => 2000-01-01T00:00:00) "
            + "into testdb.table2(time, s1) "
            + "select ${start_time}, count(*) from ${rows}";
    MPPQueryContext context = newTestContext(sql);
    Analysis analysis = PlanTester.analyze(sql, new TestMetadata(), context);
    CreateStream createStream = (CreateStream) analysis.getStatement();
    analysis.setRespDatasetHeader(
        new DatasetHeader(
            Arrays.asList(
                new ColumnHeader("start_time", INT64), new ColumnHeader("row_num", INT64)),
            true));

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
    EventScanNode eventScanNode =
        new EventScanNode(new PlanNodeId("event-scan-serde"), outputSymbols, assignments);
    when(streamQueryPlanner.doLogicalPlan(same(analysis), same(context)))
        .thenReturn(
            new LogicalQueryPlan(
                context,
                new OutputNode(
                    new PlanNodeId("output-serde"),
                    eventScanNode,
                    Arrays.asList("start_time", "row_num"),
                    outputSymbols)));

    TableConfigTaskVisitor visitor =
        new TableConfigTaskVisitor(
                Mockito.mock(IClientSession.class),
                new TestMetadata(),
                new AllowAllAccessControl(),
                new org.apache.iotdb.commons.queryengine.plan.relational.type.InternalTypeManager())
            .withStreamQueryPlanner(streamQueryPlanner);

    mockCurrentUser();
    IConfigTask configTask = visitor.visitCreateStream(createStream, context);
    Assert.assertTrue(configTask instanceof CreateStreamTask);
    StreamTask streamTask = ((CreateStreamTask) configTask).getStreamTask();
    IoTDBSubscriptionSource source = (IoTDBSubscriptionSource) streamTask.getSource();
    Assert.assertTrue(source.getPartitionColumns().isEmpty());
    Assert.assertEquals(
        Arrays.asList(TsFileConstant.TIME_COLUMN_ID, "row_num"),
        streamTask.getWindow().getEventInputColumns());
    // Schema-resolved types ship with the window (the engine must not derive them from tablets).
    Assert.assertEquals(
        Arrays.asList(TSDataType.INT64, TSDataType.INT64),
        streamTask.getWindow().getEventInputColumnTypes());

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    streamTask.serialize(dos);
    dos.flush();
    StreamTask restored = StreamTask.deserialize(ByteBuffer.wrap(baos.toByteArray()));

    IoTDBSubscriptionSource restoredSource = (IoTDBSubscriptionSource) restored.getSource();
    Assert.assertTrue(restoredSource.getPartitionColumns().isEmpty());
    Assert.assertEquals(
        Arrays.asList(TsFileConstant.TIME_COLUMN_ID, "row_num"),
        restored.getWindow().getEventInputColumns());
    Assert.assertEquals(
        Arrays.asList(TSDataType.INT64, TSDataType.INT64),
        restored.getWindow().getEventInputColumnTypes());
    // subscribeColumns is not serialized on the source; it is re-derived from the window event
    // input columns ∪ partition columns after the StreamTask is deserialized.
    Assert.assertEquals(
        Arrays.asList(TsFileConstant.TIME_COLUMN_ID, "row_num"),
        restoredSource.getSubscribeColumns());
  }

  @Test
  public void testWindowSerdeRoundTripNullEventInputColumnsAndTypes() throws IOException {
    TumbleWindow window = new TumbleWindow(1000, 0);

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    window.serialize(dos);
    dos.flush();
    StreamWindow restored = StreamWindow.deserialize(ByteBuffer.wrap(baos.toByteArray()));

    Assert.assertTrue(restored instanceof TumbleWindow);
    Assert.assertNull(restored.getEventInputColumns());
    Assert.assertNull(restored.getEventInputColumnTypes());
    Assert.assertEquals(1000, ((TumbleWindow) restored).getSize());
    Assert.assertEquals(0, ((TumbleWindow) restored).getOrigin());
  }

  @Test
  public void testSubscribeColumnsMergePartitionAndEventInputColumns() throws IOException {
    String sql =
        "create stream stream1 from testdb.table1 partition by tag1, tag2 "
            + "tumble(size => 1h, origin => 2000-01-01T00:00:00) "
            + "into testdb.table2(time, s2) "
            + "select ${start_time}, max(s1) from ${rows}";
    MPPQueryContext context = newTestContext(sql);
    Analysis analysis = PlanTester.analyze(sql, new TestMetadata(), context);
    CreateStream createStream = (CreateStream) analysis.getStatement();
    analysis.setRespDatasetHeader(
        new DatasetHeader(
            Arrays.asList(new ColumnHeader("start_time", INT64), new ColumnHeader("s1_max", INT64)),
            true));

    TableModelPlanner streamQueryPlanner = Mockito.mock(TableModelPlanner.class);
    when(streamQueryPlanner.analyze(context)).thenReturn(analysis);
    when(streamQueryPlanner.getSymbolAllocator()).thenReturn(new SymbolAllocator());

    // EventScanNode output columns (time, s1).
    Symbol time = new Symbol("time");
    Symbol s1 = new Symbol("s1");
    List<Symbol> outputSymbols = Arrays.asList(time, s1);
    Map<Symbol, ColumnSchema> assignments = new LinkedHashMap<>();
    assignments.put(
        time,
        new ColumnSchema(
            "time",
            org.apache.tsfile.read.common.type.TypeFactory.getType(INT64),
            false,
            TsTableColumnCategory.TIME));
    assignments.put(
        s1,
        new ColumnSchema(
            "s1",
            org.apache.tsfile.read.common.type.TypeFactory.getType(INT64),
            false,
            TsTableColumnCategory.FIELD));

    // PruneTableScanColumns trim output columns
    EventScanNode fullEventScanNode =
        new EventScanNode(new PlanNodeId("event-scan"), outputSymbols, assignments);
    Set<Symbol> referencedOutputs = new HashSet<>(List.of(s1));
    EventScanNode eventScanNode =
        (EventScanNode)
            PruneTableScanColumns.pruneColumns(fullEventScanNode, referencedOutputs).get();

    when(streamQueryPlanner.doLogicalPlan(same(analysis), same(context)))
        .thenReturn(
            new LogicalQueryPlan(
                context,
                new OutputNode(
                    new PlanNodeId("output"),
                    eventScanNode,
                    List.of("s1"),
                    eventScanNode.getOutputSymbols())));

    TableConfigTaskVisitor visitor =
        new TableConfigTaskVisitor(
                Mockito.mock(IClientSession.class),
                new TestMetadata(),
                new AllowAllAccessControl(),
                new org.apache.iotdb.commons.queryengine.plan.relational.type.InternalTypeManager())
            .withStreamQueryPlanner(streamQueryPlanner);

    mockCurrentUser();
    IConfigTask configTask = visitor.visitCreateStream(createStream, context);
    Assert.assertTrue(configTask instanceof CreateStreamTask);
    StreamTask streamTask = ((CreateStreamTask) configTask).getStreamTask();
    IoTDBSubscriptionSource source = (IoTDBSubscriptionSource) streamTask.getSource();

    // Partition columns come from the PARTITION BY clause of the CREATE STREAM SQL.
    Assert.assertEquals(Arrays.asList("tag1", "tag2"), source.getPartitionColumns());
    // Event input columns are the pruned columns
    Assert.assertEquals(List.of("s1"), streamTask.getWindow().getEventInputColumns());

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    streamTask.serialize(dos);
    dos.flush();
    StreamTask restored = StreamTask.deserialize(ByteBuffer.wrap(baos.toByteArray()));

    IoTDBSubscriptionSource restoredSource = (IoTDBSubscriptionSource) restored.getSource();
    // subscribeColumns = partition columns ∪ event input columns, deduplicated, order preserved.
    Assert.assertEquals(Arrays.asList("tag1", "tag2", "s1"), restoredSource.getSubscribeColumns());
  }

  @Test
  public void testCreateStreamWithPartitionByWithoutRowsKeepsPartitionColumns() throws IOException {
    String sql =
        "create stream stream1 from testdb.table1 partition by tag1, tag2 "
            + "tumble(size => 1h, origin => 2000-01-01T00:00:00) "
            + "into testdb.table2(time, tag1, tag2, s1) "
            + "select ${start_time}, ${1}, ${2}, s1 from testdb.table1";
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

    mockCurrentUser();
    IConfigTask configTask = visitor.visitCreateStream(createStream, context);
    Assert.assertTrue(configTask instanceof CreateStreamTask);
    StreamTask streamTask = ((CreateStreamTask) configTask).getStreamTask();

    // PARTITION BY columns must be honored even when the calculation plan does not reference
    // ${rows}: the ${n} placeholders resolve against these partition values at runtime.
    IoTDBSubscriptionSource source = (IoTDBSubscriptionSource) streamTask.getSource();
    Assert.assertEquals(Arrays.asList("tag1", "tag2"), source.getPartitionColumns());
    // Without ${rows} the calculation plan is a SessionScanNode over the raw SQL.
    Assert.assertEquals(
        PlanNodeType.SESSION_SCAN_NODE.getNodeType(),
        streamTask.getCalcPlan().duplicate().getShort());
    // The ${n} placeholder is registered on the window for the runtime to resolve.
    Assert.assertTrue(
        streamTask.getWindow().getLiteralPlaceholders().contains(StreamLiteral.Type.N));
  }

  @Test
  public void testStreamWindowPlaceholdersDerivedFromCalcPlan() throws IOException {
    String sql =
        "create stream stream1 from testdb.table1 partition by tag1, tag2, tag3 "
            + "variation(col => s1, delta => 1.0) "
            + "into testdb.table2(time, s1, tag1, tag2, tag3, s2) "
            + "select ${start_time}, ${prev_value}, ${1}, ${2}, ${3}, ${current_value} from ${rows}";
    MPPQueryContext context = newTestContext(sql);
    Analysis analysis = PlanTester.analyze(sql, new TestMetadata(), context);
    CreateStream createStream = (CreateStream) analysis.getStatement();
    analysis.setRespDatasetHeader(
        new DatasetHeader(List.of(new ColumnHeader("start_time", INT64)), true));

    TableModelPlanner streamQueryPlanner = Mockito.mock(TableModelPlanner.class);
    when(streamQueryPlanner.analyze(context)).thenReturn(analysis);
    when(streamQueryPlanner.getSymbolAllocator()).thenReturn(new SymbolAllocator());
    Symbol time = new Symbol("time");
    List<Symbol> outputSymbols = List.of(time);
    Map<Symbol, ColumnSchema> assignments = new LinkedHashMap<>();
    assignments.put(
        time,
        new ColumnSchema(
            "time",
            org.apache.tsfile.read.common.type.TypeFactory.getType(INT64),
            false,
            TsTableColumnCategory.TIME));
    EventScanNode eventScanNode =
        new EventScanNode(new PlanNodeId("event-scan-placeholder"), outputSymbols, assignments);
    when(streamQueryPlanner.doLogicalPlan(same(analysis), same(context)))
        .thenReturn(
            new LogicalQueryPlan(
                context,
                new OutputNode(
                    new PlanNodeId("output-placeholder"),
                    eventScanNode,
                    List.of("time"),
                    outputSymbols)));

    TableConfigTaskVisitor visitor =
        new TableConfigTaskVisitor(
                Mockito.mock(IClientSession.class),
                new TestMetadata(),
                new AllowAllAccessControl(),
                new org.apache.iotdb.commons.queryengine.plan.relational.type.InternalTypeManager())
            .withStreamQueryPlanner(streamQueryPlanner);

    mockCurrentUser();
    IConfigTask configTask = visitor.visitCreateStream(createStream, context);
    Assert.assertTrue(configTask instanceof CreateStreamTask);
    StreamTask streamTask = ((CreateStreamTask) configTask).getStreamTask();

    // The ${rows} placeholder in the calculation plan sets the rows placeholder flag.
    Assert.assertTrue(streamTask.getWindow().containsRowsPlaceholder());
    // Literal placeholders used in the calculation plan are collected onto the window.
    Assert.assertTrue(
        streamTask.getWindow().getLiteralPlaceholders().contains(StreamLiteral.Type.START_TIME));
    Assert.assertTrue(
        streamTask.getWindow().getLiteralPlaceholders().contains(StreamLiteral.Type.PREV_VALUE));
    Assert.assertTrue(
        streamTask.getWindow().getLiteralPlaceholders().contains(StreamLiteral.Type.CURRENT_VALUE));
    Assert.assertTrue(
        streamTask.getWindow().getLiteralPlaceholders().contains(StreamLiteral.Type.N));
  }

  @Test
  public void testExpressionTreeRewriterSupportsPlaceholderLiteral() {
    StreamLiteral placeholder = new StreamLiteral(StreamLiteral.Type.START_TIME);
    StreamLiteral rewritten =
        ExpressionTreeRewriter.rewriteWith(new ExpressionRewriter<Void>() {}, placeholder);
    Assert.assertSame(placeholder, rewritten);
  }

  @Test
  public void testPlaceHolderLiteralSerdeRoundTrip() throws IOException {
    StreamLiteral placeholder = new StreamLiteral(StreamLiteral.Type.N).withValue(2);
    placeholder.setDataType(org.apache.tsfile.read.common.type.TypeFactory.getType(INT64));

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    placeholder.serialize(new DataOutputStream(baos));
    StreamLiteral restored = new StreamLiteral(ByteBuffer.wrap(baos.toByteArray()));

    Assert.assertEquals(placeholder.getType(), restored.getType());
    Assert.assertEquals(placeholder.getValue(), restored.getValue());
    Assert.assertEquals(placeholder.getDataType(), restored.getDataType());
  }

  @Test
  public void testCreateStreamTaskFromSqlAndStreamTaskSerdeRoundTrip() throws IOException {
    String sql =
        "create stream s_session from testdb.table1 "
            + "tumble(size => 1h, origin => 2000-01-01T00:00:00) "
            + "into testdb.table2(time, s1) "
            + "select time, s1 from testdb.table1";
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

    mockCurrentUser();
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

  @Test
  public void testCreateStreamTaskWithQualifiedTablesDoesNotRequireSessionDatabase() {
    String sql =
        "create stream s_session from testdb.table1 "
            + "tumble(size => 1h, origin => 2000-01-01T00:00:00) "
            + "into testdb.table2(time, s1) "
            + "select time, s1 from testdb.table1";
    MPPQueryContext context = newTestContext(sql, null);
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

    mockCurrentUser();
    IConfigTask configTask = visitor.visitCreateStream(createStream, context);

    Assert.assertTrue(configTask instanceof CreateStreamTask);
    StreamTask streamTask = ((CreateStreamTask) configTask).getStreamTask();
    Assert.assertEquals("testdb", streamTask.getDatabase());
    Assert.assertEquals("testdb", ((IoTDBSubscriptionSource) streamTask.getSource()).getDatabase());
    Assert.assertEquals("testdb", ((IoTDBTarget) streamTask.getTarget()).getDatabase());
  }

  @Test
  public void testCreateStreamPreFilterNotSupported() {
    // the stream source does not support a pre-filter yet
    String sql =
        "create stream s_pre from testdb.table1 where s1 > 1 "
            + "tumble(size => 1h, origin => 2000-01-01T00:00:00) "
            + "into testdb.table2(time, s1) "
            + "select ${start_time}, count(*) from ${rows}";
    MPPQueryContext context = newTestContext(sql);
    SemanticException e =
        Assert.assertThrows(
            SemanticException.class, () -> PlanTester.analyze(sql, new TestMetadata(), context));
    Assert.assertEquals(
        DataNodeStreamMessages
            .EXCEPTION_PREFILTER_CLAUSE_OF_THE_STREAM_SOURCE_IS_NOT_SUPPORTED_YET_7BCA6A84,
        e.getMessage());
  }

  private static MPPQueryContext newTestContext(String sql) {
    return newTestContext(sql, "testdb");
  }

  private static MPPQueryContext newTestContext(String sql, String databaseName) {
    SessionInfo sessionInfo =
        new SessionInfo(
            1L,
            "test_user",
            ZoneId.systemDefault(),
            IoTDBConstant.ClientVersion.V_1_0,
            databaseName,
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

  private static <T extends PlanNode> T findFirstNode(PlanNode node, Class<T> clazz) {
    if (clazz.isInstance(node)) {
      return clazz.cast(node);
    }
    for (PlanNode child : node.getChildren()) {
      T matched = findFirstNode(child, clazz);
      if (matched != null) {
        return matched;
      }
    }
    return null;
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

  @Test
  public void testCreateStreamWithColumnsSets() {
    String sql =
        "create stream s_cols from testdb.table1 "
            + "tumble(size => 1h, origin => 2000-01-01T00:00:00) "
            + "into testdb.table2(time, S1, \"s2\") "
            + "select ${start_time}, count(*), count(*) from ${rows}";
    MPPQueryContext context = newTestContext(sql);
    Analysis analysis = PlanTester.analyze(sql, new TestMetadata(), context);
    CreateStream createStream = (CreateStream) analysis.getStatement();

    // Verify that column names, types, and categories are set
    RelationType sinkRelationType = analysis.getStreamSinkRelationType();
    Assert.assertNotNull(sinkRelationType);
    Assert.assertEquals(3, sinkRelationType.getVisibleFieldCount());
    Assert.assertEquals(
        TypeEnum.TIMESTAMP, sinkRelationType.getFieldByIndex(0).getType().getTypeEnum());
    Assert.assertEquals(
        TypeEnum.INT64, sinkRelationType.getFieldByIndex(1).getType().getTypeEnum());
    Assert.assertEquals(
        TypeEnum.INT64, sinkRelationType.getFieldByIndex(2).getType().getTypeEnum());
  }

  @Test
  public void testCreateStreamWithoutColumnsSets() {
    String sql =
        "create stream s_all_cols from testdb.table1 "
            + "tumble(size => 1h, origin => 2000-01-01T00:00:00) "
            + "into testdb.table_with_x "
            + "select ${start_time}, count(*), count(*), count(*) from ${rows}";
    MPPQueryContext context = newTestContext(sql);
    Analysis analysis = PlanTester.analyze(sql, new TestMetadata(), context);
    CreateStream createStream = (CreateStream) analysis.getStatement();

    // When no columns are specified, all sink table columns should be used
    RelationType sinkRelationType = analysis.getStreamSinkRelationType();
    Assert.assertNotNull(sinkRelationType);
    Assert.assertEquals(4, sinkRelationType.getVisibleFieldCount());
  }
}
