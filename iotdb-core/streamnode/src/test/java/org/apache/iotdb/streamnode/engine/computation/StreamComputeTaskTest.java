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

package org.apache.iotdb.streamnode.engine.computation;

import org.apache.iotdb.calc.plan.planner.memory.MemoryReservationManager;
import org.apache.iotdb.commons.audit.UserEntity;
import org.apache.iotdb.commons.queryengine.common.SessionInfo;
import org.apache.iotdb.commons.queryengine.common.SqlDialect;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.commons.queryengine.plan.relational.metadata.ColumnSchema;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.Assignments;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.EventScanNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.ProjectNode;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.SymbolReference;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.stream.StreamLiteral;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.commons.stream.ListPartitionKey;
import org.apache.iotdb.commons.stream.StreamNodeTableTypeProvider;
import org.apache.iotdb.commons.utils.FileUtils;
import org.apache.iotdb.streamnode.engine.computation.planner.LocalStreamExecutionPlanContext;
import org.apache.iotdb.streamnode.engine.computation.planner.StreamMetadataImpl;
import org.apache.iotdb.streamnode.engine.computation.planner.StreamNodeTableOperatorGenerator;
import org.apache.iotdb.streamnode.engine.computation.planner.memory.ThreadSafeStreamNodeMemoryReservationManager;
import org.apache.iotdb.streamnode.engine.task.StreamSubTaskContext;
import org.apache.iotdb.streamnode.engine.task.StreamSubTaskStateMachine;
import org.apache.iotdb.streamnode.engine.window.IEventInfo;
import org.apache.iotdb.streamnode.engine.window.WindowEventType;
import org.apache.iotdb.streamnode.utils.IEventRowsIterator;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.common.type.IntType;
import org.apache.tsfile.read.common.type.LongType;
import org.apache.tsfile.read.common.type.TimestampType;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.time.ZoneId;
import java.util.Collections;
import java.util.Optional;
import java.util.OptionalLong;

public class StreamComputeTaskTest {

  @Test
  public void testComputeAndResetWithManuallyConstructedPlanNodes() throws Exception {
    MemoryReservationManager memoryReservationManager = newMemoryReservationManager();
    StreamComputeTaskContext computeContext = new StreamComputeTaskContext(false);
    computeContext.setMemoryReservationManager(memoryReservationManager);
    LocalStreamExecutionPlanContext planContext = createPlanContext(memoryReservationManager);
    StreamComputeTask computeTask =
        new StreamComputeTask(
            createProjectNode(),
            new StreamNodeTableOperatorGenerator(new StreamMetadataImpl(), computeContext),
            computeContext,
            planContext);
    TestEventRowsIterator firstIterator = new TestEventRowsIterator(createTsBlock(1, 100));
    TestEventInfo firstEventInfo = new TestEventInfo(firstIterator, 10, 1, 300);

    computeTask.bindEventInfo(firstEventInfo);

    TsBlock firstResult = computeTask.compute();
    Assert.assertEquals(1, firstResult.getPositionCount());
    Assert.assertEquals(100, firstResult.getColumn(0).getInt(0));
    Assert.assertEquals(1, firstResult.getColumn(1).getLong(0));
    Assert.assertEquals(10, firstResult.getColumn(2).getLong(0));
    Assert.assertEquals(300, firstResult.getColumn(3).getInt(0));
    Assert.assertTrue(computeTask.isFinished());

    computeTask.reset();

    Assert.assertEquals(1, firstIterator.closeCount);

    TestEventRowsIterator secondIterator = new TestEventRowsIterator(createTsBlock(2, 200));
    TestEventInfo secondEventInfo = new TestEventInfo(secondIterator, 20, 1, 400);
    computeTask.bindEventInfo(secondEventInfo);

    TsBlock secondResult = computeTask.compute();
    Assert.assertEquals(1, secondResult.getPositionCount());
    Assert.assertEquals(200, secondResult.getColumn(0).getInt(0));
    Assert.assertEquals(1, secondResult.getColumn(1).getLong(0));
    Assert.assertEquals(20, secondResult.getColumn(2).getLong(0));
    Assert.assertEquals(400, secondResult.getColumn(3).getInt(0));
    Assert.assertTrue(computeTask.isFinished());

    computeTask.close();

    Assert.assertEquals(1, secondIterator.closeCount);
  }

  @Test
  public void testResetDeletesComputeTmpDir() throws Exception {
    MemoryReservationManager memoryReservationManager = newMemoryReservationManager();
    StreamComputeTaskContext computeContext = new StreamComputeTaskContext(false);
    computeContext.setMemoryReservationManager(memoryReservationManager);
    LocalStreamExecutionPlanContext planContext = createPlanContext(memoryReservationManager);
    StreamComputeTask computeTask =
        new StreamComputeTask(
            createProjectNode(),
            new StreamNodeTableOperatorGenerator(new StreamMetadataImpl(), computeContext),
            computeContext,
            planContext);
    File computeTmpDir =
        new File(
            StreamNodeTableOperatorGenerator.getComputeTmpDir(
                planContext.getDriverContext(), computeContext.isRecomputeTask()));
    try {
      Assert.assertTrue(computeTmpDir.mkdirs());
      Assert.assertTrue(new File(computeTmpDir, "spill.sortTemp").createNewFile());
      computeContext.setHaveTmpFile(true);
      computeTask.bindEventInfo(
          new TestEventInfo(new TestEventRowsIterator(createTsBlock(1, 100)), 10, 1, 300));

      computeTask.reset();

      Assert.assertFalse(computeTmpDir.exists());
      Assert.assertNull(computeContext.getCurrentEventInfo());
    } finally {
      computeTask.close();
      FileUtils.deleteFileOrDirectory(computeTmpDir, true);
    }
  }

  private static PlanNode createProjectNode() {
    Symbol value = new Symbol("value");
    Symbol rowNum = new Symbol("row_num");
    Symbol startTime = new Symbol("start_time");
    Symbol currentValue = new Symbol("current_value");
    EventScanNode eventScanNode =
        new EventScanNode(
            new PlanNodeId("event-scan"),
            ImmutableList.of(value),
            ImmutableMap.of(
                value,
                new ColumnSchema("value", IntType.INT32, false, TsTableColumnCategory.FIELD)));
    StreamLiteral rowNumPlaceholder = new StreamLiteral(StreamLiteral.Type.ROW_NUM);
    rowNumPlaceholder.setDataType(LongType.INT64);
    StreamLiteral startTimePlaceholder = new StreamLiteral(StreamLiteral.Type.START_TIME);
    startTimePlaceholder.setDataType(TimestampType.TIMESTAMP);
    StreamLiteral currentValuePlaceholder = new StreamLiteral(StreamLiteral.Type.CURRENT_VALUE);
    currentValuePlaceholder.setDataType(IntType.INT32);

    return new ProjectNode(
        new PlanNodeId("project"),
        eventScanNode,
        Assignments.builder()
            .put(value, new SymbolReference(value.getName()))
            .put(rowNum, rowNumPlaceholder)
            .put(startTime, startTimePlaceholder)
            .put(currentValue, currentValuePlaceholder)
            .build());
  }

  private static StreamNodeTableTypeProvider createTypeProvider() {
    Symbol value = new Symbol("value");
    Symbol rowNum = new Symbol("row_num");
    Symbol startTime = new Symbol("start_time");
    Symbol currentValue = new Symbol("current_value");
    return new StreamNodeTableTypeProvider(
        ImmutableMap.of(
            value,
            IntType.INT32,
            rowNum,
            LongType.INT64,
            startTime,
            TimestampType.TIMESTAMP,
            currentValue,
            IntType.INT32));
  }

  private static LocalStreamExecutionPlanContext createPlanContext(
      MemoryReservationManager memoryReservationManager) {
    StreamSubTaskContext subTaskContext =
        new StreamSubTaskContext(
            "stream",
            new ListPartitionKey(Collections.emptyList()),
            new StreamSubTaskStateMachine("stream-0", Runnable::run));
    return new LocalStreamExecutionPlanContext(
        createTypeProvider(),
        ZoneId.systemDefault(),
        new SessionInfo(
            0L, new UserEntity(-1, "", ""), ZoneId.systemDefault(), null, SqlDialect.TABLE),
        subTaskContext,
        memoryReservationManager);
  }

  private static MemoryReservationManager newMemoryReservationManager() {
    return new ThreadSafeStreamNodeMemoryReservationManager("stream-test");
  }

  private static TsBlock createTsBlock(long time, int value) {
    TsBlockBuilder builder = new TsBlockBuilder(Collections.singletonList(TSDataType.INT32));
    builder.getTimeColumnBuilder().writeLong(time);
    builder.getColumnBuilder(0).writeInt(value);
    builder.declarePosition();
    return builder.build();
  }

  private static class TestEventRowsIterator implements IEventRowsIterator {

    private final TsBlock tsBlock;
    private boolean consumed;
    private int closeCount;

    private TestEventRowsIterator(TsBlock tsBlock) {
      this.tsBlock = tsBlock;
    }

    @Override
    public TsBlock next() {
      if (consumed) {
        return null;
      }
      consumed = true;
      return tsBlock;
    }

    @Override
    public ListenableFuture<?> isBlocked() {
      return org.apache.iotdb.calc.execution.operator.Operator.NOT_BLOCKED;
    }

    @Override
    public boolean isFinished() {
      return consumed;
    }

    @Override
    public void close() {
      closeCount++;
    }
  }

  private static class TestEventInfo implements IEventInfo {

    private final IEventRowsIterator rowsIterator;
    private final long startTime;
    private final long rowCount;
    private final int currentValue;

    private TestEventInfo(
        IEventRowsIterator rowsIterator, long startTime, long rowCount, int currentValue) {
      this.rowsIterator = rowsIterator;
      this.startTime = startTime;
      this.rowCount = rowCount;
      this.currentValue = currentValue;
    }

    @Override
    public WindowEventType getEventType() {
      return WindowEventType.WINDOW_CLOSE;
    }

    @Override
    public OptionalLong getStartTime() {
      return OptionalLong.of(startTime);
    }

    @Override
    public OptionalLong getEndTime() {
      return OptionalLong.empty();
    }

    @Override
    public OptionalLong getPrevTime() {
      return OptionalLong.empty();
    }

    @Override
    public OptionalLong getNextTime() {
      return OptionalLong.empty();
    }

    @Override
    public OptionalLong getRowCount() {
      return OptionalLong.of(rowCount);
    }

    @Override
    public Optional<Object> getPrevValue() {
      return Optional.empty();
    }

    @Override
    public Optional<Object> getCurrentValue() {
      return Optional.of(currentValue);
    }

    @Override
    public Optional<Object> getN(int n) {
      return Optional.empty();
    }

    @Override
    public long getWindowId() {
      return -1;
    }

    @Override
    public boolean needsCompute() {
      return true;
    }

    @Override
    public IEventRowsIterator getRowsIterator() {
      return rowsIterator;
    }

    @Override
    public boolean needRetry() {
      return false;
    }

    @Override
    public void handleComputeException(Throwable cause) {}
  }
}
