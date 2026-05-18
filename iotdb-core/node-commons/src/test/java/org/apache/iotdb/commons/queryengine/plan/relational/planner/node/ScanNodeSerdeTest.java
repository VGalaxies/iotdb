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

package org.apache.iotdb.commons.queryengine.plan.relational.planner.node;

import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.ICoreQueryPlanVisitor;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.commons.queryengine.plan.relational.metadata.ColumnSchema;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;

import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.apache.tsfile.read.common.type.LongType.INT64;
import static org.apache.tsfile.read.common.type.StringType.STRING;

public class ScanNodeSerdeTest {

  @Test
  public void testSessionScanNodeSerdeAndClone() {
    SessionScanNode node =
        new SessionScanNode(new PlanNodeId("session-scan"), "select ${START_TIME}");

    ByteBuffer byteBuffer = node.serializeToByteBuffer();
    Assert.assertEquals(PlanNodeType.SESSION_SCAN_NODE.getNodeType(), byteBuffer.getShort());
    SessionScanNode deserialized = SessionScanNode.deserialize(byteBuffer);

    Assert.assertEquals(node.getPlanNodeId(), deserialized.getPlanNodeId());
    Assert.assertEquals(node.getSql(), deserialized.getSql());
    Assert.assertEquals(node, deserialized);
    Assert.assertEquals(node.hashCode(), deserialized.hashCode());

    SessionScanNode cloned = node.clone();
    Assert.assertNotSame(node, cloned);
    Assert.assertEquals(node.getPlanNodeId(), cloned.getPlanNodeId());
    Assert.assertEquals(node.getSql(), cloned.getSql());
  }

  @Test
  public void testSessionScanNodeAcceptThrowsException() {
    SessionScanNode node = new SessionScanNode(new PlanNodeId("session-scan"), "select 1");
    Assert.assertThrows(
        UnsupportedOperationException.class, () -> node.accept(new DummyVisitor(), null));
  }

  @Test
  public void testEventScanNodeSerdeCloneAndVisitorDispatch() {
    Symbol time = new Symbol("time");
    Symbol value = new Symbol("s1");
    Map<Symbol, ColumnSchema> assignments = new LinkedHashMap<>();
    assignments.put(time, new ColumnSchema("time", INT64, false, TsTableColumnCategory.TIME));
    assignments.put(value, new ColumnSchema("s1", STRING, false, TsTableColumnCategory.FIELD));
    EventScanNode node =
        new EventScanNode(new PlanNodeId("event-scan"), Arrays.asList(time, value), assignments);

    ByteBuffer byteBuffer = node.serializeToByteBuffer();
    Assert.assertEquals(PlanNodeType.EVENT_SCAN_NODE.getNodeType(), byteBuffer.getShort());
    EventScanNode deserialized = EventScanNode.deserialize(byteBuffer);

    Assert.assertEquals(node.getPlanNodeId(), deserialized.getPlanNodeId());
    Assert.assertEquals(node.getOutputSymbols(), deserialized.getOutputSymbols());
    Assert.assertEquals(node.getAssignments(), deserialized.getAssignments());

    PlanNode cloned = node.clone();
    Assert.assertTrue(cloned instanceof EventScanNode);
    EventScanNode clonedEvent = (EventScanNode) cloned;
    Assert.assertNotSame(node, clonedEvent);
    Assert.assertEquals(node.getOutputSymbols(), clonedEvent.getOutputSymbols());
    Assert.assertEquals(node.getAssignments(), clonedEvent.getAssignments());

    Assert.assertEquals("event", node.accept(new DummyVisitor(), null));
  }

  private static class DummyVisitor implements ICoreQueryPlanVisitor<String, Void> {
    @Override
    public String visitPlan(PlanNode node, Void context) {
      return "plan";
    }

    @Override
    public String visitEventScan(EventScanNode node, Void context) {
      return "event";
    }
  }
}
