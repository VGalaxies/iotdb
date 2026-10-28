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

import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.ICoreQueryPlanVisitor;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.IPlanVisitor;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.TableScanNode;
import org.apache.iotdb.commons.queryengine.plan.relational.metadata.ColumnSchema;
import org.apache.iotdb.commons.queryengine.plan.relational.metadata.QualifiedObjectName;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Expression;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

public class EventScanNode extends TableScanNode {

  private static final QualifiedObjectName TABLE = new QualifiedObjectName("", "__event_rows__");

  public EventScanNode(
      PlanNodeId id, List<Symbol> outputSymbols, Map<Symbol, ColumnSchema> assignments) {
    super(id, TABLE, outputSymbols, assignments);
    this.regionReplicaSet = new TRegionReplicaSet();
  }

  public EventScanNode(
      PlanNodeId id,
      List<Symbol> outputSymbols,
      Map<Symbol, ColumnSchema> assignments,
      Expression pushDownPredicate,
      long pushDownLimit,
      long pushDownOffset) {
    super(id, TABLE, outputSymbols, assignments, pushDownPredicate, pushDownLimit, pushDownOffset);
    this.regionReplicaSet = new TRegionReplicaSet();
  }

  public EventScanNode(
      PlanNodeId id,
      List<Symbol> outputSymbols,
      Map<Symbol, ColumnSchema> assignments,
      Expression pushDownPredicate,
      long pushDownLimit,
      long pushDownOffset,
      TRegionReplicaSet regionReplicaSet) {
    this(id, outputSymbols, assignments, pushDownPredicate, pushDownLimit, pushDownOffset);
    this.regionReplicaSet = regionReplicaSet;
  }

  protected EventScanNode() {}

  @Override
  public <R, C> R accept(IPlanVisitor<R, C> visitor, C context) {
    return ((ICoreQueryPlanVisitor<R, C>) visitor).visitEventScan(this, context);
  }

  @Override
  public PlanNode clone() {
    return new EventScanNode(
        id,
        outputSymbols,
        assignments,
        pushDownPredicate,
        pushDownLimit,
        pushDownOffset,
        regionReplicaSet);
  }

  @Override
  public PlanNodeType getType() {
    return PlanNodeType.EVENT_SCAN_NODE;
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    getType().serialize(byteBuffer);
    TableScanNode.serializeMemberVariables(this, byteBuffer, true);
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    getType().serialize(stream);
    TableScanNode.serializeMemberVariables(this, stream, true);
  }

  public static EventScanNode deserialize(ByteBuffer byteBuffer) {
    EventScanNode node = new EventScanNode();
    TableScanNode.deserializeMemberVariables(byteBuffer, node, true);
    node.setPlanNodeId(PlanNodeId.deserialize(byteBuffer));
    return node;
  }

  @Override
  public String toString() {
    return "EventScanNode-" + getPlanNodeId();
  }
}
