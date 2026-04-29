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

package org.apache.iotdb.db.queryengine.plan.relational.planner.node;

import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.IPlanVisitor;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeType;

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class SessionScanNode extends PlanNode {

  private String sql;

  public SessionScanNode(PlanNodeId id, String sql) {
    super(id);
    this.sql = sql;
  }

  protected SessionScanNode() {}

  public String getSql() {
    return sql;
  }

  @Override
  public List<String> getOutputColumnNames() {
    return Collections.emptyList();
  }

  @Override
  public List<PlanNode> getChildren() {
    return Collections.emptyList();
  }

  @Override
  public void addChild(PlanNode child) {
    throw new UnsupportedOperationException("SessionScanNode is leaf node!");
  }

  @Override
  public int allowedChildCount() {
    return NO_CHILD_ALLOWED;
  }

  @Override
  public PlanNodeType getType() {
    return PlanNodeType.SESSION_SCAN_NODE;
  }

  @Override
  public <R, C> R accept(IPlanVisitor<R, C> visitor, C context) {
    throw new UnsupportedOperationException("SessionScanNode is only used to storage sql string!");
  }

  @Override
  public SessionScanNode clone() {
    return new SessionScanNode(getPlanNodeId(), sql);
  }

  @Override
  public PlanNode replaceChildren(List<PlanNode> newChildren) {
    return this;
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    getType().serialize(byteBuffer);
    ReadWriteIOUtils.write(sql, byteBuffer);
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    getType().serialize(stream);
    ReadWriteIOUtils.write(sql, stream);
  }

  public static SessionScanNode deserialize(ByteBuffer byteBuffer) {
    SessionScanNode node = new SessionScanNode();
    node.sql = ReadWriteIOUtils.readString(byteBuffer);
    node.setPlanNodeId(PlanNodeId.deserialize(byteBuffer));
    return node;
  }

  @Override
  public String toString() {
    return "SessionScanNode-" + getPlanNodeId();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    SessionScanNode that = (SessionScanNode) o;
    return Objects.equals(sql, that.sql);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), sql);
  }
}
