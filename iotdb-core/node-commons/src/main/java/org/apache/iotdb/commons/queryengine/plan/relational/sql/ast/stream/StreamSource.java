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

package org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.stream;

import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.AstMemoryEstimationHelper;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Node;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.NodeLocation;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.QualifiedName;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Statement;

import com.google.common.collect.ImmutableList;
import org.apache.tsfile.utils.RamUsageEstimator;

import java.util.List;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;

public class StreamSource extends Statement {
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(StreamSource.class);

  private final QualifiedName table;
  private final Expression preFilter;
  private final List<Expression> partitionBy;

  public StreamSource(
      final NodeLocation location,
      final QualifiedName table,
      final Expression preFilter,
      final List<Expression> partitionBy) {
    super(location);
    this.table = table;
    this.preFilter = preFilter;
    this.partitionBy = partitionBy;
  }

  public QualifiedName getTable() {
    return table;
  }

  public Expression getPreFilter() {
    return preFilter;
  }

  public List<Expression> getPartitionBy() {
    return partitionBy;
  }

  @Override
  public List<Node> getChildren() {
    return ImmutableList.of();
  }

  @Override
  public int hashCode() {
    return Objects.hash(table, preFilter, partitionBy);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    StreamSource other = (StreamSource) obj;
    return Objects.equals(table, other.table)
        && Objects.equals(preFilter, other.preFilter)
        && Objects.equals(partitionBy, other.partitionBy);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("table", table)
        .add("preFilter", preFilter)
        .add("partitionBy", partitionBy)
        .toString();
  }

  @Override
  public long ramBytesUsed() {
    return INSTANCE_SIZE
        + AstMemoryEstimationHelper.getEstimatedSizeOfAccountableObject(table)
        + AstMemoryEstimationHelper.getEstimatedSizeOfAccountableObject(preFilter)
        + AstMemoryEstimationHelper.getEstimatedSizeOfNodeList(partitionBy);
  }
}
