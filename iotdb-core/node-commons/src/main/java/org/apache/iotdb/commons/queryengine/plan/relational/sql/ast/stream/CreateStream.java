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

import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.CommonQueryAstVisitor;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.IAstVisitor;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Identifier;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Node;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.NodeLocation;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.QualifiedName;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Query;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Statement;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Table;

import com.google.common.collect.ImmutableList;
import org.apache.tsfile.utils.RamUsageEstimator;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Objects;

public class CreateStream extends Statement {

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(CreateStream.class);

  private final Identifier streamName;
  @Nullable private final QualifiedName sourceTableName;
  @Nullable private final Expression preFilter;
  @Nullable private final List<Expression> partitionBy;
  private final EventWindow eventWindow;
  private final Query query;

  private final Table table;
  @Nullable private final List<Identifier> columns;

  public CreateStream(
      @Nullable NodeLocation location,
      @Nullable List<Identifier> columns,
      Identifier streamName,
      @Nullable QualifiedName sourceTableName,
      @Nullable Expression preFilter,
      @Nullable List<Expression> partitionBy,
      EventWindow eventWindow,
      Query query,
      Table table) {
    super(location);
    this.columns = columns;
    this.streamName = streamName;
    this.sourceTableName = sourceTableName;
    this.preFilter = preFilter;
    this.partitionBy = partitionBy;
    this.eventWindow = eventWindow;
    this.query = query;
    this.table = table;
  }

  public Identifier getStreamName() {
    return streamName;
  }

  @Nullable
  public QualifiedName getSourceTableName() {
    return sourceTableName;
  }

  @Nullable
  public Expression getPreFilter() {
    return preFilter;
  }

  @Nullable
  public List<Expression> getPartitionBy() {
    return partitionBy;
  }

  public EventWindow getEventWindow() {
    return eventWindow;
  }

  public Query getQuery() {
    return query;
  }

  public Table getTable() {
    return table;
  }

  @Nullable
  public List<Identifier> getColumns() {
    return columns;
  }

  @Override
  public <R, C> R accept(IAstVisitor<R, C> visitor, C context) {
    return ((CommonQueryAstVisitor<R, C>) visitor).visitCreateStream(this, context);
  }

  @Override
  public List<? extends Node> getChildren() {
    return ImmutableList.of();
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        streamName, sourceTableName, preFilter, partitionBy, eventWindow, query, table, columns);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    CreateStream that = (CreateStream) obj;
    return Objects.equals(streamName, that.streamName)
        && Objects.equals(sourceTableName, that.sourceTableName)
        && Objects.equals(preFilter, that.preFilter)
        && Objects.equals(partitionBy, that.partitionBy)
        && Objects.equals(eventWindow, that.eventWindow)
        && Objects.equals(query, that.query)
        && Objects.equals(table, that.table)
        && Objects.equals(columns, that.columns);
  }

  @Override
  public String toString() {
    return "CreateStream{streamName="
        + streamName
        + ", sourceTableName="
        + sourceTableName
        + ", eventWindow="
        + eventWindow
        + "}";
  }

  @Override
  public long ramBytesUsed() {
    return 0;
  }
}
