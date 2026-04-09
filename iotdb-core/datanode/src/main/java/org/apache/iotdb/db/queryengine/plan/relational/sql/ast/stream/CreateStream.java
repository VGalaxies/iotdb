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

package org.apache.iotdb.db.queryengine.plan.relational.sql.ast.stream;

import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.AstVisitor;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Identifier;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Node;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.NodeLocation;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.QualifiedName;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Query;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Statement;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Table;

import com.google.common.collect.ImmutableList;
import org.apache.tsfile.utils.RamUsageEstimator;

import javax.annotation.Nullable;

import java.util.List;

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

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitCreateStream(this, context);
  }

  @Override
  public List<? extends Node> getChildren() {
    return ImmutableList.of();
  }

  @Override
  public int hashCode() {
    return 0;
  }

  @Override
  public boolean equals(Object obj) {
    return false;
  }

  @Override
  public String toString() {
    return "";
  }

  @Override
  public long ramBytesUsed() {
    return 0;
  }
}
