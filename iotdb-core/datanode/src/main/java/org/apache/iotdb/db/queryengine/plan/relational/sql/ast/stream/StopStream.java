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
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Node;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.NodeLocation;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.QualifiedName;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Statement;

import com.google.common.collect.ImmutableList;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Objects;

public class StopStream extends Statement {

  private final QualifiedName name;

  public StopStream(@Nullable NodeLocation location, QualifiedName name) {
    super(location);
    this.name = name;
  }

  public QualifiedName getName() {
    return name;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitStopStream(this, context);
  }

  @Override
  public List<? extends Node> getChildren() {
    return ImmutableList.of();
  }

  @Override
  public int hashCode() {
    return Objects.hash(name);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    StopStream that = (StopStream) obj;
    return Objects.equals(name, that.name);
  }

  @Override
  public String toString() {
    return "StopStream{name=" + name + "}";
  }

  @Override
  public long ramBytesUsed() {
    return 0;
  }
}
