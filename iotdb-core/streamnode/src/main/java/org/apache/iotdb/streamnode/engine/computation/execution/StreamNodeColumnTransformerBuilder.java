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

package org.apache.iotdb.streamnode.engine.computation.execution;

import org.apache.iotdb.calc.execution.relational.ColumnTransformerBuilder;
import org.apache.iotdb.calc.plan.planner.memory.MemoryReservationManager;
import org.apache.iotdb.calc.plan.relational.metadata.ITypeMetadata;
import org.apache.iotdb.calc.transformation.dag.column.ColumnTransformer;
import org.apache.iotdb.calc.transformation.dag.column.leaf.LeafColumnTransformer;
import org.apache.iotdb.commons.queryengine.common.SessionInfo;
import org.apache.iotdb.commons.queryengine.plan.analyze.ITableTypeProvider;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.parameter.InputLocation;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.stream.PlaceHolderLiteral;
import org.apache.iotdb.streamnode.engine.computation.placeholder.StreamPlaceholderColumnTransformer;
import org.apache.iotdb.streamnode.engine.scheduler.task.StreamDriverContext;

import org.apache.tsfile.enums.TSDataType;

import java.util.List;
import java.util.Map;

public class StreamNodeColumnTransformerBuilder extends ColumnTransformerBuilder {

  @Override
  public ColumnTransformer visitPlaceHolderLiteral(PlaceHolderLiteral node, Context context) {
    StreamContext streamContext = (StreamContext) context;
    ColumnTransformer columnTransformer =
        streamContext.cache.computeIfAbsent(
            node,
            expression -> {
              StreamPlaceholderColumnTransformer placeholderColumnTransformer =
                  new StreamPlaceholderColumnTransformer(streamContext.driverContext, node);
              streamContext.addLeafColumnTransformer(placeholderColumnTransformer);
              return placeholderColumnTransformer;
            });
    columnTransformer.addReferenceCount();
    return columnTransformer;
  }

  public static class StreamContext extends Context {

    private final StreamDriverContext driverContext;
    private final List<LeafColumnTransformer> leafList;
    private final Map<Expression, ColumnTransformer> cache;

    public StreamContext(
        SessionInfo sessionInfo,
        List<LeafColumnTransformer> leafList,
        Map<Symbol, List<InputLocation>> inputLocations,
        Map<Expression, ColumnTransformer> cache,
        Map<Expression, ColumnTransformer> hasSeen,
        List<ColumnTransformer> commonTransformerList,
        List<TSDataType> inputDataTypes,
        int originSize,
        ITableTypeProvider typeProvider,
        ITypeMetadata metadata,
        MemoryReservationManager memoryReservationManager,
        StreamDriverContext driverContext) {
      super(
          sessionInfo,
          leafList,
          inputLocations,
          cache,
          hasSeen,
          commonTransformerList,
          inputDataTypes,
          originSize,
          typeProvider,
          metadata,
          memoryReservationManager);
      this.driverContext = driverContext;
      this.leafList = leafList;
      this.cache = cache;
    }

    private void addLeafColumnTransformer(LeafColumnTransformer columnTransformer) {
      leafList.add(columnTransformer);
    }
  }
}
