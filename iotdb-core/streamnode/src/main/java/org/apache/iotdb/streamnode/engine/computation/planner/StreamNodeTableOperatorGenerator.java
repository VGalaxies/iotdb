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

package org.apache.iotdb.streamnode.engine.computation.planner;

import org.apache.iotdb.calc.execution.operator.CommonOperatorContext;
import org.apache.iotdb.calc.execution.operator.Operator;
import org.apache.iotdb.calc.execution.operator.process.PreviousFillWithGroupOperator;
import org.apache.iotdb.calc.execution.operator.process.TableFillOperator;
import org.apache.iotdb.calc.execution.operator.process.TableLinearFillOperator;
import org.apache.iotdb.calc.execution.operator.process.TableLinearFillWithGroupOperator;
import org.apache.iotdb.calc.execution.operator.process.TableMergeSortOperator;
import org.apache.iotdb.calc.execution.operator.process.TableSortOperator;
import org.apache.iotdb.calc.execution.operator.process.TableStreamSortOperator;
import org.apache.iotdb.calc.execution.operator.process.TableTopKOperator;
import org.apache.iotdb.calc.execution.operator.process.window.RowNumberOperator;
import org.apache.iotdb.calc.execution.operator.process.window.TableWindowOperator;
import org.apache.iotdb.calc.execution.operator.process.window.TopKRankingOperator;
import org.apache.iotdb.calc.execution.operator.source.relational.MarkDistinctOperator;
import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.AggregationOperator;
import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.TableAggregator;
import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.grouped.GroupedAggregator;
import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.grouped.HashAggregationOperator;
import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.grouped.StreamingAggregationOperator;
import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.grouped.StreamingHashAggregationOperator;
import org.apache.iotdb.calc.execution.relational.ColumnTransformerBuilder;
import org.apache.iotdb.calc.plan.planner.TableOperatorGenerator;
import org.apache.iotdb.calc.transformation.dag.column.ColumnTransformer;
import org.apache.iotdb.calc.transformation.dag.column.leaf.LeafColumnTransformer;
import org.apache.iotdb.commons.queryengine.common.SessionInfo;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.parameter.InputLocation;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.AggregationNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.EventScanNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.FilterNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.LinearFillNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.MarkDistinctNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.MergeSortNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.PreviousFillNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.ProjectNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.RowNumberNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.SessionScanNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.SortNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.StreamSortNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.TopKNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.TopKRankingNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.ValueFillNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.WindowNode;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.streamnode.conf.StreamNodeDescriptor;
import org.apache.iotdb.streamnode.engine.computation.execution.StreamNodeColumnTransformerBuilder;
import org.apache.iotdb.streamnode.engine.computation.execution.StreamOperatorContext;
import org.apache.iotdb.streamnode.engine.computation.execution.operator.EventAwareOperator;
import org.apache.iotdb.streamnode.engine.computation.execution.operator.EventScanOperator;
import org.apache.iotdb.streamnode.engine.computation.execution.operator.ReusableAggregationOperator;
import org.apache.iotdb.streamnode.engine.computation.execution.operator.ReusableFilterAndProjectOperator;
import org.apache.iotdb.streamnode.engine.computation.execution.operator.ReusableHashAggregationOperator;
import org.apache.iotdb.streamnode.engine.computation.execution.operator.ReusableStreamingAggregationOperator;
import org.apache.iotdb.streamnode.engine.computation.execution.operator.ReusableStreamingHashAggregationOperator;
import org.apache.iotdb.streamnode.engine.computation.execution.operator.ReusableTableWindowOperator;
import org.apache.iotdb.streamnode.engine.computation.execution.operator.SessionScanOperator;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.type.Type;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class StreamNodeTableOperatorGenerator
    extends TableOperatorGenerator<LocalStreamExecutionPlanContext, StreamMetadataImpl>
    implements StreamNodePlanVisitor<Operator, LocalStreamExecutionPlanContext> {

  private final Map<PlanNodeId, Operator> reusableOperators = new HashMap<>();
  private final Map<PlanNodeId, CommonOperatorContext> operatorContexts = new HashMap<>();

  public StreamNodeTableOperatorGenerator(StreamMetadataImpl metadata) {
    super(metadata);
  }

  @Override
  protected String getSortTmpDir(CommonOperatorContext operatorContext) {
    StreamOperatorContext streamNodeOperatorContext = (StreamOperatorContext) operatorContext;
    streamNodeOperatorContext.getDriverContext().setHaveTmpFile(true);
    return StreamNodeDescriptor.getInstance().getConfig().getSortTmpDir()
        + File.separator
        + streamNodeOperatorContext.getDriverContext().getDriverTaskId().getFullId()
        + File.separator
        + streamNodeOperatorContext.getPipelineId()
        + File.separator;
  }

  @Override
  protected CommonOperatorContext addOperatorContext(
      LocalStreamExecutionPlanContext context, PlanNodeId planNodeId, String operatorType) {
    return operatorContexts.computeIfAbsent(
        planNodeId,
        key ->
            new StreamOperatorContext(
                context.getNextOperatorId(), planNodeId, operatorType, context.getDriverContext()));
  }

  @Override
  protected SessionInfo getSessionInfo(LocalStreamExecutionPlanContext context) {
    return context.getSessionInfo();
  }

  @Override
  public Operator visitEventScan(EventScanNode node, LocalStreamExecutionPlanContext context) {
    return reusableOperators.computeIfAbsent(
        node.getPlanNodeId(),
        key -> {
          final CommonOperatorContext operatorContext =
              addOperatorContext(
                  context, node.getPlanNodeId(), EventScanOperator.class.getSimpleName());
          EventScanOperator operator =
              new EventScanOperator((StreamOperatorContext) operatorContext);
          addEventAwareOperator(context, operator);
          return operator;
        });
  }

  @Override
  public Operator visitSessionScan(SessionScanNode node, LocalStreamExecutionPlanContext context) {
    return reusableOperators.computeIfAbsent(
        node.getPlanNodeId(),
        key -> {
          final CommonOperatorContext operatorContext =
              addOperatorContext(
                  context, node.getPlanNodeId(), SessionScanOperator.class.getSimpleName());
          SessionScanOperator operator =
              new SessionScanOperator((StreamOperatorContext) operatorContext, node.getSql());
          addEventAwareOperator(context, operator);
          return operator;
        });
  }

  private void addEventAwareOperator(
      LocalStreamExecutionPlanContext context, EventAwareOperator operator) {
    context.getDriverContext().addEventAwareOperator(operator);
  }

  private Operator cacheReusableOperator(PlanNodeId planNodeId, Operator operator) {
    reusableOperators.put(planNodeId, operator);
    return operator;
  }

  @Override
  public Operator visitFilter(FilterNode node, LocalStreamExecutionPlanContext context) {
    Operator operator = reusableOperators.get(node.getPlanNodeId());
    if (operator == null) {
      return cacheReusableOperator(node.getPlanNodeId(), super.visitFilter(node, context));
    }
    return new ReusableFilterAndProjectOperator(
        (ReusableFilterAndProjectOperator) operator, node.getChild().accept(this, context));
  }

  @Override
  public Operator visitProject(ProjectNode node, LocalStreamExecutionPlanContext context) {
    Operator operator = reusableOperators.get(node.getPlanNodeId());
    if (operator == null) {
      return cacheReusableOperator(node.getPlanNodeId(), super.visitProject(node, context));
    }
    return new ReusableFilterAndProjectOperator(
        (ReusableFilterAndProjectOperator) operator,
        node.getChild() instanceof FilterNode
            ? ((FilterNode) node.getChild()).getChild().accept(this, context)
            : node.getChild().accept(this, context));
  }

  @Override
  protected Operator constructFilterAndProjectOperator(
      Optional<Expression> predicate,
      Operator inputOperator,
      Expression[] projectExpressions,
      List<TSDataType> inputDataTypes,
      Map<Symbol, List<InputLocation>> inputLocations,
      PlanNodeId planNodeId,
      LocalStreamExecutionPlanContext context) {

    final List<TSDataType> filterOutputDataTypes = new ArrayList<>(inputDataTypes);

    // records LeafColumnTransformer of filter
    List<LeafColumnTransformer> filterLeafColumnTransformerList = new ArrayList<>();

    // records subexpression -> ColumnTransformer for filter
    Map<Expression, ColumnTransformer> filterExpressionColumnTransformerMap = new HashMap<>();

    ColumnTransformerBuilder visitor = new StreamNodeColumnTransformerBuilder();

    SessionInfo sessionInfo = getSessionInfo(context);
    ColumnTransformer filterOutputTransformer =
        predicate
            .map(
                p -> {
                  ColumnTransformerBuilder.Context filterColumnTransformerContext =
                      new StreamNodeColumnTransformerBuilder.StreamContext(
                          sessionInfo,
                          filterLeafColumnTransformerList,
                          inputLocations,
                          filterExpressionColumnTransformerMap,
                          ImmutableMap.of(),
                          ImmutableList.of(),
                          ImmutableList.of(),
                          0,
                          context.getTableTypeProvider(),
                          metadata,
                          context.getMemoryReservationManager(),
                          context.getDriverContext());

                  return visitor.process(p, filterColumnTransformerContext);
                })
            .orElse(null);

    // records LeafColumnTransformer of project expressions
    List<LeafColumnTransformer> projectLeafColumnTransformerList = new ArrayList<>();

    List<ColumnTransformer> projectOutputTransformerList = new ArrayList<>();

    Map<Expression, ColumnTransformer> projectExpressionColumnTransformerMap = new HashMap<>();

    // records common ColumnTransformer between filter and project expressions
    List<ColumnTransformer> commonTransformerList = new ArrayList<>();

    ColumnTransformerBuilder.Context projectColumnTransformerContext =
        new StreamNodeColumnTransformerBuilder.StreamContext(
            sessionInfo,
            projectLeafColumnTransformerList,
            inputLocations,
            projectExpressionColumnTransformerMap,
            filterExpressionColumnTransformerMap,
            commonTransformerList,
            filterOutputDataTypes,
            inputLocations.size(),
            context.getTableTypeProvider(),
            metadata,
            context.getMemoryReservationManager(),
            context.getDriverContext());

    for (Expression expression : projectExpressions) {
      projectOutputTransformerList.add(
          visitor.process(expression, projectColumnTransformerContext));
    }

    final CommonOperatorContext operatorContext =
        addOperatorContext(
            context, planNodeId, ReusableFilterAndProjectOperator.class.getSimpleName());

    // Project expressions don't contain Non-Mappable UDF, TransformOperator is not needed
    return new ReusableFilterAndProjectOperator(
        operatorContext,
        inputOperator,
        filterOutputDataTypes,
        filterLeafColumnTransformerList,
        filterOutputTransformer,
        commonTransformerList,
        projectLeafColumnTransformerList,
        projectOutputTransformerList,
        false,
        predicate.isPresent());
  }

  @Override
  public Operator visitAggregation(AggregationNode node, LocalStreamExecutionPlanContext context) {
    Operator operator = reusableOperators.get(node.getPlanNodeId());
    if (operator == null) {
      return cacheReusableOperator(node.getPlanNodeId(), super.visitAggregation(node, context));
    }
    Operator child = node.getChild().accept(this, context);
    if (operator instanceof ReusableStreamingAggregationOperator) {
      return new ReusableStreamingAggregationOperator(
          (ReusableStreamingAggregationOperator) operator, child);
    }
    if (operator instanceof ReusableStreamingHashAggregationOperator) {
      return new ReusableStreamingHashAggregationOperator(
          (ReusableStreamingHashAggregationOperator) operator, child);
    }
    if (operator instanceof ReusableHashAggregationOperator) {
      return new ReusableHashAggregationOperator((ReusableHashAggregationOperator) operator, child);
    }
    return new ReusableAggregationOperator((AggregationOperator) operator, child);
  }

  @Override
  protected AggregationOperator createAggregationOperator(
      CommonOperatorContext operatorContext, Operator child, List<TableAggregator> aggregators) {
    return new ReusableAggregationOperator(
        super.createAggregationOperator(operatorContext, child, aggregators), child);
  }

  @Override
  protected StreamingAggregationOperator createStreamingAggregationOperator(
      CommonOperatorContext operatorContext,
      Operator child,
      List<Type> groupByTypes,
      List<Integer> groupByChannels,
      java.util.Comparator<org.apache.iotdb.calc.utils.datastructure.SortKey> groupKeyComparator,
      List<TableAggregator> aggregators,
      long maxPartialMemory,
      boolean spillEnabled,
      long unSpillMemoryLimit) {
    return new ReusableStreamingAggregationOperator(
        super.createStreamingAggregationOperator(
            operatorContext,
            child,
            groupByTypes,
            groupByChannels,
            groupKeyComparator,
            aggregators,
            maxPartialMemory,
            spillEnabled,
            unSpillMemoryLimit),
        child);
  }

  @Override
  protected StreamingHashAggregationOperator createStreamingHashAggregationOperator(
      CommonOperatorContext operatorContext,
      Operator child,
      List<Integer> preGroupedChannels,
      List<Integer> preGroupedIndexInResult,
      List<Type> unPreGroupedTypes,
      List<Integer> unPreGroupedChannels,
      List<Integer> unPreGroupedIndexInResult,
      java.util.Comparator<org.apache.iotdb.calc.utils.datastructure.SortKey> groupKeyComparator,
      List<GroupedAggregator> aggregators,
      AggregationNode.Step step,
      int expectedGroups,
      long maxPartialMemory,
      boolean spillEnabled,
      long unSpillMemoryLimit) {
    return new ReusableStreamingHashAggregationOperator(
        operatorContext,
        child,
        preGroupedChannels,
        preGroupedIndexInResult,
        unPreGroupedTypes,
        unPreGroupedChannels,
        unPreGroupedIndexInResult,
        groupKeyComparator,
        aggregators,
        step,
        expectedGroups,
        maxPartialMemory,
        spillEnabled,
        unSpillMemoryLimit);
  }

  @Override
  protected HashAggregationOperator createHashAggregationOperator(
      CommonOperatorContext operatorContext,
      Operator child,
      List<Type> groupByTypes,
      List<Integer> groupByChannels,
      List<GroupedAggregator> aggregators,
      AggregationNode.Step step,
      int expectedGroups,
      long maxPartialMemory,
      boolean spillEnabled,
      long unSpillMemoryLimit) {
    return new ReusableHashAggregationOperator(
        super.createHashAggregationOperator(
            operatorContext,
            child,
            groupByTypes,
            groupByChannels,
            aggregators,
            step,
            expectedGroups,
            maxPartialMemory,
            spillEnabled,
            unSpillMemoryLimit),
        child);
  }

  @Override
  public Operator visitMarkDistinct(
      MarkDistinctNode node, LocalStreamExecutionPlanContext context) {
    Operator operator = reusableOperators.get(node.getPlanNodeId());
    if (operator == null) {
      return cacheReusableOperator(node.getPlanNodeId(), super.visitMarkDistinct(node, context));
    }
    return new MarkDistinctOperator(
        (MarkDistinctOperator) operator, node.getChild().accept(this, context));
  }

  @Override
  public Operator visitPreviousFill(
      PreviousFillNode node, LocalStreamExecutionPlanContext context) {
    Operator operator = reusableOperators.get(node.getPlanNodeId());
    if (operator == null) {
      return cacheReusableOperator(node.getPlanNodeId(), super.visitPreviousFill(node, context));
    }
    if (node.getGroupingKeys().isPresent()) {
      return new PreviousFillWithGroupOperator(
          (PreviousFillWithGroupOperator) operator, node.getChild().accept(this, context));
    }

    return new TableFillOperator(
        (TableFillOperator) operator, node.getChild().accept(this, context));
  }

  @Override
  public Operator visitValueFill(ValueFillNode node, LocalStreamExecutionPlanContext context) {
    Operator operator = reusableOperators.get(node.getPlanNodeId());
    if (operator == null) {
      return cacheReusableOperator(node.getPlanNodeId(), super.visitValueFill(node, context));
    }
    return new TableFillOperator(
        (TableFillOperator) operator, node.getChild().accept(this, context));
  }

  @Override
  public Operator visitLinearFill(LinearFillNode node, LocalStreamExecutionPlanContext context) {
    Operator operator = reusableOperators.get(node.getPlanNodeId());
    if (operator == null) {
      return cacheReusableOperator(node.getPlanNodeId(), super.visitLinearFill(node, context));
    }
    if (node.getGroupingKeys().isPresent()) {
      return new TableLinearFillWithGroupOperator(
          (TableLinearFillWithGroupOperator) operator, node.getChild().accept(this, context));
    }

    return new TableLinearFillOperator(
        (TableLinearFillOperator) operator, node.getChild().accept(this, context));
  }

  @Override
  public Operator visitSort(SortNode node, LocalStreamExecutionPlanContext context) {
    Operator operator = reusableOperators.get(node.getPlanNodeId());
    if (operator == null) {
      return cacheReusableOperator(node.getPlanNodeId(), super.visitSort(node, context));
    }
    return new TableSortOperator(
        (TableSortOperator) operator, node.getChild().accept(this, context));
  }

  @Override
  public Operator visitMergeSort(MergeSortNode node, LocalStreamExecutionPlanContext context) {
    Operator operator = reusableOperators.get(node.getPlanNodeId());
    if (operator == null) {
      return cacheReusableOperator(node.getPlanNodeId(), super.visitMergeSort(node, context));
    }
    List<Operator> children = new ArrayList<>(node.getChildren().size());
    node.getChildren().forEach(child -> children.add(this.process(child, context)));
    return new TableMergeSortOperator((TableMergeSortOperator) operator, children);
  }

  @Override
  public Operator visitStreamSort(StreamSortNode node, LocalStreamExecutionPlanContext context) {
    Operator operator = reusableOperators.get(node.getPlanNodeId());
    if (operator == null) {
      return cacheReusableOperator(node.getPlanNodeId(), super.visitStreamSort(node, context));
    }
    return new TableStreamSortOperator(
        (TableStreamSortOperator) operator, node.getChild().accept(this, context));
  }

  @Override
  public Operator visitTopK(TopKNode node, LocalStreamExecutionPlanContext context) {
    Operator operator = reusableOperators.get(node.getPlanNodeId());
    if (operator == null) {
      return cacheReusableOperator(node.getPlanNodeId(), super.visitTopK(node, context));
    }
    List<Operator> children = new ArrayList<>(node.getChildren().size());
    node.getChildren().forEach(child -> children.add(this.process(child, context)));
    return new TableTopKOperator((TableTopKOperator) operator, children);
  }

  @Override
  public Operator visitTopKRanking(TopKRankingNode node, LocalStreamExecutionPlanContext context) {
    Operator operator = reusableOperators.get(node.getPlanNodeId());
    if (operator == null) {
      return cacheReusableOperator(node.getPlanNodeId(), super.visitTopKRanking(node, context));
    }
    return new TopKRankingOperator(
        (TopKRankingOperator) operator, node.getChild().accept(this, context));
  }

  @Override
  public Operator visitRowNumber(RowNumberNode node, LocalStreamExecutionPlanContext context) {
    Operator operator = reusableOperators.get(node.getPlanNodeId());
    if (operator == null) {
      return cacheReusableOperator(node.getPlanNodeId(), super.visitRowNumber(node, context));
    }
    Operator child = node.getChild().accept(this, context);
    List<TSDataType> inputDataTypes =
        getOutputColumnTypes(node.getChild(), context.getTableTypeProvider());
    return new RowNumberOperator((RowNumberOperator) operator, child, inputDataTypes, 10_000);
  }

  @Override
  public Operator visitWindowFunction(WindowNode node, LocalStreamExecutionPlanContext context) {
    Operator operator = reusableOperators.get(node.getPlanNodeId());
    if (operator == null) {
      return cacheReusableOperator(
          node.getPlanNodeId(),
          new ReusableTableWindowOperator(
              (TableWindowOperator) super.visitWindowFunction(node, context)));
    }
    return new ReusableTableWindowOperator(
        (ReusableTableWindowOperator) operator, node.getChild().accept(this, context));
  }
}
