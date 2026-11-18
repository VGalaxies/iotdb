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

package org.apache.iotdb.streamnode.engine.reuse;

import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.commons.queryengine.plan.relational.metadata.ColumnSchema;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.AggregationNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.EventScanNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.FilterNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.ProjectNode;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.DoubleLiteral;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.LongLiteral;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.SymbolReference;
import org.apache.iotdb.commons.queryengine.plan.relational.type.InternalTypeManager;
import org.apache.iotdb.commons.stream.IoTDBSubscriptionSource;
import org.apache.iotdb.commons.stream.StreamProperties;
import org.apache.iotdb.commons.stream.StreamTask;
import org.apache.iotdb.commons.stream.TumbleWindow;

import org.apache.tsfile.enums.TSDataType;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * 5-point structural & semantic compatibility verifier for cross-stream base-slice reuse (§4.5):
 * source/filter/partitioning, window containment & phase alignment, watermark & lateness bounds,
 * pre-aggregation transform equivalence, and mergeable aggregate compatibility (including
 * multi-quantile KLL sharing).
 */
public final class SliceCompatibilitySignature {

  private static final Set<String> MERGEABLE_FUNCTIONS =
      Collections.unmodifiableSet(
          new HashSet<>(
              Arrays.asList(
                  "count",
                  "count_all",
                  "sum",
                  "avg",
                  "min",
                  "max",
                  "extreme",
                  "first",
                  "first_by",
                  "last",
                  "last_by",
                  "min_by",
                  "max_by",
                  "stddev",
                  "stddev_pop",
                  "stddev_samp",
                  "variance",
                  "var_pop",
                  "var_samp",
                  "corr",
                  "covar_pop",
                  "covar_samp",
                  "approx_count_distinct",
                  "approx_percentile")));

  private SliceCompatibilitySignature() {}

  public static final class AggSpec {
    private final String functionName;
    private final List<String> inputColumns;
    private final boolean distinct;
    private final @Nullable Double percentileArg;
    private final TSDataType outputType;
    private final boolean hasFilterOrMask;

    public AggSpec(
        String functionName,
        List<String> inputColumns,
        boolean distinct,
        @Nullable Double percentileArg,
        TSDataType outputType,
        boolean hasFilterOrMask) {
      this.functionName = Objects.requireNonNull(functionName).toLowerCase(Locale.ENGLISH);
      this.inputColumns = Collections.unmodifiableList(new ArrayList<>(inputColumns));
      this.distinct = distinct;
      this.percentileArg = percentileArg;
      this.outputType = outputType;
      this.hasFilterOrMask = hasFilterOrMask;
    }

    public String getFunctionName() {
      return functionName;
    }

    public List<String> getInputColumns() {
      return inputColumns;
    }

    public boolean isDistinct() {
      return distinct;
    }

    public @Nullable Double getPercentileArg() {
      return percentileArg;
    }

    public TSDataType getOutputType() {
      return outputType;
    }

    public boolean isHasFilterOrMask() {
      return hasFilterOrMask;
    }
  }

  public static final class SliceTaskDescriptor {
    private final String taskName;
    private final String database;
    private final String tableName;
    private final String normalizedPreFilter;
    private final List<String> partitionColumns;
    private final boolean leftClosed;
    private final long windowSizeMs;
    private final long originMs;
    private final long watermarkMs;
    private final long allowedLatenessMs;
    private final long maxDelayWindowCloseMs;
    private final String normalizedPreAggPlan;
    private final String sketchType;
    private final int kllK;
    private final long sliceRetentionMs;
    private final double costThreshold;
    private final boolean enableSliceReuse;
    private final boolean globalSingleAggregation;
    private final List<AggSpec> aggregates;

    public SliceTaskDescriptor(
        String taskName,
        String database,
        String tableName,
        String normalizedPreFilter,
        List<String> partitionColumns,
        boolean leftClosed,
        long windowSizeMs,
        long originMs,
        long watermarkMs,
        long allowedLatenessMs,
        long maxDelayWindowCloseMs,
        String normalizedPreAggPlan,
        String sketchType,
        int kllK,
        long sliceRetentionMs,
        double costThreshold,
        boolean enableSliceReuse,
        boolean globalSingleAggregation,
        List<AggSpec> aggregates) {
      this.taskName = Objects.requireNonNull(taskName);
      this.database = database == null ? "" : database;
      this.tableName = tableName == null ? "" : tableName;
      this.normalizedPreFilter = normalizedPreFilter == null ? "" : normalizedPreFilter;
      this.partitionColumns =
          partitionColumns == null
              ? Collections.emptyList()
              : Collections.unmodifiableList(new ArrayList<>(partitionColumns));
      this.leftClosed = leftClosed;
      this.windowSizeMs = windowSizeMs;
      this.originMs = originMs;
      this.watermarkMs = watermarkMs;
      this.allowedLatenessMs = allowedLatenessMs;
      this.maxDelayWindowCloseMs = maxDelayWindowCloseMs;
      this.normalizedPreAggPlan = normalizedPreAggPlan == null ? "" : normalizedPreAggPlan;
      this.sketchType = sketchType == null ? "tdigest" : sketchType.toLowerCase(Locale.ENGLISH);
      this.kllK = kllK;
      this.sliceRetentionMs = sliceRetentionMs;
      this.costThreshold = costThreshold;
      this.enableSliceReuse = enableSliceReuse;
      this.globalSingleAggregation = globalSingleAggregation;
      this.aggregates =
          aggregates == null
              ? Collections.emptyList()
              : Collections.unmodifiableList(new ArrayList<>(aggregates));
    }

    public String getTaskName() {
      return taskName;
    }

    public String getDatabase() {
      return database;
    }

    public String getTableName() {
      return tableName;
    }

    public String getNormalizedPreFilter() {
      return normalizedPreFilter;
    }

    public List<String> getPartitionColumns() {
      return partitionColumns;
    }

    public boolean isLeftClosed() {
      return leftClosed;
    }

    public long getWindowSizeMs() {
      return windowSizeMs;
    }

    public long getOriginMs() {
      return originMs;
    }

    public long getWatermarkMs() {
      return watermarkMs;
    }

    public long getAllowedLatenessMs() {
      return allowedLatenessMs;
    }

    public long getMaxDelayWindowCloseMs() {
      return maxDelayWindowCloseMs;
    }

    public String getNormalizedPreAggPlan() {
      return normalizedPreAggPlan;
    }

    public String getSketchType() {
      return sketchType;
    }

    public int getKllK() {
      return kllK;
    }

    public long getSliceRetentionMs() {
      return sliceRetentionMs;
    }

    public double getCostThreshold() {
      return costThreshold;
    }

    public boolean isEnableSliceReuse() {
      return enableSliceReuse;
    }

    public boolean isGlobalSingleAggregation() {
      return globalSingleAggregation;
    }

    public List<AggSpec> getAggregates() {
      return aggregates;
    }
  }

  /**
   * Evaluates 5-point compatibility and returns a mapping from each consumer aggregate index to the
   * matching producer aggregate index, or {@code null} if incompatible.
   */
  public static @Nullable int[] matchProducerToConsumer(
      SliceTaskDescriptor producer, SliceTaskDescriptor consumer) {
    if (producer == null || consumer == null) {
      return null;
    }
    if (!producer.isEnableSliceReuse() || !consumer.isEnableSliceReuse()) {
      return null;
    }
    if (!producer.isGlobalSingleAggregation() || !consumer.isGlobalSingleAggregation()) {
      return null;
    }
    if (producer.getAggregates().isEmpty() || consumer.getAggregates().isEmpty()) {
      return null;
    }

    // 1. Source / PreFilter / Partition / Boundary semantics
    if (!Objects.equals(producer.getDatabase(), consumer.getDatabase())
        || !Objects.equals(producer.getTableName(), consumer.getTableName())
        || !Objects.equals(producer.getNormalizedPreFilter(), consumer.getNormalizedPreFilter())
        || !Objects.equals(producer.getPartitionColumns(), consumer.getPartitionColumns())
        || producer.isLeftClosed() != consumer.isLeftClosed()) {
      return null;
    }

    // 2. Window containment & phase alignment: w_c = m * w_p (m >= 2) and (o_c - o_p) mod w_p == 0
    long wp = producer.getWindowSizeMs();
    long wc = consumer.getWindowSizeMs();
    if (wp <= 0 || wc < 2 * wp || (wc % wp) != 0) {
      return null;
    }
    if (Math.floorMod(consumer.getOriginMs() - producer.getOriginMs(), wp) != 0) {
      return null;
    }

    // 3. Watermark & Lateness compatibility
    if (consumer.getWatermarkMs() < producer.getWatermarkMs()
        || producer.getAllowedLatenessMs() < consumer.getAllowedLatenessMs()) {
      return null;
    }

    // 4. Pre-aggregation transform equivalence
    if (!Objects.equals(producer.getNormalizedPreAggPlan(), consumer.getNormalizedPreAggPlan())) {
      return null;
    }

    // 5. Mergeable operator compatibility (incl. quantile-agnostic KLL sharing)
    List<AggSpec> cAggs = consumer.getAggregates();
    List<AggSpec> pAggs = producer.getAggregates();
    int[] mapping = new int[cAggs.size()];
    for (int i = 0; i < cAggs.size(); i++) {
      AggSpec c = cAggs.get(i);
      if (c.isDistinct() || c.isHasFilterOrMask() || !isMergeableFunction(c.getFunctionName())) {
        return null;
      }
      int matchedIdx = -1;
      for (int j = 0; j < pAggs.size(); j++) {
        AggSpec p = pAggs.get(j);
        if (isAggregateCompatible(p, c, producer.getSketchType(), consumer.getSketchType())) {
          matchedIdx = j;
          break;
        }
      }
      if (matchedIdx < 0) {
        return null;
      }
      mapping[i] = matchedIdx;
    }
    return mapping;
  }

  public static boolean isMergeableFunction(String funcName) {
    return funcName != null && MERGEABLE_FUNCTIONS.contains(funcName.toLowerCase(Locale.ENGLISH));
  }

  private static boolean isAggregateCompatible(
      AggSpec producerAgg, AggSpec consumerAgg, String producerSketch, String consumerSketch) {
    if (producerAgg.isDistinct() || producerAgg.isHasFilterOrMask()) {
      return false;
    }
    String pFunc = normalizeCountName(producerAgg.getFunctionName());
    String cFunc = normalizeCountName(consumerAgg.getFunctionName());
    if (!Objects.equals(pFunc, cFunc)) {
      return false;
    }
    if (!Objects.equals(producerAgg.getInputColumns(), consumerAgg.getInputColumns())) {
      return false;
    }
    if ("approx_percentile".equals(cFunc)) {
      if ("kll".equalsIgnoreCase(producerSketch) && "kll".equalsIgnoreCase(consumerSketch)) {
        return true;
      }
      return Objects.equals(producerSketch, consumerSketch)
          && Objects.equals(producerAgg.getPercentileArg(), consumerAgg.getPercentileArg());
    }
    return true;
  }

  private static String normalizeCountName(String name) {
    return "count_all".equals(name) ? "count" : name;
  }

  /** Extracts a {@link SliceTaskDescriptor} from a compiled {@link StreamTask}. */
  public static @Nullable SliceTaskDescriptor fromStreamTask(
      StreamTask task, PlanNode rootPlanNode) {
    if (task == null || rootPlanNode == null) {
      return null;
    }
    if (!(task.getWindow() instanceof TumbleWindow)
        || !(task.getSource() instanceof IoTDBSubscriptionSource)) {
      return null;
    }
    StreamProperties props = task.getProperties();
    boolean enabled =
        props != null && props.getBooleanExtraProperty(StreamProperties.ENABLE_SLICE_REUSE, false);
    if (!enabled) {
      return null;
    }

    IoTDBSubscriptionSource source = (IoTDBSubscriptionSource) task.getSource();
    TumbleWindow tumble = (TumbleWindow) task.getWindow();
    AggregationNode aggNode = findGlobalAggregationNode(rootPlanNode);
    boolean isGlobalSingle = aggNode != null && aggNode.hasSingleGlobalAggregation();
    StringBuilder preAggBuilder = new StringBuilder();
    Map<Symbol, String> symbolToColumn = new HashMap<>();
    Map<Symbol, Expression> constantBindings = new HashMap<>();
    if (aggNode != null) {
      extractPreAggMetadata(aggNode.getChild(), preAggBuilder, symbolToColumn, constantBindings);
    }

    List<AggSpec> aggSpecs = new ArrayList<>();
    if (aggNode != null) {
      for (Symbol outputSym : aggNode.getOutputSymbols()) {
        AggregationNode.Aggregation agg = aggNode.getAggregations().get(outputSym);
        if (agg == null) {
          continue;
        }
        String fn = agg.getResolvedFunction().getSignature().getName().toLowerCase(Locale.ENGLISH);
        List<String> inputCols = new ArrayList<>();
        Double pctArg = null;
        for (Expression arg : agg.getArguments()) {
          if (arg instanceof SymbolReference) {
            Symbol s = Symbol.from(arg);
            Expression constExpr = constantBindings.get(s);
            if ("approx_percentile".equals(fn) && constExpr != null) {
              if (constExpr instanceof DoubleLiteral) {
                pctArg = ((DoubleLiteral) constExpr).getValue();
              } else if (constExpr instanceof LongLiteral) {
                pctArg = (double) ((LongLiteral) constExpr).getParsedValue();
              }
            } else {
              inputCols.add(symbolToColumn.getOrDefault(s, s.getName()));
            }
          } else if ("approx_percentile".equals(fn) && arg instanceof DoubleLiteral) {
            pctArg = ((DoubleLiteral) arg).getValue();
          } else if ("approx_percentile".equals(fn) && arg instanceof LongLiteral) {
            pctArg = (double) ((LongLiteral) arg).getParsedValue();
          } else {
            inputCols.add(arg.toString());
          }
        }
        TSDataType outType =
            task.getTypeProvider() != null
                ? InternalTypeManager.getTSDataType(
                    task.getTypeProvider().getTableModelType(outputSym))
                : TSDataType.DOUBLE;
        aggSpecs.add(
            new AggSpec(
                fn,
                inputCols,
                agg.isDistinct(),
                pctArg,
                outType,
                agg.getFilter().isPresent() || agg.hasMask()));
      }
    }

    String sketchType = props.getStringExtraProperty(StreamProperties.REUSE_SKETCH_TYPE, "tdigest");
    int kllK = props.getIntExtraProperty(StreamProperties.REUSE_KLL_K, 200);
    long retentionMs = props.getLongExtraProperty(StreamProperties.SLICE_RETENTION_MS, 120_000L);
    double costThreshold = props.getDoubleExtraProperty(StreamProperties.REUSE_COST_THRESHOLD, 0.0);

    return new SliceTaskDescriptor(
        task.getTaskName(),
        source.getDatabase(),
        source.getTableName(),
        source.getPreFilter() != null ? source.getPreFilter().toString() : "",
        source.getPartitionColumns(),
        props.isLeftClosed(),
        tumble.getSize(),
        tumble.getOrigin(),
        props.getWatermark(),
        props.getAllowedLateness(),
        props.getMaxDelayWindowCloseMs(),
        preAggBuilder.toString(),
        sketchType,
        kllK,
        retentionMs,
        costThreshold,
        true,
        isGlobalSingle,
        aggSpecs);
  }

  public static @Nullable AggregationNode findGlobalAggregationNode(PlanNode node) {
    if (node == null) {
      return null;
    }
    if (node instanceof AggregationNode) {
      return (AggregationNode) node;
    }
    for (PlanNode child : node.getChildren()) {
      AggregationNode found = findGlobalAggregationNode(child);
      if (found != null) {
        return found;
      }
    }
    return null;
  }

  public static void extractPreAggMetadata(
      PlanNode node,
      StringBuilder preAggBuilder,
      Map<Symbol, String> symbolToColumn,
      Map<Symbol, Expression> constantBindings) {
    if (node == null) {
      return;
    }
    for (PlanNode child : node.getChildren()) {
      extractPreAggMetadata(child, preAggBuilder, symbolToColumn, constantBindings);
    }
    if (node instanceof EventScanNode) {
      EventScanNode scan = (EventScanNode) node;
      for (Map.Entry<Symbol, ColumnSchema> e : scan.getAssignments().entrySet()) {
        symbolToColumn.put(e.getKey(), e.getValue().getName());
      }
    } else if (node instanceof FilterNode) {
      FilterNode filter = (FilterNode) node;
      preAggBuilder.append("FILTER(").append(filter.getPredicate()).append(");");
    } else if (node instanceof ProjectNode) {
      ProjectNode project = (ProjectNode) node;
      for (Map.Entry<Symbol, Expression> e : project.getAssignments().getMap().entrySet()) {
        Expression expr = e.getValue();
        if (expr instanceof DoubleLiteral || expr instanceof LongLiteral) {
          constantBindings.put(e.getKey(), expr);
        } else if (expr instanceof SymbolReference) {
          Symbol target = Symbol.from(expr);
          symbolToColumn.put(e.getKey(), symbolToColumn.getOrDefault(target, target.getName()));
        } else {
          String repr = expr.toString();
          symbolToColumn.put(e.getKey(), repr);
          preAggBuilder.append("PROJ(").append(repr).append(");");
        }
      }
    }
  }
}
