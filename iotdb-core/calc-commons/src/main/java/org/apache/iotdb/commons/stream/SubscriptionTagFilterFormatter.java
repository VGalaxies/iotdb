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

package org.apache.iotdb.commons.stream;

import org.apache.iotdb.calc.i18n.CalcMessages;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.BooleanLiteral;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.CommonQueryAstVisitor;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.ComparisonExpression;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.DereferenceExpression;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.FunctionCall;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Identifier;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.InListExpression;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.InPredicate;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.IsNotNullPredicate;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.IsNullPredicate;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.LikePredicate;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.LogicalExpression;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Node;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.NotExpression;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.QualifiedName;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.StringLiteral;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.SymbolReference;

import java.util.Set;
import java.util.stream.Collectors;

/**
 * Formats a stream source pre-filter expression into the restricted subscription tag-filter syntax.
 * The emitted string is expected to be accepted by the subscription tag-filter parser.
 */
public final class SubscriptionTagFilterFormatter {

  private static final String REGEXP_LIKE = "regexp_like";

  private static final Set<String> RESERVED_WORDS =
      Set.of("TRUE", "FALSE", "AND", "OR", "NOT", "IN", "LIKE", "REGEXP", "IS", "NULL", "ESCAPE");

  private SubscriptionTagFilterFormatter() {}

  public static String format(final Expression expression) {
    return new Formatter().process(expression);
  }

  private static final class Formatter implements CommonQueryAstVisitor<String, Void> {

    @Override
    public String visitBooleanLiteral(final BooleanLiteral node, final Void context) {
      return String.valueOf(node.getValue());
    }

    @Override
    public String visitStringLiteral(final StringLiteral node, final Void context) {
      return quoteString(node.getValue());
    }

    @Override
    public String visitIdentifier(final Identifier node, final Void context) {
      return formatIdentifier(node.getValue(), node.isDelimited());
    }

    @Override
    public String visitSymbolReference(final SymbolReference node, final Void context) {
      return formatIdentifier(node.getName(), false);
    }

    @Override
    public String visitDereferenceExpression(final DereferenceExpression node, final Void context) {
      final QualifiedName qualifiedName = DereferenceExpression.getQualifiedName(node);
      if (qualifiedName == null) {
        throw unsupported(node);
      }
      return formatIdentifier(qualifiedName.getSuffix(), false);
    }

    @Override
    public String visitLogicalExpression(final LogicalExpression node, final Void context) {
      return "("
          + node.getTerms().stream()
              .map(this::process)
              .collect(Collectors.joining(" " + node.getOperator() + " "))
          + ")";
    }

    @Override
    public String visitNotExpression(final NotExpression node, final Void context) {
      return "(NOT " + process(node.getValue()) + ")";
    }

    @Override
    public String visitComparisonExpression(final ComparisonExpression node, final Void context) {
      if (node.getOperator() != ComparisonExpression.Operator.EQUAL
          && node.getOperator() != ComparisonExpression.Operator.NOT_EQUAL) {
        throw unsupported(node);
      }
      return process(node.getLeft())
          + " "
          + node.getOperator().getValue()
          + " "
          + process(node.getRight());
    }

    @Override
    public String visitInPredicate(final InPredicate node, final Void context) {
      if (!(node.getValueList() instanceof InListExpression)) {
        throw unsupported(node);
      }
      final String values =
          ((InListExpression) node.getValueList())
              .getValues().stream().map(this::process).collect(Collectors.joining(", "));
      return "(" + process(node.getValue()) + " IN (" + values + "))";
    }

    @Override
    public String visitLikePredicate(final LikePredicate node, final Void context) {
      final StringBuilder builder =
          new StringBuilder("(")
              .append(process(node.getValue()))
              .append(" LIKE ")
              .append(process(node.getPattern()));
      node.getEscape().ifPresent(escape -> builder.append(" ESCAPE ").append(process(escape)));
      return builder.append(')').toString();
    }

    @Override
    public String visitIsNullPredicate(final IsNullPredicate node, final Void context) {
      return "(" + process(node.getValue()) + " IS NULL)";
    }

    @Override
    public String visitIsNotNullPredicate(final IsNotNullPredicate node, final Void context) {
      return "(" + process(node.getValue()) + " IS NOT NULL)";
    }

    @Override
    public String visitFunctionCall(final FunctionCall node, final Void context) {
      if (REGEXP_LIKE.equalsIgnoreCase(node.getName().toString())
          && !node.isDistinct()
          && !node.getWindow().isPresent()
          && !node.getProcessingMode().isPresent()
          && !node.getNullTreatment().isPresent()
          && node.getArguments().size() == 2) {
        return "("
            + process(node.getArguments().get(0))
            + " REGEXP "
            + process(node.getArguments().get(1))
            + ")";
      }
      throw unsupported(node);
    }

    @Override
    public String visitExpression(final Expression node, final Void context) {
      throw unsupported(node);
    }

    @Override
    public String visitNode(final Node node, final Void context) {
      throw unsupported(node);
    }

    private static IllegalArgumentException unsupported(final Node node) {
      return new IllegalArgumentException(
          String.format(
              CalcMessages.EXCEPTION_UNSUPPORTED_EXPRESSION_ARG_3E55A85A,
              node.getClass().getSimpleName()));
    }
  }

  private static String quoteString(final String value) {
    return "\"" + value.replace("\"", "\"\"") + "\"";
  }

  private static String formatIdentifier(final String value, final boolean delimited) {
    if (!delimited && !RESERVED_WORDS.contains(value.toUpperCase()) && isSimpleIdentifier(value)) {
      return value;
    }
    return "\"" + value.replace("\"", "\"\"") + "\"";
  }

  private static boolean isSimpleIdentifier(final String value) {
    if (value.isEmpty() || !isIdentifierStart(value.charAt(0))) {
      return false;
    }
    for (int i = 1; i < value.length(); i++) {
      if (!isIdentifierPart(value.charAt(i))) {
        return false;
      }
    }
    return true;
  }

  private static boolean isIdentifierStart(final char c) {
    return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_';
  }

  private static boolean isIdentifierPart(final char c) {
    return isIdentifierStart(c) || (c >= '0' && c <= '9');
  }
}
