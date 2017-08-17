/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.sql.planner;

import com.facebook.presto.sql.tree.AstVisitor;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.SymbolReference;
import com.google.common.collect.ImmutableSet;

import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.sql.tree.LogicalBinaryExpression.Type.AND;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Collections.singleton;
import static java.util.Objects.requireNonNull;

/**
 * Currently this class handles simple expressions like:
 * <p>
 * A.a < f(B.x, B.y, B.z)
 * and queries with range predicates involving a single build symbol
 * reference (A.a) like:
 * A.a < f(B.x) AND A.a > g(B.y)
 * <p>
 * where a is the build side symbol reference and x,y,z are probe
 * side symbol references.
 * <p>
 * It could be extended to handle any expressions like:
 * <p>
 * A.a * sin(A.b) / log(B.x) < cos(B.z)
 * <p>
 * by transforming it to:
 * <p>
 * f(A.a, A.b) < g(B.x, B.z)
 * <p>
 * Where f(...) and g(...) would be some functions/expressions. That
 * would allow us to perform binary search on arbitrary complex expressions
 * by sorting position links according to the result of f(...) function.
 */
public final class SortExpressionExtractor
{
    private SortExpressionExtractor() {}

    public static Optional<SortExpressionContext> extractSortExpression(Set<Symbol> buildSymbols, Expression filter)
    {
        if (!DeterminismEvaluator.isDeterministic(filter)) {
            return Optional.empty();
        }

        return new SortExpressionVisitor(buildSymbols).process(filter);
    }

    private static class SortExpressionVisitor
            extends AstVisitor<Optional<SortExpressionContext>, Void>
    {
        private final Set<Symbol> buildSymbols;

        public SortExpressionVisitor(Set<Symbol> buildSymbols)
        {
            this.buildSymbols = buildSymbols;
        }

        @Override
        protected Optional<SortExpressionContext> visitExpression(Expression expression, Void context)
        {
            return Optional.empty();
        }

        @Override
        protected Optional<SortExpressionContext> visitLogicalBinaryExpression(LogicalBinaryExpression binaryExpression, Void context)
        {
            if (binaryExpression.getType() != AND) {
                return Optional.empty();
            }
            Optional<SortExpressionContext> leftProcessed = process(binaryExpression.getLeft());
            Optional<SortExpressionContext> rightProcessed = process(binaryExpression.getRight());

            if (!leftProcessed.isPresent() || !rightProcessed.isPresent() || !leftProcessed.get().getSortExpression().equals(rightProcessed.get().getSortExpression())) {
                return Optional.empty();
            }
            return Optional.of(merge(leftProcessed.get(), rightProcessed.get()));
        }

        private SortExpressionContext merge(SortExpressionContext left, SortExpressionContext right)
        {
            checkArgument(left.getSortExpression().equals(right.getSortExpression()));
            ImmutableSet.Builder<Expression> searchExpressions = ImmutableSet.builder();
            searchExpressions.addAll(left.getSearchExpressions());
            searchExpressions.addAll(right.getSearchExpressions());
            return new SortExpressionContext(left.getSortExpression(), searchExpressions.build());
        }

        @Override
        protected Optional<SortExpressionContext> visitComparisonExpression(ComparisonExpression comparison, Void context)
        {
            switch (comparison.getType()) {
                case GREATER_THAN:
                case GREATER_THAN_OR_EQUAL:
                case LESS_THAN:
                case LESS_THAN_OR_EQUAL:
                    Optional<SymbolReference> sortChannel = asBuildSymbolReference(buildSymbols, comparison.getRight());
                    boolean hasBuildReferencesOnOtherSide = hasBuildSymbolReference(buildSymbols, comparison.getLeft());
                    if (!sortChannel.isPresent()) {
                        sortChannel = asBuildSymbolReference(buildSymbols, comparison.getLeft());
                        hasBuildReferencesOnOtherSide = hasBuildSymbolReference(buildSymbols, comparison.getRight());
                    }
                    if (sortChannel.isPresent() && !hasBuildReferencesOnOtherSide) {
                        return sortChannel.map(symbolReference -> new SortExpressionContext(symbolReference, singleton(comparison)));
                    }
                    return Optional.empty();
                default:
                    return Optional.empty();
            }
        }
    }

    private static Optional<SymbolReference> asBuildSymbolReference(Set<Symbol> buildLayout, Expression expression)
    {
        // Currently only we support only symbol as sort expression on build side
        if (expression instanceof SymbolReference) {
            SymbolReference symbolReference = (SymbolReference) expression;
            if (buildLayout.contains(new Symbol(symbolReference.getName()))) {
                return Optional.of(symbolReference);
            }
        }
        return Optional.empty();
    }

    private static boolean hasBuildSymbolReference(Set<Symbol> buildSymbols, Expression expression)
    {
        return new BuildSymbolReferenceFinder(buildSymbols).process(expression);
    }

    private static class BuildSymbolReferenceFinder
            extends AstVisitor<Boolean, Void>
    {
        private final Set<String> buildSymbols;

        public BuildSymbolReferenceFinder(Set<Symbol> buildSymbols)
        {
            this.buildSymbols = requireNonNull(buildSymbols, "buildSymbols is null").stream()
                    .map(Symbol::getName)
                    .collect(toImmutableSet());
        }

        @Override
        protected Boolean visitNode(Node node, Void context)
        {
            for (Node child : node.getChildren()) {
                if (process(child, context)) {
                    return true;
                }
            }
            return false;
        }

        @Override
        protected Boolean visitSymbolReference(SymbolReference symbolReference, Void context)
        {
            return buildSymbols.contains(symbolReference.getName());
        }
    }
}
