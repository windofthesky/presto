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
package com.facebook.presto.cost;

import com.facebook.presto.Session;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.statistics.ColumnStatistics;
import com.facebook.presto.spi.statistics.Estimate;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.LiteralInterpreter;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.tree.AstVisitor;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.ComparisonExpressionType;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Literal;
import com.facebook.presto.sql.tree.SymbolReference;

import javax.inject.Inject;

import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.spi.statistics.Estimate.zeroValue;

public class FilterStatsCalculator
{
    private final Metadata metadata;

    @Inject
    public FilterStatsCalculator(Metadata metadata)
    {
        this.metadata = metadata;
    }

    public PlanNodeStatsEstimate filterStats(
            PlanNodeStatsEstimate statsEstimate,
            Expression predicate,
            Session session,
            Map<Symbol, Type> types)
    {
        return new FilterExpressionStatsCalculatingVisitor(statsEstimate, session, types).process(predicate);
    }

    private class FilterExpressionStatsCalculatingVisitor
            extends AstVisitor<PlanNodeStatsEstimate, Void>
    {
        private final PlanNodeStatsEstimate input;
        private final Session session;
        private final Map<Symbol, Type> types;

        FilterExpressionStatsCalculatingVisitor(PlanNodeStatsEstimate input, Session session, Map<Symbol, Type> types)
        {
            this.input = input;
            this.session = session;
            this.types = types;
        }

        @Override
        protected PlanNodeStatsEstimate visitExpression(Expression node, Void context)
        {
            return filterForUnknownExpression();
        }

        private PlanNodeStatsEstimate filterForUnknownExpression()
        {
            return filterStatsByFactor(0.5);
        }

        @Override
        protected PlanNodeStatsEstimate visitComparisonExpression(ComparisonExpression node, Void context)
        {
            // FIXME left and right might not be exactly SymbolReference and Literal
            if (node.getLeft() instanceof SymbolReference && node.getRight() instanceof SymbolReference) {
                return comparisonSymbolToSymbolStats(
                        Symbol.from(node.getLeft()),
                        Symbol.from(node.getRight()),
                        node.getType()
                );
            }
            else if (node.getLeft() instanceof SymbolReference && node.getRight() instanceof Literal) {
                return comparisonSymbolToLiteralStats(
                        Symbol.from(node.getLeft()),
                        (Literal) node.getRight(),
                        node.getType()
                );
            }
            else if (node.getLeft() instanceof Literal && node.getRight() instanceof SymbolReference) {
                return comparisonSymbolToLiteralStats(
                        Symbol.from(node.getRight()),
                        (Literal) node.getLeft(),
                        node.getType().flip()
                );
            }
            else {
                return filterForUnknownExpression();
            }
        }

        private PlanNodeStatsEstimate comparisonSymbolToLiteralStats(Symbol left, Literal right, ComparisonExpressionType type)
        {
            Object literalValue = LiteralInterpreter.evaluate(metadata, session.toConnectorSession(), right);
            switch (type) {
                case EQUAL:
                    return symbolToLiteralEquality(left, literalValue);
                case NOT_EQUAL:
                    return symbolToLiteralNonEquality(left, literalValue);
                case LESS_THAN:
                case LESS_THAN_OR_EQUAL:
                    return symbolToLiteralLessThan(left, literalValue);
                case GREATER_THAN:
                case GREATER_THAN_OR_EQUAL:
                    return symbolToLiteralGreaterThan(left, literalValue);
                case IS_DISTINCT_FROM:
                    break;
            }
            return filterForUnknownExpression();
        }

        private PlanNodeStatsEstimate symbolToLiteralGreaterThan(Symbol symbol, Object literal)
        {
            TypeStatOperatorCaller operatorCaller = new TypeStatOperatorCaller(types.get(symbol), metadata.getFunctionRegistry(), session.toConnectorSession());
            ColumnStatistics symbolStats = input.getSymbolStatistics().get(symbol);

            SimplifiedHistogramStats histogram = SimplifiedHistogramStats.of(symbolStats, operatorCaller);
            SimplifiedHistogramStats newStats = histogram.intersect(new StatsHistogramRange(Optional.of(literal), Optional.empty(), operatorCaller, Optional.empty()));
            Estimate filtered = newStats.getFilteredPercent();
            return filterStatsByFactor(filtered.getValue()).mapSymbolColumnStatistics(symbol, x -> newStats.toColumnStatistics());
        }

        private PlanNodeStatsEstimate symbolToLiteralLessThan(Symbol symbol, Object literal)
        {
            TypeStatOperatorCaller operatorCaller = new TypeStatOperatorCaller(types.get(symbol), metadata.getFunctionRegistry(), session.toConnectorSession());
            ColumnStatistics symbolStats = input.getSymbolStatistics().get(symbol);

            SimplifiedHistogramStats histogram = SimplifiedHistogramStats.of(symbolStats, operatorCaller);
            SimplifiedHistogramStats newStats = histogram.intersect(new StatsHistogramRange(Optional.empty(), Optional.of(literal), operatorCaller, Optional.empty()));
            Estimate filtered = newStats.getFilteredPercent();
            return filterStatsByFactor(filtered.getValue()).mapSymbolColumnStatistics(symbol, x -> newStats.toColumnStatistics());
        }

        private PlanNodeStatsEstimate symbolToLiteralNonEquality(Symbol symbol, Object literal)
        {
            TypeStatOperatorCaller operatorCaller = new TypeStatOperatorCaller(types.get(symbol), metadata.getFunctionRegistry(), session.toConnectorSession());
            ColumnStatistics symbolStats = input.getSymbolStatistics().get(symbol);

            SimplifiedHistogramStats histogram = SimplifiedHistogramStats.of(symbolStats, operatorCaller);
            SimplifiedHistogramStats intersectStats = histogram.intersect(new StatsHistogramRange(Optional.of(literal), Optional.of(literal), operatorCaller, Optional.empty()));
            Estimate filtered = Estimate.of(1.0).subtract(intersectStats.getFilteredPercent());
            return filterStatsByFactor(filtered.getValue())
                    .mapSymbolColumnStatistics(symbol,
                            stats -> stats.asBuilder()
                                    .setNullsFraction(zeroValue())
                                    .clearRanges()
                                    .addRange(rb -> rb
                                            .setDataSize(stats.getOnlyRangeColumnStatistics().getDataSize().multiply(filtered))
                                            .setDistinctValuesCount(stats.getOnlyRangeColumnStatistics().getDistinctValuesCount().subtract(Estimate.of(1.0)))
                                            .build())
                                    .build());
        }

        private PlanNodeStatsEstimate symbolToLiteralEquality(Symbol symbol, Object literal)
        {
            TypeStatOperatorCaller operatorCaller = new TypeStatOperatorCaller(types.get(symbol), metadata.getFunctionRegistry(), session.toConnectorSession());
            ColumnStatistics symbolStats = input.getSymbolStatistics().get(symbol);

            SimplifiedHistogramStats histogram = SimplifiedHistogramStats.of(symbolStats, operatorCaller);
            SimplifiedHistogramStats newStats = histogram.intersect(new StatsHistogramRange(Optional.of(literal), Optional.of(literal), operatorCaller, Optional.empty()));
            Estimate filtered = newStats.getFilteredPercent();
            return filterStatsByFactor(filtered.getValue()).mapSymbolColumnStatistics(symbol, x -> newStats.toColumnStatistics());
        }

        private PlanNodeStatsEstimate filterStatsByFactor(double filterRate)
        {
            return input
                    .mapOutputRowCount(size -> size * filterRate);
        }

        private PlanNodeStatsEstimate comparisonSymbolToSymbolStats(Symbol left, Symbol right, ComparisonExpressionType type)
        {
            switch (type) {
                case EQUAL:
                    return symbolToSymbolEquality(left, right);
                case NOT_EQUAL:
                case LESS_THAN:
                case LESS_THAN_OR_EQUAL:
                case GREATER_THAN:
                case GREATER_THAN_OR_EQUAL:
                case IS_DISTINCT_FROM:
            }
            return filterStatsByFactor(0.5); //fixme
        }

        private PlanNodeStatsEstimate symbolToSymbolEquality(Symbol left, Symbol right)
        {
            if (!input.containsSymbolStats(left) || !input.containsSymbolStats(right)) {
                return filterStatsByFactor(0.5); //fixme
            }
            ColumnStatistics leftStats = input.getSymbolStatistics().get(left);
            ColumnStatistics rightStats = input.getSymbolStatistics().get(right);

            Estimate maxDistinctValues = Estimate.max(leftStats.getOnlyRangeColumnStatistics().getDistinctValuesCount(), rightStats.getOnlyRangeColumnStatistics().getDistinctValuesCount());
            Estimate minDistinctValues = Estimate.min(leftStats.getOnlyRangeColumnStatistics().getDistinctValuesCount(), rightStats.getOnlyRangeColumnStatistics().getDistinctValuesCount());

            double filterRate =
                    Estimate.of(1.0).divide(maxDistinctValues)
                            .multiply(Estimate.of(1.0).subtract(leftStats.getNullsFraction()))
                            .multiply(Estimate.of(1.0).subtract(rightStats.getNullsFraction())).getValue();

            ColumnStatistics newRightStats = rightStats.asBuilder()
                    .setNullsFraction(zeroValue())
                    .clearRanges()
                    .addRange(rightStats.getOnlyRangeColumnStatistics().asBuilder()
                            .setDistinctValuesCount(minDistinctValues)
                            .build())
                    .build();
            ColumnStatistics newLeftStats = leftStats.asBuilder()
                    .setNullsFraction(zeroValue())
                    .clearRanges()
                    .addRange(leftStats.getOnlyRangeColumnStatistics().asBuilder()
                            .setDistinctValuesCount(minDistinctValues)
                            .build())
                    .build();

            return filterStatsByFactor(filterRate)
                    .mapSymbolColumnStatistics(left, x -> newLeftStats)
                    .mapSymbolColumnStatistics(right, x -> newRightStats);
        }
    }
}
