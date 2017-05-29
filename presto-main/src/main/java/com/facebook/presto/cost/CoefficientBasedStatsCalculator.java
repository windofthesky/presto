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
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.statistics.ColumnStatistics;
import com.facebook.presto.spi.statistics.Estimate;
import com.facebook.presto.spi.statistics.RangeColumnStatistics;
import com.facebook.presto.spi.statistics.TableStatistics;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.DomainTranslator;
import com.facebook.presto.sql.planner.LiteralInterpreter;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.plan.EnforceSingleRowNode;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.LimitNode;
import com.facebook.presto.sql.planner.plan.OutputNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanVisitor;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.SemiJoinNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.planner.plan.ValuesNode;
import com.facebook.presto.sql.tree.AstVisitor;
import com.facebook.presto.sql.tree.BooleanLiteral;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.ComparisonExpressionType;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Literal;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.SymbolReference;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.cost.PlanNodeStatsEstimate.UNKNOWN_STATS;
import static com.facebook.presto.spi.statistics.Estimate.unknownValue;
import static com.facebook.presto.spi.statistics.Estimate.zeroValue;
import static com.google.common.collect.Iterables.getOnlyElement;

/**
 * Simple implementation of StatsCalculator. It make many arbitrary decisions (e.g filtering selectivity, join matching).
 * It serves POC purpose. To be replaced with more advanced implementation.
 */
@ThreadSafe
public class CoefficientBasedStatsCalculator
        implements StatsCalculator
{
    private static final Double FILTER_COEFFICIENT = 0.5;
    private static final Double JOIN_MATCHING_COEFFICIENT = 2.0;

    // todo some computation for outputSizeInBytes

    private final Metadata metadata;

    @Inject
    public CoefficientBasedStatsCalculator(Metadata metadata)
    {
        this.metadata = metadata;
    }

    @Override
    public PlanNodeStatsEstimate calculateStats(PlanNode planNode, Lookup lookup, Session session, Map<Symbol, Type> types)
    {
        Visitor visitor = new Visitor(lookup, session, types);
        return planNode.accept(visitor, null);
    }

    private class Visitor
            extends PlanVisitor<PlanNodeStatsEstimate, Void>
    {
        private final Lookup lookup;
        private final Session session;
        private final Map<Symbol, Type> types;

        public Visitor(Lookup lookup, Session session, Map<Symbol, Type> types)
        {
            this.lookup = lookup;
            this.session = session;
            this.types = ImmutableMap.copyOf(types);
        }

        private PlanNodeStatsEstimate lookupStats(PlanNode sourceNode)
        {
            return lookup.getStats(sourceNode, session, types);
        }

        @Override
        protected PlanNodeStatsEstimate visitPlan(PlanNode node, Void context)
        {
            // TODO: Explicitly visit GroupIdNode and throw an IllegalArgumentException
            // this can only be done once we get rid of the StatelessLookup
            return UNKNOWN_STATS;
        }

        @Override
        public PlanNodeStatsEstimate visitOutput(OutputNode node, Void context)
        {
            return lookupStats(node.getSource());
        }

        @Override
        public PlanNodeStatsEstimate visitFilter(FilterNode node, Void context)
        {
            Expression expr = node.getPredicate();
            return new FilterExpressionStatsCalculatingVisitor(lookupStats(node.getSource())).process(expr);
        }

        @Override
        public PlanNodeStatsEstimate visitProject(ProjectNode node, Void context)
        {
            return lookupStats(node.getSource());
        }

        @Override
        public PlanNodeStatsEstimate visitJoin(JoinNode node, Void context)
        {
            PlanNodeStatsEstimate leftStats = lookupStats(node.getLeft());
            PlanNodeStatsEstimate rightStats = lookupStats(node.getRight());

            if (node.getCriteria().size() == 1) {
                // FIXME, more complex criteria
                JoinNode.EquiJoinClause joinClause = getOnlyElement(node.getCriteria());
                Expression comparison = new ComparisonExpression(ComparisonExpressionType.EQUAL, joinClause.getLeft().toSymbolReference(), joinClause.getRight().toSymbolReference());
                PlanNodeStatsEstimate mergedInputCosts = crossJoinStats(leftStats, rightStats);
                return new FilterExpressionStatsCalculatingVisitor(mergedInputCosts).process(comparison);
            }

            PlanNodeStatsEstimate.Builder joinCost = PlanNodeStatsEstimate.builder();
            return joinCost.build();
        }

        public PlanNodeStatsEstimate crossJoinStats(PlanNodeStatsEstimate left, PlanNodeStatsEstimate right)
        {
            ImmutableMap.Builder<Symbol, ColumnStatistics> symbolsStatsBuilder = ImmutableMap.builder();
            symbolsStatsBuilder.putAll(left.getSymbolStatistics()).putAll(right.getSymbolStatistics());

            PlanNodeStatsEstimate.Builder statsBuilder = PlanNodeStatsEstimate.builder();
            return statsBuilder.setSymbolStatistics(symbolsStatsBuilder.build())
                    .setOutputRowCount(left.getOutputRowCount().multiply(right.getOutputRowCount()))
                    .setOutputSizeInBytes(left.getOutputSizeInBytes().multiply(right.getOutputRowCount())) // FIXME it shouldn't be order (left, right) dependent
                    .build();
        }

        @Override
        public PlanNodeStatsEstimate visitExchange(ExchangeNode node, Void context)
        {
            PlanNodeStatsEstimate estimateSum = lookupStats(node.getSources().get(0));
            for (int i = 1; i < node.getSources().size(); i++) {
                estimateSum = estimateSum.add(lookupStats(node.getSources().get(1)));
            }

            return estimateSum;
        }

        @Override
        public PlanNodeStatsEstimate visitTableScan(TableScanNode node, Void context)
        {
            Constraint<ColumnHandle> constraint = getConstraint(node, BooleanLiteral.TRUE_LITERAL);

            TableStatistics tableStatistics = metadata.getTableStatistics(session, node.getTable(), constraint);
            Map<Symbol, ColumnStatistics> outputSymbolStats = new HashMap<>();

            // TODO as stream
            for (Map.Entry<Symbol, ColumnHandle> entry : node.getAssignments().entrySet()) {
                Symbol symbol = entry.getKey();
                ColumnStatistics statistics = tableStatistics.getColumnStatistics().get(entry.getValue());
                if (statistics == null) {
                    statistics = ColumnStatistics.builder().addRange(
                                    RangeColumnStatistics.builder()
                                            .setDataSize(unknownValue())
                                            .setDistinctValuesCount(unknownValue())
                                            .setNullsFraction(unknownValue())
                                            .setHighValue(Optional.empty())
                                            .setLowValue(Optional.empty()).build()).build();
                }
                outputSymbolStats.put(symbol, statistics);
            }

            return PlanNodeStatsEstimate.builder()
                    .setOutputRowCount(tableStatistics.getRowCount())
                    .setSymbolStatistics(outputSymbolStats)
                    .build();
        }

        private Constraint<ColumnHandle> getConstraint(TableScanNode node, Expression predicate)
        {
            DomainTranslator.ExtractionResult decomposedPredicate = DomainTranslator.fromPredicate(
                    metadata,
                    session,
                    predicate,
                    types);

            TupleDomain<ColumnHandle> simplifiedConstraint = decomposedPredicate.getTupleDomain()
                    .transform(node.getAssignments()::get)
                    .intersect(node.getCurrentConstraint());

            return new Constraint<>(simplifiedConstraint, bindings -> true);
        }

        @Override
        public PlanNodeStatsEstimate visitValues(ValuesNode node, Void context)
        {
            Estimate valuesCount = new Estimate(node.getRows().size());
            return PlanNodeStatsEstimate.builder()
                    .setOutputRowCount(valuesCount)
                    .build();
        }

        @Override
        public PlanNodeStatsEstimate visitEnforceSingleRow(EnforceSingleRowNode node, Void context)
        {
            return PlanNodeStatsEstimate.builder()
                    .setOutputRowCount(new Estimate(1.0))
                    .build();
        }

        @Override
        public PlanNodeStatsEstimate visitSemiJoin(SemiJoinNode node, Void context)
        {
            PlanNodeStatsEstimate sourceStats = lookupStats(node.getSource());
            return sourceStats.mapOutputRowCount(rowCount -> rowCount * JOIN_MATCHING_COEFFICIENT);
        }

        @Override
        public PlanNodeStatsEstimate visitLimit(LimitNode node, Void context)
        {
            PlanNodeStatsEstimate sourceStats = lookupStats(node.getSource());
            PlanNodeStatsEstimate.Builder limitCost = PlanNodeStatsEstimate.builder();
            if (sourceStats.getOutputRowCount().getValue() < node.getCount()) {
                limitCost.setOutputRowCount(sourceStats.getOutputRowCount());
            }
            else {
                limitCost.setOutputRowCount(new Estimate(node.getCount()));
            }
            return limitCost.build();
        }

        private class FilterExpressionStatsCalculatingVisitor
                extends AstVisitor<PlanNodeStatsEstimate, Void>
        {
            PlanNodeStatsEstimate input;

            FilterExpressionStatsCalculatingVisitor(PlanNodeStatsEstimate input)
            {
                this.input = input;
            }

            @Override
            protected PlanNodeStatsEstimate visitNode(Node node, Void context)
            {
                return input;
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
                    // It should be collapsed already?
                    return input;
                }
            }

            private PlanNodeStatsEstimate comparisonSymbolToLiteralStats(Symbol left, Literal right, ComparisonExpressionType type)
            {
                Object literalValue = LiteralInterpreter.evaluate(metadata, session.toConnectorSession(), right);
                if (!input.containsSymbolStats(left)) {
                    return filterStatsByFactor(0.5); //fixme
                }
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
                return filterStatsByFactor(0.5); //fixme
            }

            private PlanNodeStatsEstimate symbolToLiteralGreaterThan(Symbol left, Object literalValue)
            {
                TypeStatOperatorCaller operatorCaller = new TypeStatOperatorCaller(types.get(left), metadata.getFunctionRegistry(), session.toConnectorSession());
                RangeColumnStatistics leftStats = input.getOnlyRangeStats(left);

                SimplifiedHistogramStats histogram = SimplifiedHistogramStats.of(ImmutableList.of(leftStats), operatorCaller);
                SimplifiedHistogramStats newStats = histogram.intersect(new StatsHistogramRange(Optional.of(literalValue), Optional.empty(), operatorCaller, Optional.empty()));
                Estimate filtered = newStats.getFilteredPercent();
                return filterStatsByFactor(filtered.getValue()).mapSymbolColumnStatistics(left, x -> newStats.toRangeColumnStatistics());
            }

            private PlanNodeStatsEstimate symbolToLiteralLessThan(Symbol left, Object literalValue)
            {
                TypeStatOperatorCaller operatorCaller = new TypeStatOperatorCaller(types.get(left), metadata.getFunctionRegistry(), session.toConnectorSession());
                RangeColumnStatistics leftStats = input.getOnlyRangeStats(left);

                SimplifiedHistogramStats histogram = SimplifiedHistogramStats.of(ImmutableList.of(leftStats), operatorCaller);
                SimplifiedHistogramStats newStats = histogram.intersect(new StatsHistogramRange(Optional.empty(), Optional.of(literalValue), operatorCaller, Optional.empty()));
                Estimate filtered = newStats.getFilteredPercent();
                return filterStatsByFactor(filtered.getValue()).mapSymbolColumnStatistics(left, x -> newStats.toRangeColumnStatistics());
            }

            private PlanNodeStatsEstimate symbolToLiteralNonEquality(Symbol left, Object literalValue)
            {
                TypeStatOperatorCaller operatorCaller = new TypeStatOperatorCaller(types.get(left), metadata.getFunctionRegistry(), session.toConnectorSession());
                RangeColumnStatistics leftStats = input.getOnlyRangeStats(left);

                SimplifiedHistogramStats histogram = SimplifiedHistogramStats.of(ImmutableList.of(leftStats), operatorCaller);
                SimplifiedHistogramStats intersectStats = histogram.intersect(new StatsHistogramRange(Optional.of(literalValue), Optional.of(literalValue), operatorCaller, Optional.empty()));
                Estimate filtered = Estimate.of(1.0).subtract(intersectStats.getFilteredPercent());
                return filterStatsByFactor(filtered.getValue())
                        .mapSymbolColumnStatistics(left,
                                x -> x.builderFrom()
                                        .setDataSize(x.getDataSize().multiply(filtered))
                                        .setDistinctValuesCount(x.getDistinctValuesCount().subtract(Estimate.of(1.0)))
                                        .setNullsFraction(zeroValue())
                                        .build());
            }

            private PlanNodeStatsEstimate symbolToLiteralEquality(Symbol left, Object literalValue)
            {
                TypeStatOperatorCaller operatorCaller = new TypeStatOperatorCaller(types.get(left), metadata.getFunctionRegistry(), session.toConnectorSession());
                RangeColumnStatistics leftStats = input.getOnlyRangeStats(left);

                SimplifiedHistogramStats histogram = SimplifiedHistogramStats.of(ImmutableList.of(leftStats), operatorCaller);
                SimplifiedHistogramStats newStats = histogram.intersect(new StatsHistogramRange(Optional.of(literalValue), Optional.of(literalValue), operatorCaller, Optional.empty()));
                Estimate filtered = newStats.getFilteredPercent();
                return filterStatsByFactor(filtered.getValue()).mapSymbolColumnStatistics(left, x -> newStats.toRangeColumnStatistics());
            }

            private PlanNodeStatsEstimate filterStatsByFactor(double filterRate)
            {
                return input
                        .mapOutputRowCount(size -> size * filterRate)
                        .mapOutputSizeInBytes(size -> size * filterRate);
            }

            private PlanNodeStatsEstimate comparisonSymbolToSymbolStats(Symbol left, Symbol right, ComparisonExpressionType type)
            {
                if (!input.containsSymbolStats(left) || !input.containsSymbolStats(right)) {
                    return filterStatsByFactor(0.5); //fixme
                }
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
                RangeColumnStatistics leftStats = input.getOnlyRangeStats(left);
                RangeColumnStatistics rightStats = input.getOnlyRangeStats(right);

                Estimate maxDistinctValues = Estimate.max(leftStats.getDistinctValuesCount(), rightStats.getDistinctValuesCount());
                Estimate minDistinctValues = Estimate.min(leftStats.getDistinctValuesCount(), rightStats.getDistinctValuesCount());

                double filterRate =
                        Estimate.of(1.0).divide(maxDistinctValues)
                        .multiply(Estimate.of(1.0).subtract(leftStats.getNullsFraction()))
                        .multiply(Estimate.of(1.0).subtract(rightStats.getNullsFraction())).getValue();

                RangeColumnStatistics newRightStats = rightStats.builderFrom().setNullsFraction(zeroValue()).setDistinctValuesCount(minDistinctValues).build();
                RangeColumnStatistics newLeftStats = leftStats.builderFrom().setNullsFraction(zeroValue()).setDistinctValuesCount(minDistinctValues).build();

                return filterStatsByFactor(filterRate)
                        .mapSymbolColumnStatistics(left, x -> newLeftStats)
                        .mapSymbolColumnStatistics(right, x -> newRightStats);
            }
        }
    }
}
