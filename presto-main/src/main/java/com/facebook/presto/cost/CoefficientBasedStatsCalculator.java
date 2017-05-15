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
import com.facebook.presto.sql.planner.plan.EnforceSingleRowNode;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.LimitNode;
import com.facebook.presto.sql.planner.plan.OutputNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
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
import com.facebook.presto.sql.tree.SymbolReference;
import com.google.common.collect.ImmutableMap;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.cost.PlanNodeStatsEstimate.UNKNOWN_STATS;
import static com.facebook.presto.spi.statistics.Estimate.zeroValue;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.getOnlyElement;

/**
 * Simple implementation of CostCalculator. It make many arbitrary decisions (e.g filtering selectivity, join matching).
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
    public PlanNodeStatsEstimate calculateStats(
            PlanNode planNode,
            List<PlanNodeStatsEstimate> sourceCosts,
            Session session,
            Map<Symbol, Type> types)
    {
        Visitor visitor = new Visitor(session, types);
        return planNode.accept(visitor, sourceCosts);
    }

    private class Visitor
            extends PlanVisitor<List<PlanNodeStatsEstimate>, PlanNodeStatsEstimate>
    {
        private final Session session;
        private final Map<PlanNodeId, PlanNodeStatsEstimate> statsEstimateMap;
        private final Map<Symbol, Type> types;

        public Visitor(Session session, Map<Symbol, Type> types)
        {
            this.statsEstimateMap = new HashMap<>();
            this.session = session;
            this.types = ImmutableMap.copyOf(types);
        }

        @Override
        protected PlanNodeStatsEstimate visitPlan(PlanNode node, List<PlanNodeStatsEstimate> sourceCosts)
        {
            // TODO: Explicitly visit GroupIdNode and throw an IllegalArgumentException
            // this can only be done once we get rid of the StatelessLookup
            return UNKNOWN_STATS;
        }

        @Override
        public PlanNodeStatsEstimate visitOutput(OutputNode node, List<PlanNodeStatsEstimate> sourceCosts)
        {
            return getOnlyElement(sourceCosts);
        }

        @Override
        public PlanNodeStatsEstimate visitFilter(FilterNode node, List<PlanNodeStatsEstimate> sourceCosts)
        {
            sourceCosts.get(0).getOutputRowCount();

            Expression expr = node.getPredicate();
            return new FilterExpressionStatsCalculatingVisitor(getOnlyElement(sourceCosts)).process(expr);
        }

        @Override
        public PlanNodeStatsEstimate visitProject(ProjectNode node, List<PlanNodeStatsEstimate> sourceCosts)
        {
            return getOnlyElement(sourceCosts);
        }

        @Override
        public PlanNodeStatsEstimate visitJoin(JoinNode node, List<PlanNodeStatsEstimate> sourceCosts)
        {
            checkSourceCostsCount(sourceCosts, 2);
            PlanNodeStatsEstimate leftCost = sourceCosts.get(0);
            PlanNodeStatsEstimate rightCost = sourceCosts.get(1);

            PlanNodeStatsEstimate.Builder joinCost = PlanNodeStatsEstimate.builder();
            if (!leftCost.getOutputRowCount().isValueUnknown() && !rightCost.getOutputRowCount().isValueUnknown()) {
                double rowCount = Math.max(leftCost.getOutputRowCount().getValue(), rightCost.getOutputRowCount().getValue()) * JOIN_MATCHING_COEFFICIENT;
                joinCost.setOutputRowCount(new Estimate(rowCount));
            }
            return joinCost.build();
        }

        @Override
        public PlanNodeStatsEstimate visitExchange(ExchangeNode node, List<PlanNodeStatsEstimate> sourceCosts)
        {
            Estimate rowCount = new Estimate(0);
            checkSourceCostsCount(sourceCosts, node.getSources().size());
            for (int i = 0; i < node.getSources().size(); i++) {
                PlanNodeStatsEstimate childCost = sourceCosts.get(i);
                if (childCost.getOutputRowCount().isValueUnknown()) {
                    rowCount = Estimate.unknownValue();
                }
                else {
                    rowCount = rowCount.map(value -> value + childCost.getOutputRowCount().getValue());
                }
            }

            return PlanNodeStatsEstimate.builder()
                    .setOutputRowCount(rowCount)
                    .build();
        }

        @Override
        public PlanNodeStatsEstimate visitTableScan(TableScanNode node, List<PlanNodeStatsEstimate> sourceCosts)
        {
            Constraint<ColumnHandle> constraint = getConstraint(node, BooleanLiteral.TRUE_LITERAL);

            TableStatistics tableStatistics = metadata.getTableStatistics(session, node.getTable(), constraint);
            Map<Symbol, ColumnStatistics> outputSymbolStats = new HashMap<>();

            // TODO as stream
            for (Map.Entry<Symbol, ColumnHandle> entry : node.getAssignments().entrySet()) {
                Symbol symbol = entry.getKey();
                ColumnStatistics statistics = tableStatistics.getColumnStatistics().get(entry.getValue());
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
        public PlanNodeStatsEstimate visitValues(ValuesNode node, List<PlanNodeStatsEstimate> sourceCosts)
        {
            Estimate valuesCount = new Estimate(node.getRows().size());
            return PlanNodeStatsEstimate.builder()
                    .setOutputRowCount(valuesCount)
                    .build();
        }

        @Override
        public PlanNodeStatsEstimate visitEnforceSingleRow(EnforceSingleRowNode node, List<PlanNodeStatsEstimate> sourceCosts)
        {
            return PlanNodeStatsEstimate.builder()
                    .setOutputRowCount(new Estimate(1.0))
                    .build();
        }

        @Override
        public PlanNodeStatsEstimate visitSemiJoin(SemiJoinNode node, List<PlanNodeStatsEstimate> sourceCosts)
        {
            checkState(sourceCosts.size() == 2, "must have exactly two child costs.");
            PlanNodeStatsEstimate sourceCost = sourceCosts.get(0);
            return sourceCost.mapOutputRowCount(rowCount -> rowCount * JOIN_MATCHING_COEFFICIENT);
        }

        @Override
        public PlanNodeStatsEstimate visitLimit(LimitNode node, List<PlanNodeStatsEstimate> sourceCosts)
        {
            PlanNodeStatsEstimate sourceCost = getOnlyElement(sourceCosts);
            PlanNodeStatsEstimate.Builder limitCost = PlanNodeStatsEstimate.builder();
            if (sourceCost.getOutputRowCount().getValue() < node.getCount()) {
                limitCost.setOutputRowCount(sourceCost.getOutputRowCount());
            }
            else {
                limitCost.setOutputRowCount(new Estimate(node.getCount()));
            }
            return limitCost.build();
        }

        private void checkSourceCostsCount(List<PlanNodeStatsEstimate> sourceCosts, int expectedCount)
        {
            checkArgument(sourceCosts.size() == expectedCount, "expected %s source costs, but found %s", sourceCosts.size(), expectedCount);
        }


        private class FilterExpressionStatsCalculatingVisitor
                extends AstVisitor<PlanNodeStatsEstimate, Void>
        {
            PlanNodeStatsEstimate input;
            FilterExpressionStatsCalculatingVisitor(PlanNodeStatsEstimate input) {
                this.input = input;
            }

            @Override
            protected PlanNodeStatsEstimate visitComparisonExpression(ComparisonExpression node, Void context)
            {
                // FIXME left and right might not be exactly SymbolReference and Literal
                if (node.getLeft() instanceof SymbolReference && node.getRight() instanceof SymbolReference) {
                    return comparisonSymbolToSymbolStats(
                            Symbol.from(node.getLeft()),
                            Symbol.from(node.getRight()),
                            node.getType());
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
                        break;
                    case IS_DISTINCT_FROM:
                        break;
                }
                return null;
            }

            private PlanNodeStatsEstimate symbolToLiteralLessThan(Symbol left, Object literalValue)
            {
                TypeStatOperatorCaller operatorCaller = new TypeStatOperatorCaller(types.get(left), metadata.getFunctionRegistry(), session.toConnectorSession());
                RangeColumnStatistics leftStats = input.getOnlyRangeStats(left);
                if (!leftStats.getLowValue().isPresent() || !leftStats.getHighValue().isPresent()) {
                    // TODO unknownLowHighStats(left, right, type);
                    return input;
                }

                if (operatorCaller.callComparisonOperator(ComparisonExpressionType.LESS_THAN, literalValue, leftStats.getLowValue())) {
                    return input; // FIXME return empty set stats
                }

                if (operatorCaller.callComparisonOperator(ComparisonExpressionType.GREATER_THAN_OR_EQUAL, literalValue, leftStats.getHighValue())) {
                    return input;
                }

                return input; // FIXME between case
            }

            private PlanNodeStatsEstimate symbolToLiteralNonEquality(Symbol left, Object literalValue)
            {
                TypeStatOperatorCaller operatorCaller = new TypeStatOperatorCaller(types.get(left), metadata.getFunctionRegistry(), session.toConnectorSession());
                RangeColumnStatistics leftStats = input.getOnlyRangeStats(left);
                if (!leftStats.getLowValue().isPresent() || !leftStats.getHighValue().isPresent()) {
                    // TODO unknownLowHighStats(left, right, type);
                    return input;
                }

                if (!operatorCaller.callBetweenOperator(literalValue, leftStats.getLowValue().get(), leftStats.getHighValue().get())) {
                    return input;
                }

                double distinctValuesCount = leftStats.getDistinctValuesCount().getValue();
                double filterRate = ((distinctValuesCount - 1) / distinctValuesCount) * (1 - leftStats.getNullsFraction().getValue());

                RangeColumnStatistics newColumnStats = leftStats.builderFrom()
                        .setNullsFraction(zeroValue())
                        .setDistinctValuesCount(new Estimate(leftStats.getDistinctValuesCount().getValue() - 1))
                        .setDataSize(new Estimate(filterRate * leftStats.getDataSize().getValue())).build();

                return filterStatsByFactor(filterRate).mapSymbolColumnStatistics(left, x -> newColumnStats);
            }

            private PlanNodeStatsEstimate symbolToLiteralEquality(Symbol left, Object literalValue)
            {
                TypeStatOperatorCaller operatorCaller = new TypeStatOperatorCaller(types.get(left), metadata.getFunctionRegistry(), session.toConnectorSession());
                RangeColumnStatistics leftStats = input.getOnlyRangeStats(left);
                if (!leftStats.getLowValue().isPresent() || !leftStats.getHighValue().isPresent()) {
                    // TODO unknownLowHighStats(left, right, type);
                    return input;
                }

                double filterRate = 0;
                if (operatorCaller.callBetweenOperator(literalValue, leftStats.getLowValue().get(), leftStats.getHighValue().get())) {
                    filterRate = (1 - leftStats.getNullsFraction().getValue()) / leftStats.getDistinctValuesCount().getValue();
                }

                RangeColumnStatistics newColumnStats = leftStats.builderFrom()
                        .setNullsFraction(zeroValue())
                        .setDistinctValuesCount(new Estimate(1))
                        .setDataSize(new Estimate(filterRate * leftStats.getDataSize().getValue()))
                        .setLowValue(Optional.of(literalValue))
                        .setHighValue(Optional.of(literalValue)).build();

                return filterStatsByFactor(filterRate).mapSymbolColumnStatistics(left, x -> newColumnStats);
            }

            private PlanNodeStatsEstimate filterStatsByFactor(double filterRate)
            {
                return input
                        .mapOutputRowCount(size -> size * filterRate)
                        .mapOutputSizeInBytes(size -> size * filterRate);
            }

            private PlanNodeStatsEstimate comparisonSymbolToSymbolStats(Symbol left, Symbol right, ComparisonExpressionType type)
            {
                return null;
            }
        }
    }
}
