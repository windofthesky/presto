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
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.GenericLiteral;
import com.facebook.presto.sql.tree.Literal;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.SymbolReference;
import com.google.common.collect.ImmutableMap;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.facebook.presto.cost.PlanNodeStatsEstimate.UNKNOWN_STATS;
import static com.facebook.presto.spi.statistics.Estimate.zeroValue;

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
            sourceCosts.get(0).getOutputRowCount();

            Expression expr = node.getPredicate();
            return getOnlyElement(new ExpressionVisitor().process(expr, sourceCosts));
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

            PlanNodeStatsEstimate.Builder joinCost = PlanNodeStatsEstimate.builder();
            if (!leftStats.getOutputRowCount().isValueUnknown() && !rightStats.getOutputRowCount().isValueUnknown()) {
                double rowCount = Math.max(leftStats.getOutputRowCount().getValue(), rightStats.getOutputRowCount().getValue()) * JOIN_MATCHING_COEFFICIENT;
                joinCost.setOutputRowCount(new Estimate(rowCount));
            }
            return joinCost.build();
        }

        @Override
        public PlanNodeStatsEstimate visitExchange(ExchangeNode node, Void context)
        {
            Estimate rowCount = new Estimate(0);
            for (int i = 0; i < node.getSources().size(); i++) {
                PlanNodeStatsEstimate childCost = lookupStats(node.getSources().get(i));
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
        public PlanNodeStatsEstimate visitTableScan(TableScanNode node, Void context)
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
    }

    private class ExpressionVisitor
            extends AstVisitor<List<PlanNodeStatsEstimate>, List<PlanNodeStatsEstimate>>
    {
        protected List<PlanNodeStatsEstimate> visitComparisonExpression(ComparisonExpression node, List<PlanNodeStatsEstimate> context)
        {
            if (!(node.getLeft() instanceof SymbolReference)) {
                return context;
            }
            Symbol left = Symbol.from(node.getLeft());
            switch (node.getType()) {
                case EQUAL:
                    if ((node.getRight() instanceof GenericLiteral || node.getRight() instanceof LongLiteral) && node.getLeft() instanceof SymbolReference) { // FIXME other types of literals
                        LongLiteral literal;
                        if (node.getRight() instanceof GenericLiteral) {
                            literal = new LongLiteral(((GenericLiteral) node.getRight()).getValue());
                        }
                        else {
                            literal = (LongLiteral) node.getRight();
                        }
                        return context.stream().map(statsEstimate ->
                        {
                            if (statsEstimate.containsSymbolStats(left)) {
                                RangeColumnStatistics columnStats = statsEstimate.getOnlyRangeStats(left);

                                if (!columnStats.getHighValue().isPresent() || !columnStats.getLowValue().isPresent()) {
                                    return statsEstimate;
                                }

                                boolean isInRange = literal.getValue() >= (Long) columnStats.getLowValue().get() && literal.getValue() <= (Long) columnStats.getHighValue().get();
                                double oldRowCount =  statsEstimate.getOutputRowCount().getValue();
                                double newRowCount = 0;
                                if (isInRange) {
                                    newRowCount = oldRowCount * (1 - columnStats.getNullsFraction().getValue()) / columnStats.getDistinctValuesCount().getValue();
                                }
                                double filterRate = newRowCount/oldRowCount;

                                final RangeColumnStatistics newColumnStats = columnStats.builderFrom()
                                        .setNullsFraction(zeroValue())
                                        .setDistinctValuesCount(new Estimate(isInRange ? 1 : 0))
                                        .setDataSize(new Estimate(filterRate * columnStats.getDataSize().getValue()))
                                        .setLowValue(Optional.of(literal.getValue()))
                                        .setHighValue(Optional.of(literal.getValue())).build();

                                return statsEstimate
                                        .mapSymbolColumnStatistics(left, input -> newColumnStats)
                                        .mapOutputRowCount(size -> size * filterRate)
                                        .mapOutputSizeInBytes(size -> size * filterRate);
                            }
                            return statsEstimate;
                        }).collect(Collectors.toList());
                    }
                    break;
                case NOT_EQUAL:
                    break;
                case LESS_THAN:
                    break;
                case LESS_THAN_OR_EQUAL:
                    break;
                case GREATER_THAN:
                    break;
                case GREATER_THAN_OR_EQUAL:
                    break;
                case IS_DISTINCT_FROM:
                    break;
            }
            return context;
        }
    }

    private class IsKnownSymbolReference extends AstVisitor<List<PlanNodeStatsEstimate>, Boolean>
    {
        protected Boolean visitNode(Node node, List<PlanNodeStatsEstimate> context)
        {
            return false;
        }

        protected Boolean visitSymbolReference(SymbolReference node, List<PlanNodeStatsEstimate> context)
        {
            return context.stream().anyMatch(stats -> stats.containsSymbolStats(Symbol.from(node)));
        }
    }

    private class IsLiteral extends AstVisitor<Void, Boolean>
    {
        protected Boolean visitNode(Node node, List<PlanNodeStatsEstimate> context)
        {
            return false;
        }

        protected Boolean visitLiteral(Literal node, List<PlanNodeStatsEstimate> context)
        {
            return false;
        }
    }
}
