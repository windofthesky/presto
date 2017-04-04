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

import com.facebook.presto.metadata.InternalNodeManager;
import com.facebook.presto.spi.statistics.Estimate;
import com.facebook.presto.sql.planner.plan.AggregationNode;
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

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import java.util.List;
import java.util.function.Function;

import static com.facebook.presto.cost.PlanNodeCostEstimate.UNKNOWN_COST;
import static com.facebook.presto.cost.PlanNodeCostEstimate.ZERO_COST;
import static com.facebook.presto.cost.PlanNodeCostEstimate.cpuCost;
import static com.facebook.presto.spi.statistics.Estimate.zeroValue;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * Simple implementation of CostCalculator.
 */
@ThreadSafe
public class CostCalculator
{
    private final int numberOfNodes;

    @Inject
    public CostCalculator(InternalNodeManager nodeManager)
    {
        this(nodeManager.getAllNodes().getActiveNodes().size());
    }

    public CostCalculator(int numberOfNodes)
    {
        this.numberOfNodes = numberOfNodes;
    }

    public PlanNodeCostEstimate calculateCost(
            PlanNode planNode,
            PlanNodeStatsEstimate planNodeStats,
            List<PlanNodeCostEstimate> sourceCosts,
            List<PlanNodeStatsEstimate> sourceStats)
    {
        Visitor visitor = new Visitor(
                planNodeStats,
                sourceStats,
                numberOfNodes);

        PlanNodeCostEstimate childrenCost = sourceCosts.stream()
                .reduce(PlanNodeCostEstimate.builder().build(), PlanNodeCostEstimate::add);
        return planNode.accept(visitor, null).add(childrenCost);
    }

    private class Visitor
            extends PlanVisitor<Void, PlanNodeCostEstimate>
    {
        private final PlanNodeStatsEstimate planNodeStats;
        private final List<PlanNodeStatsEstimate> sourceStats;
        private final int numberOfNodes;

        public Visitor(
                PlanNodeStatsEstimate planNodeStats,
                List<PlanNodeStatsEstimate> sourceStats,
                int numberOfNodes)
        {
            this.planNodeStats = requireNonNull(planNodeStats, "planNodeStats is null");
            this.sourceStats = requireNonNull(sourceStats, "sourceStats is null");
            this.numberOfNodes = numberOfNodes;
        }

        @Override
        protected PlanNodeCostEstimate visitPlan(PlanNode node, Void context)
        {
            return UNKNOWN_COST;
        }

        @Override
        public PlanNodeCostEstimate visitOutput(OutputNode node, Void context)
        {
            return ZERO_COST;
        }

        @Override
        public PlanNodeCostEstimate visitFilter(FilterNode node, Void context)
        {
            return cpuCost(getOnlyElement(sourceStats).getOutputSizeInBytes());
        }

        @Override
        public PlanNodeCostEstimate visitProject(ProjectNode node, Void context)
        {
            return cpuCost(planNodeStats.getOutputSizeInBytes());
        }

        @Override
        public PlanNodeCostEstimate visitAggregation(AggregationNode node, Void context)
        {
            return PlanNodeCostEstimate.builder()
                    .setCpuCost(planNodeStats.getOutputSizeInBytes())
                    .setMemoryCost(planNodeStats.getOutputSizeInBytes())
                    .build();
        }

        @Override
        public PlanNodeCostEstimate visitJoin(JoinNode node, Void context)
        {
            return calculateJoinCost(node.getDistributionType().isPresent() &&
                    node.getDistributionType().get().equals(JoinNode.DistributionType.REPLICATED));
        }

        private PlanNodeCostEstimate calculateJoinCost(boolean replicated)
        {
            Function<Double, Double> numberOfNodesMultiplier = value -> value * (replicated ? numberOfNodes : 1);

            checkSourceStatsCount(sourceStats, 2);
            PlanNodeStatsEstimate leftStats = sourceStats.get(0);
            PlanNodeStatsEstimate rightStats = sourceStats.get(1);

            Estimate cpuCost = leftStats.getOutputSizeInBytes()
                    .add(rightStats.getOutputSizeInBytes().map(numberOfNodesMultiplier))
                    .add(planNodeStats.getOutputSizeInBytes());

            Estimate memoryCost = rightStats.getOutputSizeInBytes()
                    .map(numberOfNodesMultiplier);

            return PlanNodeCostEstimate.builder()
                    .setCpuCost(cpuCost)
                    .setMemoryCost(memoryCost)
                    .build();
        }

        @Override
        public PlanNodeCostEstimate visitExchange(ExchangeNode node, Void context)
        {
            Estimate network = zeroValue();
            Estimate cpu = zeroValue();

            switch (node.getType()) {
                case GATHER:
                    network = planNodeStats.getOutputSizeInBytes();
                    break;
                case REPARTITION:
                    network = planNodeStats.getOutputSizeInBytes();
                    cpu = planNodeStats.getOutputSizeInBytes();
                    break;
                case REPLICATE:
                    network = planNodeStats.getOutputSizeInBytes().map(value -> value * numberOfNodes);
                    break;
                default:
                    throw new UnsupportedOperationException(format("Unsupported type [%s] of the exchange", node.getType()));
            }

            if (node.getScope().equals(ExchangeNode.Scope.LOCAL)) {
                network = zeroValue();
            }

            return PlanNodeCostEstimate.builder()
                .setNetworkCost(network)
                .setCpuCost(cpu)
                .build();
        }

        @Override
        public PlanNodeCostEstimate visitTableScan(TableScanNode node, Void context)
        {
            return cpuCost(planNodeStats.getOutputSizeInBytes()); // TODO: add network cost, based on input size in bytes?
        }

        @Override
        public PlanNodeCostEstimate visitValues(ValuesNode node, Void context)
        {
            return ZERO_COST;
        }

        @Override
        public PlanNodeCostEstimate visitEnforceSingleRow(EnforceSingleRowNode node, Void context)
        {
            return ZERO_COST;
        }

        @Override
        public PlanNodeCostEstimate visitSemiJoin(SemiJoinNode node, Void context)
        {
            return calculateJoinCost(node.getDistributionType().isPresent() &&
                    node.getDistributionType().get().equals(SemiJoinNode.DistributionType.REPLICATED));
        }

        @Override
        public PlanNodeCostEstimate visitLimit(LimitNode node, Void context)
        {
            return cpuCost(planNodeStats.getOutputSizeInBytes());
        }

        private void checkSourceStatsCount(List<PlanNodeStatsEstimate> sourceStats, int expectedCount)
        {
            checkArgument(sourceStats.size() == expectedCount, "expected %s source stats, but found %s", sourceStats.size(), expectedCount);
        }
    }
}
