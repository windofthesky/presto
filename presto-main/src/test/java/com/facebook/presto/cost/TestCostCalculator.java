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

import com.facebook.presto.connector.ConnectorId;
import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.statistics.Estimate;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.TestingColumnHandle;
import com.facebook.presto.sql.planner.TestingTableHandle;
import com.facebook.presto.sql.planner.plan.Assignments;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.planner.plan.ValuesNode;
import com.facebook.presto.sql.tree.Cast;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.SymbolReference;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.cost.PlanNodeCostEstimate.cpuCost;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import static org.testng.Assert.assertEquals;

@Test(singleThreaded = true)
public class TestCostCalculator
{
    private final PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();
    private final CostCalculator costCalculator = new CostCalculator(10);

    @Test
    public void testTableScan()
    {
        assertCost(
                tableScan("orderkey"),
                statsEstimate(1000),
                ImmutableList.of(),
                ImmutableList.of())
                .cpu(1000)
                .memory(0)
                .network(0);
    }

    @Test
    public void testProject()
    {
        assertCost(
                project(tableScan("orderkey"), "string", new Cast(new SymbolReference("orderkey"), "STRING")),
                statsEstimate(4000),
                ImmutableList.of(cpuCost(new Estimate(1000))),
                ImmutableList.of(statsEstimate(1000)))
                .cpu(1000 + 4000)
                .memory(0)
                .network(0);
    }

    @Test
    public void testJoin()
    {
        assertCost(
                join(
                        tableScan("orderkey"),
                        tableScan("orderkey_0"),
                        JoinNode.DistributionType.PARTITIONED,
                        "orderkey",
                        "orderkey_0"),
                statsEstimate(12000),
                ImmutableList.of(cpuCost(new Estimate(6000)), cpuCost(new Estimate(1000))),
                ImmutableList.of(statsEstimate(6000), statsEstimate(1000)))
                .cpu(12000 + 6000 + 1000 + 6000 + 1000)
                .memory(1000)
                .network(0);

        assertCost(
                join(
                        tableScan("orderkey"),
                        tableScan("orderkey_0"),
                        JoinNode.DistributionType.REPLICATED,
                        "orderkey",
                        "orderkey_0"),
                statsEstimate(12000),
                ImmutableList.of(cpuCost(new Estimate(6000)), cpuCost(new Estimate(1000))),
                ImmutableList.of(statsEstimate(6000), statsEstimate(1000)))
                .cpu(12000 + 6000 + 10000 + 6000 + 1000)
                .memory(10000)
                .network(0);
    }

    private CostAssertionBuilder assertCost(
            PlanNode node,
            PlanNodeStatsEstimate planNodeStats,
            List<PlanNodeCostEstimate> sourceCosts,
            List<PlanNodeStatsEstimate> sourceStats)
    {
        return new CostAssertionBuilder(costCalculator.calculateCost(node, planNodeStats, sourceCosts, sourceStats));
    }

    private static class CostAssertionBuilder
    {
        private final PlanNodeCostEstimate actual;

        public CostAssertionBuilder(PlanNodeCostEstimate actual)
        {
            this.actual = requireNonNull(actual, "actual is null");
        }

        public CostAssertionBuilder network(double value)
        {
            assertEquals(actual.getNetworkCost().getValue(), value, 0.1);
            return this;
        }

        public CostAssertionBuilder cpu(double value)
        {
            assertEquals(actual.getCpuCost().getValue(), value, 0.1);
            return this;
        }

        public CostAssertionBuilder memory(double value)
        {
            assertEquals(actual.getMemoryCost().getValue(), value, 0.1);
            return this;
        }
    }

    private static PlanNodeStatsEstimate statsEstimate(int outputSizeInBytes)
    {
        return PlanNodeStatsEstimate.builder()
                .setOutputRowCount(new Estimate(Math.max(outputSizeInBytes / 8, 1)))
                .setOutputSizeInBytes(new Estimate(outputSizeInBytes)).build();
    }

    private TableScanNode tableScan(String... symbols)
    {
        List<Symbol> symbolsList = Arrays.stream(symbols).map(Symbol::new).collect(toImmutableList());
        ImmutableMap.Builder<Symbol, ColumnHandle> assignments = ImmutableMap.builder();

        for (Symbol symbol : symbolsList) {
            assignments.put(symbol, new TestingColumnHandle(symbol.getName()));
        }

        return new TableScanNode(
                idAllocator.getNextId(),
                new TableHandle(new ConnectorId("tpch"), new TestingTableHandle()),
                symbolsList,
                assignments.build(),
                Optional.empty(),
                TupleDomain.none(),
                null);
    }

    private PlanNode project(PlanNode source, String symbol, Expression expression)
    {
        return new ProjectNode(
                idAllocator.getNextId(),
                source,
                Assignments.of(new Symbol(symbol), expression));
    }

    private String symbol(String name)
    {
        return name;
    }

    private JoinNode join(PlanNode left, PlanNode right, String... symbols)
    {
        return join(left, right, JoinNode.DistributionType.PARTITIONED, symbols);
    }

    private JoinNode join(PlanNode left, PlanNode right, JoinNode.DistributionType distributionType, String... symbols)
    {
        checkArgument(symbols.length % 2 == 0);
        ImmutableList.Builder<JoinNode.EquiJoinClause> criteria = ImmutableList.builder();

        for (int i = 0; i < symbols.length; i += 2) {
            criteria.add(new JoinNode.EquiJoinClause(new Symbol(symbols[i]), new Symbol(symbols[i + 1])));
        }

        return new JoinNode(
                idAllocator.getNextId(),
                JoinNode.Type.INNER,
                left,
                right,
                criteria.build(),
                ImmutableList.<Symbol>builder()
                        .addAll(left.getOutputSymbols())
                        .addAll(right.getOutputSymbols())
                        .build(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.of(distributionType));
    }

    private ValuesNode values(String... symbols)
    {
        return new ValuesNode(
                idAllocator.getNextId(),
                Arrays.stream(symbols).map(Symbol::new).collect(toImmutableList()),
                ImmutableList.of());
    }
}
