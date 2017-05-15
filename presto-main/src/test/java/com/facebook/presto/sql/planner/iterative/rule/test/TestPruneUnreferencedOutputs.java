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
package com.facebook.presto.sql.planner.iterative.rule.test;

import com.facebook.presto.metadata.Signature;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.TestingColumnHandle;
import com.facebook.presto.sql.planner.assertions.PlanMatchPattern;
import com.facebook.presto.sql.planner.iterative.rule.PruneUnreferencedOutputs;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.Assignments;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.IndexJoinNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import jdk.nashorn.internal.ir.Assignment;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.aggregation;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyTree;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.equiJoinClause;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.exchange;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.functionCall;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.indexJoin;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.indexJoinEquiJoinClause;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.join;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.semiJoin;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.strictProject;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.strictTableScan;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.symbol;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder.expression;
import static com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder.expressions;
import static io.airlift.testing.Closeables.closeAllRuntimeException;

public class TestPruneUnreferencedOutputs
{
    private RuleTester tester;

    @BeforeClass
    public void setUp()
    {
        tester = new RuleTester();
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        closeAllRuntimeException(tester);
        tester = null;
    }

    @Test
    public void testExchange()
            throws Exception
    {
        tester.assertThat(new PruneUnreferencedOutputs())
                .on(p ->
                        p.gatheringExchange(
                                ExchangeNode.Scope.REMOTE,
                                p.values(p.symbol("x", BIGINT))
                        ).replaceChildren(ImmutableList.of(
                                p.values(p.symbol("x", BIGINT), p.symbol("unused", BIGINT)))))
                .matches(
                        exchange(
                                values(ImmutableMap.of("FOO_x", 0))));

        tester.assertThat(new PruneUnreferencedOutputs())
                .on(p ->
                        p.gatheringExchange(
                                ExchangeNode.Scope.REMOTE,
                                p.values(p.symbol("x", BIGINT))))
                .doesNotFire();

        tester.assertThat(new PruneUnreferencedOutputs())
                .on(p ->
                        p.project(
                                Assignments.of(p.symbol("y", BIGINT), expression("x")),
                                p.gatheringExchange(
                                        ExchangeNode.Scope.REMOTE,
                                        p.values(p.symbol("x", BIGINT), p.symbol("unused", BIGINT)))))
                .matches(
                        strictProject(
                                ImmutableMap.of("FOO_y", PlanMatchPattern.expression("FOO_x")),
                                exchange(
                                        values(ImmutableMap.of("FOO_x", 0, "FOO_unused", 1)))));

        // The partitioning and hash columns always stay in the output of the ExchangeNode, so we can't prune them.
        tester.assertThat(new PruneUnreferencedOutputs())
                .on(p -> {
                    Symbol x = p.symbol("x", BIGINT);
                    Symbol hashedX = p.symbol("hashedX", BIGINT);
                    return p.project(
                            Assignments.of(),
                            p.exchange(exchangeBuilder -> exchangeBuilder
                                    .fixedHashDistributionParitioningScheme(
                                            ImmutableList.of(x, hashedX),
                                            ImmutableList.of(x),
                                            hashedX)
                                    .addSource(p.values(x, hashedX))
                                    .addInputsSet(ImmutableList.of(x, hashedX))));
                })
                .doesNotFire();
    }

    private class JoinSymbols
    {
        final Symbol leftKey;
        final Symbol leftKeyHash;
        final Symbol leftValue;
        final Symbol rightKey;
        final Symbol rightKeyHash;
        final Symbol rightValue;
        final Symbol semiJoinOutput;

        JoinSymbols(PlanBuilder planBuilder)
        {
            leftKey = planBuilder.symbol("leftKey", BIGINT);
            leftKeyHash = planBuilder.symbol("leftKeyHash", BIGINT);
            leftValue = planBuilder.symbol("leftValue", BIGINT);
            rightKey = planBuilder.symbol("rightKey", BIGINT);
            rightKeyHash = planBuilder.symbol("rightKeyHash", BIGINT);
            rightValue = planBuilder.symbol("rightValue", BIGINT);
            semiJoinOutput = planBuilder.symbol("semiJoinOutput", BIGINT);
        }
    }

    @Test
    public void testJoin()
            throws Exception
    {
        // Drops everything from the join's output symbols
        tester.assertThat(new PruneUnreferencedOutputs())
                .on(p -> {
                    JoinSymbols symbols = new JoinSymbols(p);
                    return p.project(
                            Assignments.of(),
                            p.join(
                                    JoinNode.Type.INNER,
                                    p.values(symbols.leftKey, symbols.leftKeyHash, symbols.leftValue),
                                    p.values(symbols.rightKey, symbols.rightKeyHash, symbols.rightValue),
                                    ImmutableList.of(new JoinNode.EquiJoinClause(symbols.leftKey, symbols.rightKey)),
                                    ImmutableList.of(symbols.leftKey, symbols.leftValue, symbols.rightKey, symbols.rightValue),
                                    Optional.of(expression("leftValue > 5")),
                                    Optional.of(symbols.leftKeyHash),
                                    Optional.of(symbols.rightKeyHash)));
                })
                .matches(
                        strictProject(
                                ImmutableMap.of(),
                                join(
                                        JoinNode.Type.INNER,
                                        ImmutableList.of(equiJoinClause("leftKey_", "rightKey_")),
                                        Optional.of("leftValue_ > 5"),
                                        values(ImmutableMap.of("leftKey_", 0, "leftKeyHash_", 1, "leftValue_", 2)),
                                        values(ImmutableMap.of("rightKey_", 0, "rightKeyHash_", 1, "rightValue_", 2)))));

        // Cross joins can't prune their outputs, so push the pruning down to new project children
        tester.assertThat(new PruneUnreferencedOutputs())
                .on(p -> {
                    JoinSymbols symbols = new JoinSymbols(p);
                    return p.project(
                            Assignments.of(),
                            p.join(
                                    JoinNode.Type.INNER,
                                    p.values(symbols.leftKey, symbols.leftValue),
                                    p.values(symbols.rightKey, symbols.rightValue),
                                    ImmutableList.of(),
                                    ImmutableList.of(symbols.leftKey, symbols.leftValue, symbols.rightKey, symbols.rightValue),
                                    Optional.empty(),
                                    Optional.empty(),
                                    Optional.empty()));
                })
                .matches(
                        strictProject(
                                ImmutableMap.of(),
                                join(
                                        JoinNode.Type.INNER,
                                        ImmutableList.of(),
                                        Optional.empty(),
                                        strictProject(
                                                ImmutableMap.of(),
                                                values(ImmutableMap.of("leftKey_", 0, "leftValue_", 1))),
                                        strictProject(
                                                ImmutableMap.of(),
                                                values(ImmutableMap.of("rightKey_", 0, "rightValue_", 1))))));

        tester.assertThat(new PruneUnreferencedOutputs())
                .on(p -> {
                    JoinSymbols symbols = new JoinSymbols(p);
                    return p.project(
                            Assignments.identity(symbols.leftKey, symbols.leftValue, symbols.rightKey, symbols.rightValue),
                            p.join(
                                    JoinNode.Type.INNER,
                                    p.values(symbols.leftKey, symbols.leftKeyHash, symbols.leftValue),
                                    p.values(symbols.rightKey, symbols.rightKeyHash, symbols.rightValue),
                                    ImmutableList.of(new JoinNode.EquiJoinClause(symbols.leftKey, symbols.rightKey)),
                                    ImmutableList.of(symbols.leftKey, symbols.leftValue, symbols.rightKey, symbols.rightValue),
                                    Optional.of(expression("leftValue > 5")),
                                    Optional.of(symbols.leftKeyHash),
                                    Optional.of(symbols.rightKeyHash)));
                })
                .doesNotFire();

        // Prune rightValue, but not the other symbols, which are all used by the join node
        tester.assertThat(new PruneUnreferencedOutputs())
                .on(p -> {
                    JoinSymbols symbols = new JoinSymbols(p);
                    return p.join(
                            JoinNode.Type.INNER,
                            p.values(symbols.leftKey, symbols.leftKeyHash, symbols.leftValue),
                            p.values(symbols.rightKey, symbols.rightKeyHash, symbols.rightValue),
                            ImmutableList.of(new JoinNode.EquiJoinClause(symbols.leftKey, symbols.rightKey)),
                            ImmutableList.of(),
                            Optional.of(expression("leftValue > 5")),
                            Optional.of(symbols.leftKeyHash),
                            Optional.of(symbols.rightKeyHash));
                })
                .matches(
                        join(
                                JoinNode.Type.INNER,
                                ImmutableList.of(equiJoinClause("leftKey_", "rightKey_")),
                                Optional.of("leftValue_ > 5"),
                                values(ImmutableMap.of("leftKey_", 0, "leftKeyHash_", 1, "leftValue_", 2)),
                                values(ImmutableMap.of("rightKey_", 0, "rightKeyHash_", 1))));
    }

    @Test
    public void testSemiJoin()
            throws Exception
    {
        // SemiJoins can't prune their outputs, so push the pruning down to a new project source
        tester.assertThat(new PruneUnreferencedOutputs())
                .on(p -> {
                    JoinSymbols symbols = new JoinSymbols(p);
                    return p.project(
                            Assignments.identity(symbols.semiJoinOutput),
                            p.semiJoin(
                                    p.values(symbols.leftKey, symbols.leftKeyHash, symbols.leftValue),
                                    p.values(symbols.rightKey, symbols.rightKeyHash, symbols.rightValue),
                                    symbols.leftKey,
                                    symbols.rightKey,
                                    symbols.semiJoinOutput,
                                    Optional.of(symbols.leftKeyHash),
                                    Optional.of(symbols.rightKeyHash)));
                })
                .matches(
                        strictProject(
                                ImmutableMap.of("semiJoinOutput_", PlanMatchPattern.expression("semiJoinOutput_")),
                                semiJoin(
                                        "leftKey_",
                                        "rightKey_",
                                        "semiJoinOutput_",
                                        strictProject(
                                                ImmutableMap.of(
                                                        "leftKey_", PlanMatchPattern.expression("leftKey_"),
                                                        "leftKeyHash_", PlanMatchPattern.expression("leftKeyHash_")),
                                                values(ImmutableMap.of("leftKey_", 0, "leftKeyHash_", 1))),
                                        values(ImmutableMap.of("rightKey_", 0, "rightKeyHash_", 1, "rightValue_", 2)))));

        tester.assertThat(new PruneUnreferencedOutputs())
                .on(p -> {
                    JoinSymbols symbols = new JoinSymbols(p);
                    return p.project(
                            Assignments.identity(symbols.leftKey),
                            p.semiJoin(
                                    p.values(symbols.leftKey, symbols.leftKeyHash, symbols.leftValue),
                                    p.values(symbols.rightKey, symbols.rightKeyHash, symbols.rightValue),
                                    symbols.leftKey,
                                    symbols.rightKey,
                                    symbols.semiJoinOutput,
                                    Optional.of(symbols.leftKeyHash),
                                    Optional.of(symbols.rightKeyHash)));
                })
                .matches(
                        strictProject(
                                ImmutableMap.of("leftKey_", PlanMatchPattern.expression("leftKey_")),
                                strictProject(
                                        ImmutableMap.of("leftKey_", PlanMatchPattern.expression("leftKey_")),
                                        values(ImmutableMap.of("leftKey_", 0, "leftKeyHash_", 1, "leftValue_", 2)))));

        tester.assertThat(new PruneUnreferencedOutputs())
                .on(p -> {
                    JoinSymbols symbols = new JoinSymbols(p);
                    return p.project(
                            Assignments.identity(symbols.semiJoinOutput),
                            p.semiJoin(
                                    p.values(symbols.leftKey, symbols.leftKeyHash),
                                    p.values(symbols.rightKey, symbols.rightKeyHash, symbols.rightValue),
                                    symbols.leftKey,
                                    symbols.rightKey,
                                    symbols.semiJoinOutput,
                                    Optional.of(symbols.leftKeyHash),
                                    Optional.of(symbols.rightKeyHash)));
                })
                .doesNotFire();

        tester.assertThat(new PruneUnreferencedOutputs())
                .on(p -> {
                    JoinSymbols symbols = new JoinSymbols(p);
                    return p.semiJoin(
                            p.values(symbols.leftKey, symbols.leftKeyHash, symbols.leftValue),
                            p.values(symbols.rightKey, symbols.rightKeyHash, symbols.rightValue),
                            symbols.leftKey,
                            symbols.rightKey,
                            symbols.semiJoinOutput,
                            Optional.of(symbols.leftKeyHash),
                            Optional.of(symbols.rightKeyHash));
                })
                .matches(
                        semiJoin(
                                "leftKey_",
                                "rightKey_",
                                "semiJoinOutput_",
                                values(ImmutableMap.of("leftKey_", 0, "leftKeyHash_", 1, "leftValue_", 2)),
                                values(ImmutableMap.of("rightKey_", 0, "rightKeyHash_", 1))));

        // TODO test the case of dropping the semiJoin entirely when its output is not required
    }

    @Test
    public void testIndexJoin()
            throws Exception
    {
        // IndexJoins can't prune their outputs, so push the pruning down to new project children
        tester.assertThat(new PruneUnreferencedOutputs())
                .on(p -> {
                    JoinSymbols symbols = new JoinSymbols(p);
                    return p.project(
                            Assignments.of(),
                            p.indexJoin(
                                    IndexJoinNode.Type.INNER,
                                    p.values(symbols.leftKey, symbols.leftKeyHash, symbols.leftValue),
                                    p.values(symbols.rightKey, symbols.rightKeyHash, symbols.rightValue),
                                    ImmutableList.of(new IndexJoinNode.EquiJoinClause(symbols.leftKey, symbols.rightKey)),
                                    Optional.of(symbols.leftKeyHash),
                                    Optional.of(symbols.rightKeyHash)));
                })
                .matches(
                        strictProject(
                                ImmutableMap.of(),
                                indexJoin(
                                        IndexJoinNode.Type.INNER,
                                        ImmutableList.of(indexJoinEquiJoinClause("leftKey_", "rightKey_")),
                                        strictProject(
                                                ImmutableMap.of(
                                                        "leftKey_", PlanMatchPattern.expression("leftKey_"),
                                                        "leftKeyHash_", PlanMatchPattern.expression("leftKeyHash_")),
                                                values(ImmutableMap.of("leftKey_", 0, "leftKeyHash_", 1, "leftKeyValue_", 2))),
                                        strictProject(
                                                ImmutableMap.of(
                                                        "rightKey_", PlanMatchPattern.expression("rightKey_"),
                                                        "rightKeyHash_", PlanMatchPattern.expression("rightKeyHash_")),
                                                values(ImmutableMap.of("rightKey_", 0, "rightKeyHash_", 1, "rightKeyValue_", 2))))));

        tester.assertThat(new PruneUnreferencedOutputs())
                .on(p -> {
                    JoinSymbols symbols = new JoinSymbols(p);
                    return p.project(
                            Assignments.of(),
                            p.indexJoin(
                                    IndexJoinNode.Type.INNER,
                                    p.values(symbols.leftKey, symbols.leftKeyHash),
                                    p.values(symbols.rightKey, symbols.rightKeyHash),
                                    ImmutableList.of(new IndexJoinNode.EquiJoinClause(symbols.leftKey, symbols.rightKey)),
                                    Optional.of(symbols.leftKeyHash),
                                    Optional.of(symbols.rightKeyHash)));
                })
                .doesNotFire();

        tester.assertThat(new PruneUnreferencedOutputs())
                .on(p -> {
                    JoinSymbols symbols = new JoinSymbols(p);
                    return p.indexJoin(
                            IndexJoinNode.Type.INNER,
                            p.values(symbols.leftKey, symbols.leftKeyHash, symbols.leftValue),
                            p.values(symbols.rightKey, symbols.rightKeyHash, symbols.rightValue),
                            ImmutableList.of(new IndexJoinNode.EquiJoinClause(symbols.leftKey, symbols.rightKey)),
                            Optional.of(symbols.leftKeyHash),
                            Optional.of(symbols.rightKeyHash));
                })
                .doesNotFire();
    }

    @Test
    public void testIndexSource()
            throws Exception
    {
        // TODO implement after testTableScan is working
    }

    @Test
    public void testProject()
            throws Exception
    {
        tester.assertThat(new PruneUnreferencedOutputs())
                .on(p ->
                        p.project(
                                Assignments.of(p.symbol("complex", BIGINT), expression("y * 2")),
                                p.project(
                                        Assignments.of(
                                                p.symbol("y", BIGINT), expression("x"),
                                                p.symbol("literal1", BIGINT), expression("1")),
                                        p.values(p.symbol("x", BIGINT)))))
                .matches(
                        strictProject(
                                ImmutableMap.of("FOO_complex", PlanMatchPattern.expression("FOO_y * 2")),
                                strictProject(
                                        ImmutableMap.of("FOO_y", PlanMatchPattern.expression("FOO_x")),
                                        values(ImmutableMap.of("FOO_x", 0)))));
    }

    @Test
    public void testTableScan()
            throws Exception
    {
        // TODO uncomment and fix this after the epic/statistics-3 branch lands, as it needs
        // e7575a8 for tableScan validation during rule testing
        /*
        tester.assertThat(new PruneUnreferencedOutputs())
                .on(p ->
                        p.project(
                                Assignments.of(p.symbol("y", BIGINT), expression("x")),
                                p.tableScan(
                                        ImmutableList.of(p.symbol("unused", BIGINT), p.symbol("x", BIGINT)),
                                        ImmutableMap.of(
                                                p.symbol("unused", BIGINT), new TestingColumnHandle("unused"),
                                                p.symbol("x", BIGINT), new TestingColumnHandle("x")))))
                .matches(
                        strictProject(
                                ImmutableMap.of("FOO_y", PlanMatchPattern.expression("FOO_x")),
                                strictTableScan("BOGUS", ImmutableMap.of("FOO_x", "x"))));
                                */
    }

    @Test
    public void testValues()
            throws Exception
    {
        // TODO extend plan matcher to validate rows of ValuesNode
        tester.assertThat(new PruneUnreferencedOutputs())
                .on(p ->
                        p.project(
                                Assignments.of(p.symbol("y", BIGINT), expression("x")),
                                p.values(p.symbol("unused", BIGINT), p.symbol("x", BIGINT))))
                .matches(
                        project(
                                ImmutableMap.of("FOO_y", PlanMatchPattern.expression("FOO_x")),
                                values(ImmutableMap.of("FOO_x", 0))));

        tester.assertThat(new PruneUnreferencedOutputs())
                .on(p ->
                        p.project(
                                Assignments.of(p.symbol("y", BIGINT), expression("x")),
                                p.values(p.symbol("x", BIGINT))))
                .doesNotFire();
    }

    @Test
    public void testAggregation()
            throws Exception
    {
        tester.assertThat(new PruneUnreferencedOutputs())
                .on(p ->
                {
                    final Symbol input = p.symbol("input", BIGINT);
                    final Symbol summation1 = p.symbol("summation1", BIGINT);
                    final Symbol unusedSummation1 = p.symbol("unusedSummation1", BIGINT);
                    final Symbol summation2 = p.symbol("summation2", BIGINT);
                    final Symbol key1 = p.symbol("key1", BIGINT);
                    final Symbol key2 = p.symbol("key2", BIGINT);
                    final Symbol keyHash2 = p.symbol("keyHash2", BIGINT);
                    final Symbol mask2 = p.symbol("mask2", BIGINT);
                    return p.aggregation(aggregationBuilder2 -> aggregationBuilder2
                            .addAggregation(summation2, expression("sum(summation1)"), ImmutableList.of(BIGINT), Optional.of(mask2))
                            .groupingSets(ImmutableList.of(ImmutableList.of(key2)))
                            .hashSymbol(keyHash2)
                            .step(AggregationNode.Step.SINGLE)
                            .source(
                                    p.aggregation(aggregationBuilder -> aggregationBuilder
                                            .addAggregation(unusedSummation1, expression("sum(input)"), ImmutableList.of(BIGINT))
                                            .addAggregation(summation1, expression("sum(input)"), ImmutableList.of(BIGINT))
                                            .addAggregation(key2, expression("avg(input)"), ImmutableList.of(BIGINT))
                                            .addAggregation(keyHash2, expression("min(input)"), ImmutableList.of(BIGINT))
                                            .addAggregation(mask2, expression("max(input)"), ImmutableList.of(BIGINT))
                                            .groupingSets(ImmutableList.of(ImmutableList.of(key1)))
                                            .step(AggregationNode.Step.SINGLE)
                                            .source(p.values(key1, input)))));
                })
                .matches(
                        aggregation(
                                ImmutableList.of(ImmutableList.of("key2_")),
                                ImmutableMap.of(Optional.of("summation2_"), functionCall("sum", ImmutableList.of("summation1_"))),
                                ImmutableMap.of(),
                                Optional.empty(),
                                AggregationNode.Step.SINGLE,
                                aggregation(
                                        ImmutableList.of(ImmutableList.of("key1_")),
                                        ImmutableMap.of(
                                                Optional.of("summation1_"), functionCall("sum", ImmutableList.of("input_")),
                                                Optional.of("key2_"), functionCall("avg", ImmutableList.of("input_")),
                                                Optional.of("keyHash2_"), functionCall("min", ImmutableList.of("input_")),
                                                Optional.of("mask2_"), functionCall("max", ImmutableList.of("input_"))),
                                        ImmutableMap.of(),
                                        Optional.empty(),
                                        AggregationNode.Step.SINGLE,
                                        values(ImmutableMap.of("key1_", 0, "input_", 1)))));
    }

    @Test
    public void testMarkDistinct()
            throws Exception
    {
        /*
        tester.assertThat(new PruneUnreferencedOutputs())
                .on(p ->
                {
                    final Symbol input = p.symbol("input", BIGINT);
                    final Symbol summation1 = p.symbol("summation1", BIGINT);
                    final Symbol unusedSummation1 = p.symbol("unusedSummation1", BIGINT);
                    final Symbol summation2 = p.symbol("summation2", BIGINT);
                    final Symbol key1 = p.symbol("key1", BIGINT);
                    final Symbol key2 = p.symbol("key2", BIGINT);
                    final Symbol keyHash2 = p.symbol("keyHash2", BIGINT);
                    final Symbol mask2 = p.symbol("mask2", BIGINT);
                    return p.markDistinct(
                    */
        tester.assertThat(new PruneUnreferencedOutputs())
                .on(p -> {
                    JoinSymbols symbols = new JoinSymbols(p);
                    return p.project(
                            Assignments.identity(symbols.leftKey),
                            p.semiJoin(
                                    p.values(symbols.leftKey, symbols.leftKeyHash, symbols.leftValue),
                                    p.values(symbols.rightKey, symbols.rightKeyHash, symbols.rightValue),
                                    symbols.leftKey,
                                    symbols.rightKey,
                                    symbols.semiJoinOutput,
                                    Optional.of(symbols.leftKeyHash),
                                    Optional.of(symbols.rightKeyHash)));
                })
                .matches(
                        strictProject(
                                ImmutableMap.of("leftKey_", PlanMatchPattern.expression("leftKey_")),
                                strictProject(
                                        ImmutableMap.of("leftKey_", PlanMatchPattern.expression("leftKey_")),
                                        values(ImmutableMap.of("leftKey_", 0, "leftKeyHash_", 1, "leftValue_", 2)))));
    }
}
