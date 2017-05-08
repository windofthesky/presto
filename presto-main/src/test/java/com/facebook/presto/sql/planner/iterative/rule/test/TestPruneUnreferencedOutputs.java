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

import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.TestingColumnHandle;
import com.facebook.presto.sql.planner.assertions.PlanMatchPattern;
import com.facebook.presto.sql.planner.iterative.rule.PruneUnreferencedOutputs;
import com.facebook.presto.sql.planner.plan.Assignments;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import jdk.nashorn.internal.ir.Assignment;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.aggregation;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyTree;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.exchange;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
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
                            p.exchange(eb -> eb
                                    .fixedHashDistributionParitioningScheme(
                                            ImmutableList.of(x, hashedX),
                                            ImmutableList.of(x),
                                            hashedX)
                                    .addSource(p.values(x, hashedX))
                                    .addInputsSet(ImmutableList.of(x, hashedX))));})
                .doesNotFire();
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
}
