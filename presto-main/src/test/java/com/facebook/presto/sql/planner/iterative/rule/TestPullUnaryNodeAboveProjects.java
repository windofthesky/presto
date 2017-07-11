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
package com.facebook.presto.sql.planner.iterative.rule;

import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.facebook.presto.sql.planner.plan.Assignments;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.MarkDistinctNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.matching.Pattern.typeOf;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.expression;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.markDistinct;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.strictProject;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static com.facebook.presto.sql.planner.iterative.rule.Util.pullUnaryNodeAboveProjects;
import static com.facebook.presto.sql.planner.iterative.rule.Util.restrictOutputs;
import static java.util.Objects.requireNonNull;

public class TestPullUnaryNodeAboveProjects
        extends BaseRuleTest
{
    /**
     * A rule only for testing the utility function.
     * PlanMatchPattern needs lots of setup, so it's easier to test the util function as a rule.
     */
    private class PullAboveProjects<N extends PlanNode>
            implements Rule<PlanNode>
    {
        private Class<N> targetClass;
        private final Pattern<PlanNode> pattern = typeOf(PlanNode.class);

        PullAboveProjects(Class<N> targetClass)
        {
            this.targetClass = requireNonNull(targetClass);
        }

        @Override
        public Pattern<PlanNode> getPattern()
        {
            return pattern;
        }

        @Override
        public Optional<PlanNode> apply(PlanNode node, Captures captures, Context context)
        {
            return pullUnaryNodeAboveProjects(context.getLookup(), targetClass, node)
                    .flatMap(Optional::<PlanNode>of)
                    .map(newRoot ->
                            restrictOutputs(context.getIdAllocator(), newRoot, ImmutableSet.copyOf(node.getOutputSymbols()))
                                    .orElse(newRoot));
        }
    }

    @Test
    public void testNoTarget()
    {
        tester().assertThat(new PullAboveProjects<>(MarkDistinctNode.class))
                .on(p -> p.values())
                .doesNotFire();
    }

    @Test
    public void testNoProjects()
    {
        tester().assertThat(new PullAboveProjects<>(MarkDistinctNode.class))
                .on(p -> p.markDistinct(p.symbol("marker"), ImmutableList.of(), p.values()))
                .matches(markDistinct("marker", ImmutableList.of(), values()));
    }

    @Test
    public void testTargetCreatedSymbolIdentity()
    {
        tester().assertThat(new PullAboveProjects<>(MarkDistinctNode.class))
                .on(p -> {
                    Symbol keyUsage = p.symbol("keyUsage");
                    Symbol marker = p.symbol("marker");
                    Symbol key = p.symbol("key");
                    Symbol unused = p.symbol("unused");
                    return
                            p.project(
                                    Assignments.of(
                                            marker, marker.toSymbolReference(),
                                            keyUsage, p.expression("key + 1")),
                                    p.project(
                                            Assignments.identity(marker),
                                            p.markDistinct(
                                                    marker,
                                                    ImmutableList.of(key),
                                                    p.values(key, unused))));
                })
                .matches(
                        strictProject(
                                ImmutableMap.of(
                                        "marker", expression("marker"),
                                        "keyUsage", expression("keyUsage")),
                                markDistinct(
                                        "marker",
                                        ImmutableList.of("key"),
                                        strictProject(
                                                ImmutableMap.of(
                                                        "keyUsage", expression("key + 1"),
                                                        "key", expression("key"),
                                                        "unused", expression("unused")),
                                                strictProject(
                                                        ImmutableMap.of(
                                                                "key", expression("key"),
                                                                "unused", expression("unused")),
                                                        values("key", "unused"))))));
    }

    @Test
    public void testTargetCreatedSymbolComplexUse()
    {
        tester().assertThat(new PullAboveProjects<>(MarkDistinctNode.class))
                .on(p -> {
                    Symbol markerUsage = p.symbol("markerUsage");
                    Symbol marker = p.symbol("marker");
                    return p.project(
                            Assignments.of(markerUsage, p.expression("marker + 1")),
                            p.markDistinct(marker, ImmutableList.of(), p.values()));
                })
                .doesNotFire();
    }

    @Test
    public void testTargetInputRedefinition()
    {
        tester().assertThat(new PullAboveProjects<>(MarkDistinctNode.class))
                .on(p -> {
                    Symbol marker = p.symbol("marker");
                    Symbol key = p.symbol("key");
                    return
                            p.project(
                                    Assignments.of(
                                            key, p.expression("1"),
                                            marker, marker.toSymbolReference()),
                                    p.project(
                                            Assignments.identity(marker),
                                            p.markDistinct(marker, ImmutableList.of(key), p.values(key))));
                })
                .doesNotFire();
    }

    @Test
    public void testTargetHidesProjection()
    {
        tester().assertThat(new PullAboveProjects<>(ExchangeNode.class))
                .on(p ->
                        p.project(
                                Assignments.of(p.symbol("one"), p.expression("1")),
                                p.exchange(exchangeBuilder -> exchangeBuilder
                                        .singleDistributionPartitioningScheme()
                                        .addInputsSet()
                                        .addSource(p.values()))))
                .doesNotFire();
    }
}
