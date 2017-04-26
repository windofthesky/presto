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

import com.facebook.presto.block.BlockEncodingManager;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.spi.type.TypeSignature;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.assertions.PlanMatchPattern;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder;
import com.facebook.presto.sql.planner.iterative.rule.test.RuleTester;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.type.TypeRegistry;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.OPTIMIZE_DISTINCT_AGGREGATIONS;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypeSignatures;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.aggregation;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.functionCall;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.groupingSet;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder.expression;

public class TestOptimizeMixedDistinctAggregations
{
    private RuleTester tester;
    private Rule optimizeMixedDistinctAggregations;
    private FunctionRegistry functionRegistry;
    private static final QualifiedName COUNT = QualifiedName.of("count");

    @BeforeClass
    public void setUp()
    {
        tester = new RuleTester(ImmutableMap.of(OPTIMIZE_DISTINCT_AGGREGATIONS, "true"));
        TypeRegistry typeManager = new TypeRegistry();
        functionRegistry = new FunctionRegistry(typeManager, new BlockEncodingManager(typeManager), new FeaturesConfig());
        optimizeMixedDistinctAggregations = new OptimizeMixedDistinctAggregations(functionRegistry);
    }

    @Test
    public void testDoesNotFireWhenNoAggregationNode()
    {
        tester.assertThat(optimizeMixedDistinctAggregations)
                .on(p -> p.values(p.symbol("a", BIGINT)))
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireWhenNoDistinct()
    {
        tester.assertThat(optimizeMixedDistinctAggregations)
                .on(p -> p.aggregate(
                        ImmutableMap.of(p.symbol("count", BIGINT), createAggregation(
                                p,
                                p.symbol("a", BIGINT),
                                getFunctionCall(COUNT, Optional.empty(), false, p.symbol("a", BIGINT)),
                                Optional.empty())),
                        ImmutableList.of(ImmutableList.of(p.symbol("b", BIGINT))),
                        p.values(p.symbol("a", BIGINT), p.symbol("b", BIGINT))))
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireWhenOnlyDistinct()
    {
        tester.assertThat(optimizeMixedDistinctAggregations)
                .on(p -> p.aggregate(
                        ImmutableMap.of(p.symbol("count", BIGINT), createAggregation(
                                p,
                                p.symbol("b", BIGINT),
                                getFunctionCall(COUNT, Optional.empty(), true, p.symbol("b", BIGINT)),
                                Optional.of(p.symbol("b$distinct", BIGINT)))),
                        ImmutableList.of(ImmutableList.of(p.symbol("b", BIGINT))),
                        p.values(p.symbol("a", BIGINT), p.symbol("b", BIGINT))))
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireWhenFilterAggregation()
    {
        tester.assertThat(optimizeMixedDistinctAggregations)
                .on(p -> p.aggregate(
                        ImmutableMap.of(
                                p.symbol("count", BIGINT), createAggregation(
                                        p,
                                        p.symbol("a", BIGINT),
                                        getFunctionCall(COUNT, Optional.empty(), false, p.symbol("a", BIGINT)),
                                        Optional.empty()),
                                p.symbol("count_1", BIGINT), createAggregation(
                                        p,
                                        p.symbol("b", BIGINT),
                                        getFunctionCall(COUNT, Optional.of(expression("b > 44")), true, p.symbol("b", BIGINT)),
                                        Optional.of(p.symbol("b$distinct", BIGINT)))),
                        ImmutableList.of(ImmutableList.of(p.symbol("b", BIGINT))),
                        p.values(p.symbol("a", BIGINT), p.symbol("b", BIGINT))))
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireWhenSourceIsNotMarkDistinct()
    {
        tester.assertThat(optimizeMixedDistinctAggregations)
                .on(p -> p.aggregate(
                        ImmutableMap.of(
                                p.symbol("count", BIGINT), createAggregation(
                                        p,
                                        p.symbol("a", BIGINT),
                                        getFunctionCall(COUNT, Optional.empty(), false, p.symbol("a", BIGINT)),
                                        Optional.empty()),
                                p.symbol("count_1", BIGINT), createAggregation(
                                        p,
                                        p.symbol("b", BIGINT),
                                        getFunctionCall(COUNT, Optional.empty(), true, p.symbol("b", BIGINT)),
                                        Optional.of(p.symbol("b$distinct", BIGINT)))),
                        ImmutableList.of(ImmutableList.of(p.symbol("b", BIGINT))),
                        p.values(p.symbol("a", BIGINT), p.symbol("b", BIGINT))))
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireWhenMarkDistinctSymbolIsAbsent()
    {
        tester.assertThat(optimizeMixedDistinctAggregations)
                .on(p -> p.aggregate(
                        ImmutableMap.of(
                                p.symbol("count", BIGINT), createAggregation(
                                        p,
                                        p.symbol("a", BIGINT),
                                        getFunctionCall(COUNT, Optional.empty(), false, p.symbol("a", BIGINT)),
                                        Optional.empty()),
                                p.symbol("count_1", BIGINT), createAggregation(
                                        p,
                                        p.symbol("b", BIGINT),
                                        getFunctionCall(COUNT, Optional.empty(), true, p.symbol("b", BIGINT)),
                                        Optional.of(p.symbol("b$distinct", BIGINT)))),
                        ImmutableList.of(ImmutableList.of(p.symbol("b", BIGINT))),
                        p.markDistinct(
                                p.symbol("dummy_symbol", BIGINT),
                                ImmutableList.of(p.symbol("b", BIGINT)),
                                p.values(p.symbol("a", BIGINT), p.symbol("b", BIGINT)))))
                .doesNotFire();
    }

    @Test
    public void testOptimizeMixedDistinctAggregation()
    {
        tester.assertThat(optimizeMixedDistinctAggregations)
                .on(p -> p.aggregate(
                        ImmutableMap.of(
                                p.symbol("count", BIGINT), createAggregation(
                                        p,
                                        p.symbol("a", BIGINT),
                                        getFunctionCall(COUNT, Optional.empty(), false, p.symbol("a", BIGINT)),
                                        Optional.empty()),
                                p.symbol("count_1", BIGINT), createAggregation(
                                        p,
                                        p.symbol("b", BIGINT),
                                        getFunctionCall(COUNT, Optional.empty(), true, p.symbol("b", BIGINT)),
                                        Optional.of(p.symbol("b$distinct", BIGINT)))),
                        ImmutableList.of(ImmutableList.of(p.symbol("b", BIGINT))),
                        p.markDistinct(
                                p.symbol("b$distinct", BIGINT),
                                ImmutableList.of(p.symbol("b", BIGINT)),
                                p.values(p.symbol("a", BIGINT), p.symbol("b", BIGINT)))))
                .matches(aggregation(
                        ImmutableList.of(ImmutableList.of("b")),
                        ImmutableMap.of(
                                Optional.of("count"), functionCall("arbitrary", ImmutableList.of("expr_1")),
                                Optional.of("count_1"), functionCall("count", ImmutableList.of("expr"))),
                        ImmutableMap.of(),
                        Optional.empty(),
                        project(
                                ImmutableMap.of("b", PlanMatchPattern.expression("b"),
                                        "expr", PlanMatchPattern.expression("IF((group_id = CAST(1 AS bigint)), b, CAST(null AS bigint))"),
                                        "expr_1", PlanMatchPattern.expression("IF((group_id = CAST(0 AS bigint)), count_0, CAST(null AS bigint))"),
                                        "b$distinct", PlanMatchPattern.expression("null")),
                                aggregation(
                                        ImmutableList.of(ImmutableList.of("b", "group_id")),
                                        ImmutableMap.of(Optional.of("count_0"), functionCall("count", ImmutableList.of("a"))),
                                        ImmutableMap.of(),
                                        Optional.empty(),
                                        groupingSet(
                                                ImmutableList.of(ImmutableList.of("a", "b"), ImmutableList.of("b")),
                                                "group_id",
                                                values(ImmutableMap.of("a", 0, "b", 1)))))));
    }

    private AggregationNode.Aggregation createAggregation(PlanBuilder p, Symbol symbol, FunctionCall functionCall, Optional<Symbol> mask)
    {
        TypeSignature typeSignature = p.getSymbols().get(symbol).getTypeSignature();
        Signature signature = functionRegistry.resolveFunction(COUNT, fromTypeSignatures(ImmutableList.of(typeSignature)));

        return new AggregationNode.Aggregation(functionCall, signature, mask);
    }

    private FunctionCall getFunctionCall(QualifiedName name, Optional<Expression> filterExpression, boolean distinct, Symbol symbol)
    {
        return new FunctionCall(name, Optional.empty(), filterExpression, distinct, ImmutableList.of(symbol.toSymbolReference()));
    }
}
