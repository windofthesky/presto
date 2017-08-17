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

import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.plan.Assignments;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder.expression;

public class TestSimpleFilterProjectSemiJoinStatsRule
{
    private StatsCalculatorTester tester;

    private SymbolStatsEstimate aStats = SymbolStatsEstimate.builder()
            .setLowValue(0)
            .setHighValue(10)
            .setDistinctValuesCount(10)
            .setNullsFraction(0.1)
            .build();

    private SymbolStatsEstimate bStats = SymbolStatsEstimate.builder()
            .setLowValue(0)
            .setHighValue(100)
            .setDistinctValuesCount(10)
            .setNullsFraction(0)
            .build();

    private SymbolStatsEstimate cStats = SymbolStatsEstimate.builder()
            .setLowValue(5)
            .setHighValue(30)
            .setDistinctValuesCount(2)
            .setNullsFraction(0.5)
            .build();

    private SymbolStatsEstimate expectedFilteredPositiveAStats = SymbolStatsEstimate.builder()
            .setDistinctValuesCount(2)
            .setLowValue(0)
            .setHighValue(10)
            .setNullsFraction(0)
            .build();

    private SymbolStatsEstimate expectedFilteredPositivePlusExtraConjunctAStats = SymbolStatsEstimate.builder()
            .setDistinctValuesCount(1.6)
            .setLowValue(0)
            .setHighValue(8)
            .setNullsFraction(0)
            .build();

    private SymbolStatsEstimate expectedFilteredNegativeAStats = SymbolStatsEstimate.builder()
            .setDistinctValuesCount(8)
            .setLowValue(0)
            .setHighValue(10)
            .setNullsFraction(0)
            .build();

    @BeforeMethod
    public void setUp()
    {
        tester = new StatsCalculatorTester();
    }

    @AfterMethod
    public void tearDown()
    {
        tester.close();
        tester = null;
    }

    @Test
    public void testFilterPositiveSemiJoin()
    {
        tester.assertStatsFor(pb -> {
            Symbol a = pb.symbol("a", BIGINT);
            Symbol b = pb.symbol("b", BIGINT);
            Symbol c = pb.symbol("c", BIGINT);
            Symbol semiJoinOutput = pb.symbol("sjo", BOOLEAN);
            return pb.filter(
                    semiJoinOutput.toSymbolReference(),
                    pb.semiJoin(
                            pb.values(a, b),
                            pb.values(c),
                            a,
                            c,
                            semiJoinOutput,
                            Optional.empty(),
                            Optional.empty(),
                            Optional.empty()));
        })
                .withSourceStats(filter -> filter.getSources().get(0).getSources().get(0), PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(1000)
                        .addSymbolStatistics(new Symbol("a"), aStats)
                        .addSymbolStatistics(new Symbol("b"), bStats)
                        .build())
                .withSourceStats(filter -> filter.getSources().get(0).getSources().get(1), PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(2000)
                        .addSymbolStatistics(new Symbol("c"), cStats)
                        .build())
                .check(check -> {
                    check.outputRowsCount(180)
                            .symbolStats("a", assertion -> assertion.isEqualTo(expectedFilteredPositiveAStats))
                            .symbolStats("b", assertion -> assertion.isEqualTo(bStats))
                            .symbolStatsUnknown("c")
                            .symbolStatsUnknown("sjo");
                });
    }

    @Test
    public void testFilterPositiveNarrowingProjectSemiJoin()
    {
        tester.assertStatsFor(pb -> {
            Symbol a = pb.symbol("a", BIGINT);
            Symbol b = pb.symbol("b", BIGINT);
            Symbol c = pb.symbol("c", BIGINT);
            Symbol semiJoinOutput = pb.symbol("sjo", BOOLEAN);
            return pb.filter(
                    expression("sjo"),
                    pb.project(Assignments.identity(semiJoinOutput, a),
                            pb.semiJoin(
                                    pb.values(a, b),
                                    pb.values(c),
                                    a,
                                    c,
                                    semiJoinOutput,
                                    Optional.empty(),
                                    Optional.empty(),
                                    Optional.empty())));
        })
                .withSourceStats(filter -> filter.getSources().get(0).getSources().get(0).getSources().get(0), PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(1000)
                        .addSymbolStatistics(new Symbol("a"), aStats)
                        .addSymbolStatistics(new Symbol("b"), bStats)
                        .build())
                .withSourceStats(filter -> filter.getSources().get(0).getSources().get(0).getSources().get(1), PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(2000)
                        .addSymbolStatistics(new Symbol("c"), cStats)
                        .build())
                .check(check -> {
                    check.outputRowsCount(180)
                            .symbolStats("a", assertion -> assertion.isEqualTo(expectedFilteredPositiveAStats))
                            .symbolStatsUnknown("b")
                            .symbolStatsUnknown("c")
                            .symbolStatsUnknown("sjo");
                });
    }

    @Test
    public void testFilterPositivePlusExtraConjunctSemiJoin()
    {
        tester.assertStatsFor(pb -> {
            Symbol a = pb.symbol("a", BIGINT);
            Symbol b = pb.symbol("b", BIGINT);
            Symbol c = pb.symbol("c", BIGINT);
            Symbol semiJoinOutput = pb.symbol("sjo", BOOLEAN);
            return pb.filter(
                    expression("sjo AND a < 8"),
                    pb.semiJoin(
                            pb.values(a, b),
                            pb.values(c),
                            a,
                            c,
                            semiJoinOutput,
                            Optional.empty(),
                            Optional.empty(),
                            Optional.empty()));
        })
                .withSourceStats(filter -> filter.getSources().get(0).getSources().get(0), PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(1000)
                        .addSymbolStatistics(new Symbol("a"), aStats)
                        .addSymbolStatistics(new Symbol("b"), bStats)
                        .build())
                .withSourceStats(filter -> filter.getSources().get(0).getSources().get(1), PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(2000)
                        .addSymbolStatistics(new Symbol("c"), cStats)
                        .build())
                .check(check -> {
                    check.outputRowsCount(144)
                            .symbolStats("a", assertion -> assertion.isEqualTo(expectedFilteredPositivePlusExtraConjunctAStats))
                            .symbolStats("b", assertion -> assertion.isEqualTo(bStats))
                            .symbolStatsUnknown("c")
                            .symbolStatsUnknown("sjo");
                });
    }

    @Test
    public void testFilterNegativeSemiJoin()
    {
        tester.assertStatsFor(pb -> {
            Symbol a = pb.symbol("a", BIGINT);
            Symbol b = pb.symbol("b", BIGINT);
            Symbol c = pb.symbol("c", BIGINT);
            Symbol semiJoinOutput = pb.symbol("sjo", BOOLEAN);
            return pb.filter(
                    expression("NOT sjo"),
                    pb.semiJoin(
                            pb.values(a, b),
                            pb.values(c),
                            a,
                            c,
                            semiJoinOutput,
                            Optional.empty(),
                            Optional.empty(),
                            Optional.empty()));
        })
                .withSourceStats(filter -> filter.getSources().get(0).getSources().get(0), PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(1000)
                        .addSymbolStatistics(new Symbol("a"), aStats)
                        .addSymbolStatistics(new Symbol("b"), bStats)
                        .build())
                .withSourceStats(filter -> filter.getSources().get(0).getSources().get(1), PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(2000)
                        .addSymbolStatistics(new Symbol("c"), cStats)
                        .build())
                .check(check -> {
                    check.outputRowsCount(720)
                            .symbolStats("a", assertion -> assertion.isEqualTo(expectedFilteredNegativeAStats))
                            .symbolStats("b", assertion -> assertion.isEqualTo(bStats))
                            .symbolStatsUnknown("c")
                            .symbolStatsUnknown("sjo");
                });
    }
}
