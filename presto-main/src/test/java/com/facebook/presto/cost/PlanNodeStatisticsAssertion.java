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

import com.facebook.presto.spi.statistics.ColumnStatistics;
import com.facebook.presto.sql.planner.Symbol;

import java.util.function.Consumer;

import static com.google.common.collect.Sets.union;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class PlanNodeStatisticsAssertion
{
    private final PlanNodeStatsEstimate actual;

    private PlanNodeStatisticsAssertion(PlanNodeStatsEstimate actual)
    {
        this.actual = actual;
    }

    public static PlanNodeStatisticsAssertion assertThat(PlanNodeStatsEstimate actual)
    {
        return new PlanNodeStatisticsAssertion(actual);
    }

    public PlanNodeStatisticsAssertion outputRowsCount(double expected)
    {
        assertEquals(actual.getOutputRowCount().getValue(), expected, "outputRowsCount mismatch");
        return this;
    }

    public PlanNodeStatisticsAssertion outputRowsCountUnknown()
    {
        assertTrue(actual.getOutputRowCount().isValueUnknown(),
                "expected unknown outputRowsCount but got " + actual.getOutputRowCount().getValue());
        return this;
    }

    public PlanNodeStatisticsAssertion symbolStats(String symbolName, Consumer<ColumnStatisticsAssertion> columnAssertionConsumer)
    {
        return symbolStats(new Symbol(symbolName), columnAssertionConsumer);
    }

    public PlanNodeStatisticsAssertion symbolStats(Symbol symbol, Consumer<ColumnStatisticsAssertion> columnAssertionConsumer)
    {
        ColumnStatisticsAssertion columnAssertion = ColumnStatisticsAssertion.assertThat(actual.getSymbolStatistics(symbol));
        columnAssertionConsumer.accept(columnAssertion);
        return this;
    }

    public PlanNodeStatisticsAssertion symbolStatsUnknown(String symbolName)
    {
        return symbolStatsUnknown(new Symbol(symbolName));
    }

    public PlanNodeStatisticsAssertion symbolStatsUnknown(Symbol symbol)
    {
        return symbolStats(symbol,
                columnStats -> columnStats
                        .lowValueUnknown()
                        .highValueUnknown()
                        .nullsFractionUnknown()
                        .distinctValuesCountUnknown());
    }

    public PlanNodeStatisticsAssertion equalTo(PlanNodeStatsEstimate expected)
    {
        assertEquals(actual.getOutputRowCount(), expected.getOutputRowCount());

        for (Symbol symbol : union(expected.getSymbolStatistics().keySet(), actual.getSymbolStatistics().keySet())) {
            assertSymbolStatsEqual(symbol, actual.getSymbolStatistics(symbol), expected.getSymbolStatistics(symbol));
        }
        return this;
    }

    private void assertSymbolStatsEqual(Symbol symbol, ColumnStatistics actual, ColumnStatistics expected)
    {
        assertEquals(actual.getNullsFraction(), expected.getNullsFraction(), "nullsFraction mismatch for " + symbol.getName());
        assertEquals(actual.getOnlyRangeColumnStatistics().getFraction(), expected.getOnlyRangeColumnStatistics().getFraction(), "non-null fraction mismatch for " + symbol.getName());
        assertEquals(actual.getOnlyRangeColumnStatistics().getLowValue(), expected.getOnlyRangeColumnStatistics().getLowValue(), "lowValue mismatch for " + symbol.getName());
        assertEquals(actual.getOnlyRangeColumnStatistics().getHighValue(), expected.getOnlyRangeColumnStatistics().getHighValue(), "lowValue mismatch for " + symbol.getName());
        assertEquals(actual.getOnlyRangeColumnStatistics().getDistinctValuesCount(), expected.getOnlyRangeColumnStatistics().getDistinctValuesCount(), "lowValue mismatch for " + symbol.getName());
        assertEquals(actual.getOnlyRangeColumnStatistics().getDataSize(), expected.getOnlyRangeColumnStatistics().getDataSize(), "datasize mismatch for " + symbol.getName());
    }
}
