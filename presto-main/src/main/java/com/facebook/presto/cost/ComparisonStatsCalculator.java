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
import com.facebook.presto.sql.tree.ComparisonExpressionType;

import java.util.Optional;
import java.util.OptionalDouble;

import static com.facebook.presto.cost.FilterStatsCalculator.filterStatsForUnknownExpression;
import static com.facebook.presto.cost.SymbolStatsEstimate.buildFrom;
import static com.facebook.presto.util.MoreMath.max;
import static com.facebook.presto.util.MoreMath.min;
import static java.lang.Double.NEGATIVE_INFINITY;
import static java.lang.Double.NaN;
import static java.lang.Double.POSITIVE_INFINITY;
import static java.lang.Double.isNaN;

public class ComparisonStatsCalculator
{
    private ComparisonStatsCalculator()
    {}

    public static PlanNodeStatsEstimate comparisonExpressionToLiteralStats(
            PlanNodeStatsEstimate inputStatistics,
            Optional<Symbol> symbol,
            SymbolStatsEstimate expressionStats,
            OptionalDouble doubleLiteral,
            ComparisonExpressionType type)
    {
        switch (type) {
            case EQUAL:
                return expressionToLiteralEquality(inputStatistics, symbol, expressionStats, doubleLiteral);
            case NOT_EQUAL:
                return expressionToLiteralNonEquality(inputStatistics, symbol, expressionStats, doubleLiteral);
            case LESS_THAN:
            case LESS_THAN_OR_EQUAL:
                return expressionToLiteralLessThan(inputStatistics, symbol, expressionStats, doubleLiteral);
            case GREATER_THAN:
            case GREATER_THAN_OR_EQUAL:
                return expressionToLiteralGreaterThan(inputStatistics, symbol, expressionStats, doubleLiteral);
            case IS_DISTINCT_FROM:
            default:
                return filterStatsForUnknownExpression(inputStatistics);
        }
    }

    private static PlanNodeStatsEstimate expressionToLiteralRangeComparison(
            PlanNodeStatsEstimate inputStatistics,
            Optional<Symbol> symbol,
            SymbolStatsEstimate expressionStats,
            StatisticRange literalRange)
    {
        StatisticRange range = StatisticRange.from(expressionStats);
        StatisticRange intersectRange = range.intersect(literalRange);

        double filterFactor = range.overlapPercentWith(intersectRange);

        PlanNodeStatsEstimate estimate = inputStatistics.mapOutputRowCount(rowCount -> filterFactor * (1 - expressionStats.getNullsFraction()) * rowCount);
        if (symbol.isPresent()) {
            SymbolStatsEstimate symbolNewEstimate =
                    SymbolStatsEstimate.builder()
                            .setAverageRowSize(expressionStats.getAverageRowSize())
                            .setStatisticsRange(intersectRange)
                            .setNullsFraction(0.0).build();
            estimate = estimate.mapSymbolColumnStatistics(symbol.get(), oldStats -> symbolNewEstimate);
        }
        return estimate;
    }

    private static PlanNodeStatsEstimate expressionToLiteralEquality(
            PlanNodeStatsEstimate inputStatistics,
            Optional<Symbol> symbol,
            SymbolStatsEstimate expressionStats,
            OptionalDouble literal)
    {
        StatisticRange literalRange;
        if (literal.isPresent()) {
            literalRange = new StatisticRange(literal.getAsDouble(), literal.getAsDouble(), 1);
        }
        else {
            literalRange = new StatisticRange(NEGATIVE_INFINITY, POSITIVE_INFINITY, 1);
        }
        return expressionToLiteralRangeComparison(inputStatistics, symbol, expressionStats, literalRange);
    }

    private static PlanNodeStatsEstimate expressionToLiteralNonEquality(
            PlanNodeStatsEstimate inputStatistics,
            Optional<Symbol> symbol,
            SymbolStatsEstimate expressionStats,
            OptionalDouble literal)
    {
        StatisticRange range = StatisticRange.from(expressionStats);

        StatisticRange literalRange;
        if (literal.isPresent()) {
            literalRange = new StatisticRange(literal.getAsDouble(), literal.getAsDouble(), 1);
        }
        else {
            literalRange = new StatisticRange(NEGATIVE_INFINITY, POSITIVE_INFINITY, 1);
        }
        StatisticRange intersectRange = range.intersect(literalRange);
        double filterFactor = 1 - range.overlapPercentWith(intersectRange);

        PlanNodeStatsEstimate estimate = inputStatistics.mapOutputRowCount(rowCount -> filterFactor * (1 - expressionStats.getNullsFraction()) * rowCount);
        if (symbol.isPresent()) {
            SymbolStatsEstimate symbolNewEstimate = buildFrom(expressionStats)
                    .setNullsFraction(0.0)
                    .setDistinctValuesCount(max(expressionStats.getDistinctValuesCount() - 1, 0))
                    .build();
            estimate = estimate.mapSymbolColumnStatistics(symbol.get(), oldStats -> symbolNewEstimate);
        }
        return estimate;
    }

    private static PlanNodeStatsEstimate expressionToLiteralLessThan(
            PlanNodeStatsEstimate inputStatistics,
            Optional<Symbol> symbol,
            SymbolStatsEstimate expressionStats,
            OptionalDouble literal)
    {
        return expressionToLiteralRangeComparison(inputStatistics, symbol, expressionStats, new StatisticRange(NEGATIVE_INFINITY, literal.orElse(POSITIVE_INFINITY), NaN));
    }

    private static PlanNodeStatsEstimate expressionToLiteralGreaterThan(
            PlanNodeStatsEstimate inputStatistics,
            Optional<Symbol> symbol,
            SymbolStatsEstimate expressionStats,
            OptionalDouble literal)
    {
        return expressionToLiteralRangeComparison(inputStatistics, symbol, expressionStats, new StatisticRange(literal.orElse(NEGATIVE_INFINITY), POSITIVE_INFINITY, NaN));
    }

    public static PlanNodeStatsEstimate comparisonSymbolToSymbolStats(PlanNodeStatsEstimate inputStatistics,
            Symbol left,
            Symbol right,
            ComparisonExpressionType type)
    {
        switch (type) {
            case EQUAL:
                return symbolToSymbolEquality(inputStatistics, left, right);
            case NOT_EQUAL:
                return symbolToSymbolNonEquality(inputStatistics, left, right);
            case LESS_THAN:
            case LESS_THAN_OR_EQUAL:
            case GREATER_THAN:
            case GREATER_THAN_OR_EQUAL:
            case IS_DISTINCT_FROM:
            default:
                return filterStatsForUnknownExpression(inputStatistics);
        }
    }

    private static PlanNodeStatsEstimate symbolToSymbolEquality(PlanNodeStatsEstimate inputStatistics,
            Symbol left,
            Symbol right)
    {
        SymbolStatsEstimate leftStats = inputStatistics.getSymbolStatistics(left);
        SymbolStatsEstimate rightStats = inputStatistics.getSymbolStatistics(right);

        if (isNaN(leftStats.getDistinctValuesCount()) || isNaN(rightStats.getDistinctValuesCount())) {
            filterStatsForUnknownExpression(inputStatistics);
        }

        StatisticRange leftRange = StatisticRange.from(leftStats);
        StatisticRange rightRange = StatisticRange.from(rightStats);

        StatisticRange intersect = leftRange.intersect(rightRange);

        double nullsFilterFactor = (1 - leftStats.getNullsFraction()) * (1 - rightStats.getNullsFraction());
        double leftFilterFactor = firstNonNaN(leftRange.overlapPercentWith(intersect), 1);
        double rightFilterFactor = firstNonNaN(rightRange.overlapPercentWith(intersect), 1);
        double leftNdvInRange = leftFilterFactor * leftRange.getDistinctValuesCount();
        double rightNdvInRange = rightFilterFactor * rightRange.getDistinctValuesCount();
        double filterFactor = 1 * leftFilterFactor * rightFilterFactor / max(leftNdvInRange, rightNdvInRange, 1);
        double retainedNdv = min(leftNdvInRange, rightNdvInRange);

        SymbolStatsEstimate newLeftStats = buildFrom(leftStats)
                .setNullsFraction(0)
                .setStatisticsRange(intersect)
                .setDistinctValuesCount(retainedNdv)
                .build();
        SymbolStatsEstimate newRightStats = buildFrom(rightStats)
                .setNullsFraction(0)
                .setStatisticsRange(intersect)
                .setDistinctValuesCount(retainedNdv)
                .build();

        return inputStatistics.mapOutputRowCount(size -> size * filterFactor * nullsFilterFactor)
                .mapSymbolColumnStatistics(left, oldLeftStats -> newLeftStats)
                .mapSymbolColumnStatistics(right, oldRightStats -> newRightStats);
    }

    private static PlanNodeStatsEstimate symbolToSymbolNonEquality(PlanNodeStatsEstimate inputStatistics,
            Symbol left,
            Symbol right)
    {
        return PlanNodeStatsEstimateMath.differenceInStats(inputStatistics, symbolToSymbolEquality(inputStatistics, left, right));
    }

    private static double firstNonNaN(double... values)
    {
        for (double value : values) {
            if (!isNaN(value)) {
                return value;
            }
        }
        throw new IllegalArgumentException("All values NaN");
    }
}
