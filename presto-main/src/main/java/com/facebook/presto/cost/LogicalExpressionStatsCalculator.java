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

import java.util.stream.Stream;

import static java.lang.Double.max;
import static java.lang.Double.min;

public class LogicalExpressionStatsCalculator
{
    private final PlanNodeStatsEstimate inputStatistics;

    public LogicalExpressionStatsCalculator(PlanNodeStatsEstimate inputStatistics)
    {
        this.inputStatistics = inputStatistics;
    }

    public PlanNodeStatsEstimate subtractStats(PlanNodeStatsEstimate left, PlanNodeStatsEstimate right)
    {
        PlanNodeStatsEstimate.Builder statsBuilder = PlanNodeStatsEstimate.builder();
        double newRowCount = left.getOutputRowCount() - right.getOutputRowCount();

        Stream.concat(left.getSymbolsWithKnownStatistics().stream(), right.getSymbolsWithKnownStatistics().stream())
                .forEach(symbol -> {
                    statsBuilder.addSymbolStatistics(symbol,
                            subtractColumnStats(left.getSymbolStatistics(symbol),
                                    left.getOutputRowCount(),
                                    right.getSymbolStatistics(symbol),
                                    right.getOutputRowCount(),
                                    newRowCount));
                });

        return statsBuilder.setOutputRowCount(newRowCount).build();
    }

    private SymbolStatsEstimate subtractColumnStats(SymbolStatsEstimate leftStats, double leftRowCount, SymbolStatsEstimate rightStats, double rightRowCount, double newRowCount)
    {
        StatisticRange leftRange = new StatisticRange(leftStats.getLowValue(), leftStats.getHighValue(), leftStats.getDistinctValuesCount());
        StatisticRange rightRange = new StatisticRange(rightStats.getLowValue(), rightStats.getHighValue(), rightStats.getDistinctValuesCount());

        StatisticRange union = leftRange.subtract(rightRange);
        double nullsCountLeft = leftStats.getNullsFraction() * leftRowCount;
        double nullsCountRight = rightStats.getNullsFraction() * rightRowCount;
        double innerFilterFactor = nullsCountRight / nullsCountLeft;

        return SymbolStatsEstimate.builder()
                .setDistinctValuesCount(union.getDistinctValuesCount())
                .setHighValue(union.getHigh())
                .setLowValue(union.getLow())
                .setAverageRowSize(leftStats.getAverageRowSize() * innerFilterFactor - (rightStats.getAverageRowSize() - leftStats.getAverageRowSize()) * (1 - innerFilterFactor)) //FIXME? // left and right should be equal in most cases anyway
                .setNullsFraction((nullsCountLeft - nullsCountRight) / newRowCount)
                .build();
    }

    public PlanNodeStatsEstimate unionStats(PlanNodeStatsEstimate left, PlanNodeStatsEstimate right)
    {
        PlanNodeStatsEstimate.Builder statsBuilder = PlanNodeStatsEstimate.builder();

        double leftFilterFactor = left.getOutputRowCount() / inputStatistics.getOutputRowCount();
        double rightFilterFactor = right.getOutputRowCount() / inputStatistics.getOutputRowCount();
        double totalRowsWithOverlaps = (leftFilterFactor + rightFilterFactor) * inputStatistics.getOutputRowCount();
        double intersectingRows = intersectStats(left, right).getOutputRowCount();
        double newRowCount = totalRowsWithOverlaps - intersectingRows;

        Stream.concat(left.getSymbolsWithKnownStatistics().stream(), right.getSymbolsWithKnownStatistics().stream())
                .forEach(symbol -> {
                    statsBuilder.addSymbolStatistics(symbol,
                            unionColumnStats(left.getSymbolStatistics(symbol),
                                    left.getOutputRowCount(),
                                    right.getSymbolStatistics(symbol),
                                    right.getOutputRowCount(), newRowCount));
                });

        return statsBuilder.setOutputRowCount(newRowCount).build();
    }

    private SymbolStatsEstimate unionColumnStats(SymbolStatsEstimate leftStats, double leftRows, SymbolStatsEstimate rightStats, double rightRows, double newRowCount)
    {
        StatisticRange leftRange = new StatisticRange(leftStats.getLowValue(), leftStats.getHighValue(), leftStats.getDistinctValuesCount());
        StatisticRange rightRange = new StatisticRange(rightStats.getLowValue(), rightStats.getHighValue(), rightStats.getDistinctValuesCount());

        StatisticRange union = leftRange.union(rightRange);
        double nullsCountLeft = leftStats.getNullsFraction() * rightRows;
        double nullsCountRight = rightStats.getNullsFraction() * leftRows;

        return SymbolStatsEstimate.builder()
                .setDistinctValuesCount(union.getDistinctValuesCount())
                .setHighValue(union.getHigh())
                .setLowValue(union.getLow())
                .setAverageRowSize((leftStats.getAverageRowSize() + rightStats.getAverageRowSize()) / 2) // FIXME, weights to average. left and right should be equal in most cases anyway
                .setNullsFraction(max(nullsCountLeft, nullsCountRight) / newRowCount)
                .build();
    }

    public PlanNodeStatsEstimate intersectStats(PlanNodeStatsEstimate left, PlanNodeStatsEstimate right)
    {
        PlanNodeStatsEstimate.Builder statsBuilder = PlanNodeStatsEstimate.builder();

        double minFilterFactor = Stream.concat(left.getSymbolsWithKnownStatistics().stream(), right.getSymbolsWithKnownStatistics().stream())
                .mapToDouble(symbol -> filterFactorOfIntersect(inputStatistics.getSymbolStatistics(symbol), left.getSymbolStatistics(symbol), right.getSymbolStatistics(symbol)))
                .min().orElse(1.0);
        double newRowCount = minFilterFactor * inputStatistics.getOutputRowCount();

        Stream.concat(left.getSymbolsWithKnownStatistics().stream(), right.getSymbolsWithKnownStatistics().stream())
                .forEach(symbol -> {
                    statsBuilder.addSymbolStatistics(symbol,
                            intersectColumnStats(left.getSymbolStatistics(symbol),
                                    left.getOutputRowCount(),
                                    right.getSymbolStatistics(symbol),
                                    right.getOutputRowCount(), newRowCount));
                });

        return statsBuilder.setOutputRowCount(newRowCount).build();
    }

    private double filterFactorOfIntersect(SymbolStatsEstimate inputStats, SymbolStatsEstimate leftStats, SymbolStatsEstimate rightStats)
    {
        StatisticRange inputRange = new StatisticRange(inputStats.getLowValue(), inputStats.getHighValue(), inputStats.getDistinctValuesCount());
        StatisticRange leftRange = new StatisticRange(leftStats.getLowValue(), leftStats.getHighValue(), leftStats.getDistinctValuesCount());
        StatisticRange rightRange = new StatisticRange(rightStats.getLowValue(), rightStats.getHighValue(), rightStats.getDistinctValuesCount());

        return inputRange.overlapPercentWith(leftRange.intersect(rightRange));
    }

    private SymbolStatsEstimate intersectColumnStats(SymbolStatsEstimate leftStats, double leftRows, SymbolStatsEstimate rightStats, double rightRows, double newRowCount)
    {
        StatisticRange leftRange = new StatisticRange(leftStats.getLowValue(), leftStats.getHighValue(), leftStats.getDistinctValuesCount());
        StatisticRange rightRange = new StatisticRange(rightStats.getLowValue(), rightStats.getHighValue(), rightStats.getDistinctValuesCount());

        StatisticRange intersect = leftRange.intersect(rightRange);
        double nullsCountLeft = leftStats.getNullsFraction() * rightRows;
        double nullsCountRight = rightStats.getNullsFraction() * leftRows;

        return SymbolStatsEstimate.builder()
                .setDistinctValuesCount(intersect.getDistinctValuesCount())
                .setHighValue(intersect.getHigh())
                .setLowValue(intersect.getLow())
                .setAverageRowSize((leftStats.getAverageRowSize() + rightStats.getAverageRowSize()) / 2) // FIXME (weights to average) left and right should be equal in most cases anyway
                .setNullsFraction(min(nullsCountLeft, nullsCountRight) / newRowCount)
                .build();
    }
}
