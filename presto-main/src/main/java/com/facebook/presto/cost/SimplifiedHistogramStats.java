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

import com.facebook.presto.spi.statistics.Estimate;
import com.facebook.presto.spi.statistics.RangeColumnStatistics;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.cost.SimplifiedHistogramStats.SimplifedHistogramStatsBuilder.aSimplifedHistogramStats;
import static com.google.common.collect.Iterables.getOnlyElement;

public class SimplifiedHistogramStats
{
    private final StatsHistogramRange range;

    private final Estimate dataSize;
    private final Estimate nullsFraction;
    private final Estimate distinctValuesCount;
    private final Estimate filteredPercent; // HACK!

    private SimplifiedHistogramStats(StatsHistogramRange range, Estimate dataSize, Estimate nullsFraction, Estimate distinctValuesCount, Estimate filteredPercent)
    {
        this.range = range;
        this.dataSize = dataSize;
        this.nullsFraction = nullsFraction;
        this.distinctValuesCount = distinctValuesCount;
        this.filteredPercent = filteredPercent;
    }

    public RangeColumnStatistics toRangeColumnStatistics()
    {
        return RangeColumnStatistics.builder().setHighValue(range.getHigh())
                .setLowValue(range.getLow())
                .setNullsFraction(nullsFraction)
                .setDistinctValuesCount(distinctValuesCount)
                .setDataSize(dataSize)
                .build();
    }

    public static SimplifiedHistogramStats of(List<RangeColumnStatistics> rangeColumnStatistics, TypeStatOperatorCaller operatorCaller)
    {
        RangeColumnStatistics stats = getOnlyElement(rangeColumnStatistics);
        SimplifiedHistogramStats newStats = new SimplifiedHistogramStats(
                new StatsHistogramRange(stats.getLowValue(), stats.getHighValue(), operatorCaller, Optional.empty()),
                stats.getDataSize(),
                stats.getNullsFraction(),
                stats.getDistinctValuesCount(),
                Estimate.unknownValue());
        newStats.range.setStatsParent(Optional.of(newStats));
        return newStats;
    }

    public Optional<Object> getLow()
    {
        return range.getLow();
    }

    public Optional<Object> getHigh()
    {
        return range.getHigh();
    }

    public SimplifiedHistogramStats intersect(StatsHistogramRange other)
    {
        StatsHistogramRange newRange = range.intersect(other);
        Estimate percentOfDataLeft = range.overlapPartOf(newRange);

        return aSimplifedHistogramStats().withRange(newRange)
                .withDataSize(dataSize.multiply(percentOfDataLeft))
                .withNullsFraction(Estimate.zeroValue())
                .withDistinctValuesCount(distinctValuesCount.multiply(percentOfDataLeft)) // FIXME nulls not counted!
                .withFilteredPercent(percentOfDataLeft)
                .build();
    }

    public Estimate getDistinctValuesCount()
    {
        return distinctValuesCount;
    }

    public Estimate getNullsFraction()
    {
        return nullsFraction;
    }

    public Estimate getFilteredPercent()
    {
        return filteredPercent;
    }

    public StatsHistogramRange getRange()
    {
        return range;
    }

    public static final class SimplifedHistogramStatsBuilder
    {
        private StatsHistogramRange range;
        private Estimate dataSize;
        private Estimate nullsFraction;
        private Estimate distinctValuesCount;
        private Estimate filteredPercent; //FIXME HACK!

        private SimplifedHistogramStatsBuilder() {}

        public static SimplifedHistogramStatsBuilder aSimplifedHistogramStats() { return new SimplifedHistogramStatsBuilder();}

        public SimplifedHistogramStatsBuilder withRange(StatsHistogramRange range)
        {
            this.range = range;
            return this;
        }

        public SimplifedHistogramStatsBuilder withDataSize(Estimate dataSize)
        {
            this.dataSize = dataSize;
            return this;
        }

        public SimplifedHistogramStatsBuilder withNullsFraction(Estimate nullsFraction)
        {
            this.nullsFraction = nullsFraction;
            return this;
        }

        public SimplifedHistogramStatsBuilder withDistinctValuesCount(Estimate distinctValuesCount)
        {
            this.distinctValuesCount = distinctValuesCount;
            return this;
        }

        public SimplifedHistogramStatsBuilder withFilteredPercent(Estimate filtered) // HACK!
        {
            this.filteredPercent = filtered;
            return this;
        }

        public SimplifiedHistogramStats build()
        {
            SimplifiedHistogramStats simplifiedHistogramStats = new SimplifiedHistogramStats(
                   range, dataSize, nullsFraction, distinctValuesCount, filteredPercent);
            simplifiedHistogramStats.range.setStatsParent(Optional.of(simplifiedHistogramStats));
            return simplifiedHistogramStats;
        }
    }
}
