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
package com.facebook.presto.sql.planner.planPrinter;

import com.facebook.presto.operator.ExchangeInfo;
import com.facebook.presto.operator.ExchangeOperator;
import com.google.common.collect.ImmutableList;
import io.airlift.stats.Distribution;

import java.util.List;
import java.util.Optional;

import static com.google.common.collect.Iterables.concat;

class ExchangeOperatorStats
{
    private final List<Long> splitsCount;
    private final List<Long> positionsCount;

    public static ExchangeOperatorStats create(ExchangeInfo exchangeInfo)
    {
        ExchangeOperator.ExchangeStats exchangeStats = exchangeInfo.getExchangeStats();
        ImmutableList.Builder<Long> splitsCountBuilder = ImmutableList.builder();
        ImmutableList.Builder<Long> positionCountBuilder = ImmutableList.builder();
        for (ExchangeOperator.SingleExchangeStats singleStats : exchangeStats.getStats()) {
            splitsCountBuilder.add((long) singleStats.getSplitsCount());
            positionCountBuilder.add(singleStats.getPositionCount());
        }
        return new ExchangeOperatorStats(splitsCountBuilder.build(), positionCountBuilder.build());
    }

    public ExchangeOperatorStats(List<Long> splitsCount, List<Long> positionsCount)
    {
        this.splitsCount = ImmutableList.copyOf(splitsCount);
        this.positionsCount = ImmutableList.copyOf(positionsCount);
    }

    public ExchangeOperatorStats(long splitsCount, long positionsCount)
    {
        this(ImmutableList.of(splitsCount), ImmutableList.of(positionsCount));
    }

    public List<Long> getSplitsCount()
    {
        return splitsCount;
    }

    public Distribution.DistributionSnapshot getSplitsCountDistribution()
    {
        return createDistributionSnapshot(splitsCount);
    }

    public List<Long> getPositionsCount()
    {
        return positionsCount;
    }

    public Distribution.DistributionSnapshot getPositionsCountDistribution()
    {
        return createDistributionSnapshot(positionsCount);
    }

    private static Distribution.DistributionSnapshot createDistributionSnapshot(List<Long> values)
    {
        Distribution distribution = new Distribution();
        values.forEach(distribution::add);
        return distribution.snapshot();
    }

    public static Optional<ExchangeOperatorStats> merge(Optional<ExchangeOperatorStats> first, Optional<ExchangeOperatorStats> second)
    {
        if (first.isPresent() && second.isPresent()) {
            return Optional.of(merge(first.get(), second.get()));
        }
        else if (first.isPresent()) {
            return first;
        }
        else if (second.isPresent()) {
            return second;
        }
        else {
            return Optional.empty();
        }
    }

    public static ExchangeOperatorStats merge(ExchangeOperatorStats first, ExchangeOperatorStats second)
    {
        return new ExchangeOperatorStats(
                ImmutableList.copyOf(concat(first.getSplitsCount(), second.getSplitsCount())),
                ImmutableList.copyOf(concat(first.getPositionsCount(), second.getPositionsCount())));
    }
}
