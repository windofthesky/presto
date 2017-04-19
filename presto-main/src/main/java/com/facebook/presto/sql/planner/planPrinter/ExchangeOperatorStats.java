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

class ExchangeOperatorStats
{
//    private final Map<StageId, List<ExchangeOperator.SingleExchangeStats>> stats;
//
//    public static ExchangeOperatorStats create(ExchangeInfo exchangeInfo)
//    {
//        ExchangeOperator.ExchangeStats exchangeStats = exchangeInfo.getExchangeStats();
//        ImmutableList.Builder<List<Long>> splitsCountBuilder = ImmutableList.builder();
//        ImmutableList.Builder<List<Long>> positionCountBuilder = ImmutableList.builder();
//        for (List<ExchangeOperator.SingleExchangeStats> singleStageStats : exchangeStats.getStatsMap().values()) {
//            splitsCountBuilder.add(singleStageStats.stream().map(stats -> (long) stats.getSplitsCount()).collect(toImmutableList()));
//            positionCountBuilder.add(singleStageStats.stream().map(stats -> stats.getPositionCount()).collect(toImmutableList()));
//        }
//        return new ExchangeOperatorStats(splitsCountBuilder.build(), positionCountBuilder.build());
//    }
//
//    public ExchangeOperatorStats(List<List<Long>> splitsCount, List<List<Long>> positionsCount)
//    {
//        this.splitsCount = ImmutableList.copyOf(splitsCount);
//        this.positionsCount = ImmutableList.copyOf(positionsCount);
//    }
//
//    public ExchangeOperatorStats(long splitsCount, long positionsCount)
//    {
//        this(ImmutableList.of(ImmutableList.of(splitsCount)), ImmutableList.of(ImmutableList.of(positionsCount)));
//    }
//
//    public List<List<Long>> getSplitsCount()
//    {
//        return splitsCount;
//    }
//
//    public List<List<Long>> getPositionsCount()
//    {
//        return positionsCount;
//    }
//
//    public static Optional<ExchangeOperatorStats> merge(Optional<ExchangeOperatorStats> first, Optional<ExchangeOperatorStats> second)
//    {
//        if (first.isPresent() && second.isPresent()) {
//            return Optional.of(merge(first.get(), second.get()));
//        }
//        else if (first.isPresent()) {
//            return first;
//        }
//        else if (second.isPresent()) {
//            return second;
//        }
//        else {
//            return Optional.empty();
//        }
//    }
//
//    public static ExchangeOperatorStats merge(ExchangeOperatorStats first, ExchangeOperatorStats second)
//    {
//        return new ExchangeOperatorStats(
//                ImmutableList.copyOf(concat(first.getSplitsCount(), second.getSplitsCount())),
//                ImmutableList.copyOf(concat(first.getPositionsCount(), second.getPositionsCount())));
//    }
}
