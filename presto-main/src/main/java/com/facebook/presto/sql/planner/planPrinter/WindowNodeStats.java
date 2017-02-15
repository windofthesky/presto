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

import com.facebook.presto.operator.WindowOperator;
import com.google.common.collect.ImmutableList;
import io.airlift.stats.Distribution;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

import static com.google.common.collect.Iterables.concat;
import static java.util.Objects.requireNonNull;

class WindowNodeStats
{
    private final List<Long> indexSize;
    private final List<Long> partitionRowsPerIndex;
    private final List<Long> partitionRowsPerDriver;
    private final int activeDrivers;
    private final int totalDrivers;

    public static WindowNodeStats create(WindowOperator.WindowInfo info)
    {
        ImmutableList.Builder<Long> indexSize = ImmutableList.builder();
        ImmutableList.Builder<Long> partitionRowsPerIndex = ImmutableList.builder();
        info.getWindowInfos().stream()
                .map(WindowOperator.SingleDriverWindowInfo::getIndexInfo)
                .flatMap(Collection::stream)
                .forEach(indexInfo -> {
                    indexSize.add(indexInfo.getSizeInBytes());
                    partitionRowsPerIndex.add(indexInfo.getTotalRowCountInPartitions());
                });

        ImmutableList.Builder<Long> partitionRowsPerDriver = ImmutableList.builder();
        int activeDrivers = 0;
        int totalDrivers = 0;

        for (WindowOperator.SingleDriverWindowInfo singleDriverWindowInfo : info.getWindowInfos()) {
            long partitionRowsCount = singleDriverWindowInfo.getIndexInfo().stream()
                    .mapToLong(WindowOperator.IndexInfo::getTotalRowCountInPartitions)
                    .sum();
            partitionRowsPerDriver.add(partitionRowsCount);
            totalDrivers++;
            if (partitionRowsCount > 0) {
                activeDrivers++;
            }
        }

        return new WindowNodeStats(indexSize.build(), partitionRowsPerIndex.build(), partitionRowsPerDriver.build(), activeDrivers, totalDrivers);
    }

    private WindowNodeStats(List<Long> indexSize, List<Long> partitionRowsPerIndex, List<Long> partitionRowsPerDriver, int activeDrivers, int totalDrivers)
    {
        this.indexSize = requireNonNull(indexSize, "indexSize is null");
        this.partitionRowsPerIndex = requireNonNull(partitionRowsPerIndex, "partitionRowsPerIndex is null");
        this.partitionRowsPerDriver = requireNonNull(partitionRowsPerDriver, "partitionRowsPerDriver is null");
        this.activeDrivers = activeDrivers;
        this.totalDrivers = totalDrivers;
    }

    public static Optional<WindowNodeStats> merge(Optional<WindowNodeStats> first, Optional<WindowNodeStats> second)
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

    public static WindowNodeStats merge(WindowNodeStats first, WindowNodeStats second)
    {
        return new WindowNodeStats(
                ImmutableList.copyOf(concat(first.indexSize, second.indexSize)),
                ImmutableList.copyOf(concat(first.partitionRowsPerIndex, second.partitionRowsPerIndex)),
                ImmutableList.copyOf(concat(first.partitionRowsPerDriver, second.partitionRowsPerDriver)),
                first.activeDrivers + second.activeDrivers,
                first.totalDrivers + second.totalDrivers);
    }

    public Distribution.DistributionSnapshot getIndexSizeDistribution()
    {
        return createDistributionSnapshot(indexSize);
    }

    public Distribution.DistributionSnapshot getPartitionRowsPerIndexDistribution()
    {
        return createDistributionSnapshot(partitionRowsPerIndex);
    }

    public Distribution.DistributionSnapshot getPartitionRowsPerDriverDistribution()
    {
        return createDistributionSnapshot(partitionRowsPerDriver);
    }

    private static Distribution.DistributionSnapshot createDistributionSnapshot(List<Long> values)
    {
        Distribution distribution = new Distribution();
        values.forEach(distribution::add);
        return distribution.snapshot();
    }

    public int getActiveDrivers()
    {
        return activeDrivers;
    }

    public int getTotalDrivers()
    {
        return totalDrivers;
    }
}
