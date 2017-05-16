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
import com.facebook.presto.sql.tree.ComparisonExpressionType;

import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;

public class StatsHistogramRange
{
    private final Optional<Object> low;
    private final Optional<Object> high;
    private final TypeStatOperatorCaller operatorCaller;
    private Optional<SimplifiedHistogramStats> statsParent;

    public StatsHistogramRange(Optional<Object> low, Optional<Object> high, TypeStatOperatorCaller operatorCaller, Optional<SimplifiedHistogramStats> statsParent)
    {
        this.low = low;
        this.high = high;
        this.operatorCaller = operatorCaller;
        this.statsParent = statsParent;
    }

    public void setStatsParent(Optional<SimplifiedHistogramStats> statsParent)
    {
        this.statsParent = statsParent;
    }

    public Optional<Object> getHigh()
    {
        return high;
    }

    public Optional<Object> getLow()
    {
        return low;
    }

    private Optional<Object> minmax(ComparisonExpressionType minMaxOperatorType, Optional<Object> first, Optional<Object> second)
    {
        if (!first.isPresent() || !second.isPresent()) {
            return Optional.empty();
        }
        if (operatorCaller.callComparisonOperator(minMaxOperatorType, first.get(), second.get())) {
            return first;
        }
        else {
            return second;
        }
    }

    public Optional<Object> min(Optional<Object> first, Optional<Object> second)
    {
        return minmax(ComparisonExpressionType.LESS_THAN, first, second);
    }

    public Optional<Object> max(Optional<Object> first, Optional<Object> second)
    {
        return minmax(ComparisonExpressionType.GREATER_THAN, first, second);
    }

    private Optional<Object> minmaxKnown(ComparisonExpressionType minMaxOperatorType, Optional<Object> first, Optional<Object> second)
    {
        if (first.isPresent() && second.isPresent()) {
            if (operatorCaller.callComparisonOperator(minMaxOperatorType, first.get(), second.get())) {
                return first;
            }
            else {
                return second;
            }
        }
        if (first.isPresent()) {
            return first;
        }
        return second;
    }

    public Optional<Object> minKnown(Optional<Object> first, Optional<Object> second)
    {
        return minmaxKnown(ComparisonExpressionType.LESS_THAN, first, second);
    }

    public Optional<Object> maxKnown(Optional<Object> first, Optional<Object> second)
    {
        return minmaxKnown(ComparisonExpressionType.GREATER_THAN, first, second);
    }

    public Estimate overlapPartOf(StatsHistogramRange other)
    {
        if (low.isPresent() && high.isPresent()) {
            return overlapPartOfClosedRange(other);
        }
        if (!low.isPresent() && high.isPresent()) {
            return overlapPartOfRightClosedRange(other);
        }
        if (low.isPresent() && !high.isPresent()) {
            return overlapPartOfLeftClosedRange(other);
        }
        return overlapPartOfUnknownRange(other);
    }

    private Estimate overlapPartOfRightClosedRange(StatsHistogramRange other)
    {
        // TODO check arguments
        if (other.low.isPresent() || other.high.isPresent()) {
            Optional<Object> lowestKnown = minKnown(other.high, other.low);
            if (operatorCaller.callComparisonOperator(ComparisonExpressionType.LESS_THAN, lowestKnown.get(), high.get())) {
                return Estimate.of(0.5); // Heuristic? FIXME multiply by nulls factor
            }
            return Estimate.zeroValue();
        }
        return Estimate.of(1.0); // FIXME multiply by nulls factor
    }

    private Estimate overlapPartOfLeftClosedRange(StatsHistogramRange other)
    {
        // TODO check arguments
        if (other.low.isPresent() || other.high.isPresent()) {
            Optional<Object> highestKnown = maxKnown(other.high, other.low);
            if (operatorCaller.callComparisonOperator(ComparisonExpressionType.GREATER_THAN, highestKnown.get(), low.get())) {
                return Estimate.of(0.5); // Heuristic? FIXME multiply by nulls factor
            }
            return Estimate.zeroValue();
        }
        return Estimate.of(1.0); // FIXME multiply by nulls factor
    }

    private Estimate overlapPartOfUnknownRange(StatsHistogramRange other)
    {
        // TODO check arguments
        if (other.low.isPresent() && other.high.isPresent()) {
            return Estimate.zeroValue();
        }
        if (other.low.isPresent() || other.high.isPresent()) {
            return Estimate.of(0.5); // Heuristic? TODO multiply by  nullsfact
        }
        return Estimate.of(1.0); // TODO multiply by  nullsfact
    }

    private Estimate overlapPartOfClosedRange(StatsHistogramRange other)
    {
        // TODO check argumentss
        Optional<Object> leftBound = max(low, other.low);
        Optional<Object> rightBound = min(high, other.high);
        if (!leftBound.isPresent()) {
            leftBound = low;
        }
        if (!rightBound.isPresent()) {
            rightBound = high;
        }
        double leftValue = operatorCaller.translateToDouble(leftBound.get());
        double rightValue = operatorCaller.translateToDouble(rightBound.get());
        double leftBase = operatorCaller.translateToDouble(low.get());
        double rightBase = operatorCaller.translateToDouble(high.get());

        if (leftValue > rightValue) {
            return Estimate.zeroValue();
        }
        if (leftValue == rightValue) {
            return Estimate.of(1).subtract(statsParent.get().getNullsFraction()).divide(statsParent.get().getDistinctValuesCount());
        }
        double histogramOverlap = (rightValue - leftValue) / (rightBase - leftBase);
        return Estimate.of(histogramOverlap * (1 - statsParent.get().getNullsFraction().valueOrDefault(0)));
    }

    public StatsHistogramRange intersect(StatsHistogramRange other)
    {
        checkArgument(operatorCaller == other.operatorCaller);
        return new StatsHistogramRange(max(low, other.low), min(high, other.high), operatorCaller, Optional.empty());
    }
}
