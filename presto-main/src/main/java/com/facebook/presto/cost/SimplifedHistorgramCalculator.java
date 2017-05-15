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

import com.facebook.presto.spi.statistics.RangeColumnStatistics;

import java.util.Comparator;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.getOnlyElement;

public class SimplifedHistorgramCalculator
{
    private final Optional<Object> low;
    private final Optional<Object> high;
    private final Comparator<Object> cmp;

    public SimplifedHistorgramCalculator(Optional<Object> low, Optional<Object> high, Comparator<Object> comparator)
    {
        this.low = low;
        this.high = high;
        this.cmp = comparator;
    }

    public static SimplifedHistorgramCalculator of(List<RangeColumnStatistics> rangeColumnStatistics, Comparator<Object> comparator)
    {
        RangeColumnStatistics stats = getOnlyElement(rangeColumnStatistics);
        return new SimplifedHistorgramCalculator(stats.getLowValue(), stats.getHighValue(), comparator);
    }

    public Object getLow()
    {
        return low;
    }

    public Object getHigh()
    {
        return high;
    }

    public SimplifedHistorgramCalculator intersect(SimplifedHistorgramCalculator other) {
        Object newLow = cmp.compare(low, other.low) <= 0 ? low : other.low;
        Object newHigh = cmp.compare(high, other.high) >= 0 ? high : other.high;

        checkState(cmp.compare(newLow, newHigh) <= 0);
        //return new SimplifedHistorgramCalculator(newLow, newHigh, cmp);
        return null;
    }

}
