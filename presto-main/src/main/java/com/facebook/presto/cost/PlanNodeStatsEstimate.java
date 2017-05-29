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
import com.facebook.presto.spi.statistics.Estimate;
import com.facebook.presto.spi.statistics.RangeColumnStatistics;
import com.facebook.presto.sql.planner.Symbol;
import com.google.common.collect.ImmutableMap;

import java.util.AbstractMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.facebook.presto.spi.statistics.Estimate.unknownValue;
import static java.util.Objects.requireNonNull;

public class PlanNodeStatsEstimate
{
    public static final PlanNodeStatsEstimate UNKNOWN_STATS = PlanNodeStatsEstimate.builder().build();
    public static final double DEFAULT_ROW_SIZE = 42;

    private final Estimate outputRowCount;
    private final Estimate outputSizeInBytes;
    private final Map<Symbol, ColumnStatistics> symbolStatistics;

    private PlanNodeStatsEstimate(Estimate outputRowCount, Estimate outputSizeInBytes, Map<Symbol, ColumnStatistics> symbolStatistics)
    {
        this.outputRowCount = requireNonNull(outputRowCount, "outputRowCount can not be null");
        this.outputSizeInBytes = requireNonNull(outputSizeInBytes, "outputSizeInBytes can not be null");
        this.symbolStatistics = symbolStatistics;
    }

    public Estimate getOutputRowCount()
    {
        return outputRowCount;
    }

    public Estimate getOutputSizeInBytes()
    {
        return outputSizeInBytes;
    }

    public PlanNodeStatsEstimate mapOutputRowCount(Function<Double, Double> mappingFunction)
    {
        return buildFrom(this).setOutputRowCount(outputRowCount.map(mappingFunction)).build();
    }

    public PlanNodeStatsEstimate mapOutputSizeInBytes(Function<Double, Double> mappingFunction)
    {
        return buildFrom(this).setOutputSizeInBytes(outputRowCount.map(mappingFunction)).build();
    }

    public PlanNodeStatsEstimate mapSymbolColumnStatistics(Symbol symbol, Function<RangeColumnStatistics, RangeColumnStatistics> mappingFunction)
    {
        return buildFrom(this).setSymbolStatistics(symbolStatistics.entrySet().stream().map(
                stat -> {
                    if (stat.getKey().equals(symbol)) {
                        return new AbstractMap.SimpleEntry<>(stat.getKey(),
                                ColumnStatistics.builder()
                                        .addRange(mappingFunction.apply(stat.getValue().getOnlyRangeColumnStatistics()))
                                        .build());
                    }
                    return stat;
                }).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))).build();
    }

    public PlanNodeStatsEstimate add(PlanNodeStatsEstimate other)
    {
        ImmutableMap.Builder<Symbol, ColumnStatistics> symbolsStatsBuilder = ImmutableMap.builder();
        symbolsStatsBuilder.putAll(getSymbolStatistics()).putAll(other.getSymbolStatistics()); // This may not count all information

        PlanNodeStatsEstimate.Builder statsBuilder = PlanNodeStatsEstimate.builder();
        return statsBuilder.setSymbolStatistics(symbolsStatsBuilder.build())
                .setOutputRowCount(getOutputRowCount().add(other.getOutputRowCount()))
                .setOutputSizeInBytes(getOutputSizeInBytes().add(other.getOutputSizeInBytes()))
                .build();
    }

    public RangeColumnStatistics getOnlyRangeStats(Symbol symbol)
    {
        return symbolStatistics.get(symbol).getOnlyRangeColumnStatistics();
    }

    public boolean containsSymbolStats(Symbol symbol)
    {
        return symbolStatistics.containsKey(symbol);
    }

    @Override
    public String toString()
    {
        return "PlanNodeStatsEstimate{outputRowCount=" + outputRowCount + ", outputSizeInBytes=" + outputSizeInBytes + '}';
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PlanNodeStatsEstimate that = (PlanNodeStatsEstimate) o;
        return Objects.equals(outputRowCount, that.outputRowCount) &&
                Objects.equals(outputSizeInBytes, that.outputSizeInBytes);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(outputRowCount, outputSizeInBytes);
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static Builder buildFrom(PlanNodeStatsEstimate other)
    {
        return builder().setOutputRowCount(other.getOutputRowCount())
                .setOutputSizeInBytes(other.getOutputSizeInBytes())
                .setSymbolStatistics(other.getSymbolStatistics());
    }

    public Map<Symbol, ColumnStatistics> getSymbolStatistics()
    {
        return symbolStatistics;
    }

    public static final class Builder
    {
        private Estimate outputRowCount = unknownValue();
        private Estimate outputSizeInBytes = unknownValue();
        private Map<Symbol, ColumnStatistics> symbolStatistics = new HashMap<>();

        public Builder setOutputRowCount(Estimate outputRowCount)
        {
            this.outputRowCount = outputRowCount;
            return this;
        }

        public Builder setOutputSizeInBytes(Estimate outputSizeInBytes)
        {
            this.outputSizeInBytes = outputSizeInBytes;
            return this;
        }

        public Builder setSymbolStatistics(Map<Symbol, ColumnStatistics> symbolStatistics)
        {
            this.symbolStatistics = symbolStatistics;
            return this;
        }

        public PlanNodeStatsEstimate build()
        {
            if (outputSizeInBytes.isValueUnknown() && !outputRowCount.isValueUnknown()) {
                outputSizeInBytes = new Estimate(DEFAULT_ROW_SIZE * outputRowCount.getValue());
            }
            return new PlanNodeStatsEstimate(outputRowCount, outputSizeInBytes, symbolStatistics);
        }
    }
}
