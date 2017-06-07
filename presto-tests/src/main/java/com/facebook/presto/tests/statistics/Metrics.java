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
package com.facebook.presto.tests.statistics;

import com.facebook.presto.cost.PlanNodeStatsEstimate;
import com.facebook.presto.spi.statistics.ColumnStatistics;
import com.facebook.presto.spi.statistics.Estimate;
import com.facebook.presto.spi.statistics.RangeColumnStatistics;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.Decimals;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;
import com.facebook.presto.testing.MaterializedRow;
import io.airlift.slice.Slices;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Optional;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.RealType.REAL;

public final class Metrics
{
    private Metrics() {}

    public static final Metric<Double> OUTPUT_ROW_COUNT = new Metric<Double>()
    {
        @Override
        public Optional<Double> getValueFromPlanNodeEstimate(PlanNodeStatsEstimate planNodeStatsEstimate, StatsContext statsContext)
        {
            return asOptional(planNodeStatsEstimate.getOutputRowCount());
        }

        @Override
        public Optional<Double> getValueFromAggregationQuery(MaterializedRow aggregationQueryResult, int fieldId, StatsContext statsContext)
        {
            return Optional.of(((Number) aggregationQueryResult.getField(fieldId)).doubleValue());
        }

        @Override
        public String getComputingAggregationSql()
        {
            return "count(*)";
        }

        @Override
        public String getName()
        {
            return "OUTPUT_ROW_COUNT";
        }
    };

    public static Metric<Double> nullsFraction(String columnName)
    {
        return new Metric<Double>()
        {
            @Override
            public Optional<Double> getValueFromPlanNodeEstimate(PlanNodeStatsEstimate planNodeStatsEstimate, StatsContext statsContext)
            {
                return asOptional(getColumnStatistics(planNodeStatsEstimate, columnName, statsContext).getNullsFraction());
            }

            @Override
            public Optional<Double> getValueFromAggregationQuery(MaterializedRow aggregationQueryResult, int fieldId, StatsContext statsContext)
            {
                return Optional.of(((Number) aggregationQueryResult.getField(fieldId)).doubleValue());
            }

            @Override
            public String getComputingAggregationSql()
            {
                return "(count(*) filter(where " + columnName + " is null)) / cast(count(*) as double)";
            }

            @Override
            public String getName()
            {
                return "NULLS_FRACTION(" + columnName + ")";
            }
        };
    }

    public static Metric<Double> distinctValuesCount(String columnName)
    {
        return new Metric<Double>()
        {
            @Override
            public Optional<Double> getValueFromPlanNodeEstimate(PlanNodeStatsEstimate planNodeStatsEstimate, StatsContext statsContext)
            {
                return asOptional(getOnlyRangeStatistics(planNodeStatsEstimate, columnName, statsContext).getDistinctValuesCount());
            }

            @Override
            public Optional getValueFromAggregationQuery(MaterializedRow aggregationQueryResult, int fieldId, StatsContext statsContext)
            {
                return Optional.of(((Number) aggregationQueryResult.getField(fieldId)).doubleValue());
            }

            @Override
            public String getComputingAggregationSql()
            {
                return "count(distinct " + columnName + ")";
            }

            @Override
            public String getName()
            {
                return "DISTINCT_VALUES_COUNT(" + columnName + ")";
            }
        };
    }

    public static Metric<Object> lowValue(String columnName)
    {
        return new Metric<Object>()
        {
            @Override
            public Optional<Object> getValueFromPlanNodeEstimate(PlanNodeStatsEstimate planNodeStatsEstimate, StatsContext statsContext)
            {
                return getOnlyRangeStatistics(planNodeStatsEstimate, columnName, statsContext).getLowValue();
            }

            @Override
            public Optional<Object> getValueFromAggregationQuery(MaterializedRow aggregationQueryResult, int fieldId, StatsContext statsContext)
            {
                Type targetType = statsContext.getTypeForSymbol(statsContext.getSymbolForColumn(columnName));
                return Optional.ofNullable(toPrestoTypeDomain(aggregationQueryResult.getField(fieldId), targetType));
            }

            @Override
            public String getComputingAggregationSql()
            {
                return "min(" + columnName + ")";
            }

            @Override
            public String getName()
            {
                return "LOW_VALUE(" + columnName + ")";
            }
        };
    }

    public static Metric<Object> highValue(String columnName)
    {
        return new Metric<Object>()
        {
            @Override
            public Optional<Object> getValueFromPlanNodeEstimate(PlanNodeStatsEstimate planNodeStatsEstimate, StatsContext statsContext)
            {
                return getOnlyRangeStatistics(planNodeStatsEstimate, columnName, statsContext).getHighValue();
            }

            @Override
            public Optional<Object> getValueFromAggregationQuery(MaterializedRow aggregationQueryResult, int fieldId, StatsContext statsContext)
            {
                Type targetType = statsContext.getTypeForSymbol(statsContext.getSymbolForColumn(columnName));
                return Optional.ofNullable(toPrestoTypeDomain(aggregationQueryResult.getField(fieldId), targetType));
            }

            @Override
            public String getComputingAggregationSql()
            {
                return "max(" + columnName + ")";
            }

            @Override
            public String getName()
            {
                return "HIGH_VALUE(" + columnName + ")";
            }
        };
    }

    private static Object toPrestoTypeDomain(Object javaValue, Type prestoType)
    {
        if (javaValue == null) {
            return null;
        }
        if (prestoType.equals(BIGINT)
                || prestoType.equals(INTEGER)) {
            return ((Number) javaValue).longValue();
        }
        else if (prestoType.equals(DOUBLE)) {
            return ((Number) javaValue).doubleValue();
        }
        else if (prestoType.equals(REAL)) {
            return Float.floatToRawIntBits(((Number) javaValue).floatValue());
        }
        else if (prestoType instanceof DecimalType) {
            DecimalType decimalType = (DecimalType) prestoType;
            BigInteger unscaledValue = Decimals.rescale(((BigDecimal) javaValue), decimalType).unscaledValue();
            if (decimalType.isShort()) {
                return unscaledValue.longValue();
            }
            else {
                return Decimals.encodeUnscaledValue(unscaledValue);
            }
        }
        else if (prestoType instanceof VarcharType) {
            return Slices.utf8Slice((String) javaValue);
        }
        else {
            throw new IllegalArgumentException("unsupported type " + prestoType);
        }
    }

    private static ColumnStatistics getColumnStatistics(PlanNodeStatsEstimate planNodeStatsEstimate, String columnName, StatsContext statsContext)
    {
        return planNodeStatsEstimate.getSymbolStatistics(statsContext.getSymbolForColumn(columnName));
    }

    private static RangeColumnStatistics getOnlyRangeStatistics(PlanNodeStatsEstimate planNodeStatsEstimate, String columnName, StatsContext statsContext)
    {
        return getColumnStatistics(planNodeStatsEstimate, columnName, statsContext).getOnlyRangeColumnStatistics();
    }

    private static Optional<Double> asOptional(Estimate estimate)
    {
        return estimate.isValueUnknown() ? Optional.empty() : Optional.of(estimate.getValue());
    }
}
