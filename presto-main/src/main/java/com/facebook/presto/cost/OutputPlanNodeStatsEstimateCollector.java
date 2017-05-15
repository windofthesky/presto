package com.facebook.presto.cost;

import com.facebook.presto.spi.statistics.ColumnStatistics;
import com.facebook.presto.spi.statistics.Estimate;
import com.facebook.presto.sql.planner.Symbol;

import java.util.Map;

public class PlanNodeStatsEstimateCollector
{
    private Estimate outputRowCount;
    private Estimate outputSizeInBytes;
    private Map<Symbol, SimplifiedHistogramStats> symbolStatistics;

    private PlanNodeStatsEstimateCollector(Estimate outputRowCount, Estimate outputSizeInBytes, Map<Symbol, SimplifiedHistogramStats> histogramStatsMap)
    {

    }

    public static OutputPlanNodeStatsEstimateCollector basedOn(PlanNodeStatsEstimate base)
    {
        this.outputRowCount = base.getOutputRowCount();
    }

    public PlanNodeStatsEstimateCollector filterDataPercent(double percent)
    {
        return this;
    }

    public PlanNodeStatsEstimateCollector changeRowCount(Estimate newRowCount)
    {
        return this;
    }

    public PlanNodeStatsEstimateCollector changeOutputSizeInBytes(Estimate newOutputSizeInBytes)
    {
        return this;
    }

    public PlanNodeStatsEstimateCollector changeDistinctValuesForSymbol(Symbol symbol, Estimate newDistinctValues)
    {
        return this;
    }

    public PlanNodeStatsEstimateCollector changeSymbolNullPercentage(Symbol symbol, Estimate newNullsFraction)
    {
        return this;
    }

    public PlanNodeStatsEstimateCollector changeDataSize(Symbol symbol, Estimate dataSize)
    {
        return this;
    }

    public PlanNodeStatsEstimateCollector ndvBoundCount()
    {
        return this;
    }

    public PlanNodeStatsEstimate collect()
    {

    }
}
