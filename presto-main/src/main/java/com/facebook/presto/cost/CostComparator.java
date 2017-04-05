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

import java.util.Comparator;

public class CostComparator
        implements Comparator<PlanNodeCostEstimate>
{
    private final double cpuWeight;
    private final double memoryWeight;
    private final double networkWeight;

    public CostComparator(double cpuWeight, double memoryWeight, double networkWeight)
    {
        this.cpuWeight = cpuWeight;
        this.memoryWeight = memoryWeight;
        this.networkWeight = networkWeight;
    }

    @Override
    public int compare(PlanNodeCostEstimate left, PlanNodeCostEstimate right)
    {
        Estimate leftCost = left.getCpuCost().map(value -> value * cpuWeight)
                .add(left.getMemoryCost().map(value -> value * memoryWeight))
                .add(left.getNetworkCost().map(value -> value * networkWeight));

        Estimate rightCost = right.getCpuCost().map(value -> value * cpuWeight)
                .add(right.getMemoryCost().map(value -> value * memoryWeight))
                .add(right.getNetworkCost().map(value -> value * networkWeight));

        if (leftCost.isValueUnknown() || rightCost.isValueUnknown()) {
            return 0;
        }
        return Double.compare(leftCost.getValue(), rightCost.getValue());
    }
}
