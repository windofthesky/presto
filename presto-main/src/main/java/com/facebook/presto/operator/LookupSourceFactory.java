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
package com.facebook.presto.operator;

import com.facebook.presto.operator.PartitionedConsumption.Partition;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spiller.PartitioningSpiller;
import com.facebook.presto.sql.planner.Symbol;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

public interface LookupSourceFactory
{
    List<Type> getTypes();

    List<Type> getOutputTypes();

    ListenableFuture<LookupSource> createLookupSource();

    Map<Symbol, Integer> getLayout();

    // this is only here for the index lookup source
    default void setTaskContext(TaskContext taskContext) {}

    void destroy();

    default PartitioningSpiller createProbeSpiller(OperatorContext operatorContext, List<Type> probeTypes, HashGenerator probeHashGenerator)
    {
        throw new UnsupportedOperationException();
    }

    default boolean hasSpilled()
    {
        return false;
    }

    default Iterator<Partition<LookupSource>> beginLookupSourceUnspilling(int lookupSourceConsumers)
    {
        throw new UnsupportedOperationException();
    }
}
