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
import com.facebook.presto.spi.Page;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.Iterator;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class SpilledLookupJoiner
{
    private final Partition<LookupSource> lookupPartition;
    private final LookupJoiner lookupJoiner;
    private final Iterator<Page> probePages;

    public SpilledLookupJoiner(
            Partition<LookupSource> lookupPartition,
            LookupJoiner lookupJoiner,
            Iterator<Page> probePages)
    {
        this.lookupPartition = requireNonNull(lookupPartition, "lookupPartition is null");
        this.lookupJoiner = lookupJoiner;
        this.probePages = requireNonNull(probePages, "probePages is null");
    }

    public ListenableFuture<?> isBlocked()
    {
        return lookupJoiner.isBlocked();
    }

    public Page getOutput()
    {
        if (!lookupJoiner.needsInput()) {
            return lookupJoiner.getOutput();
        }
        if (!probePages.hasNext()) {
            lookupJoiner.finish();
            return null;
        }
        Page probePage = probePages.next();
        lookupJoiner.addInput(probePage, spillingState);

        return lookupJoiner.getOutput();
    }

    public boolean isFinished()
    {
        return lookupJoiner.isFinished();
    }

    public void finish()
    {
        checkState(lookupPartition.load().isDone());
        lookupPartition.release();
    }
}
