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

import com.google.common.collect.ImmutableList;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.ThreadSafe;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public final class Menago // TODO rename SpillMemoryHandlingAndCoordinatingMenago
{

    private final int partitionsCount;

    /* writes */ @GuardedBy("this")
    private volatile SpillingStateSnapshot spillingState;

    private Set<LookupJoiner> activeLookupJoiners = new HashSet<>();

    private final SpilledLookupSourceHandle[] spilledLookupSources;

    public Menago(int partitionsCount)
    {
        this.partitionsCount = partitionsCount;

        this.spillingState = new SpillingStateSnapshot(
                false,
                new boolean[partitionsCount],
                0
        );

        this.spilledLookupSources = new SpilledLookupSourceHandle[partitionsCount];
    }

    public synchronized void markSpilled(int partition, SpilledLookupSourceHandle spilledLookupSourceHandle)
    {
        spilledLookupSources[partition] = requireNonNull(spilledLookupSourceHandle, "spilledLookupSourceHandle is null");
        spillingState = spillingState.markSpilled(partition);
    }

    public SpillingStateSnapshot getSpillingState()
    {
        return spillingState;
    }

    public boolean isValid(SpillingStateSnapshot savedSpillingState)
    {
        requireNonNull(savedSpillingState, "savedSpillingState is null");

        return savedSpillingState.stamp == getSpillingState().stamp;
    }

    public synchronized boolean startJoinProbe(SpillingStateSnapshot savedSpillingState, LookupJoiner joinProbe)
    {
        if (isValid(savedSpillingState)) {
            activeLookupJoiners.add(requireNonNull(joinProbe, "joinProbe is null"));
            return true;
        }
        return false;
    }

    public synchronized void finishJoinProbe(LookupJoiner joinProbe)
    {
        activeLookupJoiners.remove(requireNonNull(joinProbe, "joinProbe is null"));
    }

    public void drainCurrentJoinProbes()
    {
        ImmutableList<LookupJoiner> lookupJoiners;
        synchronized (this) {
            lookupJoiners = ImmutableList.copyOf(activeLookupJoiners);
        }
        lookupJoiners.forEach(LookupJoiner::notifySpillingStateChanged);
    }

    @Immutable
    public static final class SpillingStateSnapshot
    {
        private final boolean hasSpilled;
        private final boolean[] spilledPartitions;
        private final long stamp;

        public SpillingStateSnapshot(boolean hasSpilled, boolean[] spilledPartitions, long stamp)
        {
            this.hasSpilled = hasSpilled;
            this.spilledPartitions = spilledPartitions;
            this.stamp = stamp;
        }

        public SpillingStateSnapshot markSpilled(int partition)
        {
            checkArgument(0 <= partition && partition < partitionsCount());
            boolean[] spilledPartitions = Arrays.copyOf(this.spilledPartitions, partitionsCount());
            spilledPartitions[partition] = true;

            return new SpillingStateSnapshot(
                    true,
                    spilledPartitions,
                    stamp + 1);
        }

        public boolean hasSpilled()
        {
            return hasSpilled;
        }

        public boolean isSpilled(int partition)
        {
            return spilledPartitions[partition];
        }

        private int partitionsCount()
        {
            return spilledPartitions.length;
        }

        public boolean isNewerThan(SpillingStateSnapshot other)
        {
            return requireNonNull(other, "other is null").stamp < this.stamp;
        }
    }
}
