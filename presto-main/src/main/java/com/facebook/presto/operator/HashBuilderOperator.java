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

import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spiller.PartitioningSpillerFactory;
import com.facebook.presto.spiller.SingleStreamSpiller;
import com.facebook.presto.spiller.SingleStreamSpillerFactory;
import com.facebook.presto.sql.gen.JoinFilterFunctionCompiler.JoinFilterFunctionFactory;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.concurrent.MoreFutures;
import io.airlift.units.DataSize;

import javax.annotation.concurrent.ThreadSafe;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class HashBuilderOperator
        implements Operator
{
    public static class HashBuilderOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final PartitionedLookupSourceFactory lookupSourceFactory;
        private final List<Integer> outputChannels;
        private final List<Integer> hashChannels;
        private final Optional<Integer> preComputedHashChannel;
        private final Optional<JoinFilterFunctionFactory> filterFunctionFactory;
        private final PagesIndex.Factory pagesIndexFactory;

        private final int expectedPositions;
        private final boolean spillEnabled;
        private final DataSize memoryLimitBeforeSpill;
        private final SingleStreamSpillerFactory singleStreamSpillerFactory;

        private int partitionIndex;
        private boolean closed;

        public HashBuilderOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                List<Type> types,
                List<Integer> outputChannels,
                Map<Symbol, Integer> layout,
                List<Integer> hashChannels,
                Optional<Integer> preComputedHashChannel,
                boolean outer,
                Optional<JoinFilterFunctionFactory> filterFunctionFactory,
                int expectedPositions,
                int partitionCount,
                PagesIndex.Factory pagesIndexFactory)
        {
            this(operatorId,
                    planNodeId,
                    types,
                    outputChannels,
                    layout,
                    hashChannels,
                    preComputedHashChannel,
                    outer,
                    filterFunctionFactory,
                    expectedPositions,
                    partitionCount,
                    pagesIndexFactory,
                    false,
                    new DataSize(0, MEGABYTE),
                    (types1, localSpillContextSupplier, memoryContext) -> {
                        throw new UnsupportedOperationException();
                    },
                    (types12, partitionGenerator, partitionsCount, ignorePartitions, spillContextSupplier, memoryContext) -> {
                        throw new UnsupportedOperationException();
                    }
            );
        }

        public HashBuilderOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                List<Type> types,
                List<Integer> outputChannels,
                Map<Symbol, Integer> layout,
                List<Integer> hashChannels,
                Optional<Integer> preComputedHashChannel,
                boolean outer,
                Optional<JoinFilterFunctionFactory> filterFunctionFactory,
                int expectedPositions,
                int partitionCount,
                PagesIndex.Factory pagesIndexFactory,
                boolean spillEnabled,
                DataSize memoryLimitBeforeSpill,
                SingleStreamSpillerFactory singleStreamSpillerFactory,
                PartitioningSpillerFactory partitioningSpillerFactory)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");

            checkArgument(Integer.bitCount(partitionCount) == 1, "partitionCount must be a power of 2");
            lookupSourceFactory = new PartitionedLookupSourceFactory(
                    types,
                    outputChannels.stream()
                            .map(types::get)
                            .collect(toImmutableList()),
                    hashChannels,
                    outputChannels,
                    preComputedHashChannel,
                    filterFunctionFactory,
                    partitionCount,
                    requireNonNull(layout, "layout is null"),
                    outer,
                    partitioningSpillerFactory,
                    pagesIndexFactory);

            this.outputChannels = ImmutableList.copyOf(requireNonNull(outputChannels, "outputChannels is null"));
            this.hashChannels = ImmutableList.copyOf(requireNonNull(hashChannels, "hashChannels is null"));
            this.preComputedHashChannel = requireNonNull(preComputedHashChannel, "preComputedHashChannel is null");
            this.filterFunctionFactory = requireNonNull(filterFunctionFactory, "filterFunctionFactory is null");
            this.pagesIndexFactory = requireNonNull(pagesIndexFactory, "pagesIndexFactory is null");
            this.spillEnabled = spillEnabled;
            this.memoryLimitBeforeSpill = requireNonNull(memoryLimitBeforeSpill, "memoryLimitBeforeSpill is null");
            this.singleStreamSpillerFactory = requireNonNull(singleStreamSpillerFactory, "singleStreamSpillerFactory is null");

            this.expectedPositions = expectedPositions;
        }

        public LookupSourceFactory getLookupSourceFactory()
        {
            return lookupSourceFactory;
        }

        @Override
        public List<Type> getTypes()
        {
            return lookupSourceFactory.getTypes();
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, HashBuilderOperator.class.getSimpleName());
            HashBuilderOperator operator = new HashBuilderOperator(
                    operatorContext,
                    lookupSourceFactory,
                    partitionIndex,
                    outputChannels,
                    hashChannels,
                    preComputedHashChannel,
                    filterFunctionFactory,
                    expectedPositions,
                    pagesIndexFactory,
                    spillEnabled,
                    memoryLimitBeforeSpill,
                    singleStreamSpillerFactory);

            partitionIndex++;
            return operator;
        }

        @Override
        public void close()
        {
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            throw new UnsupportedOperationException("Parallel hash build can not be duplicated");
        }
    }

    private final OperatorContext operatorContext;
    private final PartitionedLookupSourceFactory lookupSourceFactory;
    private final int partitionIndex;

    private final List<Integer> outputChannels;
    private final List<Integer> hashChannels;
    private final Optional<Integer> preComputedHashChannel;
    private final Optional<JoinFilterFunctionFactory> filterFunctionFactory;

    private final PagesIndex index;

    private final boolean spillEnabled;
    //    private final DataSize memoryLimitBeforeSpill;
    private final SingleStreamSpillerFactory singleStreamSpillerFactory;
    private final Menago menago;
    private Optional<SingleStreamSpiller> spiller = Optional.empty();
    private ListenableFuture<?> spillInProgress = NOT_BLOCKED;

    private SettableFuture<?> unspillingRequested;
    private boolean unspillingInititated;
    private ListenableFuture<List<Page>> unspillFuture = immediateFuture(null);
    private SettableFuture<LookupSource> unspillingDone;

    private final HashCollisionsCounter hashCollisionsCounter;

    private final SpilledLookupSourceHandle spilledLookupSourceHandle = new SpilledLookupSourceHandle();

    private enum State
    {
        /**
         * Operator accepts input
         */
        CONSUMING_INPUT,

        /**
         * Memory revoking occurred during {@link #CONSUMING_INPUT}. Operator accepts input and spills it
         */
        SPILLING_INPUT,

        /**
         * LookupSource has been built and passed on without any spill occurring
         */
        LOOKUP_SOURCE_BUILT,

        /**
         * Input has been finished and spilled
         */
        LOOKUP_SOURCE_SPILLED,

        /**
         * Spilled input is being unspilled
         */
        LOOKUP_SOURCE_UNSPILLING,

        /**
         * Spilled input has been unspilled, LookupSource built from it
         */
        // TODO consider merging this state with LOOKUP_SOURCE_BUILT
        LOOKUP_SOURCE_UNSPILLED_AND_BUILT,

        /**
         * No longer needed
         */
        DISPOSED
    }

    private State state = State.CONSUMING_INPUT; // TODO state transitions

    public HashBuilderOperator(
            OperatorContext operatorContext,
            PartitionedLookupSourceFactory lookupSourceFactory,
            int partitionIndex,
            List<Integer> outputChannels,
            List<Integer> hashChannels,
            Optional<Integer> preComputedHashChannel,
            Optional<JoinFilterFunctionFactory> filterFunctionFactory,
            int expectedPositions,
            PagesIndex.Factory pagesIndexFactory,
            boolean spillEnabled,
            SingleStreamSpillerFactory singleStreamSpillerFactory)
    {
        requireNonNull(pagesIndexFactory, "pagesIndexFactory is null");

        this.operatorContext = operatorContext;
        this.partitionIndex = partitionIndex;
        this.filterFunctionFactory = filterFunctionFactory;

        this.index = pagesIndexFactory.newPagesIndex(lookupSourceFactory.getTypes(), expectedPositions);
        this.lookupSourceFactory = lookupSourceFactory;

        this.outputChannels = outputChannels;
        this.hashChannels = hashChannels;
        this.preComputedHashChannel = preComputedHashChannel;

        this.hashCollisionsCounter = new HashCollisionsCounter(operatorContext);
        operatorContext.setInfoSupplier(hashCollisionsCounter);

        this.spillEnabled = spillEnabled;
        this.singleStreamSpillerFactory = singleStreamSpillerFactory;

        unspillingRequested = SettableFuture.create();
        unspillingRequested.set(null);

        unspillingDone = SettableFuture.create();
        unspillingDone.set(null);
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public List<Type> getTypes()
    {
        return lookupSourceFactory.getTypes();
    }

    @Override
    public ListenableFuture<?> startMemoryRevoke()
    {
        if (spillEnabled && (state == State.CONSUMING_INPUT || state == State.LOOKUP_SOURCE_BUILT)) {
            menago.markSpilled(partitionIndex, spilledLookupSourceHandle);
            menago.drainCurrentJoinProbes();

            /*
             * Remove partition from PartitionedLookupSource or somehow otherwise make it consume ~0 memory.
             *
             * Alas, PartitionedLookupSourceFactory can returned plan (unpartitioned) LookupSource when there is
             * only one HashBuilderOperator. In that case, spill still is useful (think: concurrent queries), but
             * it's harder to "unplug" partition. Should PartitionedLookupSource be used in that case too?
             */
            // TODO how to unplug from already existing PartitionedLookupSource-s

            lookupSourceFactory.setPartitionSpilledLookupSourceSupplier(partitionIndex, spilledLookupSourceHandle);

            ListenableFuture<?> spillDone = spill();
            return spillDone;
        }

        // Nothing to spill
        return immediateFuture(null);
    }

    @Override
    public void finishMemoryRevoke()
    {
        index.clear();
        operatorContext.setMemoryReservation(index.getEstimatedSize().toBytes());
        operatorContext.setRevocableMemoryReservation(0L);

        switch (state) {
            case CONSUMING_INPUT:
                state = State.SPILLING_INPUT;
                break;
            case LOOKUP_SOURCE_BUILT:
                state = State.LOOKUP_SOURCE_SPILLED;
                break;
            default:
                throw new IllegalStateException("Bad state: " + state);
        }
    }

    private ListenableFuture<?> spill()
    {
        checkState(!spiller.isPresent());
        spiller = Optional.of(singleStreamSpillerFactory.create(index.getTypes(),
                operatorContext.getSpillContext().newLocalSpillContext(),
                operatorContext.getSystemMemoryContext().newLocalMemoryContext()));
        return spiller.get().spill(index.getPages());
    }

    @Override
    public void finish()
    {
        switch (state) {
            case CONSUMING_INPUT: {
                LookupSourceSupplier partition = index.createLookupSourceSupplier(operatorContext.getSession(), hashChannels, preComputedHashChannel, filterFunctionFactory, Optional.of(outputChannels));
                lookupSourceFactory.setPartitionLookupSourceSupplier(partitionIndex, partition);

                operatorContext.setMemoryReservation(partition.get().getInMemorySizeInBytes());
                hashCollisionsCounter.recordHashCollision(partition.getHashCollisions(), partition.getExpectedHashCollisions());

                state = State.LOOKUP_SOURCE_BUILT;
                return;
            }

            case SPILLING_INPUT: {
                verify(spiller.isPresent());

                if (!spillInProgress.isDone()) {
                    // Not ready to handle finish() yet
                    return;
                }

                index.clear(); // already fully spilled
                unspillingRequested = SettableFuture.create();
                unspillingDone = SettableFuture.create();
                unspillFuture = SettableFuture.create();
                lookupSourceFactory.setPartitionSpilledLookupSourceSupplier(partitionIndex, spilledLookupSourceHandle);

                operatorContext.setMemoryReservation(index.getEstimatedSize().toBytes());
                hashCollisionsCounter.recordHashCollision(0, 0);

                state = State.LOOKUP_SOURCE_SPILLED;
                return;
            }

            case LOOKUP_SOURCE_BUILT:
                return;

            case LOOKUP_SOURCE_SPILLED: {
                if (!unspillingRequested.isDone()) {
                    return;
                }
                else {
                    state = State.LOOKUP_SOURCE_UNSPILLING;
                }
            }

            // fall-through

            case LOOKUP_SOURCE_UNSPILLING: {
                verify(!unspillingDone.isDone());

                if (!unspillingInititated) {
                    unspillingInititated = true;
                    checkState(spiller.isPresent());
                    unspillFuture = MoreFutures.toListenableFuture(spiller.get().getAllSpilledPages());
                }

                // FIXME those names are hardly distinguishable
                if (unspillFuture.isDone()) {
                    List<Page> pages = MoreFutures.getFutureValue(unspillFuture);

                    // TODO can I reuse this.index ??????
                    for (Page page : pages) {
                        index.addPage(page);
                    }

                    LookupSourceSupplier partition = index.createLookupSourceSupplier(
                            operatorContext.getSession(),
                            hashChannels,
                            preComputedHashChannel,
                            filterFunctionFactory,
                            Optional.of(outputChannels));

                    operatorContext.setMemoryReservation(partition.get().getInMemorySizeInBytes());
                    hashCollisionsCounter.recordHashCollision(partition.getHashCollisions(), partition.getExpectedHashCollisions());

                    unspillingDone.set(partition.get());
                    state = State.LOOKUP_SOURCE_UNSPILLED_AND_BUILT;
                }
                return;
            }

            case LOOKUP_SOURCE_UNSPILLED_AND_BUILT:
                // TODO know when to dispose and do it
                if (Boolean.FALSE) {
                    return;
                }
                state = State.DISPOSED;

                // fall-through

            case DISPOSED:
                return;
        }
    }

    /* @ThreadSafe */
    // TODO this method is removed, replace appropriately
    private ListenableFuture<LookupSource> unspill()
    {
        checkState(!unspillingRequested.isDone(), "unspilling already requested");
        unspillingRequested.set(null);
        return unspillingDone;
    }

    @Override
    public boolean isFinished()
    {
        return state == State.DISPOSED
                && lookupSourceFactory.isDestroyed().isDone();
    }

    @Override
    public boolean needsInput()
    {
        switch (state) {
            case CONSUMING_INPUT:
                return true;

            case SPILLING_INPUT:
                return spillInProgress.isDone();

            default:
                return false;
        }
    }

    @Override
    public ListenableFuture<?> isBlocked()
    {
        // TODO if someone wanna say this method looks terribly, let's inform them proclaiming a truism is not something they could feel proud of

        switch (state) {
            case CONSUMING_INPUT:
                return NOT_BLOCKED;

            case SPILLING_INPUT:
                return spillInProgress;

            case LOOKUP_SOURCE_BUILT:
                break;

            case LOOKUP_SOURCE_SPILLED:
                break;

            case LOOKUP_SOURCE_UNSPILLING:
                break;

            case LOOKUP_SOURCE_UNSPILLED_AND_BUILT:
                break;

            case DISPOSED:
                break;
        }

//
//        if (!spillInProgress.isDone()) {
//            return spillInProgress;
//        }
//
//        if (needsInput()) {
//            return NOT_BLOCKED;
//        }

        if (!unspillingRequested.isDone()) {
            return unspillingRequested;
        }

        if (!unspillFuture.isDone()) {
            if (!unspillingInititated) {
                return NOT_BLOCKED;
            }
            return unspillFuture;
        }

        if (unspillingInititated && !unspillingDone.isDone()) {
            return NOT_BLOCKED;
        }

        // TODO why
        ListenableFuture<?> destroyed = lookupSourceFactory.isDestroyed();
        return destroyed;
    }

    @Override
    public void addInput(Page page)
    {
        requireNonNull(page, "page is null");
        checkState(!isFinished(), "Operator is already finished");

        operatorContext.recordGeneratedOutput(page.getSizeInBytes(), page.getPositionCount());

        if (spiller.isPresent()) {
            spill(page);
            return;
        }

        index.addPage(page);
        if (!operatorContext.trySetMemoryReservation(index.getEstimatedSize().toBytes())) {
            index.compact();
        }

        if (spillEnabled && index.getEstimatedSize().compareTo(memoryLimitBeforeSpill) > 0) {
            spiller = Optional.of(singleStreamSpillerFactory.create(index.getTypes(),
                    operatorContext.getSpillContext().newLocalSpillContext(),
                    operatorContext.getSystemMemoryContext().newLocalMemoryContext()));
            spillInProgress = spiller.get().spill(index.getPages());
        }
        else {
            operatorContext.setMemoryReservation(index.getEstimatedSize().toBytes());
        }
    }

    private void spill(Page page)
    {
        checkState(spillInProgress.isDone(), "previous spill still in progress");
        if (index.getPositionCount() > 0) {
            index.clear();
            operatorContext.setMemoryReservation(index.getEstimatedSize().toBytes());
        }
        spillInProgress = spiller.get().spill(page);
    }

    @Override
    public Page getOutput()
    {
        return null;
    }

    @Override
    public void close()
    {
        if (spiller.isPresent()) {
            spiller.get().close();
            spiller = Optional.empty();
        }
    }
}
