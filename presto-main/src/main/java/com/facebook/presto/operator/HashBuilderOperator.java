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
    private final DataSize memoryLimitBeforeSpill;
    private final SingleStreamSpillerFactory singleStreamSpillerFactory;
    private Optional<SingleStreamSpiller> spiller = Optional.empty();
    private ListenableFuture<?> spillInProgress = NOT_BLOCKED;

    private SettableFuture<?> unspillingRequested;
    private boolean unspillingInititated;
    private ListenableFuture<List<Page>> unspillFuture = immediateFuture(null);
    private SettableFuture<LookupSource> unspillingDone;

    private boolean noMoreInput;
    private final HashCollisionsCounter hashCollisionsCounter;

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
            DataSize memoryLimitBeforeSpill,
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
        this.memoryLimitBeforeSpill = memoryLimitBeforeSpill;
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
    public void finish()
    {
        if (!noMoreInput) {
            if (!spillInProgress.isDone()) {
                // Not ready to handle finish() yet
                return;
            }

            noMoreInput = true;

            if (spiller.isPresent()) {
                index.clear(); // already fully spilled
                unspillingRequested = SettableFuture.create();
                unspillingDone = SettableFuture.create();
                unspillFuture = SettableFuture.create();
                lookupSourceFactory.setPartitionSpilledLookupSourceSupplier(partitionIndex, this::unspill);

                operatorContext.setMemoryReservation(index.getEstimatedSize().toBytes());
                hashCollisionsCounter.recordHashCollision(0, 0);
            }
            else {
                LookupSourceSupplier partition = index.createLookupSourceSupplier(operatorContext.getSession(), hashChannels, preComputedHashChannel, filterFunctionFactory, Optional.of(outputChannels));
                lookupSourceFactory.setPartitionLookupSourceSupplier(partitionIndex, partition);

                operatorContext.setMemoryReservation(partition.get().getInMemorySizeInBytes());
                hashCollisionsCounter.recordHashCollision(partition.getHashCollisions(), partition.getExpectedHashCollisions());
            }
        }

        // TODO proper states?

        if (unspillingRequested.isDone() && !unspillingDone.isDone()) {

            if (!unspillingInititated) {
                unspillingInititated = true;
                checkState(spiller.isPresent());
                unspillFuture = MoreFutures.toListenableFuture(spiller.get().getAllSpilledPages());
            }

            // FIXME those names are hardly distinguishable
            if (unspillFuture.isDone() && !unspillingDone.isDone()) {
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
            }
        }
    }

    /* @ThreadSafe */
    private ListenableFuture<LookupSource> unspill()
    {
        checkState(!unspillingRequested.isDone(), "unspilling already requested");
        unspillingRequested.set(null);
        return unspillingDone;
    }

    @Override
    public boolean isFinished()
    {
        return noMoreInput
                && unspillingDone.isDone()
                && lookupSourceFactory.isDestroyed().isDone();
    }

    @Override
    public boolean needsInput()
    {
        return !noMoreInput;
    }

    @Override
    public ListenableFuture<?> isBlocked()
    {
        // TODO if someone wanna say this method looks terribly, let's inform them proclaiming a truism is not something they could feel proud of

        if (!spillInProgress.isDone()) {
            return spillInProgress;
        }

        if (needsInput()) {
            return NOT_BLOCKED;
        }

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

        ListenableFuture<?> destroyed = lookupSourceFactory.isDestroyed();
        if (destroyed.isDone()) {
            return destroyed;
        }
        else {
            return destroyed;
        }
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
