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

import com.facebook.presto.connector.ConnectorId;
import com.facebook.presto.execution.StageId;
import com.facebook.presto.execution.SystemMemoryUsageListener;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.execution.buffer.PagesSerde;
import com.facebook.presto.execution.buffer.PagesSerdeFactory;
import com.facebook.presto.execution.buffer.SerializedPage;
import com.facebook.presto.metadata.Split;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.UpdatablePageSource;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.split.RemoteSplit;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;

import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.NotThreadSafe;
import javax.annotation.concurrent.ThreadSafe;

import java.io.Closeable;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.concat;
import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;

public class ExchangeOperator
        implements SourceOperator, Closeable
{
    public static final ConnectorId REMOTE_CONNECTOR_ID = new ConnectorId("$remote");

    public static class ExchangeOperatorFactory
            implements SourceOperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId sourceId;
        private final ExchangeClientSupplier exchangeClientSupplier;
        private final PagesSerdeFactory serdeFactory;
        private final List<Type> types;
        private ExchangeClient exchangeClient = null;
        private boolean closed;

        public ExchangeOperatorFactory(
                int operatorId,
                PlanNodeId sourceId,
                ExchangeClientSupplier exchangeClientSupplier,
                PagesSerdeFactory serdeFactory,
                List<Type> types)
        {
            this.operatorId = operatorId;
            this.sourceId = sourceId;
            this.exchangeClientSupplier = exchangeClientSupplier;
            this.serdeFactory = serdeFactory;
            this.types = types;
        }

        @Override
        public PlanNodeId getSourceId()
        {
            return sourceId;
        }

        @Override
        public List<Type> getTypes()
        {
            return types;
        }

        @Override
        public SourceOperator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, sourceId, ExchangeOperator.class.getSimpleName());
            if (exchangeClient == null) {
                exchangeClient = exchangeClientSupplier.get(new UpdateSystemMemory(driverContext.getPipelineContext()));
            }

            return new ExchangeOperator(
                    operatorContext,
                    types,
                    sourceId,
                    serdeFactory.createPagesSerde(),
                    exchangeClient);
        }

        @Override
        public void close()
        {
            closed = true;
        }
    }

    @NotThreadSafe
    private static final class UpdateSystemMemory
            implements SystemMemoryUsageListener
    {
        private final PipelineContext pipelineContext;

        public UpdateSystemMemory(PipelineContext pipelineContext)
        {
            this.pipelineContext = requireNonNull(pipelineContext, "pipelineContext is null");
        }

        @Override
        public void updateSystemMemoryUsage(long deltaMemoryInBytes)
        {
            if (deltaMemoryInBytes > 0) {
                pipelineContext.reserveSystemMemory(deltaMemoryInBytes);
            }
            else {
                pipelineContext.freeSystemMemory(-deltaMemoryInBytes);
            }
        }
    }

    private final OperatorContext operatorContext;
    private final PlanNodeId sourceId;
    private final ExchangeClient exchangeClient;
    private final List<Type> types;
    private final PagesSerde serde;
    private final ExchangeStats.Builder statsBuilder;

    private ExchangeStats exchangeStats;

    public ExchangeOperator(
            OperatorContext operatorContext,
            List<Type> types,
            PlanNodeId sourceId,
            PagesSerde serde,
            ExchangeClient exchangeClient)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.sourceId = requireNonNull(sourceId, "sourceId is null");
        this.exchangeClient = requireNonNull(exchangeClient, "exchangeClient is null");
        this.serde = requireNonNull(serde, "serde is null");
        this.types = requireNonNull(types, "types is null");
        this.statsBuilder = new ExchangeStats.Builder();
        this.exchangeStats = new ExchangeStats(ImmutableList.of(), ImmutableList.of()); // no stats until close() is called

        operatorContext.setInfoSupplier(this::getInfo);
    }

    public ExchangeInfo getInfo()
    {
        return new ExchangeInfo(exchangeClient.getStatus(), exchangeStats);
    }

    @Override
    public PlanNodeId getSourceId()
    {
        return sourceId;
    }

    @Override
    public Supplier<Optional<UpdatablePageSource>> addSplit(Split split)
    {
        requireNonNull(split, "split is null");
        checkArgument(split.getConnectorId().equals(REMOTE_CONNECTOR_ID), "split is not a remote split");

        RemoteSplit remoteSplit = (RemoteSplit) split.getConnectorSplit();
        URI location = remoteSplit.getLocation();
        exchangeClient.addLocation(location);

        statsBuilder.addSplit(remoteSplit.getTaskId().getStageId());

        return Optional::empty;
    }

    @Override
    public void noMoreSplits()
    {
        exchangeClient.noMoreLocations();
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public List<Type> getTypes()
    {
        return types;
    }

    @Override
    public void finish()
    {
        close();
    }

    @Override
    public boolean isFinished()
    {
        return exchangeClient.isFinished();
    }

    @Override
    public ListenableFuture<?> isBlocked()
    {
        ListenableFuture<?> blocked = exchangeClient.isBlocked();
        if (blocked.isDone()) {
            return NOT_BLOCKED;
        }
        return blocked;
    }

    @Override
    public boolean needsInput()
    {
        return false;
    }

    @Override
    public void addInput(Page page)
    {
        throw new UnsupportedOperationException(getClass().getName() + " can not take input");
    }

    @Override
    public Page getOutput()
    {
        SerializedPageWithLocation pageWithLocation = exchangeClient.pollPageWithLocation();
        if (pageWithLocation == null || pageWithLocation.getPage() == null) {
            return null;
        }
        SerializedPage page = pageWithLocation.getPage();

        StageId stageId = extractStageIdFromLocation(pageWithLocation.getLocation().toString());
        statsBuilder.addPositions(stageId, page.getPositionCount());

        operatorContext.recordGeneratedInput(page.getSizeInBytes(), page.getPositionCount());
        return serde.deserialize(page);
    }

    private static StageId extractStageIdFromLocation(String location)
    {
        List<String> parts = unmodifiableList(Arrays.asList(location.split("/")));

        checkState(parts.size() >= 8, "Location should be of format: http[s]://<IP>:<PORT>/v1/task/<TASK_ID>/results/{BUFFER_ID}");

        TaskId taskId = TaskId.valueOf(parts.get(parts.size() - 3));
        return taskId.getStageId();
    }

    @Override
    public void close()
    {
        exchangeClient.close();
        exchangeStats = statsBuilder.build();
    }

    @Immutable
    public static class ExchangeStats
        implements Mergeable<ExchangeStats>
    {
        @ThreadSafe
        public static class Builder
        {
            private final Map<StageId, SingleExchangeStats.Builder> stats;

            public Builder()
            {
                stats = new HashMap<>();
            }

            public synchronized void addSplit(StageId stageId)
            {
                SingleExchangeStats.Builder builder = getBuilder(stageId);
                builder.addSplit();
            }

            public synchronized void addPositions(StageId stageId, long positionCount)
            {
                SingleExchangeStats.Builder builder = getBuilder(stageId);
                builder.addPositions(positionCount);
            }

            private SingleExchangeStats.Builder getBuilder(StageId stageId)
            {
                SingleExchangeStats.Builder builder = stats.get(stageId);
                if (builder == null) {
                    builder = SingleExchangeStats.builder();
                    stats.put(stageId, builder);
                }
                return builder;
            }

            public synchronized ExchangeStats build()
            {
                ImmutableList.Builder<StageId> stageIdBuilder = ImmutableList.builder();
                ImmutableList.Builder<SingleExchangeStats> statsBuilder = ImmutableList.builder();
                for (Map.Entry<StageId, SingleExchangeStats.Builder> entry : stats.entrySet()) {
                    stageIdBuilder.add(entry.getKey());
                    statsBuilder.add(entry.getValue().build());
                }
                return new ExchangeStats(stageIdBuilder.build(), statsBuilder.build());
            }
        }

        private final List<StageId> stageIds;
        private final List<SingleExchangeStats> stats;

        @JsonCreator
        public ExchangeStats(
                @JsonProperty("stageIds") List<StageId> stageIds,
                @JsonProperty("stats") List<SingleExchangeStats> stats)
        {
            checkArgument(stageIds.size() == stats.size(), "Number of stage IDs does not match the number of SingleExchangeStats");

            this.stageIds = ImmutableList.copyOf(stageIds);
            this.stats = ImmutableList.copyOf(stats);
        }

        @JsonProperty
        public List<StageId> getStageIds()
        {
            return stageIds;
        }

        @JsonProperty
        public List<SingleExchangeStats> getStats()
        {
            return stats;
        }

        public Map<StageId, List<SingleExchangeStats>> getStatsMap()
        {
            Map<StageId, List<SingleExchangeStats>> builder = new HashMap<>();

            for (int i = 0; i < stageIds.size(); ++i) {
                List<SingleExchangeStats> list = builder.get(stageIds.get(i));
                if (list == null) {
                    list = new ArrayList<>();
                    builder.put(stageIds.get(i), list);
                }
                list.add(stats.get(i));
            }

            return ImmutableMap.copyOf(builder);
        }

        @Override
        public ExchangeStats mergeWith(ExchangeStats other)
        {
            return new ExchangeStats(
                    ImmutableList.copyOf(concat(stageIds, other.stageIds)),
                    ImmutableList.copyOf(concat(stats, other.stats)));
        }
    }

    @Immutable
    public static class SingleExchangeStats
    {
        @ThreadSafe
        public static class Builder
        {
            private AtomicInteger splitsCount;
            private AtomicLong positionCount;

            private Builder()
            {
                splitsCount = new AtomicInteger(0);
                positionCount = new AtomicLong(0);
            }

            public void addSplit()
            {
                splitsCount.incrementAndGet();
            }

            public void addPositions(long positionCount)
            {
                this.positionCount.addAndGet(positionCount);
            }

            // build method should be called when no more splits or positions are expected
            // However, if it's called in the same time as #addSplit or #addPositions, it's still safe
            // but splitsCount and positionCount may not be in sync with each other
            public SingleExchangeStats build()
            {
                return new SingleExchangeStats(splitsCount.get(), positionCount.get());
            }
        }

        private final int splitsCount;
        private final long positionCount;

        public static Builder builder()
        {
            return new Builder();
        }

        @JsonCreator
        public SingleExchangeStats(
                @JsonProperty("splitsCount") int splitsCount,
                @JsonProperty("positionCount") long positionCount)
        {
            this.splitsCount = splitsCount;
            this.positionCount = positionCount;
        }

        @JsonProperty
        public int getSplitsCount()
        {
            return splitsCount;
        }

        @JsonProperty
        public long getPositionCount()
        {
            return positionCount;
        }
    }
}
