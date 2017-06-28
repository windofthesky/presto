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
package com.facebook.presto.hive;

import com.facebook.presto.hive.util.AsyncQueue;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.annotations.VisibleForTesting;

import java.io.FileNotFoundException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.presto.hive.HiveErrorCode.HIVE_FILE_NOT_FOUND;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_UNKNOWN_ERROR;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.concurrent.MoreFutures.failedFuture;
import static java.util.stream.Collectors.toMap;

class HiveSplitSource
        implements ConnectorSplitSource
{
    private final AsyncQueue<ConnectorSplit> queue;
    private final AtomicReference<Throwable> throwable = new AtomicReference<>();
    private final HiveSplitLoader splitLoader;
    private final Future<List<ConnectorSplitManager.DynamicFilterDescription>> filters;
    private final TypeManager typeManager;
    private volatile boolean closed;

    HiveSplitSource(int maxOutstandingSplits, HiveSplitLoader splitLoader, Executor executor, Future<List<ConnectorSplitManager.DynamicFilterDescription>> dynamicFilters, TypeManager typeManager)
    {
        this.queue = new AsyncQueue<>(maxOutstandingSplits, executor);
        this.splitLoader = splitLoader;
        this.filters = dynamicFilters;
        this.typeManager = typeManager;
    }

    @VisibleForTesting
    int getOutstandingSplitCount()
    {
        return queue.size();
    }

    CompletableFuture<?> addToQueue(Iterator<? extends ConnectorSplit> splits)
    {
        CompletableFuture<?> lastResult = CompletableFuture.completedFuture(null);
        while (splits.hasNext()) {
            ConnectorSplit split = splits.next();
            lastResult = addToQueue(split);
        }
        return lastResult;
    }

    CompletableFuture<?> addToQueue(ConnectorSplit split)
    {
        if (throwable.get() == null) {
            return queue.offer(split);
        }
        return CompletableFuture.completedFuture(null);
    }

    void finished()
    {
        if (throwable.get() == null) {
            queue.finish();
            splitLoader.stop();
        }
    }

    void fail(Throwable e)
    {
        // only record the first error message
        if (throwable.compareAndSet(null, e)) {
            // add finish the queue
            queue.finish();

            // no need to process any more jobs
            splitLoader.stop();
        }
    }

    @Override
    public CompletableFuture<List<ConnectorSplit>> getNextBatch(int maxSize)
    {
        checkState(!closed, "Provider is already closed");

        CompletableFuture<List<ConnectorSplit>> future = queue.getBatchAsync(maxSize);

        // Before returning, check if there is a registered failure.
        // If so, we want to throw the error, instead of returning because the scheduler can block
        // while scheduling splits and wait for work to finish before continuing.  In this case,
        // we want to end the query as soon as possible and abort the work
        if (throwable.get() != null) {
            return failedFuture(throwable.get());
        }

        return future.thenApply(this::dynamicallyFilterSplits);
    }

    private List<ConnectorSplit> dynamicallyFilterSplits(List<ConnectorSplit> splits)
    {
        List<ConnectorSplitManager.DynamicFilterDescription> dynamicFilterDescriptions = null;
        try {
            dynamicFilterDescriptions = filters.get(5, TimeUnit.SECONDS);
        }
        catch (Exception e) {
            // problems collecting filters, dynamic partition pruning will be skipped
            // don't have to do anything here to handle the exception
        }
        if (dynamicFilterDescriptions == null || dynamicFilterDescriptions.size() == 0) {
            // dynamic filters not available (e.g. due to timeout), don't filter anything
            return splits;
        }

        Iterator<ConnectorSplit> iter = splits.iterator();
        while (iter.hasNext()) {
            HiveSplit hiveSplit = (HiveSplit) iter.next();
            for (ConnectorSplitManager.DynamicFilterDescription dynamicFilter : dynamicFilterDescriptions) {
                Optional<Map<ColumnHandle, Domain>> domains = dynamicFilter.getTupleDomain().getDomains();
                if (!domains.isPresent()) {
                    continue;
                }

                for (HivePartitionKey partitionKey : hiveSplit.getPartitionKeys()) {
                    // TODO: optimize this to avoid filtering multiple times for multiple partition keys
                    Map<HiveColumnHandle, Domain> relevantDomains = domains.get().entrySet().stream().filter(entry -> ((HiveColumnHandle) entry.getKey()).getName().equals(partitionKey.getName())).collect(toMap(e -> (HiveColumnHandle) e.getKey(), e -> e.getValue()));
                    boolean matched = false;
                    for (Domain predicateDomain : relevantDomains.values()) {
                        Type type = partitionKey.getHiveType().getType(typeManager);
                        Object objectToWrite = getObjectOfString(partitionKey.getHiveType(), partitionKey.getValue());
                        if (predicateDomain.overlaps(Domain.singleValue(type, objectToWrite))) {
                            matched = true;
                            break;
                        }
                    }
                    if (!matched) {
                        // no relevant predicate matched, so remove that split
                        iter.remove();
                    }
                }
            }
        }
        return splits;
    }

    private static Object getObjectOfString(HiveType hiveType, String value)
    {
        if (hiveType.getHiveTypeName().equals("bigint")) {
            return Long.valueOf(value).longValue();
        }
        throw new IllegalStateException(":(");
    }

    @Override
    public boolean isFinished()
    {
        // the finished marker must be checked before checking the throwable
        // to avoid a race with the fail method
        boolean isFinished = queue.isFinished();
        if (throwable.get() != null) {
            throw propagatePrestoException(throwable.get());
        }
        return isFinished;
    }

    @Override
    public void close()
    {
        queue.finish();
        splitLoader.stop();

        closed = true;
    }

    private static RuntimeException propagatePrestoException(Throwable throwable)
    {
        if (throwable instanceof PrestoException) {
            throw (PrestoException) throwable;
        }
        if (throwable instanceof FileNotFoundException) {
            throw new PrestoException(HIVE_FILE_NOT_FOUND, throwable);
        }
        throw new PrestoException(HIVE_UNKNOWN_ERROR, throwable);
    }
}
