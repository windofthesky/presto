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

package com.facebook.presto.plugin.cache;

import com.facebook.presto.plugin.cache.CacheModule.Cache;
import com.facebook.presto.plugin.cache.CacheModule.Source;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;

import javax.inject.Inject;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public final class CacheSplitManager
        implements ConnectorSplitManager
{
    private final ConnectorSplitManager sourceSplitManager;
    private final ConnectorSplitManager cacheSplitManager;

    @Inject
    public CacheSplitManager(
            @Source ConnectorSplitManager sourceSplitManager,
            @Cache ConnectorSplitManager cacheSplitManager)
    {
        this.sourceSplitManager = requireNonNull(sourceSplitManager, "sourceSplitManager is null");
        this.cacheSplitManager = requireNonNull(cacheSplitManager, "cacheSplitManager is null");
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorTableLayoutHandle layoutHandle)
    {
        CacheTableLayoutHandle layout = (CacheTableLayoutHandle) layoutHandle;

        if (layout.getCacheTableLayoutHandle().isPresent()) {
            return cached(cacheSplitManager.getSplits(transactionHandle, session, layout.getCacheTableLayoutHandle().get()));
        }
        checkState(layout.getSourceTableLayoutHandle().isPresent(), "both source and cache layout handles can not be empty");

        return scanAndCache(
                sourceSplitManager.getSplits(transactionHandle, session, layout.getSourceTableLayoutHandle().get()),
                layout.getCacheOutputTableHandle().get(),
                layout.getSourceColumnHandles().get());
    }

    private ConnectorSplitSource cached(ConnectorSplitSource cacheSplitSource)
    {
        return new ConnectorSplitSource() {
            @Override
            public CompletableFuture<List<ConnectorSplit>> getNextBatch(int maxSize)
            {
                return cacheSplitSource.getNextBatch(maxSize).thenApply(
                        cachedSplits -> cachedSplits.stream().map(CacheSplit::cached).collect(toImmutableList()));
            }

            @Override
            public void close()
            {
                cacheSplitSource.close();
            }

            @Override
            public boolean isFinished()
            {
                return cacheSplitSource.isFinished();
            }
        };
    }

    private ConnectorSplitSource scanAndCache(
            ConnectorSplitSource sourceSplitSource,
            ConnectorOutputTableHandle outputTableHandle,
            List<ColumnHandle> sourceColumnHandles)
    {
        return new ConnectorSplitSource() {
            @Override
            public CompletableFuture<List<ConnectorSplit>> getNextBatch(int maxSize)
            {
                return sourceSplitSource.getNextBatch(maxSize).thenApply(
                        cachedSplits -> cachedSplits.stream()
                                .map(split -> CacheSplit.scanAndCache(split, outputTableHandle, sourceColumnHandles))
                                .collect(toImmutableList()));
            }

            @Override
            public void close()
            {
                sourceSplitSource.close();
            }

            @Override
            public boolean isFinished()
            {
                return sourceSplitSource.isFinished();
            }
        };
    }
}
