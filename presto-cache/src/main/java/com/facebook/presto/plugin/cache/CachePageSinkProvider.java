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

import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorPageSink;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.connector.ConnectorPageSinkProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import javax.inject.Inject;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;

public class CachePageSinkProvider
        implements ConnectorPageSinkProvider
{
    private final CachePagesStore pagesStore;

    @Inject
    public CachePageSinkProvider(CachePagesStore pagesStore)
    {
        this.pagesStore = requireNonNull(pagesStore, "pagesStore is null");
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorOutputTableHandle outputTableHandle)
    {
        CacheOutputTableHandle cacheOutputTableHandle = (CacheOutputTableHandle) outputTableHandle;
        CacheTableHandle tableHandle = cacheOutputTableHandle.getTable();
        long tableId = tableHandle.getTableId();
        checkState(cacheOutputTableHandle.getActiveTableIds().contains(tableId));

        pagesStore.cleanUp(cacheOutputTableHandle.getActiveTableIds());
        pagesStore.initialize(tableId);
        return new CachePageSink(pagesStore, tableId);
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorInsertTableHandle insertTableHandle)
    {
        CacheInsertTableHandle cacheInsertTableHandle = (CacheInsertTableHandle) insertTableHandle;
        CacheTableHandle tableHandle = cacheInsertTableHandle.getTable();
        long tableId = tableHandle.getTableId();
        checkState(cacheInsertTableHandle.getActiveTableIds().contains(tableId));

        pagesStore.cleanUp(cacheInsertTableHandle.getActiveTableIds());
        return new CachePageSink(pagesStore, tableId);
    }

    private static class CachePageSink
            implements ConnectorPageSink
    {
        private final CachePagesStore pagesStore;
        private final long tableId;

        public CachePageSink(CachePagesStore pagesStore, long tableId)
        {
            this.pagesStore = requireNonNull(pagesStore, "pagesStore is null");
            this.tableId = tableId;
        }

        @Override
        public CompletableFuture<?> appendPage(Page page)
        {
            pagesStore.add(tableId, page);
            return NOT_BLOCKED;
        }

        @Override
        public CompletableFuture<Collection<Slice>> finish()
        {
            return completedFuture(ImmutableList.of());
        }

        @Override
        public void abort()
        {
        }
    }
}
