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
import com.facebook.presto.spi.ConnectorPageSink;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.connector.ConnectorPageSinkProvider;
import com.facebook.presto.spi.connector.ConnectorPageSourceProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;

import javax.inject.Inject;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public final class CachePageSourceProvider
        implements ConnectorPageSourceProvider
{
    private final ConnectorPageSourceProvider cachePageSourceProvider;
    private final ConnectorPageSourceProvider sourcePageSourceProvider;
    private final ConnectorPageSinkProvider cachePageSinkProvider;

    @Inject
    public CachePageSourceProvider(
            @Source ConnectorPageSourceProvider sourcePageSourceProvider,
            @Cache ConnectorPageSourceProvider cachePageSourceProvider,
            @Cache ConnectorPageSinkProvider cachePageSinkProvider)
    {
        this.cachePageSourceProvider = requireNonNull(cachePageSourceProvider, "cachePageSourceProvider is null");
        this.sourcePageSourceProvider = requireNonNull(sourcePageSourceProvider, "sourcePageSourceProvider is null");
        this.cachePageSinkProvider = requireNonNull(cachePageSinkProvider, "cachePageSinkProvider is null");
    }

    @Override
    public ConnectorPageSource createPageSource(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorSplit split,
            List<ColumnHandle> columns)
    {
        CacheSplit cacheSplit = (CacheSplit) split;
        CacheTransactionHandle cacheTransaction = (CacheTransactionHandle) transactionHandle;

        if (cacheSplit.getCacheSplit().isPresent()) {
            return cachePageSourceProvider.createPageSource(
                    cacheTransaction.getCacheTransaction(),
                    session,
                    cacheSplit.getCacheSplit().get(),
                    columns);
        }

        checkState(cacheSplit.getSourceSplit().isPresent() &&
                        cacheSplit.getCacheOutputTableHandle().isPresent() &&
                        cacheSplit.getSourceColumnHandles().isPresent(),
                "CacheSplit can not have empty both cache and source splits");

        ConnectorPageSink cachePageSink = cachePageSinkProvider.createPageSink(
                cacheTransaction.getCacheTransaction(),
                session,
                cacheSplit.getCacheOutputTableHandle().get());

        // FIXME: if we scan and cache, we need to scan all columns...
        ConnectorPageSource sourcePageSource = sourcePageSourceProvider.createPageSource(
                cacheTransaction.getSourceTransaction(),
                session,
                cacheSplit.getSourceSplit().get(),
                cacheSplit.getSourceColumnHandles().get());

        return new ScanAndCachePageSource(
                cachePageSink,
                sourcePageSource,
                selectColumns(columns, cacheSplit.getSourceColumnHandles().get()));
    }

    private static List<Integer> selectColumns(List<ColumnHandle> desiredColumns, List<ColumnHandle> sourceColumns)
    {
        int index = 0;
        Map<ColumnHandle, Integer> columns = new HashMap<>();
        for (ColumnHandle sourceColumn : sourceColumns) {
           columns.put(sourceColumn, index++);
        }

        return desiredColumns.stream()
                .map(columns::get)
                .collect(toImmutableList());
    }

    private static class ScanAndCachePageSource
            implements ConnectorPageSource
    {
        private final ConnectorPageSink cachePageSink;
        private final ConnectorPageSource sourcePageSource;
        private final List<Integer> columns;

        public ScanAndCachePageSource(
                ConnectorPageSink cachePageSink,
                ConnectorPageSource sourcePageSource,
                List<Integer> columns)
        {
            this.cachePageSink = requireNonNull(cachePageSink, "cachePageSink is null");
            this.sourcePageSource = requireNonNull(sourcePageSource, "sourcePageSource is null");
            this.columns = requireNonNull(columns, "columns is null");
        }

        @Override
        public Page getNextPage()
        {
            Page page = sourcePageSource.getNextPage();
            if (page == null) {
                return null;
            }
            cachePageSink.appendPage(page);
            return getColumns(page, columns);
        }

        @Override
        public long getTotalBytes()
        {
            return sourcePageSource.getTotalBytes();
        }

        @Override
        public long getCompletedBytes()
        {
            return sourcePageSource.getCompletedBytes();
        }

        @Override
        public long getReadTimeNanos()
        {
            return sourcePageSource.getReadTimeNanos();
        }

        @Override
        public boolean isFinished()
        {
            return sourcePageSource.isFinished();
        }

        @Override
        public long getSystemMemoryUsage()
        {
            return sourcePageSource.getSystemMemoryUsage();
        }

        @Override
        public void close()
                throws IOException
        {
            sourcePageSource.close();
            cachePageSink.finish();
        }

        private static Page getColumns(Page page, List<Integer> columnIndexes)
        {
            Block[] blocks = page.getBlocks();
            Block[] outputBlocks = new Block[columnIndexes.size()];

            for (int i = 0; i < columnIndexes.size(); i++) {
                outputBlocks[i] = blocks[columnIndexes.get(i)];
            }

            return new Page(page.getPositionCount(), outputBlocks);
        }
    }
}
