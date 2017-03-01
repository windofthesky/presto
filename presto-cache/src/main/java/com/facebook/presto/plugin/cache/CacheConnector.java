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

import com.facebook.presto.plugin.cache.CacheMetadata.CacheMetadataFactory;
import com.facebook.presto.plugin.cache.CacheModule.Cache;
import com.facebook.presto.plugin.cache.CacheModule.Source;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorContext;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorPageSourceProvider;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.transaction.IsolationLevel;

import javax.inject.Inject;

import static java.util.Objects.requireNonNull;

public class CacheConnector
        implements Connector
{
    private final CacheMetadataFactory metadataFactory;
    private final CacheSplitManager splitManager;
    private final CachePageSourceProvider pageSourceProvider;
    private final Connector sourceConnector;
    private final Connector cacheConnector;

    @Inject
    public CacheConnector(
            CacheConfig config,
            ConnectorContext connectorContext,
            CacheMetadataFactory metadataFactory,
            @Source Connector sourceConnector,
            @Cache Connector cacheConnector,
            CacheSplitManager splitManager,
            CachePageSourceProvider pageSourceProvider)
    {
        this.splitManager = splitManager;
        this.pageSourceProvider = pageSourceProvider;
        this.metadataFactory = requireNonNull(metadataFactory, "metadataFactory is null");
        this.sourceConnector = requireNonNull(sourceConnector, "sourceConnector is null");
        this.cacheConnector = requireNonNull(cacheConnector, "cacheConnector is null");
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly)
    {
        return new CacheTransactionHandle(
                sourceConnector.beginTransaction(isolationLevel, readOnly),
                cacheConnector.beginTransaction(isolationLevel, readOnly));
    }

    @Override
    public void commit(ConnectorTransactionHandle transactionHandle)
    {
        CacheTransactionHandle cacheTransactionHandle = (CacheTransactionHandle) transactionHandle;
        sourceConnector.commit(cacheTransactionHandle.getSourceTransaction());
        cacheConnector.commit(cacheTransactionHandle.getCacheTransaction());
    }

    @Override
    public void rollback(ConnectorTransactionHandle transactionHandle)
    {
        CacheTransactionHandle cacheTransactionHandle = (CacheTransactionHandle) transactionHandle;
        sourceConnector.rollback(cacheTransactionHandle.getSourceTransaction());
        cacheConnector.rollback(cacheTransactionHandle.getCacheTransaction());
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorTransactionHandle transactionHandle)
    {
        CacheTransactionHandle cacheTransactionHandle = (CacheTransactionHandle) transactionHandle;
        return metadataFactory.create(
                sourceConnector.getMetadata(cacheTransactionHandle.getSourceTransaction()),
                cacheConnector.getMetadata(cacheTransactionHandle.getCacheTransaction()));
    }

    @Override
    public ConnectorSplitManager getSplitManager()
    {
        return splitManager;
    }

    @Override
    public ConnectorPageSourceProvider getPageSourceProvider()
    {
        return pageSourceProvider;
    }
}
