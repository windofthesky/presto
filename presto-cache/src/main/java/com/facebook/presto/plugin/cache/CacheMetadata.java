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

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayout;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.ConnectorTableLayoutResult;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.Node;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.google.common.collect.ImmutableList;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

@ThreadSafe
public class CacheMetadata
        implements ConnectorMetadata
{
    public static final String SCHEMA_NAME = "default";

    private final NodeManager nodeManager;
    private final String connectorId;
    private final ConnectorMetadata sourceMetadata;
    private final ConnectorMetadata cacheMetadata;

    private CacheMetadata(
            NodeManager nodeManager,
            CacheConnectorId connectorId,
            ConnectorMetadata sourceMetadata,
            ConnectorMetadata cacheMetadata)
    {
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.sourceMetadata = requireNonNull(sourceMetadata, "sourceMetadata is null");
        this.cacheMetadata = requireNonNull(cacheMetadata, "cacheMetadata is null");
    }

    @Override
    public synchronized List<String> listSchemaNames(ConnectorSession session)
    {
        return sourceMetadata.listSchemaNames(session);
    }

    @Override
    public synchronized ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName schemaTableName)
    {
        Set<Node> nodes = nodeManager.getRequiredWorkerNodes();
        List<HostAddress> hosts = nodes.stream().map(Node::getHostAndPort).collect(toList());

        ConnectorTableHandle cacheHandle = cacheMetadata.getTableHandle(session, schemaTableName);
        if (cacheHandle != null) {
            return CacheTableHandle.cached(cacheHandle, hosts);
        }

        ConnectorTableHandle sourceHandle = sourceMetadata.getTableHandle(session, schemaTableName);
        if (sourceHandle == null) {
            return null;
        }

        ConnectorTableMetadata sourceTableMetadata = sourceMetadata.getTableMetadata(session, sourceHandle);
        // Some connectors can return different schema name for the same table in getTableMetadata
        // so we must change it back to the original requested one. For example when accessing
        // tpch.tiny.nation, we want to cache it as "tiny.nation", but getTableMetadata would return us
        // "sf0.01.nation" instead.
        ConnectorTableMetadata cachedTableMetadata = new ConnectorTableMetadata(
                schemaTableName,
                sourceTableMetadata.getColumns(),
                sourceTableMetadata.getProperties(),
                sourceTableMetadata.getComment());

        cacheMetadata.createTable(session, cachedTableMetadata);
        ConnectorInsertTableHandle cachingInsertTableHandle = cacheMetadata.beginInsert(
                session,
                requireNonNull(
                        cacheMetadata.getTableHandle(session, schemaTableName),
                        "Caching connector failed to return table handle"));

        return CacheTableHandle.scanAndCache(sourceHandle, cachingInsertTableHandle, hosts);
    }

    @Override
    public synchronized ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return getMetadata(tableHandle).getTableMetadata(
                session,
                getTableHandle(tableHandle));
    }

    @Override
    public synchronized List<SchemaTableName> listTables(ConnectorSession session, String schemaNameOrNull)
    {
        return sourceMetadata.listTables(session, schemaNameOrNull);
    }

    @Override
    public synchronized Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return getMetadata(tableHandle).getColumnHandles(
                session,
                getTableHandle(tableHandle));
    }

    @Override
    public synchronized ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        return getMetadata(tableHandle).getColumnMetadata(
                session,
                getTableHandle(tableHandle),
                columnHandle);
    }

    @Override
    public synchronized Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        return sourceMetadata.listTableColumns(session, prefix);
    }

    @Override
    public synchronized void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        sourceMetadata.dropTable(session, tableHandle);
        cacheMetadata.dropTable(session, tableHandle);
    }

    @Override
    public synchronized void renameTable(ConnectorSession session, ConnectorTableHandle tableHandle, SchemaTableName newTableName)
    {
        sourceMetadata.renameTable(session, tableHandle, newTableName);
        cacheMetadata.renameTable(session, tableHandle, newTableName);
    }

    @Override
    public synchronized List<ConnectorTableLayoutResult> getTableLayouts(
            ConnectorSession session,
            ConnectorTableHandle tableHandle,
            Constraint<ColumnHandle> constraint,
            Optional<Set<ColumnHandle>> desiredColumns)
    {
        requireNonNull(tableHandle, "tableHandle is null");
        CacheTableHandle cacheTableHandle = (CacheTableHandle) tableHandle;
        if (cacheTableHandle.getCacheTableHandle().isPresent()) {
            List<ConnectorTableLayoutResult> cacheTableLayouts = cacheMetadata.getTableLayouts(
                    session,
                    getTableHandle(tableHandle),
                    constraint,
                    desiredColumns);

            return cacheTableLayouts
                    .stream()
                    .map(layout -> new ConnectorTableLayoutResult(
                            new ConnectorTableLayout(
                                    CacheTableLayoutHandle.cached(layout.getTableLayout().getHandle()),
                                    layout.getTableLayout().getColumns(),
                                    layout.getTableLayout().getPredicate(),
                                    layout.getTableLayout().getNodePartitioning(),
                                    layout.getTableLayout().getStreamPartitioningColumns(),
                                    layout.getTableLayout().getDiscretePredicates(),
                                    layout.getTableLayout().getLocalProperties()),
                            layout.getUnenforcedConstraint()))
                    .collect(toImmutableList());
        }

        checkState(cacheTableHandle.getSourceTableHandle().isPresent(), "Both cache and source table handles are empty");

        List<ConnectorTableLayoutResult> sourceTableLayouts = sourceMetadata.getTableLayouts(
                session,
                getTableHandle(tableHandle),
                constraint,
                Optional.empty());

        ConnectorTableMetadata tableMetadata = sourceMetadata.getTableMetadata(session, getTableHandle(tableHandle));
        ConnectorTableMetadata tableMetadataWithoutHiddenColumns = new ConnectorTableMetadata(
                tableMetadata.getTable(),
                tableMetadata.getColumns().stream().filter(column -> !column.isHidden()).collect(toList()),
                tableMetadata.getProperties(),
                tableMetadata.getComment());

        List<ColumnHandle> sourceColumnHandles = ImmutableList.copyOf(
                sourceMetadata.getColumnHandles(session, getTableHandle(tableHandle)).values());

        return sourceTableLayouts
                .stream()
                .map(layout -> new ConnectorTableLayoutResult(
                        new ConnectorTableLayout(
                                CacheTableLayoutHandle.scanAndCache(
                                        layout.getTableLayout().getHandle(),
                                        cacheMetadata.beginCreateTable(session, tableMetadataWithoutHiddenColumns, Optional.empty()),
                                        sourceColumnHandles),
                                layout.getTableLayout().getColumns(),
                                layout.getTableLayout().getPredicate(),
                                layout.getTableLayout().getNodePartitioning(),
                                layout.getTableLayout().getStreamPartitioningColumns(),
                                layout.getTableLayout().getDiscretePredicates(),
                                layout.getTableLayout().getLocalProperties()),
                        layout.getUnenforcedConstraint()))
                .collect(toImmutableList());
    }

    @Override
    public synchronized ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle)
    {
        return new ConnectorTableLayout(
                handle,
                Optional.empty(),
                TupleDomain.all(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableList.of());
    }

    private ConnectorMetadata getMetadata(ConnectorTableHandle tableHandle)
    {
        CacheTableHandle cacheTableHandle = (CacheTableHandle) tableHandle;
        if (cacheTableHandle.getSourceTableHandle().isPresent()) {
            return sourceMetadata;
        }
        checkState(
                cacheTableHandle.getCacheTableHandle().isPresent(),
                "Both source and cache table handles can not be empty");
        return cacheMetadata;
    }

    private static ConnectorTableHandle getTableHandle(ConnectorTableHandle tableHandle)
    {
        CacheTableHandle cacheTableHandle = (CacheTableHandle) tableHandle;
        if (cacheTableHandle.getSourceTableHandle().isPresent()) {
            return cacheTableHandle.getSourceTableHandle().get();
        }
        checkState(
                cacheTableHandle.getCacheTableHandle().isPresent(),
                "Both source and cache table handles can not be empty");
        return cacheTableHandle.getCacheTableHandle().get();
    }

    public static class CacheMetadataFactory
    {
        private final NodeManager nodeManager;
        private final CacheConnectorId connectorId;

        @Inject
        public CacheMetadataFactory(
                NodeManager nodeManager,
                CacheConnectorId connectorId)
        {
            this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
            this.connectorId = requireNonNull(connectorId, "connectorId is null");
        }

        public CacheMetadata create(ConnectorMetadata sourceMetadata, ConnectorMetadata cacheMetadata)
        {
            return new CacheMetadata(nodeManager, connectorId, sourceMetadata, cacheMetadata);
        }
    }
}
