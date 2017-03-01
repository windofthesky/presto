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
import com.facebook.presto.spi.ConnectorNewTableLayout;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayout;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.ConnectorTableLayoutResult;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.Node;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorOutputMetadata;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

@ThreadSafe
public class CacheMetadata
        implements ConnectorMetadata
{
    public static final String SCHEMA_NAME = "default";

    private final NodeManager nodeManager;
    private final String connectorId;
    private final AtomicLong nextTableId = new AtomicLong();
    private final Map<String, Long> tableIds = new ConcurrentHashMap<>();
    private final Map<Long, CacheTableHandle> tables = new ConcurrentHashMap<>();

    @Inject
    public CacheMetadata(NodeManager nodeManager, CacheConnectorId connectorId)
    {
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
    }

    @Override
    public synchronized List<String> listSchemaNames(ConnectorSession session)
    {
        return ImmutableList.of(SCHEMA_NAME);
    }

    @Override
    public synchronized ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        Long tableId = tableIds.get(tableName.getTableName());
        if (tableId == null) {
            return null;
        }
        return tables.get(tableId);
    }

    @Override
    public synchronized ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        CacheTableHandle cacheTableHandle = (CacheTableHandle) tableHandle;
        return cacheTableHandle.toTableMetadata();
    }

    @Override
    public synchronized List<SchemaTableName> listTables(ConnectorSession session, String schemaNameOrNull)
    {
        if (schemaNameOrNull != null && !schemaNameOrNull.equals(SCHEMA_NAME)) {
            return ImmutableList.of();
        }
        return tables.values().stream()
                .map(CacheTableHandle::toSchemaTableName)
                .collect(toList());
    }

    @Override
    public synchronized Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        CacheTableHandle cacheTableHandle = (CacheTableHandle) tableHandle;
        return cacheTableHandle.getColumnHandles().stream()
                .collect(toMap(CacheColumnHandle::getName, Function.identity()));
    }

    @Override
    public synchronized ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        CacheColumnHandle cacheColumnHandle = (CacheColumnHandle) columnHandle;
        return cacheColumnHandle.toColumnMetadata();
    }

    @Override
    public synchronized Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        return tables.values().stream()
                .filter(table -> prefix.matches(table.toSchemaTableName()))
                .collect(toMap(CacheTableHandle::toSchemaTableName, handle -> handle.toTableMetadata().getColumns()));
    }

    @Override
    public synchronized void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        CacheTableHandle handle = (CacheTableHandle) tableHandle;
        Long tableId = tableIds.remove(handle.getTableName());
        if (tableId != null) {
            tables.remove(tableId);
        }
    }

    @Override
    public synchronized void renameTable(ConnectorSession session, ConnectorTableHandle tableHandle, SchemaTableName newTableName)
    {
        CacheTableHandle oldTableHandle = (CacheTableHandle) tableHandle;
        CacheTableHandle newTableHandle = new CacheTableHandle(
                oldTableHandle.getConnectorId(),
                oldTableHandle.getSchemaName(),
                newTableName.getTableName(),
                oldTableHandle.getTableId(),
                oldTableHandle.getColumnHandles(),
                oldTableHandle.getHosts());
        tableIds.remove(oldTableHandle.getTableName());
        tableIds.put(newTableName.getTableName(), oldTableHandle.getTableId());
        tables.remove(oldTableHandle.getTableId());
        tables.put(oldTableHandle.getTableId(), newTableHandle);
    }

    @Override
    public synchronized void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        ConnectorOutputTableHandle outputTableHandle = beginCreateTable(session, tableMetadata, Optional.empty());
        finishCreateTable(session, outputTableHandle, ImmutableList.of());
    }

    @Override
    public synchronized CacheOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, Optional<ConnectorNewTableLayout> layout)
    {
        long nextId = nextTableId.getAndIncrement();
        Set<Node> nodes = nodeManager.getRequiredWorkerNodes();
        checkState(!nodes.isEmpty(), "No Cache nodes available");

        tableIds.put(tableMetadata.getTable().getTableName(), nextId);
        CacheTableHandle table = new CacheTableHandle(
                connectorId,
                nextId,
                tableMetadata,
                nodes.stream().map(Node::getHostAndPort).collect(Collectors.toList()));
        tables.put(table.getTableId(), table);

        return new CacheOutputTableHandle(table, ImmutableSet.copyOf(tableIds.values()));
    }

    @Override
    public synchronized Optional<ConnectorOutputMetadata> finishCreateTable(ConnectorSession session, ConnectorOutputTableHandle tableHandle, Collection<Slice> fragments)
    {
        return Optional.empty();
    }

    @Override
    public synchronized CacheInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        CacheTableHandle cacheTableHandle = (CacheTableHandle) tableHandle;
        return new CacheInsertTableHandle(cacheTableHandle, ImmutableSet.copyOf(tableIds.values()));
    }

    @Override
    public synchronized Optional<ConnectorOutputMetadata> finishInsert(ConnectorSession session, ConnectorInsertTableHandle insertHandle, Collection<Slice> fragments)
    {
        return Optional.empty();
    }

    @Override
    public synchronized List<ConnectorTableLayoutResult> getTableLayouts(
            ConnectorSession session,
            ConnectorTableHandle handle,
            Constraint<ColumnHandle> constraint,
            Optional<Set<ColumnHandle>> desiredColumns)
    {
        requireNonNull(handle, "handle is null");
        checkArgument(handle instanceof CacheTableHandle);

        CacheTableLayoutHandle layoutHandle = new CacheTableLayoutHandle((CacheTableHandle) handle);
        return ImmutableList.of(new ConnectorTableLayoutResult(getTableLayout(session, layoutHandle), constraint.getSummary()));
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
}
