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
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class CacheTableLayoutHandle
        implements ConnectorTableLayoutHandle
{
    private final Optional<ConnectorTableLayoutHandle> cacheTableLayoutHandle;
    private final Optional<ConnectorTableLayoutHandle> sourceTableLayoutHandle;
    private final Optional<ConnectorOutputTableHandle> cacheOutputTableHandle;
    private final Optional<List<ColumnHandle>> sourceColumnHandles;

    public static CacheTableLayoutHandle cached(ConnectorTableLayoutHandle cacheTableLayoutHandle)
    {
        return new CacheTableLayoutHandle(
                Optional.of(cacheTableLayoutHandle),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());
    }

    public static CacheTableLayoutHandle scanAndCache(
            ConnectorTableLayoutHandle sourceTableLayoutHandle,
            ConnectorOutputTableHandle cacheOutputTableHandle,
            List<ColumnHandle> sourceColumnHandles)
    {
        return new CacheTableLayoutHandle(
                Optional.empty(),
                Optional.of(sourceTableLayoutHandle),
                Optional.of(cacheOutputTableHandle),
                Optional.of(sourceColumnHandles));
    }

    @JsonCreator
    public CacheTableLayoutHandle(
            @JsonProperty("cacheTableLayoutHandle") Optional<ConnectorTableLayoutHandle> cacheTableLayoutHandle,
            @JsonProperty("sourceTableLayoutHandle") Optional<ConnectorTableLayoutHandle> sourceTableLayoutHandle,
            @JsonProperty("cacheOutputTableHandle") Optional<ConnectorOutputTableHandle> cacheOutputTableHandle,
            @JsonProperty("sourceColumnHandles") Optional<List<ColumnHandle>> sourceColumnHandles)
    {
        this.cacheTableLayoutHandle = requireNonNull(cacheTableLayoutHandle, "cacheTableLayoutHandle is null");
        this.sourceTableLayoutHandle = requireNonNull(sourceTableLayoutHandle, "sourceTableLayoutHandle is null");
        this.cacheOutputTableHandle = requireNonNull(cacheOutputTableHandle, "cacheOutputTableHandle is null");
        this.sourceColumnHandles = requireNonNull(sourceColumnHandles, "sourceColumnHandles is null");
    }

    @JsonProperty
    public Optional<ConnectorTableLayoutHandle> getCacheTableLayoutHandle()
    {
        return cacheTableLayoutHandle;
    }

    @JsonProperty
    public Optional<ConnectorTableLayoutHandle> getSourceTableLayoutHandle()
    {
        return sourceTableLayoutHandle;
    }

    @JsonProperty
    public Optional<ConnectorOutputTableHandle> getCacheOutputTableHandle()
    {
        return cacheOutputTableHandle;
    }

    @JsonProperty
    public Optional<List<ColumnHandle>> getSourceColumnHandles()
    {
        return sourceColumnHandles;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(
                getCacheTableLayoutHandle(),
                getSourceTableLayoutHandle(),
                getCacheOutputTableHandle());
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        CacheTableLayoutHandle other = (CacheTableLayoutHandle) obj;
        return Objects.equals(this.getCacheTableLayoutHandle(), other.getCacheTableLayoutHandle()) &&
                Objects.equals(this.getSourceTableLayoutHandle(), other.getSourceTableLayoutHandle()) &&
                Objects.equals(this.getCacheOutputTableHandle(), other.getCacheOutputTableHandle());
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("cacheTableLayoutHandle", prettyOptional(cacheTableLayoutHandle))
                .add("sourceTableLayoutHandle", prettyOptional(sourceTableLayoutHandle))
                .add("cacheOutputTableHandle", prettyOptional(cacheOutputTableHandle))
                .toString();
    }

    private static <T> String prettyOptional(Optional<T> optional)
    {
        return optional.map(Object::toString).orElse("?");
    }
}
