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
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class CacheSplit
        implements ConnectorSplit
{
    private final Optional<ConnectorSplit> cacheSplit;
    private final Optional<ConnectorSplit> sourceSplit;
    private final Optional<ConnectorOutputTableHandle> cacheOutputTableHandle;
    private final Optional<List<ColumnHandle>> sourceColumnHandles;

    public static CacheSplit cached(ConnectorSplit cacheSplit)
    {
        return new CacheSplit(
                Optional.of(cacheSplit),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());
    }

    public static CacheSplit scanAndCache(
            ConnectorSplit sourceSplit,
            ConnectorOutputTableHandle cacheOutputTableHandle,
            List<ColumnHandle> sourceColumnHandles)
    {
        return new CacheSplit(
                Optional.empty(),
                Optional.of(sourceSplit),
                Optional.of(cacheOutputTableHandle),
                Optional.of(sourceColumnHandles));
    }

    @JsonCreator
    public CacheSplit(
            @JsonProperty("cacheSplit") Optional<ConnectorSplit> cacheSplit,
            @JsonProperty("sourceSplit") Optional<ConnectorSplit> sourceSplit,
            @JsonProperty("cacheOutputTableHandle") Optional<ConnectorOutputTableHandle> cacheOutputTableHandle,
            @JsonProperty("sourceColumnHandles") Optional<List<ColumnHandle>> sourceColumnHandles)
    {
        checkState(cacheSplit.isPresent() ^ (sourceSplit.isPresent() && cacheOutputTableHandle.isPresent()),
                "CacheSplit can not have empty both cache and source splits");
        this.cacheSplit = requireNonNull(cacheSplit, "cacheSplit is null");
        this.sourceSplit = requireNonNull(sourceSplit, "sourceSplit is null");
        this.cacheOutputTableHandle = requireNonNull(cacheOutputTableHandle, "cacheOutputTableHandle is null");
        this.sourceColumnHandles = requireNonNull(sourceColumnHandles, "sourceColumnHandles is null");
    }

    @JsonProperty
    public Optional<ConnectorSplit> getCacheSplit()
    {
        return cacheSplit;
    }

    @JsonProperty
    public Optional<ConnectorSplit> getSourceSplit()
    {
        return sourceSplit;
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
    public boolean isRemotelyAccessible()
    {
        return getSplit().isRemotelyAccessible();
    }

    @Override
    public List<HostAddress> getAddresses()
    {
        return getSplit().getAddresses();
    }

    @Override
    public Object getInfo()
    {
        return getSplit().getAddresses();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(
                getCacheSplit(),
                getSourceSplit(),
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
        CacheSplit other = (CacheSplit) obj;
        return Objects.equals(this.getCacheSplit(), other.getCacheSplit()) &&
                Objects.equals(this.getSourceSplit(), other.getSourceSplit()) &&
                Objects.equals(this.getCacheOutputTableHandle(), other.getCacheOutputTableHandle());
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("cacheSplit", prettyOptional(cacheSplit))
                .add("sourceSplit", prettyOptional(sourceSplit))
                .add("cacheOutputTableHandle", prettyOptional(cacheOutputTableHandle))
                .toString();
    }

    private static <T> String prettyOptional(Optional<T> optional)
    {
        return optional.map(Object::toString).orElse("?");
    }

    private ConnectorSplit getSplit()
    {
        return cacheSplit.orElseGet(sourceSplit::get);
    }
}
