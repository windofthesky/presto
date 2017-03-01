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
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.HostAddress;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public final class CacheTableHandle
        implements ConnectorTableHandle
{
    private final Optional<ConnectorTableHandle> cacheTableHandle;
    private final Optional<ConnectorTableHandle> sourceTableHandle;
    private final Optional<ConnectorInsertTableHandle> insertToCacheTableHandle;
    private final List<HostAddress> hosts;

    @JsonCreator
    public CacheTableHandle(
            @JsonProperty("cacheTableHandle") Optional<ConnectorTableHandle> cacheTableHandle,
            @JsonProperty("sourceTableHandle") Optional<ConnectorTableHandle> sourceTableHandle,
            @JsonProperty("insertToCacheTableHandle") Optional<ConnectorInsertTableHandle> insertToCacheTableHandle,
            @JsonProperty("hosts") List<HostAddress> hosts)
    {
        this.cacheTableHandle = requireNonNull(cacheTableHandle, "cacheTableHandle is null");
        this.sourceTableHandle = requireNonNull(sourceTableHandle, "sourceTableHandle is null");
        this.insertToCacheTableHandle = requireNonNull(insertToCacheTableHandle, "insertToCacheTableHandle is null");
        this.hosts = requireNonNull(hosts, "hosts is null");
    }

    @JsonProperty
    public Optional<ConnectorTableHandle> getSourceTableHandle()
    {
        return sourceTableHandle;
    }

    @JsonProperty
    public Optional<ConnectorTableHandle> getCacheTableHandle()
    {
        return cacheTableHandle;
    }

    @JsonProperty
    public Optional<ConnectorInsertTableHandle> getInsertToCacheTableHandle()
    {
        return insertToCacheTableHandle;
    }

    @JsonProperty
    public List<HostAddress> getHosts()
    {
        return hosts;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(
                getSourceTableHandle(),
                getCacheTableHandle(),
                getInsertToCacheTableHandle(),
                getHosts());
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
        CacheTableHandle other = (CacheTableHandle) obj;
        return Objects.equals(this.getSourceTableHandle(), other.getSourceTableHandle()) &&
                Objects.equals(this.getCacheTableHandle(), other.getCacheTableHandle()) &&
                Objects.equals(this.getInsertToCacheTableHandle(), other.getInsertToCacheTableHandle()) &&
                Objects.equals(this.getHosts(), other.getHosts());
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("sourceTableHandle", prettyOptional(sourceTableHandle))
                .add("cacheTableHandle", prettyOptional(cacheTableHandle))
                .add("insertToCacheTableHandle", prettyOptional(insertToCacheTableHandle))
                .toString();
    }

    public static ConnectorTableHandle cached(ConnectorTableHandle cacheTableHandle, List<HostAddress> hosts)
    {
        return new CacheTableHandle(
                Optional.of(cacheTableHandle),
                Optional.empty(),
                Optional.empty(),
                hosts);
    }

    public static ConnectorTableHandle scanAndCache(
            ConnectorTableHandle sourceTableHandle,
            ConnectorInsertTableHandle cacheInsertTableHandle,
            List<HostAddress> hosts)
    {
        return new CacheTableHandle(
                Optional.empty(),
                Optional.of(sourceTableHandle),
                Optional.of(cacheInsertTableHandle),
                hosts);
    }

    private static <T> String prettyOptional(Optional<T> optional)
    {
        return optional.map(Object::toString).orElse("?");
    }
}
