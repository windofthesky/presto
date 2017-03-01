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
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Set;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class CacheInsertTableHandle
        implements ConnectorInsertTableHandle
{
    private final CacheTableHandle table;
    private Set<Long> activeTableIds;

    @JsonCreator
    public CacheInsertTableHandle(
            @JsonProperty("table") CacheTableHandle table,
            @JsonProperty("activeTableIds") Set<Long> activeTableIds)
    {
        this.table = requireNonNull(table, "table is null");
        this.activeTableIds = requireNonNull(activeTableIds, "activeTableIds is null");
    }

    @JsonProperty
    public CacheTableHandle getTable()
    {
        return table;
    }

    @JsonProperty
    public Set<Long> getActiveTableIds()
    {
        return activeTableIds;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("table", table)
                .add("activeTableIds", activeTableIds)
                .toString();
    }
}
