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
package com.facebook.presto.hive.metastore;

import com.facebook.presto.spi.security.PrestoPrincipal;
import com.facebook.presto.spi.security.Privilege;
import com.facebook.presto.spi.security.PrivilegeInfo;
import com.google.common.collect.ImmutableSet;

import java.util.Arrays;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static com.facebook.presto.hive.metastore.HivePrivilegeInfo.HivePrivilege.DELETE;
import static com.facebook.presto.hive.metastore.HivePrivilegeInfo.HivePrivilege.INSERT;
import static com.facebook.presto.hive.metastore.HivePrivilegeInfo.HivePrivilege.SELECT;
import static com.facebook.presto.hive.metastore.HivePrivilegeInfo.HivePrivilege.UPDATE;
import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class HivePrivilegeInfo
{
    public enum HivePrivilege
    {
        SELECT, INSERT, UPDATE, DELETE, OWNERSHIP
    }

    private final HivePrivilege hivePrivilege;
    private final boolean grantOption;
    private final PrestoPrincipal grantor;

    public static HivePrivilege toHivePrivilege(Privilege privilege)
    {
        switch (privilege) {
            case SELECT:
                return SELECT;
            case INSERT:
                return INSERT;
            case DELETE:
                return DELETE;
            case UPDATE:
                return UPDATE;
            default:
                throw new IllegalArgumentException("Unexpected privilege: " + privilege);
        }
    }

    public HivePrivilegeInfo(HivePrivilege hivePrivilege, boolean grantOption, PrestoPrincipal grantor)
    {
        this.hivePrivilege = requireNonNull(hivePrivilege, "hivePrivilege is null");
        this.grantOption = grantOption;
        this.grantor = requireNonNull(grantor, "grantor is null");
    }

    public HivePrivilege getHivePrivilege()
    {
        return hivePrivilege;
    }

    public boolean isGrantOption()
    {
        return grantOption;
    }

    public PrestoPrincipal getGrantor()
    {
        return grantor;
    }

    public boolean isContainedIn(HivePrivilegeInfo hivePrivilegeInfo)
    {
        return (getHivePrivilege().equals(hivePrivilegeInfo.getHivePrivilege()) &&
                (isGrantOption() == hivePrivilegeInfo.isGrantOption() ||
                        (!isGrantOption() && hivePrivilegeInfo.isGrantOption())));
    }

    public Set<PrivilegeInfo> toPrivilegeInfo()
    {
        switch (hivePrivilege) {
            case SELECT:
                return ImmutableSet.of(new PrivilegeInfo(Privilege.SELECT, isGrantOption()));
            case INSERT:
                return ImmutableSet.of(new PrivilegeInfo(Privilege.INSERT, isGrantOption()));
            case DELETE:
                return ImmutableSet.of(new PrivilegeInfo(Privilege.DELETE, isGrantOption()));
            case UPDATE:
                return ImmutableSet.of(new PrivilegeInfo(Privilege.UPDATE, isGrantOption()));
            case OWNERSHIP:
                return ImmutableSet.copyOf(Arrays.stream(Privilege.values())
                        .map(privilege -> new PrivilegeInfo(privilege, Boolean.TRUE))
                        .collect(Collectors.toSet()));
            default:
                throw new IllegalArgumentException("Unsupported hivePrivilege: " + hivePrivilege);
        }
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(hivePrivilege, grantOption);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        HivePrivilegeInfo hivePrivilegeInfo = (HivePrivilegeInfo) o;
        return Objects.equals(hivePrivilege, hivePrivilegeInfo.hivePrivilege) &&
                Objects.equals(grantOption, hivePrivilegeInfo.grantOption);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("privilege", hivePrivilege)
                .add("grantOption", grantOption)
                .toString();
    }
}
