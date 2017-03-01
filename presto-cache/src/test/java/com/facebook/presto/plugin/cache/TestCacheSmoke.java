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

import com.facebook.presto.Session;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.sql.SQLException;

import static com.facebook.presto.plugin.cache.CacheQueryRunner.createQueryRunner;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;

@Test(singleThreaded = true)
public class TestCacheSmoke
{
    private QueryRunner queryRunner;

    @BeforeTest
    public void setUp()
            throws Exception
    {
        queryRunner = createQueryRunner();
    }

    @Test
    public void selectSmallTable()
            throws SQLException
    {
        assertThatQueryReturnsSameValueAs("SELECT * FROM nation ORDER BY nationkey", "SELECT * FROM tpch.tiny.nation ORDER BY nationkey");
    }

    @Test
    public void selectLargeTable()
            throws SQLException
    {
        assertThatQueryReturnsSameValueAs("SELECT * FROM orders ORDER BY orderkey", "SELECT * FROM tpch.tiny.orders ORDER BY orderkey");
    }

    @Test
    public void selectSingleRow()
    {
        assertThatQueryReturnsSameValueAs("SELECT * FROM region WHERE regionkey = 1", "SELECT * FROM tpch.tiny.region WHERE regionkey = 1");
    }

    @Test
    public void selectColumnsSubset()
            throws SQLException
    {
        assertThatQueryReturnsSameValueAs("SELECT name, suppkey FROM supplier ORDER BY suppkey", "SELECT name, suppkey FROM tpch.tiny.supplier ORDER BY suppkey");
    }

    @Test
    public void selectAllColumnsAfterColumnsSubset()
            throws SQLException
    {
        assertThatQueryReturnsSameValueAs("SELECT name, suppkey FROM supplier ORDER BY suppkey", "SELECT name, suppkey FROM tpch.tiny.supplier ORDER BY suppkey");
        assertThatQueryReturnsSameValueAs("SELECT * FROM supplier ORDER BY suppkey", "SELECT * FROM tpch.tiny.supplier ORDER BY suppkey");
    }

    @Test
    public void selectsFromDifferentSchemas()
            throws SQLException
    {
        assertThatQueryReturnsSameValueAs("SELECT name, suppkey FROM cache.tiny.supplier ORDER BY suppkey", "SELECT name, suppkey FROM tpch.tiny.supplier ORDER BY suppkey");
        assertThatQueryReturnsSameValueAs("SELECT name, suppkey FROM cache.sf1.supplier ORDER BY suppkey", "SELECT name, suppkey FROM tpch.sf1.supplier ORDER BY suppkey");
    }

    private void assertThatQueryReturnsSameValueAs(String sql, String compareSql)
    {
        assertThatQueryReturnsSameValueAs(sql, compareSql, null);
    }

    private void assertThatQueryReturnsSameValueAs(String sql, String compareSql, Session session)
    {
        // first run from source
        MaterializedResult rows = session == null ? queryRunner.execute(sql) : queryRunner.execute(session, sql);
        MaterializedResult expectedRows = session == null ? queryRunner.execute(compareSql) : queryRunner.execute(session, compareSql);
        assertEquals(rows, expectedRows);

        // second from cache
        rows = session == null ? queryRunner.execute(sql) : queryRunner.execute(session, sql);
        assertEquals(rows, expectedRows);
    }
}
