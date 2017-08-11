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
package com.facebook.presto.unittests;

import com.facebook.presto.connector.unittest.BaseMetadataTest;
import com.facebook.presto.connector.unittest.MetadataTableTest;
import com.facebook.presto.plugin.postgresql.PostgreSqlPlugin;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.testing.TestingConnectorContext;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.testing.postgresql.TestingPostgreSqlServer;
import org.junit.jupiter.api.BeforeAll;

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static com.google.common.collect.Iterables.getOnlyElement;

public class TestPostgresqlMetadata
        implements BaseMetadataTest, MetadataTableTest
{
    private Connector connector;

    @BeforeAll
    public void beforeAll()
            throws Exception
    {
        this.connector = createPostgresqlConnector();
    }

    @Override
    public Connector getConnector()
    {
        return connector;
    }

    private static Connector createPostgresqlConnector()
            throws Exception
    {
        TestingPostgreSqlServer server = new TestingPostgreSqlServer("testuser", "unittests");

        PostgreSqlPlugin plugin = new PostgreSqlPlugin();
        Iterable<ConnectorFactory> connectorFactories = plugin.getConnectorFactories();

        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("connection-url", server.getJdbcUrl())
                .put("allow-drop-table", "true")
                .build();

        ConnectorFactory factory = getOnlyElement(connectorFactories);
        return factory.create("postgresql", properties, new TestingConnectorContext());
    }

    @Override
    public Map<String, Object> getTableProperties()
    {
        return ImmutableMap.of();
    }

    @Override
    public List<ColumnMetadata> getConnectorColumns()
    {
        return ImmutableList.of();
    }

    @Override
    public SchemaTableName schemaTableName(String tableName)
    {
        return new SchemaTableName("public", tableName);
    }

    @Override
    public List<String> systemSchemas()
    {
        return ImmutableList.of("pg_catalog", "public");
    }

    @Override
    public List<Consumer<ConnectorMetadata>> withSchema(ConnectorSession session, List<String> schemaNames, List<Consumer<ConnectorMetadata>> consumers)
    {
        return consumers;
    }

    @Override
    public void testCreateDropTable()
    {
    }

    @Override
    public void testRenameTableWithinSchema()
    {
    }

    @Override
    public void testRenameTableAcrossSchema()
    {
    }

    @Override
    public void testListColumnsMultiSchema()
    {
    }

    @Override
    public void testAddColumn()
    {
    }

    @Override
    public void testRenameColumn()
    {
    }
}
