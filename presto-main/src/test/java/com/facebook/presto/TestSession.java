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
package com.facebook.presto;

import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.server.security.LdapPrincipal;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.transaction.TransactionId;
import io.airlift.json.JsonCodec;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.spi.type.TimeZoneKey.getTimeZoneKey;
import static java.util.Locale.CANADA_FRENCH;
import static org.testng.Assert.assertEquals;

public class TestSession
{
    private SessionPropertyManager sessionPropertyManager;
    private JsonCodec<SessionRepresentation> codec;

    @BeforeClass
    public void setUp()
    {
        sessionPropertyManager = new SessionPropertyManager();
        codec = JsonCodec.jsonCodec(SessionRepresentation.class);
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        sessionPropertyManager = null;
        codec = null;
    }

    @Test
    public void testSessionRoundTrip()
            throws Exception
    {
        Session expected = createTestSession();
        Session actual = expected.toSessionRepresentation().toSession(sessionPropertyManager);
        assertSessionEquals(actual, expected);
    }

    @Test
    public void testBasicSessionJsonRoundTrip()
            throws Exception
    {
        assertSessionJsonRoundTrip(createTestSession());
    }

    @Test
    public void testEmptyPrincipalSessionJsonRoundTrip()
            throws Exception
    {
        Session session = Session.builder(createTestSession())
                .setIdentity(new Identity("user", Optional.empty()))
                .build();
        assertSessionJsonRoundTrip(session);
    }

    private void assertSessionJsonRoundTrip(Session session)
    {
        String serialized = codec.toJson(session.toSessionRepresentation());
        Session actual = codec.fromJson(serialized).toSession(sessionPropertyManager);
        assertSessionEquals(actual, session);
    }

    private static void assertSessionEquals(Session actual, Session expected)
    {
        assertEquals(actual.getQueryId(), expected.getQueryId());
        assertEquals(actual.getTransactionId(), expected.getTransactionId());
        assertEquals(actual.isClientTransactionSupport(), expected.isClientTransactionSupport());
        assertEquals(actual.getIdentity(), expected.getIdentity());
        assertEquals(actual.getSource(), expected.getSource());
        assertEquals(actual.getCatalog(), expected.getCatalog());
        assertEquals(actual.getSchema(), expected.getSchema());
        assertEquals(actual.getTimeZoneKey(), expected.getTimeZoneKey());
        assertEquals(actual.getLocale(), expected.getLocale());
        assertEquals(actual.getRemoteUserAddress(), expected.getRemoteUserAddress());
        assertEquals(actual.getUserAgent(), expected.getUserAgent());
        assertEquals(actual.getClientInfo(), expected.getClientInfo());
        assertEquals(actual.getStartTime(), expected.getStartTime());
        assertEquals(actual.getSystemProperties(), expected.getSystemProperties());
        assertEquals(actual.getConnectorProperties(), expected.getConnectorProperties());
        assertEquals(actual.getUnprocessedCatalogProperties(), expected.getUnprocessedCatalogProperties());
        assertEquals(actual.getPreparedStatements(), expected.getPreparedStatements());
    }

    private Session createTestSession()
            throws Exception
    {
        TransactionId transactionId = TransactionId.valueOf("123e4567-e89b-12d3-a456-426655440000");
        return Session.builder(sessionPropertyManager)
                .setQueryId(new QueryId("test_query"))
                // TODO:
                // .setTransactionId(transactionId)
                .setClientTransactionSupport()
                .setIdentity(new Identity("user", Optional.of(new LdapPrincipal("user"))))
                .setSource("test_source")
                .setCatalog("test_catalog")
                .setSchema("schema")
                .setTimeZoneKey(getTimeZoneKey("Asia/Katmandu"))
                .setLocale(CANADA_FRENCH)
                .setRemoteUserAddress("127.0.1.1")
                .setUserAgent("test_user_agent")
                .setClientInfo("test_client_info")
                .setStartTime(123456789L)
                .setSystemProperty("optimize_hash_generation", "true")
                .setSystemProperty("query_max_run_time", "1m")
                // TODO:
                // .setCatalogSessionProperty("test_catalog", "test_property_name", "test_property_value")
                .addPreparedStatement("test_prepared_statement", "SELECT 1")
                .build();
    }
}
