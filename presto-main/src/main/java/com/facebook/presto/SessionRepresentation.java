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

import com.facebook.presto.connector.ConnectorId;
import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.spi.type.TimeZoneKey;
import com.facebook.presto.transaction.TransactionId;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.collect.ImmutableMap;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.security.Principal;
import java.util.Base64;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public final class SessionRepresentation
{
    private final String queryId;
    private final Optional<TransactionId> transactionId;
    private final boolean clientTransactionSupport;
    private final String user;
    private final Optional<Principal> principal;
    private final Optional<String> source;
    private final Optional<String> catalog;
    private final Optional<String> schema;
    private final TimeZoneKey timeZoneKey;
    private final Locale locale;
    private final Optional<String> remoteUserAddress;
    private final Optional<String> userAgent;
    private final Optional<String> clientInfo;
    private final long startTime;
    private final Map<String, String> systemProperties;
    private final Map<ConnectorId, Map<String, String>> catalogProperties;
    private final Map<String, String> preparedStatements;

    @JsonCreator
    public SessionRepresentation(
            @JsonProperty("queryId") String queryId,
            @JsonProperty("transactionId") Optional<TransactionId> transactionId,
            @JsonProperty("clientTransactionSupport") boolean clientTransactionSupport,
            @JsonProperty("user") String user,
            @JsonProperty("principal") Optional<Principal> principal,
            @JsonProperty("source") Optional<String> source,
            @JsonProperty("catalog") Optional<String> catalog,
            @JsonProperty("schema") Optional<String> schema,
            @JsonProperty("timeZoneKey") TimeZoneKey timeZoneKey,
            @JsonProperty("locale") Locale locale,
            @JsonProperty("remoteUserAddress") Optional<String> remoteUserAddress,
            @JsonProperty("userAgent") Optional<String> userAgent,
            @JsonProperty("clientInfo") Optional<String> clientInfo,
            @JsonProperty("startTime") long startTime,
            @JsonProperty("systemProperties") Map<String, String> systemProperties,
            @JsonProperty("catalogProperties") Map<ConnectorId, Map<String, String>> catalogProperties,
            @JsonProperty("preparedStatements") Map<String, String> preparedStatements)
    {
        this.queryId = requireNonNull(queryId, "queryId is null");
        this.transactionId = requireNonNull(transactionId, "transactionId is null");
        this.clientTransactionSupport = clientTransactionSupport;
        this.user = requireNonNull(user, "user is null");
        this.principal = requireNonNull(principal, "principal is null");
        this.source = requireNonNull(source, "source is null");
        this.catalog = requireNonNull(catalog, "catalog is null");
        this.schema = requireNonNull(schema, "schema is null");
        this.timeZoneKey = requireNonNull(timeZoneKey, "timeZoneKey is null");
        this.locale = requireNonNull(locale, "locale is null");
        this.remoteUserAddress = requireNonNull(remoteUserAddress, "remoteUserAddress is null");
        this.userAgent = requireNonNull(userAgent, "userAgent is null");
        this.clientInfo = requireNonNull(clientInfo, "clientInfo is null");
        this.startTime = startTime;
        this.systemProperties = ImmutableMap.copyOf(systemProperties);
        this.preparedStatements = ImmutableMap.copyOf(preparedStatements);

        ImmutableMap.Builder<ConnectorId, Map<String, String>> catalogPropertiesBuilder = ImmutableMap.builder();
        for (Entry<ConnectorId, Map<String, String>> entry : catalogProperties.entrySet()) {
            catalogPropertiesBuilder.put(entry.getKey(), ImmutableMap.copyOf(entry.getValue()));
        }
        this.catalogProperties = catalogPropertiesBuilder.build();
    }

    @JsonProperty
    public String getQueryId()
    {
        return queryId;
    }

    @JsonProperty
    public Optional<TransactionId> getTransactionId()
    {
        return transactionId;
    }

    @JsonProperty
    public boolean isClientTransactionSupport()
    {
        return clientTransactionSupport;
    }

    @JsonProperty
    public String getUser()
    {
        return user;
    }

    @JsonProperty
    @JsonSerialize(using = PrincipalSerializer.class)
    @JsonDeserialize(using = PrincipalDeserializer.class)
    public Optional<Principal> getPrincipal()
    {
        return principal;
    }

    @JsonProperty
    public Optional<String> getSource()
    {
        return source;
    }

    @JsonProperty
    public Optional<String> getCatalog()
    {
        return catalog;
    }

    @JsonProperty
    public Optional<String> getSchema()
    {
        return schema;
    }

    @JsonProperty
    public TimeZoneKey getTimeZoneKey()
    {
        return timeZoneKey;
    }

    @JsonProperty
    public Locale getLocale()
    {
        return locale;
    }

    @JsonProperty
    public Optional<String> getRemoteUserAddress()
    {
        return remoteUserAddress;
    }

    @JsonProperty
    public Optional<String> getUserAgent()
    {
        return userAgent;
    }

    @JsonProperty
    public Optional<String> getClientInfo()
    {
        return clientInfo;
    }

    @JsonProperty
    public long getStartTime()
    {
        return startTime;
    }

    @JsonProperty
    public Map<String, String> getSystemProperties()
    {
        return systemProperties;
    }

    @JsonProperty
    public Map<ConnectorId, Map<String, String>> getCatalogProperties()
    {
        return catalogProperties;
    }

    @JsonProperty
    public Map<String, String> getPreparedStatements()
    {
        return preparedStatements;
    }

    public Session toSession(SessionPropertyManager sessionPropertyManager)
    {
        return new Session(
                new QueryId(queryId),
                transactionId,
                clientTransactionSupport,
                new Identity(user, principal),
                source,
                catalog,
                schema,
                timeZoneKey,
                locale,
                remoteUserAddress,
                userAgent,
                clientInfo,
                startTime,
                systemProperties,
                catalogProperties,
                ImmutableMap.of(),
                sessionPropertyManager,
                preparedStatements);
    }

    public static class PrincipalSerializer
            extends JsonSerializer<Optional<Principal>>
    {
        @Override
        public void serialize(Optional<Principal> value, JsonGenerator generator, SerializerProvider serializers)
                throws IOException
        {
            if (!value.isPresent()) {
                generator.writeNull();
                return;
            }

            Principal principal = value.get();
            byte[] serialized = serialize(principal);
            String base64encoded = Base64.getEncoder().encodeToString(serialized);
            generator.writeString(base64encoded);
        }

        private static byte[] serialize(Principal principal)
                throws IOException
        {
            checkArgument(principal instanceof Serializable, "Principal is not serializable: %s", principal.toString());
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            try (ObjectOutputStream output = new ObjectOutputStream(byteArrayOutputStream)) {
                output.writeObject(principal);
            }
            return byteArrayOutputStream.toByteArray();
        }
    }

    public static class PrincipalDeserializer
            extends JsonDeserializer<Optional<Principal>>
    {
        @Override
        public Optional<Principal> deserialize(JsonParser parser, DeserializationContext ctxt)
                throws IOException
        {
            String base64encoded = parser.getText();
            requireNonNull(base64encoded, "base64encoded is null");
            byte[] serialized = Base64.getDecoder().decode(base64encoded);
            Principal principal = deserialize(serialized);
            return Optional.of(principal);
        }

        @Override
        public Optional<Principal> getNullValue(DeserializationContext ctxt)
                throws JsonMappingException
        {
            return Optional.empty();
        }

        private static Principal deserialize(byte[] serialized)
                throws IOException
        {
            try (ObjectInputStream input = new ObjectInputStream(new ByteArrayInputStream(serialized))) {
                try {
                    Object object = input.readObject();
                    checkArgument(object instanceof Principal, "Deserialized object is not instance of Principal: %s", object.toString());
                    return (Principal) object;
                }
                catch (ClassNotFoundException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }
}
