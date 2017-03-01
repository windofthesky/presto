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

import com.facebook.presto.plugin.cache.CacheMetadata.CacheMetadataFactory;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.RecordPageSource;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorContext;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.facebook.presto.spi.connector.ConnectorPageSinkProvider;
import com.facebook.presto.spi.connector.ConnectorPageSourceProvider;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.FromStringDeserializer;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Binder;
import com.google.inject.BindingAnnotation;
import com.google.inject.Module;
import com.google.inject.Scopes;

import javax.inject.Inject;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;
import static java.util.Objects.requireNonNull;

public class CacheModule
        implements Module
{
    private final String connectorId;
    private final ConnectorContext context;

    public CacheModule(String connectorId, ConnectorContext context)
    {
        this.connectorId = requireNonNull(connectorId, "connector id is null");
        this.context = requireNonNull(context, "context is null");
    }

    @Override
    public void configure(Binder binder)
    {
        binder.bind(TypeManager.class).toInstance(context.getTypeManager());
        binder.bind(NodeManager.class).toInstance(context.getNodeManager());
        binder.bind(ConnectorContext.class).toInstance(context);

        ConnectorFactory factory = context.getConnectorFactory("tpch");
        Connector sourceConnector = factory.create("cached_dunno", ImmutableMap.of(), context);

        ConnectorFactory cachingFactory = context.getConnectorFactory("memory");
        Connector cacheConnector = cachingFactory.create(
                "caching_dunno",
                ImmutableMap.of("memory.max-data-per-node", "4GB"),
                context);

        binder.bind(Connector.class).annotatedWith(Source.class).toInstance(sourceConnector);
        binder.bind(ConnectorPageSourceProvider.class).annotatedWith(Source.class).toInstance(getSourcePageSourceProvider(sourceConnector));
        binder.bind(ConnectorSplitManager.class).annotatedWith(Source.class).toInstance(sourceConnector.getSplitManager());

        binder.bind(Connector.class).annotatedWith(Cache.class).toInstance(cacheConnector);
        binder.bind(ConnectorPageSourceProvider.class).annotatedWith(Cache.class).toInstance(cacheConnector.getPageSourceProvider());
        binder.bind(ConnectorPageSinkProvider.class).annotatedWith(Cache.class).toInstance(cacheConnector.getPageSinkProvider());
        binder.bind(ConnectorSplitManager.class).annotatedWith(Cache.class).toInstance(cacheConnector.getSplitManager());

        binder.bind(CacheConnector.class).in(Scopes.SINGLETON);
        binder.bind(CacheConnectorId.class).toInstance(new CacheConnectorId(connectorId));
        binder.bind(CacheSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(CachePageSourceProvider.class).in(Scopes.SINGLETON);
        binder.bind(CacheMetadataFactory.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(CacheConfig.class);
    }

    private static ConnectorPageSourceProvider getSourcePageSourceProvider(Connector sourceConnector)
    {
        try {
            return sourceConnector.getPageSourceProvider();
        }
        catch (UnsupportedOperationException ex) {
            return (transactionHandle, session, split, columns) -> {
                RecordSet recordSet = sourceConnector.getRecordSetProvider().getRecordSet(
                        transactionHandle,
                        session,
                        split,
                        columns);
                return new RecordPageSource(recordSet);
            };
        }
    }

    public static final class TypeDeserializer
            extends FromStringDeserializer<Type>
    {
        private final TypeManager typeManager;

        @Inject
        public TypeDeserializer(TypeManager typeManager)
        {
            super(Type.class);
            this.typeManager = requireNonNull(typeManager, "typeManager is null");
        }

        @Override
        protected Type _deserialize(String value, DeserializationContext context)
        {
            Type type = typeManager.getType(parseTypeSignature(value));
            checkArgument(type != null, "Unknown type %s", value);
            return type;
        }
    }

    @BindingAnnotation @Target({PARAMETER}) @Retention(RUNTIME)
    @interface Cache {
    }

    @BindingAnnotation @Target({PARAMETER}) @Retention(RUNTIME)
    @interface Source {
    }
}
