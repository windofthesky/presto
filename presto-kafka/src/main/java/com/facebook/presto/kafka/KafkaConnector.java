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
package com.facebook.presto.kafka;

import com.facebook.presto.spi.classloader.ThreadContextClassLoader;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorRecordSetProvider;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.session.PropertyMetadata;
import com.facebook.presto.spi.transaction.IsolationLevel;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.log.Logger;

import java.util.List;

import static com.facebook.presto.spi.transaction.IsolationLevel.READ_COMMITTED;
import static com.facebook.presto.spi.transaction.IsolationLevel.checkConnectorSupports;
import static java.util.Objects.requireNonNull;

/**
 * Kafka specific implementation of the Presto Connector SPI. This is a read only connector.
 */
public class KafkaConnector
        implements Connector
{
    private static final Logger log = Logger.get(KafkaConnector.class);

    private final LifeCycleManager lifeCycleManager;
    private final KafkaMetadata metadata;
    private final KafkaSplitManager splitManager;
    private final KafkaRecordSetProvider recordSetProvider;
    private final KafkaSessionProperties sessionProperties;
    private final ClassLoader classLoader;

    public KafkaConnector(
            LifeCycleManager lifeCycleManager,
            KafkaMetadata metadata,
            KafkaSplitManager splitManager,
            KafkaSessionProperties sessionProperties,
            KafkaRecordSetProvider recordSetProvider,
            ClassLoader classLoader)
    {
        this.lifeCycleManager = requireNonNull(lifeCycleManager, "lifeCycleManager is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.splitManager = requireNonNull(splitManager, "splitManager is null");
        this.recordSetProvider = requireNonNull(recordSetProvider, "recordSetProvider is null");
        this.sessionProperties = requireNonNull(sessionProperties, "sessionProperty is null");
        this.classLoader = requireNonNull(classLoader, "classLoader is null");
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly)
    {
        try (ThreadContextClassLoader ignore = new ThreadContextClassLoader(classLoader)) {
            checkConnectorSupports(READ_COMMITTED, isolationLevel);
        }
        return KafkaTransactionHandle.INSTANCE;
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorTransactionHandle transactionHandle)
    {
        try (ThreadContextClassLoader ignore = new ThreadContextClassLoader(classLoader)) {
            return metadata;
        }
    }

    @Override
    public ConnectorSplitManager getSplitManager()
    {
        try (ThreadContextClassLoader ignore = new ThreadContextClassLoader(classLoader)) {
            return splitManager;
        }
    }

    @Override
    public ConnectorRecordSetProvider getRecordSetProvider()
    {
        try (ThreadContextClassLoader ignore = new ThreadContextClassLoader(classLoader)) {
            return recordSetProvider;
        }
    }

    @Override
    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties.getSessionProperties();
    }

    @Override
    public final void shutdown()
    {
        try {
            lifeCycleManager.stop();
        }
        catch (Exception e) {
            log.error(e, "Error shutting down connector");
        }
    }
}
