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

import com.facebook.presto.decoder.dummy.DummyRowDecoder;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.SchemaTableName;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.ByteStreams;
import com.google.common.util.concurrent.Uninterruptibles;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.TopicMetadataResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

import javax.inject.Inject;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.google.common.base.MoreObjects.firstNonNull;
import static java.util.Arrays.asList;
import static java.util.Objects.isNull;
import static java.util.Objects.requireNonNull;

public class KafkaTableDescriptionSupplier
        implements Supplier<Map<SchemaTableName, KafkaTopicDescription>>
{
    private static final Logger log = Logger.get(KafkaTableDescriptionSupplier.class);

    private final JsonCodec<KafkaTopicDescription> topicDescriptionCodec;
    private final JsonCodec<List<KafkaTopicDescription>> topicDescListCodec;
    private final Set<URI> tableDescriptionDirs;
    private final String defaultSchema;
    private final java.util.Set<String> tableNames;

    @Inject
    KafkaTableDescriptionSupplier(KafkaConnectorConfig kafkaConnectorConfig,
            JsonCodec<KafkaTopicDescription> topicDescriptionCodec,
            JsonCodec<List<KafkaTopicDescription>> topicDescListCodec)
    {
        this.topicDescriptionCodec = requireNonNull(topicDescriptionCodec, "topicDescriptionCodec is null");
        this.topicDescListCodec = topicDescListCodec;
        requireNonNull(kafkaConnectorConfig, "kafkaConfig is null");
        this.tableDescriptionDirs = kafkaConnectorConfig.getTableDescriptionDir();
        this.defaultSchema = kafkaConnectorConfig.getDefaultSchema();
        loadKafkaTopicsDynamically(kafkaConnectorConfig);
        this.tableNames = ImmutableSet.copyOf(kafkaConnectorConfig.getTableNames());
    }

    private void loadKafkaTopicsDynamically(KafkaConnectorConfig config)
    {
        List<HostAddress> brokerNodes = config.getNodes().stream().collect(Collectors.toList());
        if (brokerNodes.isEmpty()) {
            log.error("No Broker nodes found, skipping dynamic partition loading");
            return;
        }
        int brokerIdx = 0;
        while (brokerIdx < brokerNodes.size()) {
            try {
                HostAddress brokerAddress = brokerNodes.get(brokerIdx);
                log.info("Load kafka topics dynamically from broker: " + brokerAddress);
                final int soTimeout = 100_000;
                final int bufferSize = 64 * 1024;
                SimpleConsumer consumer = new SimpleConsumer(brokerAddress.getHostText(),
                        brokerAddress.getPort(),
                        soTimeout,
                        bufferSize,
                        "presto");

                ImmutableSet.Builder topicBuilder = new ImmutableSet.Builder<String>();
                List<String> allTopics = new ArrayList<>();
                TopicMetadataRequest request = new TopicMetadataRequest(allTopics);
                TopicMetadataResponse response = consumer.send(request);
                response.topicsMetadata().stream()
                        .forEach(meta -> topicBuilder.add(meta.topic()));
                Set<String> topics = topicBuilder.build();
                if (!topics.isEmpty()) {
                    config.setTableNames(topics.stream().collect(Collectors.joining(",")));
                    log.info("Topics loaded --> " + topics);
                    return; // finish successfully
                }
            }
            catch (Exception ex) {
                log.error("Attempt " + brokerIdx + " Dynamic loading of Kafka topics failed, retry in 1 sec");
                log.error(Throwables.getStackTraceAsString(ex));
                brokerIdx++;
                Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
            }
        }
        log.error("Dynamic topic loading FAILED! No kafka topics available");
    }

    private Optional<FileSystem> getFileSystem(URI tableDescriptionDir)
    {
        try {
            Configuration conf = new Configuration();
            final Path hdfsSite = new Path("/etc/hadoop/conf/hdfs-site.xml"); // Move this to optional config
            conf.addResource(hdfsSite);
            FileSystem fs = FileSystem.get(tableDescriptionDir, conf);
            return Optional.of(fs);
        }
        catch (Exception ex) {
            log.error("Unable to initialize file system handle for URI: " + tableDescriptionDir);
            log.error(Throwables.getStackTraceAsString(ex));
            log.error("Falling back to local filesystem");
            try {
                return Optional.of(FileSystem.getLocal(new Configuration()));
            }
            catch (IOException ioe) {
                log.error("Giving up on initializing filesystem handle");
                return Optional.empty();
            }
        }
    }

    private Optional<RemoteIterator<LocatedFileStatus>> listFiles(FileSystem fs, Path path)
    {
        try {
            return Optional.of(fs.listFiles(path, true));
        }
        catch (IOException ioe) {
            log.error("Unable to get topic listing from: " + path);
            log.error(Throwables.getStackTraceAsString(ioe));
            return Optional.empty();
        }
    }

    private Set<KafkaTopicDescription> loadTableDescriptions()
    {
        ImmutableSet.Builder<KafkaTopicDescription> topicSetBuilder = new ImmutableSet.Builder<>();
        for (URI tableDescriptionDir : this.tableDescriptionDirs) {
            Optional<FileSystem> optionalFs = getFileSystem(tableDescriptionDir);
            if (!optionalFs.isPresent()) {
                continue;
            }
            FileSystem fs = optionalFs.get();
            Path tableDescPath = new Path(tableDescriptionDir);
            Optional<RemoteIterator<LocatedFileStatus>> optionaFileStatItr = listFiles(fs, tableDescPath);
            if (!optionaFileStatItr.isPresent()) {
                continue;
            }
            RemoteIterator<LocatedFileStatus> fileStatItr = optionaFileStatItr.get();
            try {
                while (fileStatItr.hasNext()) {
                    LocatedFileStatus fileStatus = fileStatItr.next();
                    if (fileStatus.isDirectory()) {
                        continue;
                    }
                    Path descPath = fileStatus.getPath();
                    if (isNull(descPath)) {
                        continue;
                    }
                    byte[] descBytes = ByteStreams.toByteArray(fs.open(descPath));

                    if (descPath.getName().endsWith(".conf")) {
                        log.info("Loading Kafka topic conf: " + descPath);

                        List<KafkaTopicDescription> topicDescriptions =
                                topicDescListCodec.fromJson(descBytes);
                        topicSetBuilder.addAll(topicDescriptions);
                    }
                    else if (descPath.getName().endsWith(".json")) {
                        log.info("Loading Kafka topic JSON: " + descPath);
                        KafkaTopicDescription table = topicDescriptionCodec.fromJson(descBytes);
                        topicSetBuilder.add(table);
                    }
                }
            }
            catch (Exception ioe) {
                log.error("Unable to read Kafka table descriptions");
                log.error(Throwables.getStackTraceAsString(ioe));
            }
        }
        return topicSetBuilder.build();
    }

    @Override
    public synchronized Map<SchemaTableName, KafkaTopicDescription> get()
    {
        final ImmutableMap.Builder<SchemaTableName, KafkaTopicDescription> descBuilder = ImmutableMap.builder();
        log.debug("Loading kafka table definitions from %s", tableDescriptionDirs);

        loadTableDescriptions().stream()
                .forEach(table -> {
                    String schemaName = firstNonNull(table.getSchemaName(), defaultSchema);
                    log.debug("Kafka table %s.%s: %s", schemaName, table.getTableName(), table);
                    descBuilder.put(new SchemaTableName(schemaName, table.getTableName()), table);
                });

        Map<SchemaTableName, KafkaTopicDescription> tableDefinitions = descBuilder.build();
        log.debug("Loaded Table definitions: %s", tableDefinitions.keySet());

        ImmutableMap.Builder<SchemaTableName, KafkaTopicDescription> builder = ImmutableMap.builder();
        for (String definedTable : tableNames) {
            SchemaTableName tableName;
            try {
                tableName = SchemaTableName.valueOf(definedTable);
            }
            catch (IllegalArgumentException iae) {
                tableName = new SchemaTableName(defaultSchema, definedTable);
            }

            if (tableDefinitions.containsKey(tableName)) {
                KafkaTopicDescription kafkaTable = tableDefinitions.get(tableName);
                log.debug("Found Table definition for %s: %s", tableName, kafkaTable);
                builder.put(tableName, kafkaTable);
            }
            else {
                // A dummy table definition only supports the internal columns.
                log.debug("Created dummy Table definition for %s", tableName);
                builder.put(tableName, new KafkaTopicDescription(tableName.getTableName(),
                        tableName.getSchemaName(),
                        definedTable,
                        new KafkaTopicFieldGroup(DummyRowDecoder.NAME, ImmutableList.<KafkaTopicFieldDescription>of()),
                        new KafkaTopicFieldGroup(DummyRowDecoder.NAME, ImmutableList.<KafkaTopicFieldDescription>of())));
            }
        }

        return builder.build();
    }

    private static List<File> listFiles(File dir)
    {
        if ((dir != null) && dir.isDirectory()) {
            File[] files = dir.listFiles();
            if (files != null) {
                log.debug("Considering files: %s", asList(files));
                return ImmutableList.copyOf(files);
            }
        }
        return ImmutableList.of();
    }
}
