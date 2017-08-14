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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import io.airlift.log.Logger;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

import java.net.URI;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static com.facebook.presto.decoder.avro.AvroSchemaUtils.toPrestoType;
import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.isNull;
import static java.util.Objects.requireNonNull;

/**
 * Groups the field descriptions for message or key.
 */
public class KafkaTopicFieldGroup
{
    private final String dataFormat;
    private final List<KafkaTopicFieldDescription> fields;
    private final String schemaURI;
    private final String schemaRoot;

    private static final Cache<Schema, Schema> AvroSchemaCache = CacheBuilder.newBuilder()
            .maximumSize(10000)
            .concurrencyLevel(25)
            .build();

    private static final Logger LOG = Logger.get(KafkaTopicFieldGroup.class);

    public KafkaTopicFieldGroup(String dataFormat,
            List<KafkaTopicFieldDescription> fields)
    {
        this(dataFormat, fields, "", "");
    }

    private static boolean valid(String s)
    {
        return (!isNull(s) && !s.isEmpty());
    }

    @JsonCreator
    public KafkaTopicFieldGroup(
            @JsonProperty("dataFormat") String dataFormat,
            @JsonProperty("fields") List<KafkaTopicFieldDescription> fields,
            @JsonProperty("schemaURI") String schemaURI,
            @JsonProperty("schemaRoot") String schemaRoot)
    {
        this.dataFormat = requireNonNull(dataFormat, "dataFormat is null");
        this.schemaURI = schemaURI;

        if (valid(schemaURI) && !valid(schemaRoot)) {
            // Lets use suitable default in absence of a schema root provided by user
            String schemaRootName = "avro-schema";
            int schemaRootEndIdx = schemaURI.toLowerCase().lastIndexOf(schemaRootName);
            // Fall-back to the parent, if 'avro-schema' folder is not part of schema-uri
            schemaRootEndIdx = (schemaRootEndIdx < 0)
                    ? schemaURI.lastIndexOf('/')
                    : schemaRootEndIdx + schemaRootName.length();
            this.schemaRoot = schemaURI.substring(0, schemaRootEndIdx).intern();
        }
        else {
            this.schemaRoot = schemaRoot;
        }

        SortedMap<Integer, Schema> schemas = ("avro".equalsIgnoreCase(dataFormat))
                ? getRemoteAvroSchemas(this.schemaURI, this.schemaRoot)
                : null;
        this.fields = (!isNull(schemas))
                ? getFieldsFromRemoteAvroSchemas(schemas)
                : ImmutableList.copyOf(requireNonNull(fields, "fields is null"));
    }

    private static SortedMap<Integer, Schema> getRemoteAvroSchemas(String schemaURI, String schemaRoot)
    {
        SortedMap<Integer, Schema> schemaMap = new TreeMap<>();
        Path path = new Path(schemaURI);
        Path parentPath = new Path(schemaRoot);
        String schemaName = path.getName();

        try {
            Configuration conf = new Configuration();
            final Path hdfsSite = new Path("/etc/hadoop/conf/hdfs-site.xml"); // Move this to optional config
            conf.addResource(hdfsSite);

            FileSystem fs = FileSystem.get(new URI(schemaURI), conf);
            RemoteIterator<LocatedFileStatus> fileStatItr = fs.listFiles(parentPath, true);
            while (fileStatItr.hasNext()) {
                LocatedFileStatus fileStatus = fileStatItr.next();
                if (fileStatus.isDirectory()) {
                    continue;
                }
                Path schemaPath = fileStatus.getPath();
                if (schemaPath != null && schemaName.equals(schemaPath.getName())) {
                    Schema.Parser parser = new Schema.Parser();
                    LOG.info("Parsing schema: " + schemaPath);
                    Schema schema = parser.parse(fs.open(schemaPath));
                    // Discard schemas with null field
                    if (isNull(schema.getField("version"))) {
                        LOG.warn("Ignoring schema with missing 'version' field: " + schemaPath);
                        continue;
                    }
                    final Schema schemaRef = schema;
                    schema = AvroSchemaCache.get(schema, () -> {
                        LOG.info("Caching Avro schema: " + schemaRef.getFullName());
                        return schemaRef;
                    });
                    Integer version = Ints.tryParse(schema.getField("version").defaultVal().toString());
                    schemaMap.put(version, schema);
                }
            }
        }
        catch (Exception ioe) {
            throw new RuntimeException("Unable to read remote AVRO schema: " + schemaURI, ioe);
        }
        //Map<String, Schema> schema = new ConcurrentHashMap<>();
        //schemaMap.forEach((version, schema) -> schemaList.add(schema));
        LOG.info("Schema: " + schemaName + " Versions found: " + schemaMap.size());
        return schemaMap;
    }

    private static List<KafkaTopicFieldDescription> getFieldsFromRemoteAvroSchemas(SortedMap<Integer, Schema> schemas)
    {
        ImmutableList.Builder<KafkaTopicFieldDescription> listBuilder = ImmutableList.builder();

        for (Schema.Field field : schemas.get(schemas.lastKey()).getFields()) {
            KafkaTopicFieldDescription fieldDescription =
                    new KafkaTopicFieldDescription(field.name(),
                            toPrestoType(field.schema()),
                            field.name(),
                            "",
                            "",
                            "",
                            false,
                            schemas.entrySet().stream()
                                    .collect(Collectors.toMap(entry -> entry.getKey(), entry -> entry.getValue().toString())));
            listBuilder.add(fieldDescription);
        }
        return listBuilder.build();
    }

    @JsonProperty
    public String getDataFormat()
    {
        return dataFormat;
    }

    @JsonProperty
    public List<KafkaTopicFieldDescription> getFields()
    {
        return fields;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("dataFormat", dataFormat)
                .add("fields", fields)
                .toString();
    }
}
