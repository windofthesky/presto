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

package com.facebook.presto.hive.statistics;

import com.facebook.presto.hive.HiveColumnHandle;
import com.facebook.presto.hive.HivePartition;
import com.facebook.presto.hive.PartitionStatistics;
import com.facebook.presto.hive.metastore.ColumnStatistics;
import com.facebook.presto.hive.metastore.Partition;
import com.facebook.presto.hive.metastore.SemiTransactionalHiveMetastore;
import com.facebook.presto.hive.metastore.Table;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.statistics.Estimate;
import com.facebook.presto.spi.statistics.TableStatistics;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;

import javax.annotation.Nullable;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.DoubleStream;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

public class MetastoreHiveStatisticsProvider implements HiveStatisticsProvider
{
    private final TypeManager typeManager;
    private final SemiTransactionalHiveMetastore metastore;

    public MetastoreHiveStatisticsProvider(TypeManager typeManager, SemiTransactionalHiveMetastore metastore)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.metastore = requireNonNull(metastore, "metastore is null");
    }

    @Override
    public TableStatistics getTableStatistics(ConnectorSession session, ConnectorTableHandle tableHandle, List<HivePartition> hivePartitions, Map<String, ColumnHandle> tableColumns)
    {
        Map<String, PartitionStatistics> partitionStatisticsMap = getPartitionsStatistics(hivePartitions, tableColumns.keySet());

        TableStatistics.Builder tableStatistics = TableStatistics.builder();

        tableStatistics.setRowCount(calculateRowsCount(partitionStatisticsMap));

        for (Map.Entry<String, ColumnHandle> columnEntry : tableColumns.entrySet()) {
            String columnName = columnEntry.getKey();
            HiveColumnHandle hiveColumnHandle = (HiveColumnHandle) columnEntry.getValue();
            if (getColumnMetadata(hiveColumnHandle).isHidden()) {
                continue;
            }
            com.facebook.presto.spi.statistics.ColumnStatistics.Builder columnStatistics = com.facebook.presto.spi.statistics.ColumnStatistics.builder();
            if (hiveColumnHandle.isPartitionKey()) {
                columnStatistics.setDistinctValuesCount(calculateDistinctValuesCountForPartitioningKey(hiveColumnHandle, hivePartitions));
                columnStatistics.setNullsCount(calculateNullsCountForPartitioningKey(hiveColumnHandle, hivePartitions, partitionStatisticsMap));
            }
            else {
                columnStatistics.setDistinctValuesCount(calculateDistinctValuesCount(partitionStatisticsMap, columnName));
                columnStatistics.setNullsCount(calculateNullsCount(partitionStatisticsMap, columnName));
            }
            tableStatistics.setColumnStatistics(hiveColumnHandle, columnStatistics.build());
        }
        return tableStatistics.build();
    }

    private Map<String, PartitionStatistics> getPartitionsStatistics(List<HivePartition> hivePartitions, Set<String> tableColumns)
    {
        if (hivePartitions.isEmpty()) {
            return ImmutableMap.of();
        }
        boolean unpartitioned = hivePartitions.stream().anyMatch(partition -> partition.getPartitionId().equals(HivePartition.UNPARTITIONED_ID));
        if (unpartitioned) {
            checkArgument(hivePartitions.size() == 1, "expected only one hive partition");
        }
        SchemaTableName tableName = getTableName(hivePartitions);

        if (unpartitioned) {
            return ImmutableMap.of(HivePartition.UNPARTITIONED_ID, getUnpartitionedStatistics(tableName, tableColumns));
        }
        else {
            return getPartitionedStatistics(tableName, hivePartitions, tableColumns);
        }
    }

    private SchemaTableName getTableName(List<HivePartition> hivePartitions)
    {
        Set<SchemaTableName> tableNames = hivePartitions.stream()
                .map(HivePartition::getTableName)
                .collect(toSet());
        checkArgument(tableNames.size() == 1, "all hive partitions must have same table name; got %s", tableNames);
        return Iterables.getOnlyElement(tableNames);
    }

    private Optional<Long> convertStringParameter(@Nullable String parameterValue)
    {
        if (parameterValue == null) {
            return Optional.empty();
        }
        try {
            long longValue = Long.parseLong(parameterValue);
            if (longValue < 0) {
                return Optional.empty();
            }
            return Optional.of(longValue);
        }
        catch (NumberFormatException e) {
            return Optional.empty();
        }
    }

    private Estimate calculateRowsCount(Map<String, PartitionStatistics> partitionStatisticsMap)
    {
        List<Long> knownPartitionRowCounts = partitionStatisticsMap.values().stream()
                .map(PartitionStatistics::getRowCount)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(toList());

        long knownPartitionRowCountsSum = knownPartitionRowCounts.stream().mapToLong(a -> a).sum();
        long partitionsWithStatsCount = knownPartitionRowCounts.size();
        long allPartitionsCount = partitionStatisticsMap.size();

        if (partitionsWithStatsCount == 0) {
            return Estimate.unknownValue();
        }
        return new Estimate(1.0 * knownPartitionRowCountsSum / partitionsWithStatsCount * allPartitionsCount);
    }

    private Estimate calculateDistinctValuesCount(Map<String, PartitionStatistics> statisticsByPartitionName, String column)
    {
        return getColumnStatisticsValue(
                statisticsByPartitionName,
                column,
                columnStatistics -> {
                    if (columnStatistics.getDistinctValuesCount().isPresent()) {
                        return OptionalDouble.of(columnStatistics.getDistinctValuesCount().getAsLong());
                    }
                    else {
                        return OptionalDouble.empty();
                    }
                },
                DoubleStream::max);
    }

    private Estimate calculateNullsCount(Map<String, PartitionStatistics> statisticsByPartitionName, String column)
    {
        return getColumnStatisticsValue(
                statisticsByPartitionName,
                column,
                columnStatistics -> {
                    if (columnStatistics.getNullsCount().isPresent()) {
                        return OptionalDouble.of(columnStatistics.getNullsCount().getAsLong());
                    }
                    else {
                        return OptionalDouble.empty();
                    }
                },
                doubleStream -> {
                    List<Double> values = doubleStream.mapToObj(Double::valueOf).collect(toList());
                    long partitionsWithStatsCount = values.size();
                    double valuesSum = values.stream().mapToDouble(Double::doubleValue).sum();
                    if (partitionsWithStatsCount == 0) {
                        return OptionalDouble.empty();
                    }
                    else {
                        int allPartitionsCount = statisticsByPartitionName.size();
                        return OptionalDouble.of(allPartitionsCount / partitionsWithStatsCount * valuesSum);
                    }
                });
    }

    private Estimate calculateDistinctValuesCountForPartitioningKey(HiveColumnHandle hiveColumnHandle, List<HivePartition> partitions)
    {
        return new Estimate(partitions.stream()
                .map(partition -> partition.getKeys().get(hiveColumnHandle))
                .collect(toSet())
                .size());
    }

    private Estimate calculateNullsCountForPartitioningKey(HiveColumnHandle hiveColumnHandle, List<HivePartition> hivePartitions, Map<String, PartitionStatistics> partitionStatisticsMap)
    {
        List<Long> knownPartitionRowCounts = partitionStatisticsMap.values().stream()
                .map(PartitionStatistics::getRowCount)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(toList());

        if (knownPartitionRowCounts.isEmpty()) {
            return Estimate.unknownValue();
        }

        long knownPartitionRowCountsSum = knownPartitionRowCounts.stream().mapToLong(Long::longValue).sum();
        long averagePartitionRowsCount = knownPartitionRowCountsSum / knownPartitionRowCounts.size();

        return new Estimate(hivePartitions.stream()
                .filter(partition -> partition.getKeys().get(hiveColumnHandle).isNull())
                .map(HivePartition::getPartitionId)
                .mapToLong(partitionId -> partitionStatisticsMap.get(partitionId).getRowCount().orElse(averagePartitionRowsCount))
                .sum());
    }

    private Estimate getColumnStatisticsValue(
            Map<String, PartitionStatistics> statisticsByPartitionName,
            String column,
            Function<ColumnStatistics, OptionalDouble> valueExtractFunction,
            Function<DoubleStream, OptionalDouble> valueAggregateFunction)
    {
        DoubleStream intermediateStream = statisticsByPartitionName.values().stream()
                .map(PartitionStatistics::getColumnStatistics)
                .filter(columnStatisticsMap -> columnStatisticsMap.containsKey(column))
                .map(columnStatisticsMap -> columnStatisticsMap.get(column))
                .map(valueExtractFunction)
                .filter(OptionalDouble::isPresent)
                .mapToDouble(OptionalDouble::getAsDouble);

        OptionalDouble statisicsValue = valueAggregateFunction.apply(intermediateStream);

        if (statisicsValue.isPresent()) {
            return new Estimate(statisicsValue.getAsDouble());
        }
        else {
            return Estimate.unknownValue();
        }
    }

    private PartitionStatistics getUnpartitionedStatistics(SchemaTableName schemaTableName, Set<String> tableColumns)
    {
        String databaseName = schemaTableName.getSchemaName();
        String tableName = schemaTableName.getTableName();
        Table table = metastore.getTable(databaseName, tableName)
                .orElseThrow(() -> new IllegalArgumentException(format("Could not get metadata for table %s.%s", databaseName, tableName)));

        Map<String, ColumnStatistics> tableColumnStatistics = metastore.getTableColumnStatistics(databaseName, tableName, tableColumns).orElse(ImmutableMap.of());

        return readStatisticsFromParameters(table.getParameters(), tableColumnStatistics);
    }

    private Map<String, PartitionStatistics> getPartitionedStatistics(SchemaTableName schemaTableName, List<HivePartition> hivePartitions, Set<String> tableColumns)
    {
        String databaseName = schemaTableName.getSchemaName();
        String tableName = schemaTableName.getTableName();

        ImmutableMap.Builder<String, PartitionStatistics> resultMap = ImmutableMap.builder();

        List<String> partitionNames = hivePartitions.stream()
                .map(HivePartition::getPartitionId)
                .collect(toList());
        Map<String, Map<String, ColumnStatistics>> partitionColumnStatisticsMap =
                metastore.getPartitionColumnStatistics(databaseName, tableName, new HashSet<>(partitionNames), tableColumns)
                        .orElse(ImmutableMap.of());

        Map<String, Optional<Partition>> partitionsByNames = metastore.getPartitionsByNames(databaseName, tableName, partitionNames);
        for (String partitionName : partitionNames) {
            Map<String, String> partitionParameters = partitionsByNames.get(partitionName)
                    .map(Partition::getParameters)
                    .orElseThrow(() -> new IllegalArgumentException(format("Could not get metadata for partition %s.%s.%s", databaseName, tableName, partitionName)));
            Map<String, ColumnStatistics> partitionColumnStatistics = partitionColumnStatisticsMap.getOrDefault(partitionName, ImmutableMap.of());
            resultMap.put(partitionName, readStatisticsFromParameters(partitionParameters, partitionColumnStatistics));
        }

        return resultMap.build();
    }

    private PartitionStatistics readStatisticsFromParameters(Map<String, String> parameters, Map<String, ColumnStatistics> columnStatistics)
    {
        boolean columnStatsAcurate = Boolean.valueOf(Optional.ofNullable(parameters.get("COLUMN_STATS_ACCURATE")).orElse("false"));
        Optional<Long> numFiles = convertStringParameter(parameters.get("numFiles"));
        Optional<Long> numRows = convertStringParameter(parameters.get("numRows"));
        Optional<Long> rawDataSize = convertStringParameter(parameters.get("rawDataSize"));
        Optional<Long> totalSize = convertStringParameter(parameters.get("totalSize"));
        return new PartitionStatistics(columnStatsAcurate, numFiles, numRows, rawDataSize, totalSize, columnStatistics);
    }

    private ColumnMetadata getColumnMetadata(ColumnHandle columnHandle)
    {
        return ((HiveColumnHandle) columnHandle).getColumnMetadata(typeManager);
    }
}
