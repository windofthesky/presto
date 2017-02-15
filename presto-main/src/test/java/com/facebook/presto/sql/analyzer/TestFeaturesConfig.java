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
package com.facebook.presto.sql.analyzer;

import com.google.common.collect.ImmutableMap;
import io.airlift.configuration.testing.ConfigAssertions;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.util.Map;

import static com.facebook.presto.sql.analyzer.RegexLibrary.JONI;
import static com.facebook.presto.sql.analyzer.RegexLibrary.RE2J;
import static io.airlift.configuration.testing.ConfigAssertions.assertDeprecatedEquivalence;
import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

public class TestFeaturesConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(ConfigAssertions.recordDefaults(FeaturesConfig.class)
                .setCpuCostWeight(0.75)
                .setMemoryCostWeight(0)
                .setNetworkCostWeight(0.25)
                .setResourceGroupsEnabled(false)
                .setDistributedIndexJoinsEnabled(false)
                .setFastInequalityJoins(true)
                .setJoinDistributionType("repartitioned")
                .setColocatedJoinsEnabled(false)
                .setJoinReorderingEnabled(true)
                .setRedistributeWrites(true)
                .setOptimizeMetadataQueries(false)
                .setOptimizeHashGeneration(true)
                .setOptimizeSingleDistinct(true)
                .setPushTableWriteThroughUnion(true)
                .setDictionaryAggregation(false)
                .setLegacyArrayAgg(false)
                .setLegacyMapSubscript(false)
                .setRegexLibrary(JONI)
                .setRe2JDfaStatesLimit(Integer.MAX_VALUE)
                .setRe2JDfaRetries(5)
                .setSpillEnabled(false)
                .setOperatorMemoryLimitBeforeSpill(DataSize.valueOf("4MB"))
                .setSpillerSpillPaths("")
                .setSpillerThreads(4)
                .setSpillMaxUsedSpaceThreshold(0.9)
                .setOptimizeMixedDistinctAggregations(false)
                .setLegacyOrderBy(false)
                .setIterativeOptimizerEnabled(true)
                .setLegacyTimestamp(false)
                .setIterativeOptimizerTimeout(new Duration(3, MINUTES))
                .setExchangeCompressionEnabled(false)
                .setEnableIntermediateAggregations(false)
                .setMemoryRevokingThreshold(0.9)
                .setMemoryRevokingTarget(0.5)
                .setParseDecimalLiteralsAsDouble(false)
                .setEnableIntermediateAggregations(false)
                .setPushAggregationThroughJoin(true)
                .setSmallTableCoefficient(0.01));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> propertiesLegacy = new ImmutableMap.Builder<String, String>()
                .put("cpu-cost-weight", "0.4")
                .put("memory-cost-weight", "0.3")
                .put("network-cost-weight", "0.2")
                .put("experimental.resource-groups-enabled", "true")
                .put("experimental.iterative-optimizer-enabled", "false")
                .put("experimental.iterative-optimizer-timeout", "10s")
                .put("deprecated.legacy-array-agg", "true")
                .put("deprecated.legacy-order-by", "true")
                .put("deprecated.legacy-map-subscript", "true")
                .put("distributed-index-joins-enabled", "true")
                .put("fast-inequality-joins", "false")
                .put("join-distribution-type", "replicated")
                .put("colocated-joins-enabled", "true")
                .put("reorder-joins", "false")
                .put("redistribute-writes", "false")
                .put("optimizer.optimize-metadata-queries", "true")
                .put("optimizer.optimize-hash-generation", "false")
                .put("optimizer.optimize-single-distinct", "false")
                .put("optimizer.optimize-mixed-distinct-aggregations", "true")
                .put("optimizer.push-table-write-through-union", "false")
                .put("optimizer.dictionary-aggregation", "true")
                .put("optimizer.push-aggregation-through-join", "false")
                .put("regex-library", "RE2J")
                .put("re2j.dfa-states-limit", "42")
                .put("re2j.dfa-retries", "42")
                .put("experimental.spill-enabled", "true")
                .put("experimental.operator-memory-limit-before-spill", "100MB")
                .put("experimental.spiller-spill-path", "/tmp/custom/spill/path1,/tmp/custom/spill/path2")
                .put("experimental.spiller-threads", "42")
                .put("experimental.spiller-max-used-space-threshold", "0.8")
                .put("experimental.memory-revoking-threshold", "0.2")
                .put("experimental.memory-revoking-target", "0.8")
                .put("exchange.compression-enabled", "true")
                .put("deprecated.legacy-timestamp", "true")
                .put("small-table-coefficient", "0.2")
                .put("deprecated.parse-decimal-literals-as-double", "true")
                .put("deprecated.legacy-timestamp", "false")
                .put("optimizer.enable-intermediate-aggregations", "true")
                .put("deprecated.parse-decimal-literals-as-double", "true")
                .put("small-table-coefficient", "0.2")
                .build();
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("cpu-cost-weight", "0.4")
                .put("memory-cost-weight", "0.3")
                .put("network-cost-weight", "0.2")
                .put("experimental.resource-groups-enabled", "true")
                .put("experimental.iterative-optimizer-enabled", "false")
                .put("experimental.iterative-optimizer-timeout", "10s")
                .put("deprecated.legacy-array-agg", "true")
                .put("deprecated.legacy-order-by", "true")
                .put("deprecated.legacy-map-subscript", "true")
                .put("distributed-index-joins-enabled", "true")
                .put("fast-inequality-joins", "false")
                .put("join-distribution-type", "replicated")
                .put("colocated-joins-enabled", "true")
                .put("reorder-joins", "false")
                .put("redistribute-writes", "false")
                .put("optimizer.optimize-metadata-queries", "true")
                .put("optimizer.optimize-hash-generation", "false")
                .put("optimizer.optimize-single-distinct", "false")
                .put("optimizer.optimize-mixed-distinct-aggregations", "true")
                .put("optimizer.push-table-write-through-union", "false")
                .put("optimizer.dictionary-aggregation", "true")
                .put("optimizer.push-aggregation-through-join", "false")
                .put("regex-library", "RE2J")
                .put("re2j.dfa-states-limit", "42")
                .put("re2j.dfa-retries", "42")
                .put("experimental.spill-enabled", "true")
                .put("experimental.operator-memory-limit-before-spill", "100MB")
                .put("experimental.spiller-spill-path", "/tmp/custom/spill/path1,/tmp/custom/spill/path2")
                .put("experimental.spiller-threads", "42")
                .put("experimental.spiller-max-used-space-threshold", "0.8")
                .put("experimental.memory-revoking-threshold", "0.2")
                .put("experimental.memory-revoking-target", "0.8")
                .put("exchange.compression-enabled", "true")
                .put("deprecated.legacy-timestamp", "true")
                .put("optimizer.enable-intermediate-aggregations", "true")
                .put("deprecated.parse-decimal-literals-as-double", "true")
                .put("small-table-coefficient", "0.2")
                .build();

        FeaturesConfig expected = new FeaturesConfig()
                .setCpuCostWeight(0.4)
                .setMemoryCostWeight(0.3)
                .setNetworkCostWeight(0.2)
                .setResourceGroupsEnabled(true)
                .setIterativeOptimizerEnabled(false)
                .setIterativeOptimizerTimeout(new Duration(10, SECONDS))
                .setDistributedIndexJoinsEnabled(true)
                .setFastInequalityJoins(false)
                .setJoinDistributionType("replicated")
                .setColocatedJoinsEnabled(true)
                .setJoinReorderingEnabled(false)
                .setRedistributeWrites(false)
                .setOptimizeMetadataQueries(true)
                .setOptimizeHashGeneration(false)
                .setOptimizeSingleDistinct(false)
                .setOptimizeMixedDistinctAggregations(true)
                .setPushTableWriteThroughUnion(false)
                .setDictionaryAggregation(true)
                .setPushAggregationThroughJoin(false)
                .setLegacyArrayAgg(true)
                .setLegacyMapSubscript(true)
                .setRegexLibrary(RE2J)
                .setRe2JDfaStatesLimit(42)
                .setRe2JDfaRetries(42)
                .setSpillEnabled(true)
                .setOperatorMemoryLimitBeforeSpill(DataSize.valueOf("100MB"))
                .setSpillerSpillPaths("/tmp/custom/spill/path1,/tmp/custom/spill/path2")
                .setSpillerThreads(42)
                .setSpillMaxUsedSpaceThreshold(0.8)
                .setLegacyOrderBy(true)
                .setExchangeCompressionEnabled(true)
                .setEnableIntermediateAggregations(true)
                .setLegacyTimestamp(true)
                .setMemoryRevokingThreshold(0.2)
                .setMemoryRevokingTarget(0.8)
                .setParseDecimalLiteralsAsDouble(true)
                .setSmallTableCoefficient(0.2);

        assertFullMapping(properties, expected);
        assertDeprecatedEquivalence(FeaturesConfig.class, properties, propertiesLegacy);
    }
}
