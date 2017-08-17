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
package com.facebook.presto.tpcds.statistics;

import com.facebook.presto.Session;
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.tests.statistics.StatisticsAssertion;
import com.facebook.presto.tpcds.TpcdsConnectorFactory;
import org.testng.annotations.Test;

import static com.facebook.presto.SystemSessionProperties.USE_NEW_STATS_CALCULATOR;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.tests.statistics.MetricComparisonStrategies.absoluteError;
import static com.facebook.presto.tests.statistics.MetricComparisonStrategies.defaultTolerance;
import static com.facebook.presto.tests.statistics.MetricComparisonStrategies.noError;
import static com.facebook.presto.tests.statistics.MetricComparisonStrategies.relativeError;
import static com.facebook.presto.tests.statistics.Metrics.OUTPUT_ROW_COUNT;
import static com.facebook.presto.tests.statistics.Metrics.distinctValuesCount;
import static java.util.Collections.emptyMap;

public class TestTpcdsLocalStats
{
    private final StatisticsAssertion statisticsAssertion;

    public TestTpcdsLocalStats()
            throws Exception
    {
        Session defaultSession = testSessionBuilder()
                .setCatalog("tpcds")
                .setSchema("sf1") // TODO decision to make (slower vs less practical). Also "tiny" doesn't work now
//                .setSystemProperty(PUSH_PARTIAL_AGGREGATION_THROUGH_JOIN, "true") TODO why is this set in tpch's test??
                .setSystemProperty(USE_NEW_STATS_CALCULATOR, "true")
                .build();

        LocalQueryRunner runner = new LocalQueryRunner(defaultSession);
        runner.createCatalog("tpcds", new TpcdsConnectorFactory(), emptyMap());
        statisticsAssertion = new StatisticsAssertion(runner);
    }

    @Test
    public void testTableScanStats()
    {
        statisticsAssertion.check("SELECT * FROM item",
                checks -> checks
                        .estimate(OUTPUT_ROW_COUNT, defaultTolerance())
                        .verifyExactColumnStatistics("i_item_sk")
                        .verifyCharacterColumnStatistics("i_item_id", noError())
                        .verifyColumnStatistics("i_brand_id", absoluteError(0.01)) // nulls fraction estimated as 0
                        .verifyCharacterColumnStatistics("i_color", absoluteError(0.01)) // nulls fraction estimated as 0
        );
    }

    @Test
    public void testCharComparison()
    {
        // cd_marital_status is char(1)
        statisticsAssertion.check("SELECT * FROM customer_demographics WHERE cd_marital_status = 'D'",
                checks -> checks
                        .estimate(OUTPUT_ROW_COUNT, defaultTolerance())
                        .estimate(distinctValuesCount("cd_marital_status"), noError()));

        // i_category is char(50)
        statisticsAssertion.check("SELECT * FROM item WHERE i_category = 'Women                                             '",
                checks -> checks.estimate(OUTPUT_ROW_COUNT, defaultTolerance()));
        statisticsAssertion.check("SELECT * FROM item WHERE i_category = cast('Women' as char(50))",
                checks -> checks.estimate(OUTPUT_ROW_COUNT, defaultTolerance()));
    }

    @Test
    public void testDecimalComparison()
    {
        // ca_gmt_offset is decimal(5,2)
        statisticsAssertion.check("SELECT * FROM customer_address WHERE ca_gmt_offset = -7",
                checks -> checks
                        .estimate(OUTPUT_ROW_COUNT, relativeError(.6)) // distribution is non-uniform
                        .estimate(distinctValuesCount("ca_gmt_offset"), noError()));

        // p_cost is decimal(15,2)
        statisticsAssertion.check("SELECT * FROM promotion WHERE p_cost < 1", // p_cost is always 1000.00, so no rows should be left
                checks -> checks.estimate(OUTPUT_ROW_COUNT, noError()));
    }
}
