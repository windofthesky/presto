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
package com.facebook.presto.sql.planner;

import com.facebook.presto.sql.planner.assertions.BasePlanTest;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyTree;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.equiJoinClause;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.join;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.node;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.tableScan;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.INNER;

public class TestCalciteOptimizer
        extends BasePlanTest
{
    @Test
    public void testSimple()
            throws Exception
    {
        assertPlan("SELECT * FROM nation",
                anyTree(
                        tableScan("nation")
                ));
    }

    @Test
    public void testSimpleProject()
            throws Exception
    {
        assertPlan("SELECT nationkey=nationkey FROM nation",
                anyTree(
                        tableScan("nation")
                ));
    }

    @Test
    public void testSimpleJoin()
            throws Exception
    {
        assertPlan("SELECT * FROM nation, region",
                anyTree(
                        join(INNER, ImmutableList.of(),
                                tableScan("nation"),
                                anyTree(
                                        tableScan("region")))));
    }

    @Test
    public void testSimpleJoinWithCondition()
            throws Exception
    {
        assertPlan("SELECT * FROM nation n JOIN region r on n.regionkey = r.regionkey",
                anyTree(
                        join(INNER, ImmutableList.of(equiJoinClause("NATION_RK", "REGION_RK")),
                                anyTree(
                                        tableScan("nation", ImmutableMap.of("NATION_RK", "regionkey"))),
                                anyTree(
                                        tableScan("region", ImmutableMap.of("REGION_RK", "regionkey"))))));
    }

    @Test
    public void testFourWayJoinWithCondition()
            throws Exception
    {
        assertPlan("SELECT * FROM region r" +
                        " JOIN nation n on n.regionkey = r.regionkey" +
                        " JOIN nation n2 on n2.regionkey = r.regionkey" +
                        " JOIN region r2 on n2.regionkey = r2.regionkey" +
                        "",
                anyTree(
                        node(JoinNode.class,
                                /*anyTree*/(
                                        node(JoinNode.class,
                                                anyTree(
                                                        tableScan("region")),
                                                anyTree(
                                                        tableScan("nation")))),
                                anyTree(
                                        node(JoinNode.class,
                                                anyTree(
                                                        tableScan("nation")),
                                                anyTree(
                                                        tableScan("region")))))));
    }
}
