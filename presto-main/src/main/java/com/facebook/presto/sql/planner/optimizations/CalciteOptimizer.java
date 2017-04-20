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
package com.facebook.presto.sql.planner.optimizations;

import com.facebook.presto.Session;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.planner.plan.calcite.PrestoRelNode;
import com.facebook.presto.sql.planner.plan.calcite.PrestoTableScan;
import com.facebook.presto.sql.planner.plan.calcite.RelOptPrestoTable;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.tools.Frameworks;

import java.util.Map;

public class CalciteOptimizer
        implements PlanOptimizer
{
    @Override
    public PlanNode optimize(PlanNode plan, Session session, Map<Symbol, Type> types, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator)
    {
        return SimplePlanRewriter.rewriteWith(new Rewriter(), plan);
    }

    private class Rewriter
            extends SimplePlanRewriter<Void>
    {
        @Override
        public PlanNode visitTableScan(TableScanNode node, RewriteContext<Void> context)
        {
            RelNode optimizedOptiqPlan = Frameworks.withPlanner((cluster, relOptSchema, rootSchema) -> {
                RelOptPrestoTable prestoTable = new RelOptPrestoTable(
                        relOptSchema,
                        "dupa",
                        cluster.getTypeFactory().createJavaType(int.class),
                        node.getTable(),
                        ImmutableList.copyOf(node.getAssignments().values()));
                PrestoTableScan scan = new PrestoTableScan(cluster, cluster.traitSetOf(PrestoRelNode.CONVENTION), prestoTable);

                RelOptPlanner planner = cluster.getPlanner();
                planner.setRoot(scan);
                return planner.findBestExp();
            }, Frameworks.newConfigBuilder().build());
            return node;
        }
    }
}
