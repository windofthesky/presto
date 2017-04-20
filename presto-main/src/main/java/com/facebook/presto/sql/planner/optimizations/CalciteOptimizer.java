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
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.optimizations.calcite.CalciteConverter;
import com.facebook.presto.sql.planner.optimizations.calcite.PrestoConverter;
import com.facebook.presto.sql.planner.plan.PlanNode;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.tools.Frameworks;

import java.util.Map;

public class CalciteOptimizer
        implements PlanOptimizer
{
    private final Metadata metadata;

    public CalciteOptimizer(Metadata metadata)
    {
        this.metadata = metadata;
    }

    @Override
    public PlanNode optimize(PlanNode plan, Session session, Map<Symbol, Type> types, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator)
    {
        CalciteConverter converter = new CalciteConverter();

        RelNode rewrittenCalcitePlan = Frameworks.withPlanner((cluster, relOptSchema, rootSchema) -> {
            RelOptPlanner planner = cluster.getPlanner();
            CalciteConverter.Context context = new CalciteConverter.Context(cluster, relOptSchema, rootSchema, metadata, session);
            RelNode converted = plan.accept(converter, context);
            planner.setRoot(converted);
            return planner.findBestExp();
        }, Frameworks.newConfigBuilder().build());

        PrestoConverter unconverter = new PrestoConverter();
        rewrittenCalcitePlan.accept(unconverter);
        return unconverter.getResult();
    }
}
