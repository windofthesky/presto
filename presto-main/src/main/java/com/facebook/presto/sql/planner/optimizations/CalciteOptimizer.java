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
import com.facebook.presto.sql.planner.optimizations.calcite.CalciteRelConverter;
import com.facebook.presto.sql.planner.optimizations.calcite.PrestoPlanNodeConverter;
import com.facebook.presto.sql.planner.optimizations.calcite.TypeConverter;
import com.facebook.presto.sql.planner.optimizations.calcite.rules.PrestoJoinCommuteRule;
import com.facebook.presto.sql.planner.plan.PlanNode;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rules.JoinAssociateRule;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;

import java.util.Map;

import static com.facebook.presto.sql.planner.plan.calcite.PrestoJoin.PRESTO_JOIN_FACTORY;
import static com.facebook.presto.sql.planner.plan.calcite.PrestoProject.PRESTO_PROJECT_FACTORY;

public class CalciteOptimizer
        implements PlanOptimizer
{
    public static final RelBuilderFactory REL_BUILDER_FACTORY = RelBuilder.proto(PRESTO_JOIN_FACTORY, PRESTO_PROJECT_FACTORY);

    private final Metadata metadata;

    public CalciteOptimizer(Metadata metadata)
    {
        this.metadata = metadata;
    }

    @Override
    public PlanNode optimize(PlanNode plan, Session session, Map<Symbol, Type> types, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator)
    {
        return Frameworks.withPlanner((cluster, relOptSchema, rootSchema) -> {
            TypeConverter typeConverter = new TypeConverter(cluster.getTypeFactory());
            CalciteRelConverter converter = new CalciteRelConverter(typeConverter, types, cluster, relOptSchema, rootSchema, metadata, session);

            RelOptPlanner planner = cluster.getPlanner();
            planner.addRule(new PrestoJoinCommuteRule());
            planner.addRule(JoinAssociateRule.INSTANCE);
            RelNode converted = plan.accept(converter, null);
            planner.setRoot(converted);
            RelNode bestPlan = planner.findBestExp();

            System.out.println(RelOptUtil.toString(bestPlan, SqlExplainLevel.ALL_ATTRIBUTES));

            PrestoPlanNodeConverter unconverter = new PrestoPlanNodeConverter(idAllocator, symbolAllocator, typeConverter);
            bestPlan.accept(unconverter);
            return unconverter.getResult();
        }, Frameworks.newConfigBuilder().build());
    }
}
