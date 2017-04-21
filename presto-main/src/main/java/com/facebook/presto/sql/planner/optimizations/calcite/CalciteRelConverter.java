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
package com.facebook.presto.sql.planner.optimizations.calcite;

import com.facebook.presto.Session;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.plan.OutputNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanVisitor;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.planner.plan.calcite.PrestoOutput;
import com.facebook.presto.sql.planner.plan.calcite.PrestoProject;
import com.facebook.presto.sql.planner.plan.calcite.PrestoRelNode;
import com.facebook.presto.sql.planner.plan.calcite.PrestoTableScan;
import com.facebook.presto.sql.planner.plan.calcite.RelOptPrestoTable;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.SchemaPlus;

import java.util.List;
import java.util.Map;

import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;

public class CalciteRelConverter
        extends PlanVisitor<Void, RelNode>
{
    private final TypeConverter typeConverter;
    private final Map<Symbol, Type> types;
    private final RelOptCluster cluster;
    private final RelOptSchema relOptSchema;
    private final SchemaPlus rootSchema;
    private final Metadata metadata;
    private final Session session;

    public CalciteRelConverter(TypeConverter typeConverter, Map<Symbol, Type> types, RelOptCluster cluster,
            RelOptSchema relOptSchema, SchemaPlus rootSchema, Metadata metadata, Session session)
    {
        this.typeConverter = typeConverter;
        this.types = types;
        this.cluster = cluster;
        this.relOptSchema = relOptSchema;
        this.rootSchema = rootSchema;
        this.metadata = metadata;
        this.session = session;
    }

    @Override
    protected PrestoRelNode visitPlan(PlanNode node, Void context)
    {
        throw new UnsupportedOperationException("Presto -> Calcite conversion for " + node.getClass().getSimpleName() + " not yet implemented");
    }

    @Override
    public PrestoRelNode visitOutput(OutputNode node, Void context)
    {
        RelNode child = visitChild(node, context);
        return new PrestoOutput(cluster, getTraitSet(), child, node.getColumnNames());
    }

    @Override
    public RelNode visitProject(ProjectNode node, Void context)
    {
        RelNode child = visitChild(node, context);
        CalciteRexConverter converter = new CalciteRexConverter(child.getRowType(), node.getSource().getOutputSymbols(), cluster.getRexBuilder());
        List<RexNode> projections = node.getAssignments().getOutputs().stream()
                .map(symbol -> node.getAssignments().get(symbol))
                .map(expression -> converter.process(expression, null))
                .collect(toImmutableList());
        return new PrestoProject(cluster, getTraitSet(), child, projections, toRowType(node));
    }

    @Override
    public PrestoRelNode visitTableScan(TableScanNode node, Void context)
    {
        List<ColumnHandle> columnHandles = node.getOutputSymbols().stream()
                .map(symbol -> node.getAssignments().get(symbol))
                .collect(toImmutableList());
        RelOptPrestoTable prestoTable = new RelOptPrestoTable(
                relOptSchema,
                getTableName(node, context),
                toRowType(node),
                node.getTable(),
                columnHandles);
        return new PrestoTableScan(cluster, getTraitSet(), prestoTable);
    }

    private RelTraitSet getTraitSet()
    {
        return cluster.traitSetOf(PrestoRelNode.CONVENTION);
    }

    private RelNode visitChild(PlanNode node, Void context)
    {
        return getOnlyElement(node.getSources()).accept(this, context);
    }

    private RelDataType toRowType(PlanNode node)
    {
        List<RelDataType> fieldTypes = node.getOutputSymbols().stream()
                .map(symbol -> typeConverter.toCalciteType(types.get(symbol)))
                .collect(toImmutableList());
        List<String> fieldNames = node.getOutputSymbols().stream()
                .map(Symbol::getName)
                .collect(toImmutableList());
        return typeConverter.getTypeFactory().createStructType(fieldTypes, fieldNames);
    }

    private String getTableName(TableScanNode node, Void context)
    {
        return metadata.getTableMetadata(session, node.getTable()).getTable().getTableName();
    }
}
