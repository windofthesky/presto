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

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.plan.Assignments;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.OutputNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.planner.plan.calcite.PrestoJoin;
import com.facebook.presto.sql.planner.plan.calcite.PrestoOutput;
import com.facebook.presto.sql.planner.plan.calcite.PrestoProject;
import com.facebook.presto.sql.planner.plan.calcite.RelOptPrestoTable;
import com.facebook.presto.sql.tree.Expression;
import com.google.common.collect.ImmutableMap;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.TableFunctionScan;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.rel.logical.LogicalExchange;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalIntersect;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalMatch;
import org.apache.calcite.rel.logical.LogicalMinus;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.logical.LogicalValues;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Lists.newArrayList;

public class PrestoPlanNodeConverter
        implements RelShuttle
{
    private final Deque<PlanNode> stack = new ArrayDeque<>();
    private final PlanNodeIdAllocator idAllocator;
    private final SymbolAllocator symbolAllocator;
    private final TypeConverter typeConverter;

    public PrestoPlanNodeConverter(PlanNodeIdAllocator idAllocator, SymbolAllocator symbolAllocator, TypeConverter typeConverter)
    {
        this.idAllocator = idAllocator;
        this.symbolAllocator = symbolAllocator;
        this.typeConverter = typeConverter;
    }

    public PlanNode getResult()
    {
        checkState(stack.size() == 1);
        return stack.pop();
    }

    @Override
    public RelNode visit(TableScan scan)
    {
        RelOptPrestoTable prestoTable = (RelOptPrestoTable) scan.getTable();
        List<Symbol> outputSymbols = getOutputSymbols(scan);
        ImmutableMap.Builder<Symbol, ColumnHandle> assignments = ImmutableMap.builder();
        for (int i = 0; i < outputSymbols.size(); ++i) {
            assignments.put(outputSymbols.get(i), prestoTable.getAssignments().get(i));
        }
        TableScanNode node = new TableScanNode(
                nextId(),
                prestoTable.getTable(),
                outputSymbols,
                assignments.build(),
                Optional.empty(),
                TupleDomain.all(),
                null);
        stack.push(node);
        return null;
    }

    @Override
    public RelNode visit(TableFunctionScan scan)
    {
        return unsupported();
    }

    @Override
    public RelNode visit(LogicalValues values)
    {
        return unsupported();
    }

    @Override
    public RelNode visit(LogicalFilter filter)
    {
        return unsupported();
    }

    @Override
    public RelNode visit(LogicalProject project)
    {
        return unsupported();
    }

    @Override
    public RelNode visit(LogicalJoin join)
    {
        return unsupported();
    }

    @Override
    public RelNode visit(LogicalCorrelate correlate)
    {
        return unsupported();
    }

    @Override
    public RelNode visit(LogicalUnion union)
    {
        return unsupported();
    }

    @Override
    public RelNode visit(LogicalIntersect intersect)
    {
        return unsupported();
    }

    @Override
    public RelNode visit(LogicalMinus minus)
    {
        return unsupported();
    }

    @Override
    public RelNode visit(LogicalAggregate aggregate)
    {
        return unsupported();
    }

    @Override
    public RelNode visit(LogicalMatch match)
    {
        return unsupported();
    }

    @Override
    public RelNode visit(LogicalSort sort)
    {
        return unsupported();
    }

    @Override
    public RelNode visit(LogicalExchange exchange)
    {
        return unsupported();
    }

    @Override
    public RelNode visit(RelNode other)
    {
        if (other instanceof PrestoOutput) {
            PrestoOutput prestoOutput = (PrestoOutput) other;
            visitChildren(other);
            PlanNode child = stack.pop();
            stack.push(new OutputNode(nextId(), child, prestoOutput.getColumnNames(), child.getOutputSymbols()));
            return null;
        }
        else if (other instanceof PrestoProject) {
            PrestoProject prestoProject = (PrestoProject) other;
            visitChildren(other);
            PlanNode child = stack.pop();
            PrestoExpressionConverter expressionConverter = new PrestoExpressionConverter(child.getOutputSymbols());
            List<Expression> expressions = prestoProject.getProjects().stream()
                    .map(rex -> rex.accept(expressionConverter))
                    .collect(toImmutableList());
            List<Symbol> outputSymbols = getOutputSymbols(other);
            Assignments.Builder assignments = Assignments.builder();
            for (int i = 0; i < outputSymbols.size(); ++i) {
                assignments.put(outputSymbols.get(i), expressions.get(i));
            }
            stack.push(new ProjectNode(nextId(), child, assignments.build()));
            return null;
        }
        else if (other instanceof PrestoJoin) {
            PrestoJoin prestoJoin = (PrestoJoin) other;
            visitChildren(prestoJoin);
            PlanNode right = stack.pop();
            PlanNode left = stack.pop();
            List<JoinNode.EquiJoinClause> criteria = Collections.emptyList();
            ArrayList<Symbol> outputSymbols = newArrayList(left.getOutputSymbols());
            outputSymbols.addAll(right.getOutputSymbols());
            Optional<Expression> filter = Optional.empty();
            Optional<Symbol> leftHashSymbol = Optional.empty();
            Optional<Symbol> rightHashSymbol = Optional.empty();
            Optional<JoinNode.DistributionType> empty = Optional.empty();
            JoinNode joinNode = new JoinNode(nextId(), convertType(prestoJoin.getJoinType()), left, right, criteria, outputSymbols, filter, leftHashSymbol, rightHashSymbol, empty);
            stack.push(joinNode);
            return null;
        }
        else {
            unsupported();
        }
        throw new IllegalStateException("This line must not be reached");
    }

    private PlanNodeId nextId()
    {
        return idAllocator.getNextId();
    }

    private JoinNode.Type convertType(JoinRelType joinType)
    {
        return JoinNode.Type.INNER;
    }

    private void visitChildren(RelNode other)
    {
        other.getInputs().forEach(n -> n.accept(this));
    }

    private RelNode unsupported()
    {
        throw new UnsupportedOperationException("Calcite -> Presto conversion not yet implemented");
    }

    private List<Symbol> getOutputSymbols(RelNode node)
    {
        return node.getRowType().getFieldList().stream()
                .map(field -> symbolAllocator.newSymbol(field.getName(), typeConverter.toPrestoType(field.getType())))
                .collect(Collectors.toList());
    }
}
