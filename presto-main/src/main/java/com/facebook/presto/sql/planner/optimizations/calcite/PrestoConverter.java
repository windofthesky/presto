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

import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.plan.OutputNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.calcite.PrestoOutput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
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
import java.util.Deque;

import static com.google.common.base.Preconditions.checkState;

public class PrestoConverter
        implements RelShuttle
{
    private final Deque<PlanNode> stack = new ArrayDeque<>();
    private final PlanNodeIdAllocator idAllocator;

    public PrestoConverter(PlanNodeIdAllocator idAllocator)
    {
        this.idAllocator = idAllocator;
    }

    public PlanNode getResult()
    {
        checkState(stack.size() == 1);
        return stack.pop();
    }

    @Override
    public RelNode visit(TableScan scan)
    {
//        PrestoTableScan prestoTableScan = (PrestoTableScan) scan;
//        TableScanNode node = new TableScanNode(
//                idAllocator.getNextId(),
//                prestoTableScan.
//        );
//        stack.push(node);
        return unsupported();
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
            stack.push(new OutputNode(null, stack.pop(), prestoOutput.getColumnNames(), prestoOutput.getOutputs()));
        }
        else {
            unsupported();
        }
        throw new IllegalStateException("This line must not be reached");
    }

    private void visitChildren(RelNode other)
    {
        other.getInputs().forEach(n -> n.accept(this));
    }

    private RelNode unsupported()
    {
        throw new UnsupportedOperationException("Calcite -> Presto conversion not yet implemented");
    }
}
