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
package com.facebook.presto.sql.planner.iterative.rule;

import com.facebook.presto.Session;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.planner.plan.WindowNode;
import com.facebook.presto.sql.tree.FunctionCall;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.Map;
import java.util.Optional;

public class RemoveEmptyOver
        implements Rule
{
    @Override
    public Optional<PlanNode> apply(PlanNode node, Lookup lookup, PlanNodeIdAllocator idAllocator, SymbolAllocator symbolAllocator, Session session)
    {
        /*
        *  check if instance of windownode
        *  check if over() clause is empty
        *  replace with JoinNode (which is a crossjoin) +  AggregationNode
        * */
        if (!(node instanceof WindowNode)) {
            return Optional.empty();
        }

        WindowNode windowNode = (WindowNode) node;

        if (!(windowNode.getSpecification().isEmpty())) {
            return Optional.empty();
        }

        PlanNode child = (PlanNode) lookup.resolve(windowNode.getSource());
        TableScanNode tableScanNode = (TableScanNode) child;
        TableScanNode tableScanNodeCopy = new TableScanNode(
                idAllocator.getNextId(),
                ((TableScanNode) child).getTable(),
                child.getOutputSymbols(),
                ((TableScanNode) child).getAssignments(),
                ((TableScanNode) child).getLayout(),
                ((TableScanNode) child).getCurrentConstraint(),
                ((TableScanNode) child).getOriginalConstraint());

        ImmutableMap.Builder<Symbol, FunctionCall> aggregationAssignments = ImmutableMap.builder();
        ImmutableMap.Builder<Symbol, Signature> functions = ImmutableMap.builder();

        for (Map.Entry<Symbol, WindowNode.Function> entry : windowNode.getWindowFunctions().entrySet()) {
            WindowNode.Function function = entry.getValue();
            aggregationAssignments.put(entry.getKey(), function.getFunctionCall());
            functions.put(entry.getKey(), function.getSignature());
        }

        AggregationNode aggregationNode = new AggregationNode(
                idAllocator.getNextId(),
                tableScanNodeCopy,
                aggregationAssignments.build(),
                functions.build(),
                ImmutableMap.of(),
                ImmutableList.of(ImmutableList.of()),
                AggregationNode.Step.SINGLE,
                Optional.empty(),
                Optional.empty());

        JoinNode joinNode = new JoinNode(
                idAllocator.getNextId(),
                JoinNode.Type.INNER,
                child,
                aggregationNode,
                ImmutableList.of(),
                ImmutableList.<Symbol>builder()
                .addAll(child.getOutputSymbols())
                .addAll(aggregationNode.getOutputSymbols())
                .build(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty()
                );

        return Optional.ofNullable(joinNode);
    }
}
