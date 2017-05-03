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
import com.facebook.presto.sql.planner.DependencyExtractor;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.Assignments;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanVisitor;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.planner.plan.ValuesNode;
import com.facebook.presto.sql.tree.Expression;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.UnmodifiableIterator;

import java.util.Arrays;
import java.util.Optional;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;

public class PruneUnreferencedOutputs
    implements Rule
{
    private static class RequiredInputSymbols
            extends PlanVisitor<Void, ImmutableList<ImmutableSet<Symbol>>>
    {
        @Override
        public ImmutableList<ImmutableSet<Symbol>> visitPlan(PlanNode node, Void context)
        {
            throw new RuntimeException("Unexpected plan node type " + node.getClass().getName());
        }

        @Override
        public ImmutableList<ImmutableSet<Symbol>> visitProject(ProjectNode node, Void context)
        {
            return ImmutableList.of(ImmutableSet.copyOf(DependencyExtractor.extractUnique(node)));
        }

        @Override
        public ImmutableList<ImmutableSet<Symbol>> visitTableScan(TableScanNode node, Void context)
        {
            return ImmutableList.of();
        }

        @Override
        public ImmutableList<ImmutableSet<Symbol>> visitValues(ValuesNode node, Void context)
        {
            return ImmutableList.of();
        }
    }

    private static class RestrictOutputSymbols extends PlanVisitor<ImmutableSet<Symbol>, PlanNode>
    {
        @Override
        public PlanNode visitPlan(PlanNode node, ImmutableSet<Symbol> requiredSymbols)
        {
            throw new RuntimeException("Unexpected plan node type " + node.getClass().getName());
        }

        @Override
        public PlanNode visitProject(ProjectNode node, ImmutableSet<Symbol> requiredSymbols)
        {
            return new ProjectNode(
                    node.getId(),
                    node.getSource(),
                    node.getAssignments().filter(requiredSymbols));
        }

        @Override
        public PlanNode visitTableScan(TableScanNode node, ImmutableSet<Symbol> requiredSymbols)
        {
            return new TableScanNode(
                    node.getId(),
                    node.getTable(),
                    ImmutableList.copyOf(requiredSymbols),
                    Maps.filterKeys(node.getAssignments(), requiredSymbols::contains),
                    node.getLayout(),
                    node.getCurrentConstraint(),
                    node.getOriginalConstraint());
        }

        @Override
        public PlanNode visitValues(ValuesNode node, ImmutableSet<Symbol> requiredSymbols)
        {
            final int[] projection = IntStream.range(0, node.getOutputSymbols().size())
                    .filter(column -> requiredSymbols.contains(node.getOutputSymbols().get(column)))
                    .toArray();

            return new ValuesNode(
                    node.getId(),
                    Arrays.stream(projection)
                            .mapToObj(node.getOutputSymbols()::get)
                            .collect(toImmutableList()),
                    node.getRows().stream()
                            .map(row -> Arrays.stream(projection)
                                    .mapToObj(row::get)
                                    .collect(toImmutableList()))
                            .collect(toImmutableList()));
        }
    }

    @Override
    public Optional<PlanNode> apply(PlanNode node, Lookup lookup, PlanNodeIdAllocator idAllocator, SymbolAllocator symbolAllocator, Session session)
    {
        final ImmutableList<ImmutableSet<Symbol>> requiredSymbols = node.accept(new RequiredInputSymbols(), null);
        final ImmutableList.Builder<PlanNode> newChildListBuilder = ImmutableList.builder();
        boolean areChildrenModified = false;
        for (int i = 0; i < requiredSymbols.size(); ++i) {
            final PlanNode childRef = node.getSources().get(i);
            if (childRef.getOutputSymbols().stream().allMatch(requiredSymbols.get(i)::contains)) {
                newChildListBuilder.add(childRef);
            } else {
                newChildListBuilder.add(lookup.resolve(childRef).accept(new RestrictOutputSymbols(), requiredSymbols.get(i)));
                areChildrenModified = true;
            }
        }

        if (!areChildrenModified) { return Optional.empty(); }
        return Optional.of(node.replaceChildren(newChildListBuilder.build()));
    }
}
