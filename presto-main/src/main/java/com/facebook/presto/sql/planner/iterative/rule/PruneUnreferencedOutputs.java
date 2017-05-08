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
import com.facebook.presto.sql.planner.PartitioningScheme;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.Assignments;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.ExplainAnalyzeNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanVisitor;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.planner.plan.ValuesNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;

public class PruneUnreferencedOutputs
        implements Rule
{

    private static ImmutableList<ImmutableSet<Symbol>> requiredJoinChildColumns(JoinNode node, ImmutableSet<Symbol> requiredOutputs)
    {
        ImmutableSet<Symbol> globallyUsable = requiredOutputs;
        if (node.getFilter().isPresent()) {
            globallyUsable = ImmutableSet.<Symbol>builder()
                    .addAll(requiredOutputs)
                    .addAll(DependencyExtractor.extractUnique(node.getFilter().get()))
                    .build();
        }

        ImmutableSet<Symbol> leftUsable = ImmutableSet.<Symbol>builder()
                .addAll(globallyUsable)
                .addAll(node.getCriteria().stream().map(JoinNode.EquiJoinClause::getLeft).iterator())
                .addAll(node.getLeftHashSymbol().map(Stream::of).orElse(Stream.empty()).iterator())
                .build();

        ImmutableSet<Symbol> rightUsable = ImmutableSet.<Symbol>builder()
                .addAll(globallyUsable)
                .addAll(node.getCriteria().stream().map(JoinNode.EquiJoinClause::getRight).iterator())
                .addAll(node.getRightHashSymbol().map(Stream::of).orElse(Stream.empty()).iterator())
                .build();

        return ImmutableList.of(
                node.getLeft().getOutputSymbols().stream()
                        .filter(leftUsable::contains)
                        .collect(toImmutableSet()),
                node.getRight().getOutputSymbols().stream()
                        .filter(rightUsable::contains)
                        .collect(toImmutableSet())
        );
    }

    private static class RequiredInputSymbols
            extends PlanVisitor<Void, ImmutableList<ImmutableSet<Symbol>>>
    {
        @Override
        public ImmutableList<ImmutableSet<Symbol>> visitPlan(PlanNode node, Void context)
        {
            throw new RuntimeException("Unexpected plan node type " + node.getClass().getName());
        }

        @Override
        public ImmutableList<ImmutableSet<Symbol>> visitExplainAnalyze(ExplainAnalyzeNode node, Void context)
        {
            return ImmutableList.of(ImmutableSet.copyOf(node.getSource().getOutputSymbols()));
        }

        @Override
        public ImmutableList<ImmutableSet<Symbol>> visitExchange(ExchangeNode node, Void context)
        {
            return node.getInputs().stream()
                    .map(ImmutableSet::copyOf)
                    .collect(toImmutableList());
        }

        @Override
        public ImmutableList<ImmutableSet<Symbol>> visitJoin(JoinNode node, Void context)
        {
            return requiredJoinChildColumns(node, ImmutableSet.copyOf(node.getOutputSymbols()));
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

    private static class RestrictOutputSymbols
            extends PlanVisitor<ImmutableSet<Symbol>, Optional<PlanNode>>
    {
        private PlanNodeIdAllocator idAllocator;

        private RestrictOutputSymbols(PlanNodeIdAllocator idAllocator)
        {
            this.idAllocator = idAllocator;
        }

        @Override
        public Optional<PlanNode> visitPlan(PlanNode node, ImmutableSet<Symbol> requiredSymbols)
        {
            throw new RuntimeException("Unexpected plan node type " + node.getClass().getName());
        }

        @Override
        public Optional<PlanNode> visitExplainAnalyze(ExplainAnalyzeNode node, ImmutableSet<Symbol> requiredSymbols)
        {
            return Optional.empty();
        }

        @Override
        public Optional<PlanNode> visitExchange(ExchangeNode node, ImmutableSet<Symbol> requiredSymbols)
        {
            Set<Symbol> expectedOutputSymbols = Sets.newHashSet(requiredSymbols);
            node.getPartitioningScheme().getHashColumn().ifPresent(expectedOutputSymbols::add);
            node.getPartitioningScheme().getPartitioning().getColumns().stream()
                    .forEach(expectedOutputSymbols::add);

            List<List<Symbol>> inputsBySource = new ArrayList<>(node.getInputs().size());
            for (int i = 0; i < node.getInputs().size(); i++) {
                inputsBySource.add(new ArrayList<>());
            }

            List<Symbol> newOutputSymbols = new ArrayList<>(node.getOutputSymbols().size());
            for (int i = 0; i < node.getOutputSymbols().size(); i++) {
                Symbol outputSymbol = node.getOutputSymbols().get(i);
                if (expectedOutputSymbols.contains(outputSymbol)) {
                    newOutputSymbols.add(outputSymbol);
                    for (int source = 0; source < node.getInputs().size(); source++) {
                        inputsBySource.get(source).add(node.getInputs().get(source).get(i));
                    }
                }
            }

            if (ImmutableList.copyOf(newOutputSymbols).size() >= ImmutableList.copyOf(node.getOutputSymbols()).size()) {
                return Optional.empty();
            }

            // newOutputSymbols contains all partition and hash symbols so simply swap the output layout
            PartitioningScheme partitioningScheme = new PartitioningScheme(
                    node.getPartitioningScheme().getPartitioning(),
                    newOutputSymbols,
                    node.getPartitioningScheme().getHashColumn(),
                    node.getPartitioningScheme().isReplicateNulls(),
                    node.getPartitioningScheme().getBucketToPartition());

            return Optional.of(new ExchangeNode(
                    node.getId(),
                    node.getType(),
                    node.getScope(),
                    partitioningScheme,
                    node.getSources(),
                    inputsBySource));
        }

        @Override
        public Optional<PlanNode> visitJoin(JoinNode node, ImmutableSet<Symbol> requiredSymbols)
        {
            List<PlanNode> newChildren;

            if (node.isCrossJoin()) {
                // TODO: remove this "if" branch when output symbols selection is supported by nested loop join
                ImmutableList<ImmutableSet<Symbol>> childrenInputs = requiredJoinChildColumns(node, requiredSymbols);
                ImmutableList.Builder<PlanNode> newChildrenBuilder = ImmutableList.builder();
                for (int childIndex = 0; childIndex < 2; ++childIndex) {
                    PlanNode oldChild = node.getSources().get(childIndex);
                    if (oldChild.getOutputSymbols().stream().allMatch(childrenInputs.get(childIndex)::contains)) {
                        newChildrenBuilder.add(oldChild);
                    }
                    else {
                        newChildrenBuilder.add(new ProjectNode(
                                idAllocator.getNextId(),
                                oldChild,
                                Assignments.identity(childrenInputs.get(childIndex))));
                    }
                }
                newChildren = newChildrenBuilder.build();
            }
            else {
                newChildren = node.getSources();
            }

            return Optional.of(new JoinNode(
                    node.getId(),
                    node.getType(),
                    newChildren.get(0),
                    newChildren.get(1),
                    node.getCriteria(),
                    node.getOutputSymbols().stream().filter(requiredSymbols::contains).collect(toImmutableList()),
                    node.getFilter(),
                    node.getLeftHashSymbol(),
                    node.getRightHashSymbol(),
                    node.getDistributionType()));
        }

        @Override
        public Optional<PlanNode> visitProject(ProjectNode node, ImmutableSet<Symbol> requiredSymbols)
        {
            // TODO:  notice if the projection is now a noop, and return its source instead?
            return Optional.of(new ProjectNode(
                    node.getId(),
                    node.getSource(),
                    node.getAssignments().filter(requiredSymbols)));
        }

        @Override
        public Optional<PlanNode> visitTableScan(TableScanNode node, ImmutableSet<Symbol> requiredSymbols)
        {
            return Optional.of(new TableScanNode(
                    node.getId(),
                    node.getTable(),
                    ImmutableList.copyOf(requiredSymbols),
                    Maps.filterKeys(node.getAssignments(), requiredSymbols::contains),
                    node.getLayout(),
                    node.getCurrentConstraint(),
                    node.getOriginalConstraint()));
        }

        @Override
        public Optional<PlanNode> visitValues(ValuesNode node, ImmutableSet<Symbol> requiredSymbols)
        {
            ImmutableList<Integer> requiredColumnNumbers = IntStream.range(0, node.getOutputSymbols().size())
                    .filter(columnNumber -> requiredSymbols.contains(node.getOutputSymbols().get(columnNumber)))
                    .boxed()
                    .collect(toImmutableList());

            return Optional.of(new ValuesNode(
                    node.getId(),
                    requiredColumnNumbers.stream()
                            .map(node.getOutputSymbols()::get)
                            .collect(toImmutableList()),
                    node.getRows().stream()
                            .map(row -> requiredColumnNumbers.stream()
                                    .map(row::get)
                                    .collect(toImmutableList()))
                            .collect(toImmutableList())));
        }
    }

    @Override
    public Optional<PlanNode> apply(PlanNode node, Lookup lookup, PlanNodeIdAllocator idAllocator, SymbolAllocator symbolAllocator, Session session)
    {
        ImmutableList<ImmutableSet<Symbol>> requiredSymbols = node.accept(new RequiredInputSymbols(), null);
        ImmutableList.Builder<PlanNode> newChildListBuilder = ImmutableList.builder();
        boolean areChildrenModified = false;
        for (int i = 0; i < requiredSymbols.size(); ++i) {
            PlanNode childRef = node.getSources().get(i);
            if (!childRef.getOutputSymbols().stream().allMatch(requiredSymbols.get(i)::contains)) {
                Optional<PlanNode> newSource = lookup.resolve(childRef).accept(new RestrictOutputSymbols(idAllocator), requiredSymbols.get(i));
                if (newSource.isPresent()) {
                    newChildListBuilder.add(newSource.get());
                    areChildrenModified = true;
                    continue;
                }
            }
            newChildListBuilder.add(childRef);
        }

        if (!areChildrenModified) {
            return Optional.empty();
        }
        return Optional.of(node.replaceChildren(newChildListBuilder.build()));
    }
}
