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
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.sql.planner.DependencyExtractor;
import com.facebook.presto.sql.planner.PartitioningScheme;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.Assignments;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.ExplainAnalyzeNode;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.IndexJoinNode;
import com.facebook.presto.sql.planner.plan.IndexSourceNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.MarkDistinctNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanVisitor;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.SemiJoinNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.planner.plan.ValuesNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.collect.Streams;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;

public class PruneUnreferencedOutputs
        implements Rule
{

    private static ImmutableList<ImmutableSet<Symbol>> requiredJoinChildrenSymbols(JoinNode node, ImmutableSet<Symbol> requiredOutputs)
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

    private static ImmutableSet<Symbol> requiredSemiJoinSourceSymbols(SemiJoinNode node, ImmutableSet<Symbol> requiredOutputs)
    {
        return ImmutableSet.<Symbol>builder()
                .addAll(requiredOutputs.stream()
                        .filter(symbol -> !symbol.equals(node.getSemiJoinOutput())).iterator())
                .add(node.getSourceJoinSymbol())
                .addAll(node.getSourceHashSymbol().map(Stream::of).orElse(Stream.empty()).iterator())
                .build();
    }

    // TODO for the non-output-symbol-selecting nodes, don't factor the required symbols this way, and instead just default to obviously requiring all the source symbols.
    private static ImmutableList<ImmutableSet<Symbol>> requiredIndexJoinChildrenSymbols(IndexJoinNode node, ImmutableSet<Symbol> requiredOutputs)
    {
        ImmutableSet<Symbol> probeUsable = ImmutableSet.<Symbol>builder()
                .addAll(requiredOutputs)
                .addAll(node.getCriteria().stream().map(IndexJoinNode.EquiJoinClause::getProbe).iterator())
                .addAll(node.getProbeHashSymbol().map(Stream::of).orElse(Stream.empty()).iterator())
                .build();

        ImmutableSet<Symbol> indexUsable = ImmutableSet.<Symbol>builder()
                .addAll(requiredOutputs)
                .addAll(node.getCriteria().stream().map(IndexJoinNode.EquiJoinClause::getIndex).iterator())
                .addAll(node.getIndexHashSymbol().map(Stream::of).orElse(Stream.empty()).iterator())
                .build();

        return ImmutableList.of(
                node.getProbeSource().getOutputSymbols().stream()
                        .filter(probeUsable::contains)
                        .collect(toImmutableSet()),
                node.getIndexSource().getOutputSymbols().stream()
                        .filter(indexUsable::contains)
                        .collect(toImmutableSet())
        );
    }

    private static ImmutableSet<Symbol> requiredMarkDistinctSourceSymbols(MarkDistinctNode node, ImmutableSet<Symbol> requiredOutputs)
    {
        return ImmutableSet.<Symbol>builder()
                .addAll(requiredOutputs.stream()
                        .filter(symbol -> !symbol.equals(node.getMarkerSymbol())).iterator())
                .addAll(node.getDistinctSymbols())
                .addAll(node.getHashSymbol().map(Stream::of).orElse(Stream.empty()).iterator())
                .build();
    }

    private static class RequiredInputSymbols
            extends PlanVisitor<Void, ImmutableList<ImmutableSet<Symbol>>>
    {
        private static ImmutableList<ImmutableSet<Symbol>> allChildSymbols(PlanNode node)
        {
            return node.getSources().stream()
                    .map(source -> ImmutableSet.copyOf(source.getOutputSymbols()))
                    .collect(toImmutableList());
        }

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
            return requiredJoinChildrenSymbols(node, ImmutableSet.copyOf(node.getOutputSymbols()));
        }

        @Override
        public ImmutableList<ImmutableSet<Symbol>> visitSemiJoin(SemiJoinNode node, Void context)
        {
            return ImmutableList.of(
                    requiredSemiJoinSourceSymbols(node, ImmutableSet.copyOf(node.getOutputSymbols())),
                    ImmutableSet.<Symbol>builder()
                            .add(node.getFilteringSourceJoinSymbol())
                            .addAll(node.getFilteringSourceHashSymbol().map(Stream::of).orElse(Stream.empty()).iterator())
                            .build()
            );
        }

        @Override
        public ImmutableList<ImmutableSet<Symbol>> visitIndexJoin(IndexJoinNode node, Void context)
        {
            return requiredIndexJoinChildrenSymbols(node, ImmutableSet.copyOf(node.getOutputSymbols()));
        }

        @Override
        public ImmutableList<ImmutableSet<Symbol>> visitIndexSource(IndexSourceNode node, Void context)
        {
            return ImmutableList.of();
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


        @Override
        public ImmutableList<ImmutableSet<Symbol>> visitAggregation(AggregationNode node, Void context)
        {
            /*
            return ImmutableList.of(
                ImmutableSet.<Symbol>builder()
                        .addAll(node.getGroupingKeys())
                        .addAll(node.getHashSymbol().map(Stream::of).orElse(Stream.empty()).iterator())
                        .addAll(node.getAssignments().values().stream()
                                .flatMap(aggregation -> Streams.concat(
                                        DependencyExtractor.extractUnique(aggregation.getCall()).stream(),
                                        aggregation.getMask().map(Stream::of).orElse(Stream.empty())))
                                .iterator())
                        .build());
                        */

            return ImmutableList.of(
                    Streams.concat(
                            node.getGroupingKeys().stream(),
                            node.getHashSymbol().map(Stream::of).orElse(Stream.empty()),
                            node.getAssignments().values().stream()
                                    .flatMap(aggregation -> Streams.concat(
                                            DependencyExtractor.extractUnique(aggregation.getCall()).stream(),
                                            aggregation.getMask().map(Stream::of).orElse(Stream.empty()))))
                            .collect(toImmutableSet()));
        }

        @Override
        public ImmutableList<ImmutableSet<Symbol>> visitMarkDistinct(MarkDistinctNode node, Void context)
        {
            return ImmutableList.of(requiredMarkDistinctSourceSymbols(node, ImmutableSet.copyOf(node.getOutputSymbols())));
        }

        @Override
        public ImmutableList<ImmutableSet<Symbol>> visitFilter(FilterNode node, Void context)
        {
            return allChildSymbols(node);
        }
    }

    private static class RestrictOutputSymbols
            extends PlanVisitor<ImmutableSet<Symbol>, PlanNode>
    {
        private PlanNodeIdAllocator idAllocator;

        private RestrictOutputSymbols(PlanNodeIdAllocator idAllocator)
        {
            this.idAllocator = idAllocator;
        }

        @Override
        public PlanNode visitPlan(PlanNode node, ImmutableSet<Symbol> requiredSymbols)
        {
            throw new RuntimeException("Unexpected plan node type " + node.getClass().getName());
        }

        @Override
        public PlanNode visitExplainAnalyze(ExplainAnalyzeNode node, ImmutableSet<Symbol> requiredSymbols)
        {
            return node;
        }

        @Override
        public PlanNode visitExchange(ExchangeNode node, ImmutableSet<Symbol> requiredSymbols)
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

            // newOutputSymbols contains all partition and hash symbols so simply swap the output layout
            PartitioningScheme partitioningScheme = new PartitioningScheme(
                    node.getPartitioningScheme().getPartitioning(),
                    newOutputSymbols,
                    node.getPartitioningScheme().getHashColumn(),
                    node.getPartitioningScheme().isReplicateNulls(),
                    node.getPartitioningScheme().getBucketToPartition());

            return new ExchangeNode(
                    node.getId(),
                    node.getType(),
                    node.getScope(),
                    partitioningScheme,
                    node.getSources(),
                    inputsBySource);
        }

        private PlanNode restrictSymbols(PlanNode node, ImmutableSet<Symbol> targetSymbols)
        {
            if (node.getOutputSymbols().stream().allMatch(targetSymbols::contains)) {
                return node;
            }
            else {
                return new ProjectNode(idAllocator.getNextId(), node, Assignments.identity(targetSymbols));
            }
        }

        // TODO: replace with Streams.zip in guava 22
        private static <A, B, R> ImmutableList<R> zipLists(List<A> listA, List<B> listB, BiFunction<A, B, R> mapping)
        {
            Iterator<A> iteratorA = listA.iterator();
            Iterator<B> iteratorB = listB.iterator();
            ImmutableList.Builder<R> resultBuilder = ImmutableList.builder();
            while (iteratorA.hasNext() && iteratorB.hasNext())
            {
                resultBuilder.add(mapping.apply(iteratorA.next(), iteratorB.next()));
            }
            return resultBuilder.build();
        }

        @Override
        public PlanNode visitJoin(JoinNode node, ImmutableSet<Symbol> requiredSymbols)
        {
            List<PlanNode> newChildren;

            if (node.isCrossJoin()) {
                // TODO: remove this "if" branch when output symbols selection is supported by nested loop join
                ImmutableList<ImmutableSet<Symbol>> childrenInputs = requiredJoinChildrenSymbols(node, requiredSymbols);
                newChildren = zipLists(node.getSources(), childrenInputs, this::restrictSymbols);
            }
            else {
                newChildren = node.getSources();
            }

            return new JoinNode(
                    node.getId(),
                    node.getType(),
                    newChildren.get(0),
                    newChildren.get(1),
                    node.getCriteria(),
                    node.getOutputSymbols().stream().filter(requiredSymbols::contains).collect(toImmutableList()),
                    node.getFilter(),
                    node.getLeftHashSymbol(),
                    node.getRightHashSymbol(),
                    node.getDistributionType());
        }

        @Override
        public PlanNode visitSemiJoin(SemiJoinNode node, ImmutableSet<Symbol> requiredSymbols)
        {
            if (!requiredSymbols.contains(node.getSemiJoinOutput())) {
                return restrictSymbols(node.getSource(), requiredSymbols);
            }

            // SemiJoinNode doesn't support output symbols selection, so we need to insert a project over the source
            ImmutableSet<Symbol> requiredSourceSymbols = requiredSemiJoinSourceSymbols(node, requiredSymbols);

            PlanNode newSourceNode = restrictSymbols(node.getSource(), requiredSourceSymbols);
            return node.replaceChildren(ImmutableList.of(newSourceNode, node.getFilteringSource()));
        }

        @Override
        public PlanNode visitIndexJoin(IndexJoinNode node, ImmutableSet<Symbol> requiredSymbols)
        {
            // IndexJoinNode doesn't support output symbols selection, so we need to insert projects over the sources
            ImmutableList<ImmutableSet<Symbol>> childrenInputs = requiredIndexJoinChildrenSymbols(node, requiredSymbols);
            return node.replaceChildren(zipLists(node.getSources(), childrenInputs, this::restrictSymbols));
        }

        @Override
        public PlanNode visitIndexSource(IndexSourceNode node, ImmutableSet<Symbol> requiredSymbols)
        {
            List<Symbol> newOutputSymbols = node.getOutputSymbols().stream()
                    .filter(requiredSymbols::contains)
                    .collect(toImmutableList());

            Set<Symbol> newLookupSymbols = node.getLookupSymbols().stream()
                    .filter(requiredSymbols::contains)
                    .collect(toImmutableSet());

            Map<Symbol, ColumnHandle> newAssignments = Maps.filterEntries(
                    node.getAssignments(),
                    entry -> requiredSymbols.contains(entry.getKey()) ||
                            node.getEffectiveTupleDomain().getDomains()
                                    .map(domains -> domains.containsKey(entry.getValue()))
                                    .orElse(false));

            return new IndexSourceNode(
                    node.getId(),
                    node.getIndexHandle(),
                    node.getTableHandle(),
                    node.getLayout(),
                    newLookupSymbols,
                    newOutputSymbols,
                    newAssignments,
                    node.getEffectiveTupleDomain());
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
            ImmutableList<Integer> requiredColumnNumbers = IntStream.range(0, node.getOutputSymbols().size())
                    .filter(columnNumber -> requiredSymbols.contains(node.getOutputSymbols().get(columnNumber)))
                    .boxed()
                    .collect(toImmutableList());

            return new ValuesNode(
                    node.getId(),
                    requiredColumnNumbers.stream()
                            .map(node.getOutputSymbols()::get)
                            .collect(toImmutableList()),
                    node.getRows().stream()
                            .map(row -> requiredColumnNumbers.stream()
                                    .map(row::get)
                                    .collect(toImmutableList()))
                            .collect(toImmutableList()));
        }

        @Override
        public PlanNode visitAggregation(AggregationNode node, ImmutableSet<Symbol> requiredSymbols)
        {
            return new AggregationNode(
                    node.getId(),
                    node.getSource(),
                    Maps.filterKeys(node.getAssignments(), requiredSymbols::contains),
                    node.getGroupingSets(),
                    node.getStep(),
                    node.getHashSymbol(),
                    node.getGroupIdSymbol());
        }

        @Override
        public PlanNode visitMarkDistinct(MarkDistinctNode node, ImmutableSet<Symbol> requiredSymbols)
        {
            if (!requiredSymbols.contains(node.getMarkerSymbol())) {
                return restrictSymbols(node.getSource(), requiredSymbols);
            }

            // MarkDistinctNode doesn't support output symbols selection, so we need to insert a project over the source
            ImmutableSet<Symbol> requiredSourceSymbols = requiredMarkDistinctSourceSymbols(node, requiredSymbols);

            PlanNode newSourceNode = restrictSymbols(node.getSource(), requiredSourceSymbols);
            return node.replaceChildren(ImmutableList.of(newSourceNode));
        }

        @Override
        public PlanNode visitFilter(FilterNode node, ImmutableSet<Symbol> requiredSymbols)
        {
            // FilterNode doesn't support output symbols selection, so we need to insert a project over the source
            ImmutableSet<Symbol> requiredSourceSymbols = Streams.concat(
                    DependencyExtractor.extractUnique(node).stream(),
                    requiredSymbols.stream()
            ).collect(toImmutableSet());

            PlanNode newSourceNode = restrictSymbols(node.getSource(), requiredSourceSymbols);
            return node.replaceChildren(ImmutableList.of(newSourceNode));
        }
    }

    @Override
    public Optional<PlanNode> apply(PlanNode node, Lookup lookup, PlanNodeIdAllocator idAllocator, SymbolAllocator symbolAllocator, Session session)
    {
        ImmutableList<ImmutableSet<Symbol>> requiredSymbols = node.accept(new RequiredInputSymbols(), null);
        ImmutableList.Builder<PlanNode> newChildListBuilder = ImmutableList.builder();
        boolean areChildrenModified = false;
        RestrictOutputSymbols restrictOutputSymbols = new RestrictOutputSymbols(idAllocator);
        for (int i = 0; i < requiredSymbols.size(); ++i) {
            PlanNode childRef = node.getSources().get(i);
            if (!childRef.getOutputSymbols().stream().allMatch(requiredSymbols.get(i)::contains)) {
                PlanNode newSource = lookup.resolve(childRef).accept(restrictOutputSymbols, requiredSymbols.get(i));
                if (newSource.getOutputSymbols().size() < childRef.getOutputSymbols().size()) {
                    newChildListBuilder.add(newSource);
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
