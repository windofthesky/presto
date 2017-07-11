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

import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolsExtractor;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.plan.Assignments;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.tree.Expression;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.util.Collection;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;

class Util
{
    private Util()
    {
    }

    /**
     * Prune the set of available inputs to those required by the given expressions.
     * <p>
     * If all inputs are used, return Optional.empty() to indicate that no pruning is necessary.
     */
    public static Optional<Set<Symbol>> pruneInputs(Collection<Symbol> availableInputs, Collection<Expression> expressions)
    {
        Set<Symbol> availableInputsSet = ImmutableSet.copyOf(availableInputs);
        Set<Symbol> prunedInputs = Sets.filter(availableInputsSet, SymbolsExtractor.extractUnique(expressions)::contains);

        if (prunedInputs.size() == availableInputsSet.size()) {
            return Optional.empty();
        }

        return Optional.of(prunedInputs);
    }

    /**
     * Transforms a plan like P->C->X to C->P->X
     */
    public static PlanNode transpose(PlanNode parent, PlanNode child)
    {
        return child.replaceChildren(ImmutableList.of(
                parent.replaceChildren(
                        child.getSources())));
    }

    /**
     * @return If the node has outputs not in permittedOutputs, returns an identity projection containing only those node outputs also in permittedOutputs.
     */
    public static Optional<PlanNode> restrictOutputs(PlanNodeIdAllocator idAllocator, PlanNode node, Set<Symbol> permittedOutputs)
    {
        List<Symbol> restrictedOutputs = node.getOutputSymbols().stream()
                .filter(permittedOutputs::contains)
                .collect(toImmutableList());

        if (restrictedOutputs.size() == node.getOutputSymbols().size()) {
            return Optional.empty();
        }

        return Optional.of(
                new ProjectNode(
                        idAllocator.getNextId(),
                        node,
                        Assignments.identity(restrictedOutputs)));
    }

    /**
     * @return The original node, with identity projections possibly inserted between node and each child, limiting the columns to those permitted.
     * Returns a present Optional iff at least one child was rewritten.
     */
    @SafeVarargs
    public static Optional<PlanNode> restrictChildOutputs(PlanNodeIdAllocator idAllocator, PlanNode node, Set<Symbol>... permittedChildOutputsArgs)
    {
        List<Set<Symbol>> permittedChildOutputs = ImmutableList.copyOf(permittedChildOutputsArgs);

        checkArgument(
                (node.getSources().size() == permittedChildOutputs.size()),
                "Mismatched child (%d) and permitted outputs (%d) sizes",
                node.getSources().size(),
                permittedChildOutputs.size());

        ImmutableList.Builder<PlanNode> newChildrenBuilder = ImmutableList.builder();
        boolean rewroteChildren = false;

        for (int i = 0; i < node.getSources().size(); ++i) {
            PlanNode oldChild = node.getSources().get(i);
            Optional<PlanNode> newChild = restrictOutputs(idAllocator, oldChild, permittedChildOutputs.get(i));
            rewroteChildren |= newChild.isPresent();
            newChildrenBuilder.add(newChild.orElse(oldChild));
        }

        if (!rewroteChildren) {
            return Optional.empty();
        }
        return Optional.of(node.replaceChildren(newChildrenBuilder.build()));
    }

    /**
     * Looks for the pattern (ProjectNode*)TargetNode, and rewrites it to TargetNode(ProjectNode*),
     * returning an empty option if the pattern doesn't match, or if it can't rewrite the projects,
     * for example because they rely on the output of the TargetNode.
     */
    public static <N extends PlanNode> Optional<N> pullUnaryNodeAboveProjects(
            Lookup lookup,
            Class<N> targetNodeClass,
            PlanNode root)
    {
        Deque<ProjectNode> projectStack = new LinkedList<>();

        PlanNode current = lookup.resolve(root);

        while (current instanceof ProjectNode) {
            ProjectNode currentProject = (ProjectNode) current;
            projectStack.push(currentProject);
            current = lookup.resolve(currentProject.getSource());
        }

        if (!targetNodeClass.isInstance(current)) {
            return Optional.empty();
        }

        N target = targetNodeClass.cast(current);
        PlanNode targetChild = lookup.resolve(target.getSources().get(0));

        Set<Symbol> targetInputs = ImmutableSet.copyOf(targetChild.getOutputSymbols());
        Set<Symbol> targetOutputs = ImmutableSet.copyOf(target.getOutputSymbols());

        PlanNode newTargetChild = targetChild;

        while (!projectStack.isEmpty()) {
            ProjectNode project = projectStack.pop();
            Set<Symbol> newTargetChildOutputs = ImmutableSet.copyOf(newTargetChild.getOutputSymbols());

            // The only kind of use of the output of the target that we can safely ignore is a simple identity propagation.
            // The target node, when hoisted above the projections, will provide the symbols directly.
            Map<Symbol, Expression> assignmentsWithoutTargetOutputIdentities = Maps.filterKeys(
                    project.getAssignments().getMap(),
                    output -> !(project.getAssignments().isIdentity(output) && targetOutputs.contains(output)));

            if (targetInputs.stream().anyMatch(assignmentsWithoutTargetOutputIdentities::containsKey)) {
                // Redefinition of an input to the target -- can't handle this case.
                return Optional.empty();
            }

            Assignments newAssignments = Assignments.builder()
                    .putAll(assignmentsWithoutTargetOutputIdentities)
                    .putIdentities(targetInputs)
                    .build();

            if (!newTargetChildOutputs.containsAll(SymbolsExtractor.extractUnique(newAssignments.getExpressions()))) {
                // Projection uses an output of the target -- can't move the target above this projection.
                return Optional.empty();
            }

            newTargetChild = new ProjectNode(project.getId(), newTargetChild, newAssignments);
        }

        N newTarget = targetNodeClass.cast(target.replaceChildren(ImmutableList.of(newTargetChild)));
        Set<Symbol> newTargetOutputs = ImmutableSet.copyOf(newTarget.getOutputSymbols());
        if (!newTargetOutputs.containsAll(root.getOutputSymbols())) {
            // The new target node is hiding some of the projections, which makes this rewrite incorrect.
            return Optional.empty();
        }
        return Optional.of(newTarget);
    }
}
