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
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.PlanNode;

import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.util.MoreLists.filteredCopy;

/**
 * Non-cross joins support output symbol selection, so absorb any project-off into the node.
 */
public class PruneJoinColumns
        extends ProjectOffPushDownRule<JoinNode>
{
    public PruneJoinColumns()
    {
        super(JoinNode.class);
    }

    @Override
    protected Optional<PlanNode> pushDownProjectOff(PlanNodeIdAllocator idAllocator, JoinNode joinNode, Set<Symbol> referencedOutputs)
    {
        if (joinNode.isCrossJoin()) {
            return Optional.empty();
        }

        return Optional.of(
                new JoinNode(
                        joinNode.getId(),
                        joinNode.getType(),
                        joinNode.getLeft(),
                        joinNode.getRight(),
                        joinNode.getCriteria(),
                        filteredCopy(joinNode.getOutputSymbols(), referencedOutputs::contains),
                        joinNode.getFilter(),
                        joinNode.getLeftHashSymbol(),
                        joinNode.getRightHashSymbol(),
                        joinNode.getDistributionType(),
                        joinNode.getDynamicFilterAssignments()));
    }
}
