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
package com.facebook.presto.sql.planner.assertions;

import com.facebook.presto.Session;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.sql.planner.plan.IndexJoinNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.tree.Expression;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.sql.planner.assertions.MatchResult.NO_MATCH;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Objects.requireNonNull;

final class IndexJoinMatcher
        implements Matcher
{
    private final IndexJoinNode.Type joinType;
    private final List<ExpectedValueProvider<IndexJoinNode.EquiJoinClause>> equiCriteria;

    IndexJoinMatcher(IndexJoinNode.Type joinType, List<ExpectedValueProvider<IndexJoinNode.EquiJoinClause>> equiCriteria)
    {
        this.joinType = requireNonNull(joinType, "joinType is null");
        this.equiCriteria = requireNonNull(equiCriteria, "equiCriteria is null");
    }

    @Override
    public boolean shapeMatches(PlanNode node)
    {
        if (!(node instanceof IndexJoinNode)) {
            return false;
        }

        IndexJoinNode indexJoinNode = (IndexJoinNode) node;
        return indexJoinNode.getType() == joinType;
    }

    @Override
    public MatchResult detailMatches(PlanNode node, Session session, Metadata metadata, SymbolAliases symbolAliases)
    {
        checkState(shapeMatches(node), "Plan testing framework error: shapeMatches returned false in detailMatches in %s", this.getClass().getName());

        IndexJoinNode indexJoinNode = (IndexJoinNode) node;

        if (indexJoinNode.getCriteria().size() != equiCriteria.size()) {
            return NO_MATCH;
        }

        /*
         * Have to use order-independent comparison; there are no guarantees what order
         * the equi criteria will have after planning and optimizing.
         */
        Set<IndexJoinNode.EquiJoinClause> actual = ImmutableSet.copyOf(indexJoinNode.getCriteria());
        Set<IndexJoinNode.EquiJoinClause> expected =
                equiCriteria.stream()
                .map(maker -> maker.getExpectedValue(symbolAliases))
                .collect(toImmutableSet());

        return new MatchResult(expected.equals(actual));
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("equiCriteria", equiCriteria)
                .toString();
    }
}
