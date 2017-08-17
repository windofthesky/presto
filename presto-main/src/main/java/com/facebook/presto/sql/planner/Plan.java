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
package com.facebook.presto.sql.planner;

import com.facebook.presto.Session;
import com.facebook.presto.cost.PlanNodeStatsEstimate;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;

import static com.facebook.presto.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Objects.requireNonNull;

public class Plan
{
    private final PlanNode root;
    private final Map<Symbol, Type> types;
    private final Lookup lookup;
    private final Session session;

    public Plan(PlanNode root, Map<Symbol, Type> types, Lookup lookup, Session session)
    {
        requireNonNull(root, "root is null");
        requireNonNull(types, "types is null");
        requireNonNull(lookup, "lookup is null");

        this.root = root;
        this.types = ImmutableMap.copyOf(types);
        this.lookup = requireNonNull(lookup, "lookup is null");
        this.session = requireNonNull(session, "session is null");
    }

    public PlanNode getRoot()
    {
        return root;
    }

    public Map<Symbol, Type> getTypes()
    {
        return types;
    }

    @VisibleForTesting
    public Map<PlanNodeId, PlanNodeStatsEstimate> getPlanNodeStats()
    {
        return getPlanNodes(root)
                .stream()
                .collect(toImmutableMap(PlanNode::getId, node -> lookup.getStats(node, session, types)));
    }

    private List<PlanNode> getPlanNodes(PlanNode root)
    {
        return searchFrom(root, Lookup.from(groupReference -> groupReference)).findAll();
    }
}
