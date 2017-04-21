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
import com.facebook.presto.spi.block.SortOrder;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.SortNode;
import com.facebook.presto.sql.planner.plan.WindowNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static com.facebook.presto.spi.block.SortOrder.ASC_NULLS_LAST;
import static com.google.common.collect.Iterables.concat;
import static java.util.stream.Collectors.toMap;

public class SortBeforeWindowFunction
    implements Rule
{
    @Override
    public Optional<PlanNode> apply(PlanNode node, Lookup lookup, PlanNodeIdAllocator idAllocator, SymbolAllocator symbolAllocator, Session session)
    {
        if (!(node instanceof WindowNode)) {
            return Optional.empty();
        }

        WindowNode windowNode = (WindowNode) node;

        PlanNode source = lookup.resolve(windowNode.getSource());
        if (!(source instanceof SortNode)) {
            List<Symbol> orderBy = ImmutableList.copyOf(concat(windowNode.getPartitionBy(), windowNode.getOrderBy()));
            if (orderBy.isEmpty()) {
                return Optional.empty();
            }
            Map<Symbol, SortOrder> orderings = new HashMap<>();
            orderings.putAll(windowNode.getPartitionBy().stream().collect(toMap(Function.identity(), e -> ASC_NULLS_LAST)));
            orderings.putAll(windowNode.getOrderings());

            SortNode sortNode = new SortNode(idAllocator.getNextId(), source, orderBy, orderings);
            WindowNode newWindowNode = new WindowNode(
                    idAllocator.getNextId(),
                    sortNode,
                    windowNode.getSpecification(),
                    windowNode.getWindowFunctions(),
                    windowNode.getHashSymbol(),
                    ImmutableSet.copyOf(windowNode.getPartitionBy()),
                    windowNode.getOrderBy().size(),
                    true);
            return Optional.of(newWindowNode);
        }

        return Optional.empty();
    }
}
