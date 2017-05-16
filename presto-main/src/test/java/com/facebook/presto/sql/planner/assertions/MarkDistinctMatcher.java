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
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.plan.MarkDistinctNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.tree.SymbolReference;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.sql.planner.assertions.MatchResult.NO_MATCH;
import static com.facebook.presto.sql.planner.assertions.MatchResult.match;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;

public class MarkDistinctMatcher
        implements Matcher
{
    private final String markerSymbol;
    private final List<String> distinctSymbols;
    private final Optional<String> hashSymbol;


    public MarkDistinctMatcher(String markerSymbol, List<String> distinctSymbols, Optional<String> hashSymbol)
    {
        this.markerSymbol = markerSymbol;
        this.distinctSymbols = distinctSymbols;
        this.hashSymbol = hashSymbol;
    }

    @Override
    public boolean shapeMatches(PlanNode node)
    {
        return node instanceof MarkDistinctNode;
    }

    @Override
    public MatchResult detailMatches(PlanNode node, Session session, Metadata metadata, SymbolAliases symbolAliases)
    {
        checkState(shapeMatches(node), "Plan testing framework error: shapeMatches returned false in detailMatches in %s", this.getClass().getName());
        MarkDistinctNode markDistinctNode = (MarkDistinctNode) node;

        if (!markDistinctNode.getHashSymbol().map(Symbol::getName)
                .equals(hashSymbol.map(alias -> symbolAliases.get(alias).getName())))
        {
            return NO_MATCH;
        }

        if (!markDistinctNode.getDistinctSymbols().stream().map(Symbol::getName).collect(toImmutableList())
                .equals(distinctSymbols.stream().map(alias -> symbolAliases.get(alias).getName()).collect(toImmutableList())))
        {
            return NO_MATCH;
        }

        return match(markerSymbol, new SymbolReference(markDistinctNode.getMarkerSymbol().getName()));
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("markerSymbol", markerSymbol)
                .add("distinctSymbols", distinctSymbols)
                .add("hashSymbol", hashSymbol)
                .toString();
    }
}
