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
package com.facebook.presto.cost;

import com.facebook.presto.Session;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.statistics.ColumnStatistics;
import com.facebook.presto.spi.statistics.TableStatistics;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.DomainTranslator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.tree.BooleanLiteral;
import com.facebook.presto.sql.tree.Expression;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.spi.statistics.ColumnStatistics.UNKNOWN_COLUMN_STATISTICS;
import static java.util.Objects.requireNonNull;

public class TableScanStatsRule
        implements ComposableStatsCalculator.Rule
{
    private final Metadata metadata;

    public TableScanStatsRule(Metadata metadata)
    {
        this.metadata = requireNonNull(metadata, "metadata can not be null");
    }

    @Override
    public Optional<PlanNodeStatsEstimate> calculate(PlanNode node, Lookup lookup, Session session, Map<Symbol, Type> types)
    {
        if (!(node instanceof TableScanNode)) {
            return Optional.empty();
        }

        TableScanNode tableScanNode = (TableScanNode) node;

        Constraint<ColumnHandle> constraint = getConstraint(tableScanNode, BooleanLiteral.TRUE_LITERAL, session, types);

        TableStatistics tableStatistics = metadata.getTableStatistics(session, tableScanNode.getTable(), constraint);
        Map<Symbol, ColumnStatistics> outputSymbolStats = new HashMap<>();

        for (Map.Entry<Symbol, ColumnHandle> entry : tableScanNode.getAssignments().entrySet()) {
            Symbol symbol = entry.getKey();
            ColumnStatistics statistics = tableStatistics.getColumnStatistics().getOrDefault(entry.getValue(), UNKNOWN_COLUMN_STATISTICS);
            outputSymbolStats.put(symbol, statistics);
        }

        return Optional.of(PlanNodeStatsEstimate.builder()
                .setOutputRowCount(tableStatistics.getRowCount())
                .setSymbolStatistics(outputSymbolStats)
                .build());
    }

    private Constraint<ColumnHandle> getConstraint(TableScanNode node, Expression predicate, Session session, Map<Symbol, Type> types)
    {
        DomainTranslator.ExtractionResult decomposedPredicate = DomainTranslator.fromPredicate(
                metadata,
                session,
                predicate,
                types);

        TupleDomain<ColumnHandle> simplifiedConstraint = decomposedPredicate.getTupleDomain()
                .transform(node.getAssignments()::get)
                .intersect(node.getCurrentConstraint());

        return new Constraint<>(simplifiedConstraint, bindings -> true);
    }
}
