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
package com.facebook.presto.sql.planner.optimizations.calcite;

import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.tree.AstVisitor;
import com.facebook.presto.sql.tree.BooleanLiteral;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.ComparisonExpressionType;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import com.facebook.presto.sql.tree.SymbolReference;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static com.google.common.base.Preconditions.checkState;

public class CalciteRexConverter
        extends AstVisitor<RexNode, Void>
{
    private final Optional<RelDataType> inputRowType;
    private final Optional<List<Symbol>> inputSymbols;
    private final RexBuilder builder;

    public CalciteRexConverter(RelDataType inputRowType, List<Symbol> inputSymbols, RexBuilder builder)
    {
        this.inputRowType = Optional.of(inputRowType);
        this.inputSymbols = Optional.of(inputSymbols);
        this.builder = builder;
    }

    public CalciteRexConverter(RexBuilder builder)
    {
        this.inputRowType = Optional.empty();
        this.inputSymbols = Optional.empty();
        this.builder = builder;
    }

    @Override
    protected RexNode visitExpression(Expression expression, Void context)
    {
        throw new UnsupportedOperationException("Presto -> Calcite conversion for " + expression.getClass().getSimpleName() + " not yet implemented");
    }

    protected RexNode visitSymbolReference(SymbolReference node, Void context)
    {
        checkState(inputSymbols.get().size() == inputRowType.get().getFieldList().size());
        int index = inputSymbols.get().stream()
                .map(Symbol::getName)
                .collect(toImmutableList())
                .indexOf(node.getName());
        return builder.makeInputRef(inputRowType.get().getFieldList().get(index).getType(), index);
    }

    protected RexNode visitLogicalBinaryExpression(LogicalBinaryExpression node, Void context)
    {
        RexNode left = process(node.getLeft(), context);
        RexNode right = process(node.getRight(), context);
        SqlOperator operator;
        if (node.getType() == LogicalBinaryExpression.Type.AND) {
            operator = SqlStdOperatorTable.AND;
        }
        else {
            checkState(node.getType() == LogicalBinaryExpression.Type.OR);
            operator = SqlStdOperatorTable.OR;
        }
        return builder.makeCall(operator, left, right);
    }

    protected RexNode visitComparisonExpression(ComparisonExpression node, Void context)
    {
        RexNode left = process(node.getLeft(), context);
        RexNode right = process(node.getRight(), context);
        SqlOperator operator;
        if (node.getType() == ComparisonExpressionType.EQUAL) {
            operator = SqlStdOperatorTable.EQUALS;
        }
        else {
            throw new UnsupportedOperationException("comparison not supported yet");
        }
        return builder.makeCall(operator, left, right);
    }

    @Override
    protected RexNode visitBooleanLiteral(BooleanLiteral node, Void context)
    {
        return builder.makeLiteral(node.getValue());
    }
}
