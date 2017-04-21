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
import com.facebook.presto.sql.tree.BooleanLiteral;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.ComparisonExpressionType;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexPatternFieldRef;
import org.apache.calcite.rex.RexRangeRef;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexVisitor;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;

public class PrestoExpressionConverter
        implements RexVisitor<Expression>
{
    Optional<List<Symbol>> inputSymbols;

    public PrestoExpressionConverter(List<Symbol> inputSymbols)
    {
        this.inputSymbols = Optional.of(inputSymbols);
    }

    public PrestoExpressionConverter()
    {
        this.inputSymbols = Optional.empty();
    }

    @Override
    public Expression visitInputRef(RexInputRef inputRef)
    {
        return inputSymbols.get().get(inputRef.getIndex()).toSymbolReference();
    }

    @Override
    public Expression visitLocalRef(RexLocalRef localRef)
    {
        return unsupported();
    }

    @Override
    public Expression visitLiteral(RexLiteral literal)
    {
        if (literal.getType().getSqlTypeName().equals(SqlTypeName.BOOLEAN)) {
            Boolean value = (Boolean) literal.getValue();
            return new BooleanLiteral(value.toString());
        }

        return unsupported();
    }

    @Override
    public Expression visitCall(RexCall call)
    {
        List<Expression> operands = call.getOperands().stream()
                .map(operand -> operand.accept(this))
                .collect(toImmutableList());
        if (call.getOperator().equals(SqlStdOperatorTable.AND)) {
            return new LogicalBinaryExpression(LogicalBinaryExpression.Type.AND, operands.get(0), operands.get(1));
        }
        else if (call.getOperator().equals(SqlStdOperatorTable.OR)) {
            return new LogicalBinaryExpression(LogicalBinaryExpression.Type.OR, operands.get(0), operands.get(1));
        }
        if (call.getOperator().equals(SqlStdOperatorTable.EQUALS)) {
            return new ComparisonExpression(ComparisonExpressionType.EQUAL, operands.get(0), operands.get(1));
        }
        return unsupported();
    }

    @Override
    public Expression visitOver(RexOver over)
    {
        return unsupported();
    }

    @Override
    public Expression visitCorrelVariable(RexCorrelVariable correlVariable)
    {
        return unsupported();
    }

    @Override
    public Expression visitDynamicParam(RexDynamicParam dynamicParam)
    {
        return unsupported();
    }

    @Override
    public Expression visitRangeRef(RexRangeRef rangeRef)
    {
        return unsupported();
    }

    @Override
    public Expression visitFieldAccess(RexFieldAccess fieldAccess)
    {
        return unsupported();
    }

    @Override
    public Expression visitSubQuery(RexSubQuery subQuery)
    {
        return unsupported();
    }

    @Override
    public Expression visitPatternFieldRef(RexPatternFieldRef fieldRef)
    {
        return unsupported();
    }

    private Expression unsupported()
    {
        throw new UnsupportedOperationException("Calcite -> Presto conversion not yet implemented");
    }
}
