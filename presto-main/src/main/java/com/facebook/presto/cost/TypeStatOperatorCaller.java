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

import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.operator.scalar.ScalarFunctionImplementation;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.function.OperatorType;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.IntegerType;
import com.facebook.presto.spi.type.SmallintType;
import com.facebook.presto.spi.type.TinyintType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;
import com.facebook.presto.sql.planner.ExpressionInterpreter;
import com.facebook.presto.sql.tree.ComparisonExpressionType;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import static com.facebook.presto.spi.function.OperatorType.BETWEEN;
import static java.util.Collections.singletonList;

/**
 * This will contain set of all function used in process of calculation stats.
 * It will be created based on Type and TypeRegistry.
 */
public class TypeStatOperatorCaller
{
    private final Type type;
    private final FunctionRegistry functionRegistry;
    private final ConnectorSession session;

    public TypeStatOperatorCaller(Type type, FunctionRegistry functionRegistry, ConnectorSession session)
    {
        this.type = type;
        this.functionRegistry = functionRegistry;
        this.session = session;
    }

    public boolean callComparisonOperator(ComparisonExpressionType comparisonType, Object left, Object right)
    {
        OperatorType operatorType = OperatorType.valueOf(comparisonType.name());
        Signature signature = functionRegistry.resolveOperator(operatorType, ImmutableList.of(type, type));
        ScalarFunctionImplementation implementation = functionRegistry.getScalarFunctionImplementation(signature);
        return (boolean) ExpressionInterpreter.invoke(session, implementation, ImmutableList.of(castToValidLiteralType(left), castToValidLiteralType(right)));
    }

    public boolean callBetweenOperator(Object which, Object low, Object high)
    {
        Signature signature = functionRegistry.resolveOperator(BETWEEN, ImmutableList.of(type, type, type));
        ScalarFunctionImplementation implementation = functionRegistry.getScalarFunctionImplementation(signature);
        return (boolean) ExpressionInterpreter.invoke(session, implementation, ImmutableList.of(which, castToValidLiteralType(low), castToValidLiteralType(high)));
    }

    public Slice castToVarchar(Object object)
    {
        Signature castSignature = functionRegistry.getCoercion(type, VarcharType.createUnboundedVarcharType());
        ScalarFunctionImplementation castImplementation = functionRegistry.getScalarFunctionImplementation(castSignature);
        return (Slice) ExpressionInterpreter.invoke(session, castImplementation, singletonList(castToValidLiteralType(object)));
    }

    public double translateToDouble(Object object)
    {
        if (object instanceof Long) {
            return (double) (long) object;
        }
        if (object instanceof Integer) {
            return (double) (int) object;
        }
        return (double) object; //fixme
    }

    public Object castToValidLiteralType(Object obj)
    {
        if (ImmutableList.of(BigintType.BIGINT, IntegerType.INTEGER, SmallintType.SMALLINT, type.equals(TinyintType.TINYINT)).contains(type) &&
                obj instanceof Double) {
            return (Long) (long) (double) obj;
        }
        return obj;
    }
}
