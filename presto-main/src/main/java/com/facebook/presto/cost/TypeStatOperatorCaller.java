package com.facebook.presto.cost;

import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.operator.scalar.ScalarFunctionImplementation;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.function.OperatorType;
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
        return (boolean) ExpressionInterpreter.invoke(session, implementation, ImmutableList.of(left, right));
    }

    public boolean callBetweenOperator(Object which, Object low, Object high)
    {
        Signature signature = functionRegistry.resolveOperator(BETWEEN, ImmutableList.of(type, type, type));
        ScalarFunctionImplementation implementation = functionRegistry.getScalarFunctionImplementation(signature);
        return (boolean) ExpressionInterpreter.invoke(session, implementation, ImmutableList.of(which, low, high));
    }

    public Slice castToVarchar(Object object)
    {
        Signature castSignature = functionRegistry.getCoercion(type, VarcharType.createUnboundedVarcharType());
        ScalarFunctionImplementation castImplementation = functionRegistry.getScalarFunctionImplementation(castSignature);
        return (Slice) ExpressionInterpreter.invoke(session, castImplementation, singletonList(object));
    }

    public double translateToDouble(Object object) {
        return (double) object; //fixme
    }
}
