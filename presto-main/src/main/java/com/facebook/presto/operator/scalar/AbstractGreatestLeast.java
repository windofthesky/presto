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
package com.facebook.presto.operator.scalar;

import com.facebook.presto.byteCode.ByteCodeBlock;
import com.facebook.presto.byteCode.ClassDefinition;
import com.facebook.presto.byteCode.DynamicClassLoader;
import com.facebook.presto.byteCode.MethodDefinition;
import com.facebook.presto.byteCode.Parameter;
import com.facebook.presto.byteCode.Scope;
import com.facebook.presto.byteCode.Variable;
import com.facebook.presto.byteCode.control.IfStatement;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.OperatorType;
import com.facebook.presto.metadata.SqlScalarFunction;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignature;
import com.facebook.presto.sql.gen.CallSiteBinder;
import com.facebook.presto.sql.gen.CompilerUtils;
import com.google.common.collect.ImmutableList;

import java.lang.invoke.MethodHandle;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static com.facebook.presto.byteCode.Access.FINAL;
import static com.facebook.presto.byteCode.Access.PRIVATE;
import static com.facebook.presto.byteCode.Access.PUBLIC;
import static com.facebook.presto.byteCode.Access.STATIC;
import static com.facebook.presto.byteCode.Access.a;
import static com.facebook.presto.byteCode.Parameter.arg;
import static com.facebook.presto.byteCode.ParameterizedType.type;
import static com.facebook.presto.metadata.Signature.internalOperator;
import static com.facebook.presto.metadata.Signature.orderableTypeParameter;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.sql.gen.ByteCodeUtils.invoke;
import static com.facebook.presto.sql.gen.CompilerUtils.defineClass;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static com.facebook.presto.util.Reflection.methodHandle;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

public abstract class AbstractGreatestLeast
        extends SqlScalarFunction
{
    private static final MethodHandle CHECK_NOT_NAN = methodHandle(AbstractGreatestLeast.class, "checkNotNaN", String.class, double.class);

    private final OperatorType operatorType;

    protected AbstractGreatestLeast(String name, OperatorType operatorType)
    {
        super(name, ImmutableList.of(orderableTypeParameter("E")), "E", ImmutableList.of("E"), true);
        this.operatorType = requireNonNull(operatorType, "operatorType is null");
    }

    @Override
    public boolean isHidden()
    {
        return false;
    }

    @Override
    public boolean isDeterministic()
    {
        return true;
    }

    @Override
    public ScalarFunctionImplementation specialize(Map<String, Type> types, List<TypeSignature> parameterTypes, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        Type type = types.get("E");
        checkArgument(type.isOrderable(), "Type must be orderable");

        MethodHandle compareMethod = functionRegistry.getScalarFunctionImplementation(internalOperator(operatorType, BOOLEAN, ImmutableList.of(type, type))).getMethodHandle();

        int arity = parameterTypes.size();
        List<Class<?>> javaTypes = IntStream.range(0, arity)
                .mapToObj(i -> type.getJavaType())
                .collect(toImmutableList());

        Class<?> clazz = generate(javaTypes, type, compareMethod);
        MethodHandle methodHandle = methodHandle(clazz, getSignature().getName(), javaTypes.toArray(new Class<?>[javaTypes.size()]));
        List<Boolean> nullableParameters = ImmutableList.copyOf(Collections.nCopies(javaTypes.size(), false));

        return new ScalarFunctionImplementation(false, nullableParameters, methodHandle, isDeterministic());
    }

    public static void checkNotNaN(String name, double value)
    {
        if (Double.isNaN(value)) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, format("Invalid argument to %s(): NaN", name));
        }
    }

    private Class<?> generate(List<Class<?>> javaTypes, Type type, MethodHandle compareMethod)
    {
        String javaTypeName = javaTypes.stream()
                .map(Class::getSimpleName)
                .collect(joining());

        ClassDefinition definition = new ClassDefinition(
                a(PUBLIC, FINAL),
                CompilerUtils.makeClassName(javaTypeName + "$" + getSignature().getName()),
                type(Object.class));

        definition.declareDefaultConstructor(a(PRIVATE));

        List<Parameter> parameters = IntStream.range(0, javaTypes.size())
                .mapToObj(i -> arg("arg" + i, javaTypes.get(i)))
                .collect(toImmutableList());

        MethodDefinition method = definition.declareMethod(
                a(PUBLIC, STATIC),
                getSignature().getName(),
                type(javaTypes.get(0)),
                parameters);

        Scope scope = method.getScope();
        ByteCodeBlock body = method.getBody();

        CallSiteBinder binder = new CallSiteBinder();

        if (type.getTypeSignature().getBase().equals(StandardTypes.DOUBLE)) {
            for (Parameter parameter : parameters) {
                body.append(parameter);
                body.append(invoke(binder.bind(CHECK_NOT_NAN.bindTo(getSignature().getName())), "checkNotNaN"));
            }
        }

        Variable value = scope.declareVariable(javaTypes.get(0), "value");

        body.append(value.set(parameters.get(0)));

        for (int i = 1; i < javaTypes.size(); i++) {
            body.append(new IfStatement()
                    .condition(new ByteCodeBlock()
                            .append(parameters.get(i))
                            .append(value)
                            .append(invoke(binder.bind(compareMethod), "compare")))
                    .ifTrue(value.set(parameters.get(i))));
        }

        body.append(value.ret());

        return defineClass(definition, Object.class, binder.getBindings(), new DynamicClassLoader(getClass().getClassLoader()));
    }
}
