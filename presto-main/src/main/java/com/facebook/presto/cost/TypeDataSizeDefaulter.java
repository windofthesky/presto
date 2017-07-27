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

import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.Symbol;

import javax.annotation.concurrent.ThreadSafe;

import java.util.Map;
import java.util.function.ToDoubleFunction;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class TypeDataSizeDefaulter
{
    private static final double DEFAULT_DATA_SIZE_PER_COLUMN = 10;

    public double defaultDataSize(Type type)
    {
        requireNonNull(type, "type is null");

        return DEFAULT_DATA_SIZE_PER_COLUMN;
    }

    public ToDoubleFunction<Symbol> createSymbolDataSizeDefaulter(Map<Symbol, Type> types)
    {
        requireNonNull(types, "types is null");

        return symbol -> {
            requireNonNull(symbol, "symbol is null");
            Type type = requireNonNull(types.get(symbol), () -> format("No type for symbol %s", symbol));
            return defaultDataSize(type);
        };
    }
}
