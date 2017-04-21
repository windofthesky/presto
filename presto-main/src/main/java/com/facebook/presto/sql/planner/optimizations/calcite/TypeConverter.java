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

import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.CharType;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.IntegerType;
import com.facebook.presto.spi.type.SmallintType;
import com.facebook.presto.spi.type.TinyintType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlTypeName;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.CharType.createCharType;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.SmallintType.SMALLINT;
import static com.facebook.presto.spi.type.TinyintType.TINYINT;
import static com.facebook.presto.spi.type.VarcharType.createUnboundedVarcharType;
import static com.facebook.presto.spi.type.VarcharType.createVarcharType;

public class TypeConverter
{
    private RelDataTypeFactory typeFactory;

    public TypeConverter(RelDataTypeFactory typeFactory)
    {
        this.typeFactory = typeFactory;
    }

    public RelDataType toCalciteType(Type type)
    {
        if (type instanceof BigintType) {
            return typeFactory.createSqlType(SqlTypeName.BIGINT);
        }
        else if (type instanceof DoubleType) {
            return typeFactory.createSqlType(SqlTypeName.DOUBLE);
        }
        else if (type instanceof IntegerType) {
            return typeFactory.createSqlType(SqlTypeName.INTEGER);
        }
        else if (type instanceof SmallintType) {
            return typeFactory.createSqlType(SqlTypeName.SMALLINT);
        }
        else if (type instanceof TinyintType) {
            return typeFactory.createSqlType(SqlTypeName.TINYINT);
        }
        else if (type instanceof CharType) {
            CharType charType = (CharType) type;
            return typeFactory.createSqlType(SqlTypeName.CHAR, charType.getLength());
        }
        else if (type instanceof VarcharType) {
            VarcharType varcharType = (VarcharType) type;
            return typeFactory.createSqlType(SqlTypeName.VARCHAR, varcharType.getLength());
        }
        else if (type instanceof BooleanType) {
            return typeFactory.createSqlType(SqlTypeName.BOOLEAN);
        }
        throw new UnsupportedOperationException("Presto type -> calcite type conversion not supported");
    }

    public Type toPrestoType(RelDataType type)
    {
        switch (type.getSqlTypeName()) {
            case BIGINT:
                return BIGINT;
            case DOUBLE:
                return DOUBLE;
            case INTEGER:
                return INTEGER;
            case SMALLINT:
                return SMALLINT;
            case TINYINT:
                return TINYINT;
            case CHAR:
                return createCharType(type.getPrecision());
            case VARCHAR:
                if (type.getPrecision() == VarcharType.UNBOUNDED_LENGTH) {
                    return createUnboundedVarcharType();
                }
                return createVarcharType(type.getPrecision());
            case BOOLEAN:
                return BOOLEAN;
            default:
                throw new UnsupportedOperationException("Calcite type -> Presto type conversion not supported");
        }
    }

    public RelDataTypeFactory getTypeFactory()
    {
        return typeFactory;
    }
}
