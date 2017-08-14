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
package com.facebook.presto.decoder.avro;

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.InterleavedBlockBuilder;
import com.facebook.presto.spi.classloader.ThreadContextClassLoader;
import com.facebook.presto.spi.type.ArrayAvroType;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.Decimals;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.IntegerType;
import com.facebook.presto.spi.type.StructType;
import com.facebook.presto.spi.type.TimestampType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarbinaryType;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slices;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import java.math.BigDecimal;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.isNull;
import static java.util.Objects.requireNonNull;

public class AvroSchemaUtils
{
    private AvroSchemaUtils()
    {
    }

    /**
     * Convert an Avro schema to Presto Type
     *
     * @param schema
     * @return
     */
    public static Type toPrestoType(Schema schema)
    {
        Schema.Type avroType = schema.getType();
        LogicalType logicalType = schema.getLogicalType();

        switch (avroType) {
            case INT:
                return IntegerType.INTEGER;
            case LONG:
                return BigintType.BIGINT;
            case FLOAT:
                return DoubleType.DOUBLE;
            case DOUBLE:
                return DoubleType.DOUBLE;
            case BOOLEAN:
                return BooleanType.BOOLEAN;
            case STRING:
                return VarcharType.VARCHAR;
            case BYTES:
                if (logicalType instanceof LogicalTypes.Decimal) {
                    LogicalTypes.Decimal decimalType = (LogicalTypes.Decimal) logicalType;
                    int precision = decimalType.getPrecision();
                    int scale = decimalType.getScale();
                    return DecimalType.createDecimalType(precision, scale);
                }
                else {
                    return VarbinaryType.VARBINARY;
                }
            case ENUM:
                return VarcharType.VARCHAR;
            case UNION:
                for (Schema schemaType : schema.getTypes()) {
                    if (!isNull(schemaType) && !Objects.equals(Schema.Type.NULL, schemaType.getType())) {
                        return toPrestoType(schemaType);
                    }
                }
            case ARRAY:
                return new ArrayAvroType(toPrestoType(schema.getElementType()));
            case RECORD:
                ImmutableList.Builder<Type> recordTypesBuilder = ImmutableList.builder();
                ImmutableList.Builder<String> recordFieldNames = ImmutableList.builder();
                for (Schema.Field field : schema.getFields()) {
                    recordTypesBuilder.add(toPrestoType(field.schema()));
                    recordFieldNames.add(field.name());
                }
                return new StructType(recordTypesBuilder.build(), Optional.of(recordFieldNames.build()));
            case MAP: // TODO: Add support for Maps if/when we start using them
                return VarbinaryType.VARBINARY;

            default:
                return null;
        }
    }

    /**
     * Serialize an Avro object to Presto Stack representation
     *
     * @param type
     * @param builder
     * @param object
     * @return
     */
    public static Block serializeObject(Type type, BlockBuilder builder, Object object)
    {
        try (ThreadContextClassLoader ignore = new ThreadContextClassLoader(type.getClass().getClassLoader())) {
            if (object instanceof GenericData.Record) {
                return serializeRecord(type, builder, (GenericData.Record) object);
            }
            else if (object instanceof GenericData.Array) {
                return serializeList(type, builder, (GenericData.Array) object);
            }
            else {
                serializePrimitive(type, builder, object);
                return null;
            }
        }
        finally {
            ThreadContextClassLoader ignore = new ThreadContextClassLoader(AvroSchemaUtils.class.getClassLoader());
        }
    }

    /**
     * Serialize a primitive type to Presto Stack representation
     *
     * @param type
     * @param builder
     * @param object
     */
    public static void serializePrimitive(Type type, BlockBuilder builder, Object object)
    {
        requireNonNull(builder, "parent builder is null");

        if (object == null) {
            builder.appendNull();
            return;
        }

        if (type instanceof BooleanType) {
            BOOLEAN.writeBoolean(builder, ((Boolean) object).booleanValue());
        }
        else if (type instanceof IntegerType) {
            INTEGER.writeLong(builder, ((Number) object).intValue());
        }
        else if (type instanceof BigintType) {
            BIGINT.writeLong(builder, ((Number) object).longValue());
        }
        else if (type instanceof DoubleType) {
            DOUBLE.writeDouble(builder, ((Number) object).doubleValue());
        }
        else if (type instanceof VarcharType) {
            VARCHAR.writeSlice(builder, Slices.utf8Slice((object.toString())));
        }
        else if (type instanceof TimestampType) {
            TIMESTAMP.writeLong(builder, ((Number) object).longValue());
        }
        else if (type instanceof VarbinaryType) {
            VARBINARY.writeSlice(builder, Slices.wrappedBuffer((byte[]) (object)));
        }
        else if (type instanceof DecimalType) {
            // Ankur: BigDecimal/BigInteger math is CPU intensive.
            // Ankur: This might become a perf issue as data grows
            DecimalType decimalType = (DecimalType) type;
            BigDecimal bigDecimal = (object instanceof BigDecimal)
                    ? (BigDecimal) object
                    : BigDecimal.valueOf(((Number) object).doubleValue());
            bigDecimal = bigDecimal.setScale(decimalType.getScale());
            Decimals.writeBigDecimal(decimalType, builder, bigDecimal);
        }
        else {
            throw new RuntimeException("Unknown primitive type: " + type + " Object: " + object + " Object class: " + (object != null ? object.getClass() : "<NONE>"));
        }
    }

    /**
     * Serialize Avro GenericData.Record type to presto Stack representation
     *
     * @param type
     * @param builder
     * @param record
     * @return
     */
    public static Block serializeRecord(Type type, BlockBuilder builder, GenericData.Record record)
    {
        if (record == null) {
            requireNonNull(builder, "parent builder is null").appendNull();
            return null;
        }

        List<Type> typeParams = type.getTypeParameters();

        BlockBuilder currentBuilder;
        if (builder != null) {
            currentBuilder = builder.beginBlockEntry();
        }
        else {
            currentBuilder = new InterleavedBlockBuilder(typeParams, new BlockBuilderStatus(), typeParams.size());
        }

        for (int index = 0; index < type.getTypeParameters().size(); index++) {
            serializeObject(type.getTypeParameters().get(index), currentBuilder, record.get(type.getDisplayName()));
        }

        if (builder != null) {
            builder.closeEntry();
            return null;
        }
        else {
            Block resultBlock = currentBuilder.build();
            return resultBlock;
        }
    }

    /**
     * Serialize Avro GenericRecord type to presto Stack representation
     *
     * @param type
     * @param builder
     * @param record
     * @return
     */
    public static Block serializeRecord(Type type, BlockBuilder builder, GenericRecord record)
    {
        if (record == null) {
            requireNonNull(builder, "parent builder is null").appendNull();
            return null;
        }

        List<Type> typeParams = type.getTypeParameters();

        BlockBuilder currentBuilder;
        if (builder != null) {
            currentBuilder = builder.beginBlockEntry();
        }
        else {
            currentBuilder = new InterleavedBlockBuilder(typeParams, new BlockBuilderStatus(), typeParams.size());
        }

        for (int index = 0; index < type.getTypeParameters().size(); index++) {
            serializeObject(type.getTypeParameters().get(index), currentBuilder, record.get(type.getDisplayName()));
        }

        if (builder != null) {
            builder.closeEntry();
            return null;
        }
        else {
            Block resultBlock = currentBuilder.build();
            return resultBlock;
        }
    }

    /**
     * Serialize a List type to presto stack representation
     *
     * @param type
     * @param builder
     * @param array
     * @return
     */
    public static Block serializeList(Type type, BlockBuilder builder, GenericData.Array array)
    {
        if (array == null) {
            requireNonNull(builder, "parent builder is null").appendNull();
            return null;
        }
        List<Type> typeParameters = type.getTypeParameters();
        checkArgument(typeParameters.size() == 1, "list must have exactly 1 type parameter");
        Type elementType = typeParameters.get(0);
        BlockBuilder currentBuilder;
        if (builder != null) {
            currentBuilder = builder.beginBlockEntry();
        }
        else {
            currentBuilder = elementType.createBlockBuilder(new BlockBuilderStatus(), array.size());
        }

        for (Object element : array) {
            serializeObject(elementType, currentBuilder, element);
        }

        if (builder != null) {
            builder.closeEntry();
            return null;
        }
        else {
            Block resultBlock = currentBuilder.build();
            return resultBlock;
        }
    }
}
