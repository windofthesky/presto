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

import com.facebook.presto.decoder.DecoderColumnHandle;
import com.facebook.presto.decoder.FieldDecoder;
import com.facebook.presto.decoder.FieldValueProvider;
import com.facebook.presto.spi.block.Block;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;
import org.apache.avro.generic.GenericRecord;

import java.util.Set;

import static com.facebook.presto.decoder.avro.AvroSchemaUtils.serializeObject;
import static com.facebook.presto.spi.type.Varchars.isVarcharType;
import static com.facebook.presto.spi.type.Varchars.truncateToLength;
import static io.airlift.slice.Slices.EMPTY_SLICE;
import static io.airlift.slice.Slices.utf8Slice;
import static java.util.Objects.requireNonNull;

public class AvroFieldDecoder
        implements FieldDecoder<GenericRecord>
{
    @Override
    public Set<Class<?>> getJavaTypes()
    {
        return ImmutableSet.<Class<?>>of(boolean.class, long.class, double.class, Block.class, Slice.class);
    }

    @Override
    public String getRowDecoderName()
    {
        return AvroRowDecoder.NAME;
    }

    @Override
    public String getFieldDecoderName()
    {
        return FieldDecoder.DEFAULT_FIELD_DECODER_NAME;
    }

    @Override
    public FieldValueProvider decode(GenericRecord value, DecoderColumnHandle columnHandle)
    {
        requireNonNull(columnHandle, "columnHandle is null");
        requireNonNull(value, "value is null");
        return new AvroFieldValueProvider(value, columnHandle);
    }

    static class AvroFieldValueProvider
            extends FieldValueProvider
    {
        private DecoderColumnHandle columnHandle;
        private Object value;
        private GenericRecord record;

        public AvroFieldValueProvider(GenericRecord record, DecoderColumnHandle columnHandle)
        {
            this.columnHandle = columnHandle;
            this.value = record.get(columnHandle.getName());
            this.record = record;
        }

        @Override
        public boolean accept(DecoderColumnHandle inColumnHandle)
        {
            return columnHandle.equals(inColumnHandle);
        }

        @Override
        public boolean getBoolean()
        {
            return Boolean.getBoolean(value == null ? "False" : value.toString());
        }

        @Override
        public long getLong()
        {
            return (value == null) ? 0L : ((Number) value).longValue();
        }

        @Override
        public double getDouble()
        {
            return (value == null) ? 0.0 : ((Number) value).doubleValue();
        }

        @Override
        public boolean isNull()
        {
            return value == null;
        }

        @Override
        public Slice getSlice()
        {
            if (isNull()) {
                return EMPTY_SLICE;
            }
            Slice slice = utf8Slice(value.toString());
            if (isVarcharType(columnHandle.getType())) {
                slice = truncateToLength(slice, columnHandle.getType());
            }
            return slice;
        }

        public Object getObject()
        {
            return requireNonNull(serializeObject(columnHandle.getType(), null, value), "serialized result is null");
        }
    }
}
