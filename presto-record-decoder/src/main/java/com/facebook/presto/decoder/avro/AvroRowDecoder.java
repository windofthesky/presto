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
import com.facebook.presto.decoder.RowDecoder;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Objects.nonNull;

public class AvroRowDecoder
        implements RowDecoder
{
    public static final String NAME = "avro";

    @Override
    public String getName()
    {
        return NAME;
    }

    private static int readInt(byte[] b)
    {
        return (((b[0]) << 24) |
                ((b[1] & 0xff) << 16) |
                ((b[2] & 0xff) << 8) |
                ((b[3] & 0xff)));
    }

    private boolean outOfBounds(int version, int min, int max)
    {
        return version < min || version > max;
    }

    private Map getRecordReaders(List<DecoderColumnHandle> columnHandles)
    {
        for (DecoderColumnHandle columnHandle : columnHandles) {
            if (!columnHandle.isInternal() && nonNull(columnHandle.getSchemas())) {
                return columnHandle.getRecordReaders();
            }
        }
        return Collections.EMPTY_MAP;
    }

    private boolean validColumnHandles(List<DecoderColumnHandle> columnHandles)
    {
        return nonNull(columnHandles) && !columnHandles.isEmpty();
    }

    private boolean validData(byte[] data)
    {
        return nonNull(data) && data.length > Integer.SIZE;
    }

    @Override
    public boolean decodeRow(byte[] data,
            Map<String, String> dataMap,
            Set<FieldValueProvider> fieldValueProviders,
            List<DecoderColumnHandle> columnHandles,
            Map<DecoderColumnHandle, FieldDecoder<?>> fieldDecoders)
    {
        if (!validColumnHandles(columnHandles) || !validData(data)) {
            return true;
        }

        int dataVersion = readInt(data);
        Map<Integer, GenericDatumReader> recordReaders = getRecordReaders(columnHandles);
        int numReaders = recordReaders.size();
        if (recordReaders.isEmpty()) {
            return true;
        }
        int readerIdx = outOfBounds(dataVersion, 1, numReaders) ? numReaders - 1 : dataVersion - 1;

        //GenericDatumReader<GenericRecord> recordReader = (GenericDatumReader) recordReaders.get(readerIdx);
        GenericDatumReader<GenericRecord> recordReader = recordReaders.get(dataVersion);
        if (recordReader == null) {
            int maxVersion = 0;
            for (Map.Entry<Integer, GenericDatumReader> entry : recordReaders.entrySet()) {
                if (entry.getKey() > maxVersion) {
                    maxVersion = entry.getKey();
                    recordReader = entry.getValue();
                }
            }
        }
        BinaryDecoder binaryDecoder = DecoderFactory.get().binaryDecoder(data, Integer.BYTES,
                data.length - Integer.BYTES, null);

        GenericRecord record = null;
        try {
            record = recordReader.read(record, binaryDecoder);
        }
        catch (Exception e) {
            return true;
        }

        for (DecoderColumnHandle columnHandle : columnHandles) {
            if (columnHandle.isInternal()) {
                continue;
            }
            //@SuppressWarnings("unchecked")
            FieldDecoder<GenericRecord> decoder = (FieldDecoder<GenericRecord>) fieldDecoders.get(columnHandle);
            if (nonNull(decoder)) {
                fieldValueProviders.add(decoder.decode(record, columnHandle));
            }
        }
        return false;
    }
}
