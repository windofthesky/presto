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
package com.facebook.presto.kafka;

import com.facebook.presto.decoder.DecoderColumnHandle;
import com.facebook.presto.decoder.FieldDecoder;
import com.facebook.presto.decoder.FieldValueProvider;
import com.facebook.presto.decoder.RowDecoder;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.MessageAndOffset;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.facebook.presto.kafka.KafkaErrorCode.KAFKA_SPLIT_ERROR;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * Kafka specific record set. Returns a cursor for a topic which iterates over a Kafka partition segment.
 */
public class KafkaRecordSet
        implements RecordSet
{
    private static final Logger log = Logger.get(KafkaRecordSet.class);
    // Ankur: Core services expect to have a max message size of 12 MB
    // hence the large buffer, coz with a smaller buffer, the consumer
    // simply gets stuck
    private static final int KAFKA_READ_BUFFER_SIZE = 16 * 1024 * 1024;
    private static final byte[] EMPTY_BYTE_ARRAY = new byte[0];
    private static final int MAX_SPLIT_END_ATTEMPTS = 100;

    private final KafkaSplit split;
    private final KafkaSimpleConsumerManager consumerManager;

    private final RowDecoder keyDecoder;
    private final RowDecoder messageDecoder;
    private final Map<DecoderColumnHandle, FieldDecoder<?>> keyFieldDecoders;
    private final Map<DecoderColumnHandle, FieldDecoder<?>> messageFieldDecoders;

    private final List<DecoderColumnHandle> columnHandles;
    private final List<Type> columnTypes;

    private final Set<FieldValueProvider> globalInternalFieldValueProviders;

    KafkaRecordSet(KafkaSplit split,
            KafkaSimpleConsumerManager consumerManager,
            List<DecoderColumnHandle> columnHandles,
            RowDecoder keyDecoder,
            RowDecoder messageDecoder,
            Map<DecoderColumnHandle, FieldDecoder<?>> keyFieldDecoders,
            Map<DecoderColumnHandle, FieldDecoder<?>> messageFieldDecoders)
    {
        this.split = requireNonNull(split, "split is null");

        this.globalInternalFieldValueProviders = ImmutableSet.of(
                KafkaInternalFieldDescription.PARTITION_ID_FIELD.forLongValue(split.getPartitionId()),
                KafkaInternalFieldDescription.SEGMENT_START_FIELD.forLongValue(split.getStart()),
                KafkaInternalFieldDescription.SEGMENT_END_FIELD.forLongValue(split.getEnd()));

        this.consumerManager = requireNonNull(consumerManager, "consumerManager is null");

        this.keyDecoder = requireNonNull(keyDecoder, "rowDecoder is null");
        this.messageDecoder = requireNonNull(messageDecoder, "rowDecoder is null");
        this.keyFieldDecoders = requireNonNull(keyFieldDecoders, "keyFieldDecoders is null");
        this.messageFieldDecoders = requireNonNull(messageFieldDecoders, "messageFieldDecoders is null");

        this.columnHandles = requireNonNull(columnHandles, "columnHandles is null");

        ImmutableList.Builder<Type> typeBuilder = ImmutableList.builder();

        for (DecoderColumnHandle handle : columnHandles) {
            typeBuilder.add(handle.getType());
        }

        this.columnTypes = typeBuilder.build();
    }

    @Override
    public List<Type> getColumnTypes()
    {
        return columnTypes;
    }

    @Override
    public RecordCursor cursor()
    {
        return new KafkaRecordCursor();
    }

    public class KafkaRecordCursor
            implements RecordCursor
    {
        private long totalBytes;
        private long totalMessages;
        private long cursorOffset = split.getStart();
        private Iterator<MessageAndOffset> messageAndOffsetIterator;
        private final AtomicBoolean reported = new AtomicBoolean();

        private FieldValueProvider[] fieldValueProviders;

        KafkaRecordCursor()
        {
        }

        @Override
        public long getTotalBytes()
        {
            return totalBytes;
        }

        @Override
        public long getCompletedBytes()
        {
            return totalBytes;
        }

        @Override
        public long getReadTimeNanos()
        {
            return 0;
        }

        @Override
        public Type getType(int field)
        {
            checkArgument(field < columnHandles.size(), "Invalid field index");
            return columnHandles.get(field).getType();
        }

        @Override
        public boolean advanceNextPosition()
        {
            long lastCursorOffset = cursorOffset;
            int retryAttempts = 0;

            while (true) {
                if (cursorOffset >= split.getEnd() || retryAttempts >= MAX_SPLIT_END_ATTEMPTS) {
                    // Ankur: If a message size larger than KAFKA_READ_BUFFER_SIZE
                    // Then simple consumer gets stuck in an infinite loop,
                    // never getting past the offset where it finds the message
                    // larger than KAFKA_READ_BUFFER_SIZE
                    return endOfData(); // Split end is exclusive.
                }
                // Create a fetch request
                openFetchRequest();

                while (messageAndOffsetIterator.hasNext()) {
                    MessageAndOffset currentMessageAndOffset = messageAndOffsetIterator.next();
                    long messageOffset = currentMessageAndOffset.offset();

                    if (messageOffset >= split.getEnd()) {
                        return endOfData(); // Past our split end. Bail.
                    }

                    if (messageOffset >= cursorOffset) {
                        return nextRow(currentMessageAndOffset);
                    }
                }
                messageAndOffsetIterator = null;

                if (lastCursorOffset == cursorOffset) {
                    // Last cursor offset did not change, update retry attempt
                    retryAttempts++;
                }
                else {
                    // Else set last cursor offset to current cursor offset
                    lastCursorOffset = cursorOffset;
                    retryAttempts = 0;
                }
            }
        }

        private boolean endOfData()
        {
            if (!reported.getAndSet(true)) {
                log.debug("Found a total of %d messages with %d bytes (%d messages expected). Last Offset: %d (%d, %d)",
                        totalMessages, totalBytes, split.getEnd() - split.getStart(),
                        cursorOffset, split.getStart(), split.getEnd());
            }
            return false;
        }

        private boolean nextRow(MessageAndOffset messageAndOffset)
        {
            cursorOffset = messageAndOffset.offset() + 1; // Cursor now points to the next message.
            totalBytes += messageAndOffset.message().payloadSize();
            totalMessages++;

            byte[] keyData = EMPTY_BYTE_ARRAY;
            byte[] messageData = EMPTY_BYTE_ARRAY;
            ByteBuffer key = messageAndOffset.message().key();
            if (key != null) {
                keyData = new byte[key.remaining()];
                key.get(keyData);
            }

            ByteBuffer message = messageAndOffset.message().payload();
            if (message != null) {
                messageData = new byte[message.remaining()];
                message.get(messageData);
            }

            Set<FieldValueProvider> fieldValueProviders = new HashSet<>();

            fieldValueProviders.addAll(globalInternalFieldValueProviders);
            fieldValueProviders.add(KafkaInternalFieldDescription.SEGMENT_COUNT_FIELD.forLongValue(totalMessages));
            fieldValueProviders.add(KafkaInternalFieldDescription.PARTITION_OFFSET_FIELD.forLongValue(messageAndOffset.offset()));
            fieldValueProviders.add(KafkaInternalFieldDescription.MESSAGE_FIELD.forByteValue(messageData));
            fieldValueProviders.add(KafkaInternalFieldDescription.MESSAGE_LENGTH_FIELD.forLongValue(messageData.length));
            fieldValueProviders.add(KafkaInternalFieldDescription.KEY_FIELD.forByteValue(keyData));
            fieldValueProviders.add(KafkaInternalFieldDescription.KEY_LENGTH_FIELD.forLongValue(keyData.length));
            fieldValueProviders.add(KafkaInternalFieldDescription.KEY_CORRUPT_FIELD.forBooleanValue(keyDecoder.decodeRow(keyData, null, fieldValueProviders, columnHandles, keyFieldDecoders)));
            fieldValueProviders.add(KafkaInternalFieldDescription.MESSAGE_CORRUPT_FIELD.forBooleanValue(messageDecoder.decodeRow(messageData, null, fieldValueProviders, columnHandles, messageFieldDecoders)));

            this.fieldValueProviders = new FieldValueProvider[columnHandles.size()];

            // If a value provider for a requested internal column is present, assign the
            // value to the internal cache. It is possible that an internal column is present
            // where no value provider exists (e.g. the '_corrupt' column with the DummyRowDecoder).
            // In that case, the cache is null (and the column is reported as null).
            for (int i = 0; i < columnHandles.size(); i++) {
                for (FieldValueProvider fieldValueProvider : fieldValueProviders) {
                    if (fieldValueProvider.accept(columnHandles.get(i))) {
                        this.fieldValueProviders[i] = fieldValueProvider;
                        break; // for(InternalColumnProvider...
                    }
                }
            }

            return true; // Advanced successfully.
        }

        @SuppressWarnings("SimplifiableConditionalExpression")
        @Override
        public boolean getBoolean(int field)
        {
            checkArgument(field < columnHandles.size(), "Invalid field index");

            checkFieldType(field, boolean.class);
            return isNull(field) ? false : fieldValueProviders[field].getBoolean();
        }

        @Override
        public long getLong(int field)
        {
            checkArgument(field < columnHandles.size(), "Invalid field index");

            checkFieldType(field, long.class);
            return isNull(field) ? 0L : fieldValueProviders[field].getLong();
        }

        @Override
        public double getDouble(int field)
        {
            checkArgument(field < columnHandles.size(), "Invalid field index");

            checkFieldType(field, double.class);
            return isNull(field) ? 0.0d : fieldValueProviders[field].getDouble();
        }

        @Override
        public Slice getSlice(int field)
        {
            checkArgument(field < columnHandles.size(), "Invalid field index");

            checkFieldType(field, Slice.class);
            return isNull(field) ? Slices.EMPTY_SLICE : fieldValueProviders[field].getSlice();
        }

        @Override
        public Object getObject(int field)
        {
            checkArgument(field < columnHandles.size(), "Invalid field index");
            checkFieldType(field, Block.class);
            return isNull(field) ? null : fieldValueProviders[field].getObject();
        }

        @Override
        public boolean isNull(int field)
        {
            checkArgument(field < columnHandles.size(), "Invalid field index");

            return fieldValueProviders[field] == null || fieldValueProviders[field].isNull();
        }

        private void checkFieldType(int field, Class<?> expected)
        {
            Class<?> actual = getType(field).getJavaType();
            checkArgument(actual == expected, "Expected field %s to be type %s but is %s", field, expected, actual);
        }

        @Override
        public void close()
        {
        }

        private void openFetchRequest()
        {
            if (messageAndOffsetIterator == null) {
                log.debug("Fetching %d bytes from offset %d (%d - %d). %d messages read so far", KAFKA_READ_BUFFER_SIZE, cursorOffset, split.getStart(), split.getEnd(), totalMessages);
                FetchRequest req = new FetchRequestBuilder()
                        .clientId("presto-worker-" + Thread.currentThread().getName())
                        .addFetch(split.getTopicName(), split.getPartitionId(), cursorOffset, KAFKA_READ_BUFFER_SIZE)
                        .maxWait(10000)
                        .build();

                // TODO - this should look at the actual node this is running on and prefer
                // that copy if running locally. - look into NodeInfo
                boolean commFailure = false;
                SimpleConsumer consumer = consumerManager.getConsumer(split.getLeader());
                FetchResponse fetchResponse = null;
                try {
                    fetchResponse = consumer.fetch(req);
                    log.debug("Fetch request returned successfully");
                }
                catch (Exception e) {
                    commFailure = true;
                    log.warn("Kafka consumer failed to talk to:  %s, %s", split.getLeader(),
                            Throwables.getStackTraceAsString(e));
                }
                if (commFailure || fetchResponse.hasError()) {
                    short errorCode = (fetchResponse == null ? -1 : fetchResponse.errorCode(split.getTopicName(), split.getPartitionId()));
                    String errorMsg = String.format("FAILED! Fetching %d bytes from offset %d (%d - %d). %d messages read so far from broker: %s, error code: %d", KAFKA_READ_BUFFER_SIZE, cursorOffset, split.getStart(), split.getEnd(), totalMessages, split.getLeader(), errorCode);

                    log.error(errorMsg);
                    consumerManager.invalidateConsumer(split.getLeader());
                    throw new PrestoException(KAFKA_SPLIT_ERROR, errorMsg);
                }

                messageAndOffsetIterator = fetchResponse.messageSet(split.getTopicName(), split.getPartitionId()).iterator();
            }
        }
    }
}
