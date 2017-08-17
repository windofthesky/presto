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
package com.facebook.presto.hive;

import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.AbstractArrayBlock;
import com.facebook.presto.spi.block.AbstractMapBlock;
import com.facebook.presto.spi.block.AbstractSingleMapBlock;
import com.facebook.presto.spi.block.ArrayBlock;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.LazyBlock;
import com.facebook.presto.spi.block.MapBlock;
import com.facebook.presto.spi.block.SingleMapBlock;
import com.facebook.presto.spi.function.OperatorType;
import com.facebook.presto.spi.type.ArrayType;
import com.facebook.presto.spi.type.MapType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.collect.ImmutableList;
import org.joda.time.DateTimeZone;

import java.lang.invoke.MethodHandle;
import java.util.List;
import java.util.function.LongUnaryOperator;

import static com.facebook.presto.spi.block.MethodHandleUtil.compose;
import static com.facebook.presto.spi.block.MethodHandleUtil.nativeValueGetter;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.google.common.base.Preconditions.checkArgument;

public class TimestampRewriter
{
    private final List<Type> columnTypes;
    private final DateTimeZone storageTimeZone;
    private final TypeManager typeManager;

    public TimestampRewriter(List<Type> columnTypes, DateTimeZone storageTimeZone, TypeManager typeManager)
    {
        this.columnTypes = columnTypes;
        this.storageTimeZone = storageTimeZone;
        this.typeManager = typeManager;
    }

    public Page rewritePageHiveToPresto(Page page)
    {
        return modifyTimestampsInPage(page, millis -> millis + storageTimeZone.getOffset(millis));
    }

    public Page rewritePagePrestoToHive(Page page)
    {
        return modifyTimestampsInPage(page, millis -> millis - storageTimeZone.getOffset(millis));
    }

    private Page modifyTimestampsInPage(Page page, LongUnaryOperator modification)
    {
        if (page == null) {
            return null;
        }

        checkArgument(page.getChannelCount() == columnTypes.size());
        Block[] blocks = new Block[page.getChannelCount()];

        for (int i = 0; i < page.getChannelCount(); ++i) {
            blocks[i] = wrapBlockInLazyTimestampRewritingBlock(page.getBlock(i), columnTypes.get(i), modification);
        }

        return new Page(blocks);
    }

    private Block wrapBlockInLazyTimestampRewritingBlock(Block block, Type type, LongUnaryOperator modification)
    {
        if (!hasTimestampParameter(type)) {
            return block;
        }
        return new LazyBlock(block.getPositionCount(), lazyBlock -> lazyBlock.setBlock(modifyTimestampsInBlock(block, type, modification)));
    }

    private Block modifyTimestampsInBlock(Block block, Type type, LongUnaryOperator modification)
    {
        if (type.equals(TIMESTAMP)) {
            return modifyTimestampsInTimestampBlock(block, modification);
        }
        if (type instanceof ArrayType) {
            return modifyTimestampsInArrayBlock(type, block, modification);
        }
        if (type instanceof MapType) {
            return modifyTimestampsInMapBlock(type, block, modification);
        }

        return block; // REMOVE AND UNCOMNET
        //throw new UnsupportedOperationException("Complex type " + type.toString() + " support is missing.");
    }

    private boolean hasTimestampParameter(Type type)
    {
        if (type.equals(TIMESTAMP)) {
            return true;
        }
        if (type.getTypeParameters().size() > 0) {
            return type.getTypeParameters().stream().anyMatch(x -> hasTimestampParameter(x));
        }
        return false;
    }

    private Block modifyTimestampsInArrayBlock(Type type, Block block, LongUnaryOperator modification)
    {
        if (block == null) {
            return null;
        }

        if (block instanceof AbstractArrayBlock) {
            AbstractArrayBlock arrayBlock = (AbstractArrayBlock) block;
            Block innerBlock = wrapBlockInLazyTimestampRewritingBlock(arrayBlock.getValues(),
                    type.getTypeParameters().get(0),
                    modification);

            return new ArrayBlock(arrayBlock.getPositionCount(), arrayBlock.getValueIsNull(), arrayBlock.getOffsets(), innerBlock);
        }
        else {
            BlockBuilder builder = type.createBlockBuilder(new BlockBuilderStatus(), block.getPositionCount());
            for (int i = 0; i < block.getPositionCount(); ++i) {
                type.writeObject(builder, wrapBlockInLazyTimestampRewritingBlock(block.getObject(i, Block.class), type.getTypeParameters().get(0), modification));
            }
            return builder.build();
        }
    }

    private Block modifyTimestampsInMapBlock(Type type, Block block, LongUnaryOperator modification)
    {
        if (block == null) {
            return null;
        }

        Type keyType = type.getTypeParameters().get(0);
        Type valueType = type.getTypeParameters().get(1);
        MethodHandle keyNativeEquals = typeManager.resolveOperator(OperatorType.EQUAL, ImmutableList.of(keyType, keyType));
        MethodHandle keyBlockNativeEquals = compose(keyNativeEquals, nativeValueGetter(keyType));
        MethodHandle keyNativeHashCode = typeManager.resolveOperator(OperatorType.HASH_CODE, ImmutableList.of(keyType));
        MethodHandle keyBlockHashCode = compose(keyNativeHashCode, nativeValueGetter(keyType));

        if (block instanceof AbstractMapBlock) {
            AbstractMapBlock mapBlock = (AbstractMapBlock) block;
            Block innerKeyBlock = wrapBlockInLazyTimestampRewritingBlock(mapBlock.getKeys(), keyType, modification);
            Block innerValueBlock = wrapBlockInLazyTimestampRewritingBlock(mapBlock.getValues(), valueType, modification);

            return MapBlock.fromKeyValueBlock(mapBlock.getMapIsNull(),
                    mapBlock.getOffsets(),
                    innerKeyBlock,
                    innerValueBlock,
                    (MapType) type,
                    keyBlockNativeEquals,
                    keyNativeHashCode,
                    keyBlockHashCode);
        }
        else {
            BlockBuilder builder = type.createBlockBuilder(new BlockBuilderStatus(), block.getPositionCount());
            for (int i = 0; i < block.getPositionCount(); ++i) {
                type.writeObject(builder, modifyTimestampsInSingleMapBlock(type, block.getObject(i, Block.class), modification));
            }
            return builder.build();
        }
    }

    private Block modifyTimestampsInSingleMapBlock(Type type, Block block, LongUnaryOperator modification)
    {
        checkArgument(block instanceof AbstractSingleMapBlock, "Maps represented by other type than AbstractSingleMapBlock are not supported.");
        if (block == null) {
            return null;
        }

        AbstractSingleMapBlock mapBlock = (AbstractSingleMapBlock) block;
        Type keyType = type.getTypeParameters().get(0);
        Type valueType = type.getTypeParameters().get(1);
        MethodHandle keyNativeEquals = typeManager.resolveOperator(OperatorType.EQUAL, ImmutableList.of(keyType, keyType));
        MethodHandle keyBlockNativeEquals = compose(keyNativeEquals, nativeValueGetter(keyType));
        MethodHandle keyNativeHashCode = typeManager.resolveOperator(OperatorType.HASH_CODE, ImmutableList.of(keyType));

        Block innerKeyBlock = wrapBlockInLazyTimestampRewritingBlock(mapBlock.getKeyBlock(), keyType, modification);
        Block innerValueBlock = wrapBlockInLazyTimestampRewritingBlock(mapBlock.getValueBlock(), valueType, modification);

        // We use null as hash in SingleMapBlock, as they are recalculated on MapBlock.closeEntry();
        return new SingleMapBlock(0, mapBlock.getPositionCount(), innerKeyBlock, innerValueBlock, null, keyType, keyNativeHashCode, keyBlockNativeEquals);
    }

    private Block modifyTimestampsInTimestampBlock(Block block, LongUnaryOperator modification)
    {
        if (block == null) {
            return null;
        }

        BlockBuilder blockBuilder = TIMESTAMP.createFixedSizeBlockBuilder(block.getPositionCount());
        for (int i = 0; i < block.getPositionCount(); ++i) {
            long millis = block.getLong(i, 0);
            blockBuilder.writeLong(modification.applyAsLong(millis));
        }

        return blockBuilder.build();
    }
}
