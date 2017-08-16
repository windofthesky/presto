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
import com.facebook.presto.spi.block.ArrayBlock;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.LazyBlock;
import com.facebook.presto.spi.type.ArrayType;
import com.facebook.presto.spi.type.Type;
import org.joda.time.DateTimeZone;

import java.util.List;
import java.util.function.LongUnaryOperator;

import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static org.weakref.jmx.internal.guava.base.Preconditions.checkArgument;

public class TimestampRewriter
{
    private final List<Type> columnTypes;
    private final DateTimeZone storageTimeZone;

    public TimestampRewriter(List<Type> columnTypes, DateTimeZone storageTimeZone)
    {
        this.columnTypes = columnTypes;
        this.storageTimeZone = storageTimeZone;
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

    private static Block wrapBlockInLazyTimestampRewritingBlock(Block block, Type type, LongUnaryOperator modification)
    {
        if (!hasTimestampParameter(type)) {
            return block;
        }
        return new LazyBlock(block.getPositionCount(), lazyBlock -> lazyBlock.setBlock(modifyTimestampsInBlock(block, type, modification)));
    }

    private static Block modifyTimestampsInBlock(Block block, Type type, LongUnaryOperator modification)
    {
        if (type.equals(TIMESTAMP)) {
            return modifyTimestampsInTimestampBlock(block, modification);
        }
        if (type instanceof ArrayType) {
            return modifyTimestampsInArrayBlock(type, block, modification);
        }

        // TODO THROW UNSUPPORTED EXCEPTION
        return block;
    }

    private static boolean hasTimestampParameter(Type type)
    {
        if (type.equals(TIMESTAMP)) {
            return true;
        }
        if (type.getTypeParameters().size() > 0) {
            return type.getTypeParameters().stream().anyMatch(x -> hasTimestampParameter(x));
        }
        return false;
    }

    private static Block modifyTimestampsInArrayBlock(Type type, Block block, LongUnaryOperator modification)
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

    private static Block modifyTimestampsInTimestampBlock(Block block, LongUnaryOperator modification)
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
