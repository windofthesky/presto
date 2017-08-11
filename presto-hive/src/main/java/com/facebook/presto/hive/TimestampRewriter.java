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
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.Type;
import org.joda.time.DateTimeZone;

import java.util.List;
import java.util.function.Function;

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

    private Page modifyTimestampsInPage(Page page, Function<Long, Long> modification)
    {
        if (page == null) {
            return null;
        }

        checkArgument(page.getChannelCount() == columnTypes.size());
        Block[] blocks = new Block[page.getChannelCount()];

        for (int i = 0; i < page.getChannelCount(); ++i) {
            if (columnTypes.get(i).equals(TIMESTAMP)) {
                blocks[i] = modifyTimestampsInBlock(page.getBlock(i), modification);
            }
            else {
                blocks[i] = page.getBlock(i);
            }
        }

        return new Page(blocks);
    }

    private Block modifyTimestampsInBlock(Block block, Function<Long, Long> modification)
    {
        if (block == null) {
            return null;
        }

        BlockBuilder blockBuilder = TIMESTAMP.createFixedSizeBlockBuilder(block.getPositionCount());
        for (int i = 0; i < block.getPositionCount(); ++i) {
            long millis = block.getLong(i, 0);
            blockBuilder.writeLong(modification.apply(millis));
        }

        return blockBuilder.build();
    }
}
