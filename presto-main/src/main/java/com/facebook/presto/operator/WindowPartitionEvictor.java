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
package com.facebook.presto.operator;

import com.facebook.presto.operator.window.WindowPartition;

import java.util.ArrayList;
import java.util.List;

import static com.facebook.presto.operator.SyntheticAddress.decodeSliceIndex;
import static com.facebook.presto.spi.block.BlockBuilderStatus.DEFAULT_MAX_BLOCK_SIZE_IN_BYTES;

public class WindowPartitionEvictor
{
    private final PagesIndex pagesIndex;
    private final List<List<Integer>> blocksToPartitionIndexes;
    private final List<Integer> finishedPartitionEnds = new ArrayList<>();

    private int firstBlock = 0;
    private int lastBlock = 0;
    private int numEvictions = 0;

    public WindowPartitionEvictor(PagesIndex pagesIndex, int expectedPositions)
    {
        this.pagesIndex = pagesIndex;
        this.blocksToPartitionIndexes = new ArrayList<>(expectedPositions / DEFAULT_MAX_BLOCK_SIZE_IN_BYTES);
    }

    public int getNumEvictions()
    {
        return numEvictions;
    }

    public void finishPartition(WindowPartition partition)
    {
        finishedPartitionEnds.add(partition.getPartitionEnd());
    }

    public void evictFinishedPartitions()
    {
        for (int i = firstBlock; i <= lastBlock; i++) {
            List<Integer> partitions = blocksToPartitionIndexes.get(i);
            if (partitions.isEmpty()) {
                if (i == firstBlock) {
                    firstBlock++;
                }
                continue;
            }

            partitions.removeAll(finishedPartitionEnds); // todo: you only need to remove the first ones b/c it's in order
            if (partitions.isEmpty() && i != lastBlock) {
                pagesIndex.evictBlock(i);
                numEvictions++;
                if (i == firstBlock) {
                    firstBlock++;
                }
            }
        }
        finishedPartitionEnds.clear();
    }

    /**
     * Must be done before the partition is sorted in the PagesIndex
     */
    public void registerPartition(int partitionStart, int partitionEnd)
    {
        long startPageAddress = pagesIndex.getValueAddresses().getLong(partitionStart);
        long endPageAddress = pagesIndex.getValueAddresses().getLong(partitionEnd - 1);
        int startBlockIndex = decodeSliceIndex(startPageAddress);
        int endBlockIndex = decodeSliceIndex(endPageAddress);

        for (int i = startBlockIndex; i <= endBlockIndex; i++) {
            if (i <= blocksToPartitionIndexes.size() || blocksToPartitionIndexes.get(i) == null) {
                List<Integer> partitions = new ArrayList<>();
                partitions.add(partitionEnd);
                blocksToPartitionIndexes.add(i, partitions);
            }
            else {
                blocksToPartitionIndexes.get(i).add(partitionEnd);
            }
            // TODO: make blocksToPartitionIndexes an ObjectArrayList or something that's faster
        }
        lastBlock = Math.max(lastBlock, endBlockIndex);
    }

    // TODO add jmx stats here
}
