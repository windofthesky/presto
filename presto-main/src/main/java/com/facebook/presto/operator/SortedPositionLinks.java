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

import com.facebook.presto.spi.Page;
import com.google.common.collect.ImmutableList;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntComparator;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import org.openjdk.jol.info.ClassLayout;

import java.util.List;

import static com.facebook.presto.operator.SyntheticAddress.decodePosition;
import static com.facebook.presto.operator.SyntheticAddress.decodeSliceIndex;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.util.Objects.requireNonNull;

/**
 * This class keeps a list of non-equi filter conditions in a join. We can use
 * SortedPositionLinks with following join condition:
 * <p>
 * <p>
 * {@code filterFunction_1(...) AND filterFunction_2(....) AND ... AND filterFunction_n(...)}
 * <p>
 * by passing any of the filterFunction_i to the SortedPositionLinks. We could not
 * do that for join condition like:
 * <p>
 * {@code filterFunction_1(...) OR filterFunction_2(....) OR ... OR filterFunction_n(...)}
 * <p>
 * To use the elements in the list of filters searchFunctions in this class,
 * it must be an expression in form of:
 * <p>
 * {@code f(probeColumn1, probeColumn2, ..., probeColumnN) COMPARE buildSymbolRef}
 * <p>
 * where {@code COMPARE} is one of: {@code < <= > >=}
 * <p>
 * and buildSymbolRef is a symbol reference on the build side of join.
 * <p>
 * That allows us to define an order of the elements in positionLinks by sorting them by
 * {@code buildSymbolRef} and then perform a binary search using {@code f(probePosition)}.
 */
public final class SortedPositionLinks
        implements PositionLinks
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(SortedPositionLinks.class).instanceSize();

    public static class FactoryBuilder
            implements PositionLinks.FactoryBuilder
    {
        private final Int2ObjectMap<IntArrayList> positionLinks;
        private final int size;
        private final IntComparator comparator;
        private final PagesHashStrategy pagesHashStrategy;
        private final LongArrayList addresses;

        public FactoryBuilder(int size, PagesHashStrategy pagesHashStrategy, LongArrayList addresses)
        {
            this.size = size;
            this.comparator = new PositionComparator(pagesHashStrategy, addresses);
            this.pagesHashStrategy = pagesHashStrategy;
            this.addresses = addresses;
            positionLinks = new Int2ObjectOpenHashMap<>();
        }

        @Override
        public int link(int from, int to)
        {
            // don't add _from_ row to chain if its sort channel value is null
            if (isNull(from)) {
                // _to_ row sort channel value might be null. However, in such
                // case it will be the only element in the chain, so sorted position
                // links enumeration will produce correct results.
                return to;
            }

            // don't add _to_ row to chain if its sort channel value is null
            if (isNull(to)) {
                return from;
            }

            // make sure that from value is the smaller one
            if (comparator.compare(from, to) > 0) {
                // _from_ is larger so, just add to current chain _to_
                List<Integer> links = positionLinks.computeIfAbsent(to, key -> new IntArrayList());
                links.add(from);
                return to;
            }
            else {
                // _to_ is larger so, move the chain to _from_
                IntArrayList links = positionLinks.remove(to);
                if (links == null) {
                    links = new IntArrayList();
                }
                links.add(to);
                checkState(positionLinks.putIfAbsent(from, links) == null, "sorted links is corrupted");
                return from;
            }
        }

        private boolean isNull(int position)
        {
            long pageAddress = addresses.getLong(position);
            int blockIndex = decodeSliceIndex(pageAddress);
            int blockPosition = decodePosition(pageAddress);
            return pagesHashStrategy.isSortChannelPositionNull(blockIndex, blockPosition);
        }

        @Override
        public Factory build()
        {
            ArrayPositionLinks.FactoryBuilder arrayPositionLinksFactoryBuilder = ArrayPositionLinks.builder(size);
            int[][] sortedPositionLinks = new int[size][];

            for (Int2ObjectMap.Entry<IntArrayList> entry : positionLinks.int2ObjectEntrySet()) {
                int key = entry.getIntKey();
                IntArrayList positions = entry.getValue();
                positions.sort(comparator);

                sortedPositionLinks[key] = new int[positions.size()];
                for (int i = 0; i < positions.size(); i++) {
                    sortedPositionLinks[key][i] = positions.get(i);
                }

                // ArrayPositionsLinks.Builder::link builds position links from
                // tail to head, so we must add them in descending order to have
                // smallest element as a head
                for (int i = positions.size() - 2; i >= 0; i--) {
                    arrayPositionLinksFactoryBuilder.link(positions.get(i), positions.get(i + 1));
                }

                // add link from starting position to position links chain
                if (!positions.isEmpty()) {
                    arrayPositionLinksFactoryBuilder.link(key, positions.get(0));
                }
            }

            return searchFunctions -> new SortedPositionLinks(
                    arrayPositionLinksFactoryBuilder.build().create(ImmutableList.of()),
                    sortedPositionLinks,
                    searchFunctions);
        }

        @Override
        public int size()
        {
            return positionLinks.size();
        }
    }

    private final PositionLinks positionLinks;
    private final int[][] sortedPositionLinks;
    private final long sizeInBytes;
    private final List<JoinFilterFunction> searchFunctions;

    private SortedPositionLinks(PositionLinks positionLinks, int[][] sortedPositionLinks, List<JoinFilterFunction> searchFunctions)
    {
        this.positionLinks = requireNonNull(positionLinks, "positionLinks is null");
        this.sortedPositionLinks = requireNonNull(sortedPositionLinks, "sortedPositionLinks is null");
        this.sizeInBytes = INSTANCE_SIZE + positionLinks.getSizeInBytes() + sizeOfPositionLinks(sortedPositionLinks);
        checkState(!searchFunctions.isEmpty(), "Using sortedPositionLinks with no search functions");
        this.searchFunctions = requireNonNull(searchFunctions, "searchFunctions is null");
    }

    private long sizeOfPositionLinks(int[][] sortedPositionLinks)
    {
        long retainedSize = sizeOf(sortedPositionLinks);
        for (int[] element : sortedPositionLinks) {
            retainedSize += sizeOf(element);
        }
        return retainedSize;
    }

    public static FactoryBuilder builder(int size, PagesHashStrategy pagesHashStrategy, LongArrayList addresses)
    {
        return new FactoryBuilder(size, pagesHashStrategy, addresses);
    }

    @Override
    public int next(int position, int probePosition, Page allProbeChannelsPage)
    {
        int nextPosition = positionLinks.next(position, probePosition, allProbeChannelsPage);
        if (nextPosition < 0) {
            return -1;
        }
        // break a position links chain if next position should be filtered out
        for (JoinFilterFunction searchFunction : searchFunctions) {
            // return if any of the filter conjuncts is false
            if (!applyLessThanFunction(searchFunction, nextPosition, probePosition, allProbeChannelsPage)) {
                return -1;
            }
        }
        return nextPosition;
    }

    @Override
    public int start(int startingPosition, int probePosition, Page allProbeChannelsPage)
    {
        for (JoinFilterFunction searchFunction : searchFunctions) {
            startingPosition = findStartPositionForFunction(searchFunction, startingPosition, probePosition, allProbeChannelsPage);
            // return as soon as a mismatch is found, since we are handling only AND predicates (conjuncts)
            if (startingPosition == -1) {
                return -1;
            }
        }
        return startingPosition;
    }

    private int findStartPositionForFunction(JoinFilterFunction filterFunction, int startingPosition, int probePosition, Page allProbeChannelsPage)
    {
        // check if filtering function to startingPosition
        if (applyLessThanFunction(filterFunction, startingPosition, probePosition, allProbeChannelsPage)) {
            return startingPosition;
        }

        if (sortedPositionLinks[startingPosition] == null) {
            return -1;
        }

        int left = 0;
        int right = sortedPositionLinks[startingPosition].length - 1;

        // do a binary search for the first position for which filter function applies
        int offset = lowerBound(filterFunction, startingPosition, left, right, probePosition, allProbeChannelsPage);
        if (offset < 0) {
            return -1;
        }
        if (!applyLessThanFunction(filterFunction, startingPosition, offset, probePosition, allProbeChannelsPage)) {
            return -1;
        }
        return sortedPositionLinks[startingPosition][offset];
    }

    /**
     * Find the first element in position links that is NOT smaller than probePosition
     */
    private int lowerBound(JoinFilterFunction function, int startingPosition, int first, int last, int probePosition, Page allProbeChannelsPage)
    {
        int middle;
        int step;
        int count = last - first;
        while (count > 0) {
            step = count / 2;
            middle = first + step;
            if (!applyLessThanFunction(function, startingPosition, middle, probePosition, allProbeChannelsPage)) {
                first = ++middle;
                count -= step + 1;
            }
            else {
                count = step;
            }
        }
        return first;
    }

    @Override
    public long getSizeInBytes()
    {
        return sizeInBytes;
    }

    private boolean applyLessThanFunction(JoinFilterFunction function, int leftPosition, int leftOffset, int rightPosition, Page rightPage)
    {
        return applyLessThanFunction(function, sortedPositionLinks[leftPosition][leftOffset], rightPosition, rightPage);
    }

    private boolean applyLessThanFunction(JoinFilterFunction function, long leftPosition, int rightPosition, Page rightPage)
    {
        return function.filter((int) leftPosition, rightPosition, rightPage);
    }

    private static class PositionComparator
            implements IntComparator
    {
        private final PagesHashStrategy pagesHashStrategy;
        private final LongArrayList addresses;

        PositionComparator(PagesHashStrategy pagesHashStrategy, LongArrayList addresses)
        {
            this.pagesHashStrategy = pagesHashStrategy;
            this.addresses = addresses;
        }

        @Override
        public int compare(int leftPosition, int rightPosition)
        {
            long leftPageAddress = addresses.getLong(leftPosition);
            int leftBlockIndex = decodeSliceIndex(leftPageAddress);
            int leftBlockPosition = decodePosition(leftPageAddress);

            long rightPageAddress = addresses.getLong(rightPosition);
            int rightBlockIndex = decodeSliceIndex(rightPageAddress);
            int rightBlockPosition = decodePosition(rightPageAddress);

            return pagesHashStrategy.compareSortChannelPositions(leftBlockIndex, leftBlockPosition, rightBlockIndex, rightBlockPosition);
        }

        @Override
        public int compare(Integer leftPosition, Integer rightPosition)
        {
            return compare(leftPosition.intValue(), rightPosition.intValue());
        }
    }
}
