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

import com.facebook.presto.operator.Menago.SpillingStateSnapshot;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spiller.SingleStreamSpiller;
import com.facebook.presto.spiller.SingleStreamSpillerFactory;
import com.google.common.collect.AbstractIterator;
import com.google.common.io.Closer;
import com.google.common.util.concurrent.ListenableFuture;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

import static com.google.common.base.Preconditions.checkState;
import static io.airlift.concurrent.MoreFutures.tryGetFutureValue;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public final class LookupJoiner
        implements Closeable
{
    private static final int MAX_POSITIONS_EVALUATED_PER_CALL = 10000;

    private /*final*/ Menago menago;
    private /*final*/ SingleStreamSpillerFactory singleStreamSpillerFactory;
    private final ListenableFuture<? extends LookupSource> lookupSourceFuture;
    private final JoinProbeFactory joinProbeFactory;
    private final boolean probeOnOuterSide;

    @GuardedBy("this")
    private LookupSource lookupSource;

    @GuardedBy("this")
    private Optional<Page> rejectedInputPage;

    @GuardedBy("this")
    private JoinProbe probe;

    @GuardedBy("this")
    private Optional<SingleStreamSpiller> outputSpiller;

    @GuardedBy("this")
    private final PageBuilder pageBuilder;

    @GuardedBy("this")
    private long joinPosition = -1;

    @GuardedBy("this")
    private boolean currentProbePositionProducedRow;

    @GuardedBy("this")
    private boolean finishing;

    public LookupJoiner(
            List<Type> types,
            ListenableFuture<? extends LookupSource> lookupSourceFuture,
            JoinProbeFactory joinProbeFactory,
            boolean probeOnOuterSide)
    {
        this.pageBuilder = new PageBuilder(types);
        this.lookupSourceFuture = requireNonNull(lookupSourceFuture, "lookupSourceFuture is null");
        this.joinProbeFactory = requireNonNull(joinProbeFactory, "joinProbeFactory is null");
        this.probeOnOuterSide = probeOnOuterSide;
//        this.spillingStateSnapshot = menago.getSpillingState();
    }

    public synchronized boolean needsInput()
    {
        if (finishing) {
            return false;
        }

        if (probe != null) {
            // Already consumed some input that needs to be processed
            return false;
        }

//        if (rejectedInputPage != null) {
//            return false;
//        }

        if (outputSpiller.isPresent()) {
            return false;
        }

        if (lookupSource == null) {
            lookupSource = tryGetFutureValue(lookupSourceFuture).orElse(null);
            if (lookupSource == null) {
                // Not ready to consume input
                return false;
            }
        }

        return true;
    }

    public /*synchronized*/ ListenableFuture<?> isBlocked()
    {
//        if (!lookupSourceFuture.isDone()) {
//            return lookupSourceFuture;
//        }
//        if (!outputSpillingFuture.isDone()) {
//            return outputSpillingFuture;
//        }
//        return NOT_BLOCKED;
        return lookupSourceFuture;
    }

    public synchronized void finish()
    {
        finishing = true;
    }

    public synchronized boolean hasRejectedInputPage()
    {
        return rejectedInputPage.isPresent();
    }

    public synchronized Optional<Page> takeRejectedInputPage()
    {
        Optional<Page> rejectedInputPage = this.rejectedInputPage;
        this.rejectedInputPage = Optional.empty();
        return rejectedInputPage;
    }

    public synchronized boolean isFinished()
    {
        return finishing && !hasOutput() && !hasRejectedInputPage();
    }

    @GuardedBy("this")
    private boolean hasOutput()
    {
        return probe != null || !pageBuilder.isEmpty();
    }

    public synchronized void addInput(Page page, SpillingStateSnapshot spillingState)
    {
        requireNonNull(page, "page is null");
        checkState(!finishing, "Operator is finishing");
        checkState(lookupSource != null, "Lookup source has not been built yet");
        checkState(probe == null, "Current page has not been completely processed yet");
        checkState(rejectedInputPage == null, "There is a rejected input page that needs re-processing");

        if (!menago.startJoinProbe(spillingState, this)) {
            /*
             * Spilling state has changed in the meantime, so `page` can contain positions that should be spilled
             * before calling here. `page` will be returned back to LookupJoinOperator for re-processing.
             */
            rejectedInputPage = Optional.of(page);
            return;
        }

        // create probe
        probe = joinProbeFactory.createJoinProbe(lookupSource, page);
//        probeSpillingState = spillingState;

        // initialize to invalid join position to force output code to advance the cursors
        joinPosition = -1;
    }

    public synchronized Page getOutput()
    {
//        if (!outputSpillingFuture.isDone()) {
//            return null;
//        }
        return getOutput(finishing);
    }

    @GuardedBy("this")
    private Page getOutput(boolean flush)
    {
        if (lookupSource == null) {
            return null;
        }

        // join probe page with the lookup source
        Counter lookupPositionsConsidered = new Counter();
        if (probe != null) {
            while (true) {
                if (probe.getPosition() >= 0) {
                    if (!joinCurrentPosition(lookupPositionsConsidered)) {
                        break;
                    }
                    if (!currentProbePositionProducedRow) {
                        currentProbePositionProducedRow = true;
                        if (!outerJoinCurrentPosition()) {
                            break;
                        }
                    }
                }
                currentProbePositionProducedRow = false;
                if (!advanceProbePosition()) {
                    break;
                }
            }
        }

        // only flush full pages unless we are done
        if (pageBuilder.isFull() || (flush && !pageBuilder.isEmpty() && probe == null)) {
            Page page = pageBuilder.build();
            pageBuilder.reset();
            return page;
        }

        return null;
    }

    /**
     * Called when a partition of a {@link LookupSource} needs to be spilled. For that exiting probes
     */
    public synchronized void notifySpillingStateChanged()
    {
//        requireNonNull(newSpillingState, "newSpillingState is null");

        if (!hasOutput()) {
            return;
        }

        ListenableFuture<?> outputSpillingFuture = getSpiller().spill(new AbstractIterator<Page>()
        {
            @Override
            @SuppressWarnings("FieldAccessNotGuarded")
            protected Page computeNext()
            {
                // getOutput() may return null even if there is data, thus we need loop
                Page page = null;
                while (probe != null && page == null) {
                    page = getOutput(true);
                }

                if (page == null) {
                    return endOfData();
                }
                return page;
            }
        });
        try {
            outputSpillingFuture.get();
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted");
        }
        catch (ExecutionException e) {
            throw new RuntimeException("Spilling failed", e);
        }
    }

    @GuardedBy("this")
    private SingleStreamSpiller getSpiller()
    {
        if (!outputSpiller.isPresent()) {
            outputSpiller = Optional.of(createSpiller());
        }
        return outputSpiller.get();
    }

    private SingleStreamSpiller createSpiller()
    {

    }

    @Override
    public void close()
    {
        Closer closer = Closer.create();
        synchronized (this) {
            closeJoinProbe();
            pageBuilder.reset();

            // closing lookup source is only here for index join
            Optional.ofNullable(lookupSource).ifPresent(closer::register);
            outputSpiller.ifPresent(closer::register);
        }

        try {
            closer.close();
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Produce rows matching join condition for the current probe position. If this method was called previously
     * for the current probe position, calling this again will produce rows that wasn't been produced in previous
     * invocations.
     *
     * @return true if all eligible rows have been produced; false otherwise (because pageBuilder became full)
     */
    @GuardedBy("this")
    private boolean joinCurrentPosition(Counter lookupPositionsConsidered)
    {
        // while we have a position on lookup side to join against...
        while (joinPosition >= 0) {
            lookupPositionsConsidered.increment();
            if (lookupSource.isJoinPositionEligible(joinPosition, probe.getPosition(), probe.getPage())) {
                currentProbePositionProducedRow = true;

                pageBuilder.declarePosition();
                // write probe columns
                probe.appendTo(pageBuilder);
                // write build columns
                lookupSource.appendTo(joinPosition, pageBuilder, probe.getOutputChannelCount());
            }

            // get next position on lookup side for this probe row
            joinPosition = lookupSource.getNextJoinPosition(joinPosition, probe.getPosition(), probe.getPage());

            if (lookupPositionsConsidered.get() >= MAX_POSITIONS_EVALUATED_PER_CALL) {
                return false;
            }
            if (pageBuilder.isFull()) {
                return false;
            }
        }
        return true;
    }

    /**
     * @return whether there are more positions on probe side
     */
    @GuardedBy("this")
    private boolean advanceProbePosition()
    {
        if (!probe.advanceNextPosition()) {
            closeJoinProbe();
            return false;
        }

        // update join position
        joinPosition = probe.getCurrentJoinPosition();
        return true;
    }

    @GuardedBy("this")
    private void closeJoinProbe() {
        probe = null;
        menago.finishJoinProbe(this);
    }

    /**
     * Produce a row for the current probe position, if it doesn't match any row on lookup side and this is an outer join.
     *
     * @return whether pageBuilder became full
     */
    @GuardedBy("this")
    private boolean outerJoinCurrentPosition()
    {
        if (probeOnOuterSide && joinPosition < 0) {
            // write probe columns
            pageBuilder.declarePosition();
            probe.appendTo(pageBuilder);

            // write nulls into build columns
            int outputIndex = probe.getOutputChannelCount();
            for (int buildChannel = 0; buildChannel < lookupSource.getChannelCount(); buildChannel++) {
                pageBuilder.getBlockBuilder(outputIndex).appendNull();
                outputIndex++;
            }
            if (pageBuilder.isFull()) {
                return false;
            }
        }
        return true;
    }

    // This class needs to be public because LookupJoiner is isolated.
    public static final class Counter
    {
        private int count;

        public void increment()
        {
            count++;
        }

        public int get()
        {
            return count;
        }
    }
}
