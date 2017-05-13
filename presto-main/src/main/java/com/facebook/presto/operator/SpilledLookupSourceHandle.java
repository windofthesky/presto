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

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

@ThreadSafe
final class SpilledLookupSourceHandle
{
    private enum State
    {
        SPILLED,
        UNSPILLING,
        PRODUCED,
        DISPOSED
    }

//    @GuardedBy("this")
//    private StateMachine<State> state = new StateMachine<>(
//            "SpilledLookupSourceHandle",
//            directExecutor(),
//            State.SPILLED,
//            ImmutableList.of(State.DISPOSED));

    @GuardedBy("this")
    private State state = State.SPILLED;

    private final SettableFuture<?> unspillingRequested = SettableFuture.create();

    @GuardedBy("this")
    private Optional<SettableFuture<LookupSource>> unspilledLookupSource = Optional.of(SettableFuture.create());

    private final SettableFuture<?> disposed = SettableFuture.create();

    public synchronized ListenableFuture<LookupSource> getLookupSource()
    {
        assertIn(State.SPILLED);
        unspillingRequested.set(null);
        setState(State.UNSPILLING);
        return unspilledLookupSource.get();
    }

    public synchronized void setLookupSource(LookupSource lookupSource)
    {
        requireNonNull(lookupSource, "lookupSource is null");

        assertIn(State.UNSPILLING);
        unspilledLookupSource.get().set(lookupSource);
        unspilledLookupSource = Optional.empty(); // let the memory go
        setState(State.PRODUCED);
    }

    public synchronized void dispose()
    {
        disposed.set(null);
        setState(State.DISPOSED);
    }

    private State getState()
    {
//        return state.get();
        return state;
    }

//    public ListenableFuture<State> getStateChange(State currentState)
//    {
//        return this.state.getStateChange(currentState);
//    }

    @GuardedBy("this")
    private void assertIn(State expectedState)
    {
        State currentState = getState();
        checkState(currentState == expectedState, "Wrong state: expected %s, got %s", expectedState, currentState);
    }

    @GuardedBy("this")
    private void setState(State newState)
    {
        //this.state.set(requireNonNull(newState, "newState is null"));
        this.state = requireNonNull(newState, "newState is null");
    }
}
