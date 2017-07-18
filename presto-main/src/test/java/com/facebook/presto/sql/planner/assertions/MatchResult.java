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
package com.facebook.presto.sql.planner.assertions;

import com.facebook.presto.sql.tree.SymbolReference;

import java.util.function.Consumer;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

public abstract class MatchResult
{
    public abstract boolean isMatch();

    public static MatchResult match()
    {
        return match(new SymbolAliases());
    }

    public static MatchResult match(String alias, SymbolReference symbolReference)
    {
        SymbolAliases newAliases = SymbolAliases.builder()
                .put(alias, symbolReference)
                .build();
        return match(newAliases);
    }

    public static MatchResult match(SymbolAliases newAliases)
    {
        return new Match(newAliases);
    }

    public static MatchResult noMatch(String matchFailureMessage)
    {
        return new NoMatch(matchFailureMessage);
    }

    public static <V> MatchResult testMatch(String fieldName, V expected, V actual)
    {
        if (expected.equals(actual)) {
            return match();
        }
        return noMatch(fieldName + ": expected '" + expected.toString() + "' but found '" + actual.toString() + "'");
    }

    public MatchResult and(MatchResult other)
    {
        if (isMatch()) {
            if (other.isMatch()) {
                return match(((Match) this).getAliases().withNewAliases(((Match) other).getAliases()));
            }
            return other;
        }
        return this;
    }

    /*
    // The Java requirement that lambdas only look at final values makes these functions not so useful.  :(

    public <R> R map(Function<NoMatch, R> noMatchFunction, Function<Match, R> matchFunction)
    {
        if (isMatch()) {
            return noMatchFunction.apply((NoMatch) this);
        }
        return matchFunction.apply((Match) this);
    }

    public void consume(Consumer<NoMatch> noMatchConsumer, Consumer<Match> matchConsumer)
    {
        if (isMatch()) {
            noMatchConsumer.accept((NoMatch) this);
            return;
        }
        matchConsumer.accept((Match) this);
    }

    public void ifMatch(Consumer<Match> matchConsumer)
    {
        if (isMatch()) {
            matchConsumer.accept((Match) this);
        }
    }

    public void ifNoMatch(Consumer<NoMatch> noMatchConsumer)
    {
        if (!isMatch()) {
            noMatchConsumer.accept((NoMatch) this);
        }
    }
    */

    public static final class Match extends MatchResult
    {
        private final SymbolAliases newAliases;

        Match(SymbolAliases newAliases)
        {
            this.newAliases = requireNonNull(newAliases, "newAliases is null");
        }

        @Override
        public boolean isMatch()
        {
            return true;
        }

        public SymbolAliases getAliases()
        {
            return newAliases;
        }

        @Override
        public String toString()
        {
            return "MATCH: " + newAliases.toString();
        }
    }

    public static final class NoMatch extends MatchResult
    {
        // TODO also track the failing matcher, so that the stringized matcher tree can highlight it.
        private String matchFailureMessage;

        NoMatch(String matchFailureMessage)
        {
            this.matchFailureMessage = requireNonNull(matchFailureMessage, "matchFailureMessage is null");
        }

        @Override
        public boolean isMatch()
        {
            return false;
        }

        public String getMatchFailureMessage()
        {
            return matchFailureMessage;
        }

        @Override
        public String toString()
        {
            return "NO_MATCH: " + matchFailureMessage;
        }
    }
}
