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
package com.facebook.presto.matching;

import com.facebook.presto.matching.pattern.EqualsPattern;
import com.facebook.presto.matching.pattern.FilterPattern;

import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;

@FunctionalInterface
public interface Property<F, T> extends BiFunction<F, Captures, Optional<T>> {

    static <F, T> Property<F, T> property(String name, Function<F, T> function) {
        return optionalProperty(name, source -> Optional.of(function.apply(source)));
    }

    static <F, T> Property<F, T> optionalProperty(String name, Function<F, Optional<T>> function) {
        return extractor(name, (value, captures) -> function.apply(value));
    }

    static <F, T> Property<F, T> extractor(String name, BiFunction<F, Captures, Optional<T>> biFunction) {
        return new Property<F, T>() {
            @Override
            public String getName() {
                return name;
            }

            @Override
            public Optional<T> apply(F f, Captures captures) {
                return biFunction.apply(f, captures);
            }
        };
    }

    static <T, R> Property<T, R> upcast(Property<? super T, R> extractor) {
        return (Property<T, R>) extractor;
    }

    default String getName() {
        return "";
    }

    default <R> PropertyPattern<F, R> matching(Pattern<R> pattern) {
        return PropertyPattern.of(this, pattern);
    }

    default PropertyPattern<F, T> capturedAs(Capture<T> capture) {
        Pattern<T> matchAll = (Pattern<T>) Pattern.any();
        return matching(matchAll.capturedAs(capture));
    }


    default PropertyPattern<F, T> equalTo(T expectedValue) {
        return matching(new EqualsPattern<>(expectedValue, null));
    }

    default PropertyPattern<F, T> matching(Predicate<? super T> predicate) {
        return matching("", predicate);
    }

    default PropertyPattern<F, T> matching(String description, Predicate<? super T> predicate) {
        return matching(new FilterPattern<>(description, UsageCallSite.get(), predicate, null));
    }
}
