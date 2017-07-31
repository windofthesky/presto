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

import com.facebook.presto.matching.pattern.*;
import com.google.common.collect.Iterables;

import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Predicate;

import static com.facebook.presto.matching.Property.extractor;
import static com.facebook.presto.matching.Property.upcast;
import static com.google.common.base.Predicates.not;

public abstract class Pattern<T> {
    private final Pattern<?> previous;

    public static Pattern<Object> any() {
        return typeOf(Object.class);
    }

    public static <T> Pattern<T> typeOf(Class<T> expectedClass) {
        return new TypeOfPattern<>(expectedClass);
    }

    protected Pattern() {
        this(null);
    }

    protected Pattern(Pattern<?> previous) {
        this.previous = previous;
    }

    public static <F, T extends Iterable<S>, S> PropertyPattern<F, T> empty(Property<F, T> property) {
        return PropertyPattern.upcast(property.matching("empty", Iterables::isEmpty));
    }

    public static <F, T extends Iterable<S>, S> PropertyPattern<F, T> nonEmpty(Property<F, T> property) {
        return PropertyPattern.upcast(property.matching("nonEmpty", not(Iterables::isEmpty)));
    }

    public Pattern<T> capturedAs(Capture<T> capture) {
        return new CapturePattern<>(capture, this);
    }

    public Pattern<T> matching(Predicate<? super T> predicate) {
        return matching("", predicate);
    }

    public Pattern<T> matching(String description, Predicate<? super T> predicate) {
        return new FilterPattern<>(description, UsageCallSite.get(), predicate, this);
    }

    public <R> Pattern<R> matching(BiFunction<? super T, Captures, Optional<R>> property) {
        return matching("", property);
    }

    public <R> Pattern<R> matching(String description, BiFunction<? super T, Captures, Optional<R>> property) {
        return matching(upcast(extractor(description, property)));
    }

    public <R> Pattern<R> matching(Property<T, R> property) {
        return new HasPropertyPattern<>(property, this);
    }

    public Pattern<T> with(Property<? super T, ?> property) {
        return new ScopedPattern<>(new HasPropertyPattern<>(property, null), this);
    }

    public Pattern<T> with(PropertyPattern<? super T, ?> propertyPattern) {
        HasPropertyPattern<? super T, ?> hasProperty = new HasPropertyPattern<>(propertyPattern.getProperty(), null);
        CombinePattern<?> combinePattern = new CombinePattern<>(hasProperty, propertyPattern.getPattern());
        return new ScopedPattern<>(combinePattern, this);
    }

    public Pattern<?> previous() {
        return previous;
    }

    public abstract Match<T> accept(Matcher matcher, Object object, Captures captures);

    public abstract void accept(PatternVisitor patternVisitor);

    @Override
    public String toString() {
        DefaultPrinter printer = new DefaultPrinter();
        accept(printer);
        return printer.result();
    }
}
