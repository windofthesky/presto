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
package com.facebook.presto.matching.pattern;

import com.facebook.presto.matching.*;

public class HasPropertyPattern<T, R>
        extends Pattern<R>
{
    private final Property<? super T, R> property;

    public HasPropertyPattern(Property<? super T, R> property, Pattern<T> previous)
    {
        super(previous);
        this.property = property;
    }

    public Property<? super T, R> getProperty()
    {
        return property;
    }

    @Override
    public Match<R> accept(Matcher matcher, Object object, Captures captures)
    {
        return matcher.matchHasProperty(this, object, captures);
    }

    @Override
    public void accept(PatternVisitor patternVisitor)
    {
        patternVisitor.visitHasProperty(this);
    }
}
