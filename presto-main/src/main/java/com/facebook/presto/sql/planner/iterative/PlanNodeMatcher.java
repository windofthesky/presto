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
package com.facebook.presto.sql.planner.iterative;

import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.DefaultMatcher;
import com.facebook.presto.matching.Match;
import com.facebook.presto.matching.Property;
import com.facebook.presto.matching.pattern.HasPropertyPattern;
import com.facebook.presto.sql.planner.plan.PlanNode;

import java.util.Optional;

public class PlanNodeMatcher
        extends DefaultMatcher
{
    private final Lookup lookup;

    public PlanNodeMatcher(Lookup lookup)
    {
        this.lookup = lookup;
    }

    @Override
    public <T, R> Match<R> matchHasProperty(HasPropertyPattern<T, R> hasPropertyPattern, Object object, Captures captures) {
        Property<? super T, R> property = hasPropertyPattern.getProperty();
        Optional<R> propertyValue = property.apply((T) object, captures);

        Optional<PlanNode> resolvedValue = propertyValue
                .map(value -> value instanceof GroupReference ? lookup.resolve(((GroupReference) value)) : value);

        Match<R> propertyMatch = resolvedValue
                .map(value -> Match.of(value, captures))
                .orElse(Match.empty());
        return propertyMatch;
    }

}
