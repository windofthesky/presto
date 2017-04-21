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
package com.facebook.presto.sql.planner.optimizations.calcite.rules;

import com.facebook.presto.sql.planner.optimizations.CalciteOptimizer;
import com.facebook.presto.sql.planner.plan.calcite.PrestoJoin;
import org.apache.calcite.rel.rules.JoinCommuteRule;

public class PrestoJoinCommuteRule
        extends JoinCommuteRule
{
    public PrestoJoinCommuteRule()
    {
        super(PrestoJoin.class, CalciteOptimizer.REL_BUILDER_FACTORY, false);
    }
}
