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
package com.facebook.presto.sql.planner.plan.calcite;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;

public interface PrestoRelNode
        extends RelNode
{
    void implement(Implementor implementor);

    /**
     * Calling convention for relational operations that occur in Hive.
     */
    Convention CONVENTION = new Convention.Impl("PRESTO", PrestoRelNode.class);

    class Implementor
    {
        public void visitChild(int ordinal, RelNode input)
        {
            ((PrestoRelNode) input).implement(this);
        }
    }
}
