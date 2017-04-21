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

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.RelMetadataQuery;

public class PrestoTableScan
        extends TableScan
        implements PrestoRelNode
{
    public PrestoTableScan(RelOptCluster cluster, RelTraitSet traitSet, RelOptPrestoTable table)
    {
        super(cluster, traitSet, table);
    }

    @Override
    public double estimateRowCount(RelMetadataQuery mq)
    {
//        List<String> name = getTable().getQualifiedName();
//        if (name.contains("region")) {
//            return 5;
//        }
//        else if (name.contains("nation")) {
//            return 25;
//        }
        return super.estimateRowCount(mq);
    }
}
