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

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.type.RelDataType;

import java.util.List;

import static com.google.common.collect.Iterables.getOnlyElement;

public class PrestoOutput
        extends AbstactPrestoRelNode
{
    private final List<String> columnNames;

    public PrestoOutput(RelOptCluster cluster, RelTraitSet traitSet, RelNode input, List<String> columnNames)
    {
        super(cluster, traitSet, ImmutableList.of(input));
        this.columnNames = columnNames;
    }

    @Override
    public RelWriter explainTerms(RelWriter pw)
    {
        return super.explainTerms(pw)
                .input("source", getOnlyElement(getInputs()))
                .item("columnNames", columnNames);
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs)
    {
        return new PrestoOutput(getCluster(), traitSet, getOnlyElement(inputs), columnNames);
    }

    public List<String> getColumnNames()
    {
        return columnNames;
    }

    @Override
    protected RelDataType deriveRowType()
    {
        return getOnlyElement(getInputs()).getRowType();
    }
}
