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

import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class PrePartitionedWindowOperator
        implements Operator
{
    public static class PrePartitionedWindowOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final List<Type> sourceTypes;
        private final List<Type> types;
        private final List<Integer> outputChannels;
        private final List<WindowFunctionDefinition> windowFunctionDefinitions;

        private boolean closed;

        public PrePartitionedWindowOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                List<? extends Type> sourceTypes,
                List<Integer> outputChannels,
                List<WindowFunctionDefinition> windowFunctionDefinitions)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.sourceTypes = ImmutableList.copyOf(sourceTypes);
            this.types = Stream.concat(
                    outputChannels.stream()
                            .map(sourceTypes::get),
                    windowFunctionDefinitions.stream()
                            .map(WindowFunctionDefinition::getType))
                    .collect(toImmutableList());
            this.windowFunctionDefinitions = ImmutableList.copyOf(windowFunctionDefinitions);
//            this.types = outputChannels.stream().map(sourceTypes::get).collect(toImmutableList());

            this.outputChannels = ImmutableList.copyOf(outputChannels);
        }

        @Override
        public List<Type> getTypes()
        {
            return types;
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");

            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, PrePartitionedWindowOperator.class.getSimpleName());
            return new PrePartitionedWindowOperator(
                    operatorContext,
                    types,
                    outputChannels,
                    windowFunctionDefinitions);
        }

        @Override
        public void close()
        {
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new PrePartitionedWindowOperatorFactory(operatorId, planNodeId, sourceTypes, outputChannels, windowFunctionDefinitions);
        }
    }

    private final OperatorContext operatorContext;
    private final List<Type> types;
    private final List<Integer> outputChannels;
    private final List<WindowFunctionDefinition> windowFunctionDefinitions;
    private boolean input;
    private Page page;

    public PrePartitionedWindowOperator(
            OperatorContext operatorContext,
            List<Type> types,
            List<Integer> outputChannels,
            List<WindowFunctionDefinition> windowFunctionDefinitions)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.types = ImmutableList.copyOf(types);
        this.outputChannels = ImmutableList.copyOf(outputChannels);
        this.windowFunctionDefinitions = ImmutableList.copyOf(windowFunctionDefinitions);

        this.input = true;
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public List<Type> getTypes()
    {
        return types;
    }

    @Override
    public void finish()
    {}

    @Override
    public boolean isFinished()
    {
        return false;
    }

    @Override
    public boolean needsInput()
    {
        return input;
    }

    @Override
    public void addInput(Page page)
    {
        this.page = page;
        input = false;
    }

    @Override
    public Page getOutput()
    {
        if (page == null) {
            return null;
        }

        Block[] outputBlocks = new Block[page.getChannelCount() + windowFunctionDefinitions.size()];
        for (int i = 0; i < page.getChannelCount(); i++) {
            outputBlocks[i] = page.getBlock(i);
        }

        BlockBuilder[] blockBuilders = new BlockBuilder[types.size() - page.getChannelCount()];

        for (int j = page.getChannelCount(); j < types.size(); j++) {
            blockBuilders[j - page.getChannelCount()] = types.get(j).createBlockBuilder(new BlockBuilderStatus(), page.getPositionCount());
        }

        for (int i = 0; i < page.getPositionCount(); i++) {
            for (int j = page.getChannelCount(); j < types.size(); j++) {
                types.get(j).writeLong(blockBuilders[j - page.getChannelCount()], i * j);
            }
        }

        for (int j = page.getChannelCount(); j < types.size(); j++) {
            outputBlocks[j] = blockBuilders[j - page.getChannelCount()].build();
        }

        input = true;
        return new Page(page.getPositionCount(), outputBlocks);
    }
}
