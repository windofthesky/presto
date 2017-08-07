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

import com.facebook.presto.memory.LocalMemoryContext;
import com.facebook.presto.operator.project.MergingPageOutput;
import com.facebook.presto.operator.project.PageProcessor;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;

import java.util.List;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkState;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class FilterAndProjectOperator
        implements Operator
{
    private final OperatorContext operatorContext;
    private final List<Type> types;
    private final LocalMemoryContext outputMemoryContext;

    private final PageProcessor processor;
    private MergingPageOutput currentOutput;
    private boolean finishing;

    public FilterAndProjectOperator(
            OperatorContext operatorContext,
            Iterable<? extends Type> types,
            PageProcessor processor,
            MergingPageOutput mergingPageOutput)
    {
        this.processor = requireNonNull(processor, "processor is null");
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.outputMemoryContext = operatorContext.getSystemMemoryContext().newLocalMemoryContext();
        this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));
        this.currentOutput = requireNonNull(mergingPageOutput, "mergingPageOutput is null");
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public final List<Type> getTypes()
    {
        return types;
    }

    @Override
    public final void finish()
    {
        currentOutput.finish();
        finishing = true;
    }

    @Override
    public final boolean isFinished()
    {
        boolean finished = finishing && currentOutput.isFinished();
        if (finished) {
            outputMemoryContext.setBytes(currentOutput.getRetainedSizeInBytes());
        }
        return finished;
    }

    @Override
    public final boolean needsInput()
    {
        return !finishing && currentOutput.needsInput();
    }

    @Override
    public final void addInput(Page page)
    {
        checkState(!finishing, "Operator is already finishing");
        requireNonNull(page, "page is null");
        checkState(currentOutput.needsInput(), "Page buffer is full");

        currentOutput.addInput(processor.process(operatorContext.getSession().toConnectorSession(), page));
        outputMemoryContext.setBytes(currentOutput.getRetainedSizeInBytes());
    }

    @Override
    public final Page getOutput()
    {
        return currentOutput.getOutput();
    }

    public static class FilterAndProjectOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final Supplier<PageProcessor> processor;
        private final List<Type> types;
        private final DataSize minOutputPageSize;
        private final int minOutputPageRowCount;
        private boolean closed;

        public FilterAndProjectOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                Supplier<PageProcessor> processor,
                List<Type> types,
                DataSize minOutputPageSize,
                int minOutputPageRowCount)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.processor = requireNonNull(processor, "processor is null");
            this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));
            this.minOutputPageSize = requireNonNull(minOutputPageSize, "minOutputPageSize is null");
            this.minOutputPageRowCount = minOutputPageRowCount;
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
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, FilterAndProjectOperator.class.getSimpleName());
            return new FilterAndProjectOperator(
                    operatorContext,
                    types,
                    processor.get(),
                    new MergingPageOutput(types, toIntExact(minOutputPageSize.toBytes()), minOutputPageRowCount));
        }

        @Override
        public void close()
        {
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new FilterAndProjectOperatorFactory(operatorId, planNodeId, processor, types, minOutputPageSize, minOutputPageRowCount);
        }
    }
}
