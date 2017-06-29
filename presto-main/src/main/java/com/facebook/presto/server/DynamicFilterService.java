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
package com.facebook.presto.server;

import com.facebook.presto.operator.DynamicFilterSummary;

import javax.annotation.concurrent.Immutable;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class DynamicFilterService
{
    private final Map<SourceDescriptor, DynamicFilterSummaryWithSenders> dynamicFilterSummaries = new HashMap<>();

    // maintain driver IDs in the map too (in case of retries etc.)
    public synchronized void storeOrMergeSummary(String queryId, String source, int stageId, int taskId, DynamicFilterSummary dynamicFilterSummary, int expectedSummariesCount)
    {
        DynamicFilterSummaryWithSenders dynamicFilterSummaryWithSenders
                = dynamicFilterSummaries.getOrDefault(SourceDescriptor.of(queryId, source), new DynamicFilterSummaryWithSenders());
        dynamicFilterSummaryWithSenders.addSummary(stageId, taskId, dynamicFilterSummary, expectedSummariesCount);

        dynamicFilterSummaries.put(SourceDescriptor.of(queryId, source), dynamicFilterSummaryWithSenders);
    }

    public synchronized void registerTask(String queryId, String source, int stageId, int taskId)
    {
        DynamicFilterSummaryWithSenders dynamicFilterSummaryWithSenders
                = dynamicFilterSummaries.getOrDefault(SourceDescriptor.of(queryId, source), new DynamicFilterSummaryWithSenders());
        dynamicFilterSummaryWithSenders.registerTask(stageId, taskId);

        dynamicFilterSummaries.put(SourceDescriptor.of(queryId, source), dynamicFilterSummaryWithSenders);
    }

    /**
     * @return Optional.of(DynamicFilterSummary) or Optional.empty if not all nodes have reported theirs summaries
     **/
    public synchronized Optional<DynamicFilterSummary> getSummary(String queryId, String source)
    {
        DynamicFilterSummaryWithSenders dynamicFilterSummaryWithSenders = dynamicFilterSummaries.get(SourceDescriptor.of(queryId, source));
        return dynamicFilterSummaryWithSenders.getSummaryIfReady();
    }

    public synchronized void removeQuery(String queryId)
    {
        Iterator<Map.Entry<SourceDescriptor, DynamicFilterSummaryWithSenders>> iter = dynamicFilterSummaries.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry<SourceDescriptor, DynamicFilterSummaryWithSenders> entry = iter.next();
            if (entry.getKey().getQueryId().equals(queryId)) {
                iter.remove();
            }
        }
    }

    private static class SourceDescriptor
    {
        private final String queryId;
        private final String source;

        public static SourceDescriptor of(String queryId, String source)
        {
            return new SourceDescriptor(queryId, source);
        }

        private SourceDescriptor(String queryId, String source)
        {
            this.queryId = requireNonNull(queryId, "queryId is null");
            this.source = requireNonNull(source, "source is null");
        }

        public String getQueryId()
        {
            return queryId;
        }

        public String getSource()
        {
            return source;
        }

        @Override
        public boolean equals(Object other)
        {
            if (other == this) {
                return true;
            }
            if (other == null || !(other instanceof SourceDescriptor)) {
                return false;
            }

            SourceDescriptor sourceDescriptor = (SourceDescriptor) other;

            return Objects.equals(queryId, sourceDescriptor.queryId) &&
                    Objects.equals(source, sourceDescriptor.source);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(queryId, source);
        }
    }

    public static class DynamicFilterSummaryWithSenders
    {
        private final Map<String, SenderStats> senderStats = new HashMap<>(); // String in form of "stageId.taskId"
        private DynamicFilterSummary dynamicFilterSummary;

        public Optional<DynamicFilterSummary> getSummaryIfReady()
        {
            for (Map.Entry<String, SenderStats> entry : senderStats.entrySet()) {
                if (!entry.getValue().isCompleted()) {
                    return Optional.empty();
                }
            }

            return Optional.of(dynamicFilterSummary);
        }

        public void registerTask(int stageId, int taskId)
        {
            String stageTaskId = stageId + "." + taskId;
            senderStats.put(stageTaskId, new SenderStats());
        }

        public void addSummary(int stageId, int taskId, DynamicFilterSummary summary, int expectedSummariesCount)
        {
            String stageTaskId = stageId + "." + taskId;

            SenderStats stats = senderStats.get(stageTaskId);
            if (stats == null || !stats.isValid()) {
                stats = new SenderStats(1, expectedSummariesCount);
            }
            else {
                stats = stats.increaseSummariesReceived();
            }
            senderStats.put(stageTaskId, stats);

            if (dynamicFilterSummary == null) {
                dynamicFilterSummary = summary;
            }
            else {
                dynamicFilterSummary = dynamicFilterSummary.mergeWith(summary);
            }
        }

        @Immutable
        private static class SenderStats
        {
            private static final int NOT_YET_PROVIDED = -1;

            private final int summariesReceived;
            private final int expectedSummariesCount;

            public SenderStats()
            {
                this(NOT_YET_PROVIDED, NOT_YET_PROVIDED);
            }

            public SenderStats(int summariesReceived, int expectedSummariesCount)
            {
                this.summariesReceived = summariesReceived;
                this.expectedSummariesCount = expectedSummariesCount;
            }

            public boolean isValid()
            {
                return summariesReceived != NOT_YET_PROVIDED && expectedSummariesCount != NOT_YET_PROVIDED;
            }

            public boolean isCompleted()
            {
                return isValid() && summariesReceived == expectedSummariesCount;
            }

            public SenderStats increaseSummariesReceived()
            {
                checkState(summariesReceived != NOT_YET_PROVIDED, "cannot increase number of received summaries when no expected count given");
                checkState(summariesReceived < expectedSummariesCount, "cannot increase number of received summaries beyond the expected count");

                return new SenderStats(summariesReceived + 1, expectedSummariesCount);
            }
        }
    }
}
