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

package com.facebook.presto.logevent;

import com.facebook.presto.spi.eventlistener.EventListener;
import com.facebook.presto.spi.eventlistener.QueryCompletedEvent;
import com.facebook.presto.spi.eventlistener.QueryCreatedEvent;
import com.facebook.presto.spi.eventlistener.SplitCompletedEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.time.Duration;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

public class LogEventListenerImpl
        implements EventListener
{
    private Logger log = Logger.getLogger(LogEventListenerImpl.class.getName());
    private FileHandler fileHandler;

    public LogEventListenerImpl(String eventLogFilePath, int fileSize)
    {
        try {
            fileHandler = new FileHandler(eventLogFilePath, fileSize, 10, true);
            fileHandler.setLevel(Level.ALL);
            SimpleFormatter formatter = new SimpleFormatter();
            fileHandler.setFormatter(formatter);
            log.addHandler(fileHandler);
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void queryCreated(QueryCreatedEvent queryCreatedEvent)
    {
    }

    @Override
    public void queryCompleted(QueryCompletedEvent queryCompletedEvent)
    {
        if (queryCompletedEvent.getContext().getCatalog().isPresent() && !queryCompletedEvent.getContext().getCatalog().get().equalsIgnoreCase("jmx")) {
            QueryLogEvent event = new QueryLogEvent(queryCompletedEvent.getContext().getUser(),
                    queryCompletedEvent.getEndTime().toString(),
                    String.valueOf(Duration.between(queryCompletedEvent.getCreateTime(), queryCompletedEvent.getEndTime()).toMillis()),
                    queryCompletedEvent.getMetadata().getQuery(),
                    String.valueOf(queryCompletedEvent.getStatistics().getPeakMemoryBytes()),
                    String.valueOf(queryCompletedEvent.getStatistics().getTotalBytes()),
                    queryCompletedEvent.getMetadata().getQueryState());

            ObjectMapper objectMapper = new ObjectMapper();
            try {
                String value = objectMapper.writeValueAsString(event);
                log.log(Level.INFO, value);
            }
            catch (JsonProcessingException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void splitCompleted(SplitCompletedEvent splitCompletedEvent)
    {
    }
}
