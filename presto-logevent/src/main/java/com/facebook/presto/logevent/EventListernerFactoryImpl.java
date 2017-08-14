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
import com.facebook.presto.spi.eventlistener.EventListenerFactory;

import java.util.Map;

public class EventListernerFactoryImpl
        implements EventListenerFactory
{
    private static final String fileLoc = "logfile";
    private static final String fileSize = "filesize";

    @Override
    public String getName()
    {
        return "logeventplugin";
    }

    @Override
    public EventListener create(Map<String, String> config)
    {
        String fileloc = config.getOrDefault(fileLoc, "/tmp/presto_event.log");
        int filesize = Integer.parseInt(config.getOrDefault(fileSize, String.valueOf(10 * 1024 * 1024)));
        return new LogEventListenerImpl(fileloc, filesize);
    }
}
