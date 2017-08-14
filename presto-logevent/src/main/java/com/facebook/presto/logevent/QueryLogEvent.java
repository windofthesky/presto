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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Created by xiaosong_niu on 3/9/17.
 */
public class QueryLogEvent
{
    public String runTime;
    public String user;
    public String query;
    public String queryState;
    public String finishTime;
    public String peakMemory;
    public String inputSize;

    @JsonCreator
    public QueryLogEvent(@JsonProperty("user") String user,
            @JsonProperty("finishtime") String finishTime,
            @JsonProperty("runtime") String runTime,
            @JsonProperty("query") String query,
            @JsonProperty("peakmemory") String peakMemory,
            @JsonProperty("inputsize") String inputSize,
            @JsonProperty("queryState") String queryState)
    {
        this.runTime = runTime;
        this.user = user;
        this.query = query;
        this.queryState = queryState;
        this.finishTime = finishTime;
        this.inputSize = inputSize;
        this.peakMemory = peakMemory;
    }
}
