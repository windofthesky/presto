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
package com.facebook.presto.hive;

import com.google.common.base.StandardSystemProperty;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import java.io.File;
import java.util.concurrent.TimeUnit;

public class HiveWasbConfig
{
    private String wasbAccessKey;
    private File wasbStagingDirectory = new File(StandardSystemProperty.JAVA_IO_TMPDIR.value());
    private int wasbMaxClientRetries = 3;
    private Duration wasbMaxBackoffTime = new Duration(10, TimeUnit.MINUTES);
    private Duration wasbMaxRetryTime = new Duration(10, TimeUnit.MINUTES);

    public String getWasbAccessKey()
    {
        return wasbAccessKey;
    }

    @Config("hive.wasb.access-key")
    public HiveWasbConfig setWasbAccessKey(String wasbAccessKey)
    {
        this.wasbAccessKey = wasbAccessKey;
        return this;
    }

    @NotNull
    public File getWasbStagingDirectory()
    {
        return wasbStagingDirectory;
    }

    @Min(0)
    public int getWasbMaxClientRetries()
    {
        return wasbMaxClientRetries;
    }

    @Config("hive.wasb.max-client-retries")
    public HiveWasbConfig setWasbMaxClientRetries(int wasbMaxClientRetries)
    {
        this.wasbMaxClientRetries = wasbMaxClientRetries;
        return this;
    }

    @MinDuration("1s")
    @NotNull
    public Duration getWasbMaxBackoffTime()
    {
        return wasbMaxBackoffTime;
    }

    @Config("hive.wasb.max-backoff-time")
    public HiveWasbConfig setWasbMaxBackoffTime(Duration wasbMaxBackoffTime)
    {
        this.wasbMaxBackoffTime = wasbMaxBackoffTime;
        return this;
    }

    @MinDuration("1ms")
    @NotNull
    public Duration getWasbMaxRetryTime()
    {
        return wasbMaxRetryTime;
    }

    @Config("hive.wasb.max-retry-time")
    public HiveWasbConfig setWasbMaxRetryTime(Duration wasbMaxRetryTime)
    {
        this.wasbMaxRetryTime = wasbMaxRetryTime;
        return this;
    }

    @Config("hive.wasb.staging-directory")
    @ConfigDescription("Temporary directory for staging files before uploading to wasb")
    public HiveWasbConfig setWasbStagingDirectory(File wasbStagingDirectory)
    {
        this.wasbStagingDirectory = wasbStagingDirectory;
        return this;
    }
}
