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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import static java.util.Objects.requireNonNull;

public class ExchangeInfo
        implements Mergeable<ExchangeInfo>, OperatorInfo
{
    private final ExchangeClientStatus clientStatus;

    @JsonCreator
    public ExchangeInfo(@JsonProperty("clientStatus") ExchangeClientStatus clientStatus)
    {
        this.clientStatus = requireNonNull(clientStatus, "clientStatus is null");
    }

    @JsonProperty
    public ExchangeClientStatus getClientStatus()
    {
        return clientStatus;
    }

    @Override
    public ExchangeInfo mergeWith(ExchangeInfo other)
    {
        return new ExchangeInfo(clientStatus.mergeWith(other.clientStatus));
    }
}
