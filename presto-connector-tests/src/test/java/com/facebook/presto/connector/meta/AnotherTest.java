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
package com.facebook.presto.connector.meta;

import org.junit.jupiter.api.Test;

import static com.facebook.presto.connector.meta.ConnectorFeature.CREATE_TABLE;
import static com.facebook.presto.connector.meta.ConnectorFeature.DROP_SCHEMA;
import static com.facebook.presto.connector.meta.ConnectorFeature.DROP_TABLE;
import static org.junit.jupiter.api.Assertions.fail;

@RequiredFeatures(DROP_SCHEMA)
public interface AnotherTest
        extends BaseTest
{
    @Test
    default void noMoreFeaturesRequiredTest()
    {
        createSchema();
        dropSchema();
    }

    @Test
    @RequiredFeatures({CREATE_TABLE, DROP_TABLE})
    default void moreFeaturesRequiredTest()
    {
        createSchema();
        createTable();
        dropTable();
        dropSchema();
    }

    default void dropSchema()
    {
        fail("Unsupported dropSchema");
    }

    default void createTable()
    {
        fail("Unsupported createTable");
    }

    default void dropTable()
    {
        fail("Unsupported dropTable");
    }
}
