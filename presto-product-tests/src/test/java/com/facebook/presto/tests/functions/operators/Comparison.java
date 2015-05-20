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
package com.facebook.presto.tests.functions.operators;

import com.teradata.test.ProductTest;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static com.facebook.presto.tests.TestGroups.COMPARISON;
import static com.facebook.presto.tests.TestGroups.QE;
import static com.teradata.test.assertions.QueryAssert.Row.row;
import static com.teradata.test.assertions.QueryAssert.assertThat;
import static com.teradata.test.query.QueryExecutor.query;

public class Comparison
        extends ProductTest
{
    @DataProvider(name = "operands")
    public static Object[][] operandsProvider()
    {
        return new Object[][] {
                {"false", "true", "boolean"},
                {"0", "1", "double"},
                {"0", "1", "bigint"},
                {"date '1991-01-01'", "date '1991-01-02'", "date"},
                {"time '01:02:03.456'", "time '01:02:03.457'", "time"},
                {"time '01:02:03.456 America/Los_Angeles'", "time '01:02:03.457 America/Los_Angeles'", "TIME WITH TIME ZONE"},
                {"TIMESTAMP '2001-08-22 03:04:05.321'", "TIMESTAMP '2001-08-22 03:04:05.322'", "TIMESTAMP"},
                {"TIMESTAMP '2001-08-22 03:04:05.321 America/Los_Angeles'", "TIMESTAMP '2001-08-22 03:04:05.322 America/Los_Angeles'", "TIMESTAMP WITH TIME ZONE"}
        };
    }

    @Test(groups = {COMPARISON, QE}, dataProvider = "operands")
    public void testLessThanOperatorExists(String leftOperand, String rightOperand, String typeName)
    {
        assertThat(query(String.format("select cast(%s as %s) < cast(%s as %s)", leftOperand, typeName, rightOperand, typeName)))
                .containsExactly(row(true));
    }

    @Test(groups = {COMPARISON, QE}, dataProvider = "operands")
    public void testGreaterThanOperatorExists(String leftOperand, String rightOperand, String typeName)
    {
        assertThat(query(String.format("select cast(%s as %s) > cast(%s as %s)", leftOperand, typeName, rightOperand, typeName)))
                .containsExactly(row(false));
    }

    @Test(groups = {COMPARISON, QE}, dataProvider = "operands")
    public void testLessThanOrEqualOperatorExists(String leftOperand, String rightOperand, String typeName)
    {
        assertThat(query(String.format("select cast(%s as %s) <= cast(%s as %s)", leftOperand, typeName, rightOperand, typeName)))
                .containsExactly(row(true));
    }

    @Test(groups = {COMPARISON, QE}, dataProvider = "operands")
    public void testGreaterThanOrEqualOperatorExists(String leftOperand, String rightOperand, String typeName)
    {
        assertThat(query(String.format("select cast(%s as %s) >= cast(%s as %s)", leftOperand, typeName, rightOperand, typeName)))
                .containsExactly(row(false));
    }

    @Test(groups = {COMPARISON, QE}, dataProvider = "operands")
    public void testEqualOperatorExists(String leftOperand, String rightOperand, String typeName)
    {
        assertThat(query(String.format("select cast(%s as %s) = cast(%s as %s)", leftOperand, typeName, rightOperand, typeName)))
                .containsExactly(row(false));
    }

    @Test(groups = {COMPARISON, QE}, dataProvider = "operands")
    public void testBetweenOperatorExists(String leftOperand, String rightOperand, String typeName)
    {
        assertThat(query(String.format("select cast(%s as %s) BETWEEN cast(%s as %s)", leftOperand, typeName, rightOperand, typeName)))
                .containsExactly(row(false));
    }
}
