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

package com.facebook.presto.sql.planner.optimizations;

import com.facebook.presto.Session;
import com.facebook.presto.SystemSessionProperties;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.sql.planner.SimplePlanVisitor;
import com.facebook.presto.sql.planner.assertions.BasePlanTest;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.testing.LocalQueryRunner;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.sql.planner.plan.JoinNode.DistributionType.PARTITIONED;
import static com.facebook.presto.sql.planner.plan.JoinNode.DistributionType.REPLICATED;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.INNER;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.testng.Assert.assertEquals;

public class TestReorderJoins
        extends BasePlanTest
{
    public TestReorderJoins()
    {
        super(ImmutableMap.of(
                SystemSessionProperties.JOIN_REORDERING_STRATEGY, "COST_BASED",
                SystemSessionProperties.JOIN_DISTRIBUTION_TYPE, "AUTOMATIC"));
    }

    @Override
    protected LocalQueryRunner createQueryRunner(Session session)
    {
        return LocalQueryRunner.queryRunnerWithFakeNodeCountForStats(session, 8);
    }

    // no joins in q1
    
    @Test
    public void testPartialTpchQ2JoinOrder()
    {
        // it looks like the join ordering here is optimal
        assertJoinOrder(
                "SELECT * " +
                        "FROM part p, supplier s, partsupp ps, nation n, region r " +
                        "WHERE p.size = 15 AND p.type like '%BRASS' AND s.suppkey = ps.suppkey AND p.partkey = ps.partkey " +
                        "AND s.nationkey = n.nationkey AND n.regionkey = r.regionkey AND r.name = 'EUROPE'",
                new Join(
                        PARTITIONED,
                        new Join(
                                PARTITIONED,
                                tpchSf10Table("partsupp"),
                                tpchSf10Table("part")),
                        new Join(
                                PARTITIONED,
                                tpchSf10Table("supplier"),
                                new Join(
                                        tpchSf10Table("nation"),
                                        tpchSf10Table("region")))));
    }

    @Test
    public void testTpchQ3JoinOrder()
    {
        assertJoinOrder(
                "SELECT " +
                        "l.orderkey, " +
                        "sum(l.extendedprice * (1 - l.discount)) AS revenue, " +
                        "o.orderdate, o.shippriority " +
                        "FROM customer AS c, orders AS o, lineitem AS l " +
                        "WHERE c.mktsegment = 'BUILDING' " +
                        "AND c.custkey = o.custkey " +
                        "AND l.orderkey = o.orderkey " +
                        "AND o.orderdate<DATE '1995-03-15' " +
                        "AND l.shipdate > DATE '1995-03-15' " +
                        "GROUP BY l.orderkey, o.orderdate, o.shippriority " +
                        "ORDER BY revenue DESC, o.orderdate LIMIT 10",
                new Join(
                        tpchSf10Table("lineitem"),
                        new Join(
                                tpchSf10Table("orders"),
                                tpchSf10Table("customer"))));
    }

    @Test
    public void testTpchQ4JoinOrder()
    {
        assertJoinOrder(
                "" +
                        "SELECT o.orderpriority, " +
                        "  count(*) AS order_count " +
                        "FROM orders o " +
                        "WHERE" +
                        "  o.orderdate >= DATE '1993-07-01'" +
                        "  AND o.orderdate < DATE '1993-07-01' + INTERVAL '3' MONTH" +
                        "  AND EXISTS (" +
                        "    SELECT * " +
                        "    FROM lineitem l" +
                        "    WHERE l.orderkey = o.orderkey AND l.commitdate < l.receiptdate" +
                        "  )" +
                        "GROUP BY o.orderpriority " +
                        "ORDER BY o.orderpriority",
                new Join(
                        INNER,
                        Optional.empty(),
                        tpchSf10Table("orders"),
                        tpchSf10Table("lineitem")));
    }

    @Test
    public void testTpchQ05JoinOrder()
    {
        assertJoinOrder(
                "" +
                        "SELECT " +
                        "  n.name, " +
                        "  sum(l.extendedprice * (1 - l.discount)) AS revenue " +
                        "FROM " +
                        "  customer AS c, " +
                        "  orders AS o, " +
                        "  lineitem AS l, " +
                        "  supplier AS s, " +
                        "  nation AS n, " +
                        "  region AS r " +
                        "WHERE " +
                        "  c.custkey = o.custkey " +
                        "  AND l.orderkey = o.orderkey " +
                        "  AND l.suppkey = s.suppkey " +
                        "  AND c.nationkey = s.nationkey " +
                        "  AND s.nationkey = n.nationkey " +
                        "  AND n.regionkey = r.regionkey " +
                        "  AND r.name = 'ASIA' " +
                        "  AND o.orderdate >= DATE '1994-01-01' " +
                        "  AND o.orderdate < DATE '1994-01-01' + INTERVAL '1' YEAR " +
                        "GROUP BY " +
                        "  n.name " +
                        "ORDER BY " +
                        "  revenue DESC ",
                new Join(
                        new Join(
                                tpchSf10Table("lineitem"),
                                new Join(
                                        tpchSf10Table("orders"),
                                        new Join(
                                                tpchSf10Table("customer"),
                                                new Join(
                                                        tpchSf10Table("nation"),
                                                        tpchSf10Table("region"))))),
                        tpchSf10Table("supplier")));
    }

    // no joins in q6

    @Test
    public void testInnerTpchQ7JoinOrder()
    {
        assertJoinOrder(
                "SELECT " +
                        "n1.name AS supp_nation, " +
                        "n2.name AS cust_nation, " +
                        "extract(YEAR FROM l.shipdate) AS l_year, " +
                        "l.extendedprice * (1 - l.discount) AS volume " +
                        "FROM " +
                        "supplier AS s, " +
                        "lineitem AS l, " +
                        "orders AS o, " +
                        "customer AS c, " +
                        "nation AS n1, " +
                        "nation AS n2 " +
                        "WHERE " +
                        "s.suppkey = l.suppkey " +
                        "AND o.orderkey = l.orderkey " +
                        "AND c.custkey = o.custkey " +
                        "AND s.nationkey = n1.nationkey " +
                        "AND c.nationkey = n2.nationkey " +
                        "AND ( " +
                        "        (n1.name = 'FRANCE' AND n2.name = 'GERMANY')" +
                        "OR (n1.name = 'GERMANY' AND n2.name = 'FRANCE') " +
                        ") " +
                        "AND l.shipdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'",
                new Join(
                        new Join(
                                tpchSf10Table("lineitem"),
                                new Join(
                                        tpchSf10Table("orders"),
                                        new Join(
                                                tpchSf10Table("customer"),
                                                tpchSf10Table("nation")))),
                        new Join(
                                tpchSf10Table("supplier"),
                                tpchSf10Table("nation"))));
    }

    @Test
    public void testInnerTpchQ8JoinOrder()
    {
        assertJoinOrder(
                "SELECT " +
                        "extract(YEAR FROM o.orderdate)AS o_year, " +
                        "l.extendedprice * (1 - l.discount) AS volume, " +
                        "n2.name AS nation " +
                        "FROM " +
                        "part AS p, " +
                        "supplier AS s, " +
                        "lineitem AS l, " +
                        "orders AS o, " +
                        "customer AS c, " +
                        "nation AS n1, " +
                        "nation AS n2, " +
                        "region AS r " +
                        "WHERE " +
                        "p.partkey = l.partkey " +
                        "AND s.suppkey = l.suppkey " +
                        "AND l.orderkey = o.orderkey " +
                        "AND o.custkey = c.custkey " +
                        "AND c.nationkey = n1.nationkey " +
                        "AND n1.regionkey = r.regionkey " +
                        "AND r.name = 'AMERICA' " +
                        "AND s.nationkey = n2.nationkey " +
                        "AND o.orderdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31' " +
                        "AND p.type = 'ECONOMY ANODIZED STEEL'",
                new Join(
                        new Join(
                                new Join(
                                        tpchSf10Table("orders"),
                                        new Join(
                                                tpchSf10Table("lineitem"),
                                                tpchSf10Table("part"))),
                                new Join(
                                        tpchSf10Table("customer"),
                                        new Join(
                                                tpchSf10Table("nation"),
                                                tpchSf10Table("region")))),
                        new Join(
                                tpchSf10Table("supplier"),
                                tpchSf10Table("nation"))));
    }

    @Test
    public void testInnerTpchQ9JoinOrder()
    {
        assertJoinOrder(
                "SELECT " +
                        "n.name AS nation, " +
                        "extract(YEAR FROM o.orderdate) AS o_year, " +
                        "l.extendedprice * (1 - l.discount) - ps.supplycost * l.quantity AS amount " +
                        "FROM " +
                        "part AS p, " +
                        "supplier AS s, " +
                        "lineitem AS l, " +
                        "partsupp AS ps, " +
                        "orders AS o, " +
                        "nation AS n " +
                        "WHERE " +
                        "s.suppkey = l.suppkey " +
                        "AND ps.suppkey = l.suppkey " +
                        "AND ps.partkey = l.partkey " +
                        "AND p.partkey = l.partkey " +
                        "AND o.orderkey = l.orderkey " +
                        "AND s.nationkey = n.nationkey " +
                        "AND p.name LIKE '%green%'",
                new Join(
                        new Join(
                                tpchSf10Table("orders"),
                                new Join(
                                        tpchSf10Table("supplier"),
                                        new Join(
                                                tpchSf10Table("part"),
                                                new Join(
                                                        tpchSf10Table("lineitem"),
                                                        tpchSf10Table("partsupp"))))),
                        tpchSf10Table("nation")));
    }

    @Test
    public void testPartialTpchQ14JoinOrder()
    {
        // it looks like the join ordering here is optimal
        assertJoinOrder(
                "SELECT * " +
                        "FROM lineitem l, part p " +
                        "WHERE l.partkey = p.partkey AND l.shipdate >= DATE '1995-09-01' AND l.shipdate < DATE '1995-09-01' + INTERVAL '1' MONTH",
                new Join(
                        REPLICATED, //TODO it should be PARTITIONED
                        tpchSf10Table("part"),
                        tpchSf10Table("lineitem")));
    }

    private TableScan tpchSf10Table(String orders)
    {
        return new TableScan(format("tpch:%s:sf10.0", orders));
    }

    private void assertJoinOrder(String sql, Node expected)
    {
        assertEquals(joinOrderString(sql), expected.print());
    }

    private String joinOrderString(String sql)
    {
        Plan plan = plan(sql);
//        getQueryRunner().inTransaction(session -> {
//            System.out.println(PlanPrinter.textLogicalPlan(plan.getRoot(), plan.getTypes(), getQueryRunner().getMetadata(), getQueryRunner().getLookup(), session));
//            return null;
//        });
        JoinOrderPrinter joinOrderPrinter = new JoinOrderPrinter();
        plan.getRoot().accept(joinOrderPrinter, 0);
        return joinOrderPrinter.result();
    }

    private static class JoinOrderPrinter
            extends SimplePlanVisitor<Integer>
    {
        private final StringBuilder stringBuilder = new StringBuilder();

        public String result()
        {
            return stringBuilder.toString();
        }

        @Override
        public Void visitJoin(JoinNode node, Integer indent)
        {
            stringBuilder.append(indentString(indent))
                    .append("join (")
                    .append(node.getType())
                    .append(", ")
                    .append(node.getDistributionType().map(JoinNode.DistributionType::toString).orElse("unknown"))
                    .append("):\n");

            super.visitPlan(node.getLeft(), indent + 1);
            super.visitPlan(node.getRight(), indent + 1);

            return null;
        }

        @Override
        public Void visitTableScan(TableScanNode node, Integer indent)
        {
            stringBuilder.append(indentString(indent))
                    .append(node.getTable().getConnectorHandle().toString())
                    .append("\n");
            return null;
        }
    }

    private static String indentString(int indent)
    {
        return Strings.repeat("    ", indent);
    }

    private interface Node
    {
        void print(StringBuilder stringBuilder, int indent);

        default String print()
        {
            StringBuilder stringBuilder = new StringBuilder();
            print(stringBuilder, 0);
            return stringBuilder.toString();
        }
    }

    private static class Join
            implements Node
    {
        private final JoinNode.Type type;
        private final Optional<JoinNode.DistributionType> distributionType;
        private final Node left;
        private final Node right;

        private Join(Node left, Node right)
        {
            this(REPLICATED, left, right);
        }

        private Join(JoinNode.DistributionType distributionType, Node left, Node right)
        {
            this(INNER, Optional.of(distributionType), left, right);
        }

        private Join(JoinNode.Type type, Optional<JoinNode.DistributionType> distributionType, Node left, Node right)
        {
            this.left = requireNonNull(left, "left is null");
            this.right = requireNonNull(right, "right is null");
            this.type = requireNonNull(type, "type is null");
            this.distributionType = requireNonNull(distributionType, "distributionType is null");
        }

        @Override
        public void print(StringBuilder stringBuilder, int indent)
        {
            stringBuilder.append(indentString(indent))
                    .append("join (")
                    .append(type)
                    .append(", ")
                    .append(distributionType.map(JoinNode.DistributionType::toString)
                            .orElse("unknown"))
                    .append("):\n");

            left.print(stringBuilder, indent + 1);
            right.print(stringBuilder, indent + 1);
        }
    }

    private static class TableScan
            implements Node
    {
        private final String tableName;

        private TableScan(String tableName)
        {
            this.tableName = tableName;
        }

        @Override
        public void print(StringBuilder stringBuilder, int indent)
        {
            stringBuilder.append(indentString(indent))
                    .append(tableName)
                    .append("\n");
        }
    }
}
