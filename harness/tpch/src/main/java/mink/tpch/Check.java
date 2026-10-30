package mink.tpch;

import java.sql.SQLException;
import java.util.List;

final class Check {
    private static final double TOLERANCE = 1e-9;

    private record Query(String name, String sql) {}

    private static final List<Query> QUERIES = List.of(
            new Query("Q1", """
                    SELECT l_returnflag, l_linestatus,
                        sum(l_quantity), sum(l_extendedprice),
                        sum(l_extendedprice * (1 - l_discount)),
                        sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)),
                        avg(l_quantity), avg(l_extendedprice), avg(l_discount),
                        count(*)
                    FROM %s
                    WHERE l_shipdate <= DATE '1998-12-01' - INTERVAL 90 DAY
                    GROUP BY l_returnflag, l_linestatus
                    ORDER BY l_returnflag, l_linestatus
                    """),
            new Query("Q6", """
                    SELECT sum(l_extendedprice * l_discount)
                    FROM %s
                    WHERE l_shipdate >= DATE '1994-01-01' AND l_shipdate < DATE '1995-01-01'
                        AND l_discount BETWEEN 0.05 AND 0.07 AND l_quantity < 24
                    """));

    private Check() {}

    static boolean run(Dataset dataset, String table) throws SQLException {
        var matched = true;

        for (var query : QUERIES) {
            var expected = dataset.query(query.sql().formatted("reference"));
            var actual = dataset.query(query.sql().formatted(table));
            var same = same(expected, actual);

            System.out.println(query.name() + ": " + (same ? "match" : "MISMATCH"));
            actual.forEach(row -> System.out.println("  " + row));
            if (!same) {
                System.out.println("  expected " + expected);
            }
            matched &= same;
        }

        return matched;
    }

    private static boolean same(List<List<Object>> expected, List<List<Object>> actual) {
        if (expected.size() != actual.size()) {
            return false;
        }

        for (var row = 0; row < expected.size(); row++) {
            var left = expected.get(row);
            var right = actual.get(row);
            if (left.size() != right.size()) {
                return false;
            }
            for (var column = 0; column < left.size(); column++) {
                if (!same(left.get(column), right.get(column))) {
                    return false;
                }
            }
        }

        return true;
    }

    private static boolean same(Object expected, Object actual) {
        if (expected instanceof Double left && actual instanceof Double right) {
            return Math.abs(left - right) <= TOLERANCE * Math.max(Math.abs(left), Math.abs(right));
        }

        return expected.equals(actual);
    }
}
