package mink.tpch;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.List;
import java.util.stream.Stream;

final class Check {
    private static final double TOLERANCE = 1e-9;

    private record Query(String name, String sql) {
        static Query load(String name) {
            try (var sql = Check.class.getResourceAsStream("/queries/" + name.toLowerCase() + ".sql")) {
                return new Query(name, new String(sql.readAllBytes(), StandardCharsets.UTF_8));
            } catch (IOException error) {
                throw new UncheckedIOException(error);
            }
        }
    }

    private static final List<Query> QUERIES = Stream.of("Q1", "Q6").map(Query::load).toList();

    private Check() {}

    static boolean run(Dataset dataset, String table) throws SQLException {
        var matched = true;

        for (var query : QUERIES) {
            var expected = dataset.query(query.sql().formatted(Dataset.REFERENCE));
            var actual = dataset.query(query.sql().formatted(table));
            var matches = same(expected, actual);

            System.out.println(query.name() + ": " + (matches ? "match" : "MISMATCH"));
            actual.forEach(row -> System.out.println("  " + row));
            if (!matches) {
                System.out.println("  expected " + expected);
            }
            matched &= matches;
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
