package mink.datasets.check;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.List;
import java.util.Objects;

import mink.datasets.reference.Reference;

public final class Check {
    private static final double TOLERANCE = 1e-9;

    private Check() {}

    public static boolean run(Reference reference, String dataset, List<String> queries, String table)
            throws SQLException {
        var matched = true;

        for (var name : queries) {
            var query = sql(dataset, name);
            var expected = reference.query(query.formatted(Reference.TABLE));
            var actual = reference.query(query.formatted(table));
            var matches = same(expected, actual);

            System.out.println(name + ": " + (matches ? "match" : "MISMATCH"));
            actual.forEach(row -> System.out.println("  " + row));
            if (!matches) {
                System.out.println("  expected " + expected);
            }
            matched &= matches;
        }

        return matched;
    }

    private static String sql(String dataset, String name) {
        try (var sql = Check.class.getResourceAsStream("/%s/%s.sql".formatted(dataset, name))) {
            return new String(sql.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException error) {
            throw new UncheckedIOException(error);
        }
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

        return Objects.equals(expected, actual);
    }
}
