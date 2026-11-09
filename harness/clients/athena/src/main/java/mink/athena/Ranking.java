package mink.athena;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

import com.amazonaws.athena.connector.lambda.domain.predicate.OrderByField;

final class Ranking {
    private final Comparator<Map<String, Object>> order;
    private final PriorityQueue<Map<String, Object>> kept;
    private final long limit;

    Ranking(List<OrderByField> fields, long limit) {
        this.order = order(fields);
        this.kept = new PriorityQueue<>(this.order.reversed());
        this.limit = limit;
    }

    void offer(Map<String, Object> row) {
        kept.add(row);
        if (kept.size() > limit) {
            kept.poll();
        }
    }

    List<Map<String, Object>> rows() {
        var rows = new ArrayList<>(kept);
        rows.sort(order);

        return rows;
    }

    private static Comparator<Map<String, Object>> order(List<OrderByField> fields) {
        Comparator<Map<String, Object>> order = (left, right) -> 0;
        for (var field : fields) {
            var name = Columns.name(field.getColumnName());
            var direction = field.getDirection();
            Comparator<Object> values = Ranking::compare;
            if (!direction.isAscending()) {
                values = values.reversed();
            }
            values = direction.isNullsFirst() ? Comparator.nullsFirst(values) : Comparator.nullsLast(values);
            order = order.thenComparing(row -> row.get(name), values);
        }

        return order;
    }

    @SuppressWarnings("unchecked")
    private static int compare(Object left, Object right) {
        if (left instanceof Comparable comparable && left.getClass().isInstance(right)) {
            return comparable.compareTo(right);
        }
        if (left instanceof byte[] leftBytes && right instanceof byte[] rightBytes) {
            return java.util.Arrays.compareUnsigned(leftBytes, rightBytes);
        }

        return 0;
    }
}
