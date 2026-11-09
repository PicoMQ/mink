package mink.athena;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;

import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.OrderByField;
import com.amazonaws.athena.connector.lambda.domain.predicate.Range;
import com.amazonaws.athena.connector.lambda.domain.predicate.SortedRangeSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.iceberg.expressions.Expression;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class Pushdown {
    private static final ArrowType BIGINT = new ArrowType.Int(64, true);
    private static final ArrowType MILLIS = new ArrowType.Timestamp(TimeUnit.MILLISECOND, "UTC");

    private static Expression expression(Map<String, ValueSet> summary) {
        var constraints = new Constraints(summary, List.of(), List.of(), Constraints.DEFAULT_NO_LIMIT, Map.of(), null);

        return Filters.expression(Mapping.SCHEMA, constraints);
    }

    @Test
    void rangesBecomeIcebergBoundsOnTheColumn() {
        try (var allocator = new BlockAllocatorImpl()) {
            var offsets = SortedRangeSet.of(false, Range.range(allocator, BIGINT, 10L, true, 20L, false));
            var since = SortedRangeSet.of(false, Range.greaterThan(allocator, MILLIS, ZonedDateTime.ofInstant(Instant.ofEpochMilli(1_000L), ZoneId.of("UTC"))));

            assertEquals("(ref(name=\"_kafka_offset\") >= 10 and ref(name=\"_kafka_offset\") < 20)",
                    expression(Map.of("_kafka_offset", offsets)).toString());
            assertEquals("(ref(name=\"_kafka_timestamp\") > 1000000 and not_null(ref(name=\"_kafka_timestamp\")))",
                    expression(Map.of("_kafka_timestamp", since)).toString());
        }
    }

    @Test
    void valuesItCannotTranslateKeepEveryFile() {
        try (var allocator = new BlockAllocatorImpl()) {
            var text = SortedRangeSet.of(false, Range.equal(allocator, ArrowType.Utf8.INSTANCE, "vehicle-001"));

            assertEquals("true", expression(Map.of()).toString());
            assertEquals("ref(name=\"vehicleId\") == \"vehicle-001\"", expression(Map.of("vehicleid", text)).toString());
        }
    }

    @Test
    void aSplitKeepsItsTopRowsInOrder() {
        var ranking = new Ranking(List.of(new OrderByField("_kafka_offset", OrderByField.Direction.DESC_NULLS_LAST)), 2);
        for (var offset : List.of(3L, 9L, 1L, 7L)) {
            ranking.offer(Map.of("_kafka_offset", offset));
        }

        assertEquals(List.of(9L, 7L), ranking.rows().stream().map(row -> row.get("_kafka_offset")).toList());
    }
}
