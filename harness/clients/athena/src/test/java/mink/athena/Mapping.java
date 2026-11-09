package mink.athena;

import java.nio.ByteBuffer;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;

import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class Mapping {
    private static final Types.StructType HEADER = Types.StructType.of(
            Types.NestedField.required(8, "key", Types.StringType.get()),
            Types.NestedField.optional(9, "value", Types.BinaryType.get()));

    static final Schema SCHEMA = new Schema(
            Types.NestedField.required(1, "_kafka_partition", Types.IntegerType.get()),
            Types.NestedField.required(2, "_kafka_offset", Types.LongType.get()),
            Types.NestedField.required(3, "_kafka_timestamp", Types.TimestampType.withZone()),
            Types.NestedField.optional(4, "_kafka_key", Types.BinaryType.get()),
            Types.NestedField.required(5, "_kafka_headers", Types.ListType.ofRequired(7, HEADER)),
            Types.NestedField.optional(6, "_kafka_value", Types.BinaryType.get()),
            Types.NestedField.optional(10, "vehicleId", Types.StringType.get()),
            Types.NestedField.optional(11, "tags", Types.MapType.ofOptional(12, 13, Types.StringType.get(), Types.LongType.get())));

    @Test
    void columnsMapToAthenaTypesUnderLowercaseNames() {
        var schema = Columns.schema(SCHEMA);

        assertEquals(new ArrowType.Timestamp(TimeUnit.MILLISECOND, "UTC"), schema.findField("_kafka_timestamp").getType());
        assertEquals(ArrowType.Binary.INSTANCE, schema.findField("_kafka_value").getType());
        assertEquals(List.of("key", "value"), schema.findField("_kafka_headers").getChildren().getFirst()
                .getChildren().stream().map(field -> field.getName()).toList());
        assertEquals(ArrowType.Utf8.INSTANCE, schema.findField("vehicleid").getType());
        assertEquals(new ArrowType.Map(false), schema.findField("tags").getType());
    }

    @Test
    void columnsAreFoundByTheirLowercaseNamesOnly() {
        var clashing = new Schema(
                Types.NestedField.optional(1, "Id", Types.LongType.get()),
                Types.NestedField.optional(2, "id", Types.LongType.get()));

        assertEquals(10, Columns.find(SCHEMA, "vehicleid").fieldId());
        assertThrows(IllegalArgumentException.class, () -> Columns.find(clashing, "id"));
    }

    @Test
    void valuesBecomeWhatTheBlockWriterTakes() {
        var header = GenericRecord.create(HEADER);
        header.setField("key", "source");
        header.setField("value", ByteBuffer.wrap(new byte[] {1, 2}));
        var record = GenericRecord.create(SCHEMA);
        record.setField("_kafka_partition", 1);
        record.setField("_kafka_offset", 42L);
        record.setField("_kafka_timestamp", OffsetDateTime.of(2026, 11, 8, 0, 0, 0, 1_000, ZoneOffset.UTC));
        record.setField("_kafka_headers", List.of(header));
        record.setField("_kafka_value", ByteBuffer.wrap(new byte[] {7}));
        record.setField("vehicleId", "vehicle-001");
        record.setField("tags", Map.of("speed", 30L));

        var row = Columns.row(record, SCHEMA);

        assertEquals(OffsetDateTime.of(2026, 11, 8, 0, 0, 0, 1_000, ZoneOffset.UTC).atZoneSameInstant(ZoneId.of("UTC")),
                row.get("_kafka_timestamp"));
        assertEquals(1, ((byte[]) row.get("_kafka_value")).length);
        assertEquals("source", ((List<Map<?, ?>>) row.get("_kafka_headers")).getFirst().get("key"));
        assertEquals("vehicle-001", row.get("vehicleid"));
        assertEquals(Map.of("speed", 30L), row.get("tags"));
    }
}
