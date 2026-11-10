package mink.athena;

import java.nio.ByteBuffer;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

final class Columns {
    private static final ZoneId UTC = ZoneId.of("UTC");

    private Columns() {}

    static Schema schema(org.apache.iceberg.Schema schema) {
        var builder = SchemaBuilder.newBuilder();
        schema.columns().forEach(column -> builder.addField(field(name(column.name()), column.type(), true)));

        return builder.build();
    }

    static String name(String column) {
        return column.toLowerCase(Locale.ROOT);
    }

    static Types.NestedField find(org.apache.iceberg.Schema schema, String name) {
        var matches = schema.columns().stream().filter(column -> name(column.name()).equals(name)).toList();

        if (matches.isEmpty()) {
            throw new IllegalArgumentException("no column " + name);
        }
        if (matches.size() > 1) {
            throw new IllegalArgumentException("columns " + matches.stream().map(Types.NestedField::name).toList()
                    + " differ only by case");
        }

        return matches.getFirst();
    }

    static Map<String, Object> row(Record record, org.apache.iceberg.Schema projected) {
        var row = new LinkedHashMap<String, Object>();
        for (var column : projected.columns()) {
            row.put(name(column.name()), value(column.type(), record.getField(column.name())));
        }

        return row;
    }

    private static Field field(String name, Type type, boolean nullable) {
        return switch (type.typeId()) {
            case STRUCT -> new Field(name, FieldType.nullable(ArrowType.Struct.INSTANCE),
                    type.asStructType().fields().stream()
                            .map(child -> field(name(child.name()), child.type(), true))
                            .toList());
            case LIST -> new Field(name, FieldType.nullable(ArrowType.List.INSTANCE),
                    List.of(field("element", type.asListType().elementType(), true)));
            case MAP -> new Field(name, FieldType.nullable(new ArrowType.Map(false)),
                    List.of(new Field(MapVector.DATA_VECTOR_NAME, FieldType.notNullable(ArrowType.Struct.INSTANCE), List.of(
                            field(MapVector.KEY_NAME, type.asMapType().keyType(), false),
                            field(MapVector.VALUE_NAME, type.asMapType().valueType(), true)))));
            default -> new Field(name, new FieldType(nullable, primitive(type), null), null);
        };
    }

    private static ArrowType primitive(Type type) {
        return switch (type.typeId()) {
            case BOOLEAN -> ArrowType.Bool.INSTANCE;
            case INTEGER -> new ArrowType.Int(32, true);
            case LONG, TIME -> new ArrowType.Int(64, true);
            case FLOAT -> new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
            case DOUBLE -> new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
            case DATE -> new ArrowType.Date(DateUnit.DAY);
            case TIMESTAMP -> ((Types.TimestampType) type).shouldAdjustToUTC()
                    ? new ArrowType.Timestamp(TimeUnit.MILLISECOND, "UTC")
                    : new ArrowType.Date(DateUnit.MILLISECOND);
            case STRING, UUID -> ArrowType.Utf8.INSTANCE;
            case BINARY, FIXED -> ArrowType.Binary.INSTANCE;
            case DECIMAL -> {
                var decimal = (Types.DecimalType) type;
                yield new ArrowType.Decimal(decimal.precision(), decimal.scale(), 128);
            }
            default -> throw new IllegalArgumentException("type " + type + " has no Athena type");
        };
    }

    static Object value(Type type, Object value) {
        if (value == null) {
            return null;
        }

        return switch (type.typeId()) {
            case TIMESTAMP -> value instanceof OffsetDateTime timestamp ? timestamp.atZoneSameInstant(UTC) : value;
            case TIME -> ((LocalTime) value).toNanoOfDay() / 1_000;
            case STRING, UUID -> value.toString();
            case BINARY -> bytes((ByteBuffer) value);
            case STRUCT -> struct(type.asStructType(), (Record) value);
            case LIST -> list(type.asListType(), (List<?>) value);
            case MAP -> map(type.asMapType(), (Map<?, ?>) value);
            default -> value;
        };
    }

    private static byte[] bytes(ByteBuffer buffer) {
        var bytes = new byte[buffer.remaining()];
        buffer.duplicate().get(bytes);

        return bytes;
    }

    private static Map<String, Object> struct(Types.StructType type, Record record) {
        var struct = new LinkedHashMap<String, Object>();
        for (var field : type.fields()) {
            struct.put(name(field.name()), value(field.type(), record.getField(field.name())));
        }

        return struct;
    }

    private static List<Object> list(Types.ListType type, List<?> values) {
        var list = new ArrayList<Object>(values.size());
        values.forEach(value -> list.add(value(type.elementType(), value)));

        return list;
    }

    private static Map<Object, Object> map(Types.MapType type, Map<?, ?> values) {
        var map = new LinkedHashMap<Object, Object>();
        values.forEach((key, value) -> map.put(value(type.keyType(), key), value(type.valueType(), value)));

        return map;
    }
}
