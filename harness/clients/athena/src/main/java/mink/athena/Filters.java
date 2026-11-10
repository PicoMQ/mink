package mink.athena;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.Optional;

import com.amazonaws.athena.connector.lambda.data.DateTimeFormatterUtil;
import com.amazonaws.athena.connector.lambda.domain.predicate.AllOrNoneValueSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.EquatableValueSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.Marker;
import com.amazonaws.athena.connector.lambda.domain.predicate.Range;
import com.amazonaws.athena.connector.lambda.domain.predicate.SortedRangeSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

final class Filters {
    private Filters() {}

    static Expression expression(Schema schema, Constraints constraints) {
        Expression expression = Expressions.alwaysTrue();
        for (Map.Entry<String, ValueSet> entry : constraints.getSummary().entrySet()) {
            var column = Columns.find(schema, entry.getKey());
            if (column.type().isPrimitiveType()) {
                expression = Expressions.and(expression, column(column, entry.getValue()));
            }
        }

        return expression;
    }

    private static Expression column(Types.NestedField column, ValueSet values) {
        var name = column.name();
        var nulls = values.isNullAllowed() ? Expressions.isNull(name) : Expressions.alwaysFalse();

        return switch (values) {
            case AllOrNoneValueSet all when all.isAll() -> Expressions.alwaysTrue();
            case AllOrNoneValueSet none -> nulls;
            case SortedRangeSet ranges when ranges.isAll() -> Expressions.alwaysTrue();
            case SortedRangeSet ranges -> {
                var expression = nulls;
                for (var range : ranges.getOrderedRanges()) {
                    expression = Expressions.or(expression, range(column, range));
                }
                yield expression;
            }
            case EquatableValueSet set when set.isWhiteList() -> {
                var expression = nulls;
                for (var position = 0; position < set.getValues().getRowCount(); position++) {
                    var literal = literal(column.type(), set.getType(), set.getValue(position));
                    if (literal.isEmpty()) {
                        yield Expressions.alwaysTrue();
                    }
                    expression = Expressions.or(expression, Expressions.equal(name, literal.get()));
                }
                yield expression;
            }
            default -> Expressions.alwaysTrue();
        };
    }

    private static Expression range(Types.NestedField column, Range range) {
        var name = column.name();
        if (range.isSingleValue()) {
            return literal(column.type(), range.getType(), range.getLow().getValue())
                    .<Expression>map(value -> Expressions.equal(name, value))
                    .orElse(Expressions.alwaysTrue());
        }

        return Expressions.and(bound(column, range.getLow(), true), bound(column, range.getHigh(), false));
    }

    private static Expression bound(Types.NestedField column, Marker marker, boolean lower) {
        if (lower ? marker.isLowerUnbounded() : marker.isUpperUnbounded()) {
            return Expressions.notNull(column.name());
        }

        var name = column.name();
        var exact = marker.getBound() == Marker.Bound.EXACTLY;
        return literal(column.type(), marker.getType(), marker.getValue())
                .<Expression>map(value -> lower
                        ? exact ? Expressions.greaterThanOrEqual(name, value) : Expressions.greaterThan(name, value)
                        : exact ? Expressions.lessThanOrEqual(name, value) : Expressions.lessThan(name, value))
                .orElse(Expressions.alwaysTrue());
    }

    static Optional<Object> literal(Type type, ArrowType arrow, Object value) {
        return Optional.ofNullable(switch (type.typeId()) {
            case BOOLEAN -> value instanceof Boolean flag ? flag : null;
            case INTEGER -> value instanceof Number number ? number.intValue() : null;
            case LONG, TIME -> value instanceof Number number ? number.longValue() : null;
            case FLOAT -> value instanceof Number number ? number.floatValue() : null;
            case DOUBLE -> value instanceof Number number ? number.doubleValue() : null;
            case DECIMAL -> value instanceof BigDecimal decimal ? decimal : null;
            case STRING, UUID -> value == null ? null : value.toString();
            case DATE -> switch (value) {
                case LocalDate date -> (int) date.toEpochDay();
                case Number days -> days.intValue();
                case null, default -> null;
            };
            case TIMESTAMP -> switch (value) {
                case Number number when arrow instanceof ArrowType.Timestamp zoned -> micros(
                        DateTimeFormatterUtil.constructZonedDateTime(number.longValue(), zoned).toInstant());
                case Number millis -> millis.longValue() * 1_000;
                case LocalDateTime timestamp -> micros(timestamp.toInstant(ZoneOffset.UTC));
                case ZonedDateTime timestamp -> micros(timestamp.toInstant());
                case null, default -> null;
            };
            default -> null;
        });
    }

    private static long micros(Instant instant) {
        return ChronoUnit.MICROS.between(Instant.EPOCH, instant);
    }
}
