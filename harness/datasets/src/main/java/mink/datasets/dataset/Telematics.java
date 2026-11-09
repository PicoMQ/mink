package mink.datasets.dataset;

import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Optional;

import mink.datasets.kafka.Schema;
import mink.datasets.kafka.Topic;
import mink.datasets.reference.Reference;

record Telematics() implements Dataset {
    private static final int POINTS = 1000;
    private static final Duration EVERY = Duration.ofMinutes(3);
    private static final Duration TICK = Duration.ofSeconds(1);

    private static final String SCHEMA = """
            {
              "type": "record",
              "name": "VehicleLocation",
              "namespace": "mink.telematics",
              "fields": [
                {"name": "vehicle_id", "type": "string"},
                {"name": "located_at", "type": {"type": "long", "logicalType": "timestamp-millis"}},
                {"name": "lat", "type": "double"},
                {"name": "lon", "type": "double"},
                {"name": "heading", "type": "double"},
                {"name": "speed", "type": "double"}
              ]
            }
            """;

    @Override
    public String name() {
        return "telematics";
    }

    @Override
    public String topic() {
        return "vehicle_locations";
    }

    @Override
    public String granularity() {
        return "hour";
    }

    @Override
    public Optional<Schema> schema() {
        return Optional.of(Schema.Avro.of(SCHEMA));
    }

    @Override
    public List<String> queries() {
        return List.of("positions", "latest");
    }

    @Override
    public double defaultScale() {
        return 50;
    }

    @Override
    public Topic.Source generate(Reference reference, double vehicles) throws SQLException {
        var last = Instant.now().truncatedTo(ChronoUnit.MINUTES).toEpochMilli();
        var at = "%d - (%d - 1 - i) * %d".formatted(last, POINTS, EVERY.toMillis());

        reference.execute("CREATE TABLE %s AS %s".formatted(
                Reference.TABLE, positions(vehicles, at, "range(%d) points(i)".formatted(POINTS))));

        return reference.rows(rows(Reference.TABLE));
    }

    @Override
    public Optional<Topic.Source> live(Reference reference, double vehicles) {
        return Optional.of(sink -> {
            for (var step = (long) POINTS; ; step++) {
                var now = Instant.now().toEpochMilli();
                var points = "(SELECT %d AS i) points".formatted(step);

                reference.rows(rows("(%s)".formatted(positions(vehicles, String.valueOf(now), points)))).each(sink);
                Thread.sleep(Math.max(0, now + TICK.toMillis() - Instant.now().toEpochMilli()));
            }
        });
    }

    private static String positions(double vehicles, String at, String points) {
        return """
                SELECT
                    printf('vehicle-%%03d', v) AS vehicle_id,
                    make_timestamp((%s) * 1000)::TIMESTAMPTZ AS located_at,
                    round(37.7 + 0.1 * sin(v + i / 50.0), 6) AS lat,
                    round(-122.45 + 0.1 * cos(v + i / 50.0), 6) AS lon,
                    ((v * 37 + i * 7) %% 360)::DOUBLE AS heading,
                    round(30 + 20 * sin(v + i / 10.0), 2) AS speed
                FROM range(%d) vehicles(v), %s
                """.formatted(at, (long) vehicles, points);
    }

    private static String rows(String positions) {
        return """
                SELECT vehicle_id,
                    json_object('vehicle_id', vehicle_id, 'located_at', epoch_ms(located_at), 'lat', lat,
                        'lon', lon, 'heading', heading, 'speed', speed)::VARCHAR,
                    epoch_ms(located_at)
                FROM %s ORDER BY located_at, vehicle_id
                """.formatted(positions);
    }
}
