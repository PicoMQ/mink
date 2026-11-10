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

record Events() implements Dataset {
    private static final Duration SPAN = Duration.ofHours(50);

    @Override
    public String name() {
        return "events";
    }

    @Override
    public String topic() {
        return "events";
    }

    @Override
    public String granularity() {
        return "hour";
    }

    @Override
    public Optional<Schema> schema() {
        return Optional.empty();
    }

    @Override
    public List<String> queries() {
        return List.of("counts");
    }

    @Override
    public double defaultScale() {
        return 10_000;
    }

    @Override
    public Topic.Source generate(Reference reference, double scale) throws SQLException {
        var events = (long) scale;
        var last = Instant.now().truncatedTo(ChronoUnit.MINUTES).toEpochMilli();
        var every = Math.max(1, SPAN.toMillis() / events);

        reference.execute("""
                CREATE TABLE %s AS SELECT
                    make_timestamp((%d - (%d - 1 - i) * %d) * 1000)::TIMESTAMPTZ AS _kafka_timestamp,
                    'event-' || i AS key,
                    encode(json_object('id', i, 'kind', ['click', 'view', 'purchase'][i %% 3 + 1])::VARCHAR) AS _kafka_value
                FROM range(%d) events(i)
                """.formatted(Reference.TABLE, last, events, every, events));

        return reference.rows("""
                SELECT key, decode(_kafka_value), epoch_ms(_kafka_timestamp)
                FROM %s ORDER BY _kafka_timestamp
                """.formatted(Reference.TABLE));
    }
}
