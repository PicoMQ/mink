package mink.datasets.dataset;

import java.sql.SQLException;
import java.util.List;
import java.util.Optional;

import mink.datasets.kafka.Schema;
import mink.datasets.kafka.Topic;
import mink.datasets.reference.Reference;

public sealed interface Dataset permits Tpch, Telematics, Events {
    String name();

    String topic();

    String granularity();

    Optional<Schema> schema();

    List<String> queries();

    double defaultScale();

    Topic.Source generate(Reference reference, double scale) throws SQLException;

    static Dataset of(String name) {
        return switch (name) {
            case "tpch" -> new Tpch();
            case "telematics" -> new Telematics();
            case "events" -> new Events();
            default -> throw new IllegalArgumentException(name + " is not a dataset: tpch, telematics or events");
        };
    }
}
