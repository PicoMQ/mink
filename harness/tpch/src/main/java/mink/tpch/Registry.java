package mink.tpch;

import java.util.Objects;

import org.apache.kafka.common.serialization.Serializer;

sealed interface Registry permits Confluent, Glue {
    Serializer<Object> serializer(String topic);

    Object value(String json);

    static Registry of(String type, String location, String schema) {
        return switch (type) {
            case "confluent" -> Confluent.of(location, schema);
            case "glue" -> new Glue(location, Objects.requireNonNull(System.getenv("AWS_REGION"), "AWS_REGION"), schema);
            default -> throw new IllegalArgumentException(type + " is not a registry type: confluent or glue");
        };
    }
}
