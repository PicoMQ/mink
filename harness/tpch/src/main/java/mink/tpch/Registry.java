package mink.tpch;

import org.apache.kafka.common.serialization.Serializer;

sealed interface Registry permits Confluent, Glue {
    Serializer<Object> serializer(String topic);

    Object value(String json);

    static Registry of(String type, String registry, String schema) {
        return switch (type) {
            case "confluent" -> Confluent.of(registry, schema);
            case "glue" -> Glue.of(registry, schema);
            default -> throw new IllegalArgumentException(type + " is not a registry type: confluent or glue");
        };
    }
}
