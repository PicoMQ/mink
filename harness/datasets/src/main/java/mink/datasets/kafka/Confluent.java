package mink.datasets.kafka;

import java.io.UncheckedIOException;
import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.schemaregistry.json.JsonSchemaUtils;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer;
import org.apache.kafka.common.serialization.Serializer;

record Confluent(Map<String, Object> settings, Schema schema) implements Encoding {
    private static final ObjectMapper JSON = new ObjectMapper();

    @Override
    @SuppressWarnings("unchecked")
    public Serializer<Object> serializer(String topic) {
        Serializer<Object> serializer = switch (schema) {
            case Schema.Json ignored -> new KafkaJsonSchemaSerializer<>();
            case Schema.Avro ignored -> (Serializer<Object>) (Serializer<?>) new KafkaAvroSerializer();
        };
        serializer.configure(settings, false);

        return serializer;
    }

    @Override
    public Object value(String json) {
        return switch (schema) {
            case Schema.Json jsonSchema -> envelope(jsonSchema, json);
            case Schema.Avro avro -> avro.record(json);
        };
    }

    private static Object envelope(Schema.Json schema, String json) {
        try {
            return JsonSchemaUtils.envelope(schema.parsed(), JSON.readTree(json));
        } catch (JsonProcessingException error) {
            throw new UncheckedIOException(error);
        }
    }
}
