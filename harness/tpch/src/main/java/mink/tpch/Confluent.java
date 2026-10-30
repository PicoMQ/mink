package mink.tpch;

import java.io.UncheckedIOException;
import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.json.JsonSchemaUtils;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer;
import org.apache.kafka.common.serialization.Serializer;

record Confluent(String url, JsonSchema schema) implements Registry {
    private static final ObjectMapper JSON = new ObjectMapper();

    static Confluent of(String url, String schema) {
        return new Confluent(url, new JsonSchema(schema));
    }

    @Override
    public Serializer<Object> serializer(String topic) {
        var serializer = new KafkaJsonSchemaSerializer<Object>();
        serializer.configure(Map.of("schema.registry.url", url), false);

        return serializer;
    }

    @Override
    public Object value(String json) {
        try {
            return JsonSchemaUtils.envelope(schema, JSON.readTree(json));
        } catch (JsonProcessingException error) {
            throw new UncheckedIOException(error);
        }
    }
}
