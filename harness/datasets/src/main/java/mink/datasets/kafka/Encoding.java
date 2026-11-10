package mink.datasets.kafka;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import org.apache.kafka.common.serialization.Serializer;

public sealed interface Encoding permits Confluent, Glue, Plain {
    String CONFLUENT_URL = "schema.registry.url";

    Serializer<Object> serializer(String topic);

    Object value(String json);

    static Encoding of(Properties kafka, Optional<Schema> schema) {
        if (schema.isEmpty()) {
            return new Plain();
        }

        var settings = new HashMap<String, Object>();
        kafka.forEach((key, value) -> settings.put((String) key, value));
        if (settings.containsKey(CONFLUENT_URL)) {
            return new Confluent(Map.copyOf(settings), schema.get());
        }
        if (settings.containsKey(AWSSchemaRegistryConstants.REGISTRY_NAME)) {
            return new Glue(Map.copyOf(settings), schema.get());
        }

        throw new IllegalArgumentException("a dataset with a schema needs " + CONFLUENT_URL + " or "
                + AWSSchemaRegistryConstants.REGISTRY_NAME + " in the Kafka properties");
    }
}
