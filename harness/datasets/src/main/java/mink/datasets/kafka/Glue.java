package mink.datasets.kafka;

import java.util.HashMap;
import java.util.Map;

import com.amazonaws.services.schemaregistry.serializers.GlueSchemaRegistryKafkaSerializer;
import com.amazonaws.services.schemaregistry.serializers.json.JsonDataWithSchema;
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import org.apache.kafka.common.serialization.Serializer;

record Glue(Map<String, Object> settings, Schema schema) implements Encoding {
    @Override
    public Serializer<Object> serializer(String topic) {
        var format = switch (schema) {
            case Schema.Json ignored -> "JSON";
            case Schema.Avro ignored -> "AVRO";
        };
        var configured = new HashMap<>(settings);
        configured.put(AWSSchemaRegistryConstants.SCHEMA_NAME, topic);
        configured.put(AWSSchemaRegistryConstants.DATA_FORMAT, format);
        configured.put(AWSSchemaRegistryConstants.SCHEMA_AUTO_REGISTRATION_SETTING, true);
        var serializer = new GlueSchemaRegistryKafkaSerializer();
        serializer.configure(configured, false);

        return serializer;
    }

    @Override
    public Object value(String json) {
        return switch (schema) {
            case Schema.Json jsonSchema -> JsonDataWithSchema.builder(jsonSchema.text(), json).build();
            case Schema.Avro avro -> avro.record(json);
        };
    }
}
