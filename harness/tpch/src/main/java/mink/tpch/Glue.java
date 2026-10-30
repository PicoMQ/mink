package mink.tpch;

import java.util.Map;
import java.util.Objects;

import com.amazonaws.services.schemaregistry.serializers.GlueSchemaRegistryKafkaSerializer;
import com.amazonaws.services.schemaregistry.serializers.json.JsonDataWithSchema;
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import org.apache.kafka.common.serialization.Serializer;

record Glue(String name, String region, String schema) implements Registry {
    static Glue of(String name, String schema) {
        return new Glue(name, Objects.requireNonNull(System.getenv("AWS_REGION"), "AWS_REGION"), schema);
    }

    @Override
    public Serializer<Object> serializer(String topic) {
        var serializer = new GlueSchemaRegistryKafkaSerializer();
        serializer.configure(Map.of(
                AWSSchemaRegistryConstants.AWS_REGION, region,
                AWSSchemaRegistryConstants.REGISTRY_NAME, name,
                AWSSchemaRegistryConstants.SCHEMA_NAME, topic,
                AWSSchemaRegistryConstants.DATA_FORMAT, "JSON",
                AWSSchemaRegistryConstants.SCHEMA_AUTO_REGISTRATION_SETTING, true), false);

        return serializer;
    }

    @Override
    public Object value(String json) {
        return JsonDataWithSchema.builder(schema, json).build();
    }
}
