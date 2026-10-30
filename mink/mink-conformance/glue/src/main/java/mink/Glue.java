package mink;

import com.amazonaws.services.schemaregistry.deserializers.protobuf.ProtobufSchemaParser;
import com.amazonaws.services.schemaregistry.serializers.GlueSchemaRegistryKafkaSerializer;
import com.amazonaws.services.schemaregistry.serializers.json.JsonDataWithSchema;
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import com.amazonaws.services.schemaregistry.utils.ProtobufMessageType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DecoderFactory;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.AlreadyExistsException;

public final class Glue {
    private static final String REGISTRY = "mink-fixtures";
    private static final List<String> FORMATS = List.of("avro", "json", "protobuf");
    private static final List<String> PARTS = List.of("key", "value");
    private static final List<String> COMPRESSIONS = List.of("NONE", "ZLIB");
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final HexFormat HEX = HexFormat.of();

    private record Source(int id, byte[] payload) {}

    public static void main(String[] arguments) throws Exception {
        Path payloads = Path.of(arguments[0]).resolve("payload");
        String region = System.getenv("AWS_REGION");

        try (GlueClient glue = GlueClient.builder().region(Region.of(region)).build()) {
            try {
                glue.createRegistry(request -> request.registryName(REGISTRY));
            } catch (AlreadyExistsException exists) {
                System.out.println("registry " + REGISTRY + " exists");
            }
        }

        for (String format : FORMATS) {
            JsonNode fixture = JSON.readTree(payloads.resolve(format).resolve("cases.json").toFile());
            ArrayNode records = JSON.createArrayNode();

            for (JsonNode testCase : fixture.get("cases")) {
                JsonNode caseRecords = testCase.get("records");

                for (int index = 0; index < caseRecords.size(); index++) {
                    for (String part : PARTS) {
                        Source source = source(fixture, testCase, caseRecords.get(index), part);
                        if (source == null) {
                            continue;
                        }

                        String definition = definition(fixture, source.id());
                        Object data = data(format, definition, source.payload());
                        if (data == null) {
                            continue;
                        }

                        for (String compression : COMPRESSIONS) {
                            String topic = testCase.get("topic").asText();
                            byte[] bytes = serializer(region, format, topic, part, compression).serialize(topic, data);

                            ObjectNode written = records.addObject();
                            written.put("case", testCase.get("name").asText());
                            written.put("record", index);
                            written.put("part", part);
                            written.put("confluent_id", source.id());
                            written.put("version", version(bytes).toString());
                            written.put("bytes", HEX.formatHex(bytes));
                        }
                    }
                }
            }

            ObjectNode written = JSON.createObjectNode();
            written.set("records", records);

            Path target = payloads.resolve("glue").resolve(format + ".json");
            Files.createDirectories(target.getParent());
            JSON.writerWithDefaultPrettyPrinter().writeValue(target.toFile(), written);
            System.out.println(target + ": " + records.size() + " records");
        }
    }

    private static Source source(JsonNode fixture, JsonNode testCase, JsonNode record, String part) {
        JsonNode hex = record.get(part);
        if (hex == null || hex.isNull() || !record.get("row").get("_kafka_" + part).isNull()) {
            return null;
        }

        byte[] bytes = HEX.parseHex(hex.asText());
        Source named = byHeader(fixture, record, part, bytes);
        if (named == null && bytes.length >= 5 && bytes[0] == 0) {
            named = new Source(ByteBuffer.wrap(bytes, 1, 4).getInt(), Arrays.copyOfRange(bytes, 5, bytes.length));
        }
        if (named == null || !merged(testCase, part, named.id()) || definition(fixture, named.id()) == null) {
            return null;
        }

        return named;
    }

    private static Source byHeader(JsonNode fixture, JsonNode record, String part, byte[] bytes) {
        String name = "__" + part + "_schema_id";
        JsonNode headers = record.get("headers");

        for (int index = headers.size() - 1; index >= 0; index--) {
            JsonNode header = headers.get(index);
            if (!header.get(0).asText().equals(name) || header.get(1).isNull()) {
                continue;
            }

            byte[] value = HEX.parseHex(header.get(1).asText());
            if (value.length < 17 || value[0] != 1) {
                return null;
            }

            ByteBuffer guid = ByteBuffer.wrap(value, 1, 16);
            String named = new UUID(guid.getLong(), guid.getLong()).toString();
            for (JsonNode known : fixture.get("guids")) {
                if (known.get("guid").asText().equals(named) && !known.get("id").isNull()) {
                    byte[] payload = new byte[value.length - 17 + bytes.length];
                    System.arraycopy(value, 17, payload, 0, value.length - 17);
                    System.arraycopy(bytes, 0, payload, value.length - 17, bytes.length);

                    return new Source(known.get("id").asInt(), payload);
                }
            }
            return null;
        }

        return null;
    }

    private static boolean merged(JsonNode testCase, String part, int id) {
        for (JsonNode merge : testCase.get("merges")) {
            if (merge.get("part").asText().equals(part) && merge.get("id").asInt() == id) {
                return merge.has("table");
            }
        }

        return false;
    }

    private static String definition(JsonNode fixture, int id) {
        for (JsonNode schema : fixture.get("registry")) {
            if (schema.get("id").asInt() != id) {
                continue;
            }

            JsonNode references = schema.get("references");
            boolean referenced = references != null && references.size() > 0;
            JsonNode definition = schema.get("definition");

            return referenced || definition == null || definition.isNull() ? null : definition.asText();
        }

        return null;
    }

    private static Object data(String format, String definition, byte[] payload) throws Exception {
        return switch (format) {
            case "avro" -> {
                Schema schema = new Schema.Parser().parse(definition);
                Object datum = new GenericDatumReader<>(schema).read(null, DecoderFactory.get().binaryDecoder(payload, null));
                yield datum instanceof GenericContainer ? datum : null;
            }
            case "json" -> JsonDataWithSchema.builder(definition, new String(payload, StandardCharsets.UTF_8)).build();
            case "protobuf" -> protobuf(definition, payload);
            default -> throw new IllegalArgumentException(format);
        };
    }

    private static DynamicMessage protobuf(String definition, byte[] payload) throws Exception {
        ByteBuffer buffer = ByteBuffer.wrap(payload);

        long count = zigzag(buffer);
        if (count != 0 && !(count == 1 && zigzag(buffer) == 0)) {
            return null;
        }

        Descriptors.FileDescriptor file = ProtobufSchemaParser.parse(definition, "fixture.proto");
        byte[] message = Arrays.copyOfRange(payload, buffer.position(), payload.length);

        return DynamicMessage.parseFrom(file.getMessageTypes().get(0), message);
    }

    private static long zigzag(ByteBuffer buffer) {
        long value = 0;

        for (int shift = 0; shift < 64; shift += 7) {
            byte next = buffer.get();
            value |= (long) (next & 0x7f) << shift;
            if ((next & 0x80) == 0) {
                break;
            }
        }

        return (value >>> 1) ^ -(value & 1);
    }

    private static GlueSchemaRegistryKafkaSerializer serializer(
            String region, String format, String topic, String part, String compression) {
        Map<String, Object> configs = new HashMap<>();
        configs.put(AWSSchemaRegistryConstants.AWS_REGION, region);
        configs.put(AWSSchemaRegistryConstants.REGISTRY_NAME, REGISTRY);
        configs.put(AWSSchemaRegistryConstants.SCHEMA_NAME, topic);
        configs.put(AWSSchemaRegistryConstants.DATA_FORMAT, format.toUpperCase());
        configs.put(AWSSchemaRegistryConstants.SCHEMA_AUTO_REGISTRATION_SETTING, true);
        configs.put(AWSSchemaRegistryConstants.COMPATIBILITY_SETTING, "NONE");
        configs.put(AWSSchemaRegistryConstants.COMPRESSION_TYPE, compression);
        configs.put(AWSSchemaRegistryConstants.PROTOBUF_MESSAGE_TYPE, ProtobufMessageType.DYNAMIC_MESSAGE.getName());

        GlueSchemaRegistryKafkaSerializer serializer = new GlueSchemaRegistryKafkaSerializer();
        serializer.configure(configs, part.equals("key"));

        return serializer;
    }

    private static UUID version(byte[] bytes) {
        ByteBuffer header = ByteBuffer.wrap(bytes, 2, 16);

        return new UUID(header.getLong(), header.getLong());
    }
}
