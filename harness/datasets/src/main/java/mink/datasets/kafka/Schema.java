package mink.datasets.kafka;

import java.io.IOException;
import java.io.UncheckedIOException;

import io.confluent.kafka.schemaregistry.json.JsonSchema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DecoderFactory;

public sealed interface Schema permits Schema.Json, Schema.Avro {
    String text();

    record Json(String text, JsonSchema parsed) implements Schema {
        public static Json of(String text) {
            return new Json(text, new JsonSchema(text));
        }
    }

    record Avro(String text, org.apache.avro.Schema parsed) implements Schema {
        public static Avro of(String text) {
            return new Avro(text, new org.apache.avro.Schema.Parser().parse(text));
        }

        GenericRecord record(String json) {
            try {
                return new GenericDatumReader<GenericRecord>(parsed)
                        .read(null, DecoderFactory.get().jsonDecoder(parsed, json));
            } catch (IOException error) {
                throw new UncheckedIOException(error);
            }
        }
    }
}
