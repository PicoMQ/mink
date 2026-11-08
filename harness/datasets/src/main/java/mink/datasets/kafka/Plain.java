package mink.datasets.kafka;

import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;

record Plain() implements Encoding {
    private static final StringSerializer STRINGS = new StringSerializer();

    @Override
    public Serializer<Object> serializer(String topic) {
        return (name, value) -> STRINGS.serialize(name, (String) value);
    }

    @Override
    public Object value(String json) {
        return json;
    }
}
