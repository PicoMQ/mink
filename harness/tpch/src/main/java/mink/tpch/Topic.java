package mink.tpch;

import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

record Topic(Properties kafka, String name) {
    private static final long PROGRESS_EVERY = 250_000;

    void create(int partitions) throws ExecutionException, InterruptedException {
        try (var admin = Admin.create(kafka)) {
            admin.createTopics(List.of(new NewTopic(name, Optional.of(partitions), Optional.empty()))).all().get();
        }
    }

    void write(Dataset dataset, Registry registry, long rows) throws Exception {
        var settings = new Properties();
        settings.putAll(kafka);
        settings.put(ProducerConfig.LINGER_MS_CONFIG, 100);

        var failure = new AtomicReference<Exception>();
        var written = new AtomicLong();

        try (var producer = new KafkaProducer<>(settings, new StringSerializer(), registry.serializer(name))) {
            dataset.rows(row -> {
                producer.send(new ProducerRecord<>(name, row.key(), registry.value(row.json())), (metadata, error) -> {
                    if (error != null) {
                        failure.compareAndSet(null, error);
                    }
                });

                if (written.incrementAndGet() % PROGRESS_EVERY == 0) {
                    System.out.printf("produced %,d of %,d%n", written.get(), rows);
                }
            });
            producer.flush();
        }

        if (failure.get() != null) {
            throw failure.get();
        }
        System.out.printf("produced %,d of %,d%n", written.get(), rows);
    }
}
