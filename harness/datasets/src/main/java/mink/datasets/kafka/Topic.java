package mink.datasets.kafka;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.StringSerializer;

public record Topic(Properties kafka, String name) {
    public interface Source {
        void each(Consumer<Row> sink) throws Exception;
    }

    private static final long PROGRESS_SECONDS = 1;

    public void create(int partitions) throws ExecutionException, InterruptedException {
        try (var admin = Admin.create(kafka)) {
            var topic = new NewTopic(name, Optional.of(partitions), Optional.empty())
                    .configs(Map.of(TopicConfig.RETENTION_MS_CONFIG, "-1"));
            admin.createTopics(List.of(topic)).all().get();
        }
        System.out.printf("created topic %s, %d partitions%n", name, partitions);
    }

    public void write(Source source, Encoding encoding, OptionalLong rows) throws Exception {
        var settings = new Properties();
        settings.putAll(kafka);
        settings.put(ProducerConfig.LINGER_MS_CONFIG, 100);

        var failure = new AtomicReference<Exception>();
        var written = new AtomicLong();
        var reported = new AtomicLong();
        var progress = Executors.newSingleThreadScheduledExecutor();
        progress.scheduleAtFixedRate(() -> report(written.get(), reported.getAndSet(written.get()), rows),
                PROGRESS_SECONDS, PROGRESS_SECONDS, TimeUnit.SECONDS);

        try (progress; var producer = new KafkaProducer<>(settings, new StringSerializer(), encoding.serializer(name))) {
            source.each(row -> {
                var record = new ProducerRecord<>(name, null, row.timestampMs(), row.key(), encoding.value(row.value()));
                producer.send(record, (metadata, error) -> {
                    if (error != null) {
                        failure.compareAndSet(null, error);
                    }
                });

                written.incrementAndGet();
            });
            producer.flush();
        }

        if (failure.get() != null) {
            throw failure.get();
        }
        report(written.get(), reported.get(), rows);
    }

    private static void report(long written, long reported, OptionalLong rows) {
        if (rows.isPresent()) {
            System.out.printf("produced %,d of %,d%n", written, rows.getAsLong());
        } else {
            System.out.printf("produced %,d, %,d a second%n", written, (written - reported) / PROGRESS_SECONDS);
        }
    }
}
