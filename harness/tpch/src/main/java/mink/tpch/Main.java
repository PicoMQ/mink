package mink.tpch;

import java.io.IOException;
import java.io.Reader;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

public final class Main {
    private static final int PARTITIONS = 2;
    private static final String GRANULARITY = "month";
    private static final double DEFAULT_SCALE = 0.5;

    public static void main(String[] arguments) throws Exception {
        if (arguments.length < 4) {
            System.err.println("usage: <kafka-properties> <confluent|glue> <registry> <mink-url> [scale]");
            System.exit(2);
        }

        var kafka = properties(Path.of(arguments[0]));
        var registry = Registry.of(arguments[1], arguments[2], Dataset.SCHEMA);
        var mink = Mink.at(URI.create(arguments[3]));
        var scale = arguments.length > 4 ? Double.parseDouble(arguments[4]) : DEFAULT_SCALE;

        try (var dataset = new Dataset()) {
            var rows = dataset.generate(scale);
            System.out.printf("generated %,d %s rows at scale %s%n", rows, Dataset.TABLE, scale);

            var topic = new Topic(kafka, Dataset.TABLE);
            topic.create(PARTITIONS);
            mink.addTable(Dataset.TABLE, GRANULARITY);

            topic.write(dataset, registry, rows);
            mink.awaitServed(Dataset.TABLE, rows);

            if (!Check.run(dataset, dataset.attach(mink))) {
                System.exit(1);
            }
        }
    }

    private static Properties properties(Path path) throws IOException {
        var properties = new Properties();

        try (Reader reader = Files.newBufferedReader(path)) {
            properties.load(reader);
        }

        return properties;
    }
}
