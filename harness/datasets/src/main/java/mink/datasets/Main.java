package mink.datasets;

import java.io.IOException;
import java.io.Reader;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

import mink.datasets.check.Check;
import mink.datasets.dataset.Dataset;
import mink.datasets.kafka.Encoding;
import mink.datasets.kafka.Topic;
import mink.datasets.reference.Reference;

public final class Main {
    private static final int PARTITIONS = 2;

    public static void main(String[] arguments) throws Exception {
        if (arguments.length < 3) {
            System.err.println("""
                    usage: <tpch|telematics|events> <kafka-properties> <mink-url> [scale]
                      scale: the TPC-H scale factor, the number of vehicles, or the number of events""");
            System.exit(2);
        }

        var dataset = Dataset.of(arguments[0]);
        var kafka = properties(Path.of(arguments[1]));
        var encoding = Encoding.of(kafka, dataset.schema());
        var mink = Mink.at(URI.create(arguments[2]));
        var scale = arguments.length > 3 ? Double.parseDouble(arguments[3]) : dataset.defaultScale();

        try (var reference = new Reference()) {
            var source = dataset.generate(reference, scale);
            var rows = reference.count();
            System.out.printf("generated %,d %s rows at scale %s%n", rows, dataset.name(), scale);

            var topic = new Topic(kafka, dataset.topic());
            topic.create(PARTITIONS);
            mink.addTable(dataset.topic(), dataset.granularity());

            topic.write(source, encoding, rows);
            mink.awaitServed(dataset.topic(), rows);

            var table = reference.attach(mink) + "." + dataset.topic();
            if (!Check.run(reference, dataset.name(), dataset.queries(), table)) {
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
