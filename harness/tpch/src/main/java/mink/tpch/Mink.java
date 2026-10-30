package mink.tpch;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

record Mink(URI address, String warehouse, String namespace) {
    private static final HttpClient HTTP = HttpClient.newHttpClient();
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final Duration SERVED_WITHIN = Duration.ofMinutes(30);
    private static final Duration POLL = Duration.ofSeconds(10);

    static Mink at(URI address) throws IOException, InterruptedException {
        var cluster = get(address.resolve("/admin/cluster"));

        return new Mink(address, cluster.path("warehouse").asText(), cluster.path("namespace").asText());
    }

    void addTable(String topic) throws IOException, InterruptedException {
        var body = JSON.createObjectNode().put("topic", topic).toString();
        var request = HttpRequest.newBuilder(address.resolve("/admin/tables"))
                .header("content-type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();

        var response = HTTP.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() != 201) {
            throw new IOException("adding table " + topic + " answered " + response.statusCode() + ": " + response.body());
        }
    }

    void awaitServed(String table, long rows) throws IOException, InterruptedException {
        var deadline = Instant.now().plus(SERVED_WITHIN);

        while (Instant.now().isBefore(deadline)) {
            var served = served(table);
            System.out.printf("served %,d of %,d%n", served, rows);
            if (served >= rows) {
                return;
            }
            Thread.sleep(POLL);
        }

        throw new IOException(table + " was not served in full within " + SERVED_WITHIN);
    }

    private long served(String table) throws IOException, InterruptedException {
        var uri = address.resolve("/catalog/v1/namespaces/%s/tables/%s".formatted(namespace, table));
        var response = HTTP.send(HttpRequest.newBuilder(uri).build(), HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() == 404) {
            return 0;
        }

        var metadata = JSON.readTree(response.body()).path("metadata");
        var current = metadata.path("current-snapshot-id").asLong();
        for (JsonNode snapshot : metadata.path("snapshots")) {
            if (snapshot.path("snapshot-id").asLong() == current) {
                return snapshot.path("summary").path("total-records").asLong();
            }
        }

        return 0;
    }

    private static JsonNode get(URI uri) throws IOException, InterruptedException {
        var response = HTTP.send(HttpRequest.newBuilder(uri).build(), HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() != 200) {
            throw new IOException(uri + " answered " + response.statusCode());
        }

        return JSON.readTree(response.body());
    }
}
