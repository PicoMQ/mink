package mink.datasets.kafka;

public record Row(String key, String value, long timestampMs) {}
