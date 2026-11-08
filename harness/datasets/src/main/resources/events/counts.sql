SELECT count(*), count(DISTINCT _kafka_value), min(_kafka_timestamp), max(_kafka_timestamp)
FROM %s
