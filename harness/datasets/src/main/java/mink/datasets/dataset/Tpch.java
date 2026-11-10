package mink.datasets.dataset;

import java.sql.SQLException;
import java.util.List;
import java.util.Optional;

import mink.datasets.kafka.Schema;
import mink.datasets.kafka.Topic;
import mink.datasets.reference.Reference;

record Tpch() implements Dataset {
    private static final String SCHEMA = """
            {
              "type": "object",
              "properties": {
                "l_orderkey": {"type": "integer"},
                "l_partkey": {"type": "integer"},
                "l_suppkey": {"type": "integer"},
                "l_linenumber": {"type": "integer"},
                "l_quantity": {"type": "number"},
                "l_extendedprice": {"type": "number"},
                "l_discount": {"type": "number"},
                "l_tax": {"type": "number"},
                "l_returnflag": {"type": "string"},
                "l_linestatus": {"type": "string"},
                "l_shipdate": {"type": "string", "format": "date"},
                "l_commitdate": {"type": "string", "format": "date"},
                "l_receiptdate": {"type": "string", "format": "date"},
                "l_shipinstruct": {"type": "string"},
                "l_shipmode": {"type": "string"},
                "l_comment": {"type": "string"}
              },
              "required": [
                "l_orderkey", "l_partkey", "l_suppkey", "l_linenumber", "l_quantity", "l_extendedprice",
                "l_discount", "l_tax", "l_returnflag", "l_linestatus", "l_shipdate", "l_commitdate",
                "l_receiptdate", "l_shipinstruct", "l_shipmode", "l_comment"
              ],
              "additionalProperties": false
            }
            """;

    @Override
    public String name() {
        return "tpch";
    }

    @Override
    public String topic() {
        return "lineitem";
    }

    @Override
    public String granularity() {
        return "month";
    }

    @Override
    public Optional<Schema> schema() {
        return Optional.of(Schema.Json.of(SCHEMA));
    }

    @Override
    public List<String> queries() {
        return List.of("q1", "q6");
    }

    @Override
    public double defaultScale() {
        return 0.5;
    }

    @Override
    public Topic.Source generate(Reference reference, double scale) throws SQLException {
        reference.execute("INSTALL tpch");
        reference.execute("LOAD tpch");
        reference.execute("CALL dbgen(sf = %s)".formatted(scale));
        reference.execute("""
                CREATE VIEW %s AS SELECT * REPLACE (
                    CAST(l_quantity AS DOUBLE) AS l_quantity,
                    CAST(l_extendedprice AS DOUBLE) AS l_extendedprice,
                    CAST(l_discount AS DOUBLE) AS l_discount,
                    CAST(l_tax AS DOUBLE) AS l_tax
                ) FROM lineitem
                """.formatted(Reference.TABLE));

        return reference.rows("""
                SELECT l_orderkey || '-' || l_linenumber, to_json(%1$s)::VARCHAR, epoch_ms(l_shipdate)
                FROM %1$s ORDER BY l_shipdate
                """.formatted(Reference.TABLE));
    }
}
