package mink.tpch;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.function.Consumer;

final class Dataset implements AutoCloseable {
    static final String TABLE = "lineitem";

    static final String SCHEMA = """
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

    record Row(String key, String json) {}

    private final Connection duckdb;

    Dataset() throws SQLException {
        var settings = new Properties();
        settings.setProperty("jdbc_stream_results", "true");

        duckdb = DriverManager.getConnection("jdbc:duckdb:", settings);
    }

    long generate(double scale) throws SQLException {
        execute("INSTALL tpch");
        execute("LOAD tpch");
        execute("CALL dbgen(sf = %s)".formatted(scale));
        execute("""
                CREATE VIEW reference AS SELECT * REPLACE (
                    CAST(l_quantity AS DOUBLE) AS l_quantity,
                    CAST(l_extendedprice AS DOUBLE) AS l_extendedprice,
                    CAST(l_discount AS DOUBLE) AS l_discount,
                    CAST(l_tax AS DOUBLE) AS l_tax
                ) FROM lineitem
                """);

        return ((Number) query("SELECT count(*) FROM reference").getFirst().getFirst()).longValue();
    }

    void rows(Consumer<Row> sink) throws SQLException {
        var sql = "SELECT l_orderkey || '-' || l_linenumber, to_json(reference)::VARCHAR FROM reference";

        try (var statement = duckdb.createStatement(); var results = statement.executeQuery(sql)) {
            while (results.next()) {
                sink.accept(new Row(results.getString(1), results.getString(2)));
            }
        }
    }

    String attach(Mink mink) throws SQLException {
        var address = mink.address();

        execute("INSTALL iceberg");
        execute("LOAD iceberg");
        execute("INSTALL httpfs");
        execute("LOAD httpfs");
        execute("""
                CREATE SECRET mink (TYPE S3, KEY_ID 'mink', SECRET 'mink', ENDPOINT '%s/s3',
                    URL_STYLE 'path', USE_SSL %s)
                """.formatted(address.getAuthority(), address.getScheme().equals("https")));
        execute("""
                ATTACH '%s' AS mink (TYPE iceberg, ENDPOINT '%s/catalog',
                    AUTHORIZATION_TYPE 'none', ACCESS_DELEGATION_MODE 'none')
                """.formatted(mink.warehouse(), address));

        return "mink.%s.%s".formatted(mink.namespace(), TABLE);
    }

    List<List<Object>> query(String sql) throws SQLException {
        var rows = new ArrayList<List<Object>>();

        try (var statement = duckdb.createStatement(); var results = statement.executeQuery(sql)) {
            var columns = results.getMetaData().getColumnCount();

            while (results.next()) {
                var row = new ArrayList<Object>(columns);
                for (var column = 1; column <= columns; column++) {
                    row.add(results.getObject(column));
                }
                rows.add(row);
            }
        }

        return rows;
    }

    private void execute(String sql) throws SQLException {
        try (var statement = duckdb.createStatement()) {
            statement.execute(sql);
        }
    }

    @Override
    public void close() throws SQLException {
        duckdb.close();
    }
}
