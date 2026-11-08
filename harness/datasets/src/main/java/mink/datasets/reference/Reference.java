package mink.datasets.reference;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.function.Consumer;

import mink.datasets.Mink;
import mink.datasets.kafka.Row;
import mink.datasets.kafka.Topic;

public final class Reference implements AutoCloseable {
    public static final String TABLE = "reference";

    private final Connection duckdb;

    public Reference() throws SQLException {
        var settings = new Properties();
        settings.setProperty("jdbc_stream_results", "true");
        settings.setProperty("memory_limit", "512MB");
        settings.setProperty("temp_directory", Path.of(System.getProperty("java.io.tmpdir"), "duckdb").toString());

        duckdb = DriverManager.getConnection("jdbc:duckdb:", settings);
        execute("INSTALL icu");
        execute("LOAD icu");
        execute("SET TimeZone = 'UTC'");
    }

    public Topic.Source rows(String sql) {
        return sink -> rows(sql, sink);
    }

    private void rows(String sql, Consumer<Row> sink) throws SQLException {
        try (var statement = duckdb.createStatement(); var results = statement.executeQuery(sql)) {
            while (results.next()) {
                sink.accept(new Row(results.getString(1), results.getString(2), results.getLong(3)));
            }
        }
    }

    public String attach(Mink mink) throws SQLException {
        var s3 = mink.s3();

        execute("INSTALL iceberg");
        execute("LOAD iceberg");
        execute("INSTALL httpfs");
        execute("LOAD httpfs");
        execute("""
                CREATE SECRET mink (TYPE S3, KEY_ID 'mink', SECRET 'mink', ENDPOINT '%s%s',
                    URL_STYLE 'path', USE_SSL %s)
                """.formatted(s3.getAuthority(), s3.getPath(), s3.getScheme().equals("https")));
        execute("""
                ATTACH '%s' AS mink (TYPE iceberg, ENDPOINT '%s',
                    AUTHORIZATION_TYPE 'none', ACCESS_DELEGATION_MODE 'none')
                """.formatted(mink.warehouse(), mink.catalog()));

        return "mink." + mink.namespace();
    }

    public List<List<Object>> query(String sql) throws SQLException {
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

    public long count() throws SQLException {
        return ((Number) query("SELECT count(*) FROM " + TABLE).getFirst().getFirst()).longValue();
    }

    public void execute(String sql) throws SQLException {
        try (var statement = duckdb.createStatement()) {
            statement.execute(sql);
        }
    }

    @Override
    public void close() throws SQLException {
        duckdb.close();
    }
}
