package mink.athena;

import java.nio.charset.StandardCharsets;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.data.BlockUtils;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.OrderByField;
import com.amazonaws.athena.connector.lambda.domain.predicate.Range;
import com.amazonaws.athena.connector.lambda.domain.predicate.SortedRangeSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesRequest;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsResponse;
import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@EnabledIfEnvironmentVariable(named = "TEST_MINK_CATALOG", matches = ".+")
class Reads {
    private static final String CATALOG = "mink";
    private static final TableName TABLE = new TableName("merged", System.getenv().getOrDefault("TEST_MINK_TABLE", "athena_events"));
    private static final FederatedIdentity IDENTITY = new FederatedIdentity("arn", "account", Map.of(), List.of(), Map.of());
    private static final Constraints NONE = constraints(Map.of(), List.of(), Constraints.DEFAULT_NO_LIMIT);

    private static BlockAllocator allocator;
    private static Metadata metadata;
    private static Records records;
    private static Schema schema;

    private record Row(int partition, long offset, String value) {}

    @BeforeAll
    static void connect() throws Exception {
        var config = Map.of(
                "catalog_uri", System.getenv("TEST_MINK_CATALOG"),
                "spill_bucket", "spill",
                "spill_prefix", "athena",
                "splits_per_page", "1");
        allocator = new BlockAllocatorImpl();
        metadata = new Metadata(config);
        records = new Records(config);
        schema = metadata.doGetTable(allocator, new GetTableRequest(IDENTITY, query(), CATALOG, TABLE, Map.of())).getSchema();
    }

    @AfterAll
    static void close() {
        allocator.close();
    }

    @Test
    void theTableIsListedUnderItsNamespace() {
        var tables = metadata.doListTables(allocator,
                new ListTablesRequest(IDENTITY, query(), CATALOG, TABLE.getSchemaName(), null, ListTablesRequest.UNLIMITED_PAGE_SIZE_VALUE));

        assertTrue(tables.getTables().contains(TABLE), tables.getTables().toString());
        assertEquals(new ArrowType.Timestamp(TimeUnit.MILLISECOND, "UTC"), schema.findField("_kafka_timestamp").getType());
    }

    @Test
    void everyRecordReadsOnceAcrossTheExternalTableAndKafka() throws Exception {
        var rows = read(NONE);
        var keys = new HashSet<String>();
        rows.forEach(row -> keys.add(row.partition() + ":" + row.offset()));

        assertEquals(440, rows.size());
        assertEquals(440, keys.size());
        assertEquals(400, rows.stream().filter(row -> row.value().contains("old")).count());
    }

    @Test
    void aTimestampFilterReadsOnlyTheRecentRecords() throws Exception {
        var since = ZonedDateTime.now(ZoneId.of("UTC")).minusDays(1);
        var recent = SortedRangeSet.of(false, Range.greaterThan(allocator, new ArrowType.Timestamp(TimeUnit.MILLISECOND, "UTC"), since));

        var rows = read(constraints(Map.of("_kafka_timestamp", recent), List.of(), Constraints.DEFAULT_NO_LIMIT));

        assertEquals(40, rows.size());
        assertTrue(rows.stream().allMatch(row -> row.value().contains("new")));
    }

    @Test
    void aLimitStopsEachSplitEarly() throws Exception {
        var rows = read(constraints(Map.of(), List.of(), 5));

        assertTrue(rows.size() <= 5 * splits(NONE).size(), String.valueOf(rows.size()));
    }

    @Test
    void topRowsComeBackLatestFirst() throws Exception {
        var latest = new OrderByField("_kafka_offset", OrderByField.Direction.DESC_NULLS_LAST);

        var rows = read(constraints(Map.of(), List.of(latest), 3));

        assertTrue(rows.stream().anyMatch(row -> row.offset() == 219), rows.toString());
    }

    private static List<Row> read(Constraints constraints) throws Exception {
        var rows = new ArrayList<Row>();
        for (var split : splits(constraints)) {
            var request = new ReadRecordsRequest(IDENTITY, CATALOG, query(), TABLE, schema, split, constraints,
                    100_000_000_000L, 100_000_000_000L);
            var response = (ReadRecordsResponse) records.doReadRecords(allocator, request);
            rows.addAll(rows(response.getRecords()));
        }

        return rows;
    }

    private static List<Split> splits(Constraints constraints) {
        var splits = new ArrayList<Split>();
        var partitions = BlockUtils.newBlock(allocator, "partitionId", Types.MinorType.INT.getType(), 1);
        String token = null;
        do {
            var response = metadata.doGetSplits(allocator,
                    new GetSplitsRequest(IDENTITY, query(), CATALOG, TABLE, partitions, List.of(), constraints, token));
            splits.addAll(response.getSplits());
            token = response.getContinuationToken();
        } while (token != null);

        return splits;
    }

    private static List<Row> rows(Block block) {
        var rows = new ArrayList<Row>();
        var partition = block.getFieldReader("_kafka_partition");
        var offset = block.getFieldReader("_kafka_offset");
        var value = block.getFieldReader("_kafka_value");
        for (var position = 0; position < block.getRowCount(); position++) {
            partition.setPosition(position);
            offset.setPosition(position);
            value.setPosition(position);
            rows.add(new Row(partition.readInteger(), offset.readLong(),
                    new String(value.readByteArray(), StandardCharsets.UTF_8)));
        }

        return rows;
    }

    private static Constraints constraints(Map<String, ValueSet> summary, List<OrderByField> order, long limit) {
        return new Constraints(summary, List.of(), order, limit, Map.of(), null);
    }

    private static String query() {
        return UUID.randomUUID().toString();
    }
}
