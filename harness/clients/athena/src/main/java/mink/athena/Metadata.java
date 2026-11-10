package mink.athena;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockWriter;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.handlers.MetadataHandler;
import com.amazonaws.athena.connector.lambda.metadata.GetDataSourceCapabilitiesRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetDataSourceCapabilitiesResponse;
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsResponse;
import com.amazonaws.athena.connector.lambda.metadata.GetTableLayoutRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableResponse;
import com.amazonaws.athena.connector.lambda.metadata.ListSchemasRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListSchemasResponse;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesResponse;
import com.amazonaws.athena.connector.lambda.metadata.optimizations.DataSourceOptimizations;
import com.amazonaws.athena.connector.lambda.metadata.optimizations.OptimizationSubType;
import com.amazonaws.athena.connector.lambda.metadata.optimizations.pushdown.FilterPushdownSubType;
import com.amazonaws.athena.connector.lambda.metadata.optimizations.pushdown.LimitPushdownSubType;
import com.amazonaws.athena.connector.lambda.metadata.optimizations.pushdown.TopNPushdownSubType;

import static com.amazonaws.athena.connector.lambda.metadata.ListTablesRequest.UNLIMITED_PAGE_SIZE_VALUE;

public final class Metadata extends MetadataHandler {
    static final String SOURCE_TYPE = "mink";

    private static final String CATALOG_URI = "catalog_uri";
    private static final String SPLIT_BYTES = "split_bytes";
    private static final String SPLITS_PER_PAGE = "splits_per_page";

    private final Tables tables;
    private final Pages pages;

    public Metadata(Map<String, String> config) {
        super(SOURCE_TYPE, config);

        tables = new Tables(config.get(CATALOG_URI));
        pages = new Pages(
                Long.parseLong(config.getOrDefault(SPLIT_BYTES, String.valueOf(128L << 20))),
                Integer.parseInt(config.getOrDefault(SPLITS_PER_PAGE, "200")));
    }

    @Override
    public ListSchemasResponse doListSchemaNames(BlockAllocator allocator, ListSchemasRequest request) {
        return new ListSchemasResponse(request.getCatalogName(), new HashSet<>(tables.schemas()));
    }

    @Override
    public ListTablesResponse doListTables(BlockAllocator allocator, ListTablesRequest request) {
        var all = tables.tables(request.getSchemaName());
        if (request.getPageSize() == UNLIMITED_PAGE_SIZE_VALUE) {
            return new ListTablesResponse(request.getCatalogName(), all, null);
        }

        var start = request.getNextToken();
        var after = all.stream()
                .filter(table -> start == null || table.getTableName().compareTo(start) >= 0)
                .toList();
        var page = after.subList(0, Math.min(after.size(), request.getPageSize()));
        var next = after.size() > page.size() ? after.get(page.size()).getTableName() : null;

        return new ListTablesResponse(request.getCatalogName(), page, next);
    }

    @Override
    public GetTableResponse doGetTable(BlockAllocator allocator, GetTableRequest request) {
        var schema = Columns.schema(tables.load(request.getTableName()).schema());

        return new GetTableResponse(request.getCatalogName(), request.getTableName(), schema, Set.of());
    }

    @Override
    public void getPartitions(BlockWriter writer, GetTableLayoutRequest request, QueryStatusChecker checker) {}

    @Override
    public GetSplitsResponse doGetSplits(BlockAllocator allocator, GetSplitsRequest request) {
        var page = request.getContinuationToken() == null
                ? pages.first(tables.load(request.getTableName()), request.getConstraints())
                : pages.next(request.getContinuationToken(), request.getConstraints());
        var io = Splits.io(page.table().io());

        var splits = new HashSet<Split>();
        for (var task : page.tasks()) {
            splits.add(Split.newBuilder(makeSpillLocation(request), makeEncryptionKey())
                    .add(Splits.TASKS, Splits.tasks(task))
                    .add(Splits.IO, io)
                    .build());
        }

        return new GetSplitsResponse(request.getCatalogName(), splits, page.next());
    }

    @Override
    public GetDataSourceCapabilitiesResponse doGetDataSourceCapabilities(
            BlockAllocator allocator, GetDataSourceCapabilitiesRequest request) {
        var capabilities = new HashMap<String, List<OptimizationSubType>>();
        put(capabilities, DataSourceOptimizations.SUPPORTS_FILTER_PUSHDOWN.withSupportedSubTypes(
                FilterPushdownSubType.SORTED_RANGE_SET,
                FilterPushdownSubType.EQUATABLE_VALUE_SET,
                FilterPushdownSubType.NULLABLE_COMPARISON));
        put(capabilities, DataSourceOptimizations.SUPPORTS_LIMIT_PUSHDOWN.withSupportedSubTypes(
                LimitPushdownSubType.INTEGER_CONSTANT));
        put(capabilities, DataSourceOptimizations.SUPPORTS_TOP_N_PUSHDOWN.withSupportedSubTypes(
                TopNPushdownSubType.SUPPORTS_ORDER_BY));

        return new GetDataSourceCapabilitiesResponse(request.getCatalogName(), capabilities);
    }

    private static void put(Map<String, List<OptimizationSubType>> capabilities,
                            Map.Entry<String, List<OptimizationSubType>> capability) {
        capabilities.put(capability.getKey(), capability.getValue());
    }
}
