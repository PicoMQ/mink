package mink.athena;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.StaticTableOperations;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.util.TableScanUtil;

final class Pages {
    private static final int LOOKBACK = 10;
    private static final long OPEN_FILE_BYTES = 4L << 20;

    private final long splitBytes;
    private final int splitsPerPage;

    record Page(Table table, List<CombinedScanTask> tasks, String next) {}

    private record Token(String metadata, String io, int position) {
        static Token first(Table table) {
            var metadata = ((HasTableOperations) table).operations().current().metadataFileLocation();

            return new Token(metadata, Splits.io(table.io()), 0);
        }

        static Token parse(String token) {
            var parts = token.split("\n", 3);

            return new Token(parts[0], parts[1], Integer.parseInt(parts[2]));
        }

        Table table() {
            return new BaseTable(new StaticTableOperations(metadata, Splits.io(io)), metadata);
        }

        @Override
        public String toString() {
            return metadata + "\n" + io + "\n" + position;
        }
    }

    Pages(long splitBytes, int splitsPerPage) {
        this.splitBytes = splitBytes;
        this.splitsPerPage = splitsPerPage;
    }

    Page first(Table table, Constraints constraints) {
        return page(Token.first(table), constraints);
    }

    Page next(String token, Constraints constraints) {
        return page(Token.parse(token), constraints);
    }

    private Page page(Token token, Constraints constraints) {
        var table = token.table();
        if (table.currentSnapshot() == null) {
            return new Page(table, List.of(), null);
        }

        var all = plan(table, constraints);
        var end = Math.min(all.size(), token.position() + splitsPerPage);
        var next = end < all.size() ? new Token(token.metadata(), token.io(), end).toString() : null;

        return new Page(table, all.subList(token.position(), end), next);
    }

    private List<CombinedScanTask> plan(Table table, Constraints constraints) {
        var files = new ArrayList<FileScanTask>();
        var scan = table.newScan().filter(Filters.expression(table.schema(), constraints));

        try (var planned = scan.planFiles()) {
            planned.forEach(files::add);
        } catch (IOException error) {
            throw new UncheckedIOException(error);
        }

        files.sort(Comparator.comparing((FileScanTask task) -> task.file().location())
                .thenComparing(FileScanTask::start));
        var split = TableScanUtil.splitFiles(CloseableIterable.withNoopClose(files), splitBytes);
        var tasks = new ArrayList<CombinedScanTask>();
        TableScanUtil.planTasks(split, splitBytes, LOOKBACK, OPEN_FILE_BYTES).forEach(tasks::add);

        return tasks;
    }
}
