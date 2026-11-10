package mink.athena;

import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.Map;

import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockSpiller;
import com.amazonaws.athena.connector.lambda.data.FieldResolver;
import com.amazonaws.athena.connector.lambda.domain.predicate.ConstraintEvaluator;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.handlers.RecordHandler;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericDeleteFilter;
import org.apache.iceberg.data.IdentityPartitionConverters;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.util.PartitionUtil;

public final class Records extends RecordHandler {
    public Records(Map<String, String> config) {
        super(Metadata.SOURCE_TYPE, config);
    }

    @Override
    protected void readWithConstraint(BlockSpiller spiller, ReadRecordsRequest request, QueryStatusChecker checker)
            throws IOException {
        var split = request.getSplit();
        var constraints = request.getConstraints();
        var evaluator = spiller.getConstraintEvaluator();
        var tasks = Splits.tasks(split.getProperty(Splits.TASKS));
        var projected = projection(tasks.getFirst().schema(), request, constraints);

        var ranking = constraints.hasNonEmptyOrderByClause() && constraints.hasLimit()
                ? new Ranking(constraints.getOrderByClause(), constraints.getLimit())
                : null;
        var written = 0L;

        try (FileIO io = Splits.io(split.getProperty(Splits.IO))) {
            for (var task : tasks) {
                try (var records = read(io, task, projected)) {
                    for (var record : records) {
                        if (!checker.isQueryRunning()) {
                            return;
                        }

                        var row = Columns.row(record, projected);
                        if (!matches(evaluator, constraints, row)) {
                            continue;
                        }

                        if (ranking != null) {
                            ranking.offer(row);
                            continue;
                        }

                        write(spiller, projected, row);
                        if (constraints.hasLimit() && ++written >= constraints.getLimit()) {
                            return;
                        }
                    }
                }
            }
        }

        if (ranking != null) {
            ranking.rows().forEach(row -> write(spiller, projected, row));
        }
    }

    private static Schema projection(Schema schema, ReadRecordsRequest request, Constraints constraints) {
        var names = new LinkedHashSet<String>();
        request.getSchema().getFields().forEach(field -> names.add(field.getName()));
        names.addAll(constraints.getSummary().keySet());
        constraints.getOrderByClause().forEach(field -> names.add(Columns.name(field.getColumnName())));

        var columns = names.stream().map(name -> Columns.find(schema, name).name()).toList();

        return schema.select(columns);
    }

    private static CloseableIterable<Record> read(FileIO io, FileScanTask task, Schema projected) {
        var deletes = new GenericDeleteFilter(io, task, task.schema(), projected);
        var required = deletes.requiredSchema();
        var constants = PartitionUtil.constantsMap(task, IdentityPartitionConverters::convertConstant);

        CloseableIterable<Record> records = Parquet.read(io.newInputFile(task.file().location()))
                .project(required)
                .split(task.start(), task.length())
                .filter(task.residual())
                .createReaderFunc(fileSchema -> GenericParquetReaders.buildReader(required, fileSchema, constants))
                .build();

        return deletes.filter(records);
    }

    private static boolean matches(ConstraintEvaluator evaluator, Constraints constraints, Map<String, Object> row) {
        for (var name : constraints.getSummary().keySet()) {
            if (!evaluator.apply(name, row.get(name))) {
                return false;
            }
        }

        return true;
    }

    private static void write(BlockSpiller spiller, Schema projected, Map<String, Object> row) {
        spiller.writeRows((Block block, int position) -> {
            for (var column : projected.columns()) {
                var name = Columns.name(column.name());
                var value = row.get(name);
                if (value == null) {
                    continue;
                }

                if (column.type().isNestedType()) {
                    block.offerComplexValue(name, position, FieldResolver.DEFAULT, value);
                } else {
                    block.offerValue(name, position, value);
                }
            }

            return 1;
        });
    }
}
