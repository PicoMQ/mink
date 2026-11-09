package mink.athena;

import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.ScanTaskParser;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.util.JsonUtil;

final class Splits {
    static final String TASKS = "tasks";
    static final String IO = "io";

    private static final String S3_FILE_IO = "org.apache.iceberg.aws.s3.S3FileIO";

    private Splits() {}

    static String tasks(CombinedScanTask combined) {
        var tasks = new ArrayList<String>();
        combined.files().forEach(task -> tasks.add(ScanTaskParser.toJson(task)));

        return json(tasks);
    }

    static List<FileScanTask> tasks(String json) {
        var tasks = new ArrayList<FileScanTask>();
        for (var task : parse(json, new TypeReference<List<String>>() {})) {
            tasks.add((FileScanTask) ScanTaskParser.fromJson(task, false));
        }

        return tasks;
    }

    static String io(FileIO io) {
        return json(new HashMap<>(io.properties()));
    }

    static FileIO io(String json) {
        return CatalogUtil.loadFileIO(S3_FILE_IO, parse(json, new TypeReference<Map<String, String>>() {}), null);
    }

    private static String json(Object value) {
        try {
            return JsonUtil.mapper().writeValueAsString(value);
        } catch (JsonProcessingException error) {
            throw new UncheckedIOException(error);
        }
    }

    private static <T> T parse(String json, TypeReference<T> type) {
        try {
            return JsonUtil.mapper().readValue(json, type);
        } catch (JsonProcessingException error) {
            throw new UncheckedIOException(error);
        }
    }
}
