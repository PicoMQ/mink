package mink.athena;

import java.util.List;
import java.util.Map;

import com.amazonaws.athena.connector.lambda.domain.TableName;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.rest.RESTCatalog;

final class Tables {
    private final RESTCatalog catalog;

    Tables(String uri) {
        catalog = new RESTCatalog();
        catalog.initialize(Metadata.SOURCE_TYPE, Map.of(CatalogProperties.URI, uri));
    }

    List<String> schemas() {
        return catalog.listNamespaces().stream()
                .map(namespace -> Columns.name(namespace.toString()))
                .toList();
    }

    List<TableName> tables(String schema) {
        return catalog.listTables(namespace(schema)).stream()
                .map(table -> new TableName(schema, Columns.name(table.name())))
                .sorted((left, right) -> left.getTableName().compareTo(right.getTableName()))
                .toList();
    }

    Table load(TableName name) {
        var namespace = namespace(name.getSchemaName());
        var matches = catalog.listTables(namespace).stream()
                .filter(table -> Columns.name(table.name()).equals(name.getTableName()))
                .toList();

        if (matches.isEmpty()) {
            throw new NoSuchTableException("Table does not exist: %s", name);
        }
        if (matches.size() > 1) {
            throw new IllegalArgumentException("tables " + matches + " differ only by case");
        }

        return catalog.loadTable(TableIdentifier.of(namespace, matches.getFirst().name()));
    }

    private Namespace namespace(String schema) {
        var matches = catalog.listNamespaces().stream()
                .filter(namespace -> Columns.name(namespace.toString()).equals(schema))
                .toList();

        if (matches.isEmpty()) {
            throw new IllegalArgumentException("no namespace " + schema);
        }
        if (matches.size() > 1) {
            throw new IllegalArgumentException("namespaces " + matches + " differ only by case");
        }

        return matches.getFirst();
    }
}
