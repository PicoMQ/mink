# Engines

Any engine with an Iceberg REST catalog connects to Mink with two addresses: the catalog and the S3 endpoint. No plugin and no client library. The console's Connect page fills in the snippets below for a running cluster.

| Engine | Version | Catalog |
| --- | --- | --- |
| DuckDB | 1.4 or later | `ATTACH ... (TYPE iceberg)` |
| PyIceberg | 0.12 | `RestCatalog` |
| Spark | 4.1, with Iceberg 1.12 | `SparkCatalog` of type `rest` |
| Trino | 484 | `iceberg` connector, catalog type `rest` |
| EMR Serverless | 7.14.0, with Spark 3.5.8 and Iceberg 1.10.1 | `SparkCatalog` of type `rest`, see [AWS](/docs/operations/aws#query-from-emr-serverless) |

Mink's S3 endpoint accepts any access key and secret. Most S3 clients require one, so the snippets set both to `mink`.

The addresses below are a node's defaults. Set `catalog_endpoint` and `s3_endpoint` to the addresses engines reach.

## DuckDB

```sql
INSTALL iceberg; LOAD iceberg;
INSTALL httpfs; LOAD httpfs;

CREATE SECRET mink (TYPE S3, KEY_ID 'mink', SECRET 'mink', REGION 'us-east-1',
  ENDPOINT '127.0.0.1:9100', URL_STYLE 'path', USE_SSL false);
ATTACH 'warehouse' AS mink (TYPE iceberg, ENDPOINT 'http://127.0.0.1:8181', AUTHORIZATION_TYPE 'none');

SELECT count(*) FROM mink.kafka."orders";
```

## PyIceberg

```python
from pyiceberg.catalog.rest import RestCatalog

catalog = RestCatalog('mink', uri='http://127.0.0.1:8181',
                      **{'s3.access-key-id': 'mink', 's3.secret-access-key': 'mink'})
table = catalog.load_table('kafka.orders')
print(table.scan().to_arrow().num_rows)
```

## Spark

```bash
spark-sql \
  --packages org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.12.0,org.apache.iceberg:iceberg-aws-bundle:1.12.0 \
  --conf spark.sql.catalog.mink=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.mink.type=rest \
  --conf spark.sql.catalog.mink.uri=http://127.0.0.1:8181 \
  --conf spark.sql.catalog.mink.io-impl=org.apache.iceberg.aws.s3.S3FileIO \
  --conf spark.sql.catalog.mink.s3.endpoint=http://127.0.0.1:9100 \
  --conf spark.sql.catalog.mink.s3.path-style-access=true \
  --conf spark.sql.catalog.mink.s3.access-key-id=mink \
  --conf spark.sql.catalog.mink.s3.secret-access-key=mink \
  -e 'SELECT count(*) FROM mink.kafka.`orders`'
```

The same settings work for a Spark Connect session, set with `spark.conf.set` before the first query.

## Trino

`etc/catalog/mink.properties`:

```properties
connector.name=iceberg
iceberg.catalog.type=rest
iceberg.rest-catalog.uri=http://127.0.0.1:8181
fs.native-s3.enabled=true
s3.endpoint=http://127.0.0.1:9100
s3.path-style-access=true
s3.region=us-east-1
s3.aws-access-key=mink
s3.aws-secret-key=mink
```

```sql
SELECT count(*) FROM mink.kafka."orders";
```

## Table names

A table has its topic's name. Names with `-` or `.` need quoting.

| Engine | Quoted |
| --- | --- |
| DuckDB, Trino | `mink.kafka."demo-events"` |
| Spark | ``mink.kafka.`demo-events` `` |
| PyIceberg | `catalog.load_table('kafka.demo-events')` |
