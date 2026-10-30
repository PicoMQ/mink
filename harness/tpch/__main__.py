import datetime
import json
import math
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

import duckdb
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer
from confluent_kafka.serialization import MessageField, SerializationContext

TOPIC = "lineitem"
PARTITIONS = 6
BATCH = 10_000
SERVED_WITHIN = datetime.timedelta(minutes=30)
TOLERANCE = 1e-9

INTEGER = {"type": "integer"}
NUMBER = {"type": "number"}
STRING = {"type": "string"}
DATE = {"type": "string", "format": "date"}

COLUMNS = {
    "l_orderkey": INTEGER,
    "l_partkey": INTEGER,
    "l_suppkey": INTEGER,
    "l_linenumber": INTEGER,
    "l_quantity": NUMBER,
    "l_extendedprice": NUMBER,
    "l_discount": NUMBER,
    "l_tax": NUMBER,
    "l_returnflag": STRING,
    "l_linestatus": STRING,
    "l_shipdate": DATE,
    "l_commitdate": DATE,
    "l_receiptdate": DATE,
    "l_shipinstruct": STRING,
    "l_shipmode": STRING,
    "l_comment": STRING,
}

SCHEMA = {
    "type": "object",
    "properties": COLUMNS,
    "required": list(COLUMNS),
    "additionalProperties": False,
}

QUERIES = {
    "Q1": """
        SELECT l_returnflag, l_linestatus,
            sum(l_quantity), sum(l_extendedprice),
            sum(l_extendedprice * (1 - l_discount)),
            sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)),
            avg(l_quantity), avg(l_extendedprice), avg(l_discount),
            count(*)
        FROM {lineitem}
        WHERE l_shipdate <= DATE '1998-12-01' - INTERVAL 90 DAY
        GROUP BY l_returnflag, l_linestatus
        ORDER BY l_returnflag, l_linestatus
    """,
    "Q6": """
        SELECT sum(l_extendedprice * l_discount)
        FROM {lineitem}
        WHERE l_shipdate >= DATE '1994-01-01' AND l_shipdate < DATE '1995-01-01'
            AND l_discount BETWEEN 0.05 AND 0.07 AND l_quantity < 24
    """,
}


def call(method, url, body=None):
    data = None if body is None else json.dumps(body).encode()
    request = urllib.request.Request(url, data=data, method=method, headers={"Content-Type": "application/json"})

    with urllib.request.urlopen(request) as response:
        return json.loads(response.read())


def kafka_config(path):
    with open(path) as lines:
        pairs = [line.strip().split("=", 1) for line in lines if line.strip() and not line.startswith("#")]

    return {key.strip(): value.strip() for key, value in pairs}


def generate(db, scale):
    db.execute("INSTALL tpch")
    db.execute("LOAD tpch")
    db.execute(f"CALL dbgen(sf = {scale})")

    columns = [
        f"CAST({name} AS DOUBLE) AS {name}" if column is NUMBER else name for name, column in COLUMNS.items()
    ]
    db.execute(f"CREATE VIEW reference AS SELECT {', '.join(columns)} FROM lineitem")

    return db.execute("SELECT count(*) FROM reference").fetchone()[0]


def create_topic(config):
    admin = AdminClient(config)
    admin.create_topics([NewTopic(TOPIC, PARTITIONS)])[TOPIC].result()


def produce(config, registry, db, rows):
    failed = []
    producer = Producer({**config, "linger.ms": 100})
    serializer = JSONSerializer(json.dumps(SCHEMA), SchemaRegistryClient({"url": registry}), conf={"validate": False})
    context = SerializationContext(TOPIC, MessageField.VALUE)

    def delivered(error, _):
        if error is not None:
            failed.append(error)

    cursor = db.execute("SELECT * FROM reference")
    names = [column[0] for column in cursor.description]
    produced = 0

    while batch := cursor.fetchmany(BATCH):
        for row in batch:
            value = {name: field.isoformat() if isinstance(field, datetime.date) else field for name, field in zip(names, row)}
            key = f"{value['l_orderkey']}-{value['l_linenumber']}".encode()
            send(producer, key, serializer(value, context), delivered)

        produced += len(batch)
        print(f"produced {produced:,} of {rows:,}", flush=True)

    producer.flush()
    if failed:
        sys.exit(f"{len(failed)} records were not delivered, the first: {failed[0]}")


def send(producer, key, value, delivered):
    while True:
        try:
            producer.produce(TOPIC, key=key, value=value, on_delivery=delivered)
            producer.poll(0)
            return
        except BufferError:
            producer.poll(1)


def add_table(mink, namespace, rows):
    call("POST", f"{mink}/admin/tables", {"topic": TOPIC})
    deadline = datetime.datetime.now() + SERVED_WITHIN

    while datetime.datetime.now() < deadline:
        served = served_records(mink, namespace)
        print(f"served {served:,} of {rows:,}", flush=True)
        if served >= rows:
            return
        time.sleep(10)

    sys.exit(f"{TOPIC} was not served in full within {SERVED_WITHIN}")


def served_records(mink, namespace):
    try:
        metadata = call("GET", f"{mink}/catalog/v1/namespaces/{namespace}/tables/{TOPIC}")["metadata"]
    except urllib.error.HTTPError as error:
        if error.code == 404:
            return 0
        raise

    current = next(snapshot for snapshot in metadata["snapshots"] if snapshot["snapshot-id"] == metadata["current-snapshot-id"])
    return int(current["summary"]["total-records"])


def attach(db, mink, cluster):
    address = urllib.parse.urlsplit(mink)

    for statement in [
        "INSTALL iceberg",
        "LOAD iceberg",
        "INSTALL httpfs",
        "LOAD httpfs",
        f"CREATE SECRET mink (TYPE S3, KEY_ID 'mink', SECRET 'mink', ENDPOINT '{address.netloc}/s3', "
        f"URL_STYLE 'path', USE_SSL {address.scheme == 'https'})",
        f"ATTACH '{cluster['warehouse']}' AS mink (TYPE iceberg, ENDPOINT '{mink}/catalog', "
        "AUTHORIZATION_TYPE 'none', ACCESS_DELEGATION_MODE 'none')",
    ]:
        db.execute(statement)

    return f"mink.{cluster['namespace']}.{TOPIC}"


def same(expected, actual):
    if isinstance(expected, float):
        return math.isclose(expected, actual, rel_tol=TOLERANCE)
    return expected == actual


def check(db, table):
    matched = True

    for name, query in QUERIES.items():
        expected = db.execute(query.format(lineitem="reference")).fetchall()
        actual = db.execute(query.format(lineitem=table)).fetchall()
        rows_match = len(expected) == len(actual) and all(
            len(left) == len(right) and all(same(e, a) for e, a in zip(left, right))
            for left, right in zip(expected, actual)
        )

        print(f"{name}: {'match' if rows_match else 'MISMATCH'}")
        for row in actual:
            print(f"  {row}")
        if not rows_match:
            print(f"  expected {expected}")
        matched = matched and rows_match

    return matched


def main():
    config_path, registry, mink = sys.argv[1:4]
    scale = float(sys.argv[4]) if len(sys.argv) > 4 else 0.5
    mink = mink.rstrip("/")
    config = kafka_config(config_path)
    db = duckdb.connect()

    cluster = call("GET", f"{mink}/admin/cluster")

    rows = generate(db, scale)
    print(f"generated {rows:,} lineitem rows at scale {scale}", flush=True)
    create_topic(config)
    produce(config, registry, db, rows)
    add_table(mink, cluster["namespace"], rows)

    if not check(db, attach(db, mink, cluster)):
        sys.exit(1)


main()
