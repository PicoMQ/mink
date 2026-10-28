import datetime
import decimal
import json
import uuid

from confluent_kafka.schema_registry.avro import AvroSerializer

from .engine import DEEPER_THAN_MINK_MAPS, RAW, UTC, call, field, micros, serialize, serializer_conf, table


class Avro:
    name = "avro"

    def register(self, url, client, topic, version):
        body = {"schema": definition(version["schema"])}

        return call("POST", f"{url}/subjects/{version['subject']}/versions", body)["id"]

    def serialize(self, client, topic, part, version, spec):
        conf = serializer_conf(version["subject"], spec.get("header", False))
        serializer = AvroSerializer(client, definition(version["schema"]), conf=conf)

        return serialize(serializer, topic, part, spec["data"])


def definition(schema):
    return json.dumps(schema, separators=(",", ":"))


def days(day):
    return (day - datetime.date(1970, 1, 1)).days


TYPES = {
    "type": "record",
    "name": "Types",
    "fields": [
        {"name": "b", "type": "boolean"},
        {"name": "i", "type": "int"},
        {"name": "l", "type": "long"},
        {"name": "f", "type": "float"},
        {"name": "d", "type": "double"},
        {"name": "s", "type": "string"},
        {"name": "by", "type": "bytes"},
        {"name": "e", "type": {"type": "enum", "name": "Color", "symbols": ["RED", "GREEN"]}},
        {"name": "fx", "type": {"type": "fixed", "name": "Hash", "size": 4}},
        {"name": "opt", "type": ["null", "string"], "default": None},
        {"name": "arr", "type": {"type": "array", "items": "int"}},
        {"name": "m", "type": {"type": "map", "values": "long"}},
        {
            "name": "nested",
            "type": {
                "type": "record",
                "name": "Inner",
                "fields": [
                    {"name": "x", "type": "int"},
                    {"name": "y", "type": ["null", "string"], "default": None},
                ],
            },
        },
        {"name": "dec", "type": {"type": "bytes", "logicalType": "decimal", "precision": 9, "scale": 2}},
        {"name": "u", "type": {"type": "string", "logicalType": "uuid"}},
        {"name": "dt", "type": {"type": "int", "logicalType": "date"}},
        {"name": "tms", "type": {"type": "int", "logicalType": "time-millis"}},
        {"name": "tus", "type": {"type": "long", "logicalType": "time-micros"}},
        {"name": "tsms", "type": {"type": "long", "logicalType": "timestamp-millis"}},
        {"name": "tsus", "type": {"type": "long", "logicalType": "timestamp-micros"}},
        {"name": "lts", "type": {"type": "long", "logicalType": "local-timestamp-micros"}},
        {"name": "dur", "type": {"type": "fixed", "name": "Span", "size": 12, "logicalType": "duration"}},
        {"name": "multi", "type": ["null", "int", "string"], "default": None},
        {
            "name": "tree",
            "type": {
                "type": "record",
                "name": "Node",
                "fields": [
                    {"name": "value", "type": "int"},
                    {"name": "children", "type": {"type": "array", "items": "Node"}},
                ],
            },
        },
    ],
}

TYPES_TABLE = table(
    1,
    field(10, "b", "boolean"),
    field(11, "i", "int"),
    field(12, "l", "long"),
    field(13, "f", "float"),
    field(14, "d", "double"),
    field(15, "s", "string"),
    field(16, "by", "binary"),
    field(17, "e", "string"),
    field(18, "fx", "fixed[4]"),
    field(19, "opt", "string"),
    field(20, "arr", {"type": "list", "element-id": 34, "element-required": False, "element": "int"}),
    field(
        21,
        "m",
        {"type": "map", "key-id": 35, "key": "string", "value-id": 36, "value-required": False, "value": "long"},
    ),
    field(22, "nested", {"type": "struct", "fields": [field(37, "x", "int"), field(38, "y", "string")]}),
    field(23, "dec", "decimal(9, 2)"),
    field(24, "u", "uuid"),
    field(25, "dt", "date"),
    field(26, "tms", "time"),
    field(27, "tus", "time"),
    field(28, "tsms", "timestamptz"),
    field(29, "tsus", "timestamptz"),
    field(30, "lts", "timestamp"),
    field(31, "dur", "fixed[12]"),
    field(32, "multi", "string"),
    field(
        33,
        "tree",
        {
            "type": "struct",
            "fields": [
                field(39, "value", "int"),
                field(40, "children", {"type": "list", "element-id": 41, "element-required": False, "element": "string"}),
            ],
        },
    ),
)

TSMS = datetime.datetime(2026, 10, 28, 12, 0, 0, 123000, tzinfo=UTC)
TSUS = datetime.datetime(2026, 10, 28, 12, 0, 0, 123456, tzinfo=UTC)
LTS = datetime.datetime(2026, 10, 28, 8, 30, 0, 654321)
DUR = bytes([1, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0])


def types_record(opt, multi):
    return {
        "b": True,
        "i": -7,
        "l": 1234567890123,
        "f": 1.5,
        "d": -2.25,
        "s": "héllo",
        "by": b"\x00\xff",
        "e": "GREEN",
        "fx": b"\x01\x02\x03\x04",
        "opt": opt,
        "arr": [1, 2, 3],
        "m": {"a": 1, "b": 2},
        "nested": {"x": 5, "y": "why"},
        "dec": decimal.Decimal("1234.56"),
        "u": uuid.UUID("f81d4fae-7dec-11d0-a765-00a0c91e6bf6"),
        "dt": datetime.date(2026, 10, 28),
        "tms": datetime.time(12, 34, 56, 789000),
        "tus": datetime.time(1, 2, 3, 456789),
        "tsms": TSMS,
        "tsus": TSUS,
        "lts": LTS,
        "dur": DUR,
        "multi": multi,
        "tree": {"value": 1, "children": [{"value": 2, "children": []}]},
    }


def types_row(opt, multi):
    return {
        "_kafka_key": None,
        "_kafka_value": None,
        "b": True,
        "i": -7,
        "l": 1234567890123,
        "f": 1.5,
        "d": -2.25,
        "s": "héllo",
        "by": "00ff",
        "e": "GREEN",
        "fx": "01020304",
        "opt": opt,
        "arr": [1, 2, 3],
        "m": [["a", 1], ["b", 2]],
        "nested": {"x": 5, "y": "why"},
        "dec": "1234.56",
        "u": "f81d4fae-7dec-11d0-a765-00a0c91e6bf6",
        "dt": days(datetime.date(2026, 10, 28)),
        "tms": ((12 * 60 + 34) * 60 + 56) * 1_000_000 + 789000,
        "tus": ((1 * 60 + 2) * 60 + 3) * 1_000_000 + 456789,
        "tsms": micros(TSMS),
        "tsus": micros(TSUS),
        "lts": micros(LTS.replace(tzinfo=UTC)),
        "dur": DUR.hex(),
        "multi": multi,
        "tree": {"value": 1, "children": ['{"value":2,"children":[]}']},
    }


ORDER_1 = {
    "type": "record",
    "name": "Order",
    "fields": [
        {"name": "id", "type": "long"},
        {"name": "amount", "type": "float"},
        {"name": "status", "type": "string"},
        {"name": "legacy", "type": "int"},
    ],
}
ORDER_2 = {
    "type": "record",
    "name": "Order",
    "fields": [
        {"name": "id", "type": "long"},
        {"name": "amount", "type": "double"},
        {"name": "status", "type": "string"},
        {"name": "legacy", "type": "int"},
        {"name": "currency", "type": ["null", "string"], "default": None},
    ],
}
ORDER_3 = {
    "type": "record",
    "name": "Order",
    "fields": [
        {"name": "id", "type": "long"},
        {"name": "amount", "type": "double"},
        {"name": "state", "aliases": ["status"], "type": "string"},
        {"name": "legacy", "type": "string"},
    ],
}

ORDER_TABLES = [
    table(1, field(10, "id", "long"), field(11, "amount", "float"), field(12, "status", "string"), field(13, "legacy", "int")),
    table(
        2,
        field(10, "id", "long"),
        field(11, "amount", "double"),
        field(12, "status", "string"),
        field(13, "legacy", "int"),
        field(14, "currency", "string"),
    ),
    table(
        3,
        field(10, "id", "long"),
        field(11, "amount", "double"),
        field(12, "state", "string"),
        field(13, "legacy_s2", "int"),
        field(14, "currency", "string"),
        field(15, "legacy", "string"),
    ),
]

KEY = {"type": "record", "name": "OrderKey", "fields": [{"name": "customer", "type": "long"}]}
KEY_TABLES = [
    table(1, field(10, "_kafka_decoded_key", {"type": "struct", "fields": [field(11, "customer", "long")]})),
    table(
        2,
        field(10, "_kafka_decoded_key", {"type": "struct", "fields": [field(11, "customer", "long")]}),
        field(12, "value", "string"),
    ),
]

WIRE = {"type": "record", "name": "Wire", "fields": [{"name": "n", "type": "int"}]}
WIRE_TABLE = table(1, field(10, "n", "int"))
ELSEWHERE = {"type": "record", "name": "Elsewhere", "fields": [{"name": "n", "type": "int"}]}
RESERVED = {"type": "record", "name": "Reserved", "fields": [{"name": "_kafka_offset", "type": "long"}]}

def deep(level=0):
    child = "long" if level == DEEPER_THAN_MINK_MAPS else deep(level + 1)

    return {"type": "record", "name": f"Deep{level}", "fields": [{"name": "next", "type": ["null", child], "default": None}]}


CASES = [
    {
        "name": "types",
        "versions": [{"part": "value", "schema": TYPES, "table": TYPES_TABLE}],
        "records": [
            {"value": {"version": 0, "data": types_record(None, 42)}, "row": types_row(None, '{"int":42}')},
            {"value": {"version": 0, "data": types_record("set", None)}, "row": types_row("set", None)},
        ],
    },
    {
        "name": "evolution",
        "versions": [
            {"part": "value", "schema": ORDER_1, "table": ORDER_TABLES[0]},
            {"part": "value", "schema": ORDER_2, "table": ORDER_TABLES[1]},
            {"part": "value", "schema": ORDER_3, "table": ORDER_TABLES[2]},
        ],
        "records": [
            {
                "value": {"version": 0, "data": {"id": 1, "amount": 1.5, "status": "new", "legacy": 7}},
                "row": {"_kafka_key": None, "_kafka_value": None, "id": 1, "amount": 1.5, "state": "new", "legacy_s2": 7, "currency": None, "legacy": None},
            },
            {
                "value": {"version": 1, "data": {"id": 2, "amount": 2.25, "status": "paid", "legacy": 8, "currency": "EUR"}},
                "row": {"_kafka_key": None, "_kafka_value": None, "id": 2, "amount": 2.25, "state": "paid", "legacy_s2": 8, "currency": "EUR", "legacy": None},
            },
            {
                "value": {"version": 2, "data": {"id": 3, "amount": 3.5, "state": "sent", "legacy": "x"}},
                "row": {"_kafka_key": None, "_kafka_value": None, "id": 3, "amount": 3.5, "state": "sent", "legacy_s2": None, "currency": None, "legacy": "x"},
            },
        ],
    },
    {
        "name": "keys",
        "versions": [
            {"part": "key", "schema": KEY, "table": KEY_TABLES[0]},
            {"part": "value", "schema": "string", "table": KEY_TABLES[1]},
        ],
        "records": [
            {
                "key": {"version": 0, "data": {"customer": 9}},
                "value": {"version": 1, "data": "hello"},
                "row": {"_kafka_key": None, "_kafka_decoded_key": {"customer": 9}, "_kafka_value": None, "value": "hello"},
            },
            {
                "key": {"raw": "plain-key"},
                "value": {"version": 1, "data": "world"},
                "row": {"_kafka_key": RAW, "_kafka_decoded_key": None, "_kafka_value": None, "value": "world"},
            },
            {
                "key": {"version": 0, "data": {"customer": 10}},
                "value": None,
                "row": {"_kafka_key": None, "_kafka_decoded_key": {"customer": 10}, "_kafka_value": None, "value": None},
            },
        ],
    },
    {
        "name": "wire",
        "versions": [
            {"part": "value", "schema": WIRE, "table": WIRE_TABLE},
            {"part": "value", "schema": ELSEWHERE, "subject": "elsewhere", "rejection": "not_accepted"},
        ],
        "records": [
            {"value": {"version": 0, "data": {"n": 1}}, "row": {"_kafka_key": None, "_kafka_value": None, "n": 1}},
            {"value": {"version": 0, "data": {"n": 2}, "header": True}, "row": {"_kafka_key": None, "_kafka_value": None, "n": 2}},
            {"value": {"raw": "no magic byte"}, "row": {"_kafka_key": None, "_kafka_value": RAW, "n": None}},
            {"value": {"version": 0, "data": {"n": 300}, "truncate": 1}, "row": {"_kafka_key": None, "_kafka_value": RAW, "n": None}},
            {"value": {"missing_id": True}, "row": {"_kafka_key": None, "_kafka_value": RAW, "n": None}},
            {"value": {"missing_guid": True}, "row": {"_kafka_key": None, "_kafka_value": RAW, "n": None}},
            {"value": {"version": 1, "data": {"n": 4}}, "row": {"_kafka_key": None, "_kafka_value": RAW, "n": None}},
        ],
    },
    {
        "name": "deep",
        "versions": [{"part": "value", "schema": deep(), "rejection": "unsupported"}],
        "records": [{"value": {"version": 0, "data": {"next": None}}, "row": {"_kafka_key": None, "_kafka_value": RAW}}],
    },
    {
        "name": "reserved",
        "versions": [{"part": "value", "schema": RESERVED, "rejection": "reserved_name"}],
        "records": [{"value": {"version": 0, "data": {"_kafka_offset": 5}}, "row": {"_kafka_key": None, "_kafka_value": RAW}}],
    },
]


