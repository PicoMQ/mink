import datetime
import json

from confluent_kafka.schema_registry import Schema, SchemaReference
from confluent_kafka.schema_registry.json_schema import JSONSerializer

from .engine import DEEPER_THAN_MINK_MAPS, RAW, UTC, call, field, micros, serialize, serializer_conf, table


class JsonSchema:
    name = "json"

    def register(self, url, client, topic, version):
        body = {"schemaType": "JSON", "schema": definition(version), "references": references(version)}

        return call("POST", f"{url}/subjects/{version['subject']}/versions", body)["id"]

    def serialize(self, client, topic, part, version, spec):
        schema = Schema(
            definition(version),
            "JSON",
            [SchemaReference(item["name"], item["subject"], item["version"]) for item in references(version)],
        )
        conf = serializer_conf(version["subject"], spec.get("header", False))
        serializer = JSONSerializer(schema, client, conf=conf)

        return serialize(serializer, topic, part, spec["data"])


def definition(version):
    return json.dumps(version["schema"], separators=(",", ":")).replace("{run}", version["run"])


def references(version):
    return [
        {key: value.replace("{run}", version["run"]) if isinstance(value, str) else value for key, value in item.items()}
        for item in version.get("references", [])
    ]


TYPES = {
    "type": "object",
    "title": "Types",
    "definitions": {
        "Point": {"type": "object", "properties": {"x": {"type": "integer"}, "y": {"type": "integer"}}},
        "Node": {
            "type": "object",
            "properties": {
                "value": {"type": "integer"},
                "children": {"type": "array", "items": {"$ref": "#/definitions/Node"}},
            },
        },
    },
    "properties": {
        "b": {"type": "boolean"},
        "i": {"type": "integer"},
        "d": {"type": "number"},
        "s": {"type": "string"},
        "ts": {"type": "string", "format": "date-time"},
        "dt": {"type": "string", "format": "date"},
        "u": {"type": "string", "format": "uuid"},
        "e": {"enum": ["RED", "GREEN"]},
        "opt": {"type": ["string", "null"]},
        "arr": {"type": "array", "items": {"type": "integer"}},
        "m": {"type": "object", "additionalProperties": {"type": "integer"}},
        "nested": {"type": "object", "properties": {"x": {"type": "integer"}, "y": {"type": "string"}}},
        "choice": {"oneOf": [{"type": "integer"}, {"type": "string"}]},
        "pair": {"type": "array", "prefixItems": [{"type": "integer"}, {"type": "string"}]},
        "point": {"$ref": "#/definitions/Point"},
        "tree": {"$ref": "#/definitions/Node"},
    },
}

TYPES_TABLE = table(
    1,
    field(10, "b", "boolean"),
    field(11, "i", "long"),
    field(12, "d", "double"),
    field(13, "s", "string"),
    field(14, "ts", "timestamptz"),
    field(15, "dt", "date"),
    field(16, "u", "uuid"),
    field(17, "e", "string"),
    field(18, "opt", "string"),
    field(19, "arr", {"type": "list", "element-id": 26, "element-required": False, "element": "long"}),
    field(
        20,
        "m",
        {"type": "map", "key-id": 27, "key": "string", "value-id": 28, "value-required": False, "value": "long"},
    ),
    field(21, "nested", {"type": "struct", "fields": [field(29, "x", "long"), field(30, "y", "string")]}),
    field(22, "choice", "string"),
    field(23, "pair", "string"),
    field(24, "point", {"type": "struct", "fields": [field(31, "x", "long"), field(32, "y", "long")]}),
    field(
        25,
        "tree",
        {
            "type": "struct",
            "fields": [
                field(33, "value", "long"),
                field(34, "children", {"type": "list", "element-id": 35, "element-required": False, "element": "string"}),
            ],
        },
    ),
)

TS = datetime.datetime(2026, 10, 28, 12, 0, 0, 123456, tzinfo=UTC)

TYPES_RECORD = {
    "b": True,
    "i": -7,
    "d": 2.5,
    "s": "héllo",
    "ts": "2026-10-28T12:00:00.123456Z",
    "dt": "2026-10-28",
    "u": "f81d4fae-7dec-11d0-a765-00a0c91e6bf6",
    "e": "GREEN",
    "opt": None,
    "arr": [1, 2],
    "m": {"b": 2, "a": 1},
    "nested": {"x": 5, "y": "why"},
    "choice": 42,
    "pair": [1, "a"],
    "point": {"x": 1, "y": 2},
    "tree": {"value": 1, "children": [{"value": 2, "children": []}]},
}

TYPES_ROW = {
    "_kafka_key": None,
    "_kafka_value": None,
    "b": True,
    "i": -7,
    "d": 2.5,
    "s": "héllo",
    "ts": micros(TS),
    "dt": (datetime.date(2026, 10, 28) - datetime.date(1970, 1, 1)).days,
    "u": "f81d4fae-7dec-11d0-a765-00a0c91e6bf6",
    "e": "GREEN",
    "opt": None,
    "arr": [1, 2],
    "m": [["a", 1], ["b", 2]],
    "nested": {"x": 5, "y": "why"},
    "choice": "42",
    "pair": '[1,"a"]',
    "point": {"x": 1, "y": 2},
    "tree": {"value": 1, "children": ['{"children":[],"value":2}']},
}


def order(*extra):
    properties = {
        "id": {"type": "integer"},
        "amount": {"type": "number"},
        "status": {"type": "string"},
        "legacy": {"type": "integer"},
    }
    for name, schema in extra:
        properties[name] = schema

    return {"type": "object", "title": "Order", "properties": properties}


ORDER_1 = order()
ORDER_2 = order(("currency", {"type": ["string", "null"]}))
ORDER_3 = order(("legacy", {"type": "string"}))

ORDER_TABLES = [
    table(1, field(10, "id", "long"), field(11, "amount", "double"), field(12, "status", "string"), field(13, "legacy", "long")),
    table(
        2,
        field(10, "id", "long"),
        field(11, "amount", "double"),
        field(12, "status", "string"),
        field(13, "legacy", "long"),
        field(14, "currency", "string"),
    ),
    table(
        3,
        field(10, "id", "long"),
        field(11, "amount", "double"),
        field(12, "status", "string"),
        field(13, "legacy_s2", "long"),
        field(14, "currency", "string"),
        field(15, "legacy", "string"),
    ),
]

POINT = {"type": "object", "title": "Point", "properties": {"x": {"type": "integer"}}}
MAIN = {"type": "object", "title": "Main", "properties": {"p": {"$ref": "point{run}.json"}}}

WIRE = {"type": "object", "title": "Wire", "properties": {"n": {"type": "integer"}}}
ELSEWHERE = {"type": "object", "title": "Elsewhere", "properties": {"n": {"type": "integer"}, "e": {"type": "string"}}}


def deep(level=0):
    child = {"type": "integer"} if level == DEEPER_THAN_MINK_MAPS else deep(level + 1)

    return {"type": "object", "title": f"Deep{level}", "properties": {"next": child}}


def value(schema, rows_table, **extra):
    return {"part": "value", "schema": schema, "table": rows_table, **extra}


CASES = [
    {
        "name": "types",
        "versions": [value(TYPES, TYPES_TABLE)],
        "records": [{"value": {"version": 0, "data": TYPES_RECORD}, "row": TYPES_ROW}],
    },
    {
        "name": "evolution",
        "versions": [value(ORDER_1, ORDER_TABLES[0]), value(ORDER_2, ORDER_TABLES[1]), value(ORDER_3, ORDER_TABLES[2])],
        "records": [
            {
                "value": {"version": 0, "data": {"id": 1, "amount": 1.5, "status": "new", "legacy": 7}},
                "row": {"_kafka_key": None, "_kafka_value": None, "id": 1, "amount": 1.5, "status": "new", "legacy_s2": 7, "currency": None, "legacy": None},
            },
            {
                "value": {"version": 1, "data": {"id": 2, "amount": 2.25, "status": "paid", "legacy": 8, "currency": "EUR"}},
                "row": {"_kafka_key": None, "_kafka_value": None, "id": 2, "amount": 2.25, "status": "paid", "legacy_s2": 8, "currency": "EUR", "legacy": None},
            },
            {
                "value": {"version": 2, "data": {"id": 3, "amount": 3.5, "status": "sent", "legacy": "x"}},
                "row": {"_kafka_key": None, "_kafka_value": None, "id": 3, "amount": 3.5, "status": "sent", "legacy_s2": None, "currency": None, "legacy": "x"},
            },
        ],
    },
    {
        "name": "references",
        "versions": [
            {"part": "value", "subject": "point", "schema": POINT, "rejection": "not_accepted"},
            value(
                MAIN,
                table(1, field(10, "p", {"type": "struct", "fields": [field(11, "x", "long")]})),
                references=[{"name": "point{run}.json", "subject": "payload-{run}-references-point", "version": 1}],
            ),
        ],
        "records": [{"value": {"version": 1, "data": {"p": {"x": 5}}}, "row": {"_kafka_key": None, "_kafka_value": None, "p": {"x": 5}}}],
    },
    {
        "name": "wire",
        "versions": [
            value(WIRE, table(1, field(10, "n", "long"))),
            {"part": "value", "subject": "elsewhere", "schema": ELSEWHERE, "rejection": "not_accepted"},
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
        "records": [{"value": {"version": 0, "data": {}}, "row": {"_kafka_key": None, "_kafka_value": RAW}}],
    },
]
