import datetime
import importlib.resources
import pathlib
import tempfile

from confluent_kafka.schema_registry.protobuf import ProtobufSerializer
from google.protobuf import descriptor_pb2, descriptor_pool, json_format, message_factory
from grpc_tools import protoc

from .engine import DEEPER_THAN_MINK_MAPS, RAW, UTC, field, micros, serialize, serializer_conf, table

INCLUDE = str(importlib.resources.files("grpc_tools") / "_proto")


class Protobuf:
    name = "protobuf"

    def __init__(self):
        self.compiled = {}

    def register(self, url, client, topic, version):
        payload, _ = self.serialize(client, topic, version["part"], version, {"data": {}})

        return int.from_bytes(payload[1:5], "big")

    def serialize(self, client, topic, part, version, spec):
        message = spec.get("message", version["message"]).format(run=version["run"])
        message_class = self.message_class(version, message)

        conf = {**serializer_conf(version["subject"], spec.get("header", False), True), "use.deprecated.format": False}
        serializer = ProtobufSerializer(message_class, client, conf=conf)
        instance = json_format.ParseDict(spec["data"], message_class())

        return serialize(serializer, topic, part, instance)

    def message_class(self, version, message):
        files = {name.format(run=version["run"]): text.format(run=version["run"]) for name, text in version["files"].items()}
        key = tuple(sorted(files.items()))

        if key not in self.compiled:
            self.compiled[key] = descriptors(files)
        return message_factory.GetMessageClass(self.compiled[key].FindMessageTypeByName(message))


def descriptors(files):
    with tempfile.TemporaryDirectory() as temporary:
        directory = pathlib.Path(temporary)
        for name, text in files.items():
            (directory / name).write_text(text)

        output = directory / "set.pb"
        arguments = ["protoc", f"-I{directory}", f"-I{INCLUDE}", "--include_imports", f"--descriptor_set_out={output}"]
        if protoc.main(arguments + list(files)) != 0:
            raise RuntimeError(f"protoc failed for {list(files)}")

        described = descriptor_pb2.FileDescriptorSet.FromString(output.read_bytes())

    pool = descriptor_pool.DescriptorPool()
    for file in described.file:
        pool.Add(file)
    return pool


def deep_proto():
    chain = [f"message Deep{level} {{{{ Deep{level + 1} next = 1; }}}}" for level in range(DEEPER_THAN_MINK_MAPS)]
    last = f"message Deep{DEEPER_THAN_MINK_MAPS} {{{{ int64 n = 1; }}}}"

    return "\n".join(['syntax = "proto3";', "package fixtures.deep{run};", *chain, last]) + "\n"


TYPES_PROTO = """syntax = "proto3";
package fixtures.types{run};

import "google/protobuf/struct.proto";
import "google/protobuf/timestamp.proto";
import "google/protobuf/wrappers.proto";

message Types {{
  bool b = 1;
  int32 i = 2;
  int64 l = 3;
  uint32 u32 = 4;
  uint64 u64 = 5;
  sint32 s32 = 6;
  fixed64 f64 = 7;
  float f = 8;
  double d = 9;
  string s = 10;
  bytes by = 11;
  Color color = 12;
  Inner inner = 13;
  repeated int32 list = 14;
  map<string, int64> counts = 15;
  oneof choice {{
    string text = 16;
    int32 number = 17;
  }}
  optional string maybe = 18;
  google.protobuf.Timestamp at = 19;
  google.protobuf.Int64Value wrapped = 20;
  google.protobuf.Struct extra = 21;
  Node tree = 22;
}}

enum Color {{
  RED = 0;
  GREEN = 1;
}}

message Inner {{
  int32 x = 1;
  string y = 2;
}}

message Node {{
  int32 value = 1;
  repeated Node children = 2;
}}
"""

TYPES_TABLE = table(
    1,
    field(10, "b", "boolean"),
    field(11, "i", "int"),
    field(12, "l", "long"),
    field(13, "u32", "long"),
    field(14, "u64", "decimal(20, 0)"),
    field(15, "s32", "int"),
    field(16, "f64", "decimal(20, 0)"),
    field(17, "f", "float"),
    field(18, "d", "double"),
    field(19, "s", "string"),
    field(20, "by", "binary"),
    field(21, "color", "string"),
    field(22, "inner", {"type": "struct", "fields": [field(32, "x", "int"), field(33, "y", "string")]}),
    field(23, "list", {"type": "list", "element-id": 34, "element-required": False, "element": "int"}),
    field(
        24,
        "counts",
        {"type": "map", "key-id": 35, "key": "string", "value-id": 36, "value-required": False, "value": "long"},
    ),
    field(25, "text", "string"),
    field(26, "number", "int"),
    field(27, "maybe", "string"),
    field(28, "at", "timestamptz"),
    field(29, "wrapped", "long"),
    field(30, "extra", "string"),
    field(
        31,
        "tree",
        {
            "type": "struct",
            "fields": [
                field(37, "value", "int"),
                field(38, "children", {"type": "list", "element-id": 39, "element-required": False, "element": "string"}),
            ],
        },
    ),
)

AT = datetime.datetime(2026, 10, 28, 12, 0, 0, 123456, tzinfo=UTC)

FULL_TYPES = {
    "b": True,
    "i": -5,
    "l": "1234567890123",
    "u32": 4000000000,
    "u64": "18446744073709551615",
    "s32": -3,
    "f64": "7",
    "f": 1.5,
    "d": -2.25,
    "s": "héllo",
    "by": "AP8=",
    "color": "GREEN",
    "inner": {"x": 5, "y": "why"},
    "list": [1, 2, 3],
    "counts": {"b": "2", "a": "1"},
    "text": "chosen",
    "at": "2026-10-28T12:00:00.123456Z",
    "wrapped": "42",
    "extra": {"k": "v", "n": 1},
    "tree": {"value": 1, "children": [{"value": 2}]},
}

FULL_ROW = {
    "_kafka_key": None,
    "_kafka_value": None,
    "b": True,
    "i": -5,
    "l": 1234567890123,
    "u32": 4000000000,
    "u64": "18446744073709551615",
    "s32": -3,
    "f64": "7",
    "f": 1.5,
    "d": -2.25,
    "s": "héllo",
    "by": "00ff",
    "color": "GREEN",
    "inner": {"x": 5, "y": "why"},
    "list": [1, 2, 3],
    "counts": [["a", 1], ["b", 2]],
    "text": "chosen",
    "number": None,
    "maybe": None,
    "at": micros(AT),
    "wrapped": 42,
    "extra": '{"k":"v","n":1.0}',
    "tree": {"value": 1, "children": ['{"value":2}']},
}

DEFAULT_ROW = {
    "_kafka_key": None,
    "_kafka_value": None,
    "b": False,
    "i": 0,
    "l": 0,
    "u32": 0,
    "u64": "0",
    "s32": 0,
    "f64": "0",
    "f": 0.0,
    "d": 0.0,
    "s": "",
    "by": "",
    "color": "RED",
    "inner": None,
    "list": [],
    "counts": [],
    "text": None,
    "number": 7,
    "maybe": None,
    "at": None,
    "wrapped": None,
    "extra": None,
    "tree": None,
}

ORDER_1 = """syntax = "proto3";
package fixtures.order{run};

message Order {{
  int64 id = 1;
  float amount = 2;
  string status = 3;
  int32 legacy = 4;
}}
"""

ORDER_2 = """syntax = "proto3";
package fixtures.order{run};

message Order {{
  int64 id = 1;
  double amount = 2;
  string status = 3;
  int32 legacy = 4;
  optional string currency = 5;
}}
"""

ORDER_3 = """syntax = "proto3";
package fixtures.order{run};

message Order {{
  int64 id = 1;
  double amount = 2;
  string state = 3;
  string legacy = 4;
}}
"""

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

DEPENDENCY = """syntax = "proto3";
package fixtures.dep{run};

message Dep {{
  int64 x = 1;
}}
"""

MAIN = """syntax = "proto3";
package fixtures.main{run};

import "dep{run}.proto";

message Main {{
  fixtures.dep{run}.Dep d = 1;
}}
"""

WIRE = """syntax = "proto3";
package fixtures.wire{run};

message First {{
  int32 n = 1;
}}

message Second {{
  string s = 1;
}}
"""

ELSEWHERE = """syntax = "proto3";
package fixtures.elsewhere{run};

message Elsewhere {{
  int32 n = 1;
}}
"""


def order(files, table):
    return {"part": "value", "files": {"order{run}.proto": files}, "message": "fixtures.order{run}.Order", "table": table}


CASES = [
    {
        "name": "types",
        "versions": [
            {
                "part": "value",
                "files": {"types{run}.proto": TYPES_PROTO},
                "message": "fixtures.types{run}.Types",
                "table": TYPES_TABLE,
            }
        ],
        "records": [
            {"value": {"version": 0, "data": FULL_TYPES}, "row": FULL_ROW},
            {"value": {"version": 0, "data": {"number": 7}}, "row": DEFAULT_ROW},
        ],
    },
    {
        "name": "evolution",
        "versions": [order(ORDER_1, ORDER_TABLES[0]), order(ORDER_2, ORDER_TABLES[1]), order(ORDER_3, ORDER_TABLES[2])],
        "records": [
            {
                "value": {"version": 0, "data": {"id": "1", "amount": 1.5, "status": "new", "legacy": 7}},
                "row": {"_kafka_key": None, "_kafka_value": None, "id": 1, "amount": 1.5, "state": "new", "legacy_s2": 7, "currency": None, "legacy": None},
            },
            {
                "value": {"version": 1, "data": {"id": "2", "amount": 2.25, "status": "paid", "legacy": 8, "currency": "EUR"}},
                "row": {"_kafka_key": None, "_kafka_value": None, "id": 2, "amount": 2.25, "state": "paid", "legacy_s2": 8, "currency": "EUR", "legacy": None},
            },
            {
                "value": {"version": 2, "data": {"id": "3", "amount": 3.5, "state": "sent", "legacy": "x"}},
                "row": {"_kafka_key": None, "_kafka_value": None, "id": 3, "amount": 3.5, "state": "sent", "legacy_s2": None, "currency": None, "legacy": "x"},
            },
        ],
    },
    {
        "name": "references",
        "versions": [
            {
                "part": "value",
                "files": {"dep{run}.proto": DEPENDENCY, "main{run}.proto": MAIN},
                "message": "fixtures.main{run}.Main",
                "table": table(1, field(10, "d", {"type": "struct", "fields": [field(11, "x", "long")]})),
            }
        ],
        "records": [{"value": {"version": 0, "data": {"d": {"x": "5"}}}, "row": {"_kafka_key": None, "_kafka_value": None, "d": {"x": 5}}}],
    },
    {
        "name": "wire",
        "versions": [
            {"part": "value", "files": {"wire{run}.proto": WIRE}, "message": "fixtures.wire{run}.First", "table": table(1, field(10, "n", "int"))},
            {
                "part": "value",
                "subject": "elsewhere",
                "files": {"elsewhere{run}.proto": ELSEWHERE},
                "message": "fixtures.elsewhere{run}.Elsewhere",
                "rejection": "not_accepted",
            },
        ],
        "records": [
            {"value": {"version": 0, "data": {"n": 1}}, "row": {"_kafka_key": None, "_kafka_value": None, "n": 1}},
            {"value": {"version": 0, "data": {"n": 2}, "header": True}, "row": {"_kafka_key": None, "_kafka_value": None, "n": 2}},
            {
                "value": {"version": 0, "message": "fixtures.wire{run}.Second", "data": {"s": "x"}},
                "row": {"_kafka_key": None, "_kafka_value": RAW, "n": None},
            },
            {"value": {"raw": "no magic byte"}, "row": {"_kafka_key": None, "_kafka_value": RAW, "n": None}},
            {"value": {"version": 0, "data": {"n": 300}, "truncate": 1}, "row": {"_kafka_key": None, "_kafka_value": RAW, "n": None}},
            {"value": {"missing_id": True}, "row": {"_kafka_key": None, "_kafka_value": RAW, "n": None}},
            {"value": {"missing_guid": True}, "row": {"_kafka_key": None, "_kafka_value": RAW, "n": None}},
            {"value": {"version": 1, "data": {"n": 4}}, "row": {"_kafka_key": None, "_kafka_value": RAW, "n": None}},
        ],
    },
    {
        "name": "deep",
        "versions": [
            {
                "part": "value",
                "files": {"deep{run}.proto": deep_proto()},
                "message": "fixtures.deep{run}.Deep0",
                "rejection": "unsupported",
            }
        ],
        "records": [{"value": {"version": 0, "data": {}}, "row": {"_kafka_key": None, "_kafka_value": RAW}}],
    },
]
