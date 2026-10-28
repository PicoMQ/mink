import sys

from . import avro, jsonschema, protobuf
from .engine import write

FORMATS = {
    "avro": (avro.Avro(), avro.CASES),
    "protobuf": (protobuf.Protobuf(), protobuf.CASES),
    "json": (jsonschema.JsonSchema(), jsonschema.CASES),
}


def main():
    url = sys.argv[1] if len(sys.argv) > 1 else "http://localhost:8081"
    names = sys.argv[2:] or list(FORMATS)

    for name in names:
        format, cases = FORMATS[name]
        write(format, cases, url)


main()
