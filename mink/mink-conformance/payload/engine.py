import datetime
import json
import pathlib
import uuid
import urllib.request

from confluent_kafka.schema_registry import (
    SchemaRegistryClient,
    header_schema_id_serializer,
    prefix_schema_id_serializer,
)
from confluent_kafka.serialization import MessageField, SerializationContext

FIXTURES = pathlib.Path(__file__).resolve().parents[3] / "conformance" / "fixtures"
BINARY = json.loads((FIXTURES / "schema" / "binary.json").read_text())["fields"]
UTC = datetime.timezone.utc
EPOCH = datetime.datetime(1970, 1, 1, tzinfo=UTC)
DEEPER_THAN_MINK_MAPS = 70
HIGHEST_SCHEMA_ID = 2**31 - 1
RAW = object()
MISSING_GUID = "00000000-0000-0000-0000-000000000000"
CONTENT = {"Content-Type": "application/vnd.schemaregistry.v1+json"}


def micros(moment):
    return (moment - EPOCH) // datetime.timedelta(microseconds=1)


def field(id, name, type):
    return {"id": id, "name": name, "required": False, "type": type}


def table(schema_id, *fields):
    return {"type": "struct", "schema-id": schema_id, "fields": BINARY + list(fields)}


def call(method, url, body=None):
    data = None if body is None else json.dumps(body).encode()
    request = urllib.request.Request(url, data=data, method=method, headers=CONTENT)

    with urllib.request.urlopen(request) as response:
        return json.loads(response.read())


def allow_any_change(url, subject):
    call("PUT", f"{url}/config/{subject}", {"compatibility": "NONE"})


def stored(url, schema_id):
    schema = call("GET", f"{url}/schemas/ids/{schema_id}")
    versions = call("GET", f"{url}/schemas/ids/{schema_id}/versions")

    references = [
        {"name": reference["name"], "id": reference_id(url, reference)}
        for reference in schema.get("references", [])
    ]
    return {
        "id": schema_id,
        "format": schema.get("schemaType", "AVRO").lower(),
        "definition": schema["schema"],
        "references": references,
        "subjects": sorted(version["subject"] for version in versions),
    }


def reference_id(url, reference):
    return call("GET", f"{url}/subjects/{reference['subject']}/versions/{reference['version']}")["id"]


def guid(url, schema_id):
    return call("GET", f"{url}/schemas/ids/{schema_id}")["guid"]


def serializer_conf(subject, header, auto_register=False):
    return {
        "auto.register.schemas": auto_register,
        "subject.name.strategy": lambda context, name: subject,
        "schema.id.serializer": header_schema_id_serializer if header else prefix_schema_id_serializer,
    }


def serialize(serializer, topic, part, data):
    headers = []
    field = MessageField.KEY if part == "key" else MessageField.VALUE

    payload = serializer(data, SerializationContext(topic, field, headers))

    return payload, [[key, value.hex()] for key, value in headers]


def part_bytes(format, client, topic, part, spec, versions):
    if spec is None:
        return None, []
    if "raw" in spec:
        return spec["raw"].encode(), []
    if spec.get("missing_id"):
        return bytes([0]) + HIGHEST_SCHEMA_ID.to_bytes(4, "big") + b"\x02", []
    if spec.get("missing_guid"):
        header = "__key_schema_id" if part == "key" else "__value_schema_id"
        return b"\x02", [[header, (bytes([1]) + uuid.UUID(MISSING_GUID).bytes).hex()]]

    version = versions[spec["version"]]
    payload, headers = format.serialize(client, topic, part, version, spec)

    if "truncate" in spec:
        payload = payload[: -spec["truncate"]]
    return payload, headers


def record_json(format, client, topic, record, versions):
    key, key_headers = part_bytes(format, client, topic, "key", record.get("key"), versions)
    value, value_headers = part_bytes(format, client, topic, "value", record.get("value"), versions)

    row = dict(record["row"])
    if row.get("_kafka_key") is RAW:
        row["_kafka_key"] = key.hex()
    if row.get("_kafka_value") is RAW:
        row["_kafka_value"] = value.hex()

    return {
        "key": None if key is None else key.hex(),
        "value": None if value is None else value.hex(),
        "headers": key_headers + value_headers,
        "row": row,
    }


def case_json(format, url, client, run, case, registry, guids):
    topic = f"payload-{run}-{case['name']}"
    versions = []
    merges = []

    for version in case["versions"]:
        subject = f"{topic}-{version.get('subject', version['part'])}"
        allow_any_change(url, subject)
        version = {**version, "subject": subject, "run": run}
        schema_id = format.register(url, client, topic, version)

        registry[schema_id] = stored(url, schema_id)
        for reference in registry[schema_id]["references"]:
            registry[reference["id"]] = stored(url, reference["id"])
        guids[guid(url, schema_id)] = schema_id
        versions.append(version)

        outcome = {"rejection": version["rejection"]} if "rejection" in version else {"table": version["table"]}
        merges.append({"part": version["part"], "id": schema_id, **outcome})

    records = [record_json(format, client, topic, record, versions) for record in case["records"]]

    return {"name": case["name"], "topic": topic, "merges": merges, "records": records}


def generate(format, cases, url):
    client = SchemaRegistryClient({"url": url})
    run = uuid.uuid4().hex[:8]
    registry = {}
    guids = {MISSING_GUID: None}

    cases = [case_json(format, url, client, run, case, registry, guids) for case in cases]

    entries = [registry[schema_id] for schema_id in sorted(registry)]
    entries.append({"id": HIGHEST_SCHEMA_ID, "missing": True})
    return {
        "registry": entries,
        "guids": [{"guid": key, "id": value} for key, value in sorted(guids.items())],
        "cases": cases,
    }


def write(format, cases, url):
    fixtures = generate(format, cases, url)

    path = FIXTURES / "payload" / format.name / "cases.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(fixtures, indent=2, ensure_ascii=False) + "\n")

    print(f"wrote {path}")
