CREATE FUNCTION now_ms() RETURNS BIGINT LANGUAGE SQL VOLATILE
    RETURN (extract(epoch FROM clock_timestamp()) * 1000)::BIGINT;

CREATE FUNCTION covers(start_offset BIGINT, end_offset BIGINT, file_start BIGINT, file_end BIGINT)
    RETURNS BOOLEAN LANGUAGE SQL IMMUTABLE
    RETURN start_offset <= file_start AND file_end <= end_offset;

CREATE TABLE topic (
    name TEXT PRIMARY KEY,
    uuid UUID NOT NULL UNIQUE,
    partition_count INTEGER NOT NULL CHECK (partition_count > 0),
    state TEXT NOT NULL CHECK (state IN ('indexing', 'removing', 'replaced', 'missing')),
    removing_at_ms BIGINT,
    CHECK ((state = 'removing') = (removing_at_ms IS NOT NULL))
);

CREATE TABLE indexed_range (
    topic TEXT NOT NULL REFERENCES topic (name) ON DELETE CASCADE,
    partition INTEGER NOT NULL CHECK (partition >= 0),
    start_offset BIGINT NOT NULL,
    end_offset BIGINT NOT NULL,
    PRIMARY KEY (topic, partition),
    CHECK (0 <= start_offset AND start_offset <= end_offset)
);

CREATE TABLE time_spec (
    topic TEXT NOT NULL REFERENCES topic (name) ON DELETE CASCADE,
    id INTEGER NOT NULL CHECK (id >= 0),
    granularity TEXT NOT NULL CHECK (granularity IN ('hour', 'day', 'month', 'none')),
    PRIMARY KEY (topic, id)
);

CREATE TABLE file (
    topic TEXT NOT NULL REFERENCES topic (name) ON DELETE CASCADE,
    partition INTEGER NOT NULL CHECK (partition >= 0),
    start_offset BIGINT NOT NULL,
    end_offset BIGINT NOT NULL,
    spec_id INTEGER NOT NULL,
    bucket INTEGER,
    schema_id INTEGER NOT NULL CHECK (schema_id >= 0),
    assembly TEXT CHECK (assembly ~ '^[0-9a-f]{16}$'),
    version SMALLINT NOT NULL,
    stats JSONB NOT NULL,
    footer BYTEA NOT NULL,
    parts JSONB,
    added_snapshot_id BIGINT,
    replaced_snapshot_id BIGINT,
    UNIQUE NULLS NOT DISTINCT (topic, partition, start_offset, end_offset, spec_id, bucket, schema_id, assembly, version),
    FOREIGN KEY (topic, spec_id) REFERENCES time_spec (topic, id) ON DELETE CASCADE,
    CHECK (start_offset >= 0 AND end_offset > start_offset),
    CHECK ((assembly IS NULL) = (parts IS NULL)),
    CHECK ((assembly IS NULL) = (added_snapshot_id IS NULL))
);

CREATE TABLE sketch (
    topic TEXT NOT NULL REFERENCES topic (name) ON DELETE CASCADE,
    day BIGINT NOT NULL,
    field_id INTEGER NOT NULL,
    sketch BYTEA NOT NULL,
    PRIMARY KEY (topic, day, field_id)
);

CREATE TABLE snapshot (
    topic TEXT NOT NULL REFERENCES topic (name) ON DELETE CASCADE,
    id BIGINT NOT NULL CHECK (id > 0),
    timestamp_ms BIGINT NOT NULL,
    epoch BIGINT NOT NULL CHECK (epoch > 0),
    PRIMARY KEY (topic, id)
);

CREATE TABLE snapshot_offsets (
    topic TEXT NOT NULL,
    snapshot_id BIGINT NOT NULL,
    partition INTEGER NOT NULL CHECK (partition >= 0),
    start_offset BIGINT NOT NULL,
    end_offset BIGINT NOT NULL,
    PRIMARY KEY (topic, snapshot_id, partition),
    FOREIGN KEY (topic, snapshot_id) REFERENCES snapshot (topic, id) ON DELETE CASCADE,
    CHECK (0 <= start_offset AND start_offset <= end_offset)
);

CREATE TABLE member (
    id UUID PRIMARY KEY,
    roles TEXT[] NOT NULL,
    address TEXT,
    admin_address TEXT,
    expires_at_ms BIGINT NOT NULL
);

CREATE TABLE lease (
    topic TEXT NOT NULL REFERENCES topic (name) ON DELETE CASCADE,
    partition INTEGER CHECK (partition >= 0),
    holder UUID NOT NULL,
    epoch BIGINT NOT NULL,
    expires_at_ms BIGINT NOT NULL,
    UNIQUE NULLS NOT DISTINCT (topic, partition)
);

CREATE TABLE object (
    topic TEXT NOT NULL REFERENCES topic (name) ON DELETE CASCADE,
    path TEXT NOT NULL,
    kind TEXT NOT NULL CHECK (kind IN ('table_metadata', 'manifest_list', 'manifest', 'statistics')),
    last_snapshot_id BIGINT,
    unreferenced_at_ms BIGINT,
    PRIMARY KEY (topic, path)
);

CREATE TABLE registry_schema (
    key TEXT PRIMARY KEY,
    format TEXT NOT NULL CHECK (format IN ('avro', 'protobuf', 'json')),
    definition TEXT NOT NULL,
    referenced JSONB NOT NULL
);

CREATE TABLE table_schema (
    topic TEXT NOT NULL REFERENCES topic (name) ON DELETE CASCADE,
    id INTEGER NOT NULL CHECK (id > 0),
    definition JSONB NOT NULL,
    PRIMARY KEY (topic, id)
);

CREATE TABLE field (
    topic TEXT NOT NULL REFERENCES topic (name) ON DELETE CASCADE,
    part TEXT NOT NULL CHECK (part IN ('key', 'value')),
    path TEXT NOT NULL,
    field_id INTEGER NOT NULL CHECK (field_id > 9),
    PRIMARY KEY (topic, part, path)
);

CREATE TABLE topic_schema (
    topic TEXT NOT NULL REFERENCES topic (name) ON DELETE CASCADE,
    part TEXT NOT NULL CHECK (part IN ('key', 'value')),
    schema_ref TEXT NOT NULL,
    registry_key TEXT REFERENCES registry_schema (key),
    table_schema_id INTEGER CHECK (table_schema_id >= 0),
    field_ids JSONB,
    rejection TEXT CHECK (rejection IN ('missing', 'not_accepted', 'unsupported', 'reserved_name')),
    PRIMARY KEY (topic, part, schema_ref),
    CHECK ((rejection IS NULL) = (registry_key IS NOT NULL)),
    CHECK ((rejection IS NULL) = (table_schema_id IS NOT NULL)),
    CHECK ((rejection IS NULL) = (field_ids IS NOT NULL))
);

CREATE INDEX file_added ON file (topic, added_snapshot_id);
CREATE INDEX file_replaced ON file (topic, replaced_snapshot_id);
CREATE INDEX topic_schema_registry ON topic_schema (registry_key);
