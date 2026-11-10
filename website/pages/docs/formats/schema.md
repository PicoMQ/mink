# Schemas

A table's schemas are the `schemas` of its [table metadata](/docs/formats/metadata). Schema 0 is the binary schema. A topic with Schema Registry records adds typed schemas 1, 2, 3 and so on, each the binary schema plus decoded columns.

## Binary schema

<div class="mink-diagram">
<svg viewBox="0 0 770 234" width="770" role="img" aria-label="The six top-level columns take field ids 1 to 6. The headers list's element takes 7, and its key and value take 8 and 9.">
  <defs>
    <marker id="schema-arrow" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
      <path d="M0 0.5 L7.5 4 L0 7.5 Z" class="arrow"/>
    </marker>
  </defs>
  <rect x="10" y="20" width="120" height="50" class="box"/>
  <text x="70" y="41" text-anchor="middle" class="label">1</text>
  <text x="70" y="59" text-anchor="middle" class="sub">_kafka_partition</text>
  <rect x="136" y="20" width="120" height="50" class="box"/>
  <text x="196" y="41" text-anchor="middle" class="label">2</text>
  <text x="196" y="59" text-anchor="middle" class="sub">_kafka_offset</text>
  <rect x="262" y="20" width="120" height="50" class="box"/>
  <text x="322" y="41" text-anchor="middle" class="label">3</text>
  <text x="322" y="59" text-anchor="middle" class="sub">_kafka_timestamp</text>
  <rect x="388" y="20" width="120" height="50" class="box"/>
  <text x="448" y="41" text-anchor="middle" class="label">4</text>
  <text x="448" y="59" text-anchor="middle" class="sub">_kafka_key</text>
  <rect x="514" y="20" width="120" height="50" class="box-accent"/>
  <text x="574" y="41" text-anchor="middle" class="label">5</text>
  <text x="574" y="59" text-anchor="middle" class="sub">_kafka_headers</text>
  <rect x="640" y="20" width="120" height="50" class="box"/>
  <text x="700" y="41" text-anchor="middle" class="label">6</text>
  <text x="700" y="59" text-anchor="middle" class="sub">_kafka_value</text>
  <rect x="514" y="100" width="120" height="50" class="box-accent"/>
  <text x="574" y="121" text-anchor="middle" class="label">7</text>
  <text x="574" y="139" text-anchor="middle" class="sub">element</text>
  <rect x="451" y="180" width="120" height="50" class="box"/>
  <text x="511" y="201" text-anchor="middle" class="label">8</text>
  <text x="511" y="219" text-anchor="middle" class="sub">key</text>
  <rect x="577" y="180" width="120" height="50" class="box"/>
  <text x="637" y="201" text-anchor="middle" class="label">9</text>
  <text x="637" y="219" text-anchor="middle" class="sub">value</text>
  <path d="M574 70 L574 96" class="edge" marker-end="url(#schema-arrow)"/>
  <path d="M554 150 L511 176" class="edge" marker-end="url(#schema-arrow)"/>
  <path d="M594 150 L637 176" class="edge" marker-end="url(#schema-arrow)"/>
</svg>
</div>

| Id | Column | Type | Required |
| --- | --- | --- | --- |
| 1 | `_kafka_partition` | `int` | yes |
| 2 | `_kafka_offset` | `long` | yes |
| 3 | `_kafka_timestamp` | `timestamptz` | yes |
| 4 | `_kafka_key` | `binary` | no |
| 5 | `_kafka_headers` | `list`, element 7 required | yes |
| 6 | `_kafka_value` | `binary` | no |
| 8, 9 | `key`, `value` of a header | `string` required, `binary` optional | |

The ids follow the order an Iceberg catalog gives a new table, every field of a struct before the fields nested in them. A catalog that creates a table with this schema keeps these ids.

## Typed schemas

| Ids | Columns |
| --- | --- |
| 1 to 9 | The binary schema |
| From 10 | `_kafka_decoded_key`, once the topic has a key schema |
| From 10 | One column per top-level field of the value schema, or one `value` column when the value schema is not a record, message or object |

- Decoded columns, struct fields, list elements and map values are optional. Map keys are required.
- Types follow the mappings in [Tables](/docs/design/tables#schema-registry).

## Field ids

A field keeps its id across schema versions while its identity path keeps its type, or a type that promotes it.

| Format | A field's identity |
| --- | --- |
| Avro | Its name, or the field its `aliases` name |
| Protobuf | Its field number |
| JSON Schema | Its property name |

- An identity path joins the identities from the top of the key or value down to the field with `.`. A list's element is `element`, and a map's key and value are `key` and `value`.
- New ids continue from the highest the topic has used, and an id is never given to another field.

## Evolution

<div class="mink-diagram">
<svg viewBox="0 0 660 178" width="660" role="img" aria-label="Schema 1 has amount as an int with id 12. Schema 2 promotes it to long and keeps id 12. Schema 3 changes it to string: amount takes the new id 15 and the old column stays as amount_s2 with id 12.">
  <defs>
    <marker id="evolution-arrow" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
      <path d="M0 0.5 L7.5 4 L0 7.5 Z" class="arrow"/>
    </marker>
  </defs>
  <text x="95" y="18" text-anchor="middle" class="sub">schema 1</text>
  <text x="330" y="18" text-anchor="middle" class="sub">schema 2</text>
  <text x="565" y="18" text-anchor="middle" class="sub">schema 3</text>
  <rect x="20" y="30" width="150" height="50" class="box"/>
  <text x="95" y="51" text-anchor="middle" class="label">amount int</text>
  <text x="95" y="69" text-anchor="middle" class="sub">id 12</text>
  <rect x="255" y="30" width="150" height="50" class="box"/>
  <text x="330" y="51" text-anchor="middle" class="label">amount long</text>
  <text x="330" y="69" text-anchor="middle" class="sub">id 12</text>
  <rect x="490" y="30" width="150" height="50" class="box-accent"/>
  <text x="565" y="51" text-anchor="middle" class="label">amount string</text>
  <text x="565" y="69" text-anchor="middle" class="sub">id 15</text>
  <rect x="490" y="110" width="150" height="50" class="edge-soft"/>
  <text x="565" y="131" text-anchor="middle" class="label">amount_s2 long</text>
  <text x="565" y="149" text-anchor="middle" class="sub">id 12</text>
  <path d="M170 55 L251 55" class="edge" marker-end="url(#evolution-arrow)"/>
  <path d="M405 55 L486 55" class="edge" marker-end="url(#evolution-arrow)"/>
  <path d="M330 80 L330 135 L486 135" class="edge-soft" marker-end="url(#evolution-arrow)"/>
  <text x="210" y="46" text-anchor="middle" class="sub">promoted</text>
  <text x="445" y="46" text-anchor="middle" class="sub">other type</text>
</svg>
</div>

The merge rules are in [Schema evolution](/docs/design/tables#schema-evolution). A merge that changes anything is the next schema id, and it becomes current.
