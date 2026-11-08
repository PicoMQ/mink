# File formats

Every file Mink writes or serves is pinned here. Fixtures in [`conformance/fixtures/`](https://github.com/PicoMQ/mink/tree/main/conformance/fixtures) hold the bytes tests compare against, checked by DuckDB, PyIceberg, Apache DataSketches and a Kafka broker.

<div class="mink-diagram">
<svg viewBox="0 0 660 356" width="660" role="img" aria-label="The table metadata file names the manifest list and the statistics file. The manifest list names Mink's manifests and the external table's manifests. Mink's manifests name Mink's data files, and the external table's manifests name the external data files.">
  <defs>
    <marker id="formats-arrow" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
      <path d="M0 0.5 L7.5 4 L0 7.5 Z" class="arrow"/>
    </marker>
  </defs>
  <rect x="30" y="20" width="240" height="56" class="box-accent"/>
  <text x="150" y="44" text-anchor="middle" class="label">table metadata</text>
  <text x="150" y="62" text-anchor="middle" class="sub">v{N}-e{E}.metadata.json</text>
  <rect x="390" y="20" width="240" height="56" class="box"/>
  <text x="510" y="44" text-anchor="middle" class="label">statistics file</text>
  <text x="510" y="62" text-anchor="middle" class="sub">{N}-e{E}-stats.puffin</text>
  <rect x="30" y="110" width="240" height="56" class="box"/>
  <text x="150" y="134" text-anchor="middle" class="label">manifest list</text>
  <text x="150" y="152" text-anchor="middle" class="sub">snap-{N}-e{E}.avro</text>
  <rect x="30" y="200" width="240" height="56" class="box"/>
  <text x="150" y="224" text-anchor="middle" class="label">Mink's manifests</text>
  <text x="150" y="242" text-anchor="middle" class="sub">{N}-e{E}-m{k}.avro</text>
  <rect x="390" y="200" width="240" height="56" class="edge-soft"/>
  <text x="510" y="224" text-anchor="middle" class="label">external manifests</text>
  <text x="510" y="242" text-anchor="middle" class="sub">the external table's</text>
  <rect x="30" y="290" width="240" height="56" class="box"/>
  <text x="150" y="314" text-anchor="middle" class="label">data files</text>
  <text x="150" y="332" text-anchor="middle" class="sub">{topic}/data/p={p}/….parquet</text>
  <rect x="390" y="290" width="240" height="56" class="edge-soft"/>
  <text x="510" y="314" text-anchor="middle" class="label">external data files</text>
  <text x="510" y="332" text-anchor="middle" class="sub">{location}/data/…</text>
  <path d="M270 48 L386 48" class="edge" marker-end="url(#formats-arrow)"/>
  <path d="M150 76 L150 106" class="edge" marker-end="url(#formats-arrow)"/>
  <path d="M150 166 L150 196" class="edge" marker-end="url(#formats-arrow)"/>
  <path d="M270 138 L510 138 L510 196" class="edge" marker-end="url(#formats-arrow)"/>
  <path d="M150 256 L150 286" class="edge" marker-end="url(#formats-arrow)"/>
  <path d="M510 256 L510 286" class="edge" marker-end="url(#formats-arrow)"/>
</svg>
</div>

| Format | Files | Fixtures |
| --- | --- | --- |
| [Data files](/docs/formats/data) | The Parquet data files Mink builds from Kafka | `path/`, `data/`, `records/` |
| [Schemas](/docs/formats/schema) | The `schemas` of table metadata | `schema/`, `payload/` |
| [Table metadata](/docs/formats/metadata) | Table metadata, manifest lists, manifests, and what tiering adds to an external table | `metadata/` |
| [Statistics](/docs/formats/statistics) | The Puffin statistics file | `statistics/` |

Dashed boxes belong to a [tiered](/docs/design/tiering) table's external table.
