# Statistics

A table publishes a distinct-value estimate per column as an Iceberg statistics file of Apache DataSketches theta sketches.

<div class="mink-diagram">
<svg viewBox="0 0 660 112" width="660" role="img" aria-label="Postgres holds one sketch per column per UTC day. A commit unions each column's day sketches into one blob of the Puffin file, and the table metadata lists the file under statistics.">
  <defs>
    <marker id="statistics-arrow" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
      <path d="M0 0.5 L7.5 4 L0 7.5 Z" class="arrow"/>
    </marker>
  </defs>
  <rect x="20" y="20" width="180" height="56" class="box"/>
  <text x="110" y="44" text-anchor="middle" class="label">day sketches</text>
  <text x="110" y="62" text-anchor="middle" class="sub">per column, in Postgres</text>
  <rect x="240" y="20" width="180" height="56" class="box-accent"/>
  <text x="330" y="44" text-anchor="middle" class="label">one blob per column</text>
  <text x="330" y="62" text-anchor="middle" class="sub">{N}-e{E}-stats.puffin</text>
  <rect x="460" y="20" width="180" height="56" class="box"/>
  <text x="550" y="44" text-anchor="middle" class="label">table metadata</text>
  <text x="550" y="62" text-anchor="middle" class="sub">statistics</text>
  <path d="M200 54 L236 54" class="edge" marker-end="url(#statistics-arrow)"/>
  <path d="M460 54 L424 54" class="edge" marker-end="url(#statistics-arrow)"/>
  <text x="218" y="40" text-anchor="middle" class="sub">union</text>
  <text x="442" y="40" text-anchor="middle" class="sub">lists</text>
  <text x="330" y="100" text-anchor="middle" class="sub">every statistics_interval</text>
</svg>
</div>

## Sketches

| | Value |
| --- | --- |
| Columns | Every primitive column of the latest schema outside lists and maps, `_kafka_key` and `_kafka_value` included |
| Sketch | Update theta sketch, `lg_k` 12, seed 9001 |
| Values | Each non-null value's Iceberg single-value encoding, never truncated. Empty strings and binaries are skipped, NaN counts as a value. |
| Stored | One per column per UTC day of `_kafka_timestamp`, compact, ordered and uncompressed, as datasketches-java and datasketches-cpp read it |

## Statistics file

| | Value |
| --- | --- |
| Path | `{warehouse}/{topic}/metadata/{N}-e{E}-stats.puffin`, for snapshot `N` and epoch `E` |
| Blobs | One per column, uncompressed, type `apache-datasketches-theta-v1`, with `fields` the column's field id, `snapshot-id` `N` and `sequence-number` the snapshot's |
| `ndv` | Each blob's property: the union's estimate, rounded |
| Listing | Snapshot `N`'s only `statistics` entry, with path, length, footer length and blob metadata |
