<script setup lang="ts">
import { computed, ref } from 'vue';

const features = [
  {
    name: 'Real-time tables',
    body: 'Log tables and primary-key tables with typed columns, bucketing, and partitioning. Append, upsert, partial update, aggregate, point lookup, and scan on the same table, with a changelog for primary-key tables.',
  },
  {
    name: 'Zero-disk storage',
    body: 'The WAL and table data live on S3-compatible object storage through the s3stream engine. Nodes hold no persistent local state. Durability and economics are those of the object store, with no cross-AZ replication traffic.',
  },
  {
    name: 'Lakehouse tiering',
    body: 'A tiering worker moves closed log segments and KV snapshots into open lakehouse table formats behind a catalog. Each lake commit records the log offset it covers. Retention on the log is independent of the lake.',
  },
  {
    name: 'One unified read',
    body: 'A query resolves to a lake snapshot plus the log tail after it, per bucket, and returns one result. DataFusion and Flight SQL run over the union with predicates pushed to both sides. Lakehouse engines read the tiered data directly.',
  },
  {
    name: 'Kafka and Arrow Flight',
    body: 'Two frontends on one engine. Kafka produce, fetch, consumer groups, and offset commits map onto log tables. Arrow Flight carries typed Arrow batches, lookups, and admin over gRPC.',
  },
] as const;

const active = ref(0);
const current = computed(() => features[active.value]);

function select(index: number) {
  active.value = index;
}
</script>

<template>
  <section class="engine mink-wrap" aria-label="The architecture">
    <p class="engine__eyebrow">the architecture</p>

    <div class="engine__plate">
      <div class="engine__copy">
        <h3>{{ current.name }}</h3>
        <p class="engine__body">{{ current.body }}</p>
      </div>

      <div class="engine__tabs" role="tablist" aria-label="Engine features">
        <button
          v-for="(feature, index) in features"
          :key="feature.name"
          class="engine__tab"
          type="button"
          role="tab"
          :class="{ active: index === active }"
          :aria-selected="index === active"
          @click="select(index)"
        >
          <span class="engine__tab-name">{{ feature.name }}</span>
        </button>
      </div>
    </div>
  </section>
</template>

<style scoped>
.engine {
  position: relative;
  z-index: 1;
  margin: 0 auto 5rem;
}

.engine__eyebrow {
  margin: 0 0 1.25rem;
  font-family: var(--mink-font-serif);
  font-size: 1.5rem;
  font-weight: 400;
  letter-spacing: -0.005em;
  color: var(--mink-ink-1);
}

.engine__plate {
  border: 1px solid var(--mink-ink-6);
  background: rgb(246 247 249 / 0.85);
}

.engine__copy {
  display: flex;
  flex-direction: column;
  gap: 0.9rem;
  min-width: 0;
  padding: 2rem 1.5rem 1.75rem;
}

.engine__copy h3 {
  margin: 0;
  font-family: var(--mink-font-serif);
  font-size: 1.6rem;
  font-weight: 400;
  line-height: 1.15;
  letter-spacing: 0;
  color: var(--mink-ink-1);
}

.engine__body {
  margin: 0;
  max-width: 36rem;
  min-height: 4.8rem;
  font-size: 1rem;
  line-height: 1.62;
  color: var(--mink-ink-3);
}

.engine__tabs {
  display: grid;
  grid-template-columns: 1fr;
  gap: 1px;
  border-top: 1px solid var(--mink-ink-6);
  background: var(--mink-ink-6);
}

.engine__tab {
  display: grid;
  align-content: center;
  min-width: 0;
  padding: 0.95rem 1rem;
  border: 0;
  border-radius: 0;
  background: rgb(246 247 249 / 0.85);
  color: var(--mink-ink-3);
  text-align: left;
  cursor: pointer;
}

.engine__tab:hover {
  background: var(--mink-surface-1);
  color: var(--mink-ink-1);
}

.engine__tab.active {
  background: var(--mink-ink-1);
  color: var(--mink-surface-0);
}

.engine__tab-name {
  min-width: 0;
  font-family: var(--vp-font-family-mono);
  font-size: 0.8125rem;
  font-weight: 400;
  line-height: 1.25;
}

@media (min-width: 860px) {
  .engine__copy {
    padding: 2.25rem 2rem;
  }

  .engine__tabs {
    grid-template-columns: repeat(5, minmax(0, 1fr));
  }
}
</style>
