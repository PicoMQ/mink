<script setup lang="ts">
import { computed, ref } from 'vue';

const features = [
  {
    name: 'Topics as tables',
    body: 'Add a topic and query it as an Iceberg table. Every row carries its partition, offset, timestamp, key, headers and value. Schema Registry fields become typed columns.',
  },
  {
    name: 'No data copies',
    body: 'Data files are built from Kafka on read, the same bytes every time. Kafka keeps the only copy of the records. Built files are cached in memory, and on disk when configured.',
  },
  {
    name: 'Real time',
    body: 'A table load covers every record committed up to that moment. No commit interval to wait for.',
  },
  {
    name: 'Kafka semantics',
    body: 'Committed records only. Aborted transactions and control records never show up. Exact row counts and one consistent snapshot across partitions.',
  },
  {
    name: 'Stateless nodes',
    body: 'An Iceberg REST catalog and an S3-compatible endpoint. Engines plan and run every query. Postgres holds the metadata, and every cache rebuilds from Kafka and Postgres.',
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
        <p class="engine__body" v-html="current.body"></p>
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
  min-height: 3.3rem;
  font-size: 1rem;
  line-height: 1.62;
  color: var(--mink-ink-3);
}

.engine__body :deep(a) {
  color: var(--mink-ink-1);
  text-decoration: underline;
  text-underline-offset: 0.15em;
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
