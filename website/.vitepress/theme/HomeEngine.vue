<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, ref } from 'vue';
import { VPButton } from 'vitepress/theme';
import Files from './Files.vue';
import Nodes from './Nodes.vue';
import Realtime from './Realtime.vue';
import Tiering from './Tiering.vue';
import Topics from './Topics.vue';

const features = [
  {
    name: 'Topics as tables',
    diagram: Topics,
    body: 'Query Kafka topics as Iceberg tables from any Iceberg-compatible engine, with typed columns from Schema Registry and support for schema evolution.',
  },
  {
    name: 'Virtual files',
    note: 'zero copy',
    diagram: Files,
    body: 'Kafka records form Iceberg data files on read, served from an S3-compatible endpoint, with only metadata stored and built files cached in memory and on disk.',
  },
  {
    name: 'Tiering',
    diagram: Tiering,
    body: 'Kafka records past a configurable age tier into an Iceberg table in an external REST catalog, with <code>merged</code>, <code>stream</code> and <code>lake</code> namespaces for the whole table and each part.',
  },
  {
    name: 'True real time',
    diagram: Realtime,
    body: 'A true real-time lakehouse. Kafka records are queryable as Iceberg tables as soon as they\'re published, consistent across partitions.',
  },
  {
    name: 'Stateless nodes',
    diagram: Nodes,
    body: 'Mink nodes hold only caches, with metadata in Postgres. Indexing and tiering of each Kafka partition move between nodes through leases, and every cache rebuilds from Kafka and Postgres.',
  },
] as const;

const active = ref(0);
const current = computed(() => features[active.value]);

function select(index: number) {
  active.value = index;
}

const engine = ref<HTMLElement>();
const plate = ref<HTMLElement>();
let observer: ResizeObserver | undefined;

function pin() {
  const box = plate.value!.getBoundingClientRect();
  const edge = engine.value!.getBoundingClientRect().right;
  const style = engine.value!.style;
  style.setProperty('--engine-top', `${box.top + scrollY}px`);
  style.setProperty('--engine-bottom', `${box.bottom + scrollY}px`);
  style.setProperty('--engine-edge', `${document.documentElement.clientWidth - edge}px`);
}

onMounted(() => {
  observer = new ResizeObserver(pin);
  observer.observe(engine.value!.parentElement!);
  observer.observe(plate.value!);
  addEventListener('resize', pin);
});

onBeforeUnmount(() => {
  observer?.disconnect();
  removeEventListener('resize', pin);
});
</script>

<template>
  <section ref="engine" class="engine mink-wrap" aria-label="The architecture">
    <div class="engine__mink" aria-hidden="true"></div>
    <div class="engine__head">
      <p class="engine__eyebrow">the architecture</p>
      <div class="engine__actions">
        <VPButton theme="brand" text="Get Started" href="/docs" />
        <VPButton theme="alt" text="GitHub" href="https://github.com/PicoMQ/mink" />
      </div>
    </div>

    <div ref="plate" class="engine__plate">
      <div class="engine__copy">
        <div class="engine__text">
          <h3>{{ current.name }}<template v-if="'note' in current"> ({{ current.note }})</template></h3>
          <p class="engine__body" v-html="current.body"></p>
        </div>
        <component :is="current.diagram" :key="current.name" class="engine__diagram" />
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
          <span v-if="'note' in feature" class="engine__tab-name">({{ feature.note }})</span>
        </button>
      </div>
    </div>
  </section>
</template>

<style scoped>
.engine {
  --engine-mink-w: min(26rem, 34vw);
  --engine-mink-h: calc(var(--engine-mink-w) * 772 / 591);
  --engine-inset: 14.5rem;
  --engine-mink-shift: 2.5rem;
  position: relative;
  z-index: 1;
  margin: 0 auto 5rem;
}

.engine__head {
  display: flex;
  flex-wrap: wrap;
  align-items: flex-end;
  gap: 1rem;
  margin-bottom: 1.25rem;
}

.engine__actions {
  display: flex;
  gap: 0.75rem;
  margin-right: var(--engine-inset);
  margin-left: auto;
}

.engine__eyebrow {
  margin: 0;
  font-family: var(--mink-font-serif);
  font-size: 1.5rem;
  font-weight: 400;
  letter-spacing: -0.005em;
  color: var(--mink-ink-1);
}

.engine__plate {
  margin-right: var(--engine-inset);
  border: 1px solid var(--mink-ink-6);
  background: rgb(246 247 249 / 0.85);
}

.engine__mink {
  position: fixed;
  top: calc(var(--engine-top, -100vh) - var(--engine-mink-h) * 0.6);
  right: calc(var(--engine-edge) - var(--engine-mink-shift));
  z-index: -1;
  width: var(--engine-mink-w);
  height: var(--engine-mink-h);
  background: url("/images/mink.webp") no-repeat right top / 100% auto;
  pointer-events: none;
}

.engine__copy {
  display: grid;
  gap: 1.5rem;
  min-width: 0;
  padding: 2rem 1.5rem 1.75rem;
}

.engine__text {
  display: flex;
  flex-direction: column;
  gap: 0.9rem;
  min-width: 0;
}

.engine__diagram {
  display: block;
  width: 100%;
  height: auto;
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

@media (max-width: 639px) {
  .engine__mink,
  .engine__diagram {
    display: none;
  }

  .engine__actions {
    margin-right: 0;
    margin-left: 0;
  }

  .engine__plate {
    margin-right: 0;
  }
}

@media (min-width: 860px) {
  .engine__copy {
    padding: 1.75rem 2rem;
  }

  .engine__tabs {
    grid-template-columns: repeat(5, minmax(0, 1fr));
  }
}

@media (min-width: 1200px) {
  .engine {
    --engine-mink-w: min(30rem, 36vw);
  }

  .engine__mink {
    top: calc(var(--engine-bottom, -100vh) - var(--engine-mink-h) * 0.977);
  }

  .engine__copy {
    grid-template-columns: 18.75rem minmax(0, 1fr);
    column-gap: 2rem;
  }
}

@media (min-width: 1200px) and (min-height: 700px) {
  .engine__eyebrow {
    font-size: 1.15rem;
  }

  .engine__copy {
    padding: 1rem 1.5rem 1.75rem;
  }

  .engine__text {
    gap: 0.55rem;
  }

  .engine__copy h3 {
    font-size: 1.3rem;
  }

  .engine__body {
    max-width: none;
    min-height: 4.05rem;
    font-size: 0.875rem;
    line-height: 1.55;
  }

  .engine__tab {
    padding: 0.65rem 0.75rem;
  }
}
</style>
