<script setup lang="ts">
import { ref } from 'vue';
import { box, clamp, ease, el, flow, lerp, span, txt, useLoop } from './motion';

const svg = ref<SVGSVGElement>();

const ARRIVALS = [[0.4, 0], [1.4, 1], [2.4, 0], [3.4, 1], [4.4, 0], [5.4, 1], [6.4, 0], [7.4, 1], [8.4, 0]];
const EVOLVE = 5;
const TYPES = ['view', 'click', 'view', 'purchase', 'click'];
const COUNTRIES = ['DE', 'US', 'IN', 'BR', 'JP'];

useLoop(svg, {
  loop: 10,
  still: 7.9,
  build(s) {
    box(20, 30, 200, 210, s);
    txt(36, 54, 'Kafka', 'name', s);
    txt(204, 54, 'events', 'lab', s, 'end');
    const lanes = [0, 1].map((p) => {
      const y = 78 + p * 56;
      txt(36, y + 16, `p${p}`, 'lab', s);
      const clip = el('clipPath', { id: `topics-lane${p}` }, s);
      el('rect', { x: 58, y: y - 2, width: 148, height: 28 }, clip);
      const g = el('g', { 'clip-path': `url(#topics-lane${p})` }, s);
      const at = ARRIVALS.filter(([, q]) => q === p).map(([a]) => a);
      const cells = [...Array(14)].map(() => el('rect', { y, width: 15, height: 22, class: 'cell' }, g));
      return { at, cells };
    });
    const registry = box(36, 190, 168, 32, s, 'chip');
    const registryName = txt(48, 210, 'Schema Registry', 'lab', s);
    const version = txt(192, 210, 'v1', 'lab', s, 'end');

    box(250, 100, 90, 64, s, 'dark');
    txt(266, 126, 'Mink', 'name on-dark', s);
    txt(266, 146, 'catalog', 'lab-dark', s);
    const kafka = flow('M220 128 H250', s, 2, 0.9);
    const schemas = flow('M204 206 C240 206 270 190 290 164', s, 2, 1.6);
    const out = flow('M340 128 H370', s, 2, 0.9);

    box(370, 30, 330, 210, s);
    el('tspan', { dx: 6, class: 'lab' }, txt(386, 54, 'merged.events', 'name', s)).textContent = '(Iceberg table)';
    const cols = { id: 386, type: 466, country: 538, offset: 600 };
    const head = {
      country: txt(cols.country, 82, 'country', 'lab', s),
      offset: txt(cols.offset, 82, '_kafka_offset', 'lab', s),
    };
    txt(cols.id, 82, 'user_id', 'lab', s);
    txt(cols.type, 82, 'type', 'lab', s);
    el('line', { x1: 386, x2: 684, y1: 90, y2: 90, class: 'rule' }, s);
    const clip = el('clipPath', { id: 'topics-rows' }, s);
    el('rect', { x: 380, y: 94, width: 310, height: 133 }, clip);
    const rowsG = el('g', { 'clip-path': 'url(#topics-rows)' }, s);
    const offsets = [4810, 3920];
    const rows = [[-1.6, 1], [-0.6, 0], ...ARRIVALS].map(([a, p], j) => {
      const g = el('g', {}, rowsG);
      const fresh = el('rect', { x: 380, y: -15, width: 310, height: 22, class: 'red' }, g);
      txt(cols.id, 0, `user-${String((j * 379) % 10000).padStart(4, '0')}`, 'lab', g);
      txt(cols.type, 0, TYPES[j % 5], 'lab', g);
      const country = txt(cols.country, 0, a < EVOLVE ? 'null' : COUNTRIES[j % 5], 'lab', g);
      const offset = txt(cols.offset, 0, `p${p} · ${offsets[p]++}`, 'lab', g);
      return { at: a + 0.6, g, fresh, country, offset };
    });
    const queries = ['DuckDB', 'Trino', 'Spark', 'PyIceberg'].map((name, i) => {
      const x = 386 + i * 80;
      box(x, 256, 72, 26, s, 'chip');
      txt(x + 36, 273, name, 'chip-t', s, 'middle');
      return flow(`M${x + 36} 256 V240`, s, 1, 1.1 + i * 0.13);
    });

    return (t) => {
      for (const lane of lanes) {
        const shown = lane.at.reduce((n, a) => n + ease(span(t, a, a + 0.35)), 0);
        lane.cells.forEach((cell, i) => {
          const k = i - 9;
          const age = k >= 0 ? t - lane.at[k] : 9;
          cell.setAttribute('x', String(62 + 19 * (7 + k - shown)));
          cell.setAttribute('class', age >= 0 && age < 0.9 ? 'red' : 'cell');
          cell.style.opacity = age < 0 ? '0' : '1';
        });
      }
      const evolved = t >= EVOLVE;
      const lit = evolved && t < EVOLVE + 1.2;
      version.textContent = evolved ? 'v2' : 'v1';
      registry.classList.toggle('on', lit);
      registryName.classList.toggle('lit', lit);
      version.classList.toggle('lit', lit);
      const v2 = ease(span(t, EVOLVE, EVOLVE + 0.6));
      head.country.style.opacity = String(v2);
      const offsetX = String(lerp(cols.country, cols.offset, v2));
      head.offset.setAttribute('x', offsetX);
      const landed = rows.reduce((n, r) => n + ease(span(t, r.at, r.at + 0.35)), 0);
      rows.forEach((r, j) => {
        r.g.setAttribute('transform', `translate(0 ${110 + (landed - j - 1) * 22})`);
        r.g.style.opacity = String(clamp(landed - j));
        r.country.style.opacity = String(v2);
        r.offset.setAttribute('x', offsetX);
        r.fresh.style.opacity = String(t > r.at ? 0.12 * (1 - span(t, r.at + 0.3, r.at + 1.4)) : 0);
      });
      kafka(t);
      out(t + 0.3);
      schemas(t, t >= EVOLVE - 1 && t < EVOLVE + 0.6 ? 1 : 0.25);
      queries.forEach((q) => q(t));
    };
  },
});
</script>

<template>
  <svg
    ref="svg"
    class="mink-flow"
    viewBox="0 24 720 260"
    role="img"
    aria-label="Records published to two partitions of the events topic appear as rows of the merged.events table. The registry moves to schema v2, the table gains a country column, and older rows read null."
  ></svg>
</template>
