<script setup lang="ts">
import { ref } from 'vue';
import { box, ease, el, flow, span, txt, useLoop } from './motion';

const svg = ref<SVGSVGElement>();

const PUBLISHES: [number, number, string][] = [
  [1, 1, 'user-4815'],
  [2.5, 0, 'user-0921'],
  [4, 2, 'user-3307'],
  [5.5, 0, 'user-1460'],
  [7, 1, 'user-2788'],
];
const COMMIT = 10, BEFORE = 16624;
const BASE = 14, RIGHT = 486, STEP = 17;
const x = (t: number) => 220 + t * 46;
const count = (n: number) => (BEFORE + n).toLocaleString('en-US');

useLoop(svg, {
  loop: COMMIT,
  still: 7.6,
  build(s) {
    box(20, 36, 160, 170, s);
    txt(36, 60, 'engine', 'name', s);
    txt(36, 84, 'SELECT count(*)', 'lab', s);
    txt(36, 100, 'FROM merged.events', 'lab', s);
    el('line', { x1: 36, x2: 164, y1: 114, y2: 114, class: 'rule' }, s);
    const total = txt(36, 142, '', 'name', s);
    const last = txt(36, 162, '', 'lab', s);

    box(220, 36, 280, 170, s);
    txt(236, 60, 'Kafka', 'name', s);
    txt(484, 60, 'events', 'lab', s, 'end');
    const lanes = [0, 1, 2].map((p) => {
      const y = 82 + p * 40;
      txt(236, y + 15, `p${p}`, 'lab', s);
      const clip = el('clipPath', { id: `realtime-lane${p}` }, s);
      el('rect', { x: 258, y: y - 2, width: 230, height: 26 }, clip);
      const g = el('g', { 'clip-path': `url(#realtime-lane${p})` }, s);
      const at = PUBLISHES.filter(([, q]) => q === p).map(([a]) => a);
      const cells = [...Array(BASE + at.length)].map(() => el('rect', { y, width: 13, height: 22, class: 'cell' }, g));
      const publish = flow(`M540 133 C520 133 520 ${y + 11} 500 ${y + 11}`, s, 2, 0.6);
      return { at, cells, publish };
    });

    box(540, 108, 160, 50, s);
    txt(556, 130, 'producer', 'name', s);
    const sending = txt(556, 148, '', 'lab', s);

    const ask = flow('M180 104 H220', s, 2, 0.8);
    const answer = flow('M220 140 H180', s, 2, 0.8);

    el('line', { x1: x(0), x2: x(COMMIT), y1: 246, y2: 246, class: 'rule' }, s);
    el('line', { x1: x(COMMIT), x2: x(COMMIT), y1: 236, y2: 256, class: 'mark' }, s);
    txt(x(0), 266, 'each dot: published and queryable at once', 'lab', s);
    txt(x(COMMIT), 266, 'next Iceberg commit', 'lab', s, 'end');
    const marks = PUBLISHES.map(([at], i) => {
      const g = el('g', {}, s);
      el('circle', { cx: x(at), cy: 246, r: 3.5, class: 'red' }, g);
      txt(x(at), 234, count(i + 1), 'lab alert', g, 'middle');
      return { at, g };
    });
    const now = el('line', { y1: 238, y2: 254, class: 'snap' }, s);

    return (t) => {
      for (const lane of lanes) {
        const shown = lane.at.reduce((n, a) => n + ease(span(t, a, a + 0.3)), 0);
        lane.cells.forEach((cell, i) => {
          const k = i - BASE;
          const age = k >= 0 ? t - lane.at[k] : -1;
          cell.setAttribute('x', String(RIGHT - 13 - STEP * (shown - 1 - k)));
          cell.setAttribute('class', k < 0 ? 'cell' : age < 0.8 ? 'red' : 'tail');
          cell.style.opacity = k >= 0 && age < 0 ? '0' : '1';
        });
        lane.publish(t, lane.at.some((a) => t > a - 0.6 && t < a) ? 1 : 0);
      }
      const published = PUBLISHES.filter(([a]) => a <= t);
      const latest = published.at(-1);
      const sendingNow = PUBLISHES.find(([a]) => t > a - 0.6 && t < a + 0.3);
      sending.textContent = sendingNow ? `${sendingNow[2]} → p${sendingNow[1]}` : 'publishing';
      ask(t);
      answer(t + 0.4);
      total.textContent = count(published.length);
      total.setAttribute('class', latest && t - latest[0] < 0.8 ? 'name alert' : 'name');
      last.textContent = latest ? `last: ${latest[2]} · p${latest[1]}` : '';
      marks.forEach((m) => (m.g.style.opacity = String(span(t, m.at, m.at + 0.2))));
      now.setAttribute('x1', String(x(t)));
      now.setAttribute('x2', String(x(t)));
    };
  },
});
</script>

<template>
  <svg
    ref="svg"
    class="mink-flow"
    viewBox="0 20 720 260"
    role="img"
    aria-label="A producer publishes five records across three partitions of the events topic. The engine keeps counting merged.events, and the count goes up the moment each record is published, long before the next Iceberg commit."
  ></svg>
</template>
