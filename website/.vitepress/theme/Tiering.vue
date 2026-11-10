<script setup lang="ts">
import { ref } from 'vue';
import { box, el, flow, mod, span, txt, useLoop } from './motion';

const svg = ref<SVGSVGElement>();

const X = 70, W = 610, AFTER = 0.6, SPEED = 0.04, INTERVAL = 6;
const VIEWS: [string, string, [boolean, boolean]][] = [
  ['lake', 'the external Iceberg table, the tiered records', [true, false]],
  ['stream', 'Kafka, from the tier boundary on', [false, true]],
  ['merged', 'both, each record once', [true, true]],
];

useLoop(svg, {
  loop: 12,
  still: 9,
  build(s) {
    const x = (f: number) => X + f * W;
    txt(x(AFTER), 34, 'tier_after', 'lab', s, 'middle');
    el('line', { x1: x(AFTER), x2: x(AFTER), y1: 42, y2: 206, class: 'mark' }, s);
    txt(x(1), 34, 'now', 'lab', s, 'end');
    const commit = txt(0, 222, 'tier commit', 'lab alert', s, 'middle');
    const lanes = [0, 1].map((p) => {
      const y = 60 + p * 70;
      txt(X - 12, y + 30, `p${p}`, 'lab', s, 'end');
      const clip = el('clipPath', { id: `tiering-lane${p}` }, s);
      el('rect', { x: X, y, width: W, height: 48 }, clip);
      const g = el('g', { 'clip-path': `url(#tiering-lane${p})` }, s);
      const lake = el('rect', { y, height: 48, class: 'lake' }, g);
      const files = el('rect', { y, height: 48, class: 'files' }, g);
      const ticks = [...Array(16)].map(() => el('line', { y1: y + 10, y2: y + 38, class: 'tick' }, g));
      const lakeName = txt(X + 14, y + 29, 'external Iceberg table', 'lab lake-t', g);
      const filesName = txt(0, y + 29, 'Mink virtual files (Iceberg table)', 'lab', g, 'middle');
      el('rect', { x: X, y, width: W, height: 48, class: 'frame' }, s);
      return { lake, files, ticks, lakeName, filesName, skew: p * 0.03 };
    });
    const arrivals = [84, 154].map((y, i) =>
      flow(`M${x(1) + 30} 50 C${x(1) + 10 + i * 10} ${y - 14} ${x(1) + 10} ${y} ${x(1) - 4} ${y}`, s, 2, 1.2 + i * 0.3));
    const chips = VIEWS.map(([name], i) => {
      const cx = X + i * 82;
      const chip = box(cx, 244, 74, 26, s, 'chip');
      const label = txt(cx + 37, 261, name, 'chip-t', s, 'middle');
      return (on: boolean) => {
        chip.classList.toggle('on', on);
        label.classList.toggle('on', on);
      };
    });
    const what = txt(X + 3 * 82 + 6, 261, '', 'lab', s);

    return (t) => {
      const since = mod(t - 1, INTERVAL);
      const view = Math.floor(t / 4) % 3;
      const [a, b] = VIEWS[view][2];
      for (const lane of lanes) {
        const boundary = AFTER - lane.skew - SPEED * since;
        lane.lake.setAttribute('x', String(X));
        lane.lake.setAttribute('width', String(x(boundary) - X));
        lane.files.setAttribute('x', String(x(boundary)));
        lane.files.setAttribute('width', String(x(1) - x(boundary)));
        lane.lake.classList.toggle('off', !a);
        lane.files.classList.toggle('off', !b);
        lane.lakeName.setAttribute('class', a ? 'lab lake-t' : 'lab');
        lane.filesName.setAttribute('class', b ? 'lab lit' : 'lab');
        lane.filesName.setAttribute('x', String((x(boundary) + x(1)) / 2));
        const lakeEnd = X + 14 + lane.lakeName.getComputedTextLength();
        const filesHalf = lane.filesName.getComputedTextLength() / 2;
        const filesMid = (x(boundary) + x(1)) / 2;
        lane.ticks.forEach((tick, k) => {
          const f = mod(k * 0.0625 - SPEED * t + lane.skew, 1);
          const old = f < boundary;
          const covered = x(f) < lakeEnd + 8 || Math.abs(x(f) - filesMid) < filesHalf + 8;
          tick.setAttribute('x1', String(x(f)));
          tick.setAttribute('x2', String(x(f)));
          tick.classList.toggle('old', old);
          tick.style.opacity = covered ? '0' : old ? '0.3' : '0.6';
        });
      }
      commit.setAttribute('x', String(x(AFTER - SPEED * since)));
      commit.style.opacity = String(1 - span(since, 0.8, 1.6));
      chips.forEach((on, i) => on(i === view));
      what.textContent = VIEWS[view][1];
      arrivals.forEach((arrive, i) => arrive(t + i * 0.4));
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
    aria-label="Each partition reads left to right as the external Iceberg table, then Mink virtual files (an Iceberg table) up to now. Records age past tier_after, and each tier commit moves the boundary up to it. The lake namespace is the external table, stream is Kafka from the boundary on, and merged is both."
  ></svg>
</template>
