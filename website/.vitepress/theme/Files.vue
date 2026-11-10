<script setup lang="ts">
import { ref } from 'vue';
import { box, ease, el, flow, lerp, span, steps, txt, useLoop } from './motion';

const svg = ref<SVGSVGElement>();

const READS: [number, string][] = [[0, 'GET footer'], [3, 'GET row group 0'], [7, 'GET row group 0']];

useLoop(svg, {
  loop: 10,
  still: 6.5,
  build(s) {
    const step = steps(20, 226, [['footer: from Postgres'], ['row group 0: built from Kafka'], ['again: from the cache']], s, 26);
    box(20, 88, 110, 56, s);
    txt(36, 112, 'engine', 'name', s);
    txt(36, 130, 'Trino, Spark', 'lab', s);

    box(250, 68, 180, 96, s, 'dark');
    txt(266, 94, 'Mink', 'name on-dark', s);
    txt(266, 112, 'S3 endpoint', 'lab-dark', s);
    const cache = ([['memory', 266], ['disk', 344]] as const).map(([name, x]) => {
      const chip = box(x, 128, 70, 22, s, 'chip');
      const label = txt(x + 35, 143, name, 'chip-t', s, 'middle');
      return (on: boolean, hit = false) => {
        chip.classList.toggle('on', on);
        chip.classList.toggle('hit', hit);
        label.classList.toggle('on', on);
      };
    });

    box(545, 36, 155, 56, s);
    txt(561, 60, 'Postgres', 'name', s);
    txt(561, 78, 'footers, stats', 'lab', s);
    box(545, 130, 155, 56, s);
    txt(561, 154, 'Kafka', 'name', s);
    txt(561, 172, 'offsets [1500, 3000)', 'lab', s);
    box(545, 224, 155, 56, s);
    txt(561, 248, 'Bucket', 'name', s);
    txt(561, 266, 'metadata files', 'lab', s);
    el('path', { d: 'M430 140 C490 140 490 252 545 252', class: 'path' }, s);

    txt(250, 216, '1500-3000.t0_498385.parquet', 'lab', s);
    const blocks = ([['row group 0', 226], ['row group 1', 248], ['footer', 270]] as const).map(([name, y]) => {
      box(250, y, 180, 18, s);
      const fill = el('rect', { x: 250, y, width: 0, height: 18, class: 'red', opacity: 0.18 }, s);
      txt(258, y + 13, name, 'lab', s);
      return fill;
    });
    const reach = el('rect', { x: 250, y: 226, width: 0, height: 18 }, el('clipPath', { id: 'files-built' }, s));
    txt(258, 239, 'row group 0', 'lab lit', el('g', { 'clip-path': 'url(#files-built)' }, s));

    const ask = flow('M130 104 H250', s, 3, 0.7);
    const request = el('g', {}, s);
    box(0, -11, 104, 22, request, 'chip on');
    const requestName = txt(52, 4, '', 'chip-t on', request, 'middle');
    const back = flow('M250 128 H130', s, 3, 0.7);
    const postgres = flow('M430 92 C490 92 490 64 545 64', s, 3, 0.8);
    const kafka = flow('M545 158 C490 158 490 116 430 116', s, 4, 0.9);

    return (t) => {
      const [at, name] = READS.findLast(([a]) => t >= a)!;
      request.setAttribute('transform', `translate(138 ${lerp(92, 82, ease(span(t, at, at + 0.3)))})`);
      request.style.opacity = String(span(t, at, at + 0.2) * (1 - span(t, at + 0.8, at + 1)));
      requestName.textContent = name;
      ask(t, t - at < 0.8 ? 1 : 0);
      const built = ease(span(t, 4.0, 5.6));
      const building = t > 4 && t < 5.6;
      blocks[2].setAttribute('width', String(t >= 1.6 ? 180 : 0));
      blocks[0].setAttribute('width', String(180 * built));
      blocks[0].setAttribute('opacity', building ? '1' : '0.18');
      reach.setAttribute('width', String(building ? 180 * built : 0));
      postgres(t, t > 0.8 && t < 1.8 ? 1 : 0);
      kafka(t, t > 3.8 && t < 5.6 ? 1 : 0);
      back(t, (t > 1.8 && t < 2.6) || (t > 6.1 && t < 6.9) || (t > 7.8 && t < 8.6) ? 1 : 0);
      cache[0](t > 5.6, t > 7.7 && t < 8.6);
      cache[1](t > 6.0);
      const now = t < 3 ? 0 : t < 7 ? 1 : 2;
      step.forEach((on, i) => on(i === now));
    };
  },
});
</script>

<template>
  <svg
    ref="svg"
    class="mink-flow"
    viewBox="0 30 720 260"
    role="img"
    aria-label="An engine reads a data file from Mink's S3 endpoint. The footer comes from Postgres with no build. Row group 0 is built from Kafka offsets 1500 to 3000 and cached in memory and on disk, and the same read again comes from the cache. The bucket holds only metadata files."
  ></svg>
</template>
