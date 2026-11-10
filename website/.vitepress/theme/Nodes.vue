<script setup lang="ts">
import { ref } from 'vue';
import { box, ease, el, flow, lerp, span, steps, txt, useLoop } from './motion';

const svg = ref<SVGSVGElement>();

useLoop(svg, {
  loop: 12,
  still: 7.5,
  build(s) {
    const step = steps(20, 74, [
      ['Nodes renew their', 'leases in Postgres.'],
      ['node 2 stops. Its lease', 'expires after lease_ttl.'],
      ['node 3 takes it. The', 'cache rebuilds.'],
      ['node 2 rejoins with', 'an empty cache.'],
    ], s);
    box(190, 16, 510, 30, s);
    txt(206, 36, 'Kafka', 'name', s);
    box(190, 240, 510, 36, s);
    txt(206, 263, 'Postgres', 'name', s);
    txt(684, 263, 'members, leases, metadata', 'lab', s, 'end');
    const nodes = [190, 365, 540].map((x, i) => {
      const card = box(x, 76, 160, 132, s, 'dark');
      const title = txt(x + 14, 100, `node ${i + 1}`, 'name on-dark', s);
      const state = txt(x + 146, 100, 'serving', 'lab-dark', s, 'end');
      txt(x + 14, 190, 'cache', 'lab-dark', s);
      const track = el('rect', { x: x + 60, y: 182, width: 86, height: 8, class: 'track' }, s);
      const fill = el('rect', { x: x + 60, y: 182, width: 0, height: 8, class: 'red' }, s);
      const index = flow(`M${x + 80} 46 V76`, s, 2, 0.9 + i * 0.1);
      const renew = flow(`M${x + 80} 208 V240`, s, 1, 1.6);
      return { x, card, title, state, track, fill, index, renew };
    });
    const leases = ['orders p0', 'orders p1', 'events p0'].map((name) => {
      const g = el('g', {}, s);
      el('rect', { x: 0, y: 0, width: 132, height: 22, class: 'lease' }, g);
      txt(10, 15, name, 'lab lit', g);
      return g;
    });
    const ttl = el('rect', { x: nodes[1].x + 14, y: 140, width: 0, height: 2, class: 'red' }, s);

    return (t) => {
      const down = t >= 3 && t < 9.5;
      const expiring = t >= 3 && t < 6;
      const move = ease(span(t, 6, 6.8));
      leases[0].setAttribute('transform', `translate(${nodes[0].x + 14} 116)`);
      leases[2].setAttribute('transform', `translate(${nodes[2].x + 14} 116)`);
      leases[1].setAttribute('transform', `translate(${lerp(nodes[1].x + 14, nodes[2].x + 14, move)} ${lerp(116, 144, move)})`);
      leases[1].style.opacity = expiring ? '0.45' : '1';
      ttl.setAttribute('width', String(expiring ? 132 * (1 - span(t, 3, 6)) : 0));
      nodes.forEach((n, i) => {
        const off = i === 1 && down;
        n.card.setAttribute('class', off ? 'box' : 'dark');
        n.title.setAttribute('class', off ? 'name' : 'name on-dark');
        n.state.textContent = off ? 'stopped' : 'serving';
        n.state.setAttribute('class', off ? 'lab' : 'lab-dark');
        n.track.classList.toggle('off', off);
        let level = 0.85;
        if (i === 1) level = t < 3 ? 0.85 : t < 9.5 ? 0 : 0.08 * span(t, 9.5, 12);
        if (i === 2) level = t < 6.8 ? 0.6 : lerp(0.6, 0.95, ease(span(t, 6.8, 9.2)));
        n.fill.setAttribute('width', String(86 * level));
        const rebuilding = i === 2 && t > 6.8 && t < 9.2;
        n.index(t, off ? 0 : rebuilding ? 1 : 0.5);
        n.renew(t + i * 0.5, off ? 0 : 1);
      });
      const now = t < 3 ? 0 : t < 6 ? 1 : t < 9.5 ? 2 : 3;
      step.forEach((on, i) => on(i === now));
    };
  },
});
</script>

<template>
  <svg
    ref="svg"
    class="mink-flow"
    viewBox="0 16 720 260"
    role="img"
    aria-label="Three Mink nodes hold partition leases and renew them in Postgres. Node 2 stops, its lease expires after lease_ttl, and node 3 takes it while its cache rebuilds from Kafka. Node 2 rejoins with an empty cache."
  ></svg>
</template>
