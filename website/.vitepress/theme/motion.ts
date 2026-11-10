import { onBeforeUnmount, onMounted, type Ref } from 'vue';

const NS = 'http://www.w3.org/2000/svg';

type Attrs = Record<string, string | number>;

export const clamp = (x: number) => Math.min(1, Math.max(0, x));
export const span = (t: number, a: number, b: number) => clamp((t - a) / (b - a));
export const ease = (x: number) => 1 - Math.pow(1 - clamp(x), 3);
export const lerp = (a: number, b: number, p: number) => a + (b - a) * p;
export const mod = (a: number, n: number) => ((a % n) + n) % n;

export function el(tag: string, attrs: Attrs, parent?: Element): SVGGraphicsElement {
  const e = document.createElementNS(NS, tag) as SVGGraphicsElement;
  for (const k in attrs) e.setAttribute(k, String(attrs[k]));
  parent?.appendChild(e);
  return e;
}

export function txt(x: number, y: number, text: string, cls: string, parent: Element, anchor = 'start') {
  const e = el('text', { x, y, class: cls, 'text-anchor': anchor }, parent);
  e.textContent = text;
  return e;
}

export const box = (x: number, y: number, w: number, h: number, parent: Element, cls = 'box') =>
  el('rect', { x, y, width: w, height: h, class: cls }, parent);

export function flow(d: string, parent: Element, n = 3, period = 1.4) {
  const path = el('path', { d, class: 'path' }, parent) as SVGPathElement;
  const length = path.getTotalLength();
  const dots = [...Array(n)].map(() => el('circle', { r: 2.6, class: 'dot' }, parent));
  return (t: number, on = 1) =>
    dots.forEach((dot, i) => {
      const u = mod(t / period + i / n, 1);
      const p = path.getPointAtLength(u * length);
      dot.setAttribute('cx', String(p.x));
      dot.setAttribute('cy', String(p.y));
      dot.style.opacity = String(on * Math.sin(Math.PI * u));
    });
}

export function steps(x: number, y: number, lines: string[][], parent: Element, gap = 44) {
  return lines.map((text, i) => {
    const g = el('g', {}, parent);
    const marker = el('rect', { x, y: y + i * gap - 12, width: 16, height: 16, class: 'red' }, g);
    txt(x + 8, y + i * gap - 0.5, String(i + 1), 'num', g, 'middle');
    const words = text.map((line, k) => txt(x + 24, y + i * gap + k * 16, line, 'step', g));
    return (on: boolean) => {
      marker.style.opacity = on ? '1' : '0.25';
      words.forEach((w) => w.classList.toggle('on', on));
    };
  });
}

interface Diagram {
  loop: number;
  still: number;
  build(svg: SVGSVGElement): (t: number) => void;
}

export function useLoop(svg: Ref<SVGSVGElement | undefined>, diagram: Diagram) {
  let frame = 0;
  let observer: IntersectionObserver | undefined;

  onMounted(() => {
    const s = svg.value!;
    const render = diagram.build(s);
    if (matchMedia('(prefers-reduced-motion: reduce)').matches) {
      render(diagram.still);
      return;
    }
    let visible = true;
    observer = new IntersectionObserver(([entry]) => (visible = entry.isIntersecting));
    observer.observe(s);
    let last = performance.now();
    let elapsed = 0;
    const tick = (now: number) => {
      if (visible && !document.hidden) {
        elapsed += (now - last) / 1000;
        const u = mod(elapsed, diagram.loop);
        render(u);
        s.style.opacity = String(Math.min(span(u, 0, 0.3), 1 - span(u, diagram.loop - 0.3, diagram.loop)));
      }
      last = now;
      frame = requestAnimationFrame(tick);
    };
    frame = requestAnimationFrame(tick);
  });

  onBeforeUnmount(() => {
    cancelAnimationFrame(frame);
    observer?.disconnect();
  });
}
