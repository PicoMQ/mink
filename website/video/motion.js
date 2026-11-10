const NS = "http://www.w3.org/2000/svg";
const $ = (id) => document.getElementById(id);
const clamp = (x) => Math.min(1, Math.max(0, x));
const out = (x) => 1 - Math.pow(1 - clamp(x), 4);
const inOut = (x) => { x = clamp(x); return x < 0.5 ? 8 * x ** 4 : 1 - Math.pow(-2 * x + 2, 4) / 2; };
const span = (t, a, b, f = out) => f((t - a) / (b - a));
const lerp = (a, b, p) => a + (b - a) * p;
const place = (el, opacity, dy = 0, scale = 1, blur = 0) => {
  el.style.opacity = opacity;
  el.style.transform = `translateY(${dy}px) scale(${scale})`;
  el.style.filter = blur > 0.05 ? `blur(${blur}px)` : "none";
};
const NEVER = 1e9;
const enter = (el, t, a, leave = NEVER, dy = 50, from = 1) => {
  const p = span(t, a, a + 0.6), q = span(t, leave, leave + 0.35);
  place(el, p * (1 - q), (1 - p) * dy - q * 70, lerp(from, 1, p), (1 - p) * 10 + q * 14);
};
const draw = (path, p) => {
  path.style.strokeDasharray = "1 1";
  path.style.strokeDashoffset = String(1 - p);
};

const svg = (tag, attrs, parent) => {
  const el = document.createElementNS(NS, tag);
  for (const [k, v] of Object.entries(attrs)) el.setAttribute(k, v);
  parent.appendChild(el);
  return el;
};
const logo = (rect, stroke) => {
  const node = document.createElementNS(NS, "svg");
  node.setAttribute("viewBox", "0 0 11 11");
  svg("rect", { width: 11, height: 11, ...(rect && { fill: rect }) }, node);
  svg("path", { fill: "none", "stroke-width": 1, "stroke-linejoin": "bevel", ...(stroke && { stroke }),
    d: "M3.84 1.5 L1.5 3.84 L1.5 6.67 L6.67 1.5 L9.5 1.5 L1.5 9.5 L4.33 9.5 L9.5 4.33 L9.5 7.16 L7.16 9.5" }, node);
  return node;
};

const value = (v, t) => (typeof v === "function" ? v(t) : v);
let beams = 0;
const link = (spec, into = $("wires")) => {
  const id = `beam${beams++}`;
  const grad = svg("linearGradient", { id, gradientUnits: "userSpaceOnUse" }, into.querySelector("defs"));
  const color = spec.color ?? "var(--ink-1)";
  grad.innerHTML = `<stop offset="0" style="stop-color: ${color}; stop-opacity: 0"/>` +
    `<stop offset="0.85" style="stop-color: ${color}; stop-opacity: 0.9"/><stop offset="1" style="stop-color: ${color}; stop-opacity: 0"/>`;
  const g = svg("g", {}, into.querySelector("g"));
  const base = svg("path", { class: "wire" }, g);
  const beam = svg("path", { class: "beam", stroke: `url(#${id})` }, g);
  const ports = [0, 1].map(() => svg("circle", { class: "port", r: 5 }, g));
  (into.links ??= []).push({ period: 1.5, offset: 0, ...spec, grad, g, base, beam, ports });
};
const shape = ([ax, ay], [bx, by], curve) => {
  if (curve === "h") return `M${ax} ${ay} C${(ax + bx) / 2} ${ay} ${(ax + bx) / 2} ${by} ${bx} ${by}`;
  if (curve === "v") return `M${ax} ${ay} C${ax} ${(ay + by) / 2} ${bx} ${(ay + by) / 2} ${bx} ${by}`;
  return `M${ax} ${ay} L${bx} ${by}`;
};
const renderLinks = (t, from = $("wires")) => {
  for (const l of from.links) {
    const a = value(l.a, t), b = value(l.b, t);
    const d = shape(a, b, l.curve);
    l.base.setAttribute("d", d);
    l.beam.setAttribute("d", d);
    l.g.style.opacity = l.show ? l.show(t) : 1;
    const reveal = l.drawIn ? span(t, l.drawIn[0], l.drawIn[1], inOut) : 1;
    if (l.dashed && value(l.dashed, t)) {
      l.base.removeAttribute("pathLength");
      l.base.style.strokeDasharray = "6 8";
      l.base.style.strokeDashoffset = "0";
    } else {
      l.base.setAttribute("pathLength", 1);
      draw(l.base, reveal);
    }
    l.ports[0].setAttribute("cx", a[0]); l.ports[0].setAttribute("cy", a[1]);
    l.ports[1].setAttribute("cx", b[0]); l.ports[1].setAttribute("cy", b[1]);
    l.ports[0].style.opacity = reveal > 0 ? 1 : 0;
    l.ports[1].style.opacity = reveal > 0.97 ? 1 : 0;
    const length = 0.45;
    const u = ((t / l.period + l.offset) % 1 + 1) % 1;
    const head = lerp(-0.05, 1 + length, u);
    const tail = head - length;
    l.grad.setAttribute("x1", lerp(a[0], b[0], tail)); l.grad.setAttribute("y1", lerp(a[1], b[1], tail));
    l.grad.setAttribute("x2", lerp(a[0], b[0], head)); l.grad.setAttribute("y2", lerp(a[1], b[1], head));
    l.beam.style.opacity = (l.flowing ? l.flowing(t) : 0) * (reveal > 0.97 ? 1 : 0);
  }
};
