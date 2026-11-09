const studio = (length) => {
  const RATE = 48000;
  const BEAT = 0.6;
  const ctx = new OfflineAudioContext(2, Math.ceil(length * RATE), RATE);
  let seed = 42;
  const random = () => {
    seed = (seed + 0x6d2b79f5) | 0;
    let r = Math.imul(seed ^ (seed >>> 15), 1 | seed);
    r = (r + Math.imul(r ^ (r >>> 7), 61 | r)) ^ r;
    return ((r ^ (r >>> 14)) >>> 0) / 4294967296;
  };

  const BAR = BEAT * 4;
  const LEVEL = { kick: 1, sub: 0.08, bass: 0.34, hat: 0.85, open: 0.63, clap: 4.8, blip: 0.17, fx: 1.1 };

  const noise = ctx.createBuffer(1, RATE * 2, RATE);
  noise.getChannelData(0).forEach((_, i, data) => (data[i] = random() * 2 - 1));
  const envelope = (at, attack, level, hold, release) => {
    const peak = Math.max(level, 0.0001);
    const g = ctx.createGain();
    g.gain.setValueAtTime(0.0001, at);
    g.gain.exponentialRampToValueAtTime(peak, at + attack);
    g.gain.setValueAtTime(peak, at + attack + hold);
    g.gain.exponentialRampToValueAtTime(0.0001, at + attack + hold + release);
    return g;
  };
  const tone = (type, freq, at, length) => {
    const o = ctx.createOscillator();
    o.type = type;
    o.frequency.setValueAtTime(freq, at);
    o.start(at);
    o.stop(at + length);
    return o;
  };
  const hiss = (at, length) => {
    const n = ctx.createBufferSource();
    n.buffer = noise;
    n.loop = true;
    n.start(at, random() * 1.5);
    n.stop(at + length);
    return n;
  };
  const filter = (type, freq, q = 0.7) => {
    const f = ctx.createBiquadFilter();
    f.type = type;
    f.frequency.value = freq;
    f.Q.value = q;
    return f;
  };

  const master = ctx.createGain();
  const compressor = ctx.createDynamicsCompressor();
  compressor.threshold.value = -12;
  compressor.knee.value = 10;
  compressor.ratio.value = 3;
  compressor.attack.value = 0.004;
  compressor.release.value = 0.18;
  master.connect(compressor).connect(ctx.destination);
  master.gain.setValueAtTime(1, 0);
  master.gain.setValueAtTime(1, length - 3);
  master.gain.linearRampToValueAtTime(0, length - 0.1);
  const music = ctx.createGain();
  music.connect(master);

  const space = ctx.createConvolver();
  const impulse = ctx.createBuffer(2, RATE * 0.5, RATE);
  for (let c = 0; c < 2; c++) {
    impulse.getChannelData(c).forEach((_, i, data) => (data[i] = (random() * 2 - 1) * (1 - i / data.length) ** 6));
  }
  space.buffer = impulse;
  const wet = ctx.createGain();
  wet.gain.value = 0.12;
  space.connect(wet).connect(master);

  const bounce = [ctx.createDelay(1), ctx.createDelay(1)];
  bounce.forEach((d) => (d.delayTime.value = BEAT * 0.75));
  const feedback = ctx.createGain();
  feedback.gain.value = 0.38;
  bounce[0].connect(bounce[1]);
  bounce[1].connect(filter("lowpass", 1800)).connect(feedback).connect(bounce[0]);
  const merger = ctx.createChannelMerger(2);
  bounce[0].connect(merger, 0, 0);
  bounce[1].connect(merger, 0, 1);
  const echoes = ctx.createGain();
  echoes.gain.value = 0.3;
  merger.connect(echoes).connect(music);

  const send = (node, { bus = master, verb = 0, echo = 0, pan = 0 } = {}) => {
    const panner = ctx.createStereoPanner();
    panner.pan.value = pan;
    node.connect(panner).connect(bus);
    if (verb) { const v = ctx.createGain(); v.gain.value = verb; panner.connect(v).connect(space); }
    if (echo) { const e = ctx.createGain(); e.gain.value = echo; panner.connect(e).connect(bounce[0]); }
    return panner;
  };

  const kick = (at, level) => {
    const o = tone("sine", 160, at, 0.4);
    o.frequency.exponentialRampToValueAtTime(45, at + 0.05);
    const g = envelope(at, 0.002, level, 0.02, 0.24);
    o.connect(g).connect(master);
    const click = envelope(at, 0.0005, level * 0.35, 0.002, 0.01);
    hiss(at, 0.03).connect(filter("highpass", 3500)).connect(click).connect(master);
    music.gain.setValueAtTime(0.35, at);
    music.gain.linearRampToValueAtTime(1, at + 0.28);
  };
  const shaper = ctx.createWaveShaper();
  shaper.curve = new Float32Array(1024).map((_, i) => Math.tanh((i / 511.5 - 1) * 2.5));
  shaper.connect(music);
  const sub = (freq, at, length, level) => {
    const o = tone("sine", freq, at, length + 0.15);
    const g = envelope(at, 0.01, level, Math.max(0, length - 0.08), 0.08);
    o.connect(g).connect(shaper);
  };
  const bass = (freq, at, level, cutoff) => {
    const f = filter("lowpass", cutoff, 4);
    f.frequency.setValueAtTime(cutoff, at);
    f.frequency.exponentialRampToValueAtTime(Math.max(120, cutoff * 0.22), at + 0.09);
    tone("sawtooth", freq, at, 0.2).connect(f);
    const g = envelope(at, 0.002, level, 0.03, 0.08);
    f.connect(g).connect(music);
  };
  const hat = (at, level, open) => {
    const g = envelope(at, 0.001, level, 0.002, open ? 0.14 : 0.025);
    const band = filter("bandpass", 10000, 1.2);
    for (const f of [205.3, 304.4, 369.6, 522.7, 540, 800]) tone("square", f * 1.6, at, 0.3).connect(band);
    band.connect(filter("highpass", 7500)).connect(g);
    send(g, { bus: music, pan: 0.2 });
  };
  const clap = (at, level) => {
    for (const [offset, decay] of [[0, 0.015], [0.008, 0.015], [0.016, 0.06]]) {
      const g = envelope(at + offset, 0.001, level, 0.002, decay);
      hiss(at + offset, 0.12).connect(filter("bandpass", 2200, 1)).connect(filter("highpass", 1000)).connect(g);
      send(g, { bus: music, verb: 0.25, pan: -0.1 });
    }
  };
  const blip = (freq, at, level, pan) => {
    const f = filter("lowpass", 2600, 2);
    f.frequency.setValueAtTime(2600, at);
    f.frequency.exponentialRampToValueAtTime(700, at + 0.08);
    tone("square", freq, at, 0.2).connect(f);
    const g = envelope(at, 0.002, level, 0.01, 0.07);
    f.connect(g);
    send(g, { bus: music, verb: 0.3, echo: 0.6, pan });
  };

  const bell = (freq, at, level, decay = 2) => {
    const carrier = tone("sine", freq, at, decay + 0.5);
    const modulator = tone("sine", freq * 3.5, at, decay + 0.5);
    const depth = ctx.createGain();
    depth.gain.setValueAtTime(freq * 2.2, at);
    depth.gain.exponentialRampToValueAtTime(freq * 0.05, at + decay);
    modulator.connect(depth).connect(carrier.frequency);
    const g = envelope(at, 0.002, level * LEVEL.fx, 0, decay);
    carrier.connect(g);
    send(g, { verb: 0.4, echo: 0.2, pan: (random() - 0.5) * 0.8 });
  };
  const droplet = (at, level) => {
    const o = tone("sine", 1500, at, 0.25);
    o.frequency.exponentialRampToValueAtTime(650, at + 0.06);
    const g = envelope(at, 0.002, level * LEVEL.fx, 0.01, 0.12);
    o.connect(g);
    send(g, { verb: 0.4, echo: 0.25, pan: (random() - 0.5) * 0.6 });
  };
  const tick = (at, level) => {
    const g = envelope(at, 0.001, level * LEVEL.fx, 0.004, 0.03);
    hiss(at, 0.06).connect(filter("bandpass", 3600, 4)).connect(g);
    send(g, { pan: (random() - 0.5) * 0.4 });
  };
  const thud = (at, level) => {
    const o = tone("sine", 75, at, 1);
    o.frequency.exponentialRampToValueAtTime(36, at + 0.25);
    const g = envelope(at, 0.004, level * LEVEL.fx, 0.05, 0.6);
    o.connect(g);
    send(g, { verb: 0.3 });
  };

  const wav = async () => {
    const rendered = await ctx.startRendering();
    const frames = rendered.length;
    const view = new DataView(new ArrayBuffer(44 + frames * 4));
    const text = (offset, value) => [...value].forEach((c, i) => view.setUint8(offset + i, c.charCodeAt(0)));
    text(0, "RIFF"); view.setUint32(4, 36 + frames * 4, true); text(8, "WAVE"); text(12, "fmt ");
    view.setUint32(16, 16, true); view.setUint16(20, 1, true); view.setUint16(22, 2, true);
    view.setUint32(24, RATE, true); view.setUint32(28, RATE * 4, true); view.setUint16(32, 4, true); view.setUint16(34, 16, true);
    text(36, "data"); view.setUint32(40, frames * 4, true);
    const left = rendered.getChannelData(0), right = rendered.getChannelData(1);
    let energy = 0;
    for (let i = 0; i < frames; i++) energy += left[i] ** 2 + right[i] ** 2;
    const gain = 0.14 / Math.sqrt(energy / (frames * 2));
    for (let i = 0; i < frames; i++) {
      view.setInt16(44 + i * 4, Math.tanh(left[i] * gain) * 32767, true);
      view.setInt16(46 + i * 4, Math.tanh(right[i] * gain) * 32767, true);
    }
    const bytes = new Uint8Array(view.buffer);
    let binary = "";
    for (let i = 0; i < bytes.length; i += 0x8000) binary += String.fromCharCode(...bytes.subarray(i, i + 0x8000));
    return btoa(binary);
  };

  return { BEAT, BAR, LEVEL, random, kick, sub, bass, hat, clap, blip, bell, droplet, tick, thud, wav };
};
