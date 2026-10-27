#!/usr/bin/env node
// Render an explainer to MP4, frame by frame (deterministic: every frame is a pure function of t).
// Needs Node >= 22 (built-in WebSocket), Google Chrome or Chromium, and ffmpeg. No npm packages.
//
//   node tools/export-explainer.mjs --name chaos --theme dark --out chaos-dark.mp4
//   node tools/export-explainer.mjs --name continuum --theme light --duration 5 --out smoke.mp4
//   node tools/export-explainer.mjs --name continuum --voice f --out continuum-narrated.mp4  # with narration + music
//   node tools/export-explainer.mjs --name continuum --layers bg --out bg.mov        # picture only, no text/panels (ProRes)
//   node tools/export-explainer.mjs --name continuum --layers panels --out panels.mov # panels only, transparent (ProRes 4444)
//
// The control plane must be running (./run.sh up): the chaos explainer uses the running cluster's regions.
import { spawn } from 'node:child_process';
import { existsSync, readFileSync, unlinkSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

const args = Object.fromEntries(process.argv.slice(2).reduce((acc, a, i, all) => {
  if (a.startsWith('--')) acc.push([a.slice(2), all[i + 1] && !all[i + 1].startsWith('--') ? all[i + 1] : 'true']);
  return acc;
}, []));
const name = args.name || 'chaos';
const theme = args.theme || 'dark';
const base = (args.url || 'http://localhost:8088').replace(/\/$/, '');
const fps = Number(args.fps || 30);
const out = args.out || `${name}-${theme}.mp4`;
const LENGTHS = { chaos: 133, continuum: 162 };
if (!LENGTHS[name]) { console.error('--name must be chaos or continuum'); process.exit(2); }
// Narration: frames are rendered through the explainer's own time remap, so the picture slows where the voice needs
// room exactly as it does in the browser; the clips and the underscore are mixed in afterwards.
const voice = args.voice && args.voice !== 'off' ? args.voice[0] : null;
const audioDir = new URL('../web/explainers/audio/', import.meta.url).pathname;
const manifestPath = `${audioDir}${name}-narration.json`;
const manifest = voice && existsSync(manifestPath) ? JSON.parse(readFileSync(manifestPath, 'utf8')) : null;
if (voice && !(manifest && manifest.voices[voice])) { console.error(`no narration for voice '${voice}' (${manifestPath})`); process.exit(2); }
let duration = Number(args.duration || LENGTHS[name]);

const CHROMES = [args.chrome, process.env.CHROME,
  '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome', '/usr/bin/google-chrome', '/usr/bin/chromium', '/usr/bin/chromium-browser'];
const chromePath = CHROMES.find((p) => p && existsSync(p));
if (!chromePath) { console.error('Chrome not found: pass --chrome PATH'); process.exit(2); }

const chrome = spawn(chromePath, ['--headless=new', '--remote-debugging-port=0', '--hide-scrollbars', '--force-device-scale-factor=1',
  `--user-data-dir=${join(tmpdir(), `explainer-export-${process.pid}`)}`, 'about:blank'], { stdio: ['ignore', 'ignore', 'pipe'] });
const wsUrl = await new Promise((resolve, reject) => {
  let buf = '';
  chrome.stderr.on('data', (d) => { buf += d; const m = buf.match(/DevTools listening on (ws:\/\/\S+)/); if (m) resolve(m[1]); });
  chrome.on('exit', () => reject(new Error('Chrome exited early')));
});

const ws = new WebSocket(wsUrl);
await new Promise((r) => ws.addEventListener('open', r, { once: true }));
let nextId = 1;
const pending = new Map();
ws.addEventListener('message', (e) => {
  const msg = JSON.parse(e.data);
  if (msg.id && pending.has(msg.id)) { const { resolve, reject } = pending.get(msg.id); pending.delete(msg.id); msg.error ? reject(new Error(msg.error.message)) : resolve(msg.result); }
});
const send = (method, params = {}, sessionId) => new Promise((resolve, reject) => {
  const id = nextId++;
  pending.set(id, { resolve, reject });
  ws.send(JSON.stringify({ id, method, params, sessionId }));
});

const { targetId } = await send('Target.createTarget', { url: 'about:blank' });
const { sessionId } = await send('Target.attachToTarget', { targetId, flatten: true });
const s = (m, p) => send(m, p, sessionId);
await s('Emulation.setDeviceMetricsOverride', { width: 1920, height: 1080, deviceScaleFactor: 1, mobile: false });
if ((args.layers || null) === 'panels') await s('Emulation.setDefaultBackgroundColorOverride', { color: { r: 0, g: 0, b: 0, a: 0 } });
await s('Page.enable');
const layers = args.layers || null;  // bg | panels: separable layers for an editing package
await s('Page.navigate', { url: `${base}/explainer/${name}?capture=1&theme=${theme}${layers ? `&layers=${layers}` : ''}` });
let ready = false;
for (let i = 0; i < 100 && !ready; i++) {
  const { result } = await s('Runtime.evaluate', { expression: "typeof window.renderAt === 'function' && document.fonts.status === 'loaded'", returnByValue: true });
  ready = result.value === true;
  if (!ready) await new Promise((r) => setTimeout(r, 200));
}
if (!ready) {
  console.error(`${base}/explainer/${name} did not load. Is the control plane running (./run.sh up)?`);
  chrome.kill();
  process.exit(1);
}
await new Promise((r) => setTimeout(r, 800));  // logo particles and land mask finish preparing
// With a voice: ask the page for its remapped length, and a scene time for every real-time frame.
let sceneTimes = null;
if (voice) {
  const { result } = await s('Runtime.evaluate', { expression: `(() => { if (typeof setVoice !== 'function') return null; setVoice('${voice}');
    const times = []; let t = 0; while (t < T_END) { times.push(t); t = advance(t, 1 / ${fps}); } times.push(T_END); return times; })()`, returnByValue: true });
  if (!result.value) { console.error('this explainer has no narration support'); process.exit(2); }
  sceneTimes = result.value;
  if (!args.duration) duration = sceneTimes.length / fps;
}

const videoOut = voice ? `${out}.video.mp4` : out;
const prores = out.endsWith('.mov');
const codec = prores
  ? ['-c:v', 'prores_ks', '-profile:v', layers === 'panels' ? '4444' : '3', '-pix_fmt', layers === 'panels' ? 'yuva444p10le' : 'yuv422p10le']
  : ['-c:v', 'libx264', '-pix_fmt', 'yuv420p', '-preset', 'slow', '-crf', '18', '-movflags', '+faststart'];
const ffmpeg = spawn('ffmpeg', ['-y', '-loglevel', 'error', '-f', 'image2pipe', '-framerate', String(fps), '-i', '-', ...codec, videoOut], { stdio: ['pipe', 'inherit', 'inherit'] });
const frames = Math.round(duration * fps);
for (let f = 0; f < frames; f++) {
  const sceneT = sceneTimes ? sceneTimes[Math.min(f, sceneTimes.length - 1)] : f / fps;
  await s('Runtime.evaluate', { expression: `window.renderAt(${sceneT.toFixed(4)})` });
  const { data } = await s('Page.captureScreenshot', layers === 'panels' ? { format: 'png', fromSurface: true } : { format: 'jpeg', quality: 95 });
  if (!ffmpeg.stdin.write(Buffer.from(data, 'base64'))) await new Promise((r) => ffmpeg.stdin.once('drain', r));
  if (f % fps === 0) process.stdout.write(`\r${name} ${theme}: ${Math.round(100 * f / frames)}%`);
}
ffmpeg.stdin.end();
await new Promise((r) => ffmpeg.on('exit', r));
if (voice) {
  // Mix: each clip placed where its chapter starts in *real* time, the underscore ducked while anyone speaks.
  const realTimeOf = (sceneT) => { const i = sceneTimes.findIndex((x) => x >= sceneT); return (i < 0 ? sceneTimes.length - 1 : i) / fps; };
  const chapters = Object.values(manifest.voices[voice].chapters);
  const inputs = ['-i', videoOut, '-i', `${audioDir}${name}-underscore.mp3`];
  const filters = [];
  let speech = '';
  chapters.forEach((ch, i) => {
    inputs.push('-i', `${audioDir}${ch.file}`);
    filters.push(`[${i + 2}:a]adelay=${Math.round(realTimeOf(ch.start) * 1000)}|${Math.round(realTimeOf(ch.start) * 1000)},apad[v${i}]`);
    speech += `[v${i}]`;
  });
  // ducking: lower the music while the (summed) speech is above a quiet threshold
  filters.push(`${speech}amix=inputs=${chapters.length}:duration=longest:normalize=0[voice]`);
  filters.push(`[voice]asplit=2[voice_out][voice_side]`);
  filters.push(`[1:a]volume=0.6[music]`);
  filters.push(`[music][voice_side]sidechaincompress=threshold=0.02:ratio=6:attack=80:release=600:makeup=1[ducked]`);
  filters.push(`[ducked][voice_out]amix=inputs=2:duration=first:normalize=0,atrim=0:${duration.toFixed(2)},afade=t=out:st=${(duration - 2).toFixed(2)}:d=2[a]`);
  const mix = spawn('ffmpeg', ['-y', '-loglevel', 'error', ...inputs, '-filter_complex', filters.join(';'), '-map', '0:v', '-map', '[a]',
    '-c:v', 'copy', '-c:a', 'aac', '-b:a', '192k', '-shortest', '-movflags', '+faststart', out], { stdio: ['ignore', 'inherit', 'inherit'] });
  const code = await new Promise((r) => mix.on('exit', r));
  if (code !== 0) { console.error('audio mix failed; the silent video is at', videoOut); process.exit(1); }
  unlinkSync(videoOut);
}
process.stdout.write(`\rwrote ${out} (${duration.toFixed(1)}s at ${fps} fps${voice ? `, voice ${manifest.voices[voice].name} + music` : ''})\n`);
ws.close();
chrome.kill();
