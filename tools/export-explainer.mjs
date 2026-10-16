#!/usr/bin/env node
// Render an explainer to MP4, frame by frame (deterministic: every frame is a pure function of t).
// Needs Node >= 22 (built-in WebSocket), Google Chrome or Chromium, and ffmpeg. No npm packages.
//
//   node tools/export-explainer.mjs --name chaos --theme dark --out chaos-dark.mp4
//   node tools/export-explainer.mjs --name chaos --theme light --duration 5 --out smoke.mp4
//
// The control plane must be running (./run.sh up): the chaos explainer uses the running cluster's regions.
import { spawn } from 'node:child_process';
import { existsSync } from 'node:fs';
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
const duration = Number(args.duration || LENGTHS[name]);
if (!LENGTHS[name]) { console.error('--name must be chaos or continuum'); process.exit(2); }

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
await s('Page.enable');
await s('Page.navigate', { url: `${base}/explainer/${name}?capture=1&theme=${theme}` });
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

const ffmpeg = spawn('ffmpeg', ['-y', '-loglevel', 'error', '-f', 'image2pipe', '-framerate', String(fps), '-i', '-',
  '-c:v', 'libx264', '-pix_fmt', 'yuv420p', '-preset', 'slow', '-crf', '18', '-movflags', '+faststart', out], { stdio: ['pipe', 'inherit', 'inherit'] });
const frames = Math.round(duration * fps);
for (let f = 0; f < frames; f++) {
  await s('Runtime.evaluate', { expression: `window.renderAt(${(f / fps).toFixed(4)})` });
  const { data } = await s('Page.captureScreenshot', { format: 'jpeg', quality: 95 });
  if (!ffmpeg.stdin.write(Buffer.from(data, 'base64'))) await new Promise((r) => ffmpeg.stdin.once('drain', r));
  if (f % fps === 0) process.stdout.write(`\r${name} ${theme}: ${Math.round(100 * f / frames)}%`);
}
ffmpeg.stdin.end();
await new Promise((r) => ffmpeg.on('exit', r));
process.stdout.write(`\rwrote ${out} (${duration}s at ${fps} fps)\n`);
ws.close();
chrome.kill();
