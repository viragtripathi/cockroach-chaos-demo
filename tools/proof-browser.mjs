#!/usr/bin/env node
// Headless-Chrome driver for the proof run: executes a JSON plan of steps and prints JSON results.
//   node tools/proof-browser.mjs plan.json
// Steps: open {url,width,height,mobile}, wait {ms}, eval {expr,as}, waitFor {expr,timeout,as}, key {key},
//        click {selector}, hover {selector}, move {x,y}, shot {file}.
// Needs Node >= 22 (built-in WebSocket) and Google Chrome or Chromium. No npm packages.
import { spawn } from 'node:child_process';
import { existsSync, readFileSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

const plan = JSON.parse(readFileSync(process.argv[2], 'utf8'));
const CHROMES = [process.env.CHROME, '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',
  '/usr/bin/google-chrome', '/usr/bin/chromium', '/usr/bin/chromium-browser'];
const chromePath = CHROMES.find((p) => p && existsSync(p));
if (!chromePath) { console.log(JSON.stringify({ error: 'Chrome not found (set CHROME=/path/to/chrome)' })); process.exit(0); }

const chrome = spawn(chromePath, ['--headless=new', '--remote-debugging-port=0', '--hide-scrollbars',
  `--user-data-dir=${join(tmpdir(), `proof-browser-${process.pid}`)}`, 'about:blank'], { stdio: ['ignore', 'ignore', 'pipe'] });
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
  const m = JSON.parse(e.data);
  if (m.id && pending.has(m.id)) { const { resolve, reject } = pending.get(m.id); pending.delete(m.id); m.error ? reject(new Error(m.error.message)) : resolve(m.result); }
});
const send = (method, params = {}, sessionId) => new Promise((resolve, reject) => {
  const id = nextId++; pending.set(id, { resolve, reject }); ws.send(JSON.stringify({ id, method, params, sessionId }));
});
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

const results = {};
let session = null;
const evaluate = async (expr) => {
  const r = await send('Runtime.evaluate', { expression: expr, returnByValue: true, awaitPromise: true }, session);
  if (r.exceptionDetails) throw new Error(r.exceptionDetails.exception?.description || 'evaluation failed');
  return r.result.value;
};
const center = async (selector) => {
  const v = await evaluate(`(() => { const t = document.querySelector(${JSON.stringify(selector)}); if (!t) return null;
    t.scrollIntoView({block: 'center'}); const r = t.getBoundingClientRect(); return [r.left + r.width / 2, r.top + r.height / 2]; })()`);
  if (!v) throw new Error(`no element matches ${selector}`);
  return v;
};

try {
  for (const step of plan) {
    if (step.do === 'open') {
      const { targetId } = await send('Target.createTarget', { url: 'about:blank' });
      session = (await send('Target.attachToTarget', { targetId, flatten: true })).sessionId;
      await send('Emulation.setDeviceMetricsOverride', { width: step.width || 1920, height: step.height || 1080,
        deviceScaleFactor: 1, mobile: Boolean(step.mobile) }, session);
      await send('Page.navigate', { url: step.url }, session);
      await sleep(step.settle ?? 3500);
    } else if (step.do === 'wait') {
      await sleep(step.ms);
    } else if (step.do === 'eval') {
      results[step.as] = await evaluate(step.expr);
    } else if (step.do === 'waitFor') {
      const deadline = Date.now() + (step.timeout || 10000);
      let value = null;
      while (Date.now() < deadline) { value = await evaluate(step.expr); if (value) break; await sleep(250); }
      results[step.as] = value;
    } else if (step.do === 'key') {
      const key = step.key;
      const code = key.length === 1 ? (/[0-9]/.test(key) ? `Digit${key}` : `Key${key.toUpperCase()}`) : key;
      await send('Input.dispatchKeyEvent', { type: 'keyDown', key, code, text: key.length === 1 ? key : undefined,
        windowsVirtualKeyCode: key === 'Escape' ? 27 : key.toUpperCase().charCodeAt(0) }, session);
      await send('Input.dispatchKeyEvent', { type: 'keyUp', key, code }, session);
      await sleep(step.settle ?? 400);
    } else if (step.do === 'click' || step.do === 'hover') {
      const [x, y] = await center(step.selector);
      await send('Input.dispatchMouseEvent', { type: 'mouseMoved', x, y }, session);
      if (step.do === 'click') {
        await send('Input.dispatchMouseEvent', { type: 'mousePressed', x, y, button: 'left', clickCount: 1 }, session);
        await send('Input.dispatchMouseEvent', { type: 'mouseReleased', x, y, button: 'left', clickCount: 1 }, session);
      }
      await sleep(step.settle ?? 600);
    } else if (step.do === 'move') {
      await send('Input.dispatchMouseEvent', { type: 'mouseMoved', x: step.x, y: step.y }, session);
      await sleep(step.settle ?? 300);
    } else if (step.do === 'shot') {
      const { data } = await send('Page.captureScreenshot', { format: 'png' }, session);
      writeFileSync(step.file, Buffer.from(data, 'base64'));
    }
  }
} catch (e) {
  results.error = String(e.message || e);
}
console.log(JSON.stringify(results));
ws.close();
chrome.kill();
process.exit(0);
