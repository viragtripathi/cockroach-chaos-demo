// Stage view (layout B): live cluster state from one event stream, drawn at 1920×1080 and scaled to the screen.
import { GLOSSARY as G } from '/static/shared/glossary.js';
import { initTips, tip } from '/static/shared/tips.js';

const $ = (id) => document.getElementById(id);
const THEME_KEY = 'crdb-stage-theme';
const REGION_COLORS = ['--brand', '--info', '--taupe', '--healthy', '--degraded', '--brand-text', '--muted'];
const PROVISIONING = new Set(['provision', 'rebuild', 'adopt']);
const ACTION_WORDS = { crash: 'Crash', stop: 'Planned stop', partition: 'Partition', brownout: 'Brownout', isolate: 'Isolate', outage: 'Outage', recover: 'Recover' };

let state = null;
let canControl = false;
let lastLiveness = new Map();
let menuFor = null;
let pressing = false;  // don't swap the region cards out from under a click in progress

function el(tag, className, text) {
  const node = document.createElement(tag);
  if (className) node.className = className;
  if (text !== undefined && text !== null) node.textContent = text;
  return node;
}
const fmt = (n) => Math.round(n).toLocaleString('en-US');
const cssVar = (name) => getComputedStyle(document.documentElement).getPropertyValue(name).trim();

/* ---------- frame, theme, clock ---------- */

function fit() {
  const s = Math.min(innerWidth / 1920, innerHeight / 1080);
  $('stage').style.transform = `scale(${s})`;
  drawChart();
}

function applyTheme(theme) {
  document.documentElement.dataset.theme = theme;
  $('theme').textContent = theme === 'dark' ? '☀' : '☾';
  drawChart();
}

function initTheme() {
  const fromUrl = new URLSearchParams(location.search).get('theme');
  applyTheme(fromUrl || localStorage.getItem(THEME_KEY) || 'dark');
  $('theme').addEventListener('click', toggleTheme);
}

function toggleTheme() {
  const next = document.documentElement.dataset.theme === 'dark' ? 'light' : 'dark';
  localStorage.setItem(THEME_KEY, next);
  applyTheme(next);
}

function tickClock() {
  const d = new Date();
  $('clock').textContent = `${d.toISOString().slice(11, 19)} UTC`;
}

/* ---------- API ---------- */

async function api(method, path, body) {
  const resp = await fetch(path, {
    method, credentials: 'same-origin',
    headers: body ? { 'Content-Type': 'application/json' } : {},
    body: body ? JSON.stringify(body) : undefined,
  });
  let data = null;
  try { data = await resp.json(); } catch (e) { /* empty body */ }
  return { ok: resp.ok, status: resp.status, data };
}

async function refreshControl() {
  const r = await api('GET', '/api/system');
  canControl = Boolean(r.ok && r.data && r.data.authorized);
  document.body.classList.toggle('can-control', canControl);
  if (state) render(state);
}

function toast(text, ok = false) {
  const t = $('toast');
  t.textContent = text;
  t.className = ok ? 'toast ok' : 'toast';
  t.hidden = false;
  clearTimeout(toast.timer);
  toast.timer = setTimeout(() => { t.hidden = true; }, 4200);
}

function explain(r) {
  if (r.status === 401) return 'Locked: open the presenter link with the control token';
  const d = r.data && r.data.detail;
  return typeof d === 'string' ? d : `Request failed (${r.status})`;
}

// Actions resolve to true (done), false (failed) or null (cancelled) so buttons can show the outcome.
async function chaos(action, kind, value, extra = {}) {
  closeMenu();
  let r = await api('POST', '/api/chaos', { action, kind, value, ...extra });
  if (r.status === 409 && r.data && r.data.detail && r.data.detail.needs_confirm) {
    const go = await confirmDialog(r.data.detail.warning);
    if (!go) return null;
    r = await api('POST', '/api/chaos', { action, kind, value, confirm: true, ...extra });
  }
  if (!r.ok) { toast(explain(r)); return false; }
  if (r.data && r.data.warnings && r.data.warnings.length) toast(r.data.warnings[0]);
  else toast(`${ACTION_WORDS[action] || action} · ${kind === 'all' ? 'everything' : value}`, true);
  return true;
}

async function post(path, body, okText) {
  closeMenu();
  const r = await api('POST', path, body);
  if (!r.ok) { toast(explain(r)); return false; }
  if (okText) toast(okText, true);
  return true;
}

async function del(path, okText) {
  closeMenu();
  const r = await api('DELETE', path);
  if (!r.ok) { toast(explain(r)); return false; }
  if (okText) toast(okText, true);
  return true;
}

/* ---------- button feedback that survives the once-a-second re-render ---------- */

const feedback = new Map();  // key -> 'working' | 'done' | 'failed'

function applyFeedback() {
  document.querySelectorAll('[data-fb]').forEach((node) => {
    const state = feedback.get(node.dataset.fb);
    node.classList.remove('working', 'done', 'failed');
    if (state) node.classList.add(state);
  });
}

async function press(key, fn) {
  if (feedback.get(key) === 'working') return;
  feedback.set(key, 'working');
  applyFeedback();
  let outcome = false;
  try { outcome = await fn(); } catch (e) { toast(String(e)); }
  if (outcome === null) feedback.delete(key);
  else feedback.set(key, outcome ? 'done' : 'failed');
  applyFeedback();
  setTimeout(() => { if (feedback.get(key) !== 'working') { feedback.delete(key); applyFeedback(); } }, 1400);
}

function confirmDialog(text) {
  return new Promise((resolve) => {
    $('confirm-text').textContent = text;
    $('confirm').hidden = false;
    const done = (v) => { $('confirm').hidden = true; $('confirm-ok').onclick = $('confirm-cancel').onclick = null; resolve(v); };
    $('confirm-ok').onclick = () => done(true);
    $('confirm-cancel').onclick = () => done(false);
    confirmDialog.cancel = () => done(false);
  });
}

/* ---------- node state ---------- */

function nodeLook(node, op) {
  if (node.runtime === 'missing') return ['missing', 'allocating'];
  if (op && op.status === 'running' && (op.kind === 'decommission' || op.kind === 'remove-region') &&
      op.steps.some((s) => s.includes(node.name) || (op.kind === 'remove-region' && s.includes(node.region)))) {
    return ['draining', 'draining'];
  }
  if (node.partitioned) return ['partitioned', 'partitioned'];
  if (node.isolated) return ['isolated', 'isolated'];
  if (node.runtime === 'stopped' || node.liveness === 'dead') return ['dead', 'down'];
  if (node.liveness === 'joining') return ['joining', 'joining'];
  if (node.liveness === 'unknown') return ['unknown', 'telemetry delayed'];
  if (node.brownout) return ['brownout', `+${node.brownout.delay_ms} ms`];
  return ['live', null];
}

function shortName(node) {
  return node.name.startsWith(`${node.zone}-`) ? node.name.slice(node.zone.length + 1) : node.name;
}

/* ---------- header and KPIs ---------- */

function renderHeader(s) {
  const c = s.cluster;
  $('cluster-name').textContent = c ? c.display_name : 'Waiting for the cluster…';
  if (!c) { $('footprint').textContent = ''; $('chip-goal').textContent = ''; $('chip-incident').hidden = true; return; }
  const providers = c.providers.map((p) => p.toUpperCase()).join(' · ');
  $('footprint').textContent = `Production · ${providers} · ${s.regions.length} region${s.regions.length === 1 ? '' : 's'} · ${c.total_nodes} nodes`;
  $('chip-goal').textContent = c.survival_goal === 'none' ? 'Replicated ×3' : `Survives ${c.survival_goal} failure`;
  tip($('chip-goal'), G.goal(c.survival_goal));

  const down = s.regions.filter((r) => r.state === 'down').map((r) => r.name);
  const deadNodes = s.nodes.filter((n) => ['dead'].includes(nodeLook(n)[0]));
  const parted = s.nodes.filter((n) => n.partitioned || n.isolated);
  const brown = s.nodes.filter((n) => n.brownout);
  const chip = $('chip-incident');
  let text = '', warn = false;
  if (down.length) text = `● Region outage · ${down.join(', ')}`;
  else if (deadNodes.length) text = `● ${deadNodes.length} node${deadNodes.length > 1 ? 's' : ''} down`;
  else if (parted.length) text = `● ${s.nodes.some((n) => n.isolated) ? 'Region isolated' : 'Network partition'}`;
  else if (brown.length) { text = `● Brownout · ${brown.length} node${brown.length > 1 ? 's' : ''}`; warn = true; }
  chip.hidden = !text;
  chip.textContent = text;
  chip.classList.toggle('warn', warn);

  const booth = s.booth;
  const bchip = $('chip-booth');
  bchip.hidden = !(booth && booth.enabled);
  if (booth && booth.enabled) bchip.textContent = booth.heal_in != null ? `Auto-heal in ${booth.heal_in} s` : 'Booth mode';
}

function setKpi(id, html, cls, sub) {
  const v = $(id);
  v.innerHTML = html;
  v.className = `val ${cls || ''}`;
  if (sub !== undefined) $(`${id}-sub`).textContent = sub;
}

function renderKpis(s) {
  const c = s.cluster;
  if (!c) { for (const id of ['k-nodes', 'k-unavail', 'k-tps', 'k-p99', 'k-money', 'k-recovery']) setKpi(id, '–'); return; }
  const down = s.nodes.filter((n) => nodeLook(n)[0] === 'dead');
  const downRegions = s.regions.filter((r) => r.state === 'down').map((r) => r.name);
  setKpi('k-nodes', `${c.live_nodes}<span class="dim">/${c.total_nodes}</span>`, c.live_nodes === c.total_nodes ? 'good' : 'bad',
    downRegions.length ? `${down.length} down · ${downRegions.join(', ')}` : down.length ? `${down.length} down` : 'every node serving');

  const { unavailable, underreplicated } = s.ranges;
  if (unavailable == null) setKpi('k-unavail', '–', '', 'telemetry delayed');
  else setKpi('k-unavail', fmt(unavailable), unavailable ? 'bad' : 'good',
    unavailable ? 'some data unreachable' : underreplicated ? `${fmt(underreplicated)} under-replicated · healing` : 'all data reachable');

  const t = s.traffic && s.traffic.cluster;
  const w = s.workload;
  setKpi('k-tps', t ? fmt(t.txn_s) : '–', 'info', w && w.running ? `all clients · payments ${fmt(w.tps)}/s` : 'all clients');
  const src = t && t.p99_ms != null ? t : w;  // cluster-side (all clients) first, payments as a fallback
  const ms = (v) => (v == null ? '–' : `${v >= 100 ? fmt(v) : v.toFixed(0)} ms`);
  const p99 = src ? src.p99_ms : null;
  setKpi('k-p99', p99 != null ? `${p99 >= 100 ? fmt(p99) : p99.toFixed(0)}<span class="unit">ms</span>` : '–', '',
    src && src.p50_ms != null ? `p50 ${ms(src.p50_ms)} · p95 ${ms(src.p95_ms)}` : 'SQL service latency');

  if (!w || w.balance == null) setKpi('k-money', '–', '', w && w.running ? 'checking the balance' : 'payments not running');
  else setKpi('k-money', `$${fmt(w.money_lost)}`, w.money_lost === 0 ? 'good' : 'bad',
    w.money_lost === 0 ? `Total $${fmt(w.balance)} ✓` : `Total $${fmt(w.balance)} ✗`);

  if (w && w.recovering) setKpi('k-recovery', '…', 'warn', 'recovering');
  else if (w && w.recovery_s != null) setKpi('k-recovery', `${w.recovery_s < 10 ? w.recovery_s.toFixed(1) : fmt(w.recovery_s)}<span class="unit">s</span>`,
    'brand', w.recovery_s === 0 ? 'no dip in throughput' : 'back to 80% of normal');
  else setKpi('k-recovery', '–', '', w && w.running ? 'no failures yet' : 'start payments to measure');
}

/* ---------- regions ---------- */

function regionActions(region, nodes) {
  const box = el('div', 'ractions control-only');
  const disturbed = nodes.some((n) => nodeLook(n)[0] !== 'live');
  const button = (label, cls, fn, tipKey) => {
    const b = el('button', `btn ${cls || ''}`, label);
    b.type = 'button';
    b.dataset.fb = `${region.name}:${label}`;
    tip(b, G.action[tipKey || label]);
    b.onclick = (e) => (label === '⋯' ? fn(e) : press(b.dataset.fb, fn));
    box.append(b);
  };
  if (disturbed) button('Recover', 'prim', () => chaos('recover', 'region', region.name));
  if (state.regions.length === 1) {
    // Single region: the failure to show is a zone outage; region-wide faults would take everything down.
    for (const z of region.zones.filter((zone) => nodes.some((x) => x.zone === zone))) {
      button(`Outage · ${z}`, 'danger', () => chaos('outage', 'zone', z), 'Zone outage');
    }
    button('Brownout', '', () => chaos('brownout', 'region', region.name));
    button('+ Node', '', () => post('/api/cluster/nodes', { region: region.name }, `Adding a node in ${region.name}`));
    button('⋯', '', (e) => openRegionMenu(region, e.currentTarget));
    return box;
  }
  if (region.state !== 'down') button('Outage', 'danger', () => chaos('outage', 'region', region.name));
  button('Partition', '', () => chaos('partition', 'region', region.name));
  if (state.regions.length < 4 && state.regions.length > 1) button('Isolate', '', () => chaos('isolate', 'region', region.name));
  button('Brownout', '', () => chaos('brownout', 'region', region.name));
  button('+ Node', '', () => post('/api/cluster/nodes', { region: region.name }, `Adding a node in ${region.name}`));
  button('⋯', '', (e) => openRegionMenu(region, e.currentTarget));
  return box;
}

function nodeTile(node, rates, maxReplicas) {
  const [look, label] = nodeLook(node, state.operation);
  const tile = el('div', `node ${look}`);
  tile.dataset.name = node.name;
  tile.dataset.fb = `node:${node.name}`;
  const bar = el('div', 'bar');
  const fill = el('i');
  fill.style.width = `${node.replicas ? Math.max(3, Math.round(100 * node.replicas / maxReplicas)) : 0}%`;
  bar.append(fill);
  const meta = el('span', 'nl');
  const showCounts = node.replicas != null && look !== 'dead';
  if (label) {
    meta.append(tipped(label, G.nodeState[look] || ''));
    if (showCounts) meta.append(document.createTextNode(' · '));
  }
  if (showCounts) {
    const goal = state.cluster ? state.cluster.survival_goal : 'none';
    meta.append(tipped(`R ${node.replicas}`, G.replicas(node.replicas, goal)), document.createTextNode(' · '),
      tipped(`L ${node.leases}`, G.leases(node.leases)));
  }
  if (!meta.childNodes.length) meta.textContent = ' ';
  tile.append(el('span', 'nn', shortName(node)), bar, meta);
  const r = rates && rates[node.name];
  if (r && r.txn_s >= 1 && look !== 'dead') {
    const rate = tipped(`${fmt(r.txn_s)}/s`, G.rate(fmt(r.txn_s)));
    rate.classList.add('rate');
    tile.append(rate);
  }
  const was = lastLiveness.get(node.name);
  if (was && was !== 'dead' && look === 'dead') tile.classList.add('flash');
  lastLiveness.set(node.name, look);
  tile.onclick = (e) => { if (canControl) openNodeMenu(node, e.currentTarget); };
  return tile;
}

/* ---------- educational tooltips (wording in shared/glossary.js) ---------- */

function tipped(text, explanation) {
  return tip(el('span', 'tip', text), explanation);
}

function renderRegions(s) {
  const box = $('regions');
  const n = s.regions.length;
  const perRow = Math.min(4, Math.max(1, n));
  const rows = Math.max(1, Math.ceil(n / 4));
  box.style.gridTemplateColumns = `repeat(${perRow}, minmax(0, 1fr))`;
  box.style.gridTemplateRows = `repeat(${rows}, minmax(0, 1fr))`;
  box.classList.toggle('compact', rows > 1);
  const byName = new Map(s.nodes.map((x) => [x.name, x]));
  const rates = s.traffic && s.traffic.nodes;
  const maxReplicas = Math.max(1, ...s.nodes.map((x) => x.replicas || 0));
  box.replaceChildren(...s.regions.map((region) => {
    const nodes = region.nodes.map((name) => byName.get(name)).filter(Boolean);
    const card = el('article', `region ${region.state}`);
    const head = el('div', 'rhead');
    head.append(el('span', `prov ${['aws', 'gcp', 'azure', 'ibm', 'oci'].includes(region.provider) ? region.provider : 'custom'}`, region.provider),
      el('span', 'rname', region.name));
    const sub = el('div', 'rsub');
    sub.append(el('span', 'rcity', region.label !== region.name ? region.label : ''));
    if (region.primary || region.secondary) {
      sub.append(tip(el('span', 'rrole', region.primary ? 'Primary region' : 'Secondary region'), region.primary ? G.primary : G.secondary));
    }
    const status = { healthy: 'Healthy', degraded: 'Degraded', down: 'Outage', unknown: '…' }[region.state] || region.state;
    head.append(tip(el('span', `chip rstatus ${region.state}`, status), G.regionState[region.state]));
    const zones = el('div', 'zones');
    const zoneNames = region.zones.filter((z) => nodes.some((x) => x.zone === z));
    for (const z of zoneNames) {
      const col = el('div', 'zone');
      col.append(tip(el('div', 'zl', z), G.zone(z)), ...nodes.filter((x) => x.zone === z).map((x) => nodeTile(x, rates, maxReplicas)));
      zones.append(col);
    }
    card.append(head, sub, zones, regionActions(region, nodes));
    return card;
  }));
  applyFeedback();
}

/* ---------- menus ---------- */

function placeMenu(anchor) {
  const menu = $('menu');
  const stage = $('stage').getBoundingClientRect();
  const a = anchor.getBoundingClientRect();
  const scale = stage.width / 1920;
  menu.hidden = false;
  let x = (a.left - stage.left) / scale, y = (a.bottom - stage.top) / scale + 6;
  const h = menu.offsetHeight, w = menu.offsetWidth;
  if (y + h > 1070) y = (a.top - stage.top) / scale - h - 6;
  if (x + w > 1910) x = 1910 - w;
  menu.style.left = `${Math.max(10, x)}px`;
  menu.style.top = `${Math.max(10, y)}px`;
}

function menuItems(title, items) {
  const menu = $('menu');
  menu.replaceChildren(el('div', 'mh', title));
  for (const [label, hint, fn, cls] of items) {
    const b = el('button', cls || '');
    b.type = 'button';
    b.append(document.createTextNode(label));
    if (hint) b.append(el('small', '', hint));
    b.onclick = () => press(menuFor && menuItems.key ? menuItems.key : label, fn);
    menu.append(b);
  }
}

function openNodeMenu(node, anchor) {
  menuFor = node.name;
  menuItems.key = `node:${node.name}`;
  const [look] = nodeLook(node);
  const items = [];
  if (look !== 'live') items.push(['Recover', 'restart, reconnect, clear shaping', () => chaos('recover', 'node', node.name)]);
  items.push(
    ['Crash', 'hard kill, no warning', () => chaos('crash', 'node', node.name), 'danger'],
    ['Planned stop', 'graceful drain, then stop', () => chaos('stop', 'node', node.name)],
    ['Partition', 'cut it off the network', () => chaos('partition', 'node', node.name)],
    ['Brownout', '+700 ms ±200, 2% loss', () => chaos('brownout', 'node', node.name)],
    ['Decommission', 'move its replicas away, then remove it', () => del(`/api/cluster/nodes/${encodeURIComponent(node.name)}`, `Decommissioning ${node.name}`), 'danger'],
  );
  menuItems(`${node.name} · ${node.zone}`, items);
  placeMenu(anchor);
}

function openRegionMenu(region, anchor) {
  menuFor = region.name;
  menuItems.key = `${region.name}:⋯`;
  const items = region.zones.map((z) => [`Zone outage · ${z}`, 'crash every node in the zone', () => chaos('outage', 'zone', z), 'danger']);
  if (state.regions.length > 1) items.push(['Isolate', 'cut it off from the other regions; clients still connect', () => chaos('isolate', 'region', region.name)]);
  if (!region.primary) items.push(['Make primary', 'move the home of regional data here', () => post('/api/cluster/primary', { region: region.name }, `Primary region → ${region.name}`)]);
  if (!region.primary) items.push(['Remove region', 'drop it from the databases, drain, remove', () => del(`/api/cluster/regions/${encodeURIComponent(region.name)}`, `Removing ${region.name}`), 'danger']);
  menuItems(region.name, items);
  placeMenu(anchor);
}

function closeMenu() { $('menu').hidden = true; menuFor = null; }

/* ---------- traffic chart ---------- */

function regionColor(i) {
  const v = cssVar(REGION_COLORS[i % REGION_COLORS.length]);
  if (v) return v;
  return document.documentElement.dataset.theme === 'light' ? '#5f5f5a' : '#e5e4dc';
}

function drawChart() {
  const canvas = $('chart');
  if (!canvas || !state) return;
  const box = canvas.getBoundingClientRect();
  const scale = $('stage').getBoundingClientRect().width / 1920 || 1;
  const w = Math.max(10, box.width / scale), h = Math.max(10, box.height / scale);
  const dpr = (window.devicePixelRatio || 1) * scale;
  canvas.width = Math.round(w * dpr); canvas.height = Math.round(h * dpr);
  const ctx = canvas.getContext('2d');
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  ctx.clearRect(0, 0, w, h);

  const traffic = state.traffic || {};
  const hist = traffic.history || [];
  const regions = state.regions.map((r) => r.name);
  $('chart-empty').hidden = hist.length > 1;
  $('legend').replaceChildren(...regions.map((r, i) => {
    const s = el('span'); const sw = el('i'); sw.style.background = regionColor(i); s.append(sw, document.createTextNode(r)); return s;
  }));
  if (hist.length < 2) return;

  const now = state.ts, span = 120, x0 = 54;
  const X = (ts) => x0 + (1 - (now - ts) / span) * (w - x0);
  const totals = hist.map((p) => regions.reduce((a, r) => a + (p.regions[r] || 0), 0));
  const peak = Math.max(10, ...totals) * 1.18;
  const step = [1, 2, 5, 10, 20, 25, 50, 100, 200, 250, 500, 1000, 2000, 5000].find((s) => peak / s <= 5) || 10000;
  const max = Math.ceil(peak / step) * step;
  const Y = (v) => h - 18 - (v / max) * (h - 30);
  const muted = cssVar('--muted'), line = cssVar('--line');

  ctx.font = '12px "Geist Mono"'; ctx.fillStyle = muted; ctx.textAlign = 'right';
  for (let v = 0; v <= max; v += step) {
    ctx.strokeStyle = line; ctx.lineWidth = 1; ctx.beginPath(); ctx.moveTo(x0, Y(v)); ctx.lineTo(w, Y(v)); ctx.stroke();
    ctx.fillText(fmt(v), x0 - 10, Y(v) + 4);
  }
  ctx.textAlign = 'left';
  for (const sec of [120, 90, 60, 30, 0]) ctx.fillText(sec ? `−${sec}s` : 'now', Math.min(w - 30, X(now - sec)), h - 2);

  const pts = hist.filter((p) => now - p.ts <= span + 2);
  const base = pts.map(() => 0);
  regions.forEach((r, i) => {
    const top = pts.map((p, k) => base[k] + (p.regions[r] || 0));
    const color = regionColor(i);
    ctx.beginPath();
    pts.forEach((p, k) => { const x = X(p.ts), y = Y(top[k]); k ? ctx.lineTo(x, y) : ctx.moveTo(x, y); });
    for (let k = pts.length - 1; k >= 0; k--) ctx.lineTo(X(pts[k].ts), Y(base[k]));
    ctx.closePath();
    ctx.globalAlpha = document.documentElement.dataset.theme === 'light' ? 0.32 : 0.38;
    ctx.fillStyle = color; ctx.fill();
    ctx.globalAlpha = 1; ctx.strokeStyle = color; ctx.lineWidth = 1.5;
    ctx.beginPath(); pts.forEach((p, k) => { const x = X(p.ts), y = Y(top[k]); k ? ctx.lineTo(x, y) : ctx.moveTo(x, y); }); ctx.stroke();
    top.forEach((v, k) => { base[k] = v; });
  });

  const marks = (state.events || []).filter((e) => (e.level === 'bad' || e.level === 'good') && now - e.ts <= span && e.ts <= now);
  const w0 = state.workload;
  if (w0 && w0.failure_at && w0.recovery_s != null && w0.recovery_s > 0 && now - (w0.failure_at + w0.recovery_s) <= span) {
    marks.push({ ts: w0.failure_at + w0.recovery_s, level: 'recovered', text: `Recovered in ${w0.recovery_s.toFixed(1)} s` });
  }
  let lastLabelX = -1e9;
  for (const m of marks.sort((a, b) => a.ts - b.ts)) {
    const x = X(m.ts);
    const color = m.level === 'bad' ? cssVar('--failed') : cssVar('--healthy-line');
    ctx.save(); ctx.strokeStyle = color; ctx.setLineDash(m.level === 'bad' ? [5, 5] : [2, 5]); ctx.lineWidth = 1.4;
    ctx.beginPath(); ctx.moveTo(x, 4); ctx.lineTo(x, h - 18); ctx.stroke(); ctx.restore();
    if (x - lastLabelX > 150) {
      ctx.fillStyle = color; ctx.font = '12px "Geist Mono"'; ctx.textAlign = x > w - 220 ? 'right' : 'left';
      ctx.fillText(m.text.toUpperCase().slice(0, 28), x + (x > w - 220 ? -8 : 8), 16);
      lastLabelX = x;
    }
  }
}

/* ---------- events, booth, scenario, operations ---------- */

function renderEvents(s) {
  const list = (s.events || []).slice(-8).reverse();
  $('events').replaceChildren(...list.map((e) => {
    const row = el('div', `ev ${e.level}`);
    row.append(el('span', 't', new Date(e.ts * 1000).toISOString().slice(11, 19)), el('span', 'x', e.text));
    return row;
  }));
  if (!list.length) $('events').replaceChildren(el('div', 'ev quiet', 'All quiet. Every region is serving.'));
}

function renderBooth(s) {
  const b = s.booth;
  const on = Boolean(b && b.enabled);
  $('booth-strip').hidden = !on;
  if (!on) return;
  $('b-nodes').textContent = fmt(b.counters.nodes_crashed || 0);
  $('b-regions').textContent = fmt(b.counters.regions_down || 0);
  const lost = s.workload && s.workload.money_lost != null ? s.workload.money_lost : 0;
  $('b-money').textContent = `$${fmt(lost)}`;
  $('b-money').className = `big ${lost ? 'bad' : 'good'}`;
}

function heat(ms, min, max) {
  if (ms <= 5) return 'color-mix(in srgb, var(--healthy) 32%, transparent)';
  const k = Math.min(1, (ms - min) / Math.max(1, max - min));
  return `color-mix(in srgb, var(--degraded) ${Math.round(14 + k * 40)}%, transparent)`;
}

function renderResult(box, result) {
  box.className = 'sc-result';
  if (result && result.matrix) {
    box.classList.add('matrix');
    const cols = Object.keys(result.matrix);
    const rowsRegions = Object.keys(result.matrix[cols[0]] || {});
    const values = cols.flatMap((g) => rowsRegions.map((r) => result.matrix[g][r]));
    const min = Math.min(...values), max = Math.max(...values);
    const table = el('table');
    const head = el('tr'); head.append(el('th', '', 'gateway ↓  row home →'), ...rowsRegions.map((r) => el('th', '', r)));
    table.append(head);
    for (const g of cols) {
      const tr = el('tr'); tr.append(el('th', '', g));
      for (const r of rowsRegions) { const v = result.matrix[g][r]; const td = el('td', '', `${v} ms`); td.style.background = heat(v, min, max); tr.append(td); }
      table.append(tr);
    }
    box.replaceChildren(table);
    return;
  }
  box.replaceChildren(...Object.entries(result || {}).map(([k, v]) => {
    const kv = el('div', 'kv'); kv.append(el('div', 'k', k), el('div', 'v', String(v))); return kv;
  }));
}

function renderScenario(s) {
  const sc = s.scenario;
  document.body.classList.toggle('has-scenario', Boolean(sc));
  $('scenario').hidden = !sc;
  if (!sc) return;
  $('sc-tier').textContent = `Tier ${sc.tier}`;
  $('sc-title').textContent = sc.title;
  const i = sc.step;
  const cur = i >= 0 ? sc.steps[i] : null;
  $('sc-step').textContent = i >= 0 ? `Step ${i + 1} / ${sc.steps.length}` : `${sc.steps.length} steps`;
  $('sc-caption').textContent = cur ? cur.caption : sc.summary;
  $('sc-observe').textContent = cur ? cur.observe : `Next: ${sc.steps[0].label}`;
  const box = $('sc-result');
  if (sc.busy) { box.className = 'sc-result'; box.replaceChildren(el('div', 'busy', i < 0 ? 'Preparing…' : 'Running…')); }
  else if (cur && cur.error) { box.className = 'sc-result'; box.replaceChildren(el('div', 'err', cur.error)); }
  else if (!cur && sc.steps[0] && sc.steps[0].error) { box.className = 'sc-result'; box.replaceChildren(el('div', 'err', sc.steps[0].error)); }
  else renderResult(box, cur && cur.result);
  const next = $('sc-next');
  const last = i >= sc.steps.length - 1;
  next.disabled = sc.busy || last;
  next.textContent = last ? 'Done' : i < 0 ? `Start: ${sc.steps[0].label} ▸` : `Next: ${sc.steps[i + 1].label} ▸`;
}

function renderOperation(s) {
  const op = s.operation;
  const prov = op && PROVISIONING.has(op.kind) && (op.status === 'running' || op.status === 'failed');
  $('provisioning').hidden = !prov;
  if (prov) {
    $('prov-title').textContent = { provision: 'Provisioning', rebuild: 'Rebuilding', adopt: 'Reconnecting' }[op.kind];
    $('prov-name').textContent = s.cluster ? s.cluster.display_name : 'New cluster';
    $('steps').replaceChildren(...op.steps.slice(-9).map((x) => el('li', '', x)));
    $('op-error').hidden = !op.error;
    $('op-error').textContent = op.error || '';
  }
  const elastic = op && !PROVISIONING.has(op.kind) && op.status === 'running';
  $('op-toast').hidden = !elastic;
  if (elastic) $('op-toast-text').textContent = op.steps[op.steps.length - 1] || 'Working…';
  if (op && !PROVISIONING.has(op.kind) && op.status === 'failed' && renderOperation.lastFailed !== op.id) {
    renderOperation.lastFailed = op.id;
    if (op.finished_at && Date.now() / 1000 - op.finished_at < 30) toast(op.error || 'The change failed');
  }
}

function render(s) {
  state = s;
  renderHeader(s);
  renderKpis(s);
  if (!menuFor && !pressing) renderRegions(s);
  renderEvents(s);
  renderBooth(s);
  renderScenario(s);
  renderOperation(s);
  drawChart();
  watchRecovery(s);
}

/* ---------- recovery celebration ---------- */

const CONFETTI_ON = new URLSearchParams(location.search).get('confetti') !== '0';
const DISTURBED = new Set(['dead', 'partitioned', 'isolated', 'brownout']);
let disturbedSince = null;

function watchRecovery(s) {
  if (!s.cluster || s.telemetry.sql !== 'ok') return;
  const op = s.operation;
  if (op && op.status === 'running' && PROVISIONING.has(op.kind)) { disturbedSince = null; return; }
  const disturbed = s.nodes.some((n) => DISTURBED.has(nodeLook(n)[0])) || s.regions.some((r) => r.state === 'down');
  if (disturbed) { if (disturbedSince === null) disturbedSince = s.ts; return; }
  const wasFor = disturbedSince === null ? 0 : s.ts - disturbedSince;
  disturbedSince = null;
  if (wasFor >= 2 && s.cluster.live_nodes === s.cluster.total_nodes) celebrate(s);
}

function celebrate(s) {
  const w = s.workload;
  const lost = w && w.money_lost != null ? ` · $${fmt(w.money_lost)} lost` : '';
  $('celebrate-text').textContent = `Fully recovered${lost}`;
  const banner = $('celebrate');
  banner.classList.remove('out');
  banner.hidden = false;
  clearTimeout(celebrate.timer);
  celebrate.timer = setTimeout(() => { banner.classList.add('out'); setTimeout(() => { banner.hidden = true; }, 450); }, 3200);
  if (CONFETTI_ON && !matchMedia('(prefers-reduced-motion: reduce)').matches) confetti();
}

function confetti() {
  const canvas = $('confetti');
  const ctx = canvas.getContext('2d');
  const colors = ['--healthy', '--brand', '--info', '--taupe', '--brand-text'].map(cssVar).filter(Boolean);
  const parts = [];
  for (const [x, dir] of [[160, 1], [1760, -1]]) {  // two cannons in the bottom corners
    for (let i = 0; i < 110; i++) {
      const angle = (-62 - Math.random() * 22) * Math.PI / 180, speed = 17 + Math.random() * 15;
      parts.push({ x, y: 1080, vx: Math.cos(angle) * speed * dir, vy: Math.sin(angle) * speed, w: 8 + Math.random() * 10,
        h: 5 + Math.random() * 7, r: Math.random() * 6.28, vr: (Math.random() - .5) * .35, c: colors[(Math.random() * colors.length) | 0],
        wob: Math.random() * 6.28 });
    }
  }
  const started = performance.now();
  cancelAnimationFrame(confetti.raf);
  const frame = (now) => {
    const t = (now - started) / 1000;
    ctx.clearRect(0, 0, 1920, 1080);
    for (const p of parts) {
      p.vy += 0.42; p.vx *= 0.992; p.vy *= 0.992; p.x += p.vx + Math.sin(p.wob + t * 6) * 0.8; p.y += p.vy; p.r += p.vr;
      ctx.save();
      ctx.globalAlpha = Math.max(0, Math.min(1, 4.2 - t));
      ctx.translate(p.x, p.y); ctx.rotate(p.r);
      ctx.fillStyle = p.c;
      ctx.fillRect(-p.w / 2, -p.h / 2 * Math.abs(Math.cos(p.wob + t * 8)), p.w, p.h * Math.abs(Math.cos(p.wob + t * 8)) + 1);
      ctx.restore();
    }
    if (t < 4.3) confetti.raf = requestAnimationFrame(frame); else ctx.clearRect(0, 0, 1920, 1080);
  };
  confetti.raf = requestAnimationFrame(frame);
}

/* ---------- drawer and keys ---------- */

function toggleDrawer(force) {
  const d = $('drawer');
  const open = force !== undefined ? force : d.hidden;
  if (open && !$('drawer-frame').src) $('drawer-frame').src = '/presenter?embed=1';
  d.hidden = !open;
  if (!open) refreshControl();
}

async function fireKey(key) {
  const id = state && state.keys ? state.keys[key] : null;
  if (!id) return;
  const r = await api('POST', `/api/triggers/${encodeURIComponent(id)}`);
  if (!r.ok) toast(explain(r));
  else if (r.data && r.data.ok === false) toast(r.data.message);
}

function onKey(e) {
  if (e.target && ['INPUT', 'TEXTAREA', 'SELECT'].includes(e.target.tagName)) return;
  if (e.metaKey || e.ctrlKey || e.altKey) return;
  const k = e.key;
  if (k >= '1' && k <= '9') fireKey(k);
  else if (k === 'p' || k === 'P') toggleDrawer();
  else if (k === 't' || k === 'T') toggleTheme();
  else if (k === 'f' || k === 'F') { if (document.fullscreenElement) document.exitFullscreen(); else document.documentElement.requestFullscreen().catch(() => {}); }
  else if (k === 'e' || k === 'E') location.href = '/explainer/playlist?return=/';
  else if ((k === 'n' || k === 'N' || k === 'ArrowRight') && canControl && state && state.scenario) post('/api/scenarios/next');
  else if (k === 'c' || k === 'C') { if (state) celebrate(state); }
  else if (k === 'Escape') { closeMenu(); toggleDrawer(false); if (confirmDialog.cancel) confirmDialog.cancel(); }
}

/* ---------- start ---------- */

initTheme();
fit();
tickClock();
setInterval(tickClock, 1000);
addEventListener('resize', fit);
addEventListener('keydown', onKey);
initTips({ layer: $('stage'), scale: () => $('stage').getBoundingClientRect().width / 1920 });
for (const [id, text] of [['k-nodes', G.liveNodes], ['k-unavail', G.unavailable], ['k-tps', G.tps], ['k-p99', G.p99],
  ['k-money', G.moneyLost], ['k-recovery', G.recovery]]) tip($(id).parentElement, text);
tip(document.querySelector('#chart-box > .eyebrow'), G.chart);
tip($('chip-incident'), G.incident);
tip($('chip-booth'), G.booth);
addEventListener('pointerdown', () => { pressing = true; }, true);
addEventListener('pointerup', () => { setTimeout(() => { pressing = false; }, 0); }, true);
document.addEventListener('click', (e) => {
  if (!$('menu').hidden && !$('menu').contains(e.target) && !e.target.closest('.node') && !e.target.closest('.btn')) closeMenu();
});
$('drawer-btn').onclick = () => toggleDrawer();
addEventListener('message', (e) => {
  if (e.origin === location.origin && e.data && e.data.type === 'crdb-close-drawer') { toggleDrawer(false); window.focus(); }
});
$('frame').addEventListener('click', () => { if (!$('drawer').hidden) toggleDrawer(false); }, true);
$('sc-next').dataset.fb = 'sc-next';
$('sc-stop').dataset.fb = 'sc-stop';
$('sc-next').onclick = () => press('sc-next', () => post('/api/scenarios/next'));
$('sc-stop').onclick = () => press('sc-stop', () => post('/api/scenarios/stop'));
refreshControl();
const events = new EventSource('/api/events');
events.onmessage = (msg) => render(JSON.parse(msg.data));
