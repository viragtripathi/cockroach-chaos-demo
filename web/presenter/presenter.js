// Presenter console: every control the stage hides, plus the real endpoints. Works on a phone or as the stage drawer.
import { GLOSSARY as G } from '/static/shared/glossary.js';
import { initTips, tip } from '/static/shared/tips.js';

const $ = (id) => document.getElementById(id);
const THEME_KEY = 'crdb-stage-theme';
const TRIGGER_ACTIONS = ['crash', 'stop', 'partition', 'brownout', 'zone-outage', 'region-outage', 'isolate', 'recover-all'];
const TRIGGER_TARGETS = ['random-safe', 'random-node', 'random-region', 'node', 'zone', 'region', 'all'];
const params = new URLSearchParams(location.search);

let state = null;
let catalog = {};
let presets = [];
let settings = null;
let kind = 'node';
let lastPresetRender = '';
let model = 'multi-region';  // deployment model for the next rebuild

function h(tag, attrs = {}, ...children) {
  const node = document.createElement(tag);
  for (const [k, v] of Object.entries(attrs)) {
    if (k === 'class') node.className = v;
    else if (k.startsWith('on')) node.addEventListener(k.slice(2), v);
    else if (v === true) node.setAttribute(k, '');
    else if (v !== false && v != null) node.setAttribute(k, v);
  }
  for (const c of children.flat()) if (c != null) node.append(c instanceof Node ? c : document.createTextNode(String(c)));
  return node;
}
const fmt = (n) => Math.round(n).toLocaleString('en-US');

// The button being clicked right now: changes show working → done/failed on it.
let clicked = null;
document.addEventListener('click', (e) => {
  clicked = e.target.closest('.btn, .seg button');
  setTimeout(() => { clicked = null; }, 0);
}, true);

function mark(btn, state) {
  if (!btn) return;
  btn.classList.remove('working', 'done', 'failed');
  if (state) btn.classList.add(state);
  if (state === 'done' || state === 'failed') setTimeout(() => btn.classList.remove(state), 1400);
}

async function api(method, path, body) {
  const btn = method !== 'GET' ? clicked : null;
  mark(btn, 'working');
  let r;
  try {
    r = await fetch(path, { method, credentials: 'same-origin', headers: body ? { 'Content-Type': 'application/json' } : {}, body: body ? JSON.stringify(body) : undefined });
  } catch (e) {
    mark(btn, 'failed');
    return { ok: false, status: 0, data: { detail: 'The control plane is not reachable' } };
  }
  let data = null;
  try { data = await r.json(); } catch (e) { /* no body */ }
  if (r.status === 401) $('locked').hidden = false;
  mark(btn, r.ok ? 'done' : r.status === 409 ? null : 'failed');
  return { ok: r.ok, status: r.status, data };
}
function detail(r) { const d = r.data && r.data.detail; return typeof d === 'string' ? d : d && d.warning ? d.warning : `failed (${r.status})`; }
function toast(text, ok = false) {
  const t = $('toast'); t.textContent = text; t.className = ok ? 'toast ok' : 'toast'; t.hidden = false;
  clearTimeout(toast.timer); toast.timer = setTimeout(() => { t.hidden = true; }, 4500);
}
async function act(method, path, body, okText) {
  const r = await api(method, path, body);
  if (!r.ok) toast(detail(r)); else if (okText) toast(okText, true);
  return r;
}

/* ---------- theme, tabs ---------- */

function applyTheme(t) { document.documentElement.dataset.theme = t; $('theme').textContent = t === 'dark' ? '☀' : '☾'; }
applyTheme(params.get('theme') || localStorage.getItem(THEME_KEY) || 'dark');
$('theme').onclick = () => { const t = document.documentElement.dataset.theme === 'dark' ? 'light' : 'dark'; localStorage.setItem(THEME_KEY, t); applyTheme(t); };
if (params.get('embed') === '1') {
  // Inside the stage drawer: tell the stage to close it (button, Esc, or P while the panel has focus).
  document.body.classList.add('embed');
  const close = () => window.parent.postMessage({ type: 'crdb-close-drawer' }, location.origin);
  $('close-drawer').onclick = close;
  addEventListener('keydown', (e) => {
    const typing = e.target && ['INPUT', 'TEXTAREA', 'SELECT'].includes(e.target.tagName);
    if (e.key === 'Escape' || (!typing && (e.key === 'p' || e.key === 'P'))) close();
  });
}

function showTab(name) {
  document.querySelectorAll('.tabs button').forEach((b) => b.classList.toggle('on', b.dataset.tab === name));
  document.querySelectorAll('.tab').forEach((s) => { s.hidden = s.id !== `tab-${name}`; });
  localStorage.setItem('crdb-presenter-tab', name);
  if (name === 'scenarios') loadScenarios();
  if (name === 'booth') loadSettings();
  if (name === 'system') loadSystem();
}
document.querySelectorAll('.tabs button').forEach((b) => { b.onclick = () => showTab(b.dataset.tab); });

/* ---------- cluster tab ---------- */

let modelSynced = false;

function renderClusterNow(s) {
  const c = s.cluster;
  if (c && !modelSynced) {  // start the toggle on the running cluster's model
    modelSynced = true;
    model = c.model || 'multi-region';
    document.querySelectorAll('#model button').forEach((b) => b.classList.toggle('on', b.dataset.v === model));
  }
  const box = $('cluster-now');
  if (!c) { box.replaceChildren(h('h3', {}, 'No cluster yet'), h('p', { class: 'hint' }, s.operation ? `${s.operation.kind}: ${s.operation.status}` : 'Waiting…')); return; }
  const item = (k, v, t) => h('div', { 'data-tip': t }, h('div', { class: 'k' }, k), h('div', { class: 'v' }, v));
  box.replaceChildren(h('h3', {}, c.display_name), quickLinks(), h('div', { class: 'now' },
    item('Status', c.status), item('Preset', c.preset), item('Model', c.model === 'single-region' ? 'single region' : 'multi-region', G.model),
    item('Nodes', `${c.live_nodes} / ${c.total_nodes} live`, G.liveNodes),
    item('Regions', String(s.regions.length)), item('Survival goal', c.survival_goal, G.goal(c.survival_goal)),
    item('Latency', c.latency, G.latency), item('Version', c.crdb_version),
    item('Unavailable', s.ranges.unavailable ?? '–', G.unavailable)));
  document.querySelectorAll('#latency button').forEach((b) => b.classList.toggle('on', b.dataset.v === c.latency));
  document.querySelectorAll('#goal button').forEach((b) => b.classList.toggle('on', b.dataset.v === c.survival_goal));
}

function fillSelect(sel, values, keep = true) {
  const key = JSON.stringify(values);
  if (sel.dataset.key === key) return;  // unchanged: don't close an open dropdown
  sel.dataset.key = key;
  const prev = sel.value;
  sel.replaceChildren(...values.map(([v, label]) => h('option', { value: v }, label)));
  if (keep && values.some(([v]) => v === prev)) sel.value = prev;
}

function renderClusterControls(s) {
  const regions = s.regions.map((r) => [r.name, `${r.name}${r.primary ? ' (primary)' : ''}`]);
  fillSelect($('primary'), regions);
  fillSelect($('add-node-region'), regions);
  const have = new Set(s.regions.map((r) => r.name));
  const provider = $('add-region-provider').value || (s.regions[0] && s.regions[0].provider) || 'aws';
  const list = (catalog[provider] || []).filter((r) => !have.has(r.name)).map((r) => [r.name, `${r.name} · ${r.label}`]);
  fillSelect($('add-region-name'), list);
  document.querySelector('.custom-name').hidden = provider !== 'custom';
  $('add-region-name').hidden = provider === 'custom';
}

async function loadPresets() {
  const r = await api('GET', '/api/presets');
  if (!r.ok) return;
  presets = r.data;
  fillSelect($('preset-src'), presets.map((p) => [p.name, p.name]));
  renderPresets();
}

function renderPresets() {
  const active = state && state.cluster ? state.cluster.preset : null;
  const key = JSON.stringify([active, presets.map((p) => p.name)]);
  if (key === lastPresetRender) return;
  lastPresetRender = key;
  $('presets').replaceChildren(...presets.map((p) => h('div', { class: `preset ${p.name === active ? 'active' : ''}` },
    h('div', { class: 'n' }, p.name + (p.name === active ? ' · running' : '')),
    p.error ? h('div', { class: 'e' }, p.error) : [
      h('div', { class: 'd' }, `${p.display_name} · ${p.survival_goal} survival`),
      h('div', { class: 'r' }, p.regions.map((r) => `${r.name} ×${r.nodes}`).join(', ')),
      h('button', { class: 'btn danger', onclick: () => rebuild(p.name), 'data-tip': G.rebuild }, 'Rebuild with this'),
    ])));
}

async function rebuild(name) {
  const label = model === 'single-region' ? 'single region' : 'multi-region';
  if (!confirm(`Rebuild the cluster from "${name}" (${label})? Every database is wiped, including Banko's defaultdb.`)) return;
  await act('POST', '/api/cluster/rebuild', { preset: name, model }, `Rebuilding from ${name} (${label})`);
}

$('model').onclick = (e) => {
  const v = e.target.dataset.v; if (!v) return;
  model = v;
  document.querySelectorAll('#model button').forEach((b) => b.classList.toggle('on', b.dataset.v === v));
};

$('latency').onclick = (e) => { const v = e.target.dataset.v; if (v) act('POST', '/api/cluster/latency', { setting: v }, `Latency: ${v}`); };
$('goal').onclick = (e) => { const v = e.target.dataset.v; if (v) act('POST', '/api/cluster/survival', { goal: v }, `Switching to ${v.toUpperCase()} survival`); };
$('primary-go').onclick = () => act('POST', '/api/cluster/primary', { region: $('primary').value }, `Primary → ${$('primary').value}`);
$('add-node').onclick = () => act('POST', '/api/cluster/nodes', { region: $('add-node-region').value }, `Adding a node in ${$('add-node-region').value}`);
$('add-region-provider').onchange = () => state && renderClusterControls(state);
$('add-region').onclick = () => {
  const provider = $('add-region-provider').value;
  const name = provider === 'custom' ? $('add-region-custom').value.trim() : $('add-region-name').value;
  act('POST', '/api/cluster/regions', { name, provider, nodes: parseInt($('add-region-nodes').value, 10) || 3 }, `Adding region ${name}`);
};
$('preset-src').onchange = async () => {
  const r = await api('GET', `/api/presets/${encodeURIComponent($('preset-src').value)}/yaml`);
  if (r.ok) { $('preset-yaml').value = r.data.yaml; if (!$('preset-name').value) $('preset-name').value = `${r.data.name}-custom`; }
};
$('preset-save').onclick = async () => {
  const name = $('preset-name').value.trim();
  const r = await api('PUT', `/api/presets/custom/${encodeURIComponent(name)}`, { yaml: $('preset-yaml').value });
  const msg = $('preset-msg');
  if (r.ok) {
    msg.className = 'msg good';
    msg.textContent = `Saved: ${r.data.regions.map((x) => `${x.name} ×${x.nodes}`).join(', ')} · ${r.data.survival_goal} survival${r.data.warnings.length ? ' · ' + r.data.warnings.join('; ') : ''}`;
    $('preset-rebuild').disabled = false;
    lastPresetRender = '';
    loadPresets();
  } else { msg.className = 'msg bad'; msg.textContent = detail(r); $('preset-rebuild').disabled = true; }
};
$('preset-rebuild').onclick = () => rebuild($('preset-name').value.trim());

/* ---------- chaos tab ---------- */

function targets(s) {
  if (kind === 'node') return s.nodes.map((n) => [n.name, `${n.name} · ${n.liveness}`]);
  if (kind === 'zone') return [...new Set(s.nodes.map((n) => n.zone))].map((z) => [z, z]);
  if (kind === 'region') return s.regions.map((r) => [r.name, `${r.name} · ${r.state}`]);
  return [['', 'every node']];
}

function renderChaos(s) {
  fillSelect($('target'), targets(s));
  const flags = [];
  for (const n of s.nodes) {
    const f = [];
    if (n.runtime !== 'running') f.push(n.runtime === 'missing' ? 'missing' : 'stopped');
    else if (n.liveness !== 'live') f.push(n.liveness);
    if (n.partitioned) f.push('partitioned');
    if (n.isolated) f.push('isolated');
    if (n.brownout) f.push(`brownout +${n.brownout.delay_ms} ms`);
    if (f.length) flags.push(h('div', {}, h('b', {}, n.name), ` ${f.join(', ')}`));
  }
  $('flags').replaceChildren(...(flags.length ? flags : [h('div', {}, 'Every node is healthy.')]));
}

$('kind').onclick = (e) => {
  const v = e.target.dataset.v; if (!v) return;
  kind = v;
  document.querySelectorAll('#kind button').forEach((b) => b.classList.toggle('on', b.dataset.v === v));
  if (state) renderChaos(state);
};
$('chaos-actions').onclick = async (e) => {
  const action = e.target.dataset.a; if (!action) return;
  const body = { action, kind, value: $('target').value };
  if (action === 'brownout') Object.assign(body, { delay_ms: +$('bo-delay').value, jitter_ms: +$('bo-jitter').value, loss_pct: +$('bo-loss').value });
  let r = await api('POST', '/api/chaos', body);
  if (r.status === 409 && r.data.detail.needs_confirm) {
    if (!confirm(`${r.data.detail.warning}\n\nDo it anyway?`)) return;
    r = await api('POST', '/api/chaos', { ...body, confirm: true });
  }
  if (!r.ok) toast(detail(r)); else toast(`${action} · ${r.data.targets.length} node(s)`, true);
};
$('recover-all').onclick = () => act('POST', '/api/chaos', { action: 'recover', kind: 'all', value: '' }, 'Recovering everything');
$('decommission').onclick = () => {
  if (kind !== 'node') { toast('Pick a node target first'); return; }
  const name = $('target').value;
  if (confirm(`Decommission ${name}? Its replicas move away, then it is removed.`)) act('DELETE', `/api/cluster/nodes/${encodeURIComponent(name)}`, null, `Decommissioning ${name}`);
};
$('remove-region').onclick = () => {
  if (kind !== 'region') { toast('Pick a region target first'); return; }
  const name = $('target').value;
  if (confirm(`Remove region ${name}? It is dropped from both databases, drained and removed.`)) act('DELETE', `/api/cluster/regions/${encodeURIComponent(name)}`, null, `Removing ${name}`);
};

/* ---------- workload tab ---------- */

function renderWorkload(s) {
  const w = s.workload;
  if (!w) return;
  const tips = { 'Retries / s': G.retries, p50: G.p50, p95: G.p95, p99: G.p99, 'Money lost': G.moneyLost, Recovery: G.recovery,
    'Transient errors retried': G.transient, 'Failed transfers': G.failed, 'Applied before a retry': G.dedup };
  const item = (k, v) => h('div', { 'data-tip': tips[k] }, h('div', { class: 'k' }, k), h('div', { class: 'v' }, v));
  const errors = Object.entries(w.errors_total || {}).map(([k, v]) => `${k} ${v}`).join(', ') || 'none';
  $('wl-stats').replaceChildren(
    item('State', w.running ? 'running' : 'stopped'), item('Transfers / s', fmt(w.tps)), item('Retries / s', fmt(w.retries_s)),
    item('Errors / s', fmt(w.errors_s)), item('p50', w.p50_ms != null ? `${w.p50_ms} ms` : '–'),
    item('p95', w.p95_ms != null ? `${w.p95_ms} ms` : '–'), item('p99', w.p99_ms != null ? `${w.p99_ms} ms` : '–'),
    item('Total balance', w.balance != null ? `$${fmt(w.balance)}` : '–'), item('Money lost', w.money_lost != null ? `$${fmt(w.money_lost)}` : '–'),
    item('Recovery', w.recovering ? 'recovering…' : w.recovery_s != null ? `${w.recovery_s.toFixed(1)} s` : '–'),
    item('Transient errors retried', errors), item('Failed transfers', fmt(w.failed_total || 0)),
    item('Applied before a retry', fmt(w.deduplicated || 0)), item('Last error', w.last_error || '–'));
  if (document.activeElement !== $('wl-workers')) $('wl-workers').value = w.workers;
  if (document.activeElement !== $('wl-rate')) $('wl-rate').value = w.rate;
}
$('wl-start').onclick = () => act('POST', '/api/workload/start', null, 'Payments started');
$('wl-stop').onclick = () => act('POST', '/api/workload/stop', null, 'Payments stopped');
$('wl-apply').onclick = () => act('PUT', '/api/workload', { workers: +$('wl-workers').value, rate: +$('wl-rate').value }, 'Applied');

/* ---------- scenarios tab ---------- */

async function loadScenarios() {
  const r = await api('GET', '/api/scenarios');
  if (!r.ok) return;
  const list = [];
  let tier = null;
  for (const s of r.data) {
    if (s.tier !== tier) { tier = s.tier; list.push(h('div', { class: 'tier' }, tier === 1 ? 'Tier 1 · resilience and multi-region' : 'Tier 2 · transactions and operations')); }
    list.push(h('div', { class: 'sc' }, h('div', { class: 't' }, s.title), h('div', { class: 's' }, `${s.summary} (${s.steps} steps)`),
      s.unmet.length ? h('div', { class: 'u' }, s.unmet.join(' · ')) : null,
      h('button', { class: 'btn', disabled: s.unmet.length > 0, onclick: () => act('POST', `/api/scenarios/${s.id}/start`, null, `Scenario: ${s.title}`) }, 'Start')));
  }
  $('sc-list').replaceChildren(...list);
}

function renderScenario(s) {
  const sc = s.scenario;
  $('sc-current').hidden = !sc;
  const key = JSON.stringify(sc);
  if (key === renderScenario.last) return;  // unchanged: keep buttons stable under the cursor
  renderScenario.last = key;
  if (!sc) return;
  const last = sc.step >= sc.steps.length - 1;
  $('sc-current').replaceChildren(
    h('h3', {}, sc.title),
    h('ol', { class: 'steps' }, sc.steps.map((st) => h('li', { class: st.status }, st.label,
      st.result ? h('div', { class: 'result' }, st.result.matrix ? 'latency matrix on stage' : Object.entries(st.result).map(([k, v]) => `${k}: ${v}`).join('\n')) : null,
      st.error ? h('div', { class: 'result' }, st.error) : null))),
    h('div', { class: 'row' },
      h('button', { class: 'btn prim', disabled: sc.busy || last, onclick: () => act('POST', '/api/scenarios/next') },
        sc.busy ? 'Running…' : last ? 'Done' : `Next: ${sc.steps[sc.step + 1].label}`),
      h('button', { class: 'btn', onclick: () => act('POST', '/api/scenarios/stop', null, 'Scenario ended') }, 'End and clean up')));
}

/* ---------- booth tab ---------- */

async function loadSettings() {
  const r = await api('GET', '/api/settings');
  if (!r.ok) return;
  settings = r.data;
  $('booth-on').checked = settings.booth.enabled;
  $('booth-heal').value = settings.booth.auto_heal_s;
  renderTriggers();
}

function triggerRow(id, spec, key) {
  const opt = (values, sel) => values.map((v) => h('option', { value: v, selected: v === sel }, v));
  return h('tr', {},
    h('td', {}, h('input', { value: id, class: 't-id' })),
    h('td', {}, h('select', { class: 't-action' }, opt(TRIGGER_ACTIONS, spec.action))),
    h('td', {}, h('select', { class: 't-target' }, opt(TRIGGER_TARGETS, spec.target || 'random-safe'))),
    h('td', {}, h('input', { value: spec.value || '', class: 't-value', placeholder: 'for node/zone/region' })),
    h('td', {}, h('select', { class: 't-key' }, h('option', { value: '' }, '–'), opt(['1', '2', '3', '4', '5', '6', '7', '8', '9'], key))),
    h('td', {}, h('button', { class: 'btn', onclick: () => act('POST', `/api/triggers/${encodeURIComponent(id)}`).then((r) => r.ok && toast(r.data.message || `${r.data.action} · ${r.data.value || 'everything'}`, r.data.ok !== false)) }, 'Fire'),
      ' ', h('button', { class: 'btn', onclick: (e) => e.target.closest('tr').remove() }, '✕')));
}

function renderTriggers() {
  const keyOf = Object.fromEntries(Object.entries(settings.keys || {}).map(([k, id]) => [id, k]));
  $('trig-rows').replaceChildren(...Object.entries(settings.triggers).map(([id, spec]) => triggerRow(id, spec, keyOf[id])));
}

$('trig-add').onclick = () => $('trig-rows').append(triggerRow(`trigger-${$('trig-rows').children.length + 1}`, { action: 'crash', target: 'random-safe' }, ''));
$('trig-save').onclick = async () => {
  const triggers = {}, keys = {};
  for (const tr of $('trig-rows').children) {
    const id = tr.querySelector('.t-id').value.trim();
    if (!id) continue;
    const spec = { action: tr.querySelector('.t-action').value, target: tr.querySelector('.t-target').value };
    const value = tr.querySelector('.t-value').value.trim();
    if (value) spec.value = value;
    triggers[id] = spec;
    const key = tr.querySelector('.t-key').value;
    if (key) keys[key] = id;
  }
  const r = await api('PUT', '/api/triggers', { triggers, keys });
  $('trig-msg').className = r.ok ? 'msg good' : 'msg bad';
  $('trig-msg').textContent = r.ok ? 'Saved' : detail(r);
  if (r.ok) loadSettings();
};
$('booth-save').onclick = () => act('PUT', '/api/booth', { enabled: $('booth-on').checked, auto_heal_s: +$('booth-heal').value }, 'Booth settings saved');
$('counters-reset').onclick = () => act('POST', '/api/booth/counters/reset', null, 'Counters reset');

function renderBooth(s) {
  const b = s.booth;
  if (!b) return;
  $('counters').textContent = `Nodes crashed ${fmt(b.counters.nodes_crashed || 0)} · regions taken down ${fmt(b.counters.regions_down || 0)}${b.heal_in != null ? ` · auto-heal in ${b.heal_in} s` : ''}`;
}

/* ---------- system tab ---------- */

let system = null;

// Endpoints are reported as localhost; use the host this page was opened from (works from a phone too).
function hostify(v) {
  return typeof v === 'string' ? v.replace('//localhost', `//${location.hostname}`).replace('@localhost', `@${location.hostname}`) : v;
}

function quickLinks() {
  if (!system) return null;
  const link = (label, url) => url && h('a', { class: 'quick', href: hostify(url), target: '_blank', rel: 'noopener' }, `${label} ↗`);
  return h('div', { class: 'links' }, link('DB Console', system.db_console), link('Load balancer', system.haproxy_stats), link('Stage', system.stage));
}

async function loadSystem() {
  const r = await api('GET', '/api/system');
  if (!r.ok) return;
  const d = r.data;
  const rows = [
    ['SQL (Banko, psql)', d.sql_url], ['SQL inside the network', d.sql_url_in_network], ['DB Console', d.db_console],
    ['Load balancer stats', d.haproxy_stats], ['Stage', d.stage], ['Presenter', d.presenter], ['Network', `${d.network} · ${d.subnet}`],
    ['Default version', d.crdb_version], ['License', d.license],
    ['Control token', d.token_required ? (d.authorized ? 'required · this browser is unlocked' : 'required · locked') : 'not set (anyone on this network can change the cluster)'],
  ].filter(([, v]) => v);
  const value = (v) => {
    v = hostify(v);
    if (/^https?:\/\//.test(v)) return h('a', { href: v, target: '_blank', rel: 'noopener' }, `${v} ↗`);
    if (/^postgres(ql)?:\/\//.test(v)) {
      return [v, ' ', h('button', { class: 'btn small', onclick: (e) => {
        navigator.clipboard.writeText(v).then(() => { mark(e.target, 'done'); toast('Copied', true); }, () => toast('Copy failed: select the text instead'));
      } }, 'Copy')];
    }
    return v;
  };
  $('endpoints').replaceChildren(...rows.flatMap(([k, v]) => [h('dt', {}, k), h('dd', {}, value(v))]));
}

function renderSystem(s) {
  const op = s.operation;
  $('operation').replaceChildren(op ? h('div', {},
    h('div', {}, `${op.kind} · ${op.status}`),
    h('ol', { class: 'steps' }, op.steps.slice(-12).map((x) => h('li', {}, x))),
    op.error ? h('p', { class: 'msg bad' }, op.error) : null) : h('p', { class: 'hint' }, 'None yet'));
  $('log').replaceChildren(...(s.events || []).slice().reverse().map((e) => h('div', { class: e.level }, `${new Date(e.ts * 1000).toLocaleTimeString()}  ${e.text}`)));
}

/* ---------- live ---------- */

function renderStatus(s) {
  const c = s.cluster;
  const el = $('status');
  if (!c) { el.textContent = s.operation ? `${s.operation.kind} · ${s.operation.status}` : 'No cluster'; el.className = 'status'; return; }
  el.textContent = `${c.display_name} · ${c.status} · ${c.live_nodes}/${c.total_nodes} live`;
  el.className = `status ${{ healthy: 'good', degraded: 'warn', critical: 'bad', unreachable: 'bad' }[c.status] || ''}`;
}

function render(s) {
  state = s;
  renderStatus(s);
  renderClusterNow(s);
  renderClusterControls(s);
  renderPresets();
  renderChaos(s);
  renderWorkload(s);
  renderScenario(s);
  renderBooth(s);
  if (!$('tab-system').hidden) renderSystem(s);
}

function staticTips() {
  initTips({ layer: document.body });
  tip($('latency'), G.latency);
  tip($('model'), G.model);
  document.querySelectorAll('#goal button').forEach((b) => tip(b, G.goal(b.dataset.v)));
  tip($('primary-go'), G.makePrimary);
  tip($('add-node'), G.addNode);
  tip($('add-region'), G.addRegion);
  tip($('preset-yaml'), G.customPreset);
  tip($('preset-rebuild'), G.rebuild);
  tip($('kind'), G.targetKind);
  document.querySelectorAll('#chaos-actions [data-a]').forEach((b) => tip(b, G.action[b.dataset.a]));
  for (const id of ['bo-delay', 'bo-jitter', 'bo-loss']) tip($(id).closest('label'), G.brownoutParams);
  tip($('recover-all'), G.action.recover);
  tip($('decommission'), G.decommission);
  tip($('remove-region'), G.removeRegion);
  tip($('wl-workers').closest('label'), G.workers);
  tip($('wl-rate').closest('label'), G.rate_target);
  tip($('booth-on').closest('label'), G.boothEnabled);
  tip($('booth-heal').closest('label'), G.autoHeal);
  tip(document.querySelector('.triggers th:nth-child(3)'), G.triggerTargets);
}

(async () => {
  staticTips();
  const sys = await api('GET', '/api/system');
  if (sys.ok) system = sys.data;
  if (sys.ok && sys.data.token_required && !sys.data.authorized) $('locked').hidden = false;
  const cat = await api('GET', '/api/catalog');
  if (cat.ok) {
    catalog = cat.data;
    fillSelect($('add-region-provider'), [...Object.keys(catalog).map((p) => [p, p.toUpperCase()]), ['custom', 'Custom']], false);
  }
  await loadPresets();
  if (presets.length) $('preset-src').onchange();
  showTab(localStorage.getItem('crdb-presenter-tab') || 'cluster');
  const events = new EventSource('/api/events');
  events.onmessage = (m) => render(JSON.parse(m.data));
})();
