import { Stage, roleName } from './stage.js';
import { diffState } from './events.js';

const SPEEDS = [0.5, 1, 1.5, 2, 3, 4];
const MAX_LOG = 80;
const KEYS = ['color', 'city', 'fruit', 'planet', 'animal', 'size', 'lang', 'mood'];
const VALUES = ['blue', 'halifax', 'mango', 'mars', 'otter', 'large', 'go', 'calm', 'red', 'tokyo',
  'kiwi', 'venus', 'heron', 'small', 'rust', 'sunny', 'green', 'lima', 'plum', 'pluto'];

const $ = (sel) => document.querySelector(sel);

let sim = null;
let stage = null;
let state = null;
let prevState = null;
let lastWrite = null;
let menuFor = null;
const seenProposals = new Map();
const reduceMotion = window.matchMedia('(prefers-reduced-motion: reduce)');

boot();

async function boot() {
  const overlay = $('#overlay');
  try {
    if (typeof WebAssembly !== 'object' || typeof Go !== 'function') {
      throw new Error(typeof Go !== 'function'
        ? 'wasm_exec.js did not load. Run scripts/build-web to produce it.'
        : 'this browser does not support WebAssembly.');
    }
    const go = new Go();
    const ready = new Promise((resolve) => { window.raftsimReady = resolve; });
    const resp = await fetch('raftsim.wasm');
    if (!resp.ok) throw new Error(`raftsim.wasm could not be fetched (HTTP ${resp.status}).`);
    let inst;
    try {
      ({ instance: inst } = await WebAssembly.instantiateStreaming(resp.clone(), go.importObject));
    } catch {
      // some servers send the wrong content type for .wasm
      ({ instance: inst } = await WebAssembly.instantiate(await resp.arrayBuffer(), go.importObject));
    }
    go.run(inst).then(() => fail('The simulation stopped unexpectedly. Reload the page to start a new cluster.'));
    await ready;
    sim = window.raftsim;
  } catch (err) {
    let msg = err && err.message ? err.message : String(err);
    if (location.protocol === 'file:') msg += ' Pages opened straight from disk cannot load WebAssembly; serve the folder over HTTP, for example with python -m http.server.';
    fail(`Couldn't start the simulation: ${msg}`);
    return;
  }
  overlay.hidden = true;
  start();
}

function fail(text) {
  const overlay = $('#overlay');
  overlay.hidden = false;
  overlay.classList.add('error');
  $('#overlay-text').textContent = text;
}

function start() {
  stage = new Stage($('#stage'), { onNodeActivate: openMenu });
  stage.animate = !reduceMotion.matches;
  reduceMotion.addEventListener('change', () => {
    stage.animate = !reduceMotion.matches;
    if (!stage.animate) stage.clearMessages();
  });

  state = JSON.parse(sim.state());
  const ids = state.nodes.map((n) => n.id);
  stage.layout(ids);
  buildTable(ids);
  const target = $('#target');
  for (const id of ids) target.add(new Option(id, id));

  const narrow = window.matchMedia('(max-width: 680px)');
  const fit = () => $('#stage').setAttribute('viewBox', narrow.matches ? '58 6 524 500' : '0 0 640 540');
  narrow.addEventListener('change', fit);
  fit();

  wireControls();
  poll();
  setInterval(poll, 100);
  requestAnimationFrame(frame);
  logEvent('net', 'Five nodes started as followers. Whoever times out first will ask for votes.');
}

function frame(now) {
  const msgs = JSON.parse(sim.messages()) || [];
  stage.addMessages(msgs);
  stage.tick(now);
  requestAnimationFrame(frame);
}

function poll() {
  state = JSON.parse(sim.state());
  stage.render(state);
  renderSummary();
  renderTable();
  renderWrites();
  for (const e of diffState(prevState, state, seenProposals)) logEvent(e.kind, e.text);
  prevState = state;
  if (menuFor) renderMenu();
}

// --- helpers over the current state

const live = () => state.nodes.filter((n) => n.alive);
const byId = (id) => state.nodes.find((n) => n.id === id);

// leaders returns the live nodes that believe they lead, newest term first.
function leaders() {
  return live().filter((n) => n.raft.role === 'Leader').sort((a, b) => b.raft.term - a.raft.term);
}

function call(result) {
  if (typeof result === 'string') logEvent('warn', result);
}

// --- summary line above the stage

function renderSummary() {
  const el = $('#summary');
  const ls = leaders();
  const up = live();
  const quorum = Math.floor(state.nodes.length / 2) + 1;
  let dot = '', head = '', sub = '';
  if (ls.length) {
    const l = ls[0];
    const following = up.filter((n) => n.raft.leader === l.id && n.raft.term === l.raft.term).length;
    dot = 'leader';
    head = `${l.id} is the leader`;
    sub = `term ${l.raft.term} · ${following} of ${state.nodes.length} nodes following · committed through #${l.raft.commit_index}`;
    const stale = ls.slice(1).map((n) => n.id);
    if (stale.length) sub += ` · ${stale.join(', ')} still ${stale.length > 1 ? 'think they lead' : 'thinks it leads'} an older term`;
  } else if (up.length < quorum) {
    head = 'No leader possible';
    sub = `only ${up.length} of ${state.nodes.length} nodes are up and a majority needs ${quorum}`;
  } else {
    const cands = up.filter((n) => n.raft.role === 'Candidate');
    const term = Math.max(...up.map((n) => n.raft.term));
    dot = cands.length ? 'candidate' : '';
    head = 'No leader';
    sub = cands.length ? `election in progress for term ${term}` : 'waiting for an election timeout';
  }
  const html = `<span class="dot ${dot}"></span><span>${head}</span><span class="sub">${sub}</span>`;
  if (el.innerHTML !== html) el.innerHTML = html;
}

// --- node table

const rows = {};

function buildTable(ids) {
  const body = $('#nodes tbody');
  for (const id of ids) {
    const tr = document.createElement('tr');
    tr.innerHTML = `<td><strong>${id}</strong></td><td class="r"></td><td class="term"></td><td class="last opt"></td><td class="commit"></td><td class="applied opt"></td><td class="data"></td>`;
    body.appendChild(tr);
    rows[id] = { tr, cells: Object.fromEntries([...tr.children].map((td) => [td.classList[0] || 'id', td])), sig: '' };
  }
}

function dataText(data) {
  const keys = Object.keys(data).sort();
  if (!keys.length) return '';
  const shown = keys.slice(0, 8).map((k) => `${k}=${data[k]}`).join('  ');
  return keys.length > 8 ? `${shown}  +${keys.length - 8} more` : shown;
}

function renderTable() {
  for (const n of state.nodes) {
    const row = rows[n.id];
    const role = roleName(n);
    const cut = !n.connected || (state.partitioned && n.group < 0);
    const data = dataText(n.data);
    const r = n.raft;
    const sig = [role, cut, r.term, r.last_index, r.commit_index, n.applied, data].join('|');
    if (sig === row.sig) continue;
    row.sig = sig;
    row.tr.className = n.alive ? '' : 'dead';
    row.cells.r.innerHTML = `<span class="role ${role}">${role}</span>${cut ? '<span class="tag">cut off</span>' : ''}`;
    row.cells.term.textContent = r.term;
    row.cells.last.textContent = r.last_index;
    row.cells.commit.textContent = r.commit_index;
    row.cells.applied.textContent = n.alive ? n.applied : '–';
    if (!n.alive) row.cells.data.innerHTML = '<span class="empty">down: data is rebuilt from the log on restart</span>';
    else if (!data) row.cells.data.innerHTML = '<span class="empty">empty</span>';
    else row.cells.data.textContent = data;
    row.cells.data.title = data;
  }

  const el = $('#converge');
  const up = live();
  let cls = '', text = '';
  if (up.length) {
    const sigs = up.map((n) => JSON.stringify(Object.keys(n.data).sort().map((k) => [k, n.data[k]])));
    const keys = Object.keys(up[0].data).length;
    if (sigs.every((s) => s === sigs[0])) {
      cls = 'ok';
      text = `Data identical on all ${up.length} live node${up.length > 1 ? 's' : ''} (${keys} key${keys === 1 ? '' : 's'})`;
    } else {
      const top = Math.max(...up.map((n) => n.applied));
      const behind = up.filter((n) => n.applied < top).map((n) => n.id);
      cls = 'lag';
      text = behind.length
        ? `Not identical yet: ${behind.join(', ')} ${behind.length > 1 ? 'are' : 'is'} behind and will catch up from the leader`
        : 'Not identical yet: replicas are still applying';
    }
  }
  if (el.textContent !== text) el.textContent = text;
  el.className = `converge ${cls}`;
}

// --- client

function sendWrite(text) {
  text = text.trim();
  if (!text) return;
  const target = $('#target').value;
  const r = sim.propose(text, target);
  const res = $('#result');
  if (!r.ok) {
    let msg = r.error;
    if (target && r.error.endsWith('not the leader')) {
      const n = byId(target);
      if (n && n.raft.leader && n.raft.leader !== target) msg += ` (it thinks ${n.raft.leader} is)`;
    }
    res.textContent = msg;
    res.className = 'result bad';
    lastWrite = null;
    return false;
  }
  lastWrite = r;
  renderWrites();
  return true;
}

function renderWrites() {
  const props = state.proposals || [];
  const list = $('#writes');
  const recent = props.slice(-5).reverse();
  const html = recent.map((p) => {
    const label = p.status === 'committed' ? `committed #${p.index}` : p.status === 'lost' ? 'lost' : 'waiting';
    return `<li><code>${escapeHTML(p.text)}</code><span class="chip ${p.status}">${p.node} · ${label}</span></li>`;
  }).join('');
  if (list.innerHTML !== html) list.innerHTML = html;

  if (!lastWrite) return;
  const p = props.find((x) => x.seq === lastWrite.seq);
  if (!p) return;
  const res = $('#result');
  let text, cls;
  if (p.status === 'committed') {
    text = `Committed at index ${p.index} (term ${p.term}). Every live node applies it.`;
    cls = 'ok';
  } else if (p.status === 'lost') {
    text = `Lost. ${p.node} accepted it at #${p.index}, but a majority has committed a different entry there, so ${p.node}'s copy gets overwritten as soon as it hears from the new leader.`;
    cls = 'bad';
  } else {
    const waited = (state.now - p.at) * state.speed;
    text = waited < 4000
      ? `Accepted by ${p.node} at #${p.index}, waiting for a majority to store it…`
      : `Not committed yet: ${p.node} cannot reach a majority. It will commit or be discarded once the network heals.`;
    cls = 'wait';
  }
  if (res.textContent !== text) res.textContent = text;
  res.className = `result ${cls}`;
}

function escapeHTML(s) {
  return s.replace(/[&<>"]/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c]));
}

// --- node menu

function openMenu(id) {
  menuFor = id;
  renderMenu();
  const first = $('#node-menu button');
  if (first) first.focus({ preventScroll: true });
}

function closeMenu() {
  menuFor = null;
  $('#node-menu').hidden = true;
}

function renderMenu() {
  const n = byId(menuFor);
  const menu = $('#node-menu');
  if (!n) return closeMenu();
  const role = roleName(n);
  const sig = `${n.id}|${role}|${n.connected}|${n.raft.term}`;
  if (menu.dataset.sig !== sig) {
    menu.dataset.sig = sig;
    const items = [];
    items.push(n.alive
      ? `<button type="button" role="menuitem" class="danger" data-m="kill">Kill ${n.id}</button>`
      : `<button type="button" role="menuitem" data-m="restart">Restart ${n.id}</button>`);
    items.push(n.connected
      ? `<button type="button" role="menuitem" data-m="disconnect">Cut off from the network</button>`
      : `<button type="button" role="menuitem" data-m="connect">Reconnect to the network</button>`);
    if (n.alive) items.push(`<button type="button" role="menuitem" data-m="target">Send writes to ${n.id}</button>`);
    menu.innerHTML = `<div class="mh"><strong>${n.id}</strong> · ${role}, term ${n.raft.term}</div>${items.join('')}`;
  }
  placeMenu(menu, n.id);
  menu.hidden = false;
}

function placeMenu(menu, id) {
  const svg = $('#stage');
  const wrap = svg.parentElement;
  const p = stage.pos[id];
  const outer = wrap.getBoundingClientRect();
  const below = p.y < 200;
  const pt = svg.createSVGPoint();
  pt.x = p.x;
  pt.y = below ? p.y + 52 : p.y - 62;
  const at = pt.matrixTransform(svg.getScreenCTM());
  const w = menu.offsetWidth || 200;
  const x = Math.min(Math.max(at.x - outer.left, w / 2 + 4), outer.width - w / 2 - 4);
  const y = at.y - outer.top;
  menu.style.left = `${x}px`;
  menu.style.top = `${y}px`;
  menu.style.transform = below ? 'translate(-50%, 0)' : 'translate(-50%, -100%)';
}

function menuAction(action) {
  const id = menuFor;
  closeMenu();
  switch (action) {
    case 'kill': call(sim.kill(id)); break;
    case 'restart': call(sim.restart(id)); break;
    case 'disconnect': call(sim.disconnect(id)); break;
    case 'connect': call(sim.connect(id)); break;
    case 'target':
      $('#target').value = id;
      $('#cmd').focus();
      break;
  }
}

// --- event log

function clock(ms) {
  const s = Math.floor(ms / 1000);
  return `${Math.floor(s / 60)}:${String(s % 60).padStart(2, '0')}`;
}

function logEvent(kind, text) {
  const log = $('#log');
  const li = document.createElement('li');
  li.className = `${kind} new`;
  const t = document.createElement('time');
  t.textContent = clock(state ? state.now : 0);
  const k = document.createElement('span');
  k.className = 'k';
  const msg = document.createElement('span');
  msg.textContent = text;
  li.append(t, k, msg);
  log.prepend(li);
  while (log.children.length > MAX_LOG) log.lastChild.remove();
}

// --- controls

function shuffle(a) {
  a = a.slice();
  for (let i = a.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [a[i], a[j]] = [a[j], a[i]];
  }
  return a;
}

const actions = {
  'kill-leader'() {
    const l = leaders()[0];
    if (!l) return logEvent('warn', 'There is no leader to kill right now.');
    call(sim.kill(l.id));
  },
  minority() {
    const l = leaders()[0];
    if (!l) return logEvent('warn', 'Wait for a leader first, then strand it.');
    const others = shuffle(state.nodes.filter((n) => n.id !== l.id).map((n) => n.id));
    call(sim.partition([[l.id, others[0]], others.slice(1)]));
    $('#target').value = l.id;
  },
  split() {
    const ids = shuffle(state.nodes.map((n) => n.id));
    call(sim.partition([ids.slice(0, 2).sort(), ids.slice(2).sort()]));
  },
  heal() {
    sim.heal();
  },
  reset() {
    sim.reset();
    prevState = null;
    lastWrite = null;
    seenProposals.clear();
    stage.clearMessages();
    $('#log').replaceChildren();
    $('#target').value = '';
    $('#result').innerHTML = 'Commands: <code>set key value</code>, <code>del key</code>.';
    $('#result').className = 'result';
    logEvent('net', 'Cluster reset: five empty nodes, starting as followers.');
  },
  'loss-30'() {
    const loss = $('#loss');
    loss.value = 30;
    loss.dispatchEvent(new Event('input'));
    loss.dispatchEvent(new Event('change'));
  },
};

function wireControls() {
  document.addEventListener('click', (e) => {
    const btn = e.target.closest('[data-act]');
    if (btn && actions[btn.dataset.act]) actions[btn.dataset.act]();
  });

  const menu = $('#node-menu');
  menu.addEventListener('click', (e) => {
    const b = e.target.closest('[data-m]');
    if (b) menuAction(b.dataset.m);
  });
  document.addEventListener('pointerdown', (e) => {
    if (menuFor && !menu.contains(e.target) && !e.target.closest('.node')) closeMenu();
  });
  document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape' && menuFor) closeMenu();
  });
  window.addEventListener('resize', () => { if (menuFor) placeMenu(menu, menuFor); });

  $('#write-form').addEventListener('submit', (e) => {
    e.preventDefault();
    const cmd = $('#cmd');
    if (sendWrite(cmd.value)) cmd.value = '';
  });

  $('#random-writes').addEventListener('click', () => {
    const keys = shuffle(KEYS).slice(0, 3);
    for (const k of keys) {
      if (!sendWrite(`set ${k} ${VALUES[Math.floor(Math.random() * VALUES.length)]}`)) break;
    }
  });

  const loss = $('#loss'), lossOut = $('#loss-out');
  loss.addEventListener('input', () => {
    lossOut.textContent = `${loss.value}%`;
    sim.setDropRate(Number(loss.value) / 100);
  });
  loss.addEventListener('change', () => logEvent('net', `Message loss set to ${loss.value}%`));

  const speed = $('#speed'), speedOut = $('#speed-out');
  speed.addEventListener('input', () => { speedOut.textContent = `${SPEEDS[speed.value]}x`; });
  speed.addEventListener('change', () => {
    sim.setSpeed(SPEEDS[speed.value]);
    logEvent('net', `Speed set to ${SPEEDS[speed.value]}x. Nodes restart with the new timeouts, so expect an election.`);
  });
}
