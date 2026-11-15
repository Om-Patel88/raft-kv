// SVG view of the cluster: nodes on a circle, the links between them and
// RPCs moving along those links.

const NS = 'http://www.w3.org/2000/svg';
const CX = 320, CY = 268, R = 182;
const NODE_R = 44;
const BOX_W = 13, BOX_H = 16, BOX_GAP = 2;
const LANE = 5;          // requests and replies travel on opposite sides of a link
const MAX_IN_FLIGHT = 160;
const DROP_FADE = 450;   // ms

function el(name, attrs = {}, parent) {
  const e = document.createElementNS(NS, name);
  for (const [k, v] of Object.entries(attrs)) e.setAttribute(k, v);
  if (parent) parent.appendChild(e);
  return e;
}

export function termClass(term) {
  return 't' + (term % 8);
}

export const roleName = (n) => (n.alive ? n.raft.role.toLowerCase() : 'down');

export class Stage {
  constructor(svg, { onNodeActivate }) {
    this.svg = svg;
    this.onNodeActivate = onNodeActivate;
    this.pos = {};
    this.nodes = {};
    this.edges = {};
    this.flying = [];
    this.animate = true;

    el('defs', {}, svg);
    this.gHalos = el('g', { class: 'halos' }, svg);
    this.gEdges = el('g', { class: 'edges' }, svg);
    this.gMsgs = el('g', { class: 'msgs' }, svg);
    this.gNodes = el('g', { class: 'nodes' }, svg);
  }

  layout(ids) {
    ids.forEach((id, i) => {
      const a = (-90 + (360 / ids.length) * i) * Math.PI / 180;
      this.pos[id] = { x: CX + R * Math.cos(a), y: CY + R * Math.sin(a) };
    });
    for (let i = 0; i < ids.length; i++) {
      for (let j = i + 1; j < ids.length; j++) {
        const a = this.pos[ids[i]], b = this.pos[ids[j]];
        const [ax, ay, bx, by] = trim(a, b, NODE_R + 2);
        this.edges[ids[i] + '|' + ids[j]] = el('line', { x1: ax, y1: ay, x2: bx, y2: by, class: 'edge' }, this.gEdges);
      }
    }
    for (const id of ids) this.buildNode(id);
  }

  buildNode(id) {
    const { x, y } = this.pos[id];
    const g = el('g', { class: 'node', transform: `translate(${x.toFixed(1)},${y.toFixed(1)})`, tabindex: '0', role: 'button' }, this.gNodes);
    const halo = el('circle', { class: 'halo', r: NODE_R + 18 }, this.gHalos);
    halo.setAttribute('cx', x.toFixed(1));
    halo.setAttribute('cy', y.toFixed(1));
    el('circle', { class: 'pulse', r: NODE_R }, g);
    el('circle', { class: 'body', r: NODE_R }, g);
    const nid = el('text', { class: 'nid', y: -3 }, g);
    nid.textContent = id;
    const term = el('text', { class: 'nterm', y: 16 }, g);
    const pill = el('g', { class: 'pill', transform: `translate(0,${-NODE_R - 13})` }, g);
    el('rect', { x: -40, y: -10, width: 80, height: 20, rx: 10 }, pill);
    const pillText = el('text', { y: 4 }, pill);
    const badge = el('text', { class: 'badge', x: NODE_R * 0.72, y: -NODE_R * 0.72 - 4 }, g);
    const strip = el('g', { class: 'strip', transform: `translate(0,${NODE_R + 12})` }, g);
    const meta = el('text', { class: 'meta', y: NODE_R + 12 + BOX_H + 15 }, g);
    const title = el('title', {}, g);

    const activate = () => this.onNodeActivate(id, { x, y: y - NODE_R });
    g.addEventListener('click', activate);
    g.addEventListener('keydown', (e) => {
      if (e.key === 'Enter' || e.key === ' ') {
        e.preventDefault();
        activate();
      }
    });
    this.nodes[id] = { g, halo, term, pillText, badge, strip, meta, title, sig: '', stripSig: '', last: 0 };
  }

  render(state) {
    const byId = {};
    for (const n of state.nodes) byId[n.id] = n;

    for (const n of state.nodes) {
      const v = this.nodes[n.id];
      if (!v) continue;
      const role = roleName(n);
      const cut = !n.connected || (state.partitioned && n.group < 0);
      const haloClass = cut ? 'iso' : state.partitioned ? 'g' + (n.group % 3) : '';
      const sig = [role, n.raft.term, haloClass, cut].join('|');
      if (sig !== v.sig) {
        v.sig = sig;
        v.g.setAttribute('class', `node ${role}`);
        v.halo.setAttribute('class', `halo ${haloClass}`);
        v.term.textContent = `term ${n.raft.term}`;
        v.pillText.textContent = role;
        v.badge.textContent = cut ? 'cut off' : '';
        const label = `${n.id}, ${role}, term ${n.raft.term}${cut ? ', cut off from the network' : ''}`;
        v.title.textContent = label;
        v.g.setAttribute('aria-label', label + '. Open actions.');
      }
      this.renderStrip(n, v);
    }

    for (const [key, line] of Object.entries(this.edges)) {
      const [a, b] = key.split('|').map((id) => byId[id]);
      let cls = 'edge';
      if (!a || !b || !a.alive || !b.alive) cls += ' idle';
      if (a && b && !reachable(state, a, b)) cls += ' cut';
      if (line.getAttribute('class') !== cls) line.setAttribute('class', cls);
    }
  }

  renderStrip(n, v) {
    const r = n.raft;
    const entries = r.recent_entries || [];
    const sig = entries.map((e) => `${e.index}.${e.term}`).join(',') + `|${r.commit_index}|${r.snapshot_index}|${n.alive}`;
    if (sig === v.stripSig) return;
    v.stripSig = sig;

    v.strip.replaceChildren();
    const lead = r.snapshot_index > 0 ? 1 : 0;
    const slots = Math.max(entries.length + lead, 1);
    const width = slots * BOX_W + (slots - 1) * BOX_GAP;
    let x = -width / 2;

    if (lead) {
      const s = el('rect', { class: 'box snap', x, y: 0, width: BOX_W, height: BOX_H, rx: 2.5 }, v.strip);
      el('title', {}, s).textContent = `snapshot through #${r.snapshot_index}`;
      x += BOX_W + BOX_GAP;
    }
    if (!entries.length && !lead) {
      el('rect', { class: 'box empty', x, y: 0, width: BOX_W, height: BOX_H, rx: 2.5 }, v.strip);
    }
    for (const e of entries) {
      const committed = e.index <= r.commit_index;
      let cls = `box ${termClass(e.term)} ${committed ? 'done' : 'open'}`;
      if (e.type === 1) cls += ' noop';
      if (e.index > v.last && v.last > 0) cls += ' fresh';
      const b = el('rect', { class: cls, x, y: 0, width: BOX_W, height: BOX_H, rx: 2.5 }, v.strip);
      const what = e.type === 1 ? 'no-op (new leader)' : e.preview || 'command';
      el('title', {}, b).textContent = `#${e.index}  term ${e.term}  ${what}  ${committed ? 'committed' : 'not committed'}`;
      x += BOX_W + BOX_GAP;
    }
    v.last = r.last_index;
    v.meta.textContent = r.last_index ? `log ${r.last_index} · commit ${r.commit_index}` : 'empty log';
  }

  addMessages(list) {
    if (!this.animate || !list.length) return;
    const now = performance.now();
    // a hidden tab stops animation frames; skip what piled up meanwhile
    const newest = list[list.length - 1].at;
    for (const m of list) {
      if (newest - m.at > 600) continue;
      if (this.flying.length >= MAX_IN_FLIGHT) break;
      const a = this.pos[m.from], b = this.pos[m.to];
      if (!a || !b) continue;
      const [ax, ay, bx, by] = trim(a, b, NODE_R + 4);
      const dx = bx - ax, dy = by - ay, len = Math.hypot(dx, dy) || 1;
      const ox = (-dy / len) * LANE, oy = (dx / len) * LANE;
      const shape = messageShape(m, this.gMsgs);
      this.flying.push({
        el: shape, dropped: m.dropped,
        ax: ax + ox, ay: ay + oy, bx: bx + ox, by: by + oy,
        start: now, dur: Math.max(m.delay, 40),
      });
    }
  }

  tick(now) {
    const keep = [];
    for (const f of this.flying) {
      let t = (now - f.start) / f.dur;
      if (f.dropped && t >= 0.5) {
        if (!f.lost) {
          f.lost = now;
          const x = f.ax + (f.bx - f.ax) * 0.5, y = f.ay + (f.by - f.ay) * 0.5;
          f.el.remove();
          f.el = el('path', { class: 'm-lost', d: 'M-4,-4L4,4M4,-4L-4,4', transform: `translate(${x.toFixed(1)},${y.toFixed(1)})` }, this.gMsgs);
        }
        const fade = 1 - (now - f.lost) / DROP_FADE;
        if (fade <= 0) {
          f.el.remove();
          continue;
        }
        f.el.setAttribute('opacity', fade.toFixed(2));
        keep.push(f);
        continue;
      }
      if (t >= 1) {
        f.el.remove();
        continue;
      }
      t = Math.max(t, 0);
      const x = f.ax + (f.bx - f.ax) * t, y = f.ay + (f.by - f.ay) * t;
      f.el.setAttribute('transform', `translate(${x.toFixed(1)},${y.toFixed(1)})`);
      keep.push(f);
    }
    this.flying = keep;
  }

  clearMessages() {
    for (const f of this.flying) f.el.remove();
    this.flying = [];
  }
}

function messageShape(m, parent) {
  const at = { transform: 'translate(-99,-99)' };
  switch (m.kind) {
    case 'RequestVote':
      return el('path', { ...at, class: 'm-vote', d: 'M0,-6L6,0L0,6L-6,0Z' }, parent);
    case 'RequestVoteReply':
      return el('circle', { ...at, class: `m-vote-r ${m.success ? 'ok' : 'no'}`, r: 4 }, parent);
    case 'AppendEntries':
      return m.entries > 0
        ? el('circle', { ...at, class: 'm-ae', r: 6.5 }, parent)
        : el('circle', { ...at, class: 'm-hb', r: 3.8 }, parent);
    case 'AppendEntriesReply':
      return el('circle', { ...at, class: `m-ae-r ${m.success ? 'ok' : 'no'}`, r: 3.8 }, parent);
    case 'InstallSnapshot':
      return el('rect', { ...at, class: 'm-snap', x: -5, y: -5, width: 10, height: 10, rx: 1.5 }, parent);
    default:
      return el('circle', { ...at, class: 'm-snap-r', r: 3.4 }, parent);
  }
}

// trim shortens the segment a-b by d at both ends.
function trim(a, b, d) {
  const dx = b.x - a.x, dy = b.y - a.y, len = Math.hypot(dx, dy) || 1;
  const ux = dx / len, uy = dy / len;
  return [a.x + ux * d, a.y + uy * d, b.x - ux * d, b.y - uy * d];
}

// reachable mirrors simnet's rule, ignoring whether the nodes are up.
export function reachable(state, a, b) {
  if (!a.connected || !b.connected) return false;
  if (state.partitioned) return a.group >= 0 && a.group === b.group;
  return true;
}
