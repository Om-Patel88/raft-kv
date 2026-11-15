// Turns successive state snapshots into a readable timeline. Nothing here
// talks to the simulation; it only compares what it saw last time.

const groupsText = (state) => {
  const groups = [];
  for (const n of state.nodes) {
    if (n.group < 0) continue;
    (groups[n.group] ||= []).push(n.id);
  }
  return groups.filter(Boolean).map((g) => `{${g.join(', ')}}`).join(' | ');
};

const netSig = (state) =>
  state.partitioned ? 'p:' + state.nodes.map((n) => n.group).join(',') : '';

export function diffState(prev, cur, proposalsSeen) {
  const out = [];
  const add = (kind, text) => out.push({ kind, text });
  if (!prev) return out;

  if (netSig(prev) !== netSig(cur)) {
    if (cur.partitioned) add('net', `Network split: ${groupsText(cur)}`);
    else add('net', 'Partition healed, every link is back');
  }

  const prevById = Object.fromEntries(prev.nodes.map((n) => [n.id, n]));
  for (const n of cur.nodes) {
    const p = prevById[n.id];
    if (!p) continue;
    const r = n.raft, pr = p.raft;

    if (p.connected && !n.connected) add('net', `${n.id} cut off from the network`);
    if (!p.connected && n.connected) add('net', `${n.id} reconnected`);

    if (p.alive && !n.alive) {
      add('down', `${n.id} crashed (its log and term survive on disk, its in-memory data does not)`);
      continue;
    }
    if (!p.alive && n.alive) {
      add('up', `${n.id} restarted at term ${r.term} with ${r.last_index} log entries`);
    }
    if (!n.alive) continue;

    if (r.role !== pr.role || r.term !== pr.term) {
      if (r.role === 'Leader') {
        add('leader', `${n.id} won the election and leads term ${r.term}`);
      } else if (r.role === 'Candidate') {
        add('cand', `${n.id} timed out waiting for a leader and asked for votes in term ${r.term}`);
      } else if (pr.role === 'Leader') {
        add('warn', `${n.id} stepped down: it saw term ${r.term}, newer than its own`);
      }
    }

    const lost = discarded(pr, r);
    if (lost) add('warn', `${n.id} threw away ${lost} uncommitted ${lost === 1 ? 'entry' : 'entries'} that conflicted with the leader's log`);
  }

  // Commits, reported once from whichever live node is furthest ahead.
  const maxCommit = (s) => Math.max(0, ...s.nodes.filter((n) => n.alive).map((n) => n.raft.commit_index));
  const was = maxCommit(prev), now = maxCommit(cur);
  if (now > was) {
    const src = cur.nodes.find((n) => n.alive && n.raft.commit_index === now);
    const ents = (src.raft.recent_entries || []).filter((e) => e.index > was && e.index <= now);
    const cmds = ents.filter((e) => e.type === 0);
    const range = now - was === 1 ? `#${now}` : `#${was + 1}–#${now}`;
    if (cmds.length === 1 && ents.length === 1) add('commit', `Entry ${range} committed: ${cmds[0].preview}`);
    else if (cmds.length === 0 && ents.length > 0) add('commit', `Entry ${range} committed (the new leader's no-op)`);
    else add('commit', `Entries ${range} committed on a majority`);
  }

  for (const p of cur.proposals || []) {
    const before = proposalsSeen.get(p.seq);
    if (before === p.status) continue;
    proposalsSeen.set(p.seq, p.status);
    if (p.status === 'lost') {
      add('warn', `Write "${p.text}" on ${p.node} can never commit: the majority committed a different entry at #${p.index}`);
    }
  }
  return out;
}

// discarded counts entries a node had last time that have since been
// replaced or truncated away without ever being committed.
function discarded(prev, cur) {
  const now = new Map((cur.recent_entries || []).map((e) => [e.index, e.term]));
  let n = 0;
  for (const e of prev.recent_entries || []) {
    if (e.index <= prev.commit_index || e.index <= cur.snapshot_index) continue;
    const t = now.get(e.index);
    if ((t !== undefined && t !== e.term) || (t === undefined && e.index > cur.last_index)) n++;
  }
  return n;
}
