package raft_test

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand/v2"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/Om-Patel88/raft-kv/raft"
	"github.com/Om-Patel88/raft-kv/raft/wal"
	"github.com/Om-Patel88/raft-kv/simnet"
)

const (
	electionMin = 150 * time.Millisecond
	electionMax = 300 * time.Millisecond
	heartbeat   = 50 * time.Millisecond
)

// storageKinds lets persistence tests run against both implementations.
var storageKinds = []string{"mem", "wal"}

// harness runs a cluster of raft nodes on a simnet and checks safety as it
// goes: every node must apply the same command at a given index, in
// increasing index order, and there may be at most one leader per term.
type harness struct {
	t         *testing.T
	net       *simnet.Network
	ids       []string
	kind      string
	dir       string
	snapEvery uint64 // service snapshots every this many applied indexes, 0 = never

	mu        sync.Mutex
	nodes     map[string]*raft.Node // live nodes only
	stores    map[string]raft.Storage
	sms       map[string]*stateMachine
	connected map[string]bool
	committed map[uint64]string // first command seen applied at each index, by anyone
	leaders   map[uint64]string // term -> leader observed by the monitor
	errs      []string

	stopMon chan struct{}
	monDone chan struct{}
}

// stateMachine is the per-incarnation "service": it just remembers what
// was applied at each index.
type stateMachine struct {
	applied  map[uint64]string
	last     uint64
	lastSnap uint64
	done     chan struct{}
	exited   chan struct{}
}

type harnessOpt func(*harness)

func withStorage(kind string) harnessOpt  { return func(h *harness) { h.kind = kind } }
func withSnapshots(n uint64) harnessOpt   { return func(h *harness) { h.snapEvery = n } }
func withNet(o simnet.Options) harnessOpt { return func(h *harness) { h.net = simnet.New(o) } }

func newHarness(t *testing.T, n int, opts ...harnessOpt) *harness {
	t.Helper()
	h := &harness{
		t:         t,
		kind:      "mem",
		nodes:     map[string]*raft.Node{},
		stores:    map[string]raft.Storage{},
		sms:       map[string]*stateMachine{},
		connected: map[string]bool{},
		committed: map[uint64]string{},
		leaders:   map[uint64]string{},
		stopMon:   make(chan struct{}),
		monDone:   make(chan struct{}),
	}
	for _, o := range opts {
		o(h)
	}
	if h.net == nil {
		h.net = simnet.New(simnet.Options{Seed: rand.Uint64()})
	}
	if h.kind == "wal" {
		h.dir = t.TempDir()
	}
	for i := 0; i < n; i++ {
		h.ids = append(h.ids, fmt.Sprintf("n%d", i+1))
	}
	for _, id := range h.ids {
		h.start(id)
	}
	go h.monitor()
	t.Cleanup(h.cleanup)
	return h
}

// openStorage returns the storage id should start on: the same
// MemoryStorage as last time, or the WAL directory reopened.
func (h *harness) openStorage(id string) raft.Storage {
	if h.kind == "mem" {
		h.mu.Lock()
		defer h.mu.Unlock()
		if s, ok := h.stores[id]; ok {
			return s
		}
		return raft.NewMemoryStorage()
	}
	w, err := wal.Open(filepath.Join(h.dir, id), wal.Options{})
	if err != nil {
		h.t.Fatalf("open wal for %s: %v", id, err)
	}
	return w
}

func (h *harness) start(id string) {
	if h.alive(id) {
		h.t.Fatalf("start %s: already running", id)
	}
	st := h.openStorage(id)
	h.mu.Lock()
	h.stores[id] = st
	h.mu.Unlock()

	ch := make(chan raft.ApplyMsg, 16)
	cfg := raft.Config{
		ID:                 id,
		Peers:              h.ids,
		ElectionTimeoutMin: electionMin,
		ElectionTimeoutMax: electionMax,
		HeartbeatInterval:  heartbeat,
		Storage:            st,
		Transport:          h.net.Transport(id),
		ApplyCh:            ch,
	}
	if os.Getenv("RAFT_DEBUG") != "" {
		cfg.Logger = log.New(os.Stderr, "", log.Lmicroseconds)
	}
	node, err := raft.NewNode(cfg)
	if err != nil {
		h.t.Fatalf("new node %s: %v", id, err)
	}
	sm := &stateMachine{applied: map[uint64]string{}, done: make(chan struct{}), exited: make(chan struct{})}

	h.mu.Lock()
	h.nodes[id] = node
	h.sms[id] = sm
	if _, seen := h.connected[id]; !seen {
		h.connected[id] = true
	}
	h.mu.Unlock()

	go h.consume(id, node, ch, sm)
	// simnet remembers disconnects across Remove/Join, so a node that
	// crashed while cut off comes back still cut off
	h.net.Join(id, node)
	node.Start()
}

// crash stops a node and takes it off the network, keeping its storage.
func (h *harness) crash(id string) {
	h.mu.Lock()
	node, sm, st := h.nodes[id], h.sms[id], h.stores[id]
	delete(h.nodes, id)
	h.mu.Unlock()
	if node == nil {
		return
	}
	h.net.Remove(id)
	node.Stop()
	close(sm.done)
	<-sm.exited
	if w, ok := st.(*wal.WAL); ok {
		w.Close()
	}
}

func (h *harness) restart(id string) {
	h.crash(id)
	h.start(id)
}

func (h *harness) disconnect(id string) {
	h.mu.Lock()
	h.connected[id] = false
	h.mu.Unlock()
	h.net.Disconnect(id)
}

func (h *harness) connect(id string) {
	h.mu.Lock()
	h.connected[id] = true
	h.mu.Unlock()
	h.net.Connect(id)
}

func (h *harness) isConnected(id string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.connected[id]
}

func (h *harness) node(id string) *raft.Node {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.nodes[id]
}

func (h *harness) alive(id string) bool { return h.node(id) != nil }

func (h *harness) fail(format string, args ...any) {
	h.mu.Lock()
	h.errs = append(h.errs, fmt.Sprintf(format, args...))
	h.mu.Unlock()
}

// checkErrors fails the test if a background check has found a problem.
func (h *harness) checkErrors() {
	h.t.Helper()
	h.mu.Lock()
	errs := append([]string(nil), h.errs...)
	h.mu.Unlock()
	for _, e := range errs {
		h.t.Fatal(e)
	}
}

func (h *harness) consume(id string, node *raft.Node, ch <-chan raft.ApplyMsg, sm *stateMachine) {
	defer close(sm.exited)
	for {
		var m raft.ApplyMsg
		select {
		case <-sm.done:
			return
		case m = <-ch:
		}

		switch {
		case m.CommandValid:
			cmd := string(m.Command)
			h.mu.Lock()
			if m.Index <= sm.last {
				h.errs = append(h.errs, fmt.Sprintf("%s applied index %d after %d", id, m.Index, sm.last))
			}
			if prev, ok := h.committed[m.Index]; ok && prev != cmd {
				h.errs = append(h.errs, fmt.Sprintf("%s applied %q at %d, but %q was applied there elsewhere", id, cmd, m.Index, prev))
			} else if !ok {
				h.committed[m.Index] = cmd
			}
			sm.applied[m.Index] = cmd
			sm.last = m.Index
			var snap []byte
			if h.snapEvery > 0 && m.Index-sm.lastSnap >= h.snapEvery {
				snap, _ = json.Marshal(sm.applied)
				sm.lastSnap = m.Index
			}
			h.mu.Unlock()
			if snap != nil {
				node.Snapshot(m.Index, snap)
			}

		case m.SnapshotValid:
			applied := map[uint64]string{}
			if err := json.Unmarshal(m.Snapshot, &applied); err != nil {
				h.fail("%s: bad snapshot: %v", id, err)
				continue
			}
			h.mu.Lock()
			if m.SnapshotIndex < sm.last {
				h.errs = append(h.errs, fmt.Sprintf("%s got snapshot at %d after applying %d", id, m.SnapshotIndex, sm.last))
			}
			for i, cmd := range applied {
				if prev, ok := h.committed[i]; ok && prev != cmd {
					h.errs = append(h.errs, fmt.Sprintf("%s snapshot has %q at %d, others applied %q", id, cmd, i, prev))
				} else if !ok {
					h.committed[i] = cmd
				}
			}
			sm.applied = applied
			sm.last = m.SnapshotIndex
			sm.lastSnap = m.SnapshotIndex
			h.mu.Unlock()

		default:
			h.fail("%s: apply message with neither kind set: %+v", id, m)
		}
	}
}

// monitor polls every live node and complains if two nodes claim to lead
// the same term.
func (h *harness) monitor() {
	defer close(h.monDone)
	tick := time.NewTicker(5 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-h.stopMon:
			return
		case <-tick.C:
		}
		for _, n := range h.liveNodes() {
			st := n.Status()
			if st.Role != raft.Leader {
				continue
			}
			h.mu.Lock()
			if prev, ok := h.leaders[st.Term]; ok && prev != st.ID {
				h.errs = append(h.errs, fmt.Sprintf("term %d has two leaders: %s and %s", st.Term, prev, st.ID))
			}
			h.leaders[st.Term] = st.ID
			h.mu.Unlock()
		}
	}
}

func (h *harness) liveNodes() []*raft.Node {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]*raft.Node, 0, len(h.nodes))
	for _, id := range h.ids {
		if n := h.nodes[id]; n != nil {
			out = append(out, n)
		}
	}
	return out
}

func (h *harness) cleanup() {
	close(h.stopMon)
	<-h.monDone
	for _, id := range h.ids {
		h.crash(id)
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, e := range h.errs {
		h.t.Error(e)
	}
}

// checkOneLeader waits for a leader among the connected nodes and returns
// it. Leaders from older terms that have not noticed yet are ignored.
func (h *harness) checkOneLeader() string {
	h.t.Helper()
	for i := 0; i < 50; i++ {
		time.Sleep(time.Duration(50+rand.IntN(50)) * time.Millisecond)
		byTerm := map[uint64][]string{}
		for _, id := range h.ids {
			n := h.node(id)
			if n == nil || !h.isConnected(id) {
				continue
			}
			if st := n.Status(); st.Role == raft.Leader {
				byTerm[st.Term] = append(byTerm[st.Term], id)
			}
		}
		var best uint64
		for term, ls := range byTerm {
			if len(ls) > 1 {
				h.t.Fatalf("term %d has %d leaders: %v", term, len(ls), ls)
			}
			best = max(best, term)
		}
		if best > 0 {
			return byTerm[best][0]
		}
	}
	h.t.Fatal("expected one leader, got none")
	return ""
}

// checkNoLeader fails if any connected node thinks it is leader.
func (h *harness) checkNoLeader() {
	h.t.Helper()
	for _, id := range h.ids {
		n := h.node(id)
		if n == nil || !h.isConnected(id) {
			continue
		}
		if st := n.Status(); st.Role == raft.Leader {
			h.t.Fatalf("%s is leader in term %d, expected no leader", id, st.Term)
		}
	}
}

// checkTerms returns the term every connected node agrees on.
func (h *harness) checkTerms() uint64 {
	h.t.Helper()
	var term uint64
	for _, id := range h.ids {
		n := h.node(id)
		if n == nil || !h.isConnected(id) {
			continue
		}
		st := n.Status()
		if term == 0 {
			term = st.Term
		} else if term != st.Term {
			h.t.Fatalf("nodes disagree on term: %d vs %d", term, st.Term)
		}
	}
	return term
}

// nCommitted reports how many live nodes have applied index, and what.
func (h *harness) nCommitted(index uint64) (int, string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	count := 0
	cmd := ""
	for _, id := range h.ids {
		if h.nodes[id] == nil {
			continue
		}
		sm := h.sms[id]
		if c, ok := sm.applied[index]; ok {
			if count > 0 && c != cmd {
				h.t.Errorf("index %d: %q vs %q", index, c, cmd)
			}
			count++
			cmd = c
		}
	}
	return count, cmd
}

// wait blocks until at least n nodes have applied index. If startTerm is
// non-zero and some node moves past it, the wait is abandoned since the
// entry may never commit.
func (h *harness) wait(index uint64, n int, startTerm uint64) (string, bool) {
	h.t.Helper()
	to := 10 * time.Millisecond
	for iter := 0; iter < 40; iter++ {
		c, cmd := h.nCommitted(index)
		if c >= n {
			return cmd, true
		}
		time.Sleep(to)
		if to < time.Second {
			to *= 2
		}
		if startTerm > 0 {
			for _, node := range h.liveNodes() {
				if node.Status().Term > startTerm {
					return "", false
				}
			}
		}
	}
	c, _ := h.nCommitted(index)
	h.t.Fatalf("only %d nodes committed index %d, wanted %d", c, index, n)
	return "", false
}

// propose offers cmd to whichever connected node is leader and returns the
// index it was placed at, or 0.
func (h *harness) propose(cmd string) (uint64, uint64) {
	for _, id := range h.ids {
		n := h.node(id)
		if n == nil || !h.isConnected(id) {
			continue
		}
		if idx, term, ok := n.Propose([]byte(cmd)); ok {
			return idx, term
		}
	}
	return 0, 0
}

// tryOne proposes cmd until it is applied by at least expected nodes, or
// the timeout runs out. It is safe to call from several goroutines.
func (h *harness) tryOne(cmd string, expected int, retry bool, timeout time.Duration) (uint64, bool) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		idx, _ := h.propose(cmd)
		if idx == 0 {
			time.Sleep(50 * time.Millisecond)
			continue
		}
		wait := time.Now().Add(2 * time.Second)
		for time.Now().Before(wait) && time.Now().Before(deadline) {
			if c, got := h.nCommitted(idx); c >= expected && got == cmd {
				return idx, true
			}
			time.Sleep(10 * time.Millisecond)
		}
		if !retry {
			return 0, false
		}
	}
	return 0, false
}

// one gets cmd committed on at least expected nodes or fails the test.
func (h *harness) one(cmd string, expected int, retry bool) uint64 {
	h.t.Helper()
	idx, ok := h.tryOne(cmd, expected, retry, 10*time.Second)
	if !ok {
		h.checkErrors()
		h.t.Fatalf("one(%q) failed to reach agreement on %d nodes", cmd, expected)
	}
	h.checkErrors()
	return idx
}

// others returns every id except the ones given.
func (h *harness) others(except ...string) []string {
	skip := map[string]bool{}
	for _, e := range except {
		skip[e] = true
	}
	var out []string
	for _, id := range h.ids {
		if !skip[id] {
			out = append(out, id)
		}
	}
	return out
}

func (h *harness) status(id string) raft.Status {
	n := h.node(id)
	if n == nil {
		h.t.Fatalf("%s is not running", id)
	}
	return n.Status()
}

// committedCommands lists every command applied anywhere, by index.
func (h *harness) committedCommands() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	idx := make([]uint64, 0, len(h.committed))
	for i := range h.committed {
		idx = append(idx, i)
	}
	sort.Slice(idx, func(a, b int) bool { return idx[a] < idx[b] })
	out := make([]string, len(idx))
	for i, k := range idx {
		out[i] = h.committed[k]
	}
	return out
}
