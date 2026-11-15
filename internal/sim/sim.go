// Package sim runs a small raft cluster on a simnet for the browser demo.
// It knows nothing about JavaScript, so it can be tested natively; the
// syscall/js glue lives in cmd/raftsim.
package sim

import (
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/Om-Patel88/raft-kv/raft"
	"github.com/Om-Patel88/raft-kv/simnet"
)

// Timing is the cluster's clock at speed 1. Everything is slowed right
// down from what a real deployment would use so a person can follow it.
type Timing struct {
	ElectionMin time.Duration
	ElectionMax time.Duration
	Heartbeat   time.Duration
	DelayMin    time.Duration
	DelayMax    time.Duration
}

var DefaultTiming = Timing{
	ElectionMin: 1500 * time.Millisecond,
	ElectionMax: 3000 * time.Millisecond,
	Heartbeat:   400 * time.Millisecond,
	DelayMin:    120 * time.Millisecond,
	DelayMax:    350 * time.Millisecond,
}

func (t Timing) scaled(speed float64) Timing {
	f := func(d time.Duration) time.Duration { return time.Duration(float64(d) / speed) }
	return Timing{f(t.ElectionMin), f(t.ElectionMax), f(t.Heartbeat), f(t.DelayMin), f(t.DelayMax)}
}

type Options struct {
	Nodes     int     // default 5
	Speed     float64 // default 1
	Timing    Timing  // default DefaultTiming
	SnapEvery uint64  // compact the log every this many commands, default 20
}

const (
	minSpeed     = 0.25
	maxSpeed     = 8
	maxDropRate  = 0.9
	maxMessages  = 2000
	maxProposals = 20
)

// Sim owns the current cluster. Reset swaps in a fresh one and tears the
// old one down in the background, so no call here ever blocks for long.
type Sim struct {
	opts  Options
	start time.Time

	mu    sync.Mutex
	c     *cluster
	speed float64
	drop  float64
	seq   int

	msgMu sync.Mutex
	msgs  []Message
}

func New(opts Options) *Sim {
	if opts.Nodes <= 0 {
		opts.Nodes = 5
	}
	if opts.Speed <= 0 {
		opts.Speed = 1
	}
	if opts.Timing == (Timing{}) {
		opts.Timing = DefaultTiming
	}
	if opts.SnapEvery == 0 {
		opts.SnapEvery = 20
	}
	s := &Sim{opts: opts, start: time.Now(), speed: opts.Speed}
	s.c = s.newCluster()
	return s
}

func (s *Sim) cluster() *cluster {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.c
}

func (s *Sim) since() float64 {
	return float64(time.Since(s.start).Microseconds()) / 1000
}

// Reset throws the whole cluster away and starts five empty nodes.
func (s *Sim) Reset() {
	c := s.newCluster()
	s.mu.Lock()
	old := s.c
	s.c = c
	s.mu.Unlock()
	s.msgMu.Lock()
	s.msgs = nil
	s.msgMu.Unlock()
	go old.shutdown()
}

func (s *Sim) Kill(id string) error       { return s.cluster().setWant(id, false) }
func (s *Sim) Restart(id string) error    { return s.cluster().setWant(id, true) }
func (s *Sim) Disconnect(id string) error { return s.cluster().disconnect(id, true) }
func (s *Sim) Connect(id string) error    { return s.cluster().disconnect(id, false) }
func (s *Sim) Heal()                      { s.cluster().heal() }

func (s *Sim) Partition(groups [][]string) error { return s.cluster().partition(groups) }

func (s *Sim) SetDropRate(p float64) {
	p = min(max(p, 0), maxDropRate)
	s.mu.Lock()
	s.drop = p
	c := s.c
	s.mu.Unlock()
	c.net.SetDropRate(p)
}

// SetSpeed scales every timeout and delay. Delays change straight away;
// the nodes have their timeouts fixed at creation, so the live ones are
// restarted on their own storage to pick up the new values.
func (s *Sim) SetSpeed(x float64) {
	x = min(max(x, minSpeed), maxSpeed)
	s.mu.Lock()
	if x == s.speed {
		s.mu.Unlock()
		return
	}
	s.speed = x
	c := s.c
	s.mu.Unlock()
	c.setTiming(s.opts.Timing.scaled(x))
}

// Messages returns and clears the messages seen since the last call.
func (s *Sim) Messages() []Message {
	s.msgMu.Lock()
	defer s.msgMu.Unlock()
	out := s.msgs
	s.msgs = nil
	if out == nil {
		out = []Message{}
	}
	return out
}

// Message is one RPC or reply on the wire, as reported by simnet.
type Message struct {
	From    string  `json:"from"`
	To      string  `json:"to"`
	Kind    string  `json:"kind"`
	Term    uint64  `json:"term"`
	Entries int     `json:"entries,omitempty"`
	Success bool    `json:"success,omitempty"`
	Dropped bool    `json:"dropped,omitempty"`
	Delay   float64 `json:"delay"` // ms
	At      float64 `json:"at"`    // ms since the sim started
}

func (s *Sim) observe() func(simnet.Event) {
	return func(ev simnet.Event) {
		m := Message{
			From: ev.From, To: ev.To, Kind: ev.Kind, Term: ev.Term,
			Entries: ev.Entries, Success: ev.Success, Dropped: ev.Dropped,
			Delay: float64(ev.Delay.Microseconds()) / 1000,
			At:    s.since(),
		}
		s.msgMu.Lock()
		if len(s.msgs) >= maxMessages {
			s.msgs = s.msgs[len(s.msgs)-maxMessages/2:]
		}
		s.msgs = append(s.msgs, m)
		s.msgMu.Unlock()
	}
}

func (s *Sim) newCluster() *cluster {
	s.mu.Lock()
	speed, drop := s.speed, s.drop
	s.mu.Unlock()
	t := s.opts.Timing.scaled(speed)
	c := &cluster{
		snapEvery: s.opts.SnapEvery,
		timing:    t,
		members:   map[string]*member{},
		disc:      map[string]bool{},
		committed: map[uint64]uint64{},
	}
	c.net = simnet.New(simnet.Options{MinDelay: t.DelayMin, MaxDelay: t.DelayMax, DropRate: drop})
	c.net.OnMessage(s.observe())
	for i := 1; i <= s.opts.Nodes; i++ {
		id := fmt.Sprintf("n%d", i)
		c.ids = append(c.ids, id)
		c.members[id] = &member{id: id, store: raft.NewMemoryStorage(), want: true}
	}
	for _, id := range c.ids {
		go c.reconcile(c.members[id])
	}
	return c
}

// Propose sends a command to target, or to the current leader if target
// is empty. Sending to a node that is not leader fails at once; sending to
// a leader that has been cut off succeeds but will never commit.
func (s *Sim) Propose(text, target string) ProposeResult {
	if _, _, _, err := parseCommand(text); err != nil {
		return ProposeResult{Error: err.Error()}
	}
	c := s.cluster()
	node, id := c.pick(target)
	if node == nil {
		if target == "" {
			return ProposeResult{Error: "no leader right now"}
		}
		return ProposeResult{Node: target, Error: target + " is down"}
	}
	idx, term, ok := node.Propose([]byte(text))
	if !ok {
		return ProposeResult{Node: id, Term: term, Error: id + " is not the leader"}
	}
	s.mu.Lock()
	s.seq++
	seq := s.seq
	s.mu.Unlock()
	c.addProposal(&proposal{Seq: seq, Node: id, Index: idx, Term: term, Text: text, At: s.since()})
	return ProposeResult{OK: true, Seq: seq, Node: id, Index: idx, Term: term}
}

type ProposeResult struct {
	OK    bool   `json:"ok"`
	Seq   int    `json:"seq,omitempty"`
	Node  string `json:"node,omitempty"`
	Index uint64 `json:"index,omitempty"`
	Term  uint64 `json:"term,omitempty"`
	Error string `json:"error,omitempty"`
}

type NodeState struct {
	ID        string            `json:"id"`
	Alive     bool              `json:"alive"`
	Connected bool              `json:"connected"` // false when individually cut off
	Group     int               `json:"group"`     // partition group, -1 if none
	Raft      raft.Status       `json:"raft"`      // last known status if dead
	Data      map[string]string `json:"data"`
	Applied   uint64            `json:"applied"`
	Restarts  int               `json:"restarts"`
}

type State struct {
	Now         float64         `json:"now"`
	Speed       float64         `json:"speed"`
	DropRate    float64         `json:"drop_rate"`
	Partitioned bool            `json:"partitioned"`
	Nodes       []NodeState     `json:"nodes"`
	Proposals   []ProposalState `json:"proposals"`
}

type ProposalState struct {
	Seq    int     `json:"seq"`
	Node   string  `json:"node"`
	Index  uint64  `json:"index"`
	Term   uint64  `json:"term"`
	Text   string  `json:"text"`
	At     float64 `json:"at"`
	Status string  `json:"status"` // pending, committed or lost
}

func (s *Sim) State() State {
	s.mu.Lock()
	c, speed, drop := s.c, s.speed, s.drop
	s.mu.Unlock()
	st := c.state()
	st.Now, st.Speed, st.DropRate = s.since(), speed, drop
	return st
}

// cluster is one incarnation of the five nodes and their network.
type cluster struct {
	ids       []string
	net       *simnet.Network
	snapEvery uint64

	mu        sync.Mutex
	timing    Timing
	timingGen int
	members   map[string]*member
	groups    [][]string // nil when not partitioned
	disc      map[string]bool
	committed map[uint64]uint64 // index -> term of every command seen applied
	props     []*proposal
	closed    bool
}

type member struct {
	id    string
	store *raft.MemoryStorage

	// opMu serialises start/stop for this member. Callers only record
	// what they want; reconcile does the slow part in its own goroutine,
	// so a kill and a restart in quick succession just end up in the
	// state that was asked for last.
	opMu sync.Mutex

	// guarded by cluster.mu
	want      bool
	node      *raft.Node
	gen       int // timingGen the running node was built with
	sm        *machine
	stop      chan struct{}
	exited    chan struct{}
	last      raft.Status
	restarts  int
	everAlive bool
}

type proposal struct {
	Seq   int
	Node  string
	Index uint64
	Term  uint64
	Text  string
	At    float64
}

func (c *cluster) member(id string) (*member, error) {
	m := c.members[id]
	if m == nil {
		return nil, fmt.Errorf("no node %q", id)
	}
	return m, nil
}

func (c *cluster) setWant(id string, want bool) error {
	c.mu.Lock()
	m, err := c.member(id)
	if err == nil {
		m.want = want
	}
	c.mu.Unlock()
	if err == nil {
		go c.reconcile(m)
	}
	return err
}

func (c *cluster) setTiming(t Timing) {
	c.net.SetDelay(t.DelayMin, t.DelayMax)
	c.mu.Lock()
	c.timing = t
	c.timingGen++
	c.mu.Unlock()
	for _, id := range c.ids {
		go c.reconcile(c.members[id])
	}
}

func (c *cluster) reconcile(m *member) {
	m.opMu.Lock()
	defer m.opMu.Unlock()
	for {
		c.mu.Lock()
		want := m.want && !c.closed
		running := m.node != nil
		stale := running && m.gen != c.timingGen
		c.mu.Unlock()
		switch {
		case running && (!want || stale):
			// a timing change is not a crash, so the applied data stays
			c.stopMember(m, !want)
		case !running && want:
			c.startMember(m)
		default:
			return
		}
	}
}

// startMember must be called with m.opMu held.
func (c *cluster) startMember(m *member) {
	c.mu.Lock()
	t, gen := c.timing, c.timingGen
	sm := m.sm
	if sm == nil {
		sm = newMachine()
	}
	c.mu.Unlock()

	ch := make(chan raft.ApplyMsg, 64)
	node, err := raft.NewNode(raft.Config{
		ID:                 m.id,
		Peers:              c.ids,
		ElectionTimeoutMin: t.ElectionMin,
		ElectionTimeoutMax: t.ElectionMax,
		HeartbeatInterval:  t.Heartbeat,
		Storage:            m.store,
		Transport:          c.net.Transport(m.id),
		ApplyCh:            ch,
	})
	if err != nil {
		// only possible with a corrupt storage, which MemoryStorage can't be
		panic(err)
	}
	stop, exited := make(chan struct{}), make(chan struct{})
	go c.consume(node, sm, ch, stop, exited)

	c.mu.Lock()
	if m.everAlive && m.sm == nil {
		m.restarts++
	}
	m.node, m.gen, m.sm, m.stop, m.exited = node, gen, sm, stop, exited
	m.everAlive = true
	c.mu.Unlock()

	c.net.Join(m.id, node)
	node.Start()
}

// stopMember must be called with m.opMu held. crash drops the node's
// in-memory state machine, as a real crash would.
func (c *cluster) stopMember(m *member, crash bool) {
	c.mu.Lock()
	node, stop, exited := m.node, m.stop, m.exited
	m.node = nil
	c.mu.Unlock()

	last := node.Status()
	c.net.Remove(m.id)
	node.Stop()
	close(stop)
	<-exited

	c.mu.Lock()
	last.Role = raft.Follower
	last.Peers = nil
	m.last = last
	if crash {
		m.sm = nil
	}
	c.mu.Unlock()
}

func (c *cluster) consume(node *raft.Node, sm *machine, ch <-chan raft.ApplyMsg, stop <-chan struct{}, exited chan<- struct{}) {
	defer close(exited)
	for {
		select {
		case <-stop:
			return
		case msg := <-ch:
			if msg.SnapshotValid {
				sm.restore(msg.SnapshotIndex, msg.Snapshot)
				continue
			}
			if !msg.CommandValid || !sm.apply(msg.Index, msg.Command) {
				continue
			}
			c.mu.Lock()
			c.committed[msg.Index] = msg.Term
			c.mu.Unlock()
			if idx, data, ok := sm.snapshotDue(c.snapEvery); ok {
				node.Snapshot(idx, data)
			}
		}
	}
}

func (c *cluster) shutdown() {
	c.mu.Lock()
	c.closed = true
	c.mu.Unlock()
	c.net.OnMessage(nil)
	var wg sync.WaitGroup
	for _, id := range c.ids {
		wg.Add(1)
		go func(m *member) {
			defer wg.Done()
			c.reconcile(m)
		}(c.members[id])
	}
	wg.Wait()
}

func (c *cluster) disconnect(id string, cut bool) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, err := c.member(id); err != nil {
		return err
	}
	if cut {
		c.disc[id] = true
		c.net.Disconnect(id)
	} else {
		delete(c.disc, id)
		c.net.Connect(id)
	}
	return nil
}

func (c *cluster) partition(groups [][]string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	seen := map[string]bool{}
	var gs [][]string
	for _, g := range groups {
		var ids []string
		for _, id := range g {
			if _, err := c.member(id); err != nil {
				return err
			}
			if seen[id] {
				return fmt.Errorf("%s is in more than one group", id)
			}
			seen[id] = true
			ids = append(ids, id)
		}
		if len(ids) > 0 {
			gs = append(gs, ids)
		}
	}
	if len(gs) == 0 {
		return errors.New("no groups given")
	}
	c.groups = gs
	c.net.Partition(gs...)
	return nil
}

func (c *cluster) heal() {
	c.mu.Lock()
	c.groups = nil
	c.disc = map[string]bool{}
	c.net.Heal()
	c.mu.Unlock()
}

// pick returns the node a proposal should go to: target if given,
// otherwise the live leader with the highest term.
func (c *cluster) pick(target string) (*raft.Node, string) {
	c.mu.Lock()
	type cand struct {
		id   string
		node *raft.Node
	}
	var live []cand
	for _, id := range c.ids {
		if n := c.members[id].node; n != nil {
			live = append(live, cand{id, n})
		}
	}
	c.mu.Unlock()

	if target != "" {
		for _, l := range live {
			if l.id == target {
				return l.node, l.id
			}
		}
		return nil, target
	}
	var best *raft.Node
	var bestID string
	var bestTerm uint64
	for _, l := range live {
		st := l.node.Status()
		if st.Role == raft.Leader && (best == nil || st.Term > bestTerm) {
			best, bestID, bestTerm = l.node, l.id, st.Term
		}
	}
	return best, bestID
}

func (c *cluster) addProposal(p *proposal) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.props = append(c.props, p)
	if len(c.props) > maxProposals {
		c.props = slices.Clone(c.props[len(c.props)-maxProposals:])
	}
}

func (c *cluster) state() State {
	type snap struct {
		m         *member
		node      *raft.Node
		sm        *machine
		connected bool
		group     int
		last      raft.Status
		restarts  int
	}
	c.mu.Lock()
	snaps := make([]snap, 0, len(c.ids))
	for _, id := range c.ids {
		m := c.members[id]
		group := -1
		for g, ids := range c.groups {
			if slices.Contains(ids, id) {
				group = g
			}
		}
		snaps = append(snaps, snap{m, m.node, m.sm, !c.disc[id], group, m.last, m.restarts})
	}
	partitioned := c.groups != nil
	c.mu.Unlock()

	st := State{Partitioned: partitioned}
	var applied uint64
	for _, s := range snaps {
		ns := NodeState{ID: s.m.id, Connected: s.connected, Group: s.group, Restarts: s.restarts}
		if s.node != nil {
			ns.Alive = true
			ns.Raft = s.node.Status()
		} else {
			ns.Raft = s.last
			ns.Raft.ID = s.m.id
		}
		if s.sm != nil {
			ns.Data, ns.Applied = s.sm.copyData()
		} else {
			ns.Data = map[string]string{}
		}
		applied = max(applied, ns.Applied)
		st.Nodes = append(st.Nodes, ns)
	}

	// Read after the machines: a command is recorded in committed before
	// anything later is applied, so once some machine is past an index
	// with nothing recorded there, that slot went to a no-op.
	c.mu.Lock()
	for _, p := range c.props {
		ps := ProposalState{Seq: p.Seq, Node: p.Node, Index: p.Index, Term: p.Term, Text: p.Text, At: p.At, Status: "pending"}
		if t, ok := c.committed[p.Index]; ok {
			if t == p.Term {
				ps.Status = "committed"
			} else {
				ps.Status = "lost"
			}
		} else if applied > p.Index || committedOther(st.Nodes, p.Index, p.Term) {
			ps.Status = "lost"
		}
		st.Proposals = append(st.Proposals, ps)
	}
	c.mu.Unlock()
	return st
}

// committedOther reports whether some live node has committed a different
// entry (usually a new leader's no-op) at index.
func committedOther(nodes []NodeState, index, term uint64) bool {
	for _, n := range nodes {
		if !n.Alive || n.Raft.CommitIndex < index {
			continue
		}
		for _, e := range n.Raft.RecentEntries {
			if e.Index == index && e.Term != term {
				return true
			}
		}
	}
	return false
}
