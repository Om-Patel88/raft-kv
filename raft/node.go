package raft

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand/v2"
	"slices"
	"sync"
	"time"
)

type Config struct {
	ID                  string
	Peers               []string // every node id in the cluster, including ID
	ElectionTimeoutMin  time.Duration
	ElectionTimeoutMax  time.Duration
	HeartbeatInterval   time.Duration
	MaxEntriesPerAppend int
	Storage             Storage
	Transport           Transport
	ApplyCh             chan<- ApplyMsg
	Logger              *log.Logger // nil means silent
}

const (
	defaultElectionMin   = 300 * time.Millisecond
	defaultElectionMax   = 600 * time.Millisecond
	defaultHeartbeat     = 100 * time.Millisecond
	defaultMaxEntries    = 64
	defaultRecentEntries = 10
)

type Node struct {
	cfg   Config
	id    string
	peers []string // everyone except id

	mu       sync.Mutex
	role     Role
	term     uint64
	votedFor string
	leader   string
	log      raftLog
	snapData []byte

	commitIndex uint64
	lastApplied uint64
	// set when a snapshot (from restart or InstallSnapshot) still has to be
	// handed to the service before any further entries
	snapPending bool

	nextIndex  map[string]uint64
	matchIndex map[string]uint64

	electionDeadline time.Time

	applyCond *sync.Cond
	triggers  map[string]chan struct{}

	started bool
	dead    bool
	ctx     context.Context
	cancel  context.CancelFunc
	stopCh  chan struct{}
	wg      sync.WaitGroup
}

func NewNode(cfg Config) (*Node, error) {
	if cfg.ID == "" {
		return nil, errors.New("raft: Config.ID is required")
	}
	if !slices.Contains(cfg.Peers, cfg.ID) {
		return nil, fmt.Errorf("raft: Peers must include the node's own id %q", cfg.ID)
	}
	if cfg.Storage == nil || cfg.Transport == nil || cfg.ApplyCh == nil {
		return nil, errors.New("raft: Storage, Transport and ApplyCh are required")
	}
	if cfg.ElectionTimeoutMin <= 0 {
		cfg.ElectionTimeoutMin = defaultElectionMin
	}
	if cfg.ElectionTimeoutMax <= cfg.ElectionTimeoutMin {
		cfg.ElectionTimeoutMax = 2 * cfg.ElectionTimeoutMin
	}
	if cfg.HeartbeatInterval <= 0 {
		cfg.HeartbeatInterval = defaultHeartbeat
	}
	if cfg.MaxEntriesPerAppend <= 0 {
		cfg.MaxEntriesPerAppend = defaultMaxEntries
	}

	st, err := cfg.Storage.Load()
	if err != nil {
		return nil, fmt.Errorf("raft: loading state: %w", err)
	}
	if len(st.Entries) > 0 && st.Entries[0].Index != st.Snapshot.Index+1 {
		return nil, fmt.Errorf("raft: stored log starts at %d but snapshot ends at %d",
			st.Entries[0].Index, st.Snapshot.Index)
	}

	n := &Node{
		cfg:      cfg,
		id:       cfg.ID,
		role:     Follower,
		term:     st.HardState.Term,
		votedFor: st.HardState.VotedFor,
		log:      newRaftLog(st.Snapshot.Index, st.Snapshot.Term, st.Entries),
		snapData: st.Snapshot.Data,
		triggers: make(map[string]chan struct{}),
		stopCh:   make(chan struct{}),
	}
	seen := map[string]bool{}
	for _, p := range cfg.Peers {
		if p != cfg.ID && !seen[p] {
			seen[p] = true
			n.peers = append(n.peers, p)
			n.triggers[p] = make(chan struct{}, 1)
		}
	}
	if st.Snapshot.Index > 0 {
		// Everything in the snapshot is committed; the applier hands it to
		// the service before anything else.
		n.commitIndex = st.Snapshot.Index
		n.snapPending = true
	}
	n.applyCond = sync.NewCond(&n.mu)
	n.ctx, n.cancel = context.WithCancel(context.Background())
	return n, nil
}

func (n *Node) Start() {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.started || n.dead {
		return
	}
	n.started = true
	n.resetElectionTimer()
	n.wg.Add(2 + len(n.peers))
	go n.ticker()
	go n.applier()
	for _, p := range n.peers {
		go n.replicator(p)
	}
	n.logf("started term=%d snap=%d last=%d", n.term, n.log.snapIndex(), n.log.lastIndex())
}

// Stop shuts the node down and waits for its goroutines. Once Stop returns
// the node never touches its Storage again, so a new Node can be built on
// the same Storage.
func (n *Node) Stop() {
	n.mu.Lock()
	if n.dead {
		n.mu.Unlock()
		n.wg.Wait()
		return
	}
	n.dead = true
	n.cancel()
	close(n.stopCh)
	n.applyCond.Broadcast()
	n.mu.Unlock()
	n.wg.Wait()
}

// Propose appends cmd to the leader's log. It returns straight away; the
// command is delivered on ApplyCh if and when it commits.
func (n *Node) Propose(cmd []byte) (index, term uint64, isLeader bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.dead || n.role != Leader {
		return 0, n.term, false
	}
	e := Entry{Index: n.log.lastIndex() + 1, Term: n.term, Type: EntryCommand, Data: cloneBytes(cmd)}
	n.appendLocal(e)
	n.advanceCommit()
	n.triggerAll()
	return e.Index, e.Term, true
}

func (n *Node) logf(format string, args ...any) {
	if n.cfg.Logger != nil {
		n.cfg.Logger.Printf("[%s t%d %s] "+format, append([]any{n.id, n.term, n.role}, args...)...)
	}
}

func (n *Node) quorum() int { return (len(n.peers)+1)/2 + 1 }

func (n *Node) electionTimeout() time.Duration {
	lo, hi := n.cfg.ElectionTimeoutMin, n.cfg.ElectionTimeoutMax
	return lo + rand.N(hi-lo)
}

func (n *Node) resetElectionTimer() {
	n.electionDeadline = time.Now().Add(n.electionTimeout())
}

func (n *Node) rpcTimeout() time.Duration {
	return n.cfg.ElectionTimeoutMin
}

// A failed write to stable storage leaves no safe way to carry on: the
// node may already have promised something it can no longer remember.
func (n *Node) must(err error) {
	if err != nil {
		panic(fmt.Sprintf("raft %s: storage failure: %v", n.id, err))
	}
}

func (n *Node) persistHardState() {
	n.must(n.cfg.Storage.SaveHardState(HardState{Term: n.term, VotedFor: n.votedFor}))
}

func (n *Node) appendLocal(es ...Entry) {
	n.must(n.cfg.Storage.Append(es))
	n.log.append(es...)
}

// becomeFollower moves to term (if newer) and follower role. The caller
// must already hold mu.
func (n *Node) becomeFollower(term uint64) {
	changed := false
	if term > n.term {
		n.term = term
		n.votedFor = ""
		n.leader = ""
		changed = true
	}
	if n.role != Follower {
		n.logf("stepping down")
		n.role = Follower
	}
	if changed {
		n.persistHardState()
	}
}

func (n *Node) triggerAll() {
	for _, p := range n.peers {
		n.trigger(p)
	}
}

func (n *Node) trigger(p string) {
	select {
	case n.triggers[p] <- struct{}{}:
	default:
	}
}

func (n *Node) ticker() {
	defer n.wg.Done()
	t := time.NewTimer(n.cfg.ElectionTimeoutMin)
	defer t.Stop()
	for {
		n.mu.Lock()
		if n.dead {
			n.mu.Unlock()
			return
		}
		var wait time.Duration
		if n.role == Leader {
			wait = n.cfg.ElectionTimeoutMin
		} else {
			wait = time.Until(n.electionDeadline)
			if wait <= 0 {
				n.startElection()
				wait = time.Until(n.electionDeadline)
			}
		}
		n.mu.Unlock()

		t.Reset(wait)
		select {
		case <-n.stopCh:
			return
		case <-t.C:
		}
	}
}

// applier delivers committed entries and snapshots on ApplyCh, in order.
// It never holds mu while sending.
func (n *Node) applier() {
	defer n.wg.Done()
	n.mu.Lock()
	for {
		for !n.dead && !n.snapPending && n.lastApplied >= n.commitIndex {
			n.applyCond.Wait()
		}
		if n.dead {
			n.mu.Unlock()
			return
		}

		if n.snapPending {
			msg := ApplyMsg{
				SnapshotValid: true,
				SnapshotIndex: n.log.snapIndex(),
				SnapshotTerm:  n.log.snapTerm(),
				Snapshot:      cloneBytes(n.snapData),
			}
			n.snapPending = false
			n.mu.Unlock()
			if !n.send(msg) {
				return
			}
			n.mu.Lock()
			if msg.SnapshotIndex > n.lastApplied {
				n.lastApplied = msg.SnapshotIndex
			}
			continue
		}

		lo := max(n.lastApplied+1, n.log.snapIndex()+1)
		hi := n.commitIndex
		if lo > hi {
			n.lastApplied = hi
			continue
		}
		ents := n.log.slice(lo, hi+1)
		n.mu.Unlock()

		for _, e := range ents {
			if e.Type != EntryCommand {
				continue
			}
			if !n.send(ApplyMsg{CommandValid: true, Index: e.Index, Term: e.Term, Command: e.Data}) {
				return
			}
		}

		n.mu.Lock()
		if hi > n.lastApplied {
			n.lastApplied = hi
		}
	}
}

func (n *Node) send(msg ApplyMsg) bool {
	select {
	case n.cfg.ApplyCh <- msg:
		return true
	case <-n.stopCh:
		return false
	}
}
