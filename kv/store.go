package kv

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/Om-Patel88/raft-kv/raft"
)

var (
	// ErrNotLeader is wrapped by *NotLeaderError; test for it with errors.Is.
	ErrNotLeader = errors.New("kv: not the leader")
	// ErrLeadershipLost means the request was proposed but this node lost
	// leadership before it was known to commit. It may or may not have
	// been applied; retrying with the same ClientID and Seq is safe.
	ErrLeadershipLost = errors.New("kv: leadership lost before the request committed")
	ErrTimeout        = errors.New("kv: timed out waiting for the request to commit")
	ErrClosed         = errors.New("kv: store is closed")
)

// NotLeaderError is returned when a request reaches a follower. Leader is
// the id of the node this one believes is leader, or "" if it doesn't know.
type NotLeaderError struct {
	Leader string
}

func (e *NotLeaderError) Error() string {
	if e.Leader == "" {
		return "kv: not the leader, leader unknown"
	}
	return fmt.Sprintf("kv: not the leader, try %s", e.Leader)
}

func (e *NotLeaderError) Unwrap() error { return ErrNotLeader }

type Config struct {
	ID                 string
	Peers              []string // every node id, including ID
	Storage            raft.Storage
	Transport          raft.Transport
	ElectionTimeoutMin time.Duration
	ElectionTimeoutMax time.Duration
	HeartbeatInterval  time.Duration

	// SnapshotThreshold is how many log indexes may be applied before the
	// store snapshots and lets raft discard its log. 0 disables snapshots.
	SnapshotThreshold int

	Logger *log.Logger // passed on to raft; nil means silent
}

type Status struct {
	raft.Status
	Keys int `json:"keys"`
}

type result struct {
	resp Response
	err  error
}

// waiter is a caller blocked in Do on the entry it proposed at some index.
type waiter struct {
	term uint64
	ch   chan result // buffered, receives exactly once
}

type Store struct {
	node      *raft.Node
	applyCh   chan raft.ApplyMsg
	threshold uint64
	logger    *log.Logger

	mu       sync.Mutex
	sm       *machine
	applied  uint64 // last index applied to sm, command or snapshot
	lastSnap uint64
	waiters  map[uint64]*waiter
	closed   bool

	started  bool // under mu
	done     chan struct{}
	loopDone chan struct{}
	once     sync.Once
}

// NewStore builds the store and its raft node but does not start them, so
// the caller can wire the node into its transport first (see Raft).
func NewStore(cfg Config) (*Store, error) {
	if cfg.SnapshotThreshold < 0 {
		return nil, errors.New("kv: negative SnapshotThreshold")
	}
	s := &Store{
		applyCh:   make(chan raft.ApplyMsg, 64),
		threshold: uint64(cfg.SnapshotThreshold),
		logger:    cfg.Logger,
		sm:        newMachine(),
		waiters:   map[uint64]*waiter{},
		done:      make(chan struct{}),
		loopDone:  make(chan struct{}),
	}
	node, err := raft.NewNode(raft.Config{
		ID:                 cfg.ID,
		Peers:              cfg.Peers,
		ElectionTimeoutMin: cfg.ElectionTimeoutMin,
		ElectionTimeoutMax: cfg.ElectionTimeoutMax,
		HeartbeatInterval:  cfg.HeartbeatInterval,
		Storage:            cfg.Storage,
		Transport:          cfg.Transport,
		ApplyCh:            s.applyCh,
		Logger:             cfg.Logger,
	})
	if err != nil {
		return nil, err
	}
	s.node = node
	return s, nil
}

// Raft returns the underlying node, mainly so its RPC handlers can be
// registered with a transport.
func (s *Store) Raft() *raft.Node { return s.node }

func (s *Store) Start() {
	s.mu.Lock()
	if s.started || s.closed {
		s.mu.Unlock()
		return
	}
	s.started = true
	s.mu.Unlock()
	go s.applyLoop()
	s.node.Start()
}

// Close stops the raft node and the apply loop. Callers still waiting in
// Do get ErrClosed. Storage is left for the caller to close.
func (s *Store) Close() {
	s.once.Do(func() {
		s.node.Stop()
		s.mu.Lock()
		s.closed = true
		started := s.started
		for idx, w := range s.waiters {
			w.ch <- result{err: ErrClosed}
			delete(s.waiters, idx)
		}
		s.mu.Unlock()
		close(s.done)
		if started {
			<-s.loopDone
		}
	})
}

func (s *Store) Status() Status {
	st := Status{Status: s.node.Status()}
	s.mu.Lock()
	st.Keys = len(s.sm.data)
	s.mu.Unlock()
	return st
}

// Do runs one request and waits until it has been applied.
//
// Every request goes through the raft log, reads included. That makes
// reads linearizable without any extra machinery: a deposed leader can
// never answer from stale state, because its read has to commit first.
// The price is a log entry and a round of replication per read, which
// ReadIndex or leader leases would avoid.
func (s *Store) Do(ctx context.Context, req Request) (Response, error) {
	if err := req.validate(); err != nil {
		return Response{}, err
	}
	cmd, err := encodeRequest(req)
	if err != nil {
		return Response{}, err
	}

	// Hold mu across Propose so the apply loop can't get to this index
	// before the waiter is registered.
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return Response{}, ErrClosed
	}
	idx, term, ok := s.node.Propose(cmd)
	if !ok {
		s.mu.Unlock()
		return Response{}, &NotLeaderError{Leader: s.leaderHint()}
	}
	w := &waiter{term: term, ch: make(chan result, 1)}
	s.waiters[idx] = w
	s.mu.Unlock()

	// Raft never delivers no-ops, so after a leader change the apply loop
	// may not see anything at or past idx for a while. Watching the term
	// lets the caller give up and retry elsewhere sooner.
	tick := time.NewTicker(50 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case r := <-w.ch:
			return r.resp, r.err
		case <-ctx.Done():
			s.dropWaiter(idx, w)
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				return Response{}, ErrTimeout
			}
			return Response{}, ctx.Err()
		case <-tick.C:
			if s.node.Status().Term != term {
				s.dropWaiter(idx, w)
				return Response{}, ErrLeadershipLost
			}
		}
	}
}

func (s *Store) leaderHint() string {
	st := s.node.Status()
	if st.Leader == st.ID {
		return ""
	}
	return st.Leader
}

func (s *Store) dropWaiter(idx uint64, w *waiter) {
	s.mu.Lock()
	if s.waiters[idx] == w {
		delete(s.waiters, idx)
	}
	s.mu.Unlock()
}

func (s *Store) logf(format string, args ...any) {
	if s.logger != nil {
		s.logger.Printf("kv: "+format, args...)
	}
}

func (s *Store) applyLoop() {
	defer close(s.loopDone)
	for {
		select {
		case <-s.done:
			return
		case m := <-s.applyCh:
			switch {
			case m.CommandValid:
				s.applyCommand(m)
			case m.SnapshotValid:
				s.applySnapshot(m)
			}
		}
	}
}

func (s *Store) applyCommand(m raft.ApplyMsg) {
	s.mu.Lock()
	if m.Index <= s.applied {
		s.mu.Unlock()
		return
	}
	var res result
	req, err := decodeRequest(m.Command)
	if err != nil {
		// Can't happen with entries written by Do, but a bad entry must
		// still be skipped the same way on every node.
		s.logf("skipping undecodable entry %d: %v", m.Index, err)
		res.err = fmt.Errorf("kv: undecodable entry: %w", err)
	} else {
		res.resp = s.sm.apply(req)
	}
	s.applied = m.Index

	if w, ok := s.waiters[m.Index]; ok {
		delete(s.waiters, m.Index)
		if w.term == m.Term {
			w.ch <- res
		} else {
			// someone else's entry took our slot
			w.ch <- result{err: ErrLeadershipLost}
		}
	}
	s.failWaitersUpTo(m.Index)

	var snap []byte
	if s.threshold > 0 && m.Index-s.lastSnap >= s.threshold {
		snap, err = s.sm.snapshot()
		if err != nil {
			s.logf("snapshot at %d: %v", m.Index, err)
			snap = nil
		} else {
			s.lastSnap = m.Index
		}
	}
	s.mu.Unlock()

	if snap != nil {
		s.node.Snapshot(m.Index, snap)
	}
}

// failWaitersUpTo fails anyone still waiting on an index at or below idx:
// that index has been applied, and it wasn't their entry. Called with mu
// held.
func (s *Store) failWaitersUpTo(idx uint64) {
	for i, w := range s.waiters {
		if i <= idx {
			delete(s.waiters, i)
			w.ch <- result{err: ErrLeadershipLost}
		}
	}
}

func (s *Store) applySnapshot(m raft.ApplyMsg) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if m.SnapshotIndex <= s.applied {
		return
	}
	if err := s.sm.restore(m.Snapshot); err != nil {
		// The snapshot came from a peer's state machine running this same
		// code, so this means real corruption.
		panic(fmt.Sprintf("kv: restoring snapshot at %d: %v", m.SnapshotIndex, err))
	}
	s.applied = m.SnapshotIndex
	s.lastSnap = m.SnapshotIndex
	// Whatever these callers proposed is either inside the snapshot or
	// lost. Either way they have to retry, and the session table will
	// answer for writes that did make it.
	s.failWaitersUpTo(m.SnapshotIndex)
}
