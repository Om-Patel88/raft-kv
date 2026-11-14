package raft

import (
	"errors"
	"fmt"
	"sync"
)

// HardState is the part of a node's state that must hit stable storage
// before it answers any RPC (Figure 2).
type HardState struct {
	Term     uint64
	VotedFor string
}

// Snapshot is the state machine image covering every entry up to and
// including Index. A zero Index means there is no snapshot.
type Snapshot struct {
	Index uint64
	Term  uint64
	Data  []byte
}

// PersistentState is everything Load hands back after a restart.
// Entries always start at Snapshot.Index+1 and are contiguous.
type PersistentState struct {
	HardState HardState
	Snapshot  Snapshot
	Entries   []Entry
}

// Storage is where a Node keeps its durable state. Every method must be
// durable when it returns. A Node calls these while holding its own lock,
// so implementations do not need to be safe for concurrent use by more
// than one node, but MemoryStorage and wal.WAL are anyway.
type Storage interface {
	Load() (PersistentState, error)
	SaveHardState(hs HardState) error

	// Append adds entries to the end of the log. The first entry must
	// directly follow the last stored entry (or the snapshot).
	Append(entries []Entry) error

	// TruncateSuffix drops every entry with Index >= from.
	TruncateSuffix(from uint64) error

	// SaveSnapshot stores snap and drops the entries it covers. If the
	// stored log has an entry at snap.Index with snap.Term, the entries
	// after it are kept; otherwise the whole log is discarded (§7).
	SaveSnapshot(snap Snapshot) error
}

var ErrNonContiguous = errors.New("raft: appended entries are not contiguous with the log")

// MemoryStorage keeps everything in memory. "Crashing" a node and building
// a new one on the same MemoryStorage behaves like a restart.
type MemoryStorage struct {
	mu      sync.Mutex
	hs      HardState
	snap    Snapshot
	entries []Entry
}

func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{}
}

func (m *MemoryStorage) Load() (PersistentState, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return PersistentState{
		HardState: m.hs,
		Snapshot:  Snapshot{Index: m.snap.Index, Term: m.snap.Term, Data: cloneBytes(m.snap.Data)},
		Entries:   cloneEntries(m.entries),
	}, nil
}

func (m *MemoryStorage) SaveHardState(hs HardState) error {
	m.mu.Lock()
	m.hs = hs
	m.mu.Unlock()
	return nil
}

func (m *MemoryStorage) lastIndex() uint64 {
	if len(m.entries) == 0 {
		return m.snap.Index
	}
	return m.entries[len(m.entries)-1].Index
}

func (m *MemoryStorage) Append(entries []Entry) error {
	if len(entries) == 0 {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if entries[0].Index != m.lastIndex()+1 {
		return fmt.Errorf("%w: have last %d, got %d", ErrNonContiguous, m.lastIndex(), entries[0].Index)
	}
	m.entries = append(m.entries, cloneEntries(entries)...)
	return nil
}

func (m *MemoryStorage) TruncateSuffix(from uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if from <= m.snap.Index {
		return fmt.Errorf("raft: truncate from %d would cut into snapshot at %d", from, m.snap.Index)
	}
	if from > m.lastIndex() {
		return nil
	}
	m.entries = m.entries[:from-m.snap.Index-1]
	return nil
}

func (m *MemoryStorage) SaveSnapshot(snap Snapshot) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if snap.Index <= m.snap.Index {
		return nil
	}
	var keep []Entry
	if snap.Index < m.lastIndex() {
		e := m.entries[snap.Index-m.snap.Index-1]
		if e.Term == snap.Term {
			keep = append([]Entry(nil), m.entries[snap.Index-m.snap.Index:]...)
		}
	}
	m.entries = keep
	m.snap = Snapshot{Index: snap.Index, Term: snap.Term, Data: cloneBytes(snap.Data)}
	return nil
}

func cloneBytes(b []byte) []byte {
	if b == nil {
		return nil
	}
	return append([]byte(nil), b...)
}

func cloneEntries(es []Entry) []Entry {
	if es == nil {
		return nil
	}
	out := make([]Entry, len(es))
	for i, e := range es {
		out[i] = e
		out[i].Data = cloneBytes(e.Data)
	}
	return out
}
