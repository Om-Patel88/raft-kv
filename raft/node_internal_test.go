package raft

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

type nopTransport struct{}

var errNop = errors.New("no network")

func (nopTransport) RequestVote(context.Context, string, *RequestVoteArgs) (*RequestVoteReply, error) {
	return nil, errNop
}
func (nopTransport) AppendEntries(context.Context, string, *AppendEntriesArgs) (*AppendEntriesReply, error) {
	return nil, errNop
}
func (nopTransport) InstallSnapshot(context.Context, string, *InstallSnapshotArgs) (*InstallSnapshotReply, error) {
	return nil, errNop
}

// terms builds a log whose entry i+1 has term ts[i].
func terms(ts ...uint64) []Entry {
	es := make([]Entry, len(ts))
	for i, t := range ts {
		es[i] = Entry{Index: uint64(i + 1), Term: t, Data: []byte{byte(i)}}
	}
	return es
}

// newTestNode returns an unstarted node; handlers work without Start.
func newTestNode(t *testing.T, st Storage) *Node {
	t.Helper()
	n, err := NewNode(Config{
		ID: "a", Peers: []string{"a", "b", "c"},
		Storage: st, Transport: nopTransport{}, ApplyCh: make(chan ApplyMsg, 100),
	})
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func TestAppendEntriesConflictReply(t *testing.T) {
	st := NewMemoryStorage()
	st.SaveHardState(HardState{Term: 5})
	st.Append(terms(1, 1, 2, 2, 2, 4))
	n := newTestNode(t, st)

	cases := []struct {
		name          string
		prevIdx       uint64
		prevTerm      uint64
		ok            bool
		conflictTerm  uint64
		conflictIndex uint64
	}{
		{"match", 4, 2, true, 0, 0},
		{"too short", 9, 5, false, 0, 7},
		{"term mismatch mid-run", 5, 3, false, 2, 3},
		{"term mismatch first of run", 3, 1, false, 2, 3},
		{"mismatch on last", 6, 5, false, 4, 6},
		{"empty prefix always matches", 0, 0, true, 0, 0},
	}
	for _, c := range cases {
		r := n.HandleAppendEntries(&AppendEntriesArgs{Term: 5, LeaderID: "b", PrevLogIndex: c.prevIdx, PrevLogTerm: c.prevTerm})
		if r.Success != c.ok || r.ConflictTerm != c.conflictTerm || r.ConflictIndex != c.conflictIndex {
			t.Errorf("%s: got %+v", c.name, r)
		}
	}
}

func TestAppendEntriesTruncatesOnlyOnConflict(t *testing.T) {
	st := NewMemoryStorage()
	st.Append(terms(1, 1, 1, 1))
	n := newTestNode(t, st)

	// a delayed AppendEntries carrying a prefix we already have must not
	// truncate the entries after it
	r := n.HandleAppendEntries(&AppendEntriesArgs{Term: 1, LeaderID: "b", Entries: terms(1, 1)})
	if !r.Success || n.Status().LastIndex != 4 {
		t.Fatalf("reply %+v, last index %d", r, n.Status().LastIndex)
	}

	// a real conflict at index 3 replaces the tail
	r = n.HandleAppendEntries(&AppendEntriesArgs{
		Term: 2, LeaderID: "b", PrevLogIndex: 2, PrevLogTerm: 1,
		Entries: []Entry{{Index: 3, Term: 2}}, LeaderCommit: 3,
	})
	if !r.Success {
		t.Fatalf("reply %+v", r)
	}
	s := n.Status()
	if s.LastIndex != 3 || s.CommitIndex != 3 {
		t.Fatalf("status %+v", s)
	}
	saved, _ := st.Load()
	want := append(terms(1, 1), Entry{Index: 3, Term: 2})
	if !reflect.DeepEqual(saved.Entries, want) {
		t.Fatalf("storage has %+v", saved.Entries)
	}
	if saved.HardState.Term != 2 {
		t.Fatalf("term not persisted: %+v", saved.HardState)
	}
}

func TestAppendEntriesCommitBoundedByNewEntries(t *testing.T) {
	st := NewMemoryStorage()
	st.Append(terms(1, 1, 1, 1, 1))
	n := newTestNode(t, st)
	// leader commit is 5, but this request only vouches for our log up to
	// index 2, so we may only commit that far
	n.HandleAppendEntries(&AppendEntriesArgs{Term: 1, LeaderID: "b", PrevLogIndex: 2, PrevLogTerm: 1, LeaderCommit: 5})
	if c := n.Status().CommitIndex; c != 2 {
		t.Fatalf("commit index = %d, want 2", c)
	}
}

func TestRequestVoteRules(t *testing.T) {
	st := NewMemoryStorage()
	st.SaveHardState(HardState{Term: 3})
	st.Append(terms(1, 2, 3))
	n := newTestNode(t, st)

	vote := func(term uint64, cand string, lastIdx, lastTerm uint64) bool {
		return n.HandleRequestVote(&RequestVoteArgs{Term: term, CandidateID: cand, LastLogIndex: lastIdx, LastLogTerm: lastTerm}).VoteGranted
	}
	if vote(2, "b", 10, 3) {
		t.Error("granted vote for a stale term")
	}
	if vote(4, "b", 2, 3) {
		t.Error("granted vote to a shorter log with the same last term")
	}
	if vote(4, "b", 9, 2) {
		t.Error("granted vote to a log with an older last term")
	}
	if !vote(4, "c", 3, 3) {
		t.Error("refused an up-to-date candidate")
	}
	if vote(4, "b", 5, 4) {
		t.Error("voted twice in term 4")
	}
	if !vote(4, "c", 3, 3) {
		t.Error("repeated request from the same candidate should be granted again")
	}
	saved, _ := st.Load()
	if saved.HardState != (HardState{Term: 4, VotedFor: "c"}) {
		t.Errorf("hard state not persisted: %+v", saved.HardState)
	}

	// a vote survives a restart
	n2 := newTestNode(t, st)
	if n2.HandleRequestVote(&RequestVoteArgs{Term: 4, CandidateID: "b", LastLogIndex: 5, LastLogTerm: 4}).VoteGranted {
		t.Error("restarted node forgot its vote")
	}
}

func TestInstallSnapshotKeepsMatchingSuffix(t *testing.T) {
	st := NewMemoryStorage()
	st.Append(terms(1, 1, 1, 2, 2))
	n := newTestNode(t, st)

	n.HandleInstallSnapshot(&InstallSnapshotArgs{Term: 2, LeaderID: "b", LastIncludedIndex: 3, LastIncludedTerm: 1, Data: []byte("s3")})
	s := n.Status()
	if s.SnapshotIndex != 3 || s.LastIndex != 5 || s.CommitIndex != 3 {
		t.Fatalf("status %+v", s)
	}

	// snapshot that disagrees with our log: everything goes
	n.HandleInstallSnapshot(&InstallSnapshotArgs{Term: 3, LeaderID: "c", LastIncludedIndex: 4, LastIncludedTerm: 3, Data: []byte("s4")})
	s = n.Status()
	if s.SnapshotIndex != 4 || s.LastIndex != 4 {
		t.Fatalf("status %+v", s)
	}
	saved, _ := st.Load()
	if saved.Snapshot.Index != 4 || string(saved.Snapshot.Data) != "s4" || len(saved.Entries) != 0 {
		t.Fatalf("storage %+v", saved)
	}

	// an older snapshot is ignored
	n.HandleInstallSnapshot(&InstallSnapshotArgs{Term: 3, LeaderID: "c", LastIncludedIndex: 2, LastIncludedTerm: 1})
	if n.Status().SnapshotIndex != 4 {
		t.Fatal("older snapshot replaced a newer one")
	}
}

func TestRestartDeliversSnapshotFirst(t *testing.T) {
	st := NewMemoryStorage()
	st.Append(terms(1, 1, 1, 1))
	st.SaveSnapshot(Snapshot{Index: 3, Term: 1, Data: []byte("img")})

	ch := make(chan ApplyMsg, 10)
	n, err := NewNode(Config{ID: "a", Peers: []string{"a"}, Storage: st, Transport: nopTransport{}, ApplyCh: ch})
	if err != nil {
		t.Fatal(err)
	}
	n.Start()
	defer n.Stop()

	m := <-ch
	if !m.SnapshotValid || m.SnapshotIndex != 3 || string(m.Snapshot) != "img" {
		t.Fatalf("first message %+v", m)
	}
	// single node: becomes leader, commits entry 4 and its own no-op at 5
	m = <-ch
	if !m.CommandValid || m.Index != 4 {
		t.Fatalf("second message %+v", m)
	}
}

func TestStopIsIdempotent(t *testing.T) {
	n := newTestNode(t, NewMemoryStorage())
	n.Start()
	n.Stop()
	n.Stop()
	if _, _, ok := n.Propose([]byte("x")); ok {
		t.Fatal("stopped node accepted a proposal")
	}
	// handlers on a stopped node must not touch storage
	r := n.HandleRequestVote(&RequestVoteArgs{Term: 99, CandidateID: "b"})
	if r.VoteGranted {
		t.Fatal("stopped node voted")
	}
}

func TestMemoryStorage(t *testing.T) {
	m := NewMemoryStorage()
	if err := m.Append(terms(1, 1, 2)); err != nil {
		t.Fatal(err)
	}
	if err := m.Append([]Entry{{Index: 5, Term: 2}}); !errors.Is(err, ErrNonContiguous) {
		t.Fatalf("gap: %v", err)
	}
	m.TruncateSuffix(3)
	m.Append([]Entry{{Index: 3, Term: 3}})
	m.SaveSnapshot(Snapshot{Index: 2, Term: 1, Data: []byte("x")})
	st, _ := m.Load()
	if st.Snapshot.Index != 2 || len(st.Entries) != 1 || st.Entries[0].Term != 3 {
		t.Fatalf("state %+v", st)
	}
	// Load hands out copies
	st.Snapshot.Data[0] = 'y'
	st2, _ := m.Load()
	if string(st2.Snapshot.Data) != "x" {
		t.Fatal("Load returned shared memory")
	}
	if err := m.TruncateSuffix(2); err == nil {
		t.Fatal("truncate into snapshot should fail")
	}
}

func TestStatusRecentEntries(t *testing.T) {
	st := NewMemoryStorage()
	es := terms(1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1)
	es[11].Data = []byte("set x = 1\n")
	st.Append(es)
	n := newTestNode(t, st)
	s := n.Status()
	if len(s.RecentEntries) != 10 || s.RecentEntries[0].Index != 3 || s.RecentEntries[9].Index != 12 {
		t.Fatalf("recent entries %+v", s.RecentEntries)
	}
	if p := s.RecentEntries[9].Preview; p != "set x = 1." {
		t.Fatalf("preview %q", p)
	}
}
