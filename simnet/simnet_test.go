package simnet

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Om-Patel88/raft-kv/raft"
)

type fakeNode struct {
	name    string
	calls   atomic.Int64
	mu      sync.Mutex
	lastAE  *raft.AppendEntriesArgs
	lastSnp *raft.InstallSnapshotArgs
}

func (f *fakeNode) HandleRequestVote(a *raft.RequestVoteArgs) *raft.RequestVoteReply {
	f.calls.Add(1)
	return &raft.RequestVoteReply{Term: a.Term, VoteGranted: true}
}

func (f *fakeNode) HandleAppendEntries(a *raft.AppendEntriesArgs) *raft.AppendEntriesReply {
	f.calls.Add(1)
	f.mu.Lock()
	f.lastAE = a
	f.mu.Unlock()
	return &raft.AppendEntriesReply{Term: a.Term, Success: true}
}

func (f *fakeNode) HandleInstallSnapshot(a *raft.InstallSnapshotArgs) *raft.InstallSnapshotReply {
	f.calls.Add(1)
	f.mu.Lock()
	f.lastSnp = a
	f.mu.Unlock()
	return &raft.InstallSnapshotReply{Term: a.Term}
}

func setup(t *testing.T, opts Options, ids ...string) (*Network, map[string]raft.Transport, map[string]*fakeNode) {
	t.Helper()
	net := New(opts)
	trs := map[string]raft.Transport{}
	nodes := map[string]*fakeNode{}
	for _, id := range ids {
		nodes[id] = &fakeNode{name: id}
		trs[id] = net.Join(id, nodes[id])
	}
	return net, trs, nodes
}

func vote(tr raft.Transport, to string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	_, err := tr.RequestVote(ctx, to, &raft.RequestVoteArgs{Term: 1})
	return err
}

func TestDelivery(t *testing.T) {
	_, trs, nodes := setup(t, Options{Seed: 1}, "a", "b")
	ctx := context.Background()
	r, err := trs["a"].RequestVote(ctx, "b", &raft.RequestVoteArgs{Term: 4, CandidateID: "a"})
	if err != nil || !r.VoteGranted || r.Term != 4 {
		t.Fatalf("reply %+v err %v", r, err)
	}
	if nodes["b"].calls.Load() != 1 {
		t.Fatal("handler not called")
	}
	if _, err := trs["a"].RequestVote(ctx, "nobody", &raft.RequestVoteArgs{}); err == nil {
		t.Fatal("send to unknown node should fail")
	}
}

func TestDisconnect(t *testing.T) {
	net, trs, nodes := setup(t, Options{Seed: 1}, "a", "b", "c")
	net.Disconnect("b")
	if vote(trs["a"], "b") == nil || vote(trs["b"], "a") == nil {
		t.Fatal("disconnected node should be unreachable in both directions")
	}
	if nodes["b"].calls.Load() != 0 {
		t.Fatal("disconnected node received a message")
	}
	if err := vote(trs["a"], "c"); err != nil {
		t.Fatalf("a->c should still work: %v", err)
	}
	net.Connect("b")
	if err := vote(trs["a"], "b"); err != nil {
		t.Fatalf("after reconnect: %v", err)
	}
}

func TestPartition(t *testing.T) {
	net, trs, _ := setup(t, Options{Seed: 1}, "a", "b", "c", "d", "e")
	net.Partition([]string{"a", "b"}, []string{"c", "d"})
	cases := []struct {
		from, to string
		ok       bool
	}{
		{"a", "b", true}, {"b", "a", true}, {"c", "d", true},
		{"a", "c", false}, {"d", "b", false},
		{"e", "a", false}, {"a", "e", false}, // e is in no group
	}
	for _, c := range cases {
		err := vote(trs[c.from], c.to)
		if (err == nil) != c.ok {
			t.Errorf("%s->%s: err = %v, want ok=%v", c.from, c.to, err, c.ok)
		}
		if net.Reachable(c.from, c.to) != c.ok {
			t.Errorf("Reachable(%s,%s) != %v", c.from, c.to, c.ok)
		}
	}
	net.Disconnect("a")
	net.Heal()
	for _, from := range []string{"a", "c", "e"} {
		if err := vote(trs[from], "b"); err != nil {
			t.Errorf("after heal %s->b: %v", from, err)
		}
	}
}

func TestDropRate(t *testing.T) {
	net, trs, _ := setup(t, Options{Seed: 7, DropRate: 1}, "a", "b")
	for i := 0; i < 20; i++ {
		if err := vote(trs["a"], "b"); !errors.Is(err, ErrDropped) {
			t.Fatalf("drop rate 1: err = %v", err)
		}
	}

	net.SetDropRate(0.3)
	fails := 0
	const n = 2000
	for i := 0; i < n; i++ {
		if vote(trs["a"], "b") != nil {
			fails++
		}
	}
	// a round trip survives with probability 0.7*0.7 = 0.49
	if fails < n*40/100 || fails > n*62/100 {
		t.Fatalf("drop rate 0.3: %d/%d round trips failed", fails, n)
	}
}

func TestRepliesDroppedIndependently(t *testing.T) {
	net, trs, nodes := setup(t, Options{Seed: 3, DropRate: 0.5}, "a", "b")
	var reqDropped, repDropped atomic.Int64
	net.OnMessage(func(e Event) {
		if !e.Dropped {
			return
		}
		if e.Kind == "RequestVote" {
			reqDropped.Add(1)
		} else {
			repDropped.Add(1)
		}
	})
	for i := 0; i < 500; i++ {
		vote(trs["a"], "b")
	}
	if reqDropped.Load() == 0 || repDropped.Load() == 0 {
		t.Fatalf("requests dropped %d, replies dropped %d", reqDropped.Load(), repDropped.Load())
	}
	// every request that was not dropped reached the handler, even if
	// its reply was lost
	if got := nodes["b"].calls.Load(); got != 500-reqDropped.Load() {
		t.Fatalf("handler calls %d, want %d", got, 500-reqDropped.Load())
	}
}

func TestDelayRespectsContext(t *testing.T) {
	_, trs, nodes := setup(t, Options{Seed: 1, MinDelay: time.Second, MaxDelay: time.Second}, "a", "b")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := trs["a"].RequestVote(ctx, "b", &raft.RequestVoteArgs{})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v", err)
	}
	if time.Since(start) > 500*time.Millisecond {
		t.Fatal("call did not return at the deadline")
	}
	if nodes["b"].calls.Load() != 0 {
		t.Fatal("message delivered after its context expired")
	}
}

func TestDelayApplied(t *testing.T) {
	net, trs, _ := setup(t, Options{Seed: 1}, "a", "b")
	net.SetDelay(20*time.Millisecond, 30*time.Millisecond)
	start := time.Now()
	if err := vote(trs["a"], "b"); err != nil {
		t.Fatal(err)
	}
	// request and reply are delayed separately
	if d := time.Since(start); d < 40*time.Millisecond {
		t.Fatalf("round trip took %v, want >= 40ms", d)
	}
}

func TestMessagesAreCopied(t *testing.T) {
	_, trs, nodes := setup(t, Options{Seed: 1}, "a", "b")
	data := []byte("hello")
	args := &raft.AppendEntriesArgs{Term: 1, Entries: []raft.Entry{{Index: 1, Term: 1, Data: data}}}
	if _, err := trs["a"].AppendEntries(context.Background(), "b", args); err != nil {
		t.Fatal(err)
	}
	data[0] = 'X'
	args.Entries[0].Index = 99
	got := nodes["b"].lastAE
	if string(got.Entries[0].Data) != "hello" || got.Entries[0].Index != 1 {
		t.Fatalf("receiver shares memory with sender: %+v", got.Entries[0])
	}

	snap := []byte("snapshot")
	trs["a"].InstallSnapshot(context.Background(), "b", &raft.InstallSnapshotArgs{Data: snap})
	snap[0] = 'X'
	if string(nodes["b"].lastSnp.Data) != "snapshot" {
		t.Fatal("snapshot data shared across the wire")
	}
}

func TestRemoveAndRejoin(t *testing.T) {
	net, trs, _ := setup(t, Options{Seed: 1}, "a", "b")
	net.Remove("b")
	if vote(trs["a"], "b") == nil {
		t.Fatal("removed node reachable")
	}
	fresh := &fakeNode{name: "b2"}
	net.Join("b", fresh)
	if err := vote(trs["a"], "b"); err != nil {
		t.Fatal(err)
	}
	if fresh.calls.Load() != 1 {
		t.Fatal("rejoined handler not used")
	}
}

// A message in flight when its destination restarts must not reach the
// new incarnation.
func TestInFlightMessageToRestartedNode(t *testing.T) {
	net, trs, _ := setup(t, Options{Seed: 1, MinDelay: 50 * time.Millisecond, MaxDelay: 50 * time.Millisecond}, "a", "b")
	fresh := &fakeNode{name: "b2"}
	done := make(chan error)
	go func() { done <- vote(trs["a"], "b") }()
	time.Sleep(10 * time.Millisecond)
	net.Remove("b")
	net.Join("b", fresh)
	if err := <-done; err == nil {
		t.Fatal("expected in-flight message to fail")
	}
	if fresh.calls.Load() != 0 {
		t.Fatal("new incarnation got the old message")
	}
}

func TestObserver(t *testing.T) {
	net, trs, _ := setup(t, Options{Seed: 1}, "a", "b")
	var mu sync.Mutex
	var events []Event
	net.OnMessage(func(e Event) {
		mu.Lock()
		events = append(events, e)
		mu.Unlock()
	})
	trs["a"].AppendEntries(context.Background(), "b", &raft.AppendEntriesArgs{Term: 2, Entries: make([]raft.Entry, 3)})
	net.Disconnect("b")
	vote(trs["a"], "b")

	mu.Lock()
	defer mu.Unlock()
	if len(events) != 3 {
		t.Fatalf("got %d events: %+v", len(events), events)
	}
	if e := events[0]; e.Kind != "AppendEntries" || e.From != "a" || e.To != "b" || e.Entries != 3 || e.Dropped {
		t.Errorf("event 0 = %+v", e)
	}
	if e := events[1]; e.Kind != "AppendEntriesReply" || e.From != "b" || !e.Success {
		t.Errorf("event 1 = %+v", e)
	}
	if e := events[2]; e.Kind != "RequestVote" || !e.Dropped {
		t.Errorf("event 2 = %+v", e)
	}
}
