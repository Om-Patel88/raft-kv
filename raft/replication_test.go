package raft_test

import (
	"fmt"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Om-Patel88/raft-kv/raft"
	"github.com/Om-Patel88/raft-kv/simnet"
)

func cmd(prefix string, i int) string { return fmt.Sprintf("%s-%d", prefix, i) }

func randCmd() string { return fmt.Sprintf("r%d", rand.Uint64()) }

func TestBasicAgree(t *testing.T) {
	h := newHarness(t, 3)
	h.checkOneLeader()
	var last uint64
	for i := 0; i < 5; i++ {
		if c, _ := h.nCommitted(last + 1); c > 0 {
			t.Fatal("some node committed before Propose")
		}
		idx := h.one(cmd("basic", i), 3, false)
		if idx <= last {
			t.Fatalf("index went from %d to %d", last, idx)
		}
		last = idx
	}
}

func TestFollowerFailureAgree(t *testing.T) {
	h := newHarness(t, 3)
	h.one("a", 3, false)

	leader := h.checkOneLeader()
	h.disconnect(h.others(leader)[0])

	// the leader and the remaining follower are still a majority
	h.one("b", 2, false)
	time.Sleep(electionMax)
	h.one("c", 2, false)

	// now cut off the other follower too: nothing can commit
	leader2 := h.checkOneLeader()
	var cut []string
	for _, id := range h.others(leader2) {
		if h.isConnected(id) {
			h.disconnect(id)
			cut = append(cut, id)
		}
	}
	idx, _, ok := h.node(leader2).Propose([]byte("d"))
	if !ok {
		t.Fatal("leader rejected Propose")
	}
	time.Sleep(2 * electionMax)
	if c, _ := h.nCommitted(idx); c > 0 {
		t.Fatalf("%d nodes committed without a majority", c)
	}

	for _, id := range h.ids {
		h.connect(id)
	}
	h.one("e", 3, true)
}

func TestNoAgreeWithoutMajority(t *testing.T) {
	h := newHarness(t, 5)
	h.one("start", 5, false)

	leader := h.checkOneLeader()
	others := h.others(leader)
	for _, id := range others[:3] {
		h.disconnect(id)
	}

	idx, _, ok := h.node(leader).Propose([]byte("lonely"))
	if !ok {
		t.Fatal("leader rejected Propose")
	}
	time.Sleep(2 * electionMax)
	if c, _ := h.nCommitted(idx); c > 0 {
		t.Fatalf("%d committed without a majority", c)
	}

	for _, id := range others[:3] {
		h.connect(id)
	}
	// whoever leads now, the cluster recovers; "lonely" may or may not
	// survive depending on who wins
	h.checkOneLeader()
	h.one("after", 5, true)
}

func TestConcurrentProposals(t *testing.T) {
	h := newHarness(t, 3)

	for attempt := 0; attempt < 5; attempt++ {
		h.checkOneLeader()
		leader := h.checkOneLeader()
		_, term, ok := h.node(leader).Propose([]byte(cmd("warm", attempt)))
		if !ok {
			continue
		}

		const n = 20
		var wg sync.WaitGroup
		idxs := make([]uint64, n)
		var failed atomic.Bool
		for i := 0; i < n; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				idx, t2, ok := h.node(leader).Propose([]byte(cmd("conc", i)))
				if !ok || t2 != term {
					failed.Store(true)
					return
				}
				idxs[i] = idx
			}(i)
		}
		wg.Wait()
		if failed.Load() {
			continue // leadership changed underneath us, try again
		}

		seen := map[uint64]bool{}
		got := map[string]bool{}
		changed := false
		for _, idx := range idxs {
			if seen[idx] {
				t.Fatalf("two proposals got index %d", idx)
			}
			seen[idx] = true
			c, ok := h.wait(idx, 3, term)
			if !ok {
				changed = true
				break
			}
			got[c] = true
		}
		if changed {
			continue
		}
		for i := 0; i < n; i++ {
			if !got[cmd("conc", i)] {
				t.Fatalf("%s missing from the log", cmd("conc", i))
			}
		}
		return
	}
	t.Fatal("term kept changing")
}

func TestRejoinPartitionedLeader(t *testing.T) {
	h := newHarness(t, 3)
	h.one("101", 3, true)

	leader1 := h.checkOneLeader()
	h.disconnect(leader1)

	// the old leader accepts proposals it can never commit
	var stale []uint64
	for _, c := range []string{"102", "103", "104"} {
		if idx, _, ok := h.node(leader1).Propose([]byte(c)); ok {
			stale = append(stale, idx)
		}
	}

	// the others elect a new leader and move on
	h.one("103-new", 2, true)

	leader2 := h.checkOneLeader()
	h.disconnect(leader2)
	h.connect(leader1)

	// leader1 rejoins with a stale log; the third node will not vote for it,
	// so it has to learn the real log from whoever wins
	h.one("104-new", 2, true)

	h.connect(leader2)
	h.one("105", 3, true)

	// none of the stale proposals may have been applied anywhere
	for _, c := range h.committedCommands() {
		if c == "102" || c == "103" || c == "104" {
			t.Fatalf("stale proposal %q from a partitioned leader was committed", c)
		}
	}
}

// TestBackup forces long divergent logs so a new leader has to walk a
// follower back over many conflicting entries.
func TestBackup(t *testing.T) {
	h := newHarness(t, 5)
	h.one(randCmd(), 5, true)

	// leader1 and one follower in a minority partition
	leader1 := h.checkOneLeader()
	o := h.others(leader1)
	h.disconnect(o[1])
	h.disconnect(o[2])
	h.disconnect(o[3])

	for i := 0; i < 50; i++ {
		h.node(leader1).Propose([]byte(randCmd()))
	}
	time.Sleep(electionMin / 2)

	h.disconnect(leader1)
	h.disconnect(o[0])
	h.connect(o[1])
	h.connect(o[2])
	h.connect(o[3])

	// lots of commands that do commit in the majority
	for i := 0; i < 50; i++ {
		h.one(randCmd(), 3, true)
	}

	// now another partitioned leader and one follower
	leader2 := h.checkOneLeader()
	other := o[1]
	if leader2 == other {
		other = o[2]
	}
	h.disconnect(other)

	for i := 0; i < 50; i++ {
		h.node(leader2).Propose([]byte(randCmd()))
	}
	time.Sleep(electionMin / 2)

	// bring back the original leader and its follower, plus other
	for _, id := range h.ids {
		h.disconnect(id)
	}
	h.connect(leader1)
	h.connect(o[0])
	h.connect(other)

	for i := 0; i < 50; i++ {
		h.one(randCmd(), 3, true)
	}

	for _, id := range h.ids {
		h.connect(id)
	}
	h.one(randCmd(), 5, true)
}

// TestFastBackoff builds a follower whose log conflicts with the leader's
// across a hundred entries of one stale term. Decrementing nextIndex one
// at a time would take ~100 rejected AppendEntries; skipping by term
// should take one or two.
func TestFastBackoff(t *testing.T) {
	const n = 100
	mk := func(term uint64, hsTerm uint64) *raft.MemoryStorage {
		s := raft.NewMemoryStorage()
		s.SaveHardState(raft.HardState{Term: hsTerm})
		es := []raft.Entry{{Index: 1, Term: 1, Data: []byte("common")}}
		for i := uint64(2); i <= n+1; i++ {
			es = append(es, raft.Entry{Index: i, Term: term, Data: []byte(fmt.Sprintf("t%d-%d", term, i))})
		}
		s.Append(es)
		return s
	}
	stores := map[string]*raft.MemoryStorage{
		"a": mk(3, 3), // a and b agree, and their log is more up to date
		"b": mk(3, 3),
		"c": mk(2, 3), // c's entries 2..101 all conflict
	}

	net := simnet.New(simnet.Options{Seed: 1})
	var rejects atomic.Int64
	net.OnMessage(func(e simnet.Event) {
		if e.Kind == "AppendEntriesReply" && e.From == "c" && !e.Success && !e.Dropped {
			rejects.Add(1)
		}
	})
	peers := []string{"a", "b", "c"}
	var nodes []*raft.Node
	applied := make(chan raft.ApplyMsg, 1024)
	for _, id := range peers {
		node, err := raft.NewNode(raft.Config{
			ID: id, Peers: peers,
			ElectionTimeoutMin: electionMin, ElectionTimeoutMax: electionMax, HeartbeatInterval: heartbeat,
			Storage: stores[id], Transport: net.Transport(id), ApplyCh: applied,
		})
		if err != nil {
			t.Fatal(err)
		}
		net.Join(id, node)
		nodes = append(nodes, node)
	}
	go func() {
		for range applied {
		}
	}()
	for _, nd := range nodes {
		nd.Start()
	}
	defer func() {
		for _, nd := range nodes {
			nd.Stop()
		}
		close(applied)
	}()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		st := nodes[2].Status()
		if st.LastIndex > n+1 && st.CommitIndex > n+1 {
			// c now has the leader's no-op past the old entries; check one
			// of the overwritten entries came from term 3
			for _, e := range st.RecentEntries {
				if e.Index <= n+1 && e.Term != 3 {
					t.Fatalf("entry %d still from term %d", e.Index, e.Term)
				}
			}
			if r := rejects.Load(); r > 5 {
				t.Fatalf("took %d rejected AppendEntries to repair the log, expected a handful", r)
			} else {
				t.Logf("log repaired after %d rejected AppendEntries", r)
			}
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("follower never caught up: %+v", nodes[2].Status())
}
