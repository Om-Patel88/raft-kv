package raft_test

import (
	"math/rand/v2"
	"testing"
	"time"

	"github.com/Om-Patel88/raft-kv/raft"
)

func TestInitialElection(t *testing.T) {
	h := newHarness(t, 3)
	h.checkOneLeader()

	// give followers a moment to hear from the leader
	time.Sleep(100 * time.Millisecond)
	term1 := h.checkTerms()
	if term1 < 1 {
		t.Fatalf("term is %d, expected at least 1", term1)
	}

	// with no failures nothing should change
	time.Sleep(2 * electionMax)
	if term2 := h.checkTerms(); term2 != term1 {
		t.Logf("warning: term changed from %d to %d with no failures", term1, term2)
	}
	h.checkOneLeader()
}

func TestReElection(t *testing.T) {
	h := newHarness(t, 3)
	leader1 := h.checkOneLeader()

	// leader cut off: the other two elect a new one
	h.disconnect(leader1)
	leader2 := h.checkOneLeader()
	if leader2 == leader1 {
		t.Fatal("disconnected leader is still the only leader")
	}

	// old leader rejoins; it must step down and not disturb the new leader
	h.connect(leader1)
	leader3 := h.checkOneLeader()
	time.Sleep(2 * electionMax)
	if st := h.status(leader1); st.Role == raft.Leader && leader3 != leader1 {
		t.Fatalf("old leader %s still thinks it leads term %d", leader1, st.Term)
	}
	h.one("after rejoin", 3, true)
}

func TestNoQuorumNoLeader(t *testing.T) {
	h := newHarness(t, 5)
	leader := h.checkOneLeader()

	// cut off the leader and two followers, leaving two nodes that cannot
	// form a majority
	cut := []string{leader}
	for _, id := range h.others(leader) {
		if len(cut) == 3 {
			break
		}
		cut = append(cut, id)
	}
	for _, id := range cut {
		h.disconnect(id)
	}
	time.Sleep(4 * electionMax)
	h.checkNoLeader()

	// one comes back: three nodes is a majority of five
	h.connect(cut[1])
	h.checkOneLeader()

	// and the rest
	h.connect(cut[0])
	h.connect(cut[2])
	h.checkOneLeader()
	h.one("quorum back", 5, true)
}

func TestManyElections(t *testing.T) {
	h := newHarness(t, 7)
	h.checkOneLeader()

	// knock out three random nodes at a time: the other four are always a
	// majority, so there must always be a leader
	for iter := 0; iter < 8; iter++ {
		perm := rand.Perm(len(h.ids))
		down := []string{h.ids[perm[0]], h.ids[perm[1]], h.ids[perm[2]]}
		for _, id := range down {
			h.disconnect(id)
		}
		h.checkOneLeader()
		for _, id := range down {
			h.connect(id)
		}
	}
	h.checkOneLeader()
}

func TestSingleNode(t *testing.T) {
	h := newHarness(t, 1)
	if l := h.checkOneLeader(); l != "n1" {
		t.Fatalf("leader = %s", l)
	}
	for i := 0; i < 5; i++ {
		h.one(cmd("solo", i), 1, false)
	}
	st := h.status("n1")
	if st.CommitIndex != st.LastIndex {
		t.Fatalf("commit %d, last %d", st.CommitIndex, st.LastIndex)
	}
	if len(st.Peers) != 0 {
		t.Fatalf("single node should have no peer progress, got %v", st.Peers)
	}
}
