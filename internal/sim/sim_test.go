package sim

import (
	"maps"
	"testing"
	"time"
)

func newTestSim(t *testing.T) *Sim {
	s := New(Options{Speed: 8, SnapEvery: 5})
	t.Cleanup(func() { s.cluster().shutdown() })
	return s
}

func waitFor(t *testing.T, s *Sim, what string, cond func(State) bool) State {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		st := s.State()
		if cond(st) {
			return st
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
	return State{}
}

// leaderOf returns the leader that every live node in ids agrees on.
func leaderOf(st State, ids ...string) string {
	leader := ""
	for _, n := range st.Nodes {
		if !n.Alive || (len(ids) > 0 && !contains(ids, n.ID)) {
			continue
		}
		if n.Raft.Leader == "" || (leader != "" && n.Raft.Leader != leader) {
			return ""
		}
		leader = n.Raft.Leader
	}
	return leader
}

func contains(ids []string, id string) bool {
	for _, x := range ids {
		if x == id {
			return true
		}
	}
	return false
}

func node(st State, id string) NodeState {
	for _, n := range st.Nodes {
		if n.ID == id {
			return n
		}
	}
	return NodeState{}
}

// converged reports whether every live node holds want.
func converged(want map[string]string) func(State) bool {
	return func(st State) bool {
		for _, n := range st.Nodes {
			if n.Alive && !maps.Equal(n.Data, want) {
				return false
			}
		}
		return true
	}
}

func findProposal(st State, seq int) ProposalState {
	for _, p := range st.Proposals {
		if p.Seq == seq {
			return p
		}
	}
	return ProposalState{}
}

func mustPropose(t *testing.T, s *Sim, text, target string) ProposeResult {
	t.Helper()
	r := s.Propose(text, target)
	if !r.OK {
		t.Fatalf("propose %q to %q: %s", text, target, r.Error)
	}
	return r
}

func TestFailoverAndPartition(t *testing.T) {
	s := newTestSim(t)
	want := map[string]string{}

	st := waitFor(t, s, "first leader", func(st State) bool { return leaderOf(st) != "" })
	first := leaderOf(st)

	r := mustPropose(t, s, "set a 1", "")
	want["a"] = "1"
	waitFor(t, s, "a=1 everywhere", converged(want))
	if p := findProposal(s.State(), r.Seq); p.Status != "committed" {
		t.Fatalf("proposal status %q, want committed", p.Status)
	}

	if err := s.Kill(first); err != nil {
		t.Fatal(err)
	}
	st = waitFor(t, s, "new leader", func(st State) bool {
		l := leaderOf(st)
		return l != "" && l != first && !node(st, first).Alive
	})
	mustPropose(t, s, "set b 2", "")
	want["b"] = "2"
	waitFor(t, s, "b=2 on survivors", converged(want))

	// the killed node rebuilds its data from the log on restart
	if err := s.Restart(first); err != nil {
		t.Fatal(err)
	}
	waitFor(t, s, "restarted node catches up", func(st State) bool {
		return node(st, first).Alive && converged(want)(st)
	})

	// Cut the leader off with one follower. It still accepts writes, but
	// they can't commit, and the majority side moves on without it.
	st = waitFor(t, s, "stable leader", func(st State) bool { return leaderOf(st) != "" })
	old := leaderOf(st)
	var minority, majority []string
	minority = append(minority, old)
	for _, n := range st.Nodes {
		if n.ID == old {
			continue
		}
		if len(minority) < 2 {
			minority = append(minority, n.ID)
		} else {
			majority = append(majority, n.ID)
		}
	}
	if err := s.Partition([][]string{minority, majority}); err != nil {
		t.Fatal(err)
	}
	stale := mustPropose(t, s, "set c stale", old)
	waitFor(t, s, "majority elects its own leader", func(st State) bool {
		l := leaderOf(st, majority...)
		return l != "" && contains(majority, l)
	})
	mustPropose(t, s, "set d 4", leaderOf(s.State(), majority...))
	want["d"] = "4"
	waitFor(t, s, "d=4 on the majority", func(st State) bool {
		for _, id := range majority {
			if !maps.Equal(node(st, id).Data, want) {
				return false
			}
		}
		return true
	})
	if p := findProposal(s.State(), stale.Seq); p.Status != "pending" && p.Status != "lost" {
		t.Fatalf("minority write is %q, it must not commit", p.Status)
	}

	s.Heal()
	st = waitFor(t, s, "everyone converges after heal", converged(want))
	if _, ok := node(st, old).Data["c"]; ok {
		t.Fatal("the stale leader's write survived")
	}
	waitFor(t, s, "stale write reported lost", func(st State) bool {
		return findProposal(st, stale.Seq).Status == "lost"
	})
}

func TestKillRestartRace(t *testing.T) {
	s := newTestSim(t)
	waitFor(t, s, "leader", func(st State) bool { return leaderOf(st) != "" })
	for i := 0; i < 20; i++ {
		s.Kill("n1")
		s.Restart("n1")
	}
	s.Kill("n2")
	waitFor(t, s, "n1 up, n2 down", func(st State) bool {
		return node(st, "n1").Alive && !node(st, "n2").Alive
	})
	s.SetSpeed(6)
	// the speed change restarts every node, so retry until a leader takes it
	waitFor(t, s, "write accepted after speed change", func(State) bool { return s.Propose("set k v", "").OK })
	waitFor(t, s, "write lands", converged(map[string]string{"k": "v"}))
}
