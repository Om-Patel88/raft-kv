package raft_test

import (
	"math/rand/v2"
	"sync"
	"testing"
	"time"

	"github.com/Om-Patel88/raft-kv/simnet"
)

func TestPersistRestart(t *testing.T) {
	for _, kind := range storageKinds {
		t.Run(kind, func(t *testing.T) {
			h := newHarness(t, 3, withStorage(kind))
			h.one("11", 3, true)

			// whole cluster
			for _, id := range h.ids {
				h.crash(id)
			}
			for _, id := range h.ids {
				h.start(id)
			}
			h.one("12", 3, true)

			// leader
			leader := h.checkOneLeader()
			h.restart(leader)
			h.one("13", 3, true)

			// leader down while the others carry on, then back
			leader = h.checkOneLeader()
			h.crash(leader)
			h.one("14", 2, true)
			h.start(leader)
			idx := h.one("15", 3, true)
			if c, _ := h.wait(idx, 3, 0); c != "15" {
				t.Fatalf("index %d = %q", idx, c)
			}

			// a follower misses entries while down
			follower := h.others(h.checkOneLeader())[0]
			h.crash(follower)
			h.one("16", 2, true)
			h.start(follower)
			h.one("17", 3, true)
		})
	}
}

// The term and vote must survive a restart, otherwise a node could vote
// twice in one term.
func TestPersistHardState(t *testing.T) {
	for _, kind := range storageKinds {
		t.Run(kind, func(t *testing.T) {
			h := newHarness(t, 3, withStorage(kind))
			h.checkOneLeader()
			before := map[string]uint64{}
			for _, id := range h.ids {
				st := h.status(id)
				before[id] = st.Term
			}
			for _, id := range h.ids {
				h.crash(id)
			}
			// restart them cut off from each other so nothing changes
			for _, id := range h.ids {
				h.disconnect(id)
				h.start(id)
				if st := h.status(id); st.Term < before[id] {
					t.Fatalf("%s came back with term %d, had %d", id, st.Term, before[id])
				}
			}
			for _, id := range h.ids {
				h.connect(id)
			}
			h.one("x", 3, true)
		})
	}
}

func TestPersistMoreFailures(t *testing.T) {
	for _, kind := range storageKinds {
		t.Run(kind, func(t *testing.T) {
			h := newHarness(t, 5, withStorage(kind))
			idx := 1
			for iter := 0; iter < 5; iter++ {
				h.one(cmd("p", idx), 5, true)
				idx++

				leader1 := h.checkOneLeader()
				o := h.others(leader1)
				h.crash(o[0])
				h.crash(o[1])
				h.one(cmd("p", idx), 3, true)
				idx++

				h.crash(leader1)
				h.crash(o[2])
				h.crash(o[3])

				h.start(o[0])
				h.start(o[1])
				time.Sleep(electionMax)
				h.start(o[2])
				h.one(cmd("p", idx), 3, true)
				idx++

				h.start(o[3])
				h.start(leader1)
			}
			h.one("final", 5, true)
		})
	}
}

// TestFigure8 recreates the situation in Figure 8 of the paper over and
// over: leaders crash right after appending, and new leaders with
// different logs take over. An entry from an old term must never be
// committed just by counting replicas.
func TestFigure8(t *testing.T) {
	for _, kind := range storageKinds {
		t.Run(kind, func(t *testing.T) {
			const servers = 5
			h := newHarness(t, servers, withStorage(kind))
			h.one(randCmd(), 1, true)

			up := servers
			for iter := 0; iter < 150; iter++ {
				leader := ""
				for _, id := range h.ids {
					if n := h.node(id); n != nil {
						if _, _, ok := n.Propose([]byte(randCmd())); ok {
							leader = id
						}
					}
				}
				if rand.IntN(1000) < 100 {
					time.Sleep(time.Duration(rand.Int64N(int64(electionMax / 2))))
				} else {
					time.Sleep(time.Duration(rand.IntN(13)) * time.Millisecond)
				}
				if leader != "" {
					h.crash(leader)
					up--
				}
				if up < 3 {
					id := h.ids[rand.IntN(servers)]
					if !h.alive(id) {
						h.start(id)
						up++
					}
				}
				h.checkErrors()
			}
			for _, id := range h.ids {
				if !h.alive(id) {
					h.start(id)
				}
			}
			h.one(randCmd(), servers, true)
		})
	}
}

// TestFigure8Unreliable is the same idea on a lossy, slow network, using
// partitions instead of crashes so messages get badly reordered.
func TestFigure8Unreliable(t *testing.T) {
	const servers = 5
	h := newHarness(t, servers, withNet(simnet.Options{
		Seed: rand.Uint64(), DropRate: 0.1, MinDelay: 0, MaxDelay: 25 * time.Millisecond,
	}))
	h.one(randCmd(), 1, true)

	for iter := 0; iter < 300; iter++ {
		if iter == 100 {
			// long delays from here on to reorder things more
			h.net.SetDelay(0, 60*time.Millisecond)
		}
		leader := ""
		for _, id := range h.ids {
			if !h.isConnected(id) {
				continue
			}
			if _, _, ok := h.node(id).Propose([]byte(randCmd())); ok {
				leader = id
			}
		}
		if rand.IntN(1000) < 100 {
			time.Sleep(time.Duration(rand.Int64N(int64(electionMax / 2))))
		} else {
			time.Sleep(time.Duration(rand.IntN(13)) * time.Millisecond)
		}
		if leader != "" && rand.IntN(1000) < 500 {
			h.disconnect(leader)
		}
		connected := 0
		for _, id := range h.ids {
			if h.isConnected(id) {
				connected++
			}
		}
		if connected < 3 {
			h.connect(h.ids[rand.IntN(servers)])
		}
		h.checkErrors()
	}

	h.net.SetDropRate(0)
	h.net.SetDelay(0, 0)
	for _, id := range h.ids {
		h.connect(id)
	}
	h.one(randCmd(), servers, true)
}

// TestUnreliableChurn has clients proposing continuously while nodes crash,
// restart, and get disconnected on a lossy network. At the end every
// command a client saw committed must still be in the log.
func TestUnreliableChurn(t *testing.T) {
	cases := []struct {
		name string
		kind string
		snap uint64
	}{
		{"mem", "mem", 0},
		{"wal", "wal", 0},
		{"mem-snapshots", "mem", 10},
		{"wal-snapshots", "wal", 10},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			const servers = 5
			h := newHarness(t, servers, withStorage(c.kind), withSnapshots(c.snap), withNet(simnet.Options{
				Seed: rand.Uint64(), DropRate: 0.05, MaxDelay: 10 * time.Millisecond,
			}))

			stop := make(chan struct{})
			var wg sync.WaitGroup
			var mu sync.Mutex
			acked := map[uint64]string{}
			for client := 0; client < 3; client++ {
				wg.Add(1)
				go func(client int) {
					defer wg.Done()
					for i := 0; ; i++ {
						select {
						case <-stop:
							return
						default:
						}
						c := cmd(string(rune('A'+client)), i)
						if idx, ok := h.tryOne(c, 1, false, time.Second); ok {
							mu.Lock()
							acked[idx] = c
							mu.Unlock()
						}
					}
				}(client)
			}

			for iter := 0; iter < 20; iter++ {
				id := h.ids[rand.IntN(servers)]
				switch r := rand.IntN(100); {
				case r < 30:
					h.disconnect(id)
				case r < 60:
					h.connect(id)
				case r < 80:
					if h.alive(id) {
						up := 0
						for _, x := range h.ids {
							if h.alive(x) {
								up++
							}
						}
						if up > 3 {
							h.crash(id)
						}
					}
				default:
					if !h.alive(id) {
						h.start(id)
					}
				}
				time.Sleep(time.Duration(rand.IntN(int(electionMax/time.Millisecond))) * time.Millisecond)
				h.checkErrors()
			}

			time.Sleep(electionMax)
			h.net.SetDropRate(0)
			h.net.SetDelay(0, 0)
			for _, id := range h.ids {
				if !h.alive(id) {
					h.start(id)
				}
				h.connect(id)
			}
			close(stop)
			wg.Wait()

			last := h.one("end", servers, true)
			for idx := range acked {
				if idx > last {
					t.Fatalf("acked index %d beyond final index %d", idx, last)
				}
			}
			h.mu.Lock()
			defer h.mu.Unlock()
			for idx, c := range acked {
				if got := h.committed[idx]; got != c {
					t.Fatalf("client saw %q committed at %d, log has %q", c, idx, got)
				}
			}
			if len(acked) == 0 {
				t.Fatal("no client command ever committed")
			}
		})
	}
}
