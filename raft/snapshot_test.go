package raft_test

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/Om-Patel88/raft-kv/simnet"
)

const snapEvery = 10

// The log should stay roughly snapEvery entries long however many
// commands go through.
func TestSnapshotLogBounded(t *testing.T) {
	h := newHarness(t, 3, withSnapshots(snapEvery))
	var last uint64
	for i := 0; i < 150; i++ {
		last = h.one(cmd("bounded", i), 3, true)
	}
	time.Sleep(2 * heartbeat)
	for _, id := range h.ids {
		st := h.status(id)
		if st.SnapshotIndex == 0 {
			t.Fatalf("%s never snapshotted", id)
		}
		if n := st.LastIndex - st.SnapshotIndex; n > 2*snapEvery+5 {
			t.Fatalf("%s holds %d log entries after compaction (first %d, last %d)", id, n, st.FirstIndex, st.LastIndex)
		}
		if st.FirstIndex != st.SnapshotIndex+1 || st.LastIndex < last {
			t.Fatalf("%s bad bounds: %+v", id, st)
		}
	}
}

func countInstalls(h *harness, to string) *atomic.Int64 {
	var n atomic.Int64
	h.net.OnMessage(func(e simnet.Event) {
		if e.Kind == "InstallSnapshot" && e.To == to && !e.Dropped {
			n.Add(1)
		}
	})
	return &n
}

// A follower that was cut off while the leader compacted its log can only
// catch up through InstallSnapshot.
func TestSnapshotInstallDisconnected(t *testing.T) {
	h := newHarness(t, 3, withSnapshots(snapEvery))
	h.one("first", 3, true)

	leader := h.checkOneLeader()
	lagger := h.others(leader)[0]
	installs := countInstalls(h, lagger)
	h.disconnect(lagger)
	for i := 0; i < 5*snapEvery; i++ {
		h.one(cmd("while-away", i), 2, true)
	}

	h.connect(lagger)
	idx := h.one("back", 3, true)
	h.wait(idx, 3, 0)
	if installs.Load() == 0 {
		t.Fatal("lagging follower caught up without InstallSnapshot")
	}
	if st := h.status(lagger); st.SnapshotIndex == 0 {
		t.Fatalf("lagger has no snapshot: %+v", st)
	}
}

func TestSnapshotInstallCrashed(t *testing.T) {
	for _, kind := range storageKinds {
		t.Run(kind, func(t *testing.T) {
			h := newHarness(t, 3, withStorage(kind), withSnapshots(snapEvery))
			h.one("first", 3, true)

			leader := h.checkOneLeader()
			lagger := h.others(leader)[0]
			installs := countInstalls(h, lagger)
			h.crash(lagger)
			for i := 0; i < 5*snapEvery; i++ {
				h.one(cmd("while-down", i), 2, true)
			}
			h.start(lagger)
			h.one("back", 3, true)
			if installs.Load() == 0 {
				t.Fatal("restarted follower caught up without InstallSnapshot")
			}
		})
	}
}

// Every node restarts from its own snapshot; the snapshot has to reach the
// service before any later entries do (the harness checks the ordering).
func TestSnapshotRestartAll(t *testing.T) {
	for _, kind := range storageKinds {
		t.Run(kind, func(t *testing.T) {
			h := newHarness(t, 3, withStorage(kind), withSnapshots(snapEvery))
			for i := 0; i < 3*snapEvery+3; i++ {
				h.one(cmd("pre", i), 3, true)
			}
			snaps := map[string]uint64{}
			for _, id := range h.ids {
				snaps[id] = h.status(id).SnapshotIndex
				h.crash(id)
			}
			for _, id := range h.ids {
				h.start(id)
				st := h.status(id)
				if st.SnapshotIndex < snaps[id] || st.SnapshotIndex == 0 {
					t.Fatalf("%s restarted with snapshot %d, had %d", id, st.SnapshotIndex, snaps[id])
				}
			}
			idx := h.one("post", 3, true)

			// each restarted service rebuilt its full history from the
			// snapshot plus the log
			for _, id := range h.ids {
				h.mu.Lock()
				n := len(h.sms[id].applied)
				h.mu.Unlock()
				if n < 3*snapEvery+3 {
					t.Fatalf("%s only knows %d commands after restart (index %d)", id, n, idx)
				}
			}
		})
	}
}

// Snapshots on a lossy network with followers repeatedly falling behind.
func TestSnapshotUnreliable(t *testing.T) {
	h := newHarness(t, 5, withSnapshots(snapEvery), withNet(simnet.Options{
		Seed: 42, DropRate: 0.1, MaxDelay: 15 * time.Millisecond,
	}))
	for iter := 0; iter < 6; iter++ {
		leader := h.checkOneLeader()
		victim := h.others(leader)[iter%4]
		if iter%2 == 0 {
			h.disconnect(victim)
		} else {
			h.crash(victim)
		}
		for i := 0; i < snapEvery+3; i++ {
			h.one(cmd("u", iter*100+i), 3, true)
		}
		if iter%2 == 0 {
			h.connect(victim)
		} else {
			h.start(victim)
		}
	}
	h.net.SetDropRate(0)
	h.one("done", 5, true)
}
