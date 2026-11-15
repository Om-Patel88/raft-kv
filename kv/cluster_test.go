package kv

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand/v2"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Om-Patel88/raft-kv/raft"
	"github.com/Om-Patel88/raft-kv/raft/wal"
	"github.com/Om-Patel88/raft-kv/simnet"
)

const (
	electionMin = 150 * time.Millisecond
	electionMax = 300 * time.Millisecond
	heartbeat   = 50 * time.Millisecond
)

// cluster runs n Stores on a simnet. Crashing a node closes its Store and
// keeps its storage; starting it again builds a fresh Store on top.
type cluster struct {
	t         *testing.T
	net       *simnet.Network
	ids       []string
	kind      string // "mem" or "wal"
	dir       string
	threshold int

	mu       sync.Mutex
	stores   map[string]*Store // live only
	storages map[string]raft.Storage
}

type clusterOpt func(*cluster)

func withWAL() clusterOpt                        { return func(c *cluster) { c.kind = "wal" } }
func withThreshold(n int) clusterOpt             { return func(c *cluster) { c.threshold = n } }
func withNetOptions(o simnet.Options) clusterOpt { return func(c *cluster) { c.net = simnet.New(o) } }

func newCluster(t *testing.T, n int, opts ...clusterOpt) *cluster {
	t.Helper()
	c := &cluster{
		t:        t,
		kind:     "mem",
		stores:   map[string]*Store{},
		storages: map[string]raft.Storage{},
	}
	for _, o := range opts {
		o(c)
	}
	if c.net == nil {
		c.net = simnet.New(simnet.Options{})
	}
	if c.kind == "wal" {
		c.dir = t.TempDir()
	}
	for i := 1; i <= n; i++ {
		c.ids = append(c.ids, fmt.Sprintf("n%d", i))
	}
	for _, id := range c.ids {
		c.start(id)
	}
	t.Cleanup(func() {
		for _, id := range c.ids {
			c.crash(id)
		}
	})
	return c
}

func (c *cluster) start(id string) {
	c.t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.stores[id] != nil {
		return
	}
	st := c.storages[id]
	if st == nil || c.kind == "wal" {
		if c.kind == "wal" {
			w, err := wal.Open(filepath.Join(c.dir, id), wal.Options{NoSync: true})
			if err != nil {
				c.t.Fatalf("open wal %s: %v", id, err)
			}
			st = w
		} else {
			st = raft.NewMemoryStorage()
		}
		c.storages[id] = st
	}
	cfg := Config{
		ID:                 id,
		Peers:              c.ids,
		Storage:            st,
		Transport:          c.net.Transport(id),
		ElectionTimeoutMin: electionMin,
		ElectionTimeoutMax: electionMax,
		HeartbeatInterval:  heartbeat,
		SnapshotThreshold:  c.threshold,
	}
	if os.Getenv("RAFT_DEBUG") != "" {
		cfg.Logger = log.New(os.Stderr, "", log.Lmicroseconds)
	}
	s, err := NewStore(cfg)
	if err != nil {
		c.t.Fatalf("new store %s: %v", id, err)
	}
	c.stores[id] = s
	c.net.Join(id, s.Raft())
	s.Start()
}

func (c *cluster) crash(id string) {
	c.mu.Lock()
	s := c.stores[id]
	delete(c.stores, id)
	st := c.storages[id]
	c.mu.Unlock()
	if s == nil {
		return
	}
	c.net.Remove(id)
	s.Close()
	if w, ok := st.(*wal.WAL); ok {
		w.Close()
	}
}

func (c *cluster) store(id string) *Store {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.stores[id]
}

// leader waits until exactly one live node among ids (all nodes if none
// given) believes it leads the newest term, and returns it.
func (c *cluster) leader(among ...string) string {
	c.t.Helper()
	if len(among) == 0 {
		among = c.ids
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		var best uint64
		lead := ""
		for _, id := range among {
			s := c.store(id)
			if s == nil {
				continue
			}
			if st := s.Status(); st.Role == raft.Leader && st.Term >= best {
				best, lead = st.Term, id
			}
		}
		if lead != "" {
			return lead
		}
		time.Sleep(20 * time.Millisecond)
	}
	c.t.Fatalf("no leader among %v", among)
	return ""
}

func (c *cluster) others(except string) []string {
	var out []string
	for _, id := range c.ids {
		if id != except {
			out = append(out, id)
		}
	}
	return out
}

type nodeState struct {
	applied  uint64
	data     map[string]string
	sessions map[string]session
}

func (c *cluster) state(id string) nodeState {
	s := c.store(id)
	if s == nil {
		c.t.Fatalf("%s is not running", id)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	st := nodeState{applied: s.applied, data: map[string]string{}, sessions: map[string]session{}}
	for k, v := range s.sm.data {
		st.data[k] = v
	}
	for k, v := range s.sm.sessions {
		st.sessions[k] = v
	}
	return st
}

// waitConverged waits for every live node to have applied the same index
// and hold identical data and session tables.
func (c *cluster) waitConverged(timeout time.Duration) nodeState {
	c.t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		var first nodeState
		same := true
		live := 0
		for _, id := range c.ids {
			if c.store(id) == nil {
				continue
			}
			st := c.state(id)
			if live == 0 {
				first = st
			} else if st.applied != first.applied || !reflect.DeepEqual(st.data, first.data) ||
				!reflect.DeepEqual(st.sessions, first.sessions) {
				same = false
			}
			live++
		}
		if same {
			return first
		}
		if time.Now().After(deadline) {
			for _, id := range c.ids {
				if c.store(id) != nil {
					st := c.state(id)
					c.t.Logf("%s: applied=%d keys=%d sessions=%d", id, st.applied, len(st.data), len(st.sessions))
				}
			}
			c.t.Fatal("nodes did not converge")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// tclient is a minimal retrying client, the same idea as package client
// but talking to Stores directly. It is not safe for concurrent use.
type tclient struct {
	c      *cluster
	id     string
	seq    uint64
	leader string
	// wander makes every request start at a random node instead of the
	// last known leader, so a stale leader gets asked too
	wander bool
}

func (c *cluster) client(id string) *tclient { return &tclient{c: c, id: id} }

// do retries req (with the same seq) against any live node until it
// succeeds or timeout passes.
func (tc *tclient) do(req Request, timeout time.Duration) (Response, error) {
	tc.seq++
	req.ClientID, req.Seq = tc.id, tc.seq
	deadline := time.Now().Add(timeout)
	try := tc.leader
	if tc.wander {
		try = ""
	}
	var lastErr error
	for time.Now().Before(deadline) {
		if try == "" {
			try = tc.c.ids[rand.IntN(len(tc.c.ids))]
		}
		s := tc.c.store(try)
		if s == nil {
			try = ""
			time.Sleep(5 * time.Millisecond)
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), min(500*time.Millisecond, time.Until(deadline)))
		resp, err := s.Do(ctx, req)
		cancel()
		if err == nil {
			tc.leader = try
			return resp, nil
		}
		lastErr = err
		var nl *NotLeaderError
		if errors.As(err, &nl) && nl.Leader != "" && nl.Leader != try {
			try = nl.Leader
			continue
		}
		try = ""
		time.Sleep(10 * time.Millisecond)
	}
	return Response{}, fmt.Errorf("%s seq %d: gave up: %w", tc.id, req.Seq, lastErr)
}

func (tc *tclient) mustDo(t *testing.T, req Request) Response {
	t.Helper()
	resp, err := tc.do(req, 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

func doOn(s *Store, req Request, timeout time.Duration) (Response, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return s.Do(ctx, req)
}

func TestClusterBasicOps(t *testing.T) {
	c := newCluster(t, 3)
	s := c.store(c.leader())
	do := func(r Request) Response {
		t.Helper()
		resp, err := doOn(s, r, 2*time.Second)
		if err != nil {
			t.Fatalf("%+v: %v", r, err)
		}
		return resp
	}

	do(Request{Op: OpPut, Key: "a", Value: "1"})
	if r := do(Request{Op: OpGet, Key: "a"}); !r.Found || r.Value != "1" {
		t.Fatalf("get a: %+v", r)
	}
	do(Request{Op: OpAppend, Key: "a", Value: "2"})
	if r := do(Request{Op: OpCAS, Key: "a", Expected: "1", Value: "x"}); r.Swapped {
		t.Fatal("cas against stale value swapped")
	}
	if r := do(Request{Op: OpCAS, Key: "a", Expected: "12", Value: "x"}); !r.Swapped {
		t.Fatal("cas against current value did not swap")
	}
	if r := do(Request{Op: OpGet, Key: "a"}); r.Value != "x" {
		t.Fatalf("after cas: %+v", r)
	}
	if r := do(Request{Op: OpDelete, Key: "a"}); !r.Found {
		t.Fatal("delete said not found")
	}
	if r := do(Request{Op: OpGet, Key: "a"}); r.Found {
		t.Fatalf("still there after delete: %+v", r)
	}
	if _, err := doOn(s, Request{Op: OpPut}, time.Second); !errors.Is(err, ErrBadRequest) {
		t.Fatalf("empty key: %v", err)
	}

	do(Request{Op: OpPut, Key: "b", Value: "everywhere"})
	st := c.waitConverged(2 * time.Second)
	if st.data["b"] != "everywhere" {
		t.Fatalf("followers have %v", st.data)
	}
	if k := s.Status().Keys; k != 1 {
		t.Fatalf("status keys = %d", k)
	}
}

func TestClusterNotLeader(t *testing.T) {
	c := newCluster(t, 3)
	lead := c.leader()
	if _, err := doOn(c.store(lead), Request{Op: OpPut, Key: "k", Value: "v"}, 2*time.Second); err != nil {
		t.Fatal(err)
	}
	for _, id := range c.others(lead) {
		_, err := doOn(c.store(id), Request{Op: OpGet, Key: "k"}, time.Second)
		var nl *NotLeaderError
		if !errors.As(err, &nl) || !errors.Is(err, ErrNotLeader) {
			t.Fatalf("%s: got %v, want NotLeaderError", id, err)
		}
		if nl.Leader != lead {
			t.Fatalf("%s: leader hint %q, want %q", id, nl.Leader, lead)
		}
	}
}

// Clients keep appending while the leader crashes and later comes back.
// Every acknowledged append must be in the final value exactly once.
func TestClusterFailoverUnderLoad(t *testing.T) {
	c := newCluster(t, 3)
	c.leader()

	const nclients = 3
	stop := make(chan struct{})
	var wg sync.WaitGroup
	acked := make([][]string, nclients)
	for i := 0; i < nclients; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			tc := c.client(fmt.Sprintf("c%d", i))
			for n := 0; ; n++ {
				select {
				case <-stop:
					return
				default:
				}
				tok := fmt.Sprintf("%d;", n)
				if _, err := tc.do(Request{Op: OpAppend, Key: tc.id, Value: tok}, 10*time.Second); err != nil {
					t.Error(err)
					return
				}
				acked[i] = append(acked[i], tok)
			}
		}(i)
	}

	time.Sleep(300 * time.Millisecond)
	old := c.leader()
	c.crash(old)
	newLead := c.leader(c.others(old)...)
	if newLead == old {
		t.Fatal("crashed node still leader")
	}
	time.Sleep(500 * time.Millisecond)
	c.start(old)
	time.Sleep(300 * time.Millisecond)
	close(stop)
	wg.Wait()

	st := c.waitConverged(5 * time.Second)
	for i := 0; i < nclients; i++ {
		want := ""
		for _, tok := range acked[i] {
			want += tok
		}
		if len(acked[i]) < 5 {
			t.Errorf("c%d only got %d appends through", i, len(acked[i]))
		}
		if got := st.data[fmt.Sprintf("c%d", i)]; got != want {
			t.Errorf("c%d: value %q, want %q", i, got, want)
		}
	}
}

// A leader cut off with a minority must not complete writes or reads,
// even though it still thinks it is leader.
func TestClusterMinorityLeader(t *testing.T) {
	c := newCluster(t, 5)
	old := c.leader()
	if _, err := doOn(c.store(old), Request{Op: OpPut, Key: "k", Value: "before"}, 2*time.Second); err != nil {
		t.Fatal(err)
	}
	rest := c.others(old)
	c.net.Partition([]string{old, rest[0]}, rest[1:])

	if _, err := doOn(c.store(old), Request{Op: OpPut, Key: "k", Value: "lost"}, 500*time.Millisecond); err == nil {
		t.Fatal("minority leader acknowledged a write")
	}

	newLead := c.leader(rest[1:]...)
	if _, err := doOn(c.store(newLead), Request{Op: OpPut, Key: "k", Value: "after"}, 2*time.Second); err != nil {
		t.Fatal(err)
	}

	// The old leader may still believe it leads; its read has to fail
	// rather than return "before".
	if r, err := doOn(c.store(old), Request{Op: OpGet, Key: "k"}, 500*time.Millisecond); err == nil {
		t.Fatalf("minority leader served a read: %+v", r)
	}

	c.net.Heal()
	tc := c.client("checker")
	if r := tc.mustDo(t, Request{Op: OpGet, Key: "k"}); r.Value != "after" {
		t.Fatalf("after heal k = %q, want after", r.Value)
	}
	st := c.waitConverged(3 * time.Second)
	if st.data["k"] != "after" {
		t.Fatalf("converged on %q", st.data["k"])
	}
}

func TestClusterRestartRecovers(t *testing.T) {
	for _, tc := range []struct {
		name string
		opts []clusterOpt
	}{
		{"mem", nil},
		{"wal", []clusterOpt{withWAL()}},
		{"wal-snapshots", []clusterOpt{withWAL(), withThreshold(10)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := newCluster(t, 3, tc.opts...)
			cl := c.client("w")
			want := map[string]string{}
			for i := 0; i < 40; i++ {
				k := fmt.Sprintf("k%d", i%7)
				cl.mustDo(t, Request{Op: OpAppend, Key: k, Value: fmt.Sprintf("%d,", i)})
				want[k] += fmt.Sprintf("%d,", i)
			}

			// one node misses some writes while down
			lagger := c.others(c.leader())[0]
			c.crash(lagger)
			for i := 40; i < 60; i++ {
				k := fmt.Sprintf("k%d", i%7)
				cl.mustDo(t, Request{Op: OpAppend, Key: k, Value: fmt.Sprintf("%d,", i)})
				want[k] += fmt.Sprintf("%d,", i)
			}
			c.start(lagger)
			st := c.waitConverged(5 * time.Second)
			if !reflect.DeepEqual(st.data, want) {
				t.Fatalf("after one restart: %v, want %v", st.data, want)
			}

			// then everyone goes down at once
			for _, id := range c.ids {
				c.crash(id)
			}
			for _, id := range c.ids {
				c.start(id)
			}
			// replaying the session table: a retry of the last write
			// must still be recognised after the restart
			cl.seq--
			cl.mustDo(t, Request{Op: OpAppend, Key: "k3", Value: "59,"})
			for k, v := range want {
				r := cl.mustDo(t, Request{Op: OpGet, Key: k})
				if r.Value != v {
					t.Fatalf("after full restart %s = %q, want %q", k, r.Value, v)
				}
			}
			if st := c.waitConverged(5 * time.Second); !reflect.DeepEqual(st.data, want) {
				t.Fatalf("converged to %v", st.data)
			}
			if tc.name == "wal-snapshots" {
				for _, id := range c.ids {
					if c.store(id).Status().SnapshotIndex == 0 {
						t.Errorf("%s never took a snapshot", id)
					}
				}
			}
		})
	}
}

func TestClusterSnapshotBoundsLog(t *testing.T) {
	const threshold = 20
	c := newCluster(t, 3, withThreshold(threshold))
	cl := c.client("w")
	cl.mustDo(t, Request{Op: OpPut, Key: "x", Value: ""})

	lagger := c.others(c.leader())[0]
	c.net.Disconnect(lagger)
	for i := 0; i < 200; i++ {
		cl.mustDo(t, Request{Op: OpAppend, Key: "x", Value: "."})
		if i%10 == 0 {
			cl.mustDo(t, Request{Op: OpGet, Key: "x"})
		}
	}
	for _, id := range c.others(lagger) {
		st := c.store(id).Status()
		if st.SnapshotIndex == 0 {
			t.Fatalf("%s never snapshotted", id)
		}
		if n := st.LastIndex - st.SnapshotIndex; n > 2*threshold {
			t.Fatalf("%s holds %d entries, threshold %d", id, n, threshold)
		}
	}

	// the lagger is now behind the leader's snapshot and can only catch
	// up by installing it, dedup table and all
	c.net.Connect(lagger)
	cl.mustDo(t, Request{Op: OpAppend, Key: "x", Value: "!"})
	st := c.waitConverged(5 * time.Second)
	if want := strings.Repeat(".", 200) + "!"; st.data["x"] != want {
		t.Fatalf("x = %q, want %q", st.data["x"], want)
	}
	if s := st.sessions["w"]; s.Seq != cl.seq {
		t.Fatalf("session seq %d, want %d", s.Seq, cl.seq)
	}
	if c.store(lagger).Status().SnapshotIndex == 0 {
		t.Fatal("lagger caught up without a snapshot")
	}
}
