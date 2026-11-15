package kv

import (
	"fmt"
	"math/rand/v2"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Om-Patel88/raft-kv/simnet"
)

// TestChaos runs concurrent clients appending unique tokens to a shared
// key and to a key of their own, while nodes get partitioned, cut off,
// crashed and restarted on a lossy network. Afterwards every acknowledged
// append has to be there exactly once and in each client's order, and
// every node has to end up with the same state.
func TestChaos(t *testing.T) {
	seed := rand.Uint64()
	t.Logf("seed %d", seed)
	rng := rand.New(rand.NewPCG(seed, 1))
	c := newCluster(t, 5, withWAL(), withThreshold(30), withNetOptions(simnet.Options{
		Seed:     seed,
		MaxDelay: 5 * time.Millisecond,
		DropRate: 0.05,
	}))
	c.leader()

	const (
		nclients = 5
		duration = 5 * time.Second
	)
	type outcome struct {
		acked   []int // counters whose append returned success
		unknown int   // counter of an append that gave up, -1 if none
	}
	results := make([]outcome, nclients)
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < nclients; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			tc := c.client(fmt.Sprintf("c%d", i))
			results[i].unknown = -1
			for n := 0; ; n++ {
				select {
				case <-stop:
					return
				default:
				}
				// shared and own key alternate, so a token lands in one
				// of them depending on n's parity
				key := "shared"
				if n%2 == 1 {
					key = tc.id
				}
				tok := fmt.Sprintf("%s:%d;", tc.id, n)
				if _, err := tc.do(Request{Op: OpAppend, Key: key, Value: tok}, 20*time.Second); err != nil {
					t.Errorf("client %s stuck: %v", tc.id, err)
					results[i].unknown = n
					return
				}
				results[i].acked = append(results[i].acked, n)
			}
		}(i)
	}

	// the nemesis
	down := map[string]bool{}
	end := time.Now().Add(duration)
	actions := 0
	for time.Now().Before(end) {
		time.Sleep(time.Duration(100+rng.IntN(250)) * time.Millisecond)
		actions++
		id := c.ids[rng.IntN(len(c.ids))]
		switch rng.IntN(6) {
		case 0:
			perm := rng.Perm(len(c.ids))
			cut := 1 + rng.IntN(len(c.ids)-1)
			var a, b []string
			for j, p := range perm {
				if j < cut {
					a = append(a, c.ids[p])
				} else {
					b = append(b, c.ids[p])
				}
			}
			c.net.Partition(a, b)
		case 1:
			c.net.Disconnect(id)
		case 2:
			if len(down) < 2 && !down[id] {
				c.crash(id)
				down[id] = true
			}
		case 3:
			for d := range down {
				c.start(d)
				delete(down, d)
				break
			}
		case 4:
			c.net.Heal()
		case 5:
			// crash and restart straight away
			if !down[id] {
				c.crash(id)
				c.start(id)
			}
		}
	}

	c.net.Heal()
	c.net.SetDropRate(0)
	for d := range down {
		c.start(d)
	}
	close(stop)
	wg.Wait()
	t.Logf("%d nemesis actions", actions)

	checker := c.client("checker")
	values := map[string]string{}
	for _, k := range []string{"shared", "c0", "c1", "c2", "c3", "c4"} {
		values[k] = checker.mustDo(t, Request{Op: OpGet, Key: k}).Value
	}
	st := c.waitConverged(10 * time.Second)
	for k, v := range values {
		if st.data[k] != v {
			t.Errorf("%s: read %q but nodes hold %q", k, v, st.data[k])
		}
	}

	// count where every token ended up
	seen := map[string][]int{} // client -> counters, in log order
	where := map[string]string{}
	for k, v := range values {
		for _, tok := range strings.Split(strings.TrimSuffix(v, ";"), ";") {
			if tok == "" {
				continue
			}
			if prev, dup := where[tok]; dup {
				t.Errorf("token %s appears twice (in %s and %s)", tok, prev, k)
			}
			where[tok] = k
			cid, ns, _ := strings.Cut(tok, ":")
			n, err := strconv.Atoi(ns)
			if err != nil {
				t.Fatalf("garbage token %q in %s", tok, k)
			}
			wantKey := "shared"
			if n%2 == 1 {
				wantKey = cid
			}
			if k != wantKey {
				t.Errorf("token %s found in %s", tok, k)
			}
			seen[cid+"/"+k] = append(seen[cid+"/"+k], n)
		}
	}
	total := 0
	for i, r := range results {
		cid := fmt.Sprintf("c%d", i)
		total += len(r.acked)
		for _, n := range r.acked {
			if _, ok := where[fmt.Sprintf("%s:%d", cid, n)]; !ok {
				t.Errorf("acknowledged append %s:%d is missing", cid, n)
			}
		}
		for _, k := range []string{"shared", cid} {
			ns := seen[cid+"/"+k]
			for j := 1; j < len(ns); j++ {
				if ns[j] <= ns[j-1] {
					t.Errorf("%s tokens out of order in %s: %d after %d", cid, k, ns[j], ns[j-1])
					break
				}
			}
		}
		// nothing beyond what the client sent may show up
		last := -1
		if len(r.acked) > 0 {
			last = r.acked[len(r.acked)-1]
		}
		for k, ns := range seen {
			if strings.HasPrefix(k, cid+"/") {
				for _, n := range ns {
					if n > last && n != r.unknown {
						t.Errorf("%s:%d was never sent but is in %s", cid, n, k)
					}
				}
			}
		}
	}
	t.Logf("%d acknowledged appends, applied index %d", total, st.applied)
	if total < 50 {
		t.Errorf("only %d appends got through in %v", total, duration)
	}
}
