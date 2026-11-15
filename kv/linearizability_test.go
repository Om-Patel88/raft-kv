package kv

import (
	"fmt"
	"math"
	"math/rand/v2"
	"sync"
	"testing"
	"time"
)

// regOp is one operation on a single register, with the times (relative to
// the start of the test) it was invoked and returned. A write whose
// outcome is unknown has ret = math.MaxInt64: it may have taken effect at
// any point after call, or never.
type regOp struct {
	write bool
	value string // written, or read ("" for a missing key)
	call  int64
	ret   int64
}

func (o regOp) pending() bool { return o.ret == math.MaxInt64 }

// linearizable reports whether some total order of ops respects real time
// (an op that returned before another was invoked comes first) and makes
// every read see the latest write before it, starting from "".
//
// It is the usual depth-first search: an op can go next only if no other
// op still to be placed returned before it was invoked. States already
// explored (set of placed ops, register value) are remembered so the
// search stays small on short histories. Pending writes may be left out.
func linearizable(ops []regOp) bool {
	if len(ops) > 64 {
		panic("history too long for the checker")
	}
	var required uint64
	for i, o := range ops {
		if !o.pending() {
			required |= 1 << i
		}
	}
	type state struct {
		placed uint64
		value  string
	}
	seen := map[state]bool{}
	var search func(placed uint64, value string) bool
	search = func(placed uint64, value string) bool {
		if placed&required == required {
			return true
		}
		st := state{placed, value}
		if seen[st] {
			return false
		}
		seen[st] = true

		earliestRet := int64(math.MaxInt64)
		for i, o := range ops {
			if placed&(1<<i) == 0 && o.ret < earliestRet {
				earliestRet = o.ret
			}
		}
		for i, o := range ops {
			if placed&(1<<i) != 0 || o.call > earliestRet {
				continue
			}
			if o.write {
				if search(placed|1<<i, o.value) {
					return true
				}
			} else if o.value == value && search(placed|1<<i, value) {
				return true
			}
		}
		return false
	}
	return search(0, "")
}

func TestLinearizableChecker(t *testing.T) {
	w := func(v string, call, ret int64) regOp { return regOp{write: true, value: v, call: call, ret: ret} }
	r := func(v string, call, ret int64) regOp { return regOp{value: v, call: call, ret: ret} }
	pend := int64(math.MaxInt64)
	for _, tc := range []struct {
		name string
		ops  []regOp
		ok   bool
	}{
		{"empty", nil, true},
		{"initial read", []regOp{r("", 0, 1)}, true},
		{"sequential", []regOp{w("a", 0, 1), r("a", 2, 3), w("b", 4, 5), r("b", 6, 7)}, true},
		{"stale read", []regOp{w("a", 0, 1), w("b", 2, 3), r("a", 4, 5)}, false},
		{"read from nowhere", []regOp{w("a", 0, 1), r("x", 2, 3)}, false},
		{"concurrent write either way", []regOp{w("a", 0, 10), r("a", 1, 2), r("", 0, 1)}, true},
		{"reads disagree on order", []regOp{
			w("a", 0, 10), w("b", 0, 10),
			r("a", 1, 2), r("b", 3, 4), r("a", 5, 6),
		}, false},
		{"pending write may land late", []regOp{w("a", 0, pend), r("", 1, 2), r("a", 3, 4)}, true},
		{"pending write may never land", []regOp{w("a", 0, pend), r("", 5, 6)}, true},
		{"pending write cannot come back", []regOp{
			w("a", 0, pend), r("a", 1, 2), w("b", 3, 4), r("a", 5, 6),
		}, false},
	} {
		if got := linearizable(tc.ops); got != tc.ok {
			t.Errorf("%s: linearizable = %v, want %v", tc.name, got, tc.ok)
		}
	}
}

// Concurrent clients put and get one key while the leader gets isolated
// and the network heals; the resulting history has to be linearizable.
func TestClusterLinearizable(t *testing.T) {
	for round := 0; round < 3; round++ {
		c := newCluster(t, 3)
		c.leader()
		start := time.Now()
		now := func() int64 { return int64(time.Since(start)) }

		const nclients, nops = 4, 12
		var mu sync.Mutex
		var history []regOp
		var wg sync.WaitGroup
		for i := 0; i < nclients; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				tc := c.client(fmt.Sprintf("c%d", i))
				tc.wander = true
				for n := 0; n < nops; n++ {
					op := regOp{write: rand.IntN(2) == 0}
					req := Request{Op: OpGet, Key: "reg"}
					if op.write {
						op.value = fmt.Sprintf("%d-%d", i, n)
						req = Request{Op: OpPut, Key: "reg", Value: op.value}
					}
					op.call = now()
					resp, err := tc.do(req, 3*time.Second)
					op.ret = now()
					if err != nil {
						if !op.write {
							continue // a read that never finished tells us nothing
						}
						op.ret = math.MaxInt64
					} else if !op.write {
						op.value = resp.Value
					}
					mu.Lock()
					history = append(history, op)
					mu.Unlock()
					time.Sleep(time.Duration(rand.IntN(30)) * time.Millisecond)
				}
			}(i)
		}

		time.Sleep(100 * time.Millisecond)
		lead := c.leader()
		c.net.Partition([]string{lead}, c.others(lead))
		time.Sleep(800 * time.Millisecond)
		c.net.Heal()
		wg.Wait()
		for _, id := range c.ids {
			c.crash(id)
		}

		if !linearizable(history) {
			for _, o := range history {
				t.Logf("write=%v value=%q call=%d ret=%d", o.write, o.value, o.call, o.ret)
			}
			t.Fatalf("round %d: history of %d ops is not linearizable", round, len(history))
		}
	}
}
