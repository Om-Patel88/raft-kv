package client

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

type hit struct {
	server string
	client string
	seq    string
}

// fakeCluster is three httptest servers: a follower that redirects to the
// leader, a node that is unavailable, and the leader, which fails the
// first request it sees with 503 to force a retry.
func fakeCluster(t *testing.T) (addrs []string, hits func() []hit) {
	var mu sync.Mutex
	var log []hit
	record := func(name string, r *http.Request) {
		mu.Lock()
		log = append(log, hit{name, r.Header.Get(headerClientID), r.Header.Get(headerSeq)})
		mu.Unlock()
	}

	leaderSeen := 0
	leader := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		record("leader", r)
		mu.Lock()
		leaderSeen++
		first := leaderSeen == 1
		mu.Unlock()
		if first {
			w.WriteHeader(http.StatusServiceUnavailable)
			fmt.Fprint(w, `{"error":"leadership lost"}`)
			return
		}
		switch {
		case r.Method == "GET" && r.URL.Path == "/kv/missing":
			w.WriteHeader(http.StatusNotFound)
		case r.Method == "GET":
			fmt.Fprint(w, `{"value":"hello"}`)
		case strings.HasSuffix(r.URL.Path, "/cas"):
			fmt.Fprint(w, `{"swapped":true}`)
		case r.URL.Path == "/kv/bad":
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, `{"error":"nope"}`)
		default:
			w.WriteHeader(http.StatusNoContent)
		}
	}))
	follower := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		record("follower", r)
		w.WriteHeader(http.StatusMisdirectedRequest)
		fmt.Fprintf(w, `{"error":"not leader","leader_id":"n3","leader_addr":%q}`, strings.TrimPrefix(leader.URL, "http://"))
	}))
	down := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		record("down", r)
		w.WriteHeader(http.StatusServiceUnavailable)
		fmt.Fprint(w, `{"error":"no leader"}`)
	}))
	t.Cleanup(func() { leader.Close(); follower.Close(); down.Close() })

	addrs = []string{follower.URL, down.URL, strings.TrimPrefix(leader.URL, "http://")}
	return addrs, func() []hit {
		mu.Lock()
		defer mu.Unlock()
		return append([]hit(nil), log...)
	}
}

func TestClientRetriesWithSameSeq(t *testing.T) {
	addrs, hits := fakeCluster(t)
	c := New(addrs)
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := c.Put(ctx, "k", "v"); err != nil {
		t.Fatal(err)
	}
	// follower redirects to leader, leader says 503, client moves on to
	// the next server (wrapping around), and eventually the leader again
	hs := hits()
	if hs[0].server != "follower" || hs[1].server != "leader" || hs[len(hs)-1].server != "leader" {
		t.Fatalf("unexpected route: %+v", hs)
	}
	for _, h := range hs {
		if h.seq != "1" || h.client != c.ID() {
			t.Fatalf("retry changed identity: %+v", hs)
		}
	}

	// now it knows the leader and goes there first, with the next seq
	n := len(hs)
	v, ok, err := c.Get(ctx, "k")
	if err != nil || !ok || v != "hello" {
		t.Fatalf("get: %q %v %v", v, ok, err)
	}
	if hs = hits()[n:]; len(hs) != 1 || hs[0].server != "leader" || hs[0].seq != "2" {
		t.Fatalf("second op went %+v", hs)
	}

	if _, ok, err := c.Get(ctx, "missing"); ok || err != nil {
		t.Fatalf("missing key: %v %v", ok, err)
	}
	if swapped, err := c.CAS(ctx, "k", "a", "b"); err != nil || !swapped {
		t.Fatalf("cas: %v %v", swapped, err)
	}
	var se *StatusError
	if err := c.Put(ctx, "bad", "x"); !errors.As(err, &se) || se.Code != 400 || se.Message != "nope" {
		t.Fatalf("bad request: %v", err)
	}
}

func TestClientGivesUp(t *testing.T) {
	c := New([]string{"127.0.0.1:1"}) // nothing listens on port 1
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	err := c.Put(ctx, "k", "v")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("got %v, want deadline exceeded", err)
	}
}
