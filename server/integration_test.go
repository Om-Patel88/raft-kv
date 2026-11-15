package server_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Om-Patel88/raft-kv/client"
	"github.com/Om-Patel88/raft-kv/raft"
	"github.com/Om-Patel88/raft-kv/raft/wal"
	"github.com/Om-Patel88/raft-kv/server"
)

// node is one real server process, minus the process.
type node struct {
	id   string
	srv  *server.Server
	wal  *wal.WAL
	done chan error
}

type testCluster struct {
	t     *testing.T
	dir   string
	peers map[string]string
	nodes map[string]*node
}

func newTestCluster(t *testing.T, n int) *testCluster {
	t.Helper()
	c := &testCluster{t: t, dir: t.TempDir(), peers: map[string]string{}, nodes: map[string]*node{}}
	// Grab free ports up front so every node knows the others' addresses.
	listeners := map[string]net.Listener{}
	for i := 1; i <= n; i++ {
		id := fmt.Sprintf("n%d", i)
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		listeners[id] = l
		c.peers[id] = l.Addr().String()
	}
	for id, l := range listeners {
		c.start(id, l)
	}
	t.Cleanup(func() {
		for id := range c.nodes {
			c.stop(id)
		}
	})
	return c
}

func (c *testCluster) start(id string, l net.Listener) {
	c.t.Helper()
	if l == nil {
		var err error
		// the old port should be free again now that the server is gone
		for i := 0; i < 50; i++ {
			if l, err = net.Listen("tcp", c.peers[id]); err == nil {
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
		if err != nil {
			c.t.Fatalf("relisten on %s: %v", c.peers[id], err)
		}
	}
	w, err := wal.Open(filepath.Join(c.dir, id), wal.Options{})
	if err != nil {
		c.t.Fatal(err)
	}
	srv, err := server.New(server.Config{
		ID:                 id,
		Peers:              c.peers,
		Storage:            w,
		SnapshotThreshold:  20,
		ElectionTimeoutMin: 150 * time.Millisecond,
		ElectionTimeoutMax: 300 * time.Millisecond,
		HeartbeatInterval:  50 * time.Millisecond,
		RequestTimeout:     time.Second,
	})
	if err != nil {
		c.t.Fatal(err)
	}
	nd := &node{id: id, srv: srv, wal: w, done: make(chan error, 1)}
	c.nodes[id] = nd
	go func() { nd.done <- srv.Serve(l) }()
}

func (c *testCluster) stop(id string) {
	nd := c.nodes[id]
	if nd == nil {
		return
	}
	delete(c.nodes, id)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := nd.srv.Shutdown(ctx); err != nil {
		c.t.Errorf("shutdown %s: %v", id, err)
	}
	if err := <-nd.done; err != nil {
		c.t.Errorf("serve %s: %v", id, err)
	}
	nd.wal.Close()
}

func (c *testCluster) addrs() []string {
	var out []string
	for i := 1; i <= len(c.peers); i++ {
		out = append(out, c.peers[fmt.Sprintf("n%d", i)])
	}
	return out
}

func (c *testCluster) leader(cl *client.Client) (string, *client.NodeStatus) {
	c.t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		for id := range c.nodes {
			st, err := cl.Status(context.Background(), c.peers[id])
			if err == nil && st.Role == raft.Leader {
				return id, st
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	c.t.Fatal("no leader")
	return "", nil
}

func ctxT(t *testing.T) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func TestHTTPCluster(t *testing.T) {
	c := newTestCluster(t, 3)
	cl := client.New(c.addrs())
	defer cl.Close()
	ctx := ctxT(t)

	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	get := func(key string) (string, bool) {
		t.Helper()
		v, ok, err := cl.Get(ctx, key)
		must(err)
		return v, ok
	}

	must(cl.Put(ctx, "a", "1"))
	if v, ok := get("a"); !ok || v != "1" {
		t.Fatalf("get a = %q %v", v, ok)
	}
	swapped, err := cl.CAS(ctx, "a", "1", "2")
	must(err)
	if !swapped {
		t.Fatal("cas 1->2 did not swap")
	}
	if swapped, _ = cl.CAS(ctx, "a", "1", "3"); swapped {
		t.Fatal("cas against a stale value swapped")
	}
	must(cl.Append(ctx, "a", "x"))
	must(cl.Put(ctx, "gone", "soon"))
	must(cl.Delete(ctx, "gone"))
	if _, ok := get("gone"); ok {
		t.Fatal("deleted key still there")
	}
	must(cl.Put(ctx, "spaces and/slashes", "ok"))
	if v, _ := get("spaces and/slashes"); v != "ok" {
		t.Fatalf("odd key = %q", v)
	}
	for i := 0; i < 30; i++ {
		must(cl.Append(ctx, "log", "."))
	}

	// shut the leader down; the client has to find the new one
	old, st := c.leader(cl)
	before := st.CommitIndex
	c.stop(old)
	if v, _ := get("a"); v != "2x" {
		t.Fatalf("after failover a = %q, want 2x", v)
	}
	must(cl.Put(ctx, "b", "written while "+old+" was down"))
	newLead, _ := c.leader(cl)
	if newLead == old {
		t.Fatal("stopped node is still leader")
	}

	// bring it back and wait for it to catch up
	c.start(old, nil)
	_, lst := c.leader(cl)
	want := lst.CommitIndex
	deadline := time.Now().Add(5 * time.Second)
	for {
		st, err := cl.Status(ctx, c.peers[old])
		if err == nil && st.LastApplied >= want && st.Keys == 4 {
			if st.CommitIndex <= before {
				t.Fatalf("restarted node commit %d, was %d before", st.CommitIndex, before)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s did not catch up: %+v (err %v), leader commit %d", old, st, err, want)
		}
		time.Sleep(50 * time.Millisecond)
	}
	if v, _ := get("log"); v != strings.Repeat(".", 30) {
		t.Fatalf("log = %q", v)
	}
}

// Checks the raw HTTP surface: status codes, redirects, and that plain
// requests without client headers work.
func TestHTTPAPI(t *testing.T) {
	c := newTestCluster(t, 3)
	cl := client.New(c.addrs())
	defer cl.Close()
	lead, _ := c.leader(cl)
	if err := cl.Put(ctxT(t), "k", "v"); err != nil {
		t.Fatal(err)
	}

	req := func(method, addr, path, body string, hdr ...string) (int, string) {
		t.Helper()
		r, _ := http.NewRequest(method, "http://"+addr+path, strings.NewReader(body))
		for i := 0; i+1 < len(hdr); i += 2 {
			r.Header.Set(hdr[i], hdr[i+1])
		}
		resp, err := http.DefaultClient.Do(r)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(b)
	}
	la := c.peers[lead]

	if code, body := req("GET", la, "/kv/k", ""); code != 200 || !strings.Contains(body, `"value":"v"`) {
		t.Fatalf("get: %d %s", code, body)
	}
	if code, _ := req("GET", la, "/kv/missing", ""); code != 404 {
		t.Fatalf("missing key: %d", code)
	}
	if code, _ := req("PUT", la, "/kv/plain", "no headers"); code != 204 {
		t.Fatalf("put without headers: %d", code)
	}
	if code, _ := req("POST", la, "/kv/plain/append", "!", server.HeaderClientID, "x", server.HeaderSeq, "zero"); code != 400 {
		t.Fatalf("bad seq: %d", code)
	}
	// the append above was rejected, so the value is still "no headers"
	if code, body := req("POST", la, "/kv/plain/cas", `{"expected":"no headers!","new":"z"}`); code != 200 || !strings.Contains(body, `"swapped":false`) {
		t.Fatalf("cas: %d %s", code, body)
	}
	if code, body := req("POST", la, "/kv/plain/cas", `{"expected":"no headers","new":"z"}`); code != 200 || !strings.Contains(body, `"swapped":true`) {
		t.Fatalf("cas: %d %s", code, body)
	}
	if code, _ := req("POST", la, "/kv/plain/cas", `not json`); code != 400 {
		t.Fatalf("bad cas body: %d", code)
	}
	if code, body := req("GET", la, "/healthz", ""); code != 200 || body != "ok\n" {
		t.Fatalf("healthz: %d %q", code, body)
	}

	// a retried write with the same id and seq applies once
	for i := 0; i < 3; i++ {
		if code, _ := req("POST", la, "/kv/once/append", "x", server.HeaderClientID, "curl", server.HeaderSeq, "7"); code != 204 {
			t.Fatalf("append: %d", code)
		}
	}
	if _, body := req("GET", la, "/kv/once", ""); !strings.Contains(body, `"value":"x"`) {
		t.Fatalf("duplicate appends applied: %s", body)
	}

	for id, addr := range c.peers {
		if id == lead {
			continue
		}
		code, body := req("GET", addr, "/kv/k", "")
		if code != http.StatusMisdirectedRequest {
			t.Fatalf("follower %s answered %d %s", id, code, body)
		}
		var e struct {
			LeaderID   string `json:"leader_id"`
			LeaderAddr string `json:"leader_addr"`
		}
		json.Unmarshal([]byte(body), &e)
		if e.LeaderID != lead || e.LeaderAddr != la {
			t.Fatalf("follower %s hint %+v, want %s at %s", id, e, lead, la)
		}

		// a client that only knows this follower gets redirected
		fc := client.New([]string{addr})
		if v, ok, err := fc.Get(ctxT(t), "k"); err != nil || !ok || v != "v" {
			t.Fatalf("via follower: %q %v %v", v, ok, err)
		}
		fc.Close()

		var st client.NodeStatus
		_, body = req("GET", addr, "/status", "")
		if err := json.Unmarshal([]byte(body), &st); err != nil {
			t.Fatal(err)
		}
		if st.ID != id || st.Addr != addr || st.Leader != lead || st.LeaderAddr != la || st.Role != raft.Follower {
			t.Fatalf("status of %s: %s", id, body)
		}
	}
}

// With two of three nodes gone there is no leader to be found, and the
// client gives up when its context does instead of hanging.
func TestHTTPNoQuorum(t *testing.T) {
	c := newTestCluster(t, 3)
	cl := client.New(c.addrs())
	defer cl.Close()
	lead, _ := c.leader(cl)
	if err := cl.Put(ctxT(t), "k", "v"); err != nil {
		t.Fatal(err)
	}
	for id := range c.peers {
		if id != lead {
			c.stop(id)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
	defer cancel()
	start := time.Now()
	if _, _, err := cl.Get(ctx, "k"); err == nil {
		t.Fatal("read succeeded without a quorum")
	}
	if d := time.Since(start); d > 3*time.Second {
		t.Fatalf("client took %v to give up", d)
	}
}
