// Command raftkv runs one node of a raftkv cluster.
//
//	raftkv -id n1 -addr 127.0.0.1:8001 \
//	    -peers n1=127.0.0.1:8001,n2=127.0.0.1:8002,n3=127.0.0.1:8003 \
//	    -data ./data/n1
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/Om-Patel88/raft-kv/raft/wal"
	"github.com/Om-Patel88/raft-kv/server"
)

type options struct {
	id, addr, peers, dataDir  string
	electionMin, electionMax  time.Duration
	heartbeat, requestTimeout time.Duration
	snapshotThreshold         int
	noSync, verbose           bool
}

func main() {
	var o options
	flag.StringVar(&o.id, "id", "", "this node's id, e.g. n1")
	flag.StringVar(&o.addr, "addr", "", "listen address; defaults to this node's entry in -peers")
	flag.StringVar(&o.peers, "peers", "", "comma-separated id=host:port for every node, including this one")
	flag.StringVar(&o.dataDir, "data", "", "data directory (default ./data/<id>)")
	flag.DurationVar(&o.electionMin, "election-min", 300*time.Millisecond, "minimum election timeout")
	flag.DurationVar(&o.electionMax, "election-max", 600*time.Millisecond, "maximum election timeout")
	flag.DurationVar(&o.heartbeat, "heartbeat", 100*time.Millisecond, "leader heartbeat interval")
	flag.DurationVar(&o.requestTimeout, "request-timeout", 3*time.Second, "how long a client request may wait to commit")
	flag.IntVar(&o.snapshotThreshold, "snapshot-threshold", 1000, "snapshot after this many log entries, 0 to disable")
	flag.BoolVar(&o.noSync, "nosync", false, "skip fsync (faster, but not crash safe)")
	flag.BoolVar(&o.verbose, "v", false, "log raft internals")
	flag.Parse()

	logger := log.New(os.Stderr, "", log.LstdFlags)
	if err := run(logger, o); err != nil {
		logger.Printf("raftkv: %v", err)
		os.Exit(1)
	}
}

func parsePeers(s string) (map[string]string, error) {
	peers := map[string]string{}
	for _, p := range strings.Split(s, ",") {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		id, addr, ok := strings.Cut(p, "=")
		if !ok || id == "" || addr == "" {
			return nil, fmt.Errorf("bad peer %q, want id=host:port", p)
		}
		if _, dup := peers[id]; dup {
			return nil, fmt.Errorf("peer %s listed twice", id)
		}
		peers[id] = addr
	}
	if len(peers) == 0 {
		return nil, errors.New("-peers is required")
	}
	return peers, nil
}

func run(logger *log.Logger, o options) error {
	id, addr, dataDir := o.id, o.addr, o.dataDir
	if id == "" {
		return errors.New("-id is required")
	}
	peers, err := parsePeers(o.peers)
	if err != nil {
		return err
	}
	if _, ok := peers[id]; !ok {
		return fmt.Errorf("-peers has no entry for %s", id)
	}
	// The address in -peers is what other nodes and clients are told; -addr
	// only changes what we bind to (e.g. 0.0.0.0:8001).
	if addr == "" {
		addr = peers[id]
	}
	if dataDir == "" {
		dataDir = "data/" + id
	}

	w, err := wal.Open(dataDir, wal.Options{NoSync: o.noSync})
	if err != nil {
		return fmt.Errorf("opening %s: %w", dataDir, err)
	}
	defer w.Close()

	var raftLog *log.Logger
	if o.verbose {
		raftLog = log.New(os.Stderr, "", log.LstdFlags|log.Lmicroseconds)
	}
	srv, err := server.New(server.Config{
		ID:                 id,
		Peers:              peers,
		Storage:            w,
		SnapshotThreshold:  o.snapshotThreshold,
		ElectionTimeoutMin: o.electionMin,
		ElectionTimeoutMax: o.electionMax,
		HeartbeatInterval:  o.heartbeat,
		RequestTimeout:     o.requestTimeout,
		Logger:             logger,
		RaftLogger:         raftLog,
	})
	if err != nil {
		return err
	}

	l, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	logger.Printf("node %s: data in %s, peers %s", id, dataDir, o.peers)

	errc := make(chan error, 1)
	go func() { errc <- srv.Serve(l) }()

	stopWatch := make(chan struct{})
	go watchRole(logger, srv, stopWatch)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	select {
	case err := <-errc:
		close(stopWatch)
		srv.Shutdown(context.Background())
		return err
	case s := <-sig:
		logger.Printf("%v received, shutting down", s)
	}
	close(stopWatch)
	signal.Stop(sig)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		logger.Printf("shutdown: %v", err)
	}
	if err := <-errc; err != nil {
		return err
	}
	logger.Printf("stopped")
	return nil
}

// watchRole logs whenever the node's role, term or known leader changes.
// Polling is crude but keeps raft free of callbacks.
func watchRole(logger *log.Logger, srv *server.Server, stop <-chan struct{}) {
	t := time.NewTicker(50 * time.Millisecond)
	defer t.Stop()
	var last string
	for {
		select {
		case <-stop:
			return
		case <-t.C:
		}
		st := srv.Store().Status()
		cur := fmt.Sprintf("%s term=%d leader=%s", st.Role, st.Term, st.Leader)
		if st.Leader == "" {
			cur = fmt.Sprintf("%s term=%d leader=none", st.Role, st.Term)
		}
		if cur != last {
			logger.Printf("%s", cur)
			last = cur
		}
	}
}
