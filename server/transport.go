package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"time"

	"github.com/Om-Patel88/raft-kv/raft"
)

const (
	pathRequestVote     = "/raft/request-vote"
	pathAppendEntries   = "/raft/append-entries"
	pathInstallSnapshot = "/raft/install-snapshot"
)

// HTTPTransport sends raft RPCs as JSON over HTTP. Deadlines come from the
// ctx raft passes in, so the http.Client itself has no timeout.
type HTTPTransport struct {
	peers  map[string]string // node id -> host:port
	client *http.Client
}

var _ raft.Transport = (*HTTPTransport)(nil)

func NewHTTPTransport(peers map[string]string) *HTTPTransport {
	cp := make(map[string]string, len(peers))
	for id, addr := range peers {
		cp[id] = addr
	}
	return &HTTPTransport{
		peers: cp,
		client: &http.Client{Transport: &http.Transport{
			DialContext:         (&net.Dialer{Timeout: time.Second, KeepAlive: 30 * time.Second}).DialContext,
			MaxIdleConnsPerHost: 4,
			IdleConnTimeout:     time.Minute,
		}},
	}
}

func (t *HTTPTransport) RequestVote(ctx context.Context, to string, args *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
	var reply raft.RequestVoteReply
	return &reply, t.call(ctx, to, pathRequestVote, args, &reply)
}

func (t *HTTPTransport) AppendEntries(ctx context.Context, to string, args *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
	var reply raft.AppendEntriesReply
	return &reply, t.call(ctx, to, pathAppendEntries, args, &reply)
}

func (t *HTTPTransport) InstallSnapshot(ctx context.Context, to string, args *raft.InstallSnapshotArgs) (*raft.InstallSnapshotReply, error) {
	var reply raft.InstallSnapshotReply
	return &reply, t.call(ctx, to, pathInstallSnapshot, args, &reply)
}

// Close drops idle connections to peers.
func (t *HTTPTransport) Close() { t.client.CloseIdleConnections() }

func (t *HTTPTransport) call(ctx context.Context, to, path string, args, reply any) error {
	addr, ok := t.peers[to]
	if !ok {
		return fmt.Errorf("transport: unknown peer %q", to)
	}
	body, err := json.Marshal(args)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+addr+path, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := t.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("transport: %s%s: %s: %s", to, path, resp.Status, bytes.TrimSpace(msg))
	}
	return json.NewDecoder(resp.Body).Decode(reply)
}
