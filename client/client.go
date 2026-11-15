// Package client talks to a raftkv cluster over HTTP. It finds the leader
// on its own, follows redirects from followers and retries through
// failovers. Retries reuse the request's sequence number, so a write that
// is retried after a lost reply is still applied only once.
package client

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Om-Patel88/raft-kv/raft"
)

const (
	headerClientID = "X-Client-Id"
	headerSeq      = "X-Seq"

	attemptTimeout = 5 * time.Second // the server gives up on a commit after 3s
	minBackoff     = 20 * time.Millisecond
	maxBackoff     = 500 * time.Millisecond
)

// StatusError is a response the client won't retry, such as a bad request.
type StatusError struct {
	Code    int
	Message string
}

func (e *StatusError) Error() string {
	return fmt.Sprintf("server returned %d: %s", e.Code, e.Message)
}

// NodeStatus is what GET /status returns.
type NodeStatus struct {
	raft.Status
	Keys       int    `json:"keys"`
	Addr       string `json:"addr"`
	LeaderAddr string `json:"leader_addr,omitempty"`
}

// Client is safe for concurrent use, but runs one operation at a time:
// the server only remembers the latest sequence number per client, so
// overlapping operations under one id could be mistaken for duplicates.
// Use several Clients for parallel traffic.
type Client struct {
	hc *http.Client
	id string

	opMu sync.Mutex // held for a whole logical operation
	seq  uint64

	mu      sync.Mutex
	servers []string
	leader  int
}

// New returns a client for the given server addresses (host:port).
func New(servers []string) *Client {
	c := &Client{
		id: newID(),
		hc: &http.Client{Transport: &http.Transport{
			DialContext:         (&net.Dialer{Timeout: time.Second}).DialContext,
			MaxIdleConnsPerHost: 2,
			IdleConnTimeout:     30 * time.Second,
		}},
	}
	for _, s := range servers {
		if s = cleanAddr(s); s != "" {
			c.servers = append(c.servers, s)
		}
	}
	return c
}

func newID() string {
	b := make([]byte, 8)
	rand.Read(b)
	return hex.EncodeToString(b)
}

func cleanAddr(s string) string {
	s = strings.TrimSpace(s)
	s = strings.TrimPrefix(s, "http://")
	return strings.TrimSuffix(s, "/")
}

func (c *Client) ID() string { return c.id }

func (c *Client) Servers() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.servers...)
}

// Close drops idle connections.
func (c *Client) Close() { c.hc.CloseIdleConnections() }

// Get returns the value for key and whether it exists.
func (c *Client) Get(ctx context.Context, key string) (string, bool, error) {
	code, body, err := c.do(ctx, http.MethodGet, key, "", nil)
	if err != nil {
		return "", false, err
	}
	switch code {
	case http.StatusOK:
		var r struct {
			Value string `json:"value"`
		}
		if err := json.Unmarshal(body, &r); err != nil {
			return "", false, fmt.Errorf("bad response: %w", err)
		}
		return r.Value, true, nil
	case http.StatusNotFound:
		return "", false, nil
	}
	return "", false, statusError(code, body)
}

func (c *Client) Put(ctx context.Context, key, value string) error {
	return c.noContent(c.do(ctx, http.MethodPut, key, "", []byte(value)))
}

func (c *Client) Delete(ctx context.Context, key string) error {
	return c.noContent(c.do(ctx, http.MethodDelete, key, "", nil))
}

func (c *Client) Append(ctx context.Context, key, value string) error {
	return c.noContent(c.do(ctx, http.MethodPost, key, "/append", []byte(value)))
}

// CAS sets key to newValue if its current value is expected (a missing key
// counts as ""), and reports whether it did.
func (c *Client) CAS(ctx context.Context, key, expected, newValue string) (bool, error) {
	req, _ := json.Marshal(map[string]string{"expected": expected, "new": newValue})
	code, body, err := c.do(ctx, http.MethodPost, key, "/cas", req)
	if err != nil {
		return false, err
	}
	if code != http.StatusOK {
		return false, statusError(code, body)
	}
	var r struct {
		Swapped bool `json:"swapped"`
	}
	if err := json.Unmarshal(body, &r); err != nil {
		return false, fmt.Errorf("bad response: %w", err)
	}
	return r.Swapped, nil
}

// Status asks one server (by address) for its status. It does not retry.
func (c *Client) Status(ctx context.Context, addr string) (*NodeStatus, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+cleanAddr(addr)+"/status", nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.hc.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, statusError(resp.StatusCode, body)
	}
	var st NodeStatus
	if err := json.Unmarshal(body, &st); err != nil {
		return nil, fmt.Errorf("bad status from %s: %w", addr, err)
	}
	return &st, nil
}

func (c *Client) noContent(code int, body []byte, err error) error {
	if err != nil {
		return err
	}
	if code != http.StatusNoContent && code != http.StatusOK {
		return statusError(code, body)
	}
	return nil
}

func statusError(code int, body []byte) error {
	var e struct {
		Error string `json:"error"`
	}
	msg := strings.TrimSpace(string(body))
	if json.Unmarshal(body, &e) == nil && e.Error != "" {
		msg = e.Error
	}
	return &StatusError{Code: code, Message: msg}
}

type errorBody struct {
	Error      string `json:"error"`
	LeaderID   string `json:"leader_id"`
	LeaderAddr string `json:"leader_addr"`
}

// do runs one logical operation until some server gives a definite answer
// or ctx runs out. It returns the final status code and body.
func (c *Client) do(ctx context.Context, method, key, suffix string, body []byte) (int, []byte, error) {
	if key == "" {
		return 0, nil, errors.New("empty key")
	}
	c.opMu.Lock()
	defer c.opMu.Unlock()
	c.seq++
	seq := c.seq
	path := "/kv/" + url.PathEscape(key) + suffix

	c.mu.Lock()
	n := len(c.servers)
	target := c.leader
	c.mu.Unlock()
	if n == 0 {
		return 0, nil, errors.New("no servers configured")
	}

	backoff := minBackoff
	redirects := 0
	var lastErr error
	for {
		if err := ctx.Err(); err != nil {
			if lastErr != nil {
				return 0, nil, fmt.Errorf("%w (last error: %v)", err, lastErr)
			}
			return 0, nil, err
		}
		addr := c.server(target)
		code, respBody, err := c.send(ctx, method, addr, path, body, seq)

		retry := true
		switch {
		case err != nil:
			lastErr = err
		case code == http.StatusMisdirectedRequest:
			var eb errorBody
			json.Unmarshal(respBody, &eb)
			lastErr = fmt.Errorf("%s is not the leader", addr)
			if eb.LeaderAddr != "" && redirects < 2*n {
				redirects++
				target = c.indexOf(eb.LeaderAddr)
				continue // follow the hint straight away
			}
		case code == http.StatusServiceUnavailable:
			lastErr = fmt.Errorf("%s: %s", addr, statusError(code, respBody).(*StatusError).Message)
		default:
			retry = false
		}
		if !retry {
			c.mu.Lock()
			c.leader = target
			c.mu.Unlock()
			return code, respBody, nil
		}

		// try the next server after a short pause
		redirects = 0
		target = (target + 1) % c.numServers()
		select {
		case <-ctx.Done():
		case <-time.After(backoff):
		}
		backoff = min(2*backoff, maxBackoff)
	}
}

func (c *Client) send(ctx context.Context, method, addr, path string, body []byte, seq uint64) (int, []byte, error) {
	actx, cancel := context.WithTimeout(ctx, attemptTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(actx, method, "http://"+addr+path, bytes.NewReader(body))
	if err != nil {
		return 0, nil, err
	}
	req.Header.Set(headerClientID, c.id)
	req.Header.Set(headerSeq, strconv.FormatUint(seq, 10))
	resp, err := c.hc.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, nil, err
	}
	return resp.StatusCode, b, nil
}

func (c *Client) server(i int) string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.servers[i%len(c.servers)]
}

func (c *Client) numServers() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.servers)
}

// indexOf finds addr in the server list, adding it if a leader hint
// pointed somewhere the client wasn't told about.
func (c *Client) indexOf(addr string) int {
	addr = cleanAddr(addr)
	c.mu.Lock()
	defer c.mu.Unlock()
	for i, s := range c.servers {
		if s == addr {
			return i
		}
	}
	c.servers = append(c.servers, addr)
	return len(c.servers) - 1
}
