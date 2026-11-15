// Package server puts a kv.Store behind HTTP: a JSON client API, the raft
// RPC endpoints peers talk to, and an HTTP raft.Transport.
package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"sort"
	"strconv"
	"time"

	"github.com/Om-Patel88/raft-kv/kv"
	"github.com/Om-Patel88/raft-kv/raft"
)

const (
	HeaderClientID = "X-Client-Id"
	HeaderSeq      = "X-Seq"

	maxBody               = 1 << 20
	defaultRequestTimeout = 3 * time.Second
)

type Config struct {
	ID    string
	Peers map[string]string // node id -> host:port, including this node

	Storage            raft.Storage
	SnapshotThreshold  int
	ElectionTimeoutMin time.Duration
	ElectionTimeoutMax time.Duration
	HeartbeatInterval  time.Duration

	// RequestTimeout bounds how long a client request waits to commit.
	RequestTimeout time.Duration

	Logger     *log.Logger // server events, nil means silent
	RaftLogger *log.Logger // raft internals, nil means silent
}

type Server struct {
	cfg       Config
	addr      string
	store     *kv.Store
	transport *HTTPTransport
	http      *http.Server
	logger    *log.Logger
}

// StatusResponse is the body of GET /status.
type StatusResponse struct {
	kv.Status
	Addr       string `json:"addr"`
	LeaderAddr string `json:"leader_addr,omitempty"`
}

// errorResponse is the body of every non-2xx answer. LeaderID and
// LeaderAddr are filled in on 421 so the client knows where to go.
type errorResponse struct {
	Error      string `json:"error"`
	LeaderID   string `json:"leader_id,omitempty"`
	LeaderAddr string `json:"leader_addr,omitempty"`
}

type casBody struct {
	Expected string `json:"expected"`
	New      string `json:"new"`
}

func New(cfg Config) (*Server, error) {
	addr, ok := cfg.Peers[cfg.ID]
	if !ok {
		return nil, fmt.Errorf("server: peers do not include this node (%s)", cfg.ID)
	}
	if cfg.RequestTimeout <= 0 {
		cfg.RequestTimeout = defaultRequestTimeout
	}
	ids := make([]string, 0, len(cfg.Peers))
	for id := range cfg.Peers {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	tr := NewHTTPTransport(cfg.Peers)
	store, err := kv.NewStore(kv.Config{
		ID:                 cfg.ID,
		Peers:              ids,
		Storage:            cfg.Storage,
		Transport:          tr,
		ElectionTimeoutMin: cfg.ElectionTimeoutMin,
		ElectionTimeoutMax: cfg.ElectionTimeoutMax,
		HeartbeatInterval:  cfg.HeartbeatInterval,
		SnapshotThreshold:  cfg.SnapshotThreshold,
		Logger:             cfg.RaftLogger,
	})
	if err != nil {
		return nil, err
	}
	s := &Server{cfg: cfg, addr: addr, store: store, transport: tr, logger: cfg.Logger}
	s.http = &http.Server{
		Handler:           s.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	return s, nil
}

func (s *Server) Store() *kv.Store { return s.store }
func (s *Server) Addr() string     { return s.addr }

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /kv/{key}", s.handleGet)
	mux.HandleFunc("PUT /kv/{key}", s.handlePut)
	mux.HandleFunc("DELETE /kv/{key}", s.handleDelete)
	mux.HandleFunc("POST /kv/{key}/append", s.handleAppend)
	mux.HandleFunc("POST /kv/{key}/cas", s.handleCAS)
	mux.HandleFunc("GET /status", s.handleStatus)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, "ok\n")
	})

	node := s.store.Raft()
	mux.HandleFunc("POST "+pathRequestVote, rpcHandler(node.HandleRequestVote))
	mux.HandleFunc("POST "+pathAppendEntries, rpcHandler(node.HandleAppendEntries))
	mux.HandleFunc("POST "+pathInstallSnapshot, rpcHandler(node.HandleInstallSnapshot))
	return mux
}

// Serve starts the store and serves on l until Shutdown. It returns nil
// after a clean shutdown.
func (s *Server) Serve(l net.Listener) error {
	s.store.Start()
	s.logf("serving on %s", l.Addr())
	err := s.http.Serve(l)
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

func (s *Server) ListenAndServe() error {
	l, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}
	return s.Serve(l)
}

// Shutdown stops accepting connections, lets in-flight requests finish
// (bounded by ctx), then stops the store. The caller still owns Storage.
func (s *Server) Shutdown(ctx context.Context) error {
	err := s.http.Shutdown(ctx)
	s.store.Close()
	s.transport.Close()
	return err
}

func (s *Server) logf(format string, args ...any) {
	if s.logger != nil {
		s.logger.Printf(format, args...)
	}
}

func rpcHandler[A, R any](handle func(*A) *R) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var args A
		// snapshots can be big; the client API limit doesn't apply here
		if err := json.NewDecoder(r.Body).Decode(&args); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		writeJSON(w, http.StatusOK, handle(&args))
	}
}

func (s *Server) handleGet(w http.ResponseWriter, r *http.Request) {
	resp, ok := s.do(w, r, kv.Request{Op: kv.OpGet})
	if !ok {
		return
	}
	if !resp.Found {
		writeJSON(w, http.StatusNotFound, errorResponse{Error: "key not found"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"value": resp.Value})
}

func (s *Server) handlePut(w http.ResponseWriter, r *http.Request) {
	body, ok := readBody(w, r)
	if !ok {
		return
	}
	if _, ok := s.do(w, r, kv.Request{Op: kv.OpPut, Value: body}); ok {
		w.WriteHeader(http.StatusNoContent)
	}
}

func (s *Server) handleDelete(w http.ResponseWriter, r *http.Request) {
	if _, ok := s.do(w, r, kv.Request{Op: kv.OpDelete}); ok {
		w.WriteHeader(http.StatusNoContent)
	}
}

func (s *Server) handleAppend(w http.ResponseWriter, r *http.Request) {
	body, ok := readBody(w, r)
	if !ok {
		return
	}
	if _, ok := s.do(w, r, kv.Request{Op: kv.OpAppend, Value: body}); ok {
		w.WriteHeader(http.StatusNoContent)
	}
}

func (s *Server) handleCAS(w http.ResponseWriter, r *http.Request) {
	body, ok := readBody(w, r)
	if !ok {
		return
	}
	var cb casBody
	if err := json.Unmarshal([]byte(body), &cb); err != nil {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "cas body must be {\"expected\": ..., \"new\": ...}"})
		return
	}
	resp, ok := s.do(w, r, kv.Request{Op: kv.OpCAS, Expected: cb.Expected, Value: cb.New})
	if ok {
		writeJSON(w, http.StatusOK, map[string]bool{"swapped": resp.Swapped})
	}
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	st := s.store.Status()
	writeJSON(w, http.StatusOK, StatusResponse{
		Status:     st,
		Addr:       s.addr,
		LeaderAddr: s.cfg.Peers[st.Leader],
	})
}

// do fills in the key and client identity, runs req and writes an error
// response if it fails. It reports whether the caller should write the
// success response.
func (s *Server) do(w http.ResponseWriter, r *http.Request, req kv.Request) (kv.Response, bool) {
	req.Key = r.PathValue("key")
	// Without a client id the request is still served, just without
	// duplicate detection. Making up an id per request would only fill
	// the session table with entries nobody will ever retry.
	if id := r.Header.Get(HeaderClientID); id != "" {
		seq, err := strconv.ParseUint(r.Header.Get(HeaderSeq), 10, 64)
		if err != nil || seq == 0 {
			writeJSON(w, http.StatusBadRequest, errorResponse{Error: HeaderSeq + " must be a positive integer when " + HeaderClientID + " is set"})
			return kv.Response{}, false
		}
		req.ClientID, req.Seq = id, seq
	}

	ctx, cancel := context.WithTimeout(r.Context(), s.cfg.RequestTimeout)
	defer cancel()
	resp, err := s.store.Do(ctx, req)
	if err == nil {
		return resp, true
	}

	var nl *kv.NotLeaderError
	switch {
	case errors.As(err, &nl) && nl.Leader != "":
		writeJSON(w, http.StatusMisdirectedRequest, errorResponse{
			Error:      "not leader",
			LeaderID:   nl.Leader,
			LeaderAddr: s.cfg.Peers[nl.Leader],
		})
	case errors.As(err, &nl):
		writeJSON(w, http.StatusServiceUnavailable, errorResponse{Error: "no leader"})
	case errors.Is(err, kv.ErrBadRequest):
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: err.Error()})
	default:
		// lost leadership, timed out, shutting down: all worth retrying,
		// possibly on another node
		writeJSON(w, http.StatusServiceUnavailable, errorResponse{Error: err.Error()})
	}
	return kv.Response{}, false
}

func readBody(w http.ResponseWriter, r *http.Request) (string, bool) {
	b, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBody))
	if err != nil {
		var tooBig *http.MaxBytesError
		if errors.As(err, &tooBig) {
			writeJSON(w, http.StatusRequestEntityTooLarge, errorResponse{Error: "body too large"})
		} else {
			writeJSON(w, http.StatusBadRequest, errorResponse{Error: err.Error()})
		}
		return "", false
	}
	return string(b), true
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}
