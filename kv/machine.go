// Package kv is a replicated key-value store built on the raft package.
// A Store turns client requests into log entries and applies committed
// entries to an in-memory map.
package kv

import (
	"encoding/json"
	"errors"
	"fmt"
)

type Op string

const (
	OpGet    Op = "get"
	OpPut    Op = "put"
	OpDelete Op = "del"
	OpAppend Op = "append"
	OpCAS    Op = "cas"
)

// Request is one client operation. It is also what goes into the raft log,
// hence the short JSON names.
//
// ClientID and Seq give exactly-once semantics: a client numbers its
// operations 1, 2, 3, ... and reuses the number when it retries, so the
// state machine can recognise a command it has already applied. An empty
// ClientID opts out of this.
type Request struct {
	Op       Op     `json:"op"`
	Key      string `json:"k"`
	Value    string `json:"v,omitempty"`
	Expected string `json:"e,omitempty"` // CAS only
	ClientID string `json:"c,omitempty"`
	Seq      uint64 `json:"s,omitempty"`
}

type Response struct {
	Value   string `json:"v,omitempty"`
	Found   bool   `json:"f,omitempty"`
	Swapped bool   `json:"w,omitempty"`
}

var ErrBadRequest = errors.New("kv: bad request")

func (r Request) validate() error {
	switch r.Op {
	case OpGet, OpPut, OpDelete, OpAppend, OpCAS:
	default:
		return fmt.Errorf("%w: unknown op %q", ErrBadRequest, r.Op)
	}
	if r.Key == "" {
		return fmt.Errorf("%w: empty key", ErrBadRequest)
	}
	return nil
}

// session is what the state machine remembers about a client: the last
// write it applied for it and the answer it gave.
type session struct {
	Seq  uint64   `json:"seq"`
	Resp Response `json:"resp"`
}

// machine is the deterministic part of the store. It knows nothing about
// raft; the same sequence of Apply calls always gives the same state.
type machine struct {
	data     map[string]string
	sessions map[string]session
}

func newMachine() *machine {
	return &machine{data: map[string]string{}, sessions: map[string]session{}}
}

func (m *machine) apply(r Request) Response {
	// Reads change nothing, so running one twice is harmless and they
	// skip the session table entirely.
	if r.Op == OpGet {
		v, ok := m.data[r.Key]
		return Response{Value: v, Found: ok}
	}
	if r.ClientID != "" {
		if s, ok := m.sessions[r.ClientID]; ok && r.Seq <= s.Seq {
			// A retry of the last write gets the original answer. Anything
			// older is a stray duplicate the client stopped waiting for.
			if r.Seq == s.Seq {
				return s.Resp
			}
			return Response{}
		}
	}

	var resp Response
	switch r.Op {
	case OpPut:
		m.data[r.Key] = r.Value
	case OpDelete:
		_, resp.Found = m.data[r.Key]
		delete(m.data, r.Key)
	case OpAppend:
		m.data[r.Key] += r.Value
	case OpCAS:
		// a missing key compares equal to "", so CAS with an empty
		// expected value doubles as create-if-absent
		if m.data[r.Key] == r.Expected {
			m.data[r.Key] = r.Value
			resp.Swapped = true
		}
	}
	if r.ClientID != "" {
		m.sessions[r.ClientID] = session{Seq: r.Seq, Resp: resp}
	}
	return resp
}

type snapshotImage struct {
	Data     map[string]string  `json:"data"`
	Sessions map[string]session `json:"sessions"`
}

func (m *machine) snapshot() ([]byte, error) {
	return json.Marshal(snapshotImage{Data: m.data, Sessions: m.sessions})
}

func (m *machine) restore(b []byte) error {
	var img snapshotImage
	if err := json.Unmarshal(b, &img); err != nil {
		return err
	}
	if img.Data == nil {
		img.Data = map[string]string{}
	}
	if img.Sessions == nil {
		img.Sessions = map[string]session{}
	}
	m.data, m.sessions = img.Data, img.Sessions
	return nil
}

func encodeRequest(r Request) ([]byte, error) { return json.Marshal(r) }

func decodeRequest(b []byte) (Request, error) {
	var r Request
	err := json.Unmarshal(b, &r)
	return r, err
}
