// Package simnet is an in-memory network for raft nodes, with knobs for
// dropping, delaying and partitioning traffic. It is used by the tests and
// by the browser demo.
package simnet

import (
	"context"
	"errors"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/Om-Patel88/raft-kv/raft"
)

var (
	ErrDropped     = errors.New("simnet: message dropped")
	ErrUnreachable = errors.New("simnet: destination unreachable")
)

// Handler receives RPCs. *raft.Node satisfies it.
type Handler interface {
	HandleRequestVote(*raft.RequestVoteArgs) *raft.RequestVoteReply
	HandleAppendEntries(*raft.AppendEntriesArgs) *raft.AppendEntriesReply
	HandleInstallSnapshot(*raft.InstallSnapshotArgs) *raft.InstallSnapshotReply
}

type Options struct {
	Seed     uint64 // 0 picks a seed from the clock
	MinDelay time.Duration
	MaxDelay time.Duration
	DropRate float64 // probability that any single message is lost
}

// Event describes one message on the wire. Kind is the RPC name for
// requests and the RPC name plus "Reply" for replies.
type Event struct {
	From    string
	To      string
	Kind    string
	Term    uint64
	Entries int  // AppendEntries only
	Success bool // replies only: vote granted / append succeeded
	Dropped bool
	Delay   time.Duration
}

type Network struct {
	mu           sync.Mutex
	rng          *rand.Rand
	handlers     map[string]Handler
	disconnected map[string]bool
	group        map[string]int // nil when there is no partition
	dropRate     float64
	minDelay     time.Duration
	maxDelay     time.Duration
	observer     func(Event)
}

func New(opts Options) *Network {
	seed := opts.Seed
	if seed == 0 {
		seed = uint64(time.Now().UnixNano())
	}
	n := &Network{
		rng:          rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15)),
		handlers:     make(map[string]Handler),
		disconnected: make(map[string]bool),
		dropRate:     opts.DropRate,
	}
	n.setDelay(opts.MinDelay, opts.MaxDelay)
	return n
}

// Join registers handler under id and returns the transport that node
// should send with. Joining an id again (after Remove, on restart)
// replaces the handler.
func (n *Network) Join(id string, h Handler) raft.Transport {
	n.mu.Lock()
	n.handlers[id] = h
	n.mu.Unlock()
	return &transport{net: n, from: id}
}

// Transport returns the sending side for id without registering a
// handler. It lets a caller build the transport before the node that will
// handle incoming messages exists, then call Join once it does.
func (n *Network) Transport(id string) raft.Transport {
	return &transport{net: n, from: id}
}

// Remove takes id off the network, as if the machine crashed. Messages to
// it fail until it joins again.
func (n *Network) Remove(id string) {
	n.mu.Lock()
	delete(n.handlers, id)
	n.mu.Unlock()
}

// Disconnect cuts id off from everyone, in both directions.
func (n *Network) Disconnect(id string) {
	n.mu.Lock()
	n.disconnected[id] = true
	n.mu.Unlock()
}

func (n *Network) Connect(id string) {
	n.mu.Lock()
	delete(n.disconnected, id)
	n.mu.Unlock()
}

// Partition splits the network: nodes can only talk within their own
// group. Nodes not listed in any group are isolated.
func (n *Network) Partition(groups ...[]string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.group = make(map[string]int)
	for g, ids := range groups {
		for _, id := range ids {
			n.group[id] = g
		}
	}
}

// Heal removes any partition and reconnects every disconnected node.
func (n *Network) Heal() {
	n.mu.Lock()
	n.group = nil
	n.disconnected = make(map[string]bool)
	n.mu.Unlock()
}

func (n *Network) SetDropRate(p float64) {
	n.mu.Lock()
	n.dropRate = p
	n.mu.Unlock()
}

func (n *Network) SetDelay(min, max time.Duration) {
	n.mu.Lock()
	n.setDelay(min, max)
	n.mu.Unlock()
}

func (n *Network) setDelay(lo, hi time.Duration) {
	lo = max(lo, 0)
	hi = max(hi, lo)
	n.minDelay, n.maxDelay = lo, hi
}

// OnMessage installs fn as an observer of every message, including the
// dropped ones. fn is called synchronously on the sending path, so it must
// be fast and must not block or call back into the network. Pass nil to
// remove it.
func (n *Network) OnMessage(fn func(Event)) {
	n.mu.Lock()
	n.observer = fn
	n.mu.Unlock()
}

// Reachable reports whether a message from one node to another would
// currently get through (ignoring random drops).
func (n *Network) Reachable(from, to string) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.reachable(from, to)
}

func (n *Network) reachable(from, to string) bool {
	if n.handlers[from] == nil || n.handlers[to] == nil {
		return false
	}
	if n.disconnected[from] || n.disconnected[to] {
		return false
	}
	if n.group != nil {
		gf, okf := n.group[from]
		gt, okt := n.group[to]
		if !okf || !okt || gf != gt {
			return false
		}
	}
	return true
}

// hop decides the fate of one message: whether it is lost and how long it
// takes. It also returns the handler at the far end, captured now so a
// node that restarts while the message is in flight does not receive it.
func (n *Network) hop(from, to string) (h Handler, dropped bool, delay time.Duration, obs func(Event)) {
	n.mu.Lock()
	defer n.mu.Unlock()
	h = n.handlers[to]
	dropped = !n.reachable(from, to) || (n.dropRate > 0 && n.rng.Float64() < n.dropRate)
	delay = n.minDelay
	if n.maxDelay > n.minDelay {
		delay += time.Duration(n.rng.Int64N(int64(n.maxDelay - n.minDelay)))
	}
	return h, dropped, delay, n.observer
}

// stillThere checks, at delivery time, that the path is still open and the
// destination is the same incarnation the message was addressed to.
func (n *Network) stillThere(from, to string, h Handler) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.reachable(from, to) && n.handlers[to] == h
}

func wait(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// rpc carries one request/response exchange across the network. Requests
// and replies are dropped and delayed independently.
func rpc[A, R any](ctx context.Context, n *Network, from, to string, args *A,
	describe func(*A, *R) Event, handle func(Handler, *A) *R) (*R, error) {

	if err := ctx.Err(); err != nil {
		return nil, err
	}

	h, dropped, delay, obs := n.hop(from, to)
	if obs != nil {
		ev := describe(args, nil)
		ev.From, ev.To, ev.Dropped, ev.Delay = from, to, dropped || h == nil, delay
		obs(ev)
	}
	if err := wait(ctx, delay); err != nil {
		return nil, err
	}
	if dropped || h == nil {
		return nil, ErrDropped
	}
	if !n.stillThere(from, to, h) {
		return nil, ErrUnreachable
	}

	reply := handle(h, args)

	back, dropped, delay, obs := n.hop(to, from)
	if obs != nil {
		ev := describe(nil, reply)
		ev.From, ev.To, ev.Dropped, ev.Delay = to, from, dropped || back == nil, delay
		obs(ev)
	}
	if err := wait(ctx, delay); err != nil {
		return nil, err
	}
	if dropped || back == nil {
		return nil, ErrDropped
	}
	return reply, nil
}

type transport struct {
	net  *Network
	from string
}

func (t *transport) RequestVote(ctx context.Context, to string, args *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
	return rpc(ctx, t.net, t.from, to, args,
		func(a *raft.RequestVoteArgs, r *raft.RequestVoteReply) Event {
			if a != nil {
				return Event{Kind: "RequestVote", Term: a.Term}
			}
			return Event{Kind: "RequestVoteReply", Term: r.Term, Success: r.VoteGranted}
		},
		func(h Handler, a *raft.RequestVoteArgs) *raft.RequestVoteReply {
			cp := *a
			r := *h.HandleRequestVote(&cp)
			return &r
		})
}

func (t *transport) AppendEntries(ctx context.Context, to string, args *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
	return rpc(ctx, t.net, t.from, to, args,
		func(a *raft.AppendEntriesArgs, r *raft.AppendEntriesReply) Event {
			if a != nil {
				return Event{Kind: "AppendEntries", Term: a.Term, Entries: len(a.Entries)}
			}
			return Event{Kind: "AppendEntriesReply", Term: r.Term, Success: r.Success}
		},
		func(h Handler, a *raft.AppendEntriesArgs) *raft.AppendEntriesReply {
			cp := *a
			cp.Entries = copyEntries(a.Entries)
			r := *h.HandleAppendEntries(&cp)
			return &r
		})
}

func (t *transport) InstallSnapshot(ctx context.Context, to string, args *raft.InstallSnapshotArgs) (*raft.InstallSnapshotReply, error) {
	return rpc(ctx, t.net, t.from, to, args,
		func(a *raft.InstallSnapshotArgs, r *raft.InstallSnapshotReply) Event {
			if a != nil {
				return Event{Kind: "InstallSnapshot", Term: a.Term}
			}
			return Event{Kind: "InstallSnapshotReply", Term: r.Term, Success: true}
		},
		func(h Handler, a *raft.InstallSnapshotArgs) *raft.InstallSnapshotReply {
			cp := *a
			cp.Data = append([]byte(nil), a.Data...)
			r := *h.HandleInstallSnapshot(&cp)
			return &r
		})
}

// copyEntries deep-copies so sender and receiver never share a backing
// array, the same as if the entries had been serialised.
func copyEntries(es []raft.Entry) []raft.Entry {
	if es == nil {
		return nil
	}
	out := make([]raft.Entry, len(es))
	for i, e := range es {
		out[i] = e
		if e.Data != nil {
			out[i].Data = append([]byte(nil), e.Data...)
		}
	}
	return out
}
