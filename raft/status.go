package raft

import "unicode/utf8"

type PeerProgress struct {
	NextIndex  uint64 `json:"next_index"`
	MatchIndex uint64 `json:"match_index"`
}

type EntrySummary struct {
	Index   uint64    `json:"index"`
	Term    uint64    `json:"term"`
	Type    EntryType `json:"type"`
	Preview string    `json:"preview,omitempty"`
}

type Status struct {
	ID            string                  `json:"id"`
	Role          Role                    `json:"role"`
	Term          uint64                  `json:"term"`
	VotedFor      string                  `json:"voted_for,omitempty"`
	Leader        string                  `json:"leader,omitempty"`
	CommitIndex   uint64                  `json:"commit_index"`
	LastApplied   uint64                  `json:"last_applied"`
	FirstIndex    uint64                  `json:"first_index"`
	LastIndex     uint64                  `json:"last_index"`
	SnapshotIndex uint64                  `json:"snapshot_index"`
	Peers         map[string]PeerProgress `json:"peers,omitempty"`
	RecentEntries []EntrySummary          `json:"recent_entries,omitempty"`
}

// Status returns a snapshot of the node's state, taken under the lock so
// all fields are consistent with each other.
func (n *Node) Status() Status {
	n.mu.Lock()
	defer n.mu.Unlock()
	s := Status{
		ID:            n.id,
		Role:          n.role,
		Term:          n.term,
		VotedFor:      n.votedFor,
		Leader:        n.leader,
		CommitIndex:   n.commitIndex,
		LastApplied:   n.lastApplied,
		FirstIndex:    n.log.snapIndex() + 1,
		LastIndex:     n.log.lastIndex(),
		SnapshotIndex: n.log.snapIndex(),
	}
	if n.role == Leader {
		s.Peers = make(map[string]PeerProgress, len(n.peers))
		for _, p := range n.peers {
			s.Peers[p] = PeerProgress{NextIndex: n.nextIndex[p], MatchIndex: n.matchIndex[p]}
		}
	}
	lo := max(n.log.snapIndex()+1, n.log.lastIndex()+1-min(n.log.lastIndex(), defaultRecentEntries))
	for i := lo; i <= n.log.lastIndex(); i++ {
		e := n.log.entry(i)
		s.RecentEntries = append(s.RecentEntries, EntrySummary{
			Index: e.Index, Term: e.Term, Type: e.Type, Preview: preview(e.Data),
		})
	}
	return s
}

func preview(b []byte) string {
	const maxLen = 24
	cut := len(b) > maxLen
	if cut {
		b = b[:maxLen]
	}
	out := make([]rune, 0, len(b))
	for len(b) > 0 {
		r, size := utf8.DecodeRune(b)
		b = b[size:]
		if r == utf8.RuneError || r < 0x20 || r == 0x7f {
			r = '.'
		}
		out = append(out, r)
	}
	if cut {
		return string(out) + "..."
	}
	return string(out)
}
