package raft

import (
	"context"
	"time"
)

// replicator owns the stream of AppendEntries/InstallSnapshot RPCs to one
// peer. Only one RPC per peer is in flight at a time, which keeps
// nextIndex bookkeeping simple. It wakes on a heartbeat tick or when
// something new needs to go out (Propose, commit advance, new leader).
func (n *Node) replicator(peer string) {
	defer n.wg.Done()
	hb := time.NewTimer(n.cfg.HeartbeatInterval)
	defer hb.Stop()
	for {
		select {
		case <-n.stopCh:
			return
		case <-n.triggers[peer]:
		case <-hb.C:
		}
		more := n.replicateOnce(peer)
		hb.Reset(n.cfg.HeartbeatInterval)
		if more {
			n.trigger(peer)
		}
	}
}

// replicateOnce sends one RPC to peer if we are leader and reports whether
// there is more to send straight away.
func (n *Node) replicateOnce(peer string) bool {
	n.mu.Lock()
	if n.dead || n.role != Leader {
		n.mu.Unlock()
		return false
	}

	next := n.nextIndex[peer]
	if next <= n.log.snapIndex() {
		// The entries this peer needs have been compacted away.
		args := &InstallSnapshotArgs{
			Term:              n.term,
			LeaderID:          n.id,
			LastIncludedIndex: n.log.snapIndex(),
			LastIncludedTerm:  n.log.snapTerm(),
			Data:              n.snapData, // never modified in place, safe to share
		}
		n.mu.Unlock()

		ctx, cancel := context.WithTimeout(n.ctx, n.rpcTimeout())
		reply, err := n.cfg.Transport.InstallSnapshot(ctx, peer, args)
		cancel()
		if err != nil {
			return false
		}
		return n.handleSnapshotReply(peer, args, reply)
	}

	prev := next - 1
	hi := min(n.log.lastIndex(), prev+uint64(n.cfg.MaxEntriesPerAppend))
	args := &AppendEntriesArgs{
		Term:         n.term,
		LeaderID:     n.id,
		PrevLogIndex: prev,
		PrevLogTerm:  n.log.term(prev),
		LeaderCommit: n.commitIndex,
	}
	if hi > prev {
		args.Entries = n.log.slice(next, hi+1)
	}
	n.mu.Unlock()

	ctx, cancel := context.WithTimeout(n.ctx, n.rpcTimeout())
	reply, err := n.cfg.Transport.AppendEntries(ctx, peer, args)
	cancel()
	if err != nil {
		return false
	}
	return n.handleAppendReply(peer, args, reply)
}

func (n *Node) handleAppendReply(peer string, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.dead {
		return false
	}
	if reply.Term > n.term {
		n.becomeFollower(reply.Term)
		n.resetElectionTimer()
		return false
	}
	// The world may have moved on while the RPC was out.
	if n.term != args.Term || n.role != Leader {
		return false
	}

	if reply.Success {
		match := args.PrevLogIndex + uint64(len(args.Entries))
		if match > n.matchIndex[peer] {
			n.matchIndex[peer] = match
		}
		if n.matchIndex[peer]+1 > n.nextIndex[peer] {
			n.nextIndex[peer] = n.matchIndex[peer] + 1
		}
		n.advanceCommit()
		return n.nextIndex[peer] <= n.log.lastIndex()
	}

	// Fast backoff: if we have entries from the follower's conflicting
	// term, resume just after our last one; otherwise skip the follower's
	// whole term.
	next := reply.ConflictIndex
	if reply.ConflictTerm != 0 {
		if i := n.log.lastIndexOfTerm(reply.ConflictTerm); i != 0 {
			next = i + 1
		}
	}
	next = max(next, n.matchIndex[peer]+1, 1)
	next = min(next, n.log.lastIndex()+1)
	n.nextIndex[peer] = next
	return true
}

// advanceCommit moves commitIndex to the highest index stored on a
// majority, but only counts replicas for entries from the current term
// (§5.4.2); older entries then commit indirectly.
func (n *Node) advanceCommit() {
	if n.role != Leader {
		return
	}
	for idx := n.log.lastIndex(); idx > n.commitIndex; idx-- {
		if n.log.term(idx) != n.term {
			break // terms only go down from here
		}
		count := 1
		for _, p := range n.peers {
			if n.matchIndex[p] >= idx {
				count++
			}
		}
		if count >= n.quorum() {
			n.commitIndex = idx
			n.applyCond.Broadcast()
			// let followers learn the new commit index without waiting
			// for the next heartbeat
			n.triggerAll()
			return
		}
	}
}

func (n *Node) HandleAppendEntries(args *AppendEntriesArgs) *AppendEntriesReply {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.dead || args.Term < n.term {
		return &AppendEntriesReply{Term: n.term}
	}
	// A valid leader exists for this term; candidates in the same term give
	// way to it too.
	n.becomeFollower(args.Term)
	n.leader = args.LeaderID
	n.resetElectionTimer()

	prevIndex, prevTerm, ents := args.PrevLogIndex, args.PrevLogTerm, args.Entries
	if prevIndex < n.log.snapIndex() {
		// Anything up to the snapshot is committed and therefore matches;
		// skip the part of the request we have already compacted.
		skip := n.log.snapIndex() - prevIndex
		if skip >= uint64(len(ents)) {
			return &AppendEntriesReply{Term: n.term, Success: true}
		}
		ents = ents[skip:]
		prevIndex, prevTerm = n.log.snapIndex(), n.log.snapTerm()
	}

	if prevIndex > n.log.lastIndex() {
		return &AppendEntriesReply{Term: n.term, ConflictIndex: n.log.lastIndex() + 1}
	}
	if t := n.log.term(prevIndex); t != prevTerm {
		return &AppendEntriesReply{
			Term:          n.term,
			ConflictTerm:  t,
			ConflictIndex: n.log.firstIndexOfTerm(prevIndex),
		}
	}

	// Only truncate on an actual conflict. A delayed, shorter
	// AppendEntries must not chop off entries a newer one already added.
	for i, e := range ents {
		if e.Index > n.log.lastIndex() {
			n.appendLocal(cloneEntries(ents[i:])...)
			break
		}
		if n.log.term(e.Index) != e.Term {
			n.must(n.cfg.Storage.TruncateSuffix(e.Index))
			n.log.truncateFrom(e.Index)
			n.appendLocal(cloneEntries(ents[i:])...)
			break
		}
	}

	if args.LeaderCommit > n.commitIndex {
		lastNew := prevIndex + uint64(len(ents))
		c := min(args.LeaderCommit, lastNew)
		if c > n.commitIndex {
			n.commitIndex = c
			n.applyCond.Broadcast()
		}
	}
	return &AppendEntriesReply{Term: n.term, Success: true}
}
