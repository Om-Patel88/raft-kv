package raft

// Snapshot tells the node that the service's state machine has captured
// everything through index, so the log up to there can be thrown away.
// It is normally called from the goroutine reading ApplyCh.
func (n *Node) Snapshot(index uint64, data []byte) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.dead || index <= n.log.snapIndex() || index > n.commitIndex {
		return
	}
	snap := Snapshot{Index: index, Term: n.log.term(index), Data: cloneBytes(data)}
	n.must(n.cfg.Storage.SaveSnapshot(snap))
	n.log.compact(snap.Index, snap.Term)
	n.snapData = snap.Data
	n.logf("compacted log through %d", index)
}

func (n *Node) HandleInstallSnapshot(args *InstallSnapshotArgs) *InstallSnapshotReply {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.dead || args.Term < n.term {
		return &InstallSnapshotReply{Term: n.term}
	}
	n.becomeFollower(args.Term)
	n.leader = args.LeaderID
	n.resetElectionTimer()

	// Already have (and have committed) everything this covers.
	if args.LastIncludedIndex <= n.commitIndex {
		return &InstallSnapshotReply{Term: n.term}
	}

	snap := Snapshot{Index: args.LastIncludedIndex, Term: args.LastIncludedTerm, Data: cloneBytes(args.Data)}
	n.must(n.cfg.Storage.SaveSnapshot(snap))
	n.log.compact(snap.Index, snap.Term)
	n.snapData = snap.Data
	n.commitIndex = snap.Index
	n.snapPending = true
	n.applyCond.Broadcast()
	n.logf("installed snapshot through %d from %s", snap.Index, args.LeaderID)
	return &InstallSnapshotReply{Term: n.term}
}

func (n *Node) handleSnapshotReply(peer string, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
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
	if n.term != args.Term || n.role != Leader {
		return false
	}
	if args.LastIncludedIndex > n.matchIndex[peer] {
		n.matchIndex[peer] = args.LastIncludedIndex
	}
	if n.matchIndex[peer]+1 > n.nextIndex[peer] {
		n.nextIndex[peer] = n.matchIndex[peer] + 1
	}
	n.advanceCommit()
	return n.nextIndex[peer] <= n.log.lastIndex()
}
