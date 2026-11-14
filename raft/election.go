package raft

import "context"

// startElection is called by the ticker with mu held once the election
// timeout passes without hearing from a leader (§5.2).
func (n *Node) startElection() {
	n.term++
	n.role = Candidate
	n.votedFor = n.id
	n.leader = ""
	n.persistHardState()
	n.resetElectionTimer()
	n.logf("starting election")

	if n.quorum() == 1 {
		n.becomeLeader()
		return
	}

	args := &RequestVoteArgs{
		Term:         n.term,
		CandidateID:  n.id,
		LastLogIndex: n.log.lastIndex(),
		LastLogTerm:  n.log.lastTerm(),
	}
	votes := 1
	for _, p := range n.peers {
		n.wg.Add(1)
		go n.requestVote(p, args, &votes)
	}
}

// votes is shared between the goroutines of one election and only touched
// under mu.
func (n *Node) requestVote(peer string, args *RequestVoteArgs, votes *int) {
	defer n.wg.Done()
	ctx, cancel := context.WithTimeout(n.ctx, n.rpcTimeout())
	reply, err := n.cfg.Transport.RequestVote(ctx, peer, args)
	cancel()
	if err != nil {
		return
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	if n.dead {
		return
	}
	if reply.Term > n.term {
		n.becomeFollower(reply.Term)
		n.resetElectionTimer()
		return
	}
	// stale reply from an earlier election, or we already won/lost
	if n.term != args.Term || n.role != Candidate {
		return
	}
	if reply.VoteGranted {
		*votes++
		if *votes >= n.quorum() {
			n.becomeLeader()
		}
	}
}

func (n *Node) becomeLeader() {
	n.role = Leader
	n.leader = n.id
	n.nextIndex = make(map[string]uint64, len(n.peers))
	n.matchIndex = make(map[string]uint64, len(n.peers))
	for _, p := range n.peers {
		n.nextIndex[p] = n.log.lastIndex() + 1
		n.matchIndex[p] = 0
	}
	n.logf("became leader, last=%d", n.log.lastIndex())

	// A no-op from the new term lets entries left over from earlier terms
	// commit without waiting for a client request (§5.4.2, §8).
	n.appendLocal(Entry{Index: n.log.lastIndex() + 1, Term: n.term, Type: EntryNoop})
	n.advanceCommit()
	n.triggerAll()
}

func (n *Node) HandleRequestVote(args *RequestVoteArgs) *RequestVoteReply {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.dead {
		return &RequestVoteReply{Term: n.term}
	}
	if args.Term < n.term {
		return &RequestVoteReply{Term: n.term}
	}
	if args.Term > n.term {
		n.becomeFollower(args.Term)
	}

	// §5.4.1: only vote for a candidate whose log is at least as up to date
	upToDate := args.LastLogTerm > n.log.lastTerm() ||
		(args.LastLogTerm == n.log.lastTerm() && args.LastLogIndex >= n.log.lastIndex())

	granted := false
	if (n.votedFor == "" || n.votedFor == args.CandidateID) && upToDate {
		granted = true
		if n.votedFor != args.CandidateID {
			n.votedFor = args.CandidateID
			n.persistHardState()
		}
		n.resetElectionTimer()
	}
	return &RequestVoteReply{Term: n.term, VoteGranted: granted}
}
