package raft

import (
	"context"
	"fmt"
)

type EntryType uint8

const (
	EntryCommand EntryType = iota
	EntryNoop
)

func (t EntryType) String() string {
	switch t {
	case EntryCommand:
		return "command"
	case EntryNoop:
		return "noop"
	}
	return fmt.Sprintf("EntryType(%d)", uint8(t))
}

type Entry struct {
	Index uint64    `json:"index"`
	Term  uint64    `json:"term"`
	Type  EntryType `json:"type"`
	Data  []byte    `json:"data,omitempty"`
}

type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

func (r Role) String() string {
	switch r {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	}
	return fmt.Sprintf("Role(%d)", int(r))
}

func (r Role) MarshalText() ([]byte, error) { return []byte(r.String()), nil }

func (r *Role) UnmarshalText(b []byte) error {
	switch string(b) {
	case "Follower":
		*r = Follower
	case "Candidate":
		*r = Candidate
	case "Leader":
		*r = Leader
	default:
		return fmt.Errorf("raft: unknown role %q", b)
	}
	return nil
}

// ApplyMsg is sent on Config.ApplyCh. Exactly one of CommandValid and
// SnapshotValid is set.
type ApplyMsg struct {
	CommandValid bool
	Index        uint64
	Term         uint64
	Command      []byte

	SnapshotValid bool
	SnapshotIndex uint64
	SnapshotTerm  uint64
	Snapshot      []byte
}

type RequestVoteArgs struct {
	Term         uint64 `json:"term"`
	CandidateID  string `json:"candidate_id"`
	LastLogIndex uint64 `json:"last_log_index"`
	LastLogTerm  uint64 `json:"last_log_term"`
}

type RequestVoteReply struct {
	Term        uint64 `json:"term"`
	VoteGranted bool   `json:"vote_granted"`
}

type AppendEntriesArgs struct {
	Term         uint64  `json:"term"`
	LeaderID     string  `json:"leader_id"`
	PrevLogIndex uint64  `json:"prev_log_index"`
	PrevLogTerm  uint64  `json:"prev_log_term"`
	Entries      []Entry `json:"entries,omitempty"`
	LeaderCommit uint64  `json:"leader_commit"`
}

// AppendEntriesReply carries ConflictTerm/ConflictIndex on failure so the
// leader can skip back a whole term at a time instead of one entry per RPC.
// ConflictTerm is 0 when the follower's log is simply too short.
type AppendEntriesReply struct {
	Term          uint64 `json:"term"`
	Success       bool   `json:"success"`
	ConflictTerm  uint64 `json:"conflict_term,omitempty"`
	ConflictIndex uint64 `json:"conflict_index,omitempty"`
}

type InstallSnapshotArgs struct {
	Term              uint64 `json:"term"`
	LeaderID          string `json:"leader_id"`
	LastIncludedIndex uint64 `json:"last_included_index"`
	LastIncludedTerm  uint64 `json:"last_included_term"`
	Data              []byte `json:"data"`
}

type InstallSnapshotReply struct {
	Term uint64 `json:"term"`
}

type Transport interface {
	RequestVote(ctx context.Context, to string, args *RequestVoteArgs) (*RequestVoteReply, error)
	AppendEntries(ctx context.Context, to string, args *AppendEntriesArgs) (*AppendEntriesReply, error)
	InstallSnapshot(ctx context.Context, to string, args *InstallSnapshotArgs) (*InstallSnapshotReply, error)
}
