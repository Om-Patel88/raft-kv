package raft

// raftLog is the in-memory copy of the log. ents[0] is a placeholder that
// stands for the last entry covered by the snapshot (Index 0, Term 0 when
// there is none), so index arithmetic never has to special-case an empty
// log.
type raftLog struct {
	ents []Entry
}

func newRaftLog(snapIndex, snapTerm uint64, ents []Entry) raftLog {
	l := raftLog{ents: make([]Entry, 0, len(ents)+1)}
	l.ents = append(l.ents, Entry{Index: snapIndex, Term: snapTerm})
	l.ents = append(l.ents, ents...)
	return l
}

func (l *raftLog) snapIndex() uint64 { return l.ents[0].Index }
func (l *raftLog) snapTerm() uint64  { return l.ents[0].Term }
func (l *raftLog) lastIndex() uint64 { return l.ents[len(l.ents)-1].Index }
func (l *raftLog) lastTerm() uint64  { return l.ents[len(l.ents)-1].Term }

// term returns the term of the entry at i, which must lie in
// [snapIndex, lastIndex].
func (l *raftLog) term(i uint64) uint64 {
	return l.ents[i-l.snapIndex()].Term
}

func (l *raftLog) has(i uint64) bool {
	return i >= l.snapIndex() && i <= l.lastIndex()
}

func (l *raftLog) entry(i uint64) Entry {
	return l.ents[i-l.snapIndex()]
}

// slice returns a deep copy of entries in [lo, hi).
func (l *raftLog) slice(lo, hi uint64) []Entry {
	off := l.snapIndex()
	return cloneEntries(l.ents[lo-off : hi-off])
}

func (l *raftLog) append(es ...Entry) {
	l.ents = append(l.ents, es...)
}

// truncateFrom drops entries with index >= i.
func (l *raftLog) truncateFrom(i uint64) {
	l.ents = l.ents[:i-l.snapIndex()]
}

// compact discards everything up to and including index, keeping later
// entries only if the log agrees with the snapshot at index.
func (l *raftLog) compact(index, term uint64) {
	var rest []Entry
	if l.has(index) && index < l.lastIndex() && l.term(index) == term {
		rest = l.ents[index-l.snapIndex()+1:]
	}
	nl := make([]Entry, 0, len(rest)+1)
	nl = append(nl, Entry{Index: index, Term: term})
	nl = append(nl, rest...)
	l.ents = nl
}

// firstIndexOfTerm walks back from i to find the first entry of that
// entry's term, stopping at the snapshot boundary.
func (l *raftLog) firstIndexOfTerm(i uint64) uint64 {
	t := l.term(i)
	for i > l.snapIndex()+1 && l.term(i-1) == t {
		i--
	}
	return i
}

// lastIndexOfTerm returns the last index holding term t, or 0 if the log
// (past the snapshot) has no entry from that term.
func (l *raftLog) lastIndexOfTerm(t uint64) uint64 {
	for i := l.lastIndex(); i > l.snapIndex(); i-- {
		lt := l.term(i)
		if lt == t {
			return i
		}
		if lt < t {
			break
		}
	}
	return 0
}
