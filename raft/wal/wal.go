// Package wal is a file-backed raft.Storage.
//
// A data directory holds three files:
//
//	log       append-only entry records, each framed as
//	          [len uint32][crc32 uint32][index][term][type][data]
//	state     current term and vote, replaced atomically
//	snapshot  latest snapshot, replaced atomically
//
// A crash can leave a half-written record at the end of the log. Open
// detects that and truncates it, since the write was never acknowledged.
// A bad checksum anywhere before the tail is reported as corruption.
package wal

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/Om-Patel88/raft-kv/raft"
)

const (
	logName      = "log"
	stateName    = "state"
	snapshotName = "snapshot"

	headerSize = 8
	// entry payload without data: index, term, type
	entryFixed = 8 + 8 + 1
	maxRecord  = 64 << 20
)

var ErrCorrupt = errors.New("wal: corrupt record")

var crcTable = crc32.MakeTable(crc32.Castagnoli)

type Options struct {
	// NoSync skips fsync. Only for tests and demos where durability
	// across a machine crash does not matter.
	NoSync bool
}

type recMeta struct {
	index uint64
	term  uint64
	off   int64
}

type WAL struct {
	mu   sync.Mutex
	dir  string
	opts Options

	f    *os.File
	size int64
	recs []recMeta // one per entry in the log file, in order

	snapIndex uint64
	snapTerm  uint64
}

var _ raft.Storage = (*WAL)(nil)

// Open opens (or creates) the WAL in dir and repairs a torn tail if there
// is one.
func Open(dir string, opts Options) (*WAL, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	for _, name := range []string{logName, stateName, snapshotName} {
		os.Remove(filepath.Join(dir, name+".tmp"))
	}
	w := &WAL{dir: dir, opts: opts}

	snap, err := w.readSnapshot()
	if err != nil {
		return nil, err
	}
	w.snapIndex, w.snapTerm = snap.Index, snap.Term

	f, err := os.OpenFile(w.path(logName), os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	w.f = f
	buf, err := io.ReadAll(f)
	if err != nil {
		f.Close()
		return nil, err
	}
	_, recs, validLen, err := scan(buf)
	if err != nil {
		f.Close()
		return nil, err
	}
	if validLen < int64(len(buf)) {
		// torn final record: it was never acknowledged, so drop it
		if err := f.Truncate(validLen); err != nil {
			f.Close()
			return nil, err
		}
		if err := w.sync(f); err != nil {
			f.Close()
			return nil, err
		}
	}
	w.recs, w.size = recs, validLen

	// A crash between writing a snapshot and rewriting the log leaves
	// entries the snapshot already covers; finish the job now.
	if len(w.recs) > 0 && w.recs[0].index <= w.snapIndex {
		if err := w.compactLog(w.snapIndex, w.snapTerm); err != nil {
			w.f.Close()
			return nil, err
		}
	}
	if len(w.recs) > 0 && w.recs[0].index != w.snapIndex+1 {
		w.f.Close()
		return nil, fmt.Errorf("%w: log starts at %d, snapshot ends at %d", ErrCorrupt, w.recs[0].index, w.snapIndex)
	}
	return w, nil
}

func (w *WAL) path(name string) string { return filepath.Join(w.dir, name) }

func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.f == nil {
		return nil
	}
	err := w.f.Close()
	w.f = nil
	return err
}

func (w *WAL) Load() (raft.PersistentState, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	var st raft.PersistentState

	hs, err := w.readState()
	if err != nil {
		return st, err
	}
	snap, err := w.readSnapshot()
	if err != nil {
		return st, err
	}
	buf := make([]byte, w.size)
	if _, err := w.f.ReadAt(buf, 0); err != nil && err != io.EOF {
		return st, err
	}
	ents, _, validLen, err := scan(buf)
	if err != nil {
		return st, err
	}
	if validLen != w.size {
		return st, fmt.Errorf("%w: log changed underneath us", ErrCorrupt)
	}
	st.HardState, st.Snapshot, st.Entries = hs, snap, ents
	return st, nil
}

func (w *WAL) lastIndex() uint64 {
	if len(w.recs) == 0 {
		return w.snapIndex
	}
	return w.recs[len(w.recs)-1].index
}

func (w *WAL) Append(entries []raft.Entry) error {
	if len(entries) == 0 {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if entries[0].Index != w.lastIndex()+1 {
		return fmt.Errorf("%w: have last %d, got %d", raft.ErrNonContiguous, w.lastIndex(), entries[0].Index)
	}
	var buf bytes.Buffer
	metas := make([]recMeta, 0, len(entries))
	for i, e := range entries {
		if i > 0 && e.Index != entries[i-1].Index+1 {
			return raft.ErrNonContiguous
		}
		metas = append(metas, recMeta{index: e.Index, term: e.Term, off: w.size + int64(buf.Len())})
		appendRecord(&buf, encodeEntry(e))
	}
	if _, err := w.f.WriteAt(buf.Bytes(), w.size); err != nil {
		return err
	}
	if err := w.sync(w.f); err != nil {
		return err
	}
	w.size += int64(buf.Len())
	w.recs = append(w.recs, metas...)
	return nil
}

func (w *WAL) TruncateSuffix(from uint64) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if from <= w.snapIndex {
		return fmt.Errorf("wal: truncate from %d would cut into snapshot at %d", from, w.snapIndex)
	}
	if from > w.lastIndex() {
		return nil
	}
	i := from - w.recs[0].index
	off := w.recs[i].off
	if err := w.f.Truncate(off); err != nil {
		return err
	}
	if err := w.sync(w.f); err != nil {
		return err
	}
	w.size = off
	w.recs = w.recs[:i]
	return nil
}

func (w *WAL) SaveHardState(hs raft.HardState) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	p := make([]byte, 8, 8+len(hs.VotedFor))
	binary.LittleEndian.PutUint64(p, hs.Term)
	p = append(p, hs.VotedFor...)
	var buf bytes.Buffer
	appendRecord(&buf, p)
	return w.writeAtomic(stateName, buf.Bytes())
}

func (w *WAL) SaveSnapshot(snap raft.Snapshot) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if snap.Index <= w.snapIndex {
		return nil
	}
	p := make([]byte, 16, 16+len(snap.Data))
	binary.LittleEndian.PutUint64(p[0:], snap.Index)
	binary.LittleEndian.PutUint64(p[8:], snap.Term)
	p = append(p, snap.Data...)
	var buf bytes.Buffer
	appendRecord(&buf, p)
	// Snapshot first, then the log: if we crash in between, Open sees
	// stale entries below the snapshot and drops them.
	if err := w.writeAtomic(snapshotName, buf.Bytes()); err != nil {
		return err
	}
	w.snapIndex, w.snapTerm = snap.Index, snap.Term
	return w.compactLog(snap.Index, snap.Term)
}

// compactLog rewrites the log file without the entries covered by a
// snapshot at (index, term). Later entries survive only if the log agrees
// with the snapshot at index.
func (w *WAL) compactLog(index, term uint64) error {
	keepFrom := len(w.recs)
	if len(w.recs) > 0 && index >= w.recs[0].index-1 && index < w.lastIndex() {
		ok := index == w.recs[0].index-1 // log starts right after; nothing to compare
		if !ok {
			ok = w.recs[index-w.recs[0].index].term == term
		}
		if ok {
			keepFrom = int(index + 1 - w.recs[0].index)
		}
	}
	if keepFrom == 0 {
		return nil
	}

	var tail []byte
	var base int64
	if keepFrom < len(w.recs) {
		base = w.recs[keepFrom].off
		tail = make([]byte, w.size-base)
		if _, err := w.f.ReadAt(tail, base); err != nil && err != io.EOF {
			return err
		}
	}

	tmp := w.path(logName + ".tmp")
	if err := w.writeFile(tmp, tail); err != nil {
		return err
	}
	// Windows will not rename over a file that is still open.
	if err := w.f.Close(); err != nil {
		return err
	}
	w.f = nil
	if err := os.Rename(tmp, w.path(logName)); err != nil {
		return err
	}
	w.syncDir()
	f, err := os.OpenFile(w.path(logName), os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	w.f = f

	recs := make([]recMeta, 0, len(w.recs)-keepFrom)
	for _, r := range w.recs[keepFrom:] {
		r.off -= base
		recs = append(recs, r)
	}
	w.recs = recs
	w.size = int64(len(tail))
	return nil
}

func (w *WAL) readState() (raft.HardState, error) {
	var hs raft.HardState
	b, err := os.ReadFile(w.path(stateName))
	if errors.Is(err, os.ErrNotExist) {
		return hs, nil
	}
	if err != nil {
		return hs, err
	}
	p, err := readSingle(b)
	if err != nil || len(p) < 8 {
		return hs, fmt.Errorf("%w: state file", ErrCorrupt)
	}
	hs.Term = binary.LittleEndian.Uint64(p)
	hs.VotedFor = string(p[8:])
	return hs, nil
}

func (w *WAL) readSnapshot() (raft.Snapshot, error) {
	var s raft.Snapshot
	b, err := os.ReadFile(w.path(snapshotName))
	if errors.Is(err, os.ErrNotExist) {
		return s, nil
	}
	if err != nil {
		return s, err
	}
	p, err := readSingle(b)
	if err != nil || len(p) < 16 {
		return s, fmt.Errorf("%w: snapshot file", ErrCorrupt)
	}
	s.Index = binary.LittleEndian.Uint64(p[0:])
	s.Term = binary.LittleEndian.Uint64(p[8:])
	s.Data = append([]byte(nil), p[16:]...)
	return s, nil
}

// writeAtomic replaces name with data so that readers see either the old
// or the new contents, never a mix: write a temp file, fsync, rename.
func (w *WAL) writeAtomic(name string, data []byte) error {
	tmp := w.path(name + ".tmp")
	if err := w.writeFile(tmp, data); err != nil {
		return err
	}
	if err := os.Rename(tmp, w.path(name)); err != nil {
		return err
	}
	w.syncDir()
	return nil
}

func (w *WAL) writeFile(path string, data []byte) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err := w.sync(f); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

func (w *WAL) sync(f *os.File) error {
	if w.opts.NoSync {
		return nil
	}
	return f.Sync()
}

// syncDir makes a rename durable on Linux. Windows cannot open a directory
// for syncing, so errors are ignored there.
func (w *WAL) syncDir() {
	if w.opts.NoSync {
		return
	}
	if d, err := os.Open(w.dir); err == nil {
		d.Sync()
		d.Close()
	}
}

func appendRecord(buf *bytes.Buffer, payload []byte) {
	var hdr [headerSize]byte
	binary.LittleEndian.PutUint32(hdr[0:], uint32(len(payload)))
	binary.LittleEndian.PutUint32(hdr[4:], crc32.Checksum(payload, crcTable))
	buf.Write(hdr[:])
	buf.Write(payload)
}

// readSingle decodes a file that holds exactly one record.
func readSingle(b []byte) ([]byte, error) {
	if len(b) < headerSize {
		return nil, ErrCorrupt
	}
	n := binary.LittleEndian.Uint32(b[0:])
	if int(n) != len(b)-headerSize {
		return nil, ErrCorrupt
	}
	p := b[headerSize:]
	if crc32.Checksum(p, crcTable) != binary.LittleEndian.Uint32(b[4:]) {
		return nil, ErrCorrupt
	}
	return p, nil
}

func encodeEntry(e raft.Entry) []byte {
	p := make([]byte, entryFixed, entryFixed+len(e.Data))
	binary.LittleEndian.PutUint64(p[0:], e.Index)
	binary.LittleEndian.PutUint64(p[8:], e.Term)
	p[16] = byte(e.Type)
	return append(p, e.Data...)
}

// scan parses log records from buf. It returns the entries, their
// metadata, and the length of the valid prefix. A short or checksum-failing
// record at the very end counts as a torn write and ends the valid prefix;
// a bad record followed by more data is corruption.
func scan(buf []byte) ([]raft.Entry, []recMeta, int64, error) {
	var ents []raft.Entry
	var recs []recMeta
	off := 0
	for off < len(buf) {
		if len(buf)-off < headerSize {
			break
		}
		n := int(binary.LittleEndian.Uint32(buf[off:]))
		sum := binary.LittleEndian.Uint32(buf[off+4:])
		end := off + headerSize + n
		if n > maxRecord || end > len(buf) {
			break
		}
		p := buf[off+headerSize : end]
		if crc32.Checksum(p, crcTable) != sum {
			if end == len(buf) {
				break
			}
			return nil, nil, 0, fmt.Errorf("%w at offset %d", ErrCorrupt, off)
		}
		if len(p) < entryFixed {
			return nil, nil, 0, fmt.Errorf("%w: short entry at offset %d", ErrCorrupt, off)
		}
		e := raft.Entry{
			Index: binary.LittleEndian.Uint64(p[0:]),
			Term:  binary.LittleEndian.Uint64(p[8:]),
			Type:  raft.EntryType(p[16]),
		}
		if len(p) > entryFixed {
			e.Data = append([]byte(nil), p[entryFixed:]...)
		}
		if len(ents) > 0 && e.Index != ents[len(ents)-1].Index+1 {
			return nil, nil, 0, fmt.Errorf("%w: index %d follows %d", ErrCorrupt, e.Index, ents[len(ents)-1].Index)
		}
		ents = append(ents, e)
		recs = append(recs, recMeta{index: e.Index, term: e.Term, off: int64(off)})
		off = end
	}
	return ents, recs, int64(off), nil
}
