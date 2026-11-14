package wal

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/Om-Patel88/raft-kv/raft"
)

func ents(lo, hi, term uint64) []raft.Entry {
	var out []raft.Entry
	for i := lo; i <= hi; i++ {
		out = append(out, raft.Entry{Index: i, Term: term, Type: raft.EntryCommand, Data: []byte(fmt.Sprintf("cmd-%d", i))})
	}
	return out
}

func mustOpen(t *testing.T, dir string) *WAL {
	t.Helper()
	w, err := Open(dir, Options{})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	return w
}

func mustLoad(t *testing.T, w *WAL) raft.PersistentState {
	t.Helper()
	st, err := w.Load()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	return st
}

func reopen(t *testing.T, w *WAL, dir string) *WAL {
	t.Helper()
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return mustOpen(t, dir)
}

func TestRoundTrip(t *testing.T) {
	dir := t.TempDir()
	w := mustOpen(t, dir)

	st := mustLoad(t, w)
	if st.HardState != (raft.HardState{}) || len(st.Entries) != 0 || st.Snapshot.Index != 0 {
		t.Fatalf("fresh wal not empty: %+v", st)
	}

	if err := w.SaveHardState(raft.HardState{Term: 3, VotedFor: "n2"}); err != nil {
		t.Fatal(err)
	}
	want := ents(1, 5, 1)
	want[2].Type = raft.EntryNoop
	want[2].Data = nil
	if err := w.Append(want[:2]); err != nil {
		t.Fatal(err)
	}
	if err := w.Append(want[2:]); err != nil {
		t.Fatal(err)
	}

	w = reopen(t, w, dir)
	defer w.Close()
	st = mustLoad(t, w)
	if st.HardState != (raft.HardState{Term: 3, VotedFor: "n2"}) {
		t.Fatalf("hard state = %+v", st.HardState)
	}
	if !reflect.DeepEqual(st.Entries, want) {
		t.Fatalf("entries = %+v, want %+v", st.Entries, want)
	}

	if err := w.Append(ents(7, 7, 1)); !errors.Is(err, raft.ErrNonContiguous) {
		t.Fatalf("gap append: err = %v", err)
	}
}

func TestTruncateSuffix(t *testing.T) {
	dir := t.TempDir()
	w := mustOpen(t, dir)
	w.Append(ents(1, 10, 1))
	if err := w.TruncateSuffix(6); err != nil {
		t.Fatal(err)
	}
	if err := w.Append(ents(6, 8, 2)); err != nil {
		t.Fatal(err)
	}
	w = reopen(t, w, dir)
	defer w.Close()

	want := append(ents(1, 5, 1), ents(6, 8, 2)...)
	if got := mustLoad(t, w).Entries; !reflect.DeepEqual(got, want) {
		t.Fatalf("entries = %+v", got)
	}
}

func TestTornTailIsTruncated(t *testing.T) {
	dir := t.TempDir()
	w := mustOpen(t, dir)
	w.Append(ents(1, 4, 1))
	w.Close()

	// chop the last record in half, as if we crashed mid-write
	path := filepath.Join(dir, logName)
	info, _ := os.Stat(path)
	if err := os.Truncate(path, info.Size()-5); err != nil {
		t.Fatal(err)
	}

	w = mustOpen(t, dir)
	defer w.Close()
	if got := mustLoad(t, w).Entries; !reflect.DeepEqual(got, ents(1, 3, 1)) {
		t.Fatalf("entries after torn tail = %+v", got)
	}
	// the file is repaired, so appending carries on from 4
	if err := w.Append(ents(4, 5, 2)); err != nil {
		t.Fatal(err)
	}
	w = reopen(t, w, dir)
	defer w.Close()
	want := append(ents(1, 3, 1), ents(4, 5, 2)...)
	if got := mustLoad(t, w).Entries; !reflect.DeepEqual(got, want) {
		t.Fatalf("entries = %+v", got)
	}
}

func TestTornHeaderIsTruncated(t *testing.T) {
	dir := t.TempDir()
	w := mustOpen(t, dir)
	w.Append(ents(1, 2, 1))
	w.Close()

	f, _ := os.OpenFile(filepath.Join(dir, logName), os.O_WRONLY|os.O_APPEND, 0)
	f.Write([]byte{9, 0, 0})
	f.Close()

	w = mustOpen(t, dir)
	defer w.Close()
	if got := mustLoad(t, w).Entries; len(got) != 2 {
		t.Fatalf("got %d entries", len(got))
	}
}

func TestBadChecksumOnLastRecordIsTorn(t *testing.T) {
	dir := t.TempDir()
	w := mustOpen(t, dir)
	w.Append(ents(1, 3, 1))
	w.Close()

	path := filepath.Join(dir, logName)
	b, _ := os.ReadFile(path)
	b[len(b)-1] ^= 0xff
	os.WriteFile(path, b, 0o644)

	w = mustOpen(t, dir)
	defer w.Close()
	if got := mustLoad(t, w).Entries; !reflect.DeepEqual(got, ents(1, 2, 1)) {
		t.Fatalf("entries = %+v", got)
	}
}

func TestCorruptRecordDetected(t *testing.T) {
	dir := t.TempDir()
	w := mustOpen(t, dir)
	w.Append(ents(1, 3, 1))
	w.Close()

	// flip a data byte inside the first record; later records are intact,
	// so this cannot be a torn write
	path := filepath.Join(dir, logName)
	b, _ := os.ReadFile(path)
	b[headerSize+entryFixed] ^= 0xff
	os.WriteFile(path, b, 0o644)

	if _, err := Open(dir, Options{}); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("open corrupt wal: err = %v, want ErrCorrupt", err)
	}
}

func TestCorruptStateDetected(t *testing.T) {
	dir := t.TempDir()
	w := mustOpen(t, dir)
	w.SaveHardState(raft.HardState{Term: 7, VotedFor: "a"})
	w.Close()

	path := filepath.Join(dir, stateName)
	b, _ := os.ReadFile(path)
	b[len(b)-1] ^= 0x01
	os.WriteFile(path, b, 0o644)

	w = mustOpen(t, dir)
	defer w.Close()
	if _, err := w.Load(); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("load: err = %v, want ErrCorrupt", err)
	}
}

func TestSnapshotCompactsLog(t *testing.T) {
	dir := t.TempDir()
	w := mustOpen(t, dir)
	w.Append(ents(1, 10, 1))
	if err := w.SaveSnapshot(raft.Snapshot{Index: 6, Term: 1, Data: []byte("state@6")}); err != nil {
		t.Fatal(err)
	}
	// appends continue after compaction
	if err := w.Append(ents(11, 12, 2)); err != nil {
		t.Fatal(err)
	}
	w = reopen(t, w, dir)
	defer w.Close()

	st := mustLoad(t, w)
	if st.Snapshot.Index != 6 || st.Snapshot.Term != 1 || string(st.Snapshot.Data) != "state@6" {
		t.Fatalf("snapshot = %+v", st.Snapshot)
	}
	want := append(ents(7, 10, 1), ents(11, 12, 2)...)
	if !reflect.DeepEqual(st.Entries, want) {
		t.Fatalf("entries = %+v", st.Entries)
	}
	if err := w.TruncateSuffix(6); err == nil {
		t.Fatal("truncating into the snapshot should fail")
	}
}

func TestSnapshotConflictDropsLog(t *testing.T) {
	dir := t.TempDir()
	w := mustOpen(t, dir)
	defer w.Close()
	w.Append(ents(1, 10, 1))
	// snapshot from a leader whose entry 5 has a different term: our
	// later entries cannot be trusted
	if err := w.SaveSnapshot(raft.Snapshot{Index: 5, Term: 2}); err != nil {
		t.Fatal(err)
	}
	if got := mustLoad(t, w).Entries; len(got) != 0 {
		t.Fatalf("expected empty log, got %d entries", len(got))
	}
	if err := w.Append(ents(6, 6, 2)); err != nil {
		t.Fatal(err)
	}
}

func TestSnapshotBeyondLog(t *testing.T) {
	dir := t.TempDir()
	w := mustOpen(t, dir)
	w.Append(ents(1, 3, 1))
	w.SaveSnapshot(raft.Snapshot{Index: 20, Term: 4, Data: []byte("x")})
	w = reopen(t, w, dir)
	defer w.Close()
	st := mustLoad(t, w)
	if st.Snapshot.Index != 20 || len(st.Entries) != 0 {
		t.Fatalf("state = %+v", st)
	}
	if err := w.Append(ents(21, 22, 4)); err != nil {
		t.Fatal(err)
	}
}

// Simulates a crash after the snapshot file was replaced but before the
// log was rewritten.
func TestCrashBetweenSnapshotAndCompaction(t *testing.T) {
	dir := t.TempDir()
	w := mustOpen(t, dir)
	w.Append(ents(1, 8, 1))
	w.Close()
	logPath := filepath.Join(dir, logName)
	oldLog, _ := os.ReadFile(logPath)

	w = mustOpen(t, dir)
	w.SaveSnapshot(raft.Snapshot{Index: 5, Term: 1, Data: []byte("s")})
	w.Close()
	os.WriteFile(logPath, oldLog, 0o644)

	w = mustOpen(t, dir)
	defer w.Close()
	st := mustLoad(t, w)
	if st.Snapshot.Index != 5 || !reflect.DeepEqual(st.Entries, ents(6, 8, 1)) {
		t.Fatalf("state = %+v", st)
	}
}

func TestLeftoverTempFilesIgnored(t *testing.T) {
	dir := t.TempDir()
	w := mustOpen(t, dir)
	w.SaveHardState(raft.HardState{Term: 2})
	w.Close()
	os.WriteFile(filepath.Join(dir, stateName+".tmp"), []byte("garbage"), 0o644)

	w = mustOpen(t, dir)
	defer w.Close()
	if st := mustLoad(t, w); st.HardState.Term != 2 {
		t.Fatalf("term = %d", st.HardState.Term)
	}
	if _, err := os.Stat(filepath.Join(dir, stateName+".tmp")); !os.IsNotExist(err) {
		t.Fatal("temp file should have been removed")
	}
}
